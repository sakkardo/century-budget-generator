# Lessons Learned

## RE Taxes Feature - Deployment Lessons (2026-03-27)

1. **sys.path matters for sibling imports**: `workflow.py` importing `from dof_taxes import ...` failed on Railway because `budget_app/` wasn't on `sys.path`. Fix: added `sys.path.insert(0, str(Path(__file__).parent))` in `app.py`. Always check import paths when adding new modules to `budget_app/`.

2. **GitHub upload page is the reliable path**: The edit page (CodeMirror 6) doesn't support programmatic content replacement via DragEvent. The upload page (`/upload/main/budget_app`) + `file-attachment` DragEvent approach works consistently.

3. **JavaScript `.replace()` has dollar-sign expansion**: `$'` in replacement strings expands to "portion after match". Use manual `indexOf` + `substring` instead of `.replace()` when replacement content contains `$` characters (common in template literals).

4. **Railway auto-deploys from main**: Commits to `sakkardo/century-budget-generator` main branch trigger Railway deployment automatically. Takes ~30-60 seconds.

## QA Results - Entity 204 (2026-03-27)

- All 11 tabs render with zero JS errors
- RE Taxes: Save (PUT), Refresh (GET), calculations all working
- Download Excel returns 200 with correct XLSX content type
- Pre-existing data issues (not bugs): Insurance proposed $3.5M (vs current $642K), Taxes $7.3M - likely from unset assumptions
- Capital Budget table in Summary shows GL codes without descriptions
- Only entity 204 has budget data loaded; 148, 206, 106 need YSL uploads

## FA Dashboard Redesign - Deployment (2026-03-28)

1. **Browser find/replace for large file uploads**: For files too large to chunk through JS injection, fetch the current version from `raw.githubusercontent.com` in the browser, apply find/replace edits in JS memory, then upload via `file-attachment` DragEvent on the GitHub upload page. All edits stay in browser memory.

2. **GitHub upload page commit button**: The "Commit changes" button is NOT `button[type="submit"]` — it's a regular `<button>` found by text content `'Commit changes'`. The first `submit` button is "Submit feedback" (search feedback form). Always filter by text content.

3. **file-attachment DragEvent works**: Create `new File([blob], name)`, add to `DataTransfer`, dispatch `DragEvent('drop')` on the `file-attachment` element. Wait 3 seconds for processing before committing.

4. **Unicode in JS string replacements**: Use `\\uXXXX` escape sequences in template literals for special characters (▾ = `\\u25be`, — = `\\u2014`, · = `\\u00b7`).

## Architecture Notes

- `dof_taxes.py` lives in `budget_app/` alongside `workflow.py`
- RE Taxes tab only appears for co-ops (controlled by `is_coop()` in `dof_taxes.py`)
- PROPERTY_TAX_CONFIG has data for entities: 204 (444 E 86th), 148 (130 E 18th), 206 (77 Bleecker), 106 (5 W 14th - no data yet)
- The dashboard API must succeed (YSL data loaded) for RE Taxes tab to appear - it's embedded in the dashboard response

---

## Yardi ASP.NET Postback Rules — CRITICAL (2026-03-30)

### fetch() CANNOT change Yardi ReportType — EVER
- ASP.NET WebForms ViewState is server-side stateful
- `fetch()` GET creates a new page session with default ViewState
- Including `ReportType:DropDownList=2` in a fetch POST body is silently IGNORED
- Tested: hidden inputs only, full FormData, URLSearchParams, multipart — ALL fail
- The ONLY way to change ReportType is via real form submission (`__doPostBack`)

### The Working Pattern: Iframe Postback (v7/v3)
1. Create hidden iframe: `const wf = document.createElement('iframe'); wf.src = PAGE_URL`
2. Wait for load: `await new Promise(r => { wf.onload = r })`
3. Access iframe DOM: `wf.contentDocument.querySelector(...)`, `wf.contentWindow.__doPostBack(...)`
4. Change dropdown in iframe DOM + `__doPostBack('ReportType:DropDownList', '')` → iframe reloads
5. Set property/period in iframe DOM + `__doPostBack('PropertyLookup:LookupCode', '')` → iframe reloads
6. Re-set fields after each postback (form innerHTML gets replaced)
7. For Excel: `new FormData(wf.contentDocument.querySelector('form'))` + `fetch()` — works because iframe's ViewState has correct RT
8. Delete empty `__VIEWSTATE` from FormData if `__VIEWSTATE__` exists (Yardi uses `__VIEWSTATE__` with trailing underscore, `__VIEWSTATE` is always empty)
9. Main page stays alive — combined script continues

### Yardi APAnalytics.aspx Report Types
| Value | Report |
|-------|--------|
| 1 | Expense Distribution |
| 2 | **Expense Distribution (Paid Only)** ← we use this |
| 3 | **Aging** ← we use this for AP/Open AP |
| 7 | Payment Register |
| 8 | Vendor Directory |
| 9 | Payee Ledger |
| 10 | Payee Total |
| 12 | Payment Run Report |
| 13 | AP Templates |

### Combined Script Architecture
- 4 parts run sequentially in one IIFE: YSL → ExpDist → MaintProof → APAging
- Scripts MUST NOT call `window.location.href` — kills the entire combined script
- Each APAnalytics script creates its own hidden iframe to do its work
- `triggerDownload(blob, entity)` must have exact signature — app.py patches it to inject `_autoUpload`
- Closing pattern must be exactly: `URL.revokeObjectURL(a.href);\n  }` (LF, 2 spaces)
- CRLF normalization: app.py does `.replace("\r\n", "\n")` on script load to ensure patches match

### CRITICAL: ReportType Reverts After Property Postback (2026-03-30)
- **Root cause found:** AP Aging script set RT=3 once, but `__doPostBack('PropertyLookup:LookupCode')` resets the form including the ReportType dropdown back to default (RT=2 = Expense Distribution)
- **Result:** Every AP Aging export was actually downloading Expense Distribution data — parser saw "Expense Distribution (Paid Only)" in Row 1, failed detection, and the file got skipped
- **Fix (v4):** Added `ensureAgingRT()` helper that checks and re-sets RT=3 after every property postback. Also verify RT immediately before Excel fetch
- **Why Expense Dist worked:** Its RT (2) happens to be the default, so property postback resetting the dropdown had no effect
- **Secondary fix:** Changed AP Aging file extension from `.xls` to `.xlsx` (Yardi exports xlsx-format files regardless of report type)
- **Detection belt-and-suspenders:** auto-process now uses the `X-File-Type: ap` upload header as primary routing before content detection

### The 4 Yardi Reports and Their Flow
1. **YSL Annual Budget** → `SysSqlScript.aspx` → `parse_ysl_file()` → `budgets` + `budget_lines` tables → Budget Workbook
2. **Expense Distribution** → `APAnalytics.aspx` RT=2 → `parse_expense_distribution()` → `expense_reports` + `expense_invoices` → GL drill-down + accrual adjustments
3. **AP Aging** → `APAnalytics.aspx` RT=3 → `parse_open_ap_report()` → `open_ap_reports` + `open_ap_invoices` → `unpaid_bills` on BudgetLines
4. **Maint Proof** → `CustomCorrespGenerate.aspx` → `parse_maintenance_proof()` → `maint_proof_reports` + `maint_proof_units` → income validation

---

## Database Session Poisoning — PostgreSQL (2026-03-30)

### Bare `except` Without `db.session.rollback()` Kills Everything
- If a raw SQL query fails (wrong table name, missing table), PostgreSQL puts the transaction in error state
- ALL subsequent ORM queries on that connection silently return nothing/None
- Connection pooling carries the poison across HTTP requests
- **ALWAYS** add `db.session.rollback()` in except blocks for raw SQL queries

### The `audit_upload` vs `audit_uploads` Bug
- `list_budgets` queried non-existent `audit_upload` (should be `audit_uploads`)
- Bare `except` caught the error without rollback
- Every ORM query after that returned None — "Budget not found" on delete, detail, status endpoints
- `Budget.query.all()` in `list_budgets` worked because it ran BEFORE the bad SQL

### App-Level `before_request` Rollback
- `@app.before_request` with `db.session.rollback()` ensures every request starts with a clean session
- Must be on the **app level** (`app.py`), not just the blueprint
- `/api/auto-process` is defined on `app.py`, not the workflow blueprint — blueprint-only hooks don't cover it

---

## Fresh Start + Budget Delete Behavior (2026-03-30)

### Fresh Start
- Creates a new budget version (version+1), old stays untouched
- Should NOT delete expense/AP data — the Yardi script re-uploads fresh copies
- `store_expense_report()` automatically replaces existing reports for same entity/period
- Only clear PM customizations (invoice reclasses) if needed — but even this is unnecessary since fresh data replaces it

### Budget Delete
- Use raw SQL for all FK-dependent deletes in dependency order:
  - `presentation_edits` (refs budget_lines) → `budget_revisions` (refs budget_lines AND budgets) → `presentation_sessions` → `ar_handoffs` → `data_sources` → `budget_lines` → `budgets`
- Delete entity-level data (expense_reports, open_ap_reports) in separate per-table try/except with rollback
- Only `approved` budgets are protected from deletion

### Timestamp Display
- `datetime.utcnow` stores UTC — append `"Z"` to `.isoformat()` output
- Without `Z`, browsers treat bare ISO strings as local time (shows ~4 hours ahead in EST)
