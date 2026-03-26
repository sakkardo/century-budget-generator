# Sprint 2 Plan — Century Budget Generator

## Status: COMPLETE

---

## What Was Done

### Phase 1: Merge-Audit Endpoint (DONE)
- Enhanced `/api/dashboard/<entity_code>` to return structured multi-year audit data from ALL confirmed uploads (not just latest)
- Added "Historical Actuals" panel to FA dashboard detail page showing all Century categories with year-by-year audit amounts
- Added audit actual column to Budget Detail table — shows totals at category header rows, maps budget_line categories to Century audit categories via `BUDGET_CAT_TO_CENTURY`
- Fixed `get_confirmed_actuals()` bug in audited_financials.py — was reading `data["years"]` but mapped_data uses `year_totals`

### Phase 2: Status Workflow Bugs (DONE)
- Simplified `BUDGET_STATUSES` to match what the UI actually uses: `draft → pm_pending → pm_in_progress → fa_review → approved/returned`
- Removed unused `fa_first_review`/`fa_second_review` statuses that the UI never referenced
- Rewrote `change_budget_status()` to use `VALID_TRANSITIONS` dict instead of hardcoded checks
- Added `returned → draft` transition so returned budgets can be re-edited

### Phase 3: End-to-End Flow Test (DONE)
Tested full cycle with entity 204 (444 East 86th Street):
1. Budget exists in draft — verified dashboard detail page renders
2. Send to PM — status changes to pm_pending, PM portal shows building
3. PM edits — increase_pct adjustment on supply line, auto-saves
4. PM submits — status changes to fa_review
5. FA approves — status changes to approved

Bugs fixed during testing:
- `DataSource` model resolution error (SQLAlchemy mapper) — added `db.configure_mappers()` after all models defined
- UTF-8 encoding error reading JS files on Windows — added `encoding="utf-8"` to file reads in app.py
- PM portal showing "undefined" for building names — `getBuildingName()` was looking for `building.name` instead of `building.building_name`

### Phase 4: Expense Distribution Integration (DONE)
- Added "View Expense Report" button in PM edit page header linking to `/pm/<entity_code>/expenses`
- Added expandable invoice drill-down per GL row — click any GL code to see invoices inline
- Drill-down fetches from `/api/expense-dist/<entity_code>`, caches result, shows payee/invoice/date/amount
- Handles case where no expense data exists (shows info message)
- Toggle collapse on second click

### Remaining (Not Started)
- Phase 4c: Add expense distribution total column to PM grid (nice-to-have, deferred)

---

## Files Modified
- `budget_app/workflow.py` — Majority of changes (status workflow, audit display, expense drill-down, bug fixes)
- `budget_app/audited_financials.py` — Fixed `get_confirmed_actuals()` bug
- `budget_app/app.py` — Fixed UTF-8 encoding for JS file reads
