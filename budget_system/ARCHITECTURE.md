# Century Budget — Architecture Reference

> Technical reference for devs. Last updated April 5, 2026.

## Stack
- **Backend:** Flask (Python), PostgreSQL
- **Hosting:** Railway (auto-deploy from GitHub main branch)
- **Deploy:** `bash deploy.sh` from `Budgets/` folder
- **Production URL:** `century-budget-generator-production.up.railway.app`

## File Layout
| Path | Purpose |
|---|---|
| `budget_app/workflow.py` | Main Flask app — all routes, UI rendering, JS |
| `budget_app/app.py` | App factory, DB init, migrations list |
| `budget_system/` | Parsing scripts (YSL, AP Aging, Exp Dist, Maint Proof) |
| `deploy.sh` | Push to GitHub → Railway |

## DB Models

### `BudgetLine`
Core model. One row per GL code per budget version.

Fields: `gl_code`, `description`, `category`, `prior_year`, `ytd_actual`, `accrual_adj`, `unpaid_bills`, `ytd_budget`, `curr_budget`, `proposed_budget`, `increase_pct`, `notes`, `estimate_override`, `forecast_override`, `proposed_formula`, `fa_proposed_status`, `fa_proposed_note`, `fa_override_value`

### `PayrollPosition`
`id, entity_code, budget_year, position_name, employee_count, hourly_rate, sort_order, created_at, updated_at`

### `PayrollAssumption`
`id, entity_code, budget_year, assumptions_json, updated_at`

### Reclass models
Track PM invoice-level and GL-level reclasses with audit trail (`BudgetRevision`).

## API Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/process` | Upload any of 4 report types (auto-detects) |
| GET/POST | `/api/fa-lines/<entity_code>` | Read/save GL line edits |
| GET/POST | `/api/payroll/positions/<entity_code>` | Employee roster |
| GET/POST | `/api/payroll/assumptions/<entity_code>` | Payroll tab assumptions |
| POST | `/api/re-taxes/<entity_code>` | RE Taxes overrides |
| POST | `/api/reclass/accept` | FA accepts invoice reclass |
| POST | `/api/budget-proposal/review` | FA accept/reject/comment on PM proposals |

## Formula Engine

All formula logic lives in `workflow.py` JS blocks. Core functions:

**FA grid (`fa*` prefix):**
- `faComputeEstimate()`, `faComputeForecast()` — match Excel IF/IFERROR patterns
- `faLineChanged()` — cascade trigger
- `faUpdateSheetTotals()` — sheet-level rollup
- `faGetFormulaTooltip()` — builds `= 45220 + 3000 + ...` strings from live DOM

**Formula bar (shared):**
- `safeEvalFormula()` — evaluates `=expr` strings safely
- `fxCellFocus() / fxCellBlur()` — open/close formula bar
- `formulaBarAccept() / Cancel() / Clear()` — edit lifecycle

**PM grid (`pm*` prefix):**
- Identical API to FA, scoped to PM portal
- `pmLineChanged()`, `pmUpdateTotals()`, `pmGetFormulaTooltip()`, etc.

**RE Taxes tab (`re*` prefix):**
- `renderRETaxesTab()`, `reCalcTaxes()`, `_buildReTaxFormula()`, `reTaxFxClick()`

**Summary tab (`sum*` prefix):**
- `renderBudgetSummary()`, `sumFxClick()`, `_buildSumFormula()`, `_buildSumDetail()`

## Override State Model
Each formula cell has 3 states:
1. **Auto** — green `fx` badge. Recomputes on cascade. No override saved.
2. **Number override** — orange `✎` pencil. `estimate_override` or `forecast_override` or `proposed_budget` set directly.
3. **Formula override** — blue `fx` badge. `proposed_formula` set. Re-evaluates on cascade.

Clear button nulls the relevant override field → reverts to Auto.

## Auto-Detection Rules (`/api/process`)
| Report | Detected by |
|---|---|
| YSL | Default if no other match (Row 1 is scanned for other report names) |
| Expense Distribution | Row 1 contains "expense distribution" or "expense dist" |
| AP Aging | Headers contain "payee code" + "current owed", OR filename contains "aging"/"openap" |
| Maintenance Proof | Row 1 contains "maintenance proof" OR filename starts with "adhoc_amp" |

## Variance/% Change (corrected Apr 3, 2026)
```
$ Variance = Curr Budget - 12 Mo Forecast       // Excel: =+(L#-J#)
% Change  = (Curr Budget - 12 Mo Forecast) / 12 Mo Forecast   // Excel: =+(L#-J#)/J#
```
Applied across 20+ locations in workflow.py.

## Accrual Adjustment Sign
Stored as **negative** — backs out prior-year expenses from YTD:
```python
accrual_adj = -abs(total)  # total = sum of prior-year invoices for that GL
```

## Migration Pattern
Auto-migrations run on app startup via `_migrations` list in `app.py`. Each migration is a `CREATE TABLE IF NOT EXISTS` or `ALTER TABLE ADD COLUMN IF NOT EXISTS` — safe to re-run. PostgreSQL compatible.

## Known Technical Gotchas
- `table-layout:fixed` + `max-width:0` on `<td>` kills padding → wrap content in `<div>` with padding + `overflow:hidden`
- `overflow:hidden` does NOT work on `<td>` per CSS spec — only block elements
- Tax rates in RE Taxes tab source from Assumptions JSON (`re_taxes_overrides.tax_rate`), fall back to hardcoded config in `dof_taxes.py`

---

*See also: USER_GUIDE.md for end-user workflow, CHANGELOG.md for dated changes.*
