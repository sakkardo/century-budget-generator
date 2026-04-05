# Century Budget Generation Process
## As of April 1, 2026

### Overview
4 Yardi reports feed the budget workbook for each entity. Upload order matters — YSL must go first (creates the budget and GL lines), then the other 3 in any order.

---

### Step 1: Upload YSL Annual Budget (FIRST — creates the budget)
- Go to `century-budget-generator-production.up.railway.app/generate`
- Scroll to **Manual Upload** section
- Click "Choose Files" → select the YSL .xlsx file for the entity
- Click "Upload & Process"
- This creates a fresh budget version with all GL lines populated from Yardi actuals
- The entity code and building name are read from inside the file

### Step 2: Upload Expense Distribution
- Same Manual Upload section
- Select the Expense Distribution .xlsx file (must say "Expense Distribution" in Row 1)
- Click "Upload & Process"
- This stores all invoices and auto-calculates **accrual adjustments** — any invoice dated 12/31 of the prior year or earlier gets summed by GL code into the `accrual_adj` field on BudgetLine
- Entity code is detected from the file

### Step 3: Upload AP Aging
- Same Manual Upload section
- Select the AP Aging .xlsx file (detected by "payee code" + "current owed" headers)
- Click "Upload & Process"
- This populates the **Unpaid Bills** column — sums `current_owed` by GL code from open AP invoices
- Entity code is detected from the file

### Step 4: Upload Maintenance Proof
- Same Manual Upload section
- Select the Maintenance Proof .xlsx file
- Click "Upload & Process"
- This stores unit-level maintenance/common charge data
- Entity code is detected from the filename pattern (e.g., `Adhoc_AMP_204.xlsx`)

### Step 5: Review on FA Dashboard
- Go to the FA Dashboard for the entity
- Verify:
  - GL codes populated (from YSL)
  - Accrual Adjustment column has values (from Expense Distribution prior-year invoices)
  - Unpaid Bills column has values (from AP Aging)
  - Maintenance Proof data is stored

---

### Alternative: Automated Script (YSL + Maint Proof + AP Aging)
Instead of manual upload for 3 of the 4 reports:
1. On the Generate page, select entities, click **"Generate Yardi Script"**
2. Open Yardi in Chrome, press F12, paste script, hit Enter
3. Script runs Part 1 (YSL) + Part 2 (Maintenance Proof) + Part 3 (AP Aging) and auto-uploads to the server
4. Expense Distribution must still be uploaded manually (Yardi ViewState prevents automation)

---

### File Type Auto-Detection
The app identifies file types automatically:
- **YSL**: Default if not detected as another type (Row 1 checked for other report names)
- **Expense Distribution**: Row 1 contains "expense distribution" or "expense dist"
- **AP Aging**: Headers contain "payee code" + "current owed", or filename contains "aging"/"openap"
- **Maintenance Proof**: Row 1 contains "maintenance proof" or "adhoc_amp"

### Step 6: Edit Budget on FA Dashboard
- Click any sheet tab (Income, Payroll, Energy, Repairs & Supplies, Gen & Admin, RE Taxes)
- Editable cells: Prior Year, YTD Actual, Accrual Adj, Unpaid Bills, YTD Budget, Notes, Increase %
- Formula cells (green `fx` badge): Estimate, Forecast, Proposed — click to view/edit via the formula bar

#### Formula Bar System
- **Click any green `fx` cell** → formula bar at top shows the `=expression`
- **Type a number** → Accept saves it as an override (cell shows orange `✎` pencil badge)
- **Type a formula** (e.g., `=5000*12`) → Accept evaluates and saves (Proposed shows blue `fx` badge)
- **Type "formula" or "auto"** → Accept reverts to the auto-calculated value (back to green `fx`)
- **Cancel** → discards edit; **Clear** → removes override, reverts to auto-calc

#### Excel-Matching Formulas (from 204 budget template)
- **Estimate**: `=IF((YTD+Accrual+Unpaid) >= PriorYear, (YTD+Accrual+Unpaid)/YTD_MONTHS*REMAINING_MONTHS, PriorYear-(YTD+Accrual+Unpaid))`
- **Forecast**: `=YTD + Accrual + Unpaid + Estimate`
- **Proposed**: `=Forecast * (1 + Increase%)`
- **Variance**: `=Curr Budget - 12 Mo Forecast` (corrected April 3, 2026 — was incorrectly Proposed - Prior)
- **% Change**: `=(Curr Budget - 12 Mo Forecast) / 12 Mo Forecast` (corrected April 3 — matches 204 Excel `=+(L#-J#)/J#`)

#### Recalculation Cascade
Editing any cell (accrual, unpaid, increase %, or overrides) triggers: Estimate → Forecast → Proposed → Variance → Category Subtotals → Sheet Total. Same behavior as Excel.

---

### Key Technical Details
- All uploads go through `/api/process` endpoint
- Manual upload always creates a fresh budget version (`fresh_start=true`)
- The app is hosted on Railway, auto-deploys from GitHub main branch
- Deploy command: `bash deploy.sh` (from Budgets/ folder)
- Entity code is read from inside each file — no need to select it on the page for manual uploads
- Formula functions are in `workflow.py`: `faComputeEstimate`, `faComputeForecast`, `faLineChanged`, `faUpdateSheetTotals`, `faGetFormulaTooltip`
- Formula bar functions: `fxCellFocus`, `fxCellBlur`, `formulaBarAccept`, `formulaBarCancel`, `formulaBarClear`, `safeEvalFormula`

---

### PM Portal Features (April 1, 2026)

#### Invoice Drill-Down Table
- Click "View invoices" on any GL row to expand invoice detail
- Columns: Payee → Description → Invoice # → Date → Amount → Check # → Action
- Amounts formatted with cents and accounting parentheses for negatives: `($70.00)`
- Long text (payee names, descriptions) truncated with ellipsis
- Reclass button opens searchable modal with all 576 GL codes grouped by 9 categories

#### Searchable Reclass Modal
- Click "Reclass to…" on any invoice → modal with search bar + all GL codes
- Search filters by GL code, description, or category name
- GL codes grouped by category, sorted numerically
- Works for both invoice-level (inline) and GL-level reclasses

#### Accrual Adjustments
- Stored as **negative** values — they back out prior-year expenses from YTD
- Formula: `accrual_adj = -abs(total)` where total = sum of prior-year invoices for that GL
- Fixed in `expense_distribution.py` `apply_accrual_adjustments()` for all future uploads

#### CSS Lesson: table-layout:fixed + max-width:0 Kills Padding
- `max-width:0` on `<td>` collapses computed padding to `0px` — adjacent cell text merges visually
- Fix: remove all styling from `<td>`, wrap content in `<div>` with padding + `overflow:hidden`
- `overflow:hidden` does NOT work on `<td>` elements per CSS spec — only on block elements like `<div>`

---

### RE Taxes Tab — Formula Bar & Redesign (April 2, 2026)

#### Layout
- Budget workbook-style table: 1st Half (Jul–Dec), 2nd Half (Jan–Jun), Gross, Exemptions, Net
- Editable inputs: Transitional AV (Prior & Current), Tax Rate, Est Tax Rate, Exemption growth % and current year amounts
- All inputs use `data-raw` attribute for raw numeric values; display shows formatted text (`$39,979,620`)
- `reRaw(id)` helper reads `parseFloat(el.dataset.raw)` from any input
- `reSetCalc(id, text)` updates calculated cells by targeting inner `.re-fx-val` spans to preserve fx badges
- Tax rate sourced from Assumptions tab (`re_taxes_overrides.tax_rate`), falls back to hardcoded config in `dof_taxes.py`

#### Formula Bar (mirrors FA grid exactly)
- Sticky formula bar at top with `fx` badge, label, editable input, live preview, Accept/Cancel/Clear buttons
- **Click any green `fx` cell** → label shows cell name, bar shows **actual math with real numbers** (no words)
- Example: clicking "1st Half Tax" shows `= 39979620 * 0.096324 / 2` with preview `= $1,925,498`
- `_buildReTaxFormula(id)` dynamically builds the formula string by reading live cell values via `reRaw()`

#### Formula Mapping (matches 204 budget Excel)
- **1st Half Tax** (`re_h1_tax`): `= AV × Rate / 2` — Excel: `=(+G11*G12)/2`
- **2nd Half Tax** (`re_h2_tax`): `= AV2 × EstRate / 2` — Excel: `=(+G16*G17)/2`
- **Trans AV Change** (`re_trans_pct`): `= AV2 / AV - 1`
- **Gross Tax** (`re_gross`): `= H1Tax + H2Tax` — Excel: `=SUM(I11:I17)`
- **Exemption Budgets**: `= Current × (1 + Growth)` — Excel: `=+F25*1.02` pattern
- **Co-op Abatement**: Excel: `=+((I11*2)*17.5%)*89%`
- **Total Exemptions**: `= SUM(all exemption budgets)`
- **Net Tax** (`re_net`): `= Gross - Total Exemptions`

#### Override Behavior (same as FA grid)
- **Type a formula** (e.g., `= 2000000 * 0.10 / 2`) → Accept evaluates, saves override, badge turns blue `fx`
- **Type a plain number** (e.g., `1950000`) → Accept saves override, badge turns orange `✎` pencil
- **Type "auto" or clear** → reverts to auto-calculated formula, badge returns to green `fx`
- Overrides auto-save via `saveRETaxes()` and persist in `assumptions_json.re_taxes_overrides`

#### Key Functions (all in `workflow.py`)
- `renderRETaxesTab()` — renders the full RE Taxes HTML with formula bar
- `reTaxFxClick(el)` — populates formula bar when clicking an fx cell
- `_buildReTaxFormula(id)` — builds live formula string with actual numeric values
- `reTaxFormulaPreview()` — live preview via `safeEvalFormula()`
- `reTaxFormulaAccept()` / `reTaxFormulaCancel()` / `reTaxFormulaClear()` — edit lifecycle
- `reCalcTaxes()` — recalculates all derived cells using `reRaw()` + `reSetCalc()`
- `saveRETaxes()` — saves all RE tax data to server via `/api/re-taxes/<entityCode>`

---

### Summary Tab — Formula Bar & GL Breakdown (April 2, 2026)

#### Overview
The 📊 Summary tab (`renderBudgetSummary`) now has a full formula bar system. Every column except **Prior Year Actual** is clickable with a green `fx` badge. Clicking any cell shows where the numbers come from.

#### Columns with Formula Bar
- **Current Budget** — sum of `current_budget` across GL lines in that category
- **Proposed Budget** — sum of `proposed_budget` (or `forecast × (1 + increase_pct)`) across GL lines
- **$ Variance** — `= Proposed - Prior`
- **% Change** — `= (Proposed / Prior - 1) × 100`

#### Formula Bar Behavior
- **Click any fx cell** → label shows category/field, bar shows real math with actual numbers
- **Detail breakdown** appears below the bar showing each GL code, description, and dollar amount
- Example: clicking "Energy / Proposed" shows:
  - Bar: `= 601963 + 12769 + 232423`
  - Detail: `Energy — 3 GL lines: 5252-0000 Gas - Heating $601,963 + 5250-0000 Gas $12,769 + 5255-0000 Electricity $232,423`
- Categories with 10+ GL lines show: `= SUM(18 GL lines) = 9,434,429`
- Variance cells show: `= 847155 - 847155`
- % Change cells show: `= (847155 / 847155 - 1) * 100`
- Summary is **read-only** — formula bar shows formulas but does not accept edits

#### Data Flow
- Each `SUMMARY_ROWS` entry maps to a sheet + optional `rowRange` filter
- `_sumCatData[cellId]` stores the field type and GL lines array for each fx cell
- `_buildSumFormula(cellId)` reads live GL line values to construct the formula string
- `_buildSumDetail(cellId)` generates the GL-level breakdown with codes and descriptions
- Total Operating Expenses and NOI rows aggregate across all expense/income SUMMARY_ROWS

#### Key Functions (all in `workflow.py`)
- `renderBudgetSummary(contentDiv)` — renders the Summary tab HTML with formula bar
- `sumFxClick(el)` — populates formula bar and detail when clicking an fx cell
- `_buildSumFormula(cellId)` — builds formula string from live GL data
- `_buildSumDetail(cellId)` — builds GL code/description/amount breakdown
- `_sumFormulaPreview()` — evaluates formula and shows result
- `sumFormulaCancel()` — dismisses the formula bar selection

---

### Variance & % Change Formula Correction (April 3, 2026)

#### The Bug
All $ Variance and % Change formulas across the entire budget (FA dashboard, PM portal, Summary tab) were using the wrong formula: `Proposed - Prior Year` / `(Proposed / Prior) - 1`. The correct formula per the 204 Excel reference is:
- **$ Variance** = `Curr Budget - 12 Mo Forecast` (Excel: `=+(L#-J#)`)
- **% Change** = `(Curr Budget - 12 Mo Forecast) / 12 Mo Forecast` (Excel: `=+(L#-J#)/J#`)

#### Locations Fixed (20+ in workflow.py)
- `buildLineRow()` — line-level variance/pct cells + data-formula attributes
- `faLineChanged()` — cascade recalculation
- `subtotalRow()` / `updateTotalRow()` — category subtotals and sheet totals
- `fxSubtotalFocus()` — formula bar labels for subtotals
- Summary tab — category rows, Total Expenses, NOI, tooltip labels
- `_buildSumFormula()` — Summary formula bar expressions
- PM portal — header cards, line rows, `lineChanged()`, category/grand totals, Summary view

#### Verification
Confirmed on production: GL 4010-0000 shows formula `= $9,384,325 - $9,034,311`, header cards show Variance `$-8,506,743` / `-27.9%`.

---

### PM Portal Tier 1-5 Cell Functionality (April 3, 2026)

#### Overview
PM portal now has identical cell functionality to the FA dashboard. All 5 tiers implemented:

#### Tier 1 — fx Badges
- Blue `fx` badges on all formula cells: Estimate, Forecast, Proposed, $ Variance, % Change
- fx badges on all subtotal and grand total cells
- Badge CSS: `.pm-fx` — positioned absolute top-right, 9px font, blue background

#### Tier 2 — Editable Formula Bar
- Full formula bar matching FA: editable input, Accept/Cancel/Clear buttons, live preview
- `pmFormulaBar`, `pmFormulaLabel`, `pmFormulaPreview`, `pmFormulaAccept`, `pmFormulaCancel`, `pmFormulaClear`
- Click any formula cell → bar shows computable math with actual numbers (e.g., `=70611.75-(7799.23+-2350.02+3958.66)`)
- Editable for Estimate/Forecast/Proposed; read-only display for Variance/% Change and subtotals
- Live preview shows computed result as you type (`= $61,204`)
- Enter = Accept, Escape = Cancel keyboard shortcuts

#### Tier 3 — Editable Cells ($cell pattern)
- All cells editable (Prior Year, YTD Actual, Accrual Adj, Unpaid Bills, YTD Budget, Curr Budget, Increase %)
- Cells show formatted `$1,234` normally, switch to raw number on focus, reformat on blur
- `pmCellBlur(el)` handles formatting + LINES update + cascade trigger
- Increase % properly converts display (5.0) ↔ decimal (0.05)
- Formula cells (Estimate/Forecast/Proposed) editable via formula bar Accept
- Override handling: `line.estimate_override`, `line.forecast_override`, `line.proposed_budget`

#### Tier 4 — Subtotal Formula Cells
- Subtotal and grand total rows have fx badges + `data-col`/`data-raw` attributes
- Clicking subtotal → formula bar shows read-only SUM formula
- `pmSubtotalFocus(td)` gathers GL values and displays `= $70,612 + $7,799 + ...`
- `pmUpdateTotals()` recalculates all subtotal and grand total rows when any cell changes

#### Tier 5 — Input Alignment
- `font-variant-numeric: tabular-nums` on `.number` and `.pm-cell` classes
- Consistent cell sizing: `.pm-cell` (90px), `.pm-cell-pct` (60px)
- Green formula cell background: `input.pm-cell-fx { background:#f0fdf4; border:1px solid #bbf7d0; }`
- CSS specificity fix: `input.pm-cell-fx` overrides generic `input[type="text"] { background:#fffff0 }`

#### Cascade Recalculation
`pmLineChanged(gl, field, value)` triggers full cascade:
1. Editing any cell → updates LINES object
2. Recalculates: Estimate → Forecast → Proposed → Variance → % Change
3. Updates formula cells' `data-formula` attributes with new math
4. Calls `pmUpdateTotals()` to refresh subtotal and grand total rows
5. Triggers debounced `saveAll()` (800ms)

#### Override System
- **Type a formula** (e.g., `=50000*12`) → `safeEvalFormula()` evaluates → badge turns blue `fx`
- **Type a number** (e.g., `600000`) → badge turns orange `✎` pencil
- **Clear** → nulls override (`estimate_override`, `forecast_override`, `proposed_formula`), reverts to auto-calc, badge returns to default `fx`
- Green confirmation flash on Accept

#### Key Functions (all in PM `<script>` block in `workflow.py`)
- `pmCellBlur()` — formats editable cell on blur, triggers cascade
- `pmFxCellFocus()` — opens formula bar for formula cells
- `pmSubtotalFocus()` — shows SUM formula for subtotals (read-only)
- `pmFormulaBarPreview()` — live preview while typing
- `pmFormulaBarAccept()` — commits formula/value, sets overrides, updates badge
- `pmFormulaBarCancel()` — reverts formula bar
- `pmFormulaBarClear()` — removes override, reverts to auto-calc
- `pmFormulaBarKeydown()` — Enter/Escape handling
- `pmGetFormulaTooltip()` — builds formula strings with actual numbers (matches FA's `faGetFormulaTooltip`)
- `pmLineChanged()` — cascade recalculation + data-formula refresh
- `pmUpdateTotals()` — recalculates all subtotal and grand total rows
- `parseDollar()` / `safeEvalFormula()` — added to PM scope (were only in FA scope)

#### Pending (on hold)
- **Approval Workflow**: PM changes write to staging table `PendingPMChange`, FA reviews/accepts/rejects before applying to main budget. Architecture proposed but paused per user direction.

---

### PM/FA Collaborative Review System (April 3, 2026)

#### Overview
Full collaborative workflow where PM makes changes (notes, reclasses, budget proposals) and FA reviews, accepts, rejects, or comments. All actions visible in both dashboards with audit trail.

#### FA Dashboard — PM Review Panel (3 Tabs)
Collapsible panel above the Budget Workbook with yellow gradient header and pulsing badge showing total items needing review.

**Tab 1: PM Notes**
- Displays all GL lines where PM left notes
- Clickable GL code badges scroll to the row in the workbook
- Shows description + note text in amber callout

**Tab 2: Invoice Reclasses**
- Aggregates invoice-level reclasses by from_gl → to_gl pairs
- Shows invoice count, total amount, PM note
- **Expandable rows**: click any group to reveal individual invoices (vendor, description, invoice #, date, amount)
- Accept button: moves ytd_actual between GLs via `/api/reclass/accept` with BudgetRevision audit trail
- Undo button: restores invoices to original GL code

**Tab 3: Budget Proposals**
- Detects PM changes: `increase_pct != 0`, estimate/forecast overrides, or proposed_budget differs from current_budget
- Shows: GL code, description, current budget, PM proposed, $ change (% change), method used, status
- **Accept**: one-click, sets `fa_proposed_status = 'accepted'`, appends `[FA ACCEPTED]` to notes
- **Reject**: modal with optional override value + reason. FA's override replaces proposed_budget. Appends `[FA REJECTED]` with details to notes
- **Comment**: modal for note only. Status shows "Commented", action buttons stay available
- All actions create `BudgetRevision` audit entries

#### PM Dashboard — My Changes Panel (2 Tabs)
Read-only collapsible panel above the spreadsheet with blue gradient header.

**Tab 1: My Notes**
- Shows all GLs where PM left notes
- FA responses (ACCEPTED/REJECTED/COMMENT) displayed inline in blue callout below PM's note
- Splits note text by line: PM notes in amber, FA responses in blue

**Tab 2: My Reclasses**
- Shows grouped invoice reclasses with from_gl → to_gl
- **Expandable rows**: click to see individual invoices (same as FA version)
- FA Status badge: Pending (amber) or Accepted (green)
- Read-only — no action buttons

#### PM Dashboard — YTD Reclass Adjustment Fix
- `applyReclassAdjustments()` refactored from IIFE to named async function
- Resets all YTD values to `_db_ytd_actual` before reapplying adjustments (prevents double-counting)
- Called after every `inlineReclass()` and `inlineUndoReclass()` with `await`
- Expense cache (`_expenseCache`) cleared before re-run
- YTD totals now update immediately after each reclass action (previously only on page load)

#### New BudgetLine Fields
```python
fa_proposed_status = db.Column(db.String(20), nullable=True)  # null=pending, accepted, rejected, commented
fa_proposed_note = db.Column(db.Text, default="")
fa_override_value = db.Column(db.Float, nullable=True)  # FA's override when rejecting
```
- Auto-migration entries in `app.py` `_migrations` list
- Included in `to_dict()` serialization

#### New API Endpoints
- **`POST /api/budget-proposal/review`** — FA accept/reject/comment on PM budget proposals
  - Validates action ∈ {accepted, rejected, commented}
  - Float conversion with try/except for override_value
  - Appends timestamped note entry to line.notes
  - Creates BudgetRevision audit entry
  - Uses module-level datetime import (not inline)

- **`POST /api/reclass/accept`** (existing) — FA accepts invoice reclass, moves ytd_actual between GLs

#### Key JS Functions — FA Dashboard
- `switchPmTab(button, tabId)` — handles 3-tab switching (Notes, Reclasses, Proposals)
- `toggleReclassInvDetail(gid)` — expand/collapse invoice detail rows in reclass tab
- `proposalActionButtons(glCode)` — generates Accept/Reject/Comment buttons
- `acceptProposal(glCode)` — one-click accept with API call
- `openProposalModal(glCode, action)` — opens reject/comment modal
- `closeProposalModal()` — dismisses modal
- `submitProposalReview()` — sends reject/comment to API, updates DOM
- `updateProposalBadge()` — recounts pending items, updates badge text

#### Key JS Functions — PM Dashboard
- `switchPmMcTab(button, tabId)` — handles 2-tab switching (Notes, Reclasses)
- `pmToggleReclassInv(gid)` — expand/collapse invoice detail rows
- `populateMyChanges()` — async IIFE that populates both tabs on load
- `applyReclassAdjustments()` — named async function for YTD adjustment

#### Commit Trail (April 3 session)
| Commit | Description |
|--------|-------------|
| `4465417` | Last stable before session |
| `fc0477d` | Budget Proposals tab on FA |
| `5e4fa11` | PM YTD reclass fix |
| `503d11a` | PM My Changes panel |
| `d7d0707` | Invoice detail expansion + debug fixes |

#### Debug Findings (April 3)
- Float conversion in `/api/budget-proposal/review` had no error handling — fixed with try/except returning 400
- Redundant inline `from datetime import datetime as _dt` — removed, uses module-level import
- PM reclass onclick "missing button guard" — confirmed not needed (PM reclasses are read-only, no buttons)
- XSS in description fields — low risk (internal users only, Yardi data pipeline)

---

## April 4, 2026 Session

### Critical Bug Fixes

#### Bug #1 - FA cell save dropping fields (DATA LOSS)
Severity: Critical - every FA dollar cell edit and Inc% edit was silently discarded.

Root cause: faAutoSave() used a single-timer debounce that stored only the most recent field. When cellBlur triggered faLineChanged() which called faAutoSave twice in rapid succession (source field + recalculated proposed_budget), call #2 cancelled call #1's timer. Only the last field reached the server.

Fix (e0935a2): Replaced single-field timer with a pending accumulator object. Every faAutoSave() call now adds to _faSavePending[gl][field] = value, then a single debounced flush POSTs all batched fields in one request.

Live verification: Edited Accrual Adj=$5,000 and Inc %=10% on GL 5250-0000. Before fix: DB showed accrual=$1,208 (unchanged). After fix: both persisted.

#### Bug #2/#3 - No error handling on save endpoints
Added try/except/rollback to both PUT /api/fa-lines and PUT /api/lines endpoints. Prevents PostgreSQL session poisoning on commit failures.

#### Bug #4 - PM audit trail missing
PM endpoint was silently overwriting fields with no history. Now creates BudgetRevision entries for every field change with source="pm" (mirrors FA's source="web").

### Excel Export Test Scripts (Not Wired to App)
Built two standalone scripts proving full budget export with FA/PM edits:
- test_excel_export.py - flat-layout proof-of-concept from scratch
- excel_export_v2.py - copies Budget_Final_Template_v2.xlsx + fills all 11 sheets including FA/PM edits, RE Taxes, Payroll roster/assumptions, and 400+ unmapped GL codes with live Excel formulas

Status: Reference only. Existing /api/download-budget/<entity_code> endpoint still uses template_populator.populate_template() which only exports raw Yardi data. To wire up: extract fill_* functions into budget_app/excel_exporter.py and replace the download endpoint body.

Known issues (for future session):
- Budget Summary RE Taxes total pulls from Gen & Admin 6315 GL lines, not from the RE Taxes calculation sheet
- Payroll template had a bug: formulas referenced $L$7/$L$8/$L$10/$L$11 (text labels) instead of $M$7-$M$11 (values). The test script patches those refs on copy.

### Payroll Tab - Major Rebuild

#### New: Roster to GL Linkage (16 GLs auto-driven)
The Payroll roster calculation now drives 16 GL Proposed values automatically. Changing hourly rate, employee count, or any assumption cascades through to the GL lines in real time.

PAYROLL_COMPONENT_MAP (workflow.py around line 6737):
- annual_base to 5105-0000 (Gross Payroll)
- ot to 5105-0010 (Overtime Pay)
- vsh_vacation/vsh_holiday/vsh_sick to 5105-0015/0020/0025 (1/3 of VSH each)
- bonus to 5105-0035 (flat dollars/emp times count)
- employer_taxes to 5145-0000 (FICA+SUI+FUI+MTA)
- workers_comp to 5165-0000
- nys_disability to 5166-0000
- pfl to 5168-0000
- welfare to 5155-0015
- pension to 5160-0010
- supp_retirement to 5160-0020
- legal_fund/training_fund/profit_sharing to 5160-0025/0030/0035

pushRosterToGL() - called after every recalcPayroll():
- Updates _payrollGLLines in memory
- Sets line._linked = true for mapped GLs
- Saves batched changes via /api/fa-lines (800ms debounce)
- Skips rows where user has manually overridden (proposed_formula is set)

#### New: Bonus Dollars/Employee Column
- Added bonus_per_employee FLOAT DEFAULT 0 column to payroll_positions table
- New "Bonus $/Emp" column in roster table (after Hourly Rate)
- Total bonus = sum of (count x bonus_per_employee) -> drives GL 5105-0035

#### New: Per-Position Effective Week Override
- Added effective_week_override FLOAT NULL column to payroll_positions
- New "Eff Wk Override" column in roster - leave blank for global, or set a specific week for that position only
- Use case: one Resident Manager gets a mid-year raise on Wk 20 while everyone else waits until Wk 16
- recalcPayroll() uses per-position pre/post week split when override is set

#### New: Excel-Style Formula Bar on Payroll Tab
The Payroll tab now has the same formula bar as R&S/other tabs. All calculated cells are clickable.

Editable cells (via formula bar):
- Estimate, Forecast - editable override with live preview
- Proposed (non-linked rows) - editable override, type formulas or plain numbers
- Inc% (non-linked rows) - editable yellow input
- All dollar cells - Prior Year, YTD Actual, Accrual Adj, Unpaid Bills, YTD Budget, Curr Budget - editable text inputs

Read-only view-formula cells:
- Proposed (linked rows) - driven by roster, not editable. Click to see formula.
- Roster calculated cells - Weekly Pay, Pre/Post Wages, Post-Incr Rate, Annual Base, OT, VSH, Total Comp. Click to see math formula in the bar, read-only.

Formula bar is sticky - stays at top of viewport as user scrolls through GL rows.

Extended fxCellFocus() to support a new data-readonly="true" attribute so roster cells can open in view-only mode.

#### New: Clickable Linked GLs Breakdown
The Linked GLs (Auto) tile in the tie-out bar is now clickable. Expands an inline table showing:
- Each of the 16 linked GL codes
- The roster component driving it
- Current roster-driven value
- Override input + button per row

Entering a value and clicking Override sets manual value, marks the row as proposed_formula="manual" (unlinks from roster), saves via /api/fa-lines, row moves to Manual GLs column in tie-out.

#### Fixed: Payroll Template Formula Bug
The Budget_Final_Template_v2.xlsx had assumption formulas referencing column L which contains text labels instead of numeric values (values live in column M). The excel_export_v2.py script now rewrites these formula references from L to M on template copy.

#### Cosmetic / UX Fixes
- Green backgrounds on all roster calculated cells (matches R&S cell-fx styling)
- Font unified to Plus Jakarta Sans across the Payroll tab (removed SF Mono/Fira Code monospace)
- Assumption field alignment fix - prAssumpRow() always renders the 12px suffix span so non-% rows do not shift right
- Pre-Incr Weeks / Post-Incr Weeks now editable (was read-only)

### Commit Trail (April 4 session)
| Commit | Description |
|--------|-------------|
| e0935a2 | Fix critical FA cell save bug (Bug #1 - data loss) |
| 7a5ed8c | Add PM audit trail + error handling |
| 00ad2b0 | Add Excel export test scripts (TEST/REFERENCE) |
| e8f3df1 | Payroll: wire roster/assumptions to drive 15 GL proposed values |
| a7910fa | Payroll: add Bonus dollars/Emp column + fx badges |
| a59a691 | Payroll: make Estimate/Forecast/Proposed editable via formula bar |
| 7e3e04a | Payroll: add formula bar HTML to tab |
| 0613e2a | Payroll: fix formula strings to match safeEvalFormula grammar |
| ee2ef95 | Payroll: make Pre-Incr Weeks and Post-Incr Weeks editable |
| d6a5bb6 | Payroll: make all GL cells editable + sticky formula bar |
| 9a12ded | Payroll: match R&S cell styling exactly |
| 1df8502 | Payroll: fix sticky formula bar scroll context |
| a01d3fa | Payroll: clickable Linked GLs tile with breakdown + override |
| bc34dfc | Payroll: make Employee Roster calculated cells clickable |
| 4d625c4 | Payroll: add green bg to roster calc cells |
| 70af345 | Payroll: per-position Eff Wk override, unified fonts, aligned assumptions |
| 54bc06e | Payroll: inherit font-family on assumption inputs |

### Debug Audit Results (April 4)
Systematically verified all 17 commits against production:
- Zero console errors on page load
- All helper functions present: pushRosterToGL, payrollCellEdited, prDollarCellBlur, togglePrLinkedBreakdown, prOverrideLinkedGL
- DOM structure correct: 16 components mapped, 3 bonus inputs, 3 Eff Wk inputs, 261 editable GL cells, 24 roster fx cells
- Eff Wk Override end-to-end: porter wk=26 -> Annual Base $925,540 (from $1,004,080), persisted to DB, cleared correctly
- Bug #1 regression test passed: edited accrual_adj=500 + increase_pct=7% together, both saved (accumulator batching works)
- Assumption field alignment: all inputs align at exact x-positions per column (763/1153/1544)
- Font consistency: Plus Jakarta Sans everywhere after 54bc06e fix
