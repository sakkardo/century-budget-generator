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
- **Variance**: `=Proposed - PriorYear`
- **% Change**: `=Proposed / PriorYear - 1`

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
