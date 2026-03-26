# Sprint 2 — Century Budget Generator

## Context
App: https://century-budget-generator-production.up.railway.app/
Repo: https://github.com/sakkardo/century-budget-generator
Test building: entity 204 (444 East 86th Street)

Read `.claude/plans/crispy-waddling-quokka.md` and memory at `~/.claude/projects/C--Users-jsirotkin-My-Drive-Claude-Claude-Work-Projects-Budgets/memory/MEMORY.md` for prior session context.

## Sprint 1 Completed
- Global nav, status pipeline, toast notifications, loading states
- PM dropdown fix, expense distribution entity mismatch fix
- FA dashboard: all sheets fully editable (15-column format)
- PM review: R&S lines only, matching template layout
- Assumptions tab with auto-save + budget recalculation
- Expense table name bug fix

## Sprint 2 Goal
Transform the budget workbook from a data entry tool into a **trustworthy, guided, presentable** budget platform. Every number must be explainable, every change must be traceable, and the final output must be board-ready.

---

## PRIORITY 1: Subtotals, Sheet Totals & Budget Summary

### 1A. Add subtotals to every Budget Workbook tab
- Each sheet tab (Income, Payroll, Energy, Water & Sewer, Repairs & Supplies, Gen & Admin) needs:
  - Category subtotal rows (e.g., "Total Supplies", "Total Repairs", "Total Maintenance" on R&S tab)
  - Sheet total row at the bottom (bold, shaded background)
  - Subtotals should sum all numeric columns: Prior Year, YTD Actual, Accrual Adj, Unpaid Bills, YTD Budget, Sep-Dec Estimate, 12 Month Forecast, Current Budget, Proposed Budget, $ Variance, % Change
- Subtotal rows are NOT editable — they are always computed from child rows

### 1B. Add Budget Summary tab to FA Dashboard building detail page
- New tab in the Budget Workbook section called "Summary"
- Layout matches the Budget Summary sheet in the Excel template:
  - Row 8: Total Operating Income (sum of all Income sheet lines)
  - Row 11: Payroll & Related (sum of Payroll sheet)
  - Row 12: Energy (sum of Energy sheet)
  - Row 13: Water & Sewer (sum of Water & Sewer sheet)
  - Row 14: Repairs & Supplies (sum of R&S sheet)
  - Row 15: Professional Fees (Gen & Admin rows 8-16)
  - Row 16: Administrative & Other (Gen & Admin rows 20-49)
  - Row 17: Insurance (Gen & Admin rows 53-64)
  - Row 18: Taxes (Gen & Admin rows 68-78)
  - Row 19: Financial Expenses (Gen & Admin rows 82-90)
  - Row 20: Total Operating Expenses (sum of rows 11-19)
  - Row 22: Net Operating Income (Row 8 - Row 20)
- Columns: Prior Year Actual, Current Budget, Proposed Budget, $ Variance, % Change
- This is the view a CFO presents to a board

### 1C. Smarter forecasting
- The compute_forecast function currently hardcodes YTD_MONTHS=2 and REMAINING=10
- Make this dynamic: read the actual budget period from the Budget record (or from the Setup sheet data)
- The Sep-Dec Estimate column formula should be: `(YTD Actual + Accrual + Unpaid) / ytd_months * remaining_months`
- When YTD data seems anomalous (e.g., YTD already exceeds full prior year), flag it visually but still compute
- Add a "Forecast Method" indicator per line: "Annualized" vs "Prior Year Adjusted" vs "Manual Override"

---

## PRIORITY 2: Cell Indicators & Formula Visibility

### 2A. Cell-level data source indicators
- Color/style coding for cells in the Budget Workbook:
  - **Light blue background**: System-calculated (from Yardi YSL import)
  - **White background**: FA-entered (manually overridden)
  - **Light yellow background**: PM-submitted value
  - **Light green background**: Formula-calculated (Sep-Dec Estimate, 12 Month Forecast, Proposed Budget)
- Add a legend at the top of each sheet tab explaining the colors

### 2B. Formula visibility on demand
- When an FA hovers over or clicks a formula-calculated cell (green), show a tooltip with the formula:
  - Sep-Dec Estimate: "= (YTD Actual + Accrual + Unpaid) / 2 × 10 = ($7,799 + $0 + $0) / 2 × 10 = $38,995"
  - 12 Month Forecast: "= YTD Actual + Accrual + Unpaid + Sep-Dec Estimate = $46,794"
  - Proposed Budget: "= 12 Month Forecast × (1 + Increase%) = $46,794 × 1.00 = $46,794"
- When the FA manually overrides a formula cell, change it from green to white and log the override
- Add an "undo to formula" option that reverts to the calculated value

### 2C. Change history per cell
- The BudgetRevision table already exists — build a UI for it
- Clicking any cell shows a small popover: "Changed by Jacob Sirotkin on 3/25 at 11:42pm — was $70,612, now $75,000"
- Log every change: PM edits, FA overrides, assumption-driven recalculations, and system imports

---

## PRIORITY 3: FA Guided Workflow

### 3A. Budget completion checklist on building detail page
- Replace the simple "Draft → PM Review → FA Review → Approved" stepper with a detailed checklist:
  1. ☐ Data imported (Yardi YSL)
  2. ☐ Expense distribution loaded
  3. ☐ Audit data confirmed
  4. ☐ Assumptions set (energy, water, insurance, payroll)
  5. ☐ Income sheet reviewed
  6. ☐ Payroll sheet reviewed
  7. ☐ Energy sheet reviewed
  8. ☐ Water & Sewer reviewed
  9. ☐ R&S sheet reviewed (with PM input)
  10. ☐ Gen & Admin reviewed
  11. ☐ Budget Summary balanced
  12. ☐ Ready for presentation
- Auto-check items when the FA has viewed/edited each tab
- Each item links directly to the relevant section

### 3B. Assumptions-first workflow
- When an FA opens a building for the first time, prompt them to set assumptions BEFORE reviewing line items
- The Assumptions panel should be a sidebar or top section, not a hidden button
- Show "impact preview": when the FA changes energy rate increase from 3% to 5%, immediately show how many lines change and by how much total

---

## PRIORITY 4: PM Expense Drill-Down

### 4A. Invoice detail per GL code in PM review
- The expense_distribution data already exists (178 invoices, $1.4M for entity 204)
- Under each GL line in the PM review table, add an expandable row showing:
  - Invoice vendor, date, amount, description
  - Sorted by amount descending
  - Total of invoices vs. the YTD Actual amount (they should match or be close)
- This gives PMs the context to make informed projections

---

## PRIORITY 5: Presentation & Export

### 5A. Board presentation view
- New route: /presentation/<token> (using existing PresentationSession model)
- Clean, print-ready layout with NO workflow UI (no nav bar, no edit buttons, no status pills)
- Shows: Building name, budget year, Budget Summary table, key variance callouts
- Auto-generated narrative: "Total operating expenses are projected at $X, a Y% increase over current year, driven primarily by [top 3 variance categories]"

### 5B. Enhanced Excel export
- The existing "Download Excel" should produce a workbook matching Budget_Final_Template_v2.xlsx exactly
- Include all sheets: Setup, Budget Summary, Income, Payroll, Energy, Water & Sewer, R&S, Gen & Admin, Insurance Schedule, Yearly Comparison
- Formulas should be Excel formulas (not hardcoded values) where possible
- This is what gets sent to the board — it must be polished

### 5C. Current vs. Proposed comparison view
- Side-by-side view on the Budget Summary tab: Current Year Budget | Proposed Budget | $ Variance | % Change
- Highlight rows with >10% change in red/green
- This is the core of any budget presentation

---

## Technical Notes
- The compute_forecast function is in workflow.py around line 648
- Assumptions recalculation is around line 1105
- BudgetRevision model exists but has no UI (line 313)
- PresentationSession model exists but route is incomplete (line 342)
- The BUDGET_SUMMARY_MAPPING in template_populator.py (line 47) defines the roll-up structure — reuse this for the web Budget Summary tab
- Current hardcoded YTD_MONTHS=2 appears at lines 661, 1173

## Execution Order
Work through these in priority order (1→2→3→4→5). Within each priority, complete all sub-items before moving to the next priority. Test with entity 204 after each major feature. Commit frequently with descriptive messages.
