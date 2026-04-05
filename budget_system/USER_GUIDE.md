# Century Budget — User Guide

> For FA / PM day-to-day budget work. Last updated April 5, 2026.

## Table of Contents
1. [Quick Start — 4-Upload Flow](#quick-start)
2. [Alternative: Automated Yardi Script](#automated-script)
3. [Editing Budgets on FA Dashboard](#editing-fa)
4. [Formula Bar — Click, Type, Accept](#formula-bar)
5. [PM Portal Workflow](#pm-portal)
6. [Common Problems & FAQs](#faqs)

---

<a id="quick-start"></a>
## 1. Quick Start — 4-Upload Flow

4 Yardi reports feed the budget workbook for each entity. **Upload order matters — YSL must go first** (creates the budget and GL lines). The other 3 can come in any order.

**Step 1. YSL Annual Budget (first — creates the budget)**
- Go to `century-budget-generator-production.up.railway.app/generate`
- Scroll to **Manual Upload** section
- Click "Choose Files" → select the YSL .xlsx file for the entity
- Click "Upload & Process"
- Creates a fresh budget version with all GL lines from Yardi actuals. Entity code + building name are read from inside the file.

**Step 2. Expense Distribution**
- Same Manual Upload section
- Select the Expense Distribution .xlsx (must say "Expense Distribution" in Row 1)
- Click "Upload & Process"
- Stores all invoices and auto-calculates **accrual adjustments** — any invoice dated 12/31 of the prior year or earlier gets summed by GL into the `accrual_adj` field.

**Step 3. AP Aging**
- Same section. Detected by "payee code" + "current owed" headers.
- Populates the **Unpaid Bills** column (sums `current_owed` by GL from open AP).

**Step 4. Maintenance Proof**
- Same section. Detected by filename pattern (e.g., `Adhoc_AMP_204.xlsx`).
- Stores unit-level maintenance/common charge data.

**Step 5. Review on FA Dashboard**
Verify each column populated: GL codes (from YSL), Accrual Adjustment (from Expense Distribution), Unpaid Bills (from AP Aging), Maintenance Proof data.

---

<a id="automated-script"></a>
## 2. Alternative — Automated Yardi Script

Instead of manual upload for 3 of the 4 reports:
1. On `/generate`, select entities, click **"Generate Yardi Script"**
2. Open Yardi in Chrome, press F12, paste script, hit Enter
3. Script runs YSL + Maintenance Proof + AP Aging and auto-uploads to the server
4. Expense Distribution must still be uploaded manually (Yardi ViewState prevents automation)

---

<a id="editing-fa"></a>
## 3. Editing Budgets on FA Dashboard

Click any sheet tab: Income, Payroll, Energy, Repairs & Supplies, Gen & Admin, RE Taxes.

**Editable cells:** Prior Year, YTD Actual, Accrual Adj, Unpaid Bills, YTD Budget, Notes, Increase %

**Formula cells (green `fx` badge):** Estimate, Forecast, Proposed — click to view/edit via the formula bar.

**Recalculation cascade** — editing any cell triggers: Estimate → Forecast → Proposed → Variance → Category Subtotals → Sheet Total. Same behavior as Excel.

---

<a id="formula-bar"></a>
## 4. Formula Bar — Click, Type, Accept

- **Click any green `fx` cell** → formula bar at top shows the `=expression` with real numbers
- **Type a number** → Accept saves as override (cell shows orange `✎` pencil)
- **Type a formula** (e.g., `=5000*12`) → Accept evaluates and saves
- **Type "formula" or "auto"** → Accept reverts to auto-calculated
- **Cancel** → discards edit
- **Clear** → removes override, reverts to auto-calc

### Formulas (match 204 Excel template)
| Field | Formula |
|---|---|
| Estimate | `=IF((YTD+Accrual+Unpaid) >= PriorYear, (YTD+Accrual+Unpaid)/YTD_MONTHS*REMAINING_MONTHS, PriorYear-(YTD+Accrual+Unpaid))` |
| Forecast | `=YTD + Accrual + Unpaid + Estimate` |
| Proposed | `=Forecast * (1 + Increase%)` |
| Variance | `=Curr Budget - 12 Mo Forecast` |
| % Change | `=(Curr Budget - 12 Mo Forecast) / 12 Mo Forecast` |

---

<a id="pm-portal"></a>
## 5. PM Portal Workflow

PMs see their entities in a streamlined view with the same formula bar + cell edit experience as FA.

**Invoice drill-down:** Click "View invoices" on any GL row to expand invoice detail (Payee, Description, Invoice #, Date, Amount, Check #).

**Reclass a single invoice:** Click "Reclass to…" → modal with searchable list of 576 GL codes grouped by 9 categories. Search filters by GL code, description, or category.

**PM changes flow to FA review:** When a PM adds notes, reclasses invoices, or proposes budget changes, these appear in the FA dashboard's PM Review panel (3 tabs: Notes, Reclasses, Proposals). FA can Accept / Reject (with override) / Comment. All actions create audit entries.

---

<a id="faqs"></a>
## 6. Common Problems & FAQs

**Upload failed, "missing entity code":** The file's Row 1 or filename pattern isn't what the app expects. Check the entity code is inside the file (YSL, ExpDist) or in the filename (Maint Proof: `Adhoc_AMP_204.xlsx`).

**Accrual Adjustment column is empty:** Only invoices dated 12/31 of prior year or earlier count. Check the Expense Distribution file actually contains such invoices.

**Unpaid Bills column is empty:** AP Aging must contain open invoices (`current_owed > 0`). Paid-only AP won't populate it.

**"YSL must go first" — why?** YSL creates the budget rows. Without it, the other 3 reports have nothing to attach to.

**Formula bar won't show my number:** Make sure you clicked Accept. Enter = Accept, Escape = Cancel.

**Override won't clear:** Click Clear button in the formula bar (not Delete on keyboard).

**A PM's change isn't showing:** Refresh the FA dashboard. The PM Review panel polls on page load.

---

*See also: ARCHITECTURE.md for technical reference, CHANGELOG.md for dated changes.*
