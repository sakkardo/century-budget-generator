# Century Budget — Changelog

> Dated sessions with commits. Newest first.

## April 5, 2026 — Palette Cleanup + Doc Split
**Design:** dropped purple from Payroll Assumptions section (12 hex codes swapped to brown ramp). Unifies visual language across FA, PM, Payroll, Summary, RE Taxes tabs.
**Docs:** split 501-line `BUDGET_PROCESS_April_1_2026.md` into `USER_GUIDE.md`, `ARCHITECTURE.md`, `CHANGELOG.md`.

## April 4, 2026 — Enhanced Payroll Tab Phase 1
Commit: `4794192`
- 4 collapsible sections: Assumptions / Roster / Taxes+Benefits / GL Detail
- Wage calculation engine matching 204 Excel (pre/post-increase split, OT, Vac/Sick/Hol)
- Auto-calc Payroll Taxes (FICA/SUI/FUI/MTA), Workers Comp, Union Benefits (32BJ)
- Tie-out bar comparing Roster Calculated Total vs GL Proposed Total
- New DB models: `PayrollPosition`, `PayrollAssumption`
- New endpoints: `/api/payroll/positions`, `/api/payroll/assumptions`
- GL Detail with expandable sub-category grouping (Wages 5105 / Taxes 5140-5145 / Benefits 5150-5160 / Other 5162+)
- Inline editable Notes + Inc % per GL

## April 3, 2026 — Collaborative Review + Variance Fix
Commits: `fc0477d`, `5e4fa11`, `503d11a`, `d7d0707`
- **Variance/% Change formula correction** — all 20+ locations changed from `Proposed - Prior` to `Curr Budget - 12 Mo Forecast` (matches 204 Excel).
- **FA Dashboard PM Review Panel** — 3 tabs: PM Notes / Invoice Reclasses / Budget Proposals. Accept/Reject/Comment with audit trail.
- **PM Dashboard My Changes Panel** — 2 tabs: My Notes / My Reclasses (read-only). FA responses shown inline.
- **PM YTD Reclass Adjustment Fix** — refactored `applyReclassAdjustments()` to named async fn; resets to `_db_ytd_actual` before reapplying (prevents double-count).
- **PM Tier 1-5 Cell Functionality** — full FA-parity: fx badges, formula bar, editable cells, subtotal formulas, tabular-nums alignment, cascade recalc.
- New BudgetLine fields: `fa_proposed_status`, `fa_proposed_note`, `fa_override_value`.
- New endpoint: `POST /api/budget-proposal/review`.

## April 2, 2026 — RE Taxes + Summary Formula Bars
- **RE Taxes tab redesign** — budget-workbook-style table (1st Half, 2nd Half, Gross, Exemptions, Net). Formula bar with real numeric math (`= 39979620 * 0.096324 / 2`). Excel formula parity for all tax calcs.
- **Summary tab formula bar** — click any fx cell to see GL-level breakdown (`= 601963 + 12769 + 232423`). Read-only.
- Both use shared `safeEvalFormula()` + fx/override badge conventions.

## April 1, 2026 — PM Portal Invoice Drill-Down + Reclass Modal
- Click "View invoices" on any GL row → expand invoice detail table
- Searchable reclass modal with all 576 GL codes grouped by 9 categories
- Accrual adjustments stored as negative values (fix in `expense_distribution.py`)
- CSS lesson: `table-layout:fixed` + `max-width:0` kills `<td>` padding — wrap in `<div>`.

---

*For the canonical step-by-step process, see USER_GUIDE.md.*
*For technical reference (DB models, API endpoints, formula engine), see ARCHITECTURE.md.*
