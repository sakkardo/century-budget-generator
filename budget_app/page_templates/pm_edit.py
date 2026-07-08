# Extracted from workflow.py 2026-07-05 (clean-architecture tranche 1).
# BYTE-IDENTICAL constant — template edits happen HERE now. Keep the
# string style unchanged (raw vs non-raw matters for JS escapes; see
# the wizard-template-js-escapes memory / check_template_js gate).

PM_EDIT_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PM Edit — {{ building_name }} — Century Management</title>
<style>
/* Force scrollbars always visible (fixes macOS auto-hide on horizontal/vertical scroll) */
::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
::-webkit-scrollbar-track { background: #f1f5f9; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
::-webkit-scrollbar-corner { background: #f1f5f9; }
* { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
  :root {
    --blue: #5a4a3f;
    --blue-dark: #3d322a;
    --blue-light: #f5efe7;
    --green: #057a55;
    --green-light: #def7ec;
    --orange: #d97706;
    --orange-light: #fef3c7;
    --red: #e02424;
    --gray-50: #f4f1eb;
    --gray-100: #ede9e1;
    --gray-200: #e5e0d5;
    --gray-300: #d5cfc5;
    --gray-500: #8a7e72;
    --gray-700: #4a4039;
    --gray-900: #1a1714;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--gray-50);
    color: var(--gray-900);
    line-height: 1.5;
  }
  /* ── Global Nav ── */
  .top-nav { background: white; border-bottom: 1px solid var(--gray-200); padding: 0 20px; display: flex; align-items: center; height: 48px; position: sticky; top: 0; z-index: 100; }
  .top-nav .nav-brand { font-weight: 700; font-size: 15px; color: var(--blue); text-decoration: none; margin-right: 32px; }
  .top-nav .nav-links { display: flex; gap: 4px; }
  .top-nav .nav-link { padding: 6px 14px; font-size: 13px; font-weight: 500; color: var(--gray-500); text-decoration: none; border-radius: 6px; transition: all 0.15s; }
  .top-nav .nav-link:hover { background: var(--gray-100); color: var(--gray-900); }
  .top-nav .nav-link.active { background: var(--blue-light); color: var(--blue); }
  /* ── Toast ── */
  .toast-container { position: fixed; top: 60px; right: 20px; z-index: 9999; display: flex; flex-direction: column; gap: 8px; }
  .toast { padding: 12px 20px; border-radius: 8px; font-size: 14px; font-weight: 500; box-shadow: 0 4px 12px rgba(0,0,0,0.15); animation: slideIn 0.3s ease; max-width: 360px; }
  .toast-success { background: var(--green); color: white; }
  .toast-error { background: var(--red); color: white; }
  .toast-info { background: var(--blue); color: white; }
  @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }

  header {
    background: linear-gradient(135deg, var(--blue) 0%, var(--blue-dark) 100%);
    color: white;
    padding: 24px 20px;
  }
  header h1 { font-size: 24px; margin-bottom: 4px; }
  header p { opacity: 0.9; font-size: 14px; }
  .container { max-width: 1500px; margin: 0 auto; padding: 24px 20px; }

  .controls {
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: white;
    border-radius: 12px;
    padding: 16px 24px;
    margin-bottom: 20px;
    border: 1px solid var(--gray-200);
    flex-wrap: wrap;
    gap: 12px;
  }
  .status-pill {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
  }
  .status-pm_pending { background: var(--orange-light); color: var(--orange); }
  .status-pm_in_progress { background: var(--blue-light); color: var(--blue); }
  .status-returned { background: #fde8e8; color: var(--red); }
  .status-fa_review { background: var(--orange-light); color: var(--orange); }
  .status-approved { background: var(--green-light); color: var(--green); }
  .status-draft { background: var(--gray-100); color: var(--gray-500); }

  .fa-notes {
    background: #fef3c7;
    border: 1px solid #fbbf24;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 20px;
    font-size: 14px;
  }
  .fa-notes strong { color: var(--orange); }

  .save-indicator {
    font-size: 13px;
    color: var(--gray-500);
    padding: 6px 12px;
    border-radius: 999px;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    transition: all 0.2s ease;
    font-weight: 500;
  }
  .save-indicator.saving {
    color: var(--orange);
    background: #fff7ed;
    border: 1px solid #fed7aa;
  }
  .save-indicator.saving::before {
    content: '';
    width: 10px; height: 10px;
    border-radius: 50%;
    background: var(--orange);
    animation: save-pulse 1s ease-in-out infinite;
  }
  .save-indicator.saved {
    color: var(--green);
    background: #f0fdf4;
    border: 1px solid #bbf7d0;
  }
  .save-indicator.saved::before { content: '\2713'; font-weight: 700; }
  .save-indicator.failed {
    color: white;
    background: var(--red);
    border: 1px solid #991b1b;
    font-weight: 600;
    cursor: pointer;
    animation: save-fail-pulse 1.6s ease-in-out infinite;
  }
  .save-indicator.failed::before { content: '\26A0'; font-size: 14px; }
  @keyframes save-pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.4; transform: scale(0.7); }
  }
  @keyframes save-fail-pulse {
    0%   { box-shadow: 0 0 0 0 rgba(220, 38, 38, 0.7); }
    70%  { box-shadow: 0 0 0 12px rgba(220, 38, 38, 0); }
    100% { box-shadow: 0 0 0 0 rgba(220, 38, 38, 0); }
  }

  /* ── Formula Bar ── */
  .pm-formula-bar {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 12px;
    padding: 10px 20px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 12px;
    min-height: 44px;
    transition: all 0.2s;
  }
  .pm-formula-bar.active { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(90,74,63,0.08); }
  .pm-formula-bar .fb-label { font-size: 11px; font-weight: 700; color: var(--blue); text-transform: uppercase; white-space: nowrap; min-width: 60px; }
  .pm-formula-bar .fb-cell-ref { font-family: monospace; font-size: 13px; font-weight: 600; color: var(--gray-700); background: var(--gray-100); padding: 2px 8px; border-radius: 4px; min-width: 90px; text-align: center; }
  .pm-formula-bar .fb-formula { font-family: 'Courier New', monospace; font-size: 13px; color: var(--gray-700); flex: 1; padding: 4px 8px; background: var(--gray-50); border: 1px solid var(--gray-200); border-radius: 4px; }
  .pm-formula-bar .fb-badge { font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: 600; }
  .pm-formula-bar .fb-badge.auto { background: var(--green-light); color: var(--green); border: 1px solid var(--green); }

  /* ── Reclass Modal ── */
  .reclass-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.4); z-index: 1000; display: flex; align-items: center; justify-content: center; }
  .reclass-modal { background: white; border-radius: 12px; width: 560px; max-height: 80vh; display: flex; flex-direction: column; box-shadow: 0 20px 60px rgba(0,0,0,0.3); }
  .reclass-modal .rm-header { padding: 16px 20px; border-bottom: 1px solid var(--gray-200); display: flex; justify-content: space-between; align-items: center; }
  .reclass-modal .rm-header h3 { font-size: 15px; font-weight: 700; color: var(--blue); }
  .reclass-modal .rm-search { padding: 12px 20px; border-bottom: 1px solid var(--gray-200); }
  .reclass-modal .rm-search input { width: 100%; padding: 8px 12px; border: 1px solid var(--gray-300); border-radius: 6px; font-size: 13px; outline: none; }
  .reclass-modal .rm-search input:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(90,74,63,0.08); }
  .reclass-modal .rm-list { flex: 1; overflow-y: auto; max-height: 400px; }
  .reclass-modal .rm-cat-header { padding: 6px 20px; font-size: 11px; font-weight: 700; text-transform: uppercase; color: var(--blue); background: var(--blue-light); position: sticky; top: 0; }
  .reclass-modal .rm-gl-row { padding: 8px 20px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; font-size: 13px; border-bottom: 1px solid var(--gray-100); }
  .reclass-modal .rm-gl-row:hover { background: var(--blue-light); }
  .reclass-modal .rm-gl-row .gl-code { font-family: monospace; font-weight: 600; min-width: 90px; }
  .reclass-modal .rm-gl-row .gl-desc { flex: 1; color: var(--gray-700); }
  .reclass-modal .rm-footer { padding: 12px 20px; border-top: 1px solid var(--gray-200); display: flex; gap: 8px; justify-content: flex-end; }

  .grid-wrapper {
    background: white;
    border-radius: 12px;
    border: 1px solid var(--gray-200);
    overflow: visible;  /* FA dir 2026-05-18: was overflow:hidden which clipped sticky thead. */
    /* Expand the white card to match the table width so the row background
       extends cleanly to the right edge of the Notes column. min-width 100%
       fills the parent; max-content lets it grow if the table is wider. */
    width: max-content;
    min-width: 100%;
  }
  /* FA dir 2026-05-18 (scroll fix): one scroll context only — the window.
     CSS spec gotcha: overflow-x:auto + overflow-y:visible computes both as
     auto, which makes grid-container its own scroll context and breaks
     position:sticky on the thead. So BOTH must be visible. If the table is
     wider than the viewport on narrow screens, the page scrolls horizontally
     (annoying but acceptable — vertical scroll with sticky headers is the
     primary daily use). */
  .grid-container { overflow: visible; }
  .grid-container::-webkit-scrollbar { width:10px; height:12px; }
  .grid-container::-webkit-scrollbar-track { background:var(--gray-100); border-radius:6px; }
  .grid-container::-webkit-scrollbar-thumb { background:#8b7355; border-radius:6px; min-height:40px; }
  .grid-container::-webkit-scrollbar-thumb:hover { background:#6b5740; }
  .grid-container::-webkit-scrollbar-corner { background:var(--gray-100); }

  table { border-collapse: separate; border-spacing: 0; font-size: 13px; width: 100%; }
  /* FA dir 2026-05-18: thead sticks below the nav (48px) AND the formula bar
     (~46px). Both are sticky themselves, so column headers slot in below them. */
  .grid-container > table > thead { position: sticky; top: 94px; z-index: 20; }
  .grid-container > table > thead th { position: sticky; top: 94px; z-index: 22; background: #fafbfc; }
  /* Inner drill-down tables (invoice details) must NOT inherit sticky thead */
  .invoice-detail-row table thead,
  .invoice-detail-row table thead tr,
  .invoice-detail-row table thead th,
  .invoice-detail-row table thead td { position: static !important; top: auto !important; }
  th {
    padding: 8px 6px;
    text-align: left;
    font-weight: 600;
    border-bottom: 2px solid var(--gray-300);
    white-space: nowrap;
    background: var(--gray-100);
  }
  th.number { text-align: right; }
  td, th { white-space: nowrap; width: 1px; }
  td { padding: 6px 6px; border-bottom: 1px solid var(--gray-200); }
  td.number { text-align: right; font-variant-numeric: tabular-nums; }
  tbody tr:hover td { background: var(--blue-light); }
  tbody tr:hover td.frozen { background: #ede5d8; }
  /* Frozen columns */
  th.frozen, td.frozen { position: sticky; z-index: 15; background: white; }
  thead th.frozen { z-index: 25; background: var(--gray-100); }
  .frozen-gl { left: 0; min-width: 80px; }
  .frozen-desc { left: 80px; min-width: 200px; max-width: 200px; width: 200px; border-right: 2px solid var(--gray-300); box-shadow: 2px 0 8px rgba(90,74,63,0.08); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  thead th.frozen.frozen-desc { width: 200px; min-width: 200px; max-width: 200px; }
  .col-notes { color: var(--gray-500); font-size: 12px; min-width: 140px; text-align: center; }
  .col-notes input.note-warn { background: #fef3c7; border-color: #fbbf24; }
  .col-notes input.note-warn::placeholder { color: #92400e; font-weight: 500; }

  .category-header td {
    background: var(--blue-light);
    font-weight: 700;
    color: var(--blue);
    font-size: 14px;
    padding: 10px 10px;
    border-bottom: 2px solid var(--blue);
  }
  .category-header td.frozen { background: var(--blue-light); }
  .subtotal-row td {
    background: var(--gray-100);
    font-weight: 700;
    border-top: 2px solid var(--gray-300);
  }
  .subtotal-row td.frozen { background: var(--gray-100); }
  .grand-total td {
    background: #1e3a5f;
    color: white;
    font-weight: 700;
    font-size: 14px;
  }
  .grand-total td.frozen { background: #1e3a5f; color: white; }
  /* Reclass/invoice drill-down rows — clean frozen cell borders */
  tr.drill-row td.frozen { border-right: none; box-shadow: none; }

  input[type="number"], input[type="text"] {
    padding: 5px 8px;
    border: 1px solid var(--gray-300);
    border-radius: 4px;
    font-size: 13px;
    background: #fbfaf4;
  }
  input[type="number"] { text-align: right; min-width: 55px; width: auto; }
  input[type="text"] { min-width: 140px; width: 100%; }
  input.pm-cell, input.pm-cell-fx { width: auto; min-width: 55px; }
  input.pm-cell-pct { width: auto; min-width: 45px; }
  input:focus { outline: none; border-color: var(--blue); box-shadow: 0 0 0 2px var(--blue-light); }
  input:disabled { background: var(--gray-100); color: var(--gray-500); }

  .btn {
    background: var(--blue);
    color: white;
    border: none;
    padding: 10px 24px;
    border-radius: 6px;
    font-weight: 600;
    cursor: pointer;
    font-size: 14px;
  }
  .btn:hover { background: #1542b8; }
  .btn:disabled { background: var(--gray-300); cursor: not-allowed; }
  .btn-green { background: var(--green); }
  .btn-green:hover { background: #046c4e; }

  .invoice-detail-row > td { padding: 0 !important; }
  .invoice-detail-row:hover { background: transparent !important; }
  .invoice-detail-row .drill-sticky, .drill-sticky { position:sticky; left:220px; z-index:10; width:fit-content; min-width:850px; }

  /* PM Cell Styles */
  .pm-cell { min-width:50px; width:auto; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; text-align:right; background:#fbfaf4; cursor:text; font-variant-numeric:tabular-nums; }
  .pm-cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px var(--blue-light, #f5efe7); }
  input.pm-cell-fx { background:transparent; border:1px solid #e5e1d8; box-shadow:inset 3px 0 0 #16a34a; color:#15803d; }
  input.pm-cell-fx:focus { background:#ecfdf5; }
  .pm-fx { display:none !important; }
  .subtotal-row td.pm-fx-td { background:#e8f5e9; }
  .subtotal-row td.pm-fx-td .sub-val { color:#1b5e20; }
  .grand-total td.pm-fx-td { background:#1a3d2e; }
  .grand-total td.pm-fx-td .sub-val { color:#a5d6a7; }
  .pm-cell-pct { min-width:45px; width:auto; }

  /* FA directive 2026-05-11: PM R&M review-gate styling. R&M rows must
     each have an explicit PM action (typed % or $, or "No change" click)
     before the budget can be submitted back to the FA. G&A and other
     sections do not get these classes. */
  .pm-row-rm-unreviewed > td.frozen-gl {
    box-shadow: inset 4px 0 0 var(--red, #ef4444);
  }
  .pm-row-rm-unreviewed > td { background-color: #fef5f5 !important; }
  .pm-row-rm-unreviewed > td.frozen { background-color: #fef5f5 !important; }
  .pm-row-rm-reviewed > td.frozen-gl {
    box-shadow: inset 4px 0 0 var(--green, #16a34a);
  }
  .pm-rm-state-badge {
    display: inline-block;
    font-size: 9px;
    font-weight: 700;
    padding: 1px 6px;
    border-radius: 999px;
    margin-left: 6px;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    vertical-align: middle;
  }
  .pm-rm-state-badge.unreviewed { background: #fee2e2; color: #b91c1c; }
  .pm-rm-state-badge.reviewed { background: #dcfce7; color: #15803d; }
  .pm-rm-state-badge.no-change { background: #e0e7ff; color: #4338ca; }

  .pm-no-change-btn {
    display: inline-block;
    margin-top: 3px;
    padding: 2px 8px;
    font-size: 10px;
    font-weight: 600;
    border: 1px solid var(--gray-300);
    border-radius: 4px;
    background: white;
    color: var(--gray-600);
    cursor: pointer;
    white-space: nowrap;
  }
  .pm-no-change-btn:hover {
    background: #f0fdf4;
    border-color: var(--green, #16a34a);
    color: #15803d;
  }

  /* FA dir 2026-05-18: single-entry PM model. Pills replace the editable
     Increase % / $ inputs. Proposed cell becomes the one editable input. */
  .pm-pill {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 11px;
    font-weight: 600;
    font-family: ui-monospace, Menlo, monospace;
    line-height: 1.4;
    white-space: nowrap;
  }
  .pm-pill.up   { background: #fee2e2; color: #b91c1c; }
  .pm-pill.down { background: #dcfce7; color: #15803d; }
  .pm-pill.flat { background: #f1f5f9; color: #64748b; }

  input.pm-cell-proposed {
    width: 90px;
    min-width: 80px;
    text-align: right;
    padding: 5px 6px;
    border: 2px solid #1d4ed8;
    border-radius: 5px;
    font: 600 13px ui-monospace, Menlo, monospace;
    background: #eff6ff;
    color: #0f172a;
    outline: none;
  }
  input.pm-cell-proposed:focus {
    background: #ffffff;
    box-shadow: 0 0 0 3px rgba(29,78,216,.18);
  }
  input.pm-cell-proposed::placeholder {
    color: #94a3b8;
    font-weight: 400;
    font-style: italic;
  }
  input.pm-cell-proposed:disabled {
    background: #f9fafb;
    border-color: #e5e7eb;
    color: #64748b;
    font-weight: 400;
  }

  /* FA dir 2026-05-18 (visual cleanup v3): strip chrome from reference cells
     on the PM portal. Reference data (Prior/YTD/Accrual/Unpaid/Curr Budget,
     Estimate/Forecast read-only previews, $Variance/%Change diagnostics)
     becomes plain dim monospace text instead of looking like editable inputs.
     Scoped to body.pm-portal so the FA dashboard (different template) is
     untouched. */
  body.pm-portal input.pm-cell:disabled,
  body.pm-portal input.pm-cell-fx[readonly],
  body.pm-portal input.pm-cell-fx {
    background: transparent !important;
    border: 0 !important;
    box-shadow: none !important;
    padding: 4px 2px !important;
    color: #64748b !important;
    font-family: ui-monospace, Menlo, monospace !important;
    font-size: 12px !important;
    cursor: default !important;
  }
  /* Current Budget gets slightly stronger weight as the comparison anchor. */
  body.pm-portal input#pm_bud_,
  body.pm-portal input[data-field="current_budget"]:disabled {
    color: var(--gray-700, #1f2937) !important;
    font-weight: 600 !important;
  }
  /* Diagnostic cells: dimmed text with sign-only color (no boxed look). */
  body.pm-portal input[data-field="variance"],
  body.pm-portal input[data-field="pct_change"] {
    background: transparent !important;
    border: 0 !important;
    box-shadow: none !important;
    padding: 4px 2px !important;
    font-family: ui-monospace, Menlo, monospace !important;
    font-size: 12px !important;
  }
  /* fx span sticker already hidden globally via .pm-fx{display:none}; nothing more needed. */
  /* Subtle column-zone separators: identity | reference | decision | diagnostic | notes.
     Six dividers tell the eye where each "zone" ends without needing extra chrome. */
  body.pm-portal #linesTable td:nth-child(2),      /* after Description */
  body.pm-portal #linesTable th:nth-child(2),
  body.pm-portal #linesTable td:nth-child(8),      /* after Forecast */
  body.pm-portal #linesTable th:nth-child(8),
  body.pm-portal #linesTable td:nth-child(9),      /* after Curr Budget */
  body.pm-portal #linesTable th:nth-child(9),
  body.pm-portal #linesTable td:nth-child(12),     /* after Increase % */
  body.pm-portal #linesTable th:nth-child(12),
  body.pm-portal #linesTable td:nth-child(14),     /* after % Change (before Notes) */
  body.pm-portal #linesTable th:nth-child(14) {
    border-right: 1px solid var(--gray-200, #e5e7eb);
  }
  /* Drop bright green background on Increase $ td that came from either-or
     model (kept as inline style on .pm-cell-dollar's TD historically). */
  body.pm-portal #linesTable td:nth-child(11) {
    background: transparent !important;
  }
  /* Notes red-dot indicator: replaces yellow .note-warn border. Subtler. */
  body.pm-portal td.col-notes { position: relative; }
  body.pm-portal td.col-notes input.note-warn {
    background: transparent !important;
    border-color: var(--gray-300, #d1d5db) !important;
    padding-left: 18px !important;
  }
  body.pm-portal td.col-notes input.note-warn::before {
    content: "";
  }
  body.pm-portal td.col-notes::before {
    content: "";
    display: none;
    position: absolute;
    left: 6px;
    top: 50%;
    transform: translateY(-50%);
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--red, #dc2626);
    z-index: 2;
    pointer-events: none;
  }
  body.pm-portal td.col-notes.needs-note::before { display: block; }
  body.pm-portal td.col-notes.needs-note input { padding-left: 18px !important; }
  /* Inline "= No change" icon button (positioned right of Proposed input,
     same height — no row-jitter when only some rows show the button). */
  body.pm-portal .pm-no-change-inline {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 28px;
    margin-left: 4px;
    border: 1px solid var(--gray-300, #d1d5db);
    border-radius: 5px;
    background: white;
    color: var(--gray-600, #64748b);
    cursor: pointer;
    font-size: 14px;
    font-weight: 600;
    vertical-align: middle;
    line-height: 1;
  }
  body.pm-portal .pm-no-change-inline:hover {
    background: #dcfce7;
    color: #15803d;
    border-color: #16a34a;
  }

  /* FA dir 2026-05-18: when pm-v2 is also active (opt-in "action zone" overlay),
     neutralize its chrome that conflicts with single-entry visuals. pm-v2 was
     designed for the either-or model where Inc% and Inc$ were action inputs;
     under single-entry those are derived pills, so the heavy blue header bands,
     gold side rails, and tan backgrounds make the page busier, not cleaner. */
  body.pm-portal.pm-v2 #linesTable tbody td:nth-child(5),
  body.pm-portal.pm-v2 #linesTable tbody td:nth-child(6) {
    background: transparent !important;
  }
  body.pm-portal.pm-v2 #linesTable thead th:nth-child(10),
  body.pm-portal.pm-v2 #linesTable thead th:nth-child(11) {
    background: #fafbfc !important;
    color: var(--gray-600, #64748b) !important;
    border-bottom: 1px solid var(--gray-200, #e5e7eb) !important;
    border-left: 0 !important;
    border-right: 0 !important;
    font-weight: 600 !important;
    font-size: 11px !important;
    letter-spacing: 0.04em !important;
    text-transform: uppercase;
  }
  body.pm-portal.pm-v2 #linesTable tbody td:nth-child(10),
  body.pm-portal.pm-v2 #linesTable tbody td:nth-child(11) {
    background: transparent !important;
    border-left: 0 !important;
    border-right: 0 !important;
  }
  /* Keep pm-v2's Current Budget anchor styling (col 9 tan emphasis) — that one
     plays nicely with single-entry's "compare against this" mental model. */

  .pm-rm-progress-strip {
    background: white;
    border-radius: 10px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    margin-bottom: 14px;
    padding: 12px 16px;
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
  }
  .pm-rm-progress-strip.complete {
    background: linear-gradient(to right, #f0fdf4, white 50%);
    border-left: 4px solid var(--green, #16a34a);
  }
  .pm-rm-progress-strip.incomplete {
    background: linear-gradient(to right, #fef5f5, white 50%);
    border-left: 4px solid var(--red, #ef4444);
  }
  .pm-rm-progress-title {
    font-weight: 700;
    font-size: 14px;
    color: #111827;
  }
  .pm-rm-progress-counter {
    font-size: 12px;
    font-weight: 600;
    padding: 3px 10px;
    border-radius: 999px;
    background: #fee2e2;
    color: #b91c1c;
    font-variant-numeric: tabular-nums;
  }
  .pm-rm-progress-counter.complete { background: #dcfce7; color: #15803d; }
  .pm-rm-progress-bar {
    flex: 1;
    height: 8px;
    min-width: 120px;
    background: #e5e7eb;
    border-radius: 999px;
    overflow: hidden;
  }
  .pm-rm-progress-fill {
    height: 100%;
    background: linear-gradient(to right, #ef4444, #f59e0b);
    border-radius: 999px;
    transition: width 0.3s;
  }
  .pm-rm-progress-fill.complete { background: var(--green, #16a34a); }
  .pm-rm-progress-actions {
    display: flex;
    gap: 6px;
  }
  .pm-rm-action-btn {
    padding: 5px 12px;
    font-size: 11px;
    font-weight: 600;
    border: 1px solid var(--gray-300);
    border-radius: 5px;
    background: white;
    color: var(--gray-700);
    cursor: pointer;
  }
  .pm-rm-action-btn:hover { background: var(--gray-50); }
  .pm-rm-action-btn.primary {
    background: var(--red, #ef4444);
    color: white;
    border-color: var(--red, #ef4444);
  }
  .pm-rm-action-btn.primary:hover { background: #b91c1c; }

  /* FA directive 2026-05-11 (Option A): live-mirror styling for the
     sibling cell when one of the increase fields is entered. The mirror
     is a passive readout (italic + dimmed) so the PM sees the equivalent
     in the other unit without losing the either-or model — only one of
     the two is stored as the real entry. !important needed to override
     the $ column's inline green styling when in mirror state. */
  input.pm-cell.pm-cell-mirror {
    color: var(--gray-400, #9ca3af) !important;
    font-style: italic !important;
    background: transparent !important;
    border-color: var(--gray-200, #e5e7eb) !important;
    box-shadow: none !important;
  }
  input.pm-cell.pm-cell-mirror:focus {
    color: var(--gray-700, #374151) !important;
    font-style: normal !important;
  }

  .submit-btn-blocked {
    background: var(--gray-300) !important;
    color: var(--gray-500) !important;
    cursor: not-allowed !important;
    position: relative;
  }
  .submit-btn-blocked::after {
    content: attr(data-blocker);
    position: absolute;
    bottom: calc(100% + 8px);
    right: 0;
    background: #1f2937;
    color: white;
    padding: 6px 10px;
    border-radius: 6px;
    font-size: 11px;
    font-weight: 500;
    white-space: nowrap;
    opacity: 0;
    pointer-events: none;
    transition: opacity 0.15s;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
  }
  .submit-btn-blocked:hover::after { opacity: 1; }
</style>
</head>
<body class="pm-portal">

<!-- Global Nav -->
<nav class="top-nav">
  <a href="/" class="nav-brand">Century Management</a>
  <div class="nav-links">
    <a href="/" class="nav-link">Home</a>
    <a href="/dashboard" class="nav-link">FA Dashboard</a>
    <a href="/pm" class="nav-link active">PM Portal</a>
    <a href="/audited-financials" class="nav-link">Audited Financials</a>
  </div>
</nav>

<!-- Toast container -->
<div class="toast-container" id="toastContainer"></div>

<header>
  <h1>{{ building_name }}</h1>
  <p>Entity {{ entity_code }} — Repairs & Supplies Budget Review</p>
</header>
<div class="container">
  {% if fa_notes %}
  <div class="fa-notes">
    <strong>FA Notes:</strong> {{ fa_notes }}
  </div>
  {% endif %}

  <div class="controls">
    <div>
      Status: <span class="status-pill status-{{ status }}">{{ status | replace('_', ' ') }}</span>
      <span id="saveIndicator" class="save-indicator"></span>
    </div>
    <div style="display:flex; gap:12px; align-items:center;">
      <button id="zeroToggle" onclick="toggleZeroRows()" class="btn" style="background:var(--gray-200); color:var(--gray-600); font-size:12px; padding:6px 14px; border:1px solid var(--gray-300); border-radius:6px; cursor:pointer;"></button>
      <a href="/pm/{{ entity_code }}/expenses" class="btn" style="background:var(--gray-500); text-decoration:none;">View Expense Report</a>
      <button class="btn btn-green" id="submitBtn" onclick="submitForReview()">Submit for FA Review</button>
    </div>
  </div>

  <!-- My Changes Summary Panel (read-only) -->
  <div id="pmMyChangesPanel" style="display:none; background:white; border-radius:12px; box-shadow:0 1px 3px rgba(0,0,0,0.1); border:1px solid var(--gray-200); margin-bottom:16px;">
    <div onclick="this.nextElementSibling.classList.toggle('pm-panel-hidden'); this.querySelector('.pm-chev').classList.toggle('pm-chev-closed');" style="display:flex; align-items:center; justify-content:space-between; padding:14px 20px; cursor:pointer; background:linear-gradient(135deg, var(--blue-light) 0%, #e8e0d4 100%); border-radius:12px 12px 0 0; border-bottom:1px solid var(--gray-200);">
      <h3 style="font-size:14px; font-weight:700; color:var(--blue); display:flex; align-items:center; gap:8px;">
        My Changes
        <span id="pmMyChangesBadge" style="background:var(--blue); color:white; font-size:11px; font-weight:700; padding:2px 10px; border-radius:10px;"></span>
      </h3>
      <span class="pm-chev" style="font-size:12px; color:var(--gray-500); transition:transform 0.2s;">▾</span>
    </div>
    <div class="pm-panel-body">
      <div id="pmMyChangesTabs" style="display:flex; border-bottom:1px solid var(--gray-200); background:var(--gray-50);">
        <div class="pm-mc-tab active" onclick="switchPmMcTab(this,'pmMyNotesContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--blue); cursor:pointer; border-bottom:2px solid var(--blue); background:white;">My Notes <span id="pmMyNotesCount" style="background:var(--blue-light); color:var(--blue); font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
        <div class="pm-mc-tab" onclick="switchPmMcTab(this,'pmMyReclassContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--gray-500); cursor:pointer; border-bottom:2px solid transparent;">My Reclasses <span id="pmMyReclassCount" style="background:#fef3c7; color:#92400e; font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
      </div>
      <!-- My Notes Tab -->
      <div id="pmMyNotesContent" style="padding:16px 20px;">
        <div id="pmMyNotesEmpty" style="text-align:center; padding:20px; color:var(--gray-500); font-size:13px; display:none;">You haven't added any notes yet.</div>
        <div id="pmMyNotesContainer"></div>
      </div>
      <!-- My Reclasses Tab -->
      <div id="pmMyReclassContent" style="padding:16px 20px; display:none;">
        <div id="pmMyReclassEmpty" style="text-align:center; padding:20px; color:var(--gray-500); font-size:13px; display:none;">No invoice reclasses yet.</div>
        <table id="pmMyReclassTable" style="width:100%; border-collapse:collapse; font-size:13px;">
          <thead><tr>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">From GL</th>
            <th style="font-size:11px; padding:6px 4px; border-bottom:1px solid var(--gray-200);"></th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">To GL</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Invoices</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Amount</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">My Note</th>
            <th style="text-align:center; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">FA Status</th>
          </tr></thead>
          <tbody id="pmMyReclassBody"></tbody>
        </table>
      </div>
      <div style="padding:10px 20px; background:var(--gray-50); border-top:1px solid var(--gray-200); border-radius:0 0 12px 12px; font-size:11px; color:var(--gray-500);">
        Read-only summary of your changes. FA will review and accept/reject in their dashboard.
      </div>
    </div>
  </div>
  <style>
    .pm-panel-hidden { display: none !important; }
    .pm-chev-closed { transform: rotate(-90deg); }
    .pm-sheet-tabs { display:flex; gap:4px; padding:0 0 12px 0; }
    .pm-sheet-tab { padding:8px 20px; font-size:13px; font-weight:600; border:1px solid var(--gray-300); border-radius:8px 8px 0 0; cursor:pointer; background:var(--gray-100); color:var(--gray-600); transition:all 0.15s; }
    .pm-sheet-tab:hover { background:var(--gray-200); }
    .pm-sheet-tab.active { background:white; color:var(--blue); border-bottom:2px solid var(--blue); box-shadow:0 -1px 3px rgba(0,0,0,0.06); }

    /* ────────────────────────────────────────────────────────────────────
       PM Portal v2 — visual-hierarchy redesign (2026-05-17).
       Activated by adding ?ui=v2 to the URL (or localStorage pm_ui=v2).
       Zero behavioral change. CSS-only overlay: when body.pm-v2 is absent,
       the original UI renders unchanged. Approved direction:
         - Locked context recedes
         - Current Budget gets visual weight as the comparison anchor
         - Increase % + Increase $ framed in brown as the action zone
         - "No change" button is a ghost until clicked (reserves green for "you entered a value")
         - $ Variance bold, % Change demoted
         - Existing inline styles on Increase $ get overridden with !important
       Column positions (15 total):
         1=GL  2=Desc  3=Prior  4=YTD  5=Accrual  6=Unpaid  7=Estimate(fx)
         8=Forecast(fx)  9=Current Budget [ANCHOR]
         10=Increase %  11=Increase $  [ACTION]
         12=Proposed(fx)  13=$ Var  14=% Δ  15=Notes
       ──────────────────────────────────────────────────────────────────── */

    /* Locked context — Prior, YTD, Accrual, Unpaid (cols 3–6) */
    body.pm-v2 #linesTable thead th:nth-child(3),
    body.pm-v2 #linesTable thead th:nth-child(4),
    body.pm-v2 #linesTable thead th:nth-child(5),
    body.pm-v2 #linesTable thead th:nth-child(6) {
      color: var(--gray-500);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      font-weight: 600;
    }
    body.pm-v2 #linesTable tbody td:nth-child(3),
    body.pm-v2 #linesTable tbody td:nth-child(4),
    body.pm-v2 #linesTable tbody td:nth-child(5),
    body.pm-v2 #linesTable tbody td:nth-child(6) {
      background: #F7F4ED;
    }
    body.pm-v2 #linesTable tbody td:nth-child(3) input.pm-cell[disabled],
    body.pm-v2 #linesTable tbody td:nth-child(4) input.pm-cell[disabled],
    body.pm-v2 #linesTable tbody td:nth-child(5) input.pm-cell[disabled],
    body.pm-v2 #linesTable tbody td:nth-child(6) input.pm-cell[disabled] {
      color: var(--gray-500);
      font-weight: 400;
      background: transparent;
    }

    /* Current Budget — the anchor (col 9). Still locked, but visually upgraded
       to bridge between locked context and the action zone. */
    body.pm-v2 #linesTable thead th:nth-child(9) {
      background: #EEE7D5 !important;
      color: var(--blue) !important;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      border-right: 1px solid #c9b89a;
    }
    body.pm-v2 #linesTable tbody td:nth-child(9) {
      background: #EEE7D5;
      border-right: 1px solid #c9b89a;
    }
    body.pm-v2 #linesTable tbody td:nth-child(9) input.pm-cell[disabled] {
      color: var(--gray-900);
      font-weight: 700;
      background: transparent;
    }

    /* Action zone — Increase % + Increase $ (cols 10–11). Brown header band,
       gold side rails, white cell background. Filled inputs glow green.
       !important overrides the inline mint green styles on the Increase $ column. */
    body.pm-v2 #linesTable thead th:nth-child(10),
    body.pm-v2 #linesTable thead th:nth-child(11) {
      background: var(--blue) !important;
      color: white !important;
      font-weight: 700;
      text-transform: uppercase;
      font-size: 10.5px;
      letter-spacing: 0.06em;
      border-bottom: 2px solid var(--blue);
    }
    body.pm-v2 #linesTable thead th:nth-child(10) { border-left: 2px solid var(--blue); }
    body.pm-v2 #linesTable thead th:nth-child(11) { border-right: 2px solid var(--blue); }
    body.pm-v2 #linesTable tbody td:nth-child(10) {
      background: white !important;
      border-left: 1px solid #c9b89a;
    }
    body.pm-v2 #linesTable tbody td:nth-child(11) {
      background: white !important;     /* override inline #f0fdf4 */
      border-right: 1px solid #c9b89a;
    }
    body.pm-v2 #linesTable tbody td:nth-child(10) input.pm-cell-pct,
    body.pm-v2 #linesTable tbody td:nth-child(11) input.pm-cell-dollar {
      background: white !important;     /* override inline mint #d1fae5 */
      border: 1px solid var(--gray-300) !important;
      color: var(--gray-900);
      font-weight: 500;
    }
    body.pm-v2 #linesTable tbody td:nth-child(10) input.pm-cell-pct:focus,
    body.pm-v2 #linesTable tbody td:nth-child(11) input.pm-cell-dollar:focus {
      border-color: var(--blue) !important;
      box-shadow: 0 0 0 2px rgba(90,74,63,0.18);
    }
    /* Mirror cells stay italic-dimmed (existing pm-cell-mirror styling untouched) */

    /* Filled state: when input has a non-mirror value, glow green (success).
       Detection via aria-style attribute or class — fallback uses :not(:placeholder-shown). */
    body.pm-v2 #linesTable tbody td:nth-child(10) input.pm-cell-pct:not(.pm-cell-mirror):not([value=""]):not([value]),
    body.pm-v2 #linesTable tbody td:nth-child(11) input.pm-cell-dollar:not(.pm-cell-mirror):not([value=""]):not([value]) {
      /* CSS attribute selectors can't reliably detect "value has content" so we
         rely on inline JS to add .pm-v2-filled when a value is committed. */
    }
    body.pm-v2 #linesTable tbody td input.pm-v2-filled {
      background: #ECFDF5 !important;
      border-color: #6EE7B7 !important;
      color: #065F46 !important;
      font-weight: 600;
    }

    /* "No change" button — ghost until clicked. Existing class is .pm-no-change-btn. */
    body.pm-v2 .pm-no-change-btn {
      padding: 3px 8px !important;
      background: transparent !important;
      color: var(--gray-500) !important;
      border: 1px dashed var(--gray-300) !important;
      border-radius: 3px !important;
      font-size: 11px !important;
      font-weight: 500 !important;
      white-space: nowrap !important;
      box-shadow: none !important;
    }
    body.pm-v2 .pm-no-change-btn:hover {
      border-style: solid !important;
      border-color: var(--blue) !important;
      color: var(--blue) !important;
      background: var(--blue-light) !important;
    }
    /* When the row is stamped no_change, the badge already renders via .pm-rm-state-badge.no-change
       (line 24161) — keep that, but tighten its visual weight in v2. */
    body.pm-v2 .pm-rm-state-badge.no-change {
      background: #dbeafe !important;
      color: #1e40af !important;
      border: 1px solid #93c5fd !important;
    }

    /* $ Variance (col 13) — bold primary diagnostic. */
    body.pm-v2 #linesTable tbody td:nth-child(13) {
      font-weight: 700;
    }
    /* % Change (col 14) — demoted secondary, smaller + muted. */
    body.pm-v2 #linesTable tbody td:nth-child(14) {
      font-size: 11.5px;
      opacity: 0.75;
    }

    /* Default table header (non-action columns) — slightly tighter, muted text */
    body.pm-v2 #linesTable thead th {
      font-size: 11px;
    }

    /* Subtle improvement: real minus sign hint via tabular-nums for all numeric cells.
       Existing template already uses font-variant-numeric tabular-nums via .number,
       so this is a no-op but documented for completeness. */
  </style>

  <div class="pm-sheet-tabs">
    <div class="pm-sheet-tab active" data-sheet="Repairs &amp; Supplies" onclick="pmSwitchSheet('Repairs &amp; Supplies', this)">Repairs &amp; Supplies <span id="rsCount" style="background:var(--blue);color:white;font-size:10px;padding:1px 6px;border-radius:10px;margin-left:4px;"></span></div>
    <div class="pm-sheet-tab" data-sheet="Gen &amp; Admin" onclick="pmSwitchSheet('Gen &amp; Admin', this)">General &amp; Admin <span id="gaCount" style="background:var(--blue);color:white;font-size:10px;padding:1px 6px;border-radius:10px;margin-left:4px;"></span></div>
  </div>

  <!-- FA directive 2026-05-11: R&M review-gate progress strip. Visible only
       on the Repairs & Supplies tab; hidden on Gen & Admin. Shows X of Y
       reviewed, a progress bar, Jump-to-next, and the bulk "no change"
       sweep button. -->
  <div id="pmRmProgressStrip" class="pm-rm-progress-strip incomplete" style="display:none;">
    <div class="pm-rm-progress-title">Repairs &amp; Maintenance</div>
    <div id="pmRmProgressCounter" class="pm-rm-progress-counter">0 of 0 reviewed</div>
    <div class="pm-rm-progress-bar">
      <div id="pmRmProgressFill" class="pm-rm-progress-fill" style="width:0%;"></div>
    </div>
    <div class="pm-rm-progress-actions">
      <button class="pm-rm-action-btn primary" id="pmRmJumpBtn" onclick="pmRmJumpToNext()">Jump to next unreviewed →</button>
      <button class="pm-rm-action-btn" id="pmRmBulkBtn" onclick="pmRmBulkNoChange()">Mark all unreviewed as 0%</button>
    </div>
  </div>

  <div class="grid-wrapper">
    <div class="grid-container">
      <div id="pmFormulaBarWrap" style="display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200); border-radius:8px; margin-bottom:0; position:sticky; top:48px; z-index:50; box-shadow:0 2px 4px rgba(0,0,0,0.04);">
        <span style="font-size:11px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>
        <span id="pmFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:var(--gray-600); white-space:nowrap; min-width:100px;"></span>
        <input id="pmFormulaBar" type="text" placeholder="Click a green formula cell to view its formula..." style="display:block; flex:1; padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:monospace; background:white;" oninput="pmFormulaBarPreview()" onkeydown="pmFormulaBarKeydown(event)">
        <span id="pmFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:var(--green); white-space:nowrap; min-width:80px; text-align:right;"></span>
        <button id="pmFormulaAccept" style="display:none; padding:4px 14px; font-size:12px; font-weight:600; background:var(--green); color:white; border:none; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarAccept()">Accept</button>
        <button id="pmFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:var(--gray-200); color:var(--gray-700); border:none; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarCancel()">Cancel</button>
        <button id="pmFormulaClear" style="display:none; padding:4px 10px; font-size:11px; background:#fef2f2; color:var(--red); border:1px solid #fecaca; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarClear()" title="Remove formula, revert to auto-calc">Clear</button>
        <button id="pmFormulaUndo" style="display:none; padding:4px 10px; font-size:11px; background:#fff7ed; color:#c2410c; border:1px solid #fed7aa; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarUndo()" title="Undo the last accepted formula change">↶ Undo</button>
        <!-- FA dir 2026-05-19: PM-side per-tab Undo + History (mirrors FA dashboard) -->
        <span style="display:inline-block; width:1px; height:22px; background:var(--gray-300); margin:0 4px;"></span>
        <button onclick="pmTabUndoLast()" title="Restore the most recent change on this tab" style="padding:4px 10px; font-size:11px; background:white; color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer; font-weight:600; white-space:nowrap;">↩ Undo last</button>
        <button onclick="pmTabShowHistory()" title="See the last 50 changes on this tab" style="padding:4px 10px; font-size:11px; background:white; color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer; font-weight:600; white-space:nowrap;">⏱ History</button>
      </div>
      <table id="linesTable">
        <thead>
          <tr>
            <th class="frozen frozen-gl">GL Code</th>
            <th class="frozen frozen-desc">Description</th>
            <th class="number">Prior Year<br>Actual</th>
            <th class="number">YTD<br>Actual</th>
            <th class="number">Accrual<br>Adj</th>
            <th class="number">Unpaid<br>Bills</th>
            <th class="number">{{ estimate_label }}<br>Estimate</th>
            <th class="number">12 Month<br>Forecast</th>
            <th class="number">Current<br>Budget</th>
            <th class="number" title="(Proposed − Current Budget) / Current Budget">% Inc vs<br>Curr Budget</th>
            <th class="number" title="Proposed − Current Budget">$ Inc vs<br>Curr Budget</th>
            <th class="number">2027 Proposed<br>Budget</th>
            <th class="number" title="(Proposed − 12-Mo Forecast) / 12-Mo Forecast">% Inc vs<br>12-Mo Forecast</th>
            <th class="number" title="Proposed − 12-Mo Forecast">$ Inc vs<br>12-Mo Forecast</th>
            <th class="col-notes">Notes</th>
          </tr>
        </thead>
        <tbody id="linesBody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
const ENTITY = "{{ entity_code }}";
const CAN_EDIT = {{ can_edit }};
const BUDGET_STATUS = "{{ budget_status }}";
const LINES = {{ lines_json | safe }};
const ALL_GL_CODES = {{ all_gl_json | safe }};
const YTD_MONTHS = {{ ytd_months }};
const REMAINING_MONTHS = {{ remaining_months }};

// FA dir 2026-05-21: PM is allowed to edit a small, FA-curated subset of
// G&A GLs. All other G&A lines stay visible (so the PM sees the full
// picture) but the Proposed input is locked. Portfolio-wide allowlist.
// Adjusting this list is a code change — keeps it deliberate.
const PM_EDITABLE_GA_GLS = new Set([
  '6505-0000',  // Engineering & Architectural Fees
  '6515-0000',  // Legal Fees
  '6590-0000',  // Other Professional Fees
  '6720-0000',  // (pending FA label confirmation)
  '6726-0000',  // (pending FA label confirmation)
  '6740-0000',  // (pending FA label confirmation)
]);
function pmGaLineLocked(line) {
  // Returns true if this line is on the G&A sheet AND its GL is NOT in
  // the editable allowlist. Used by the row renderer to disable the
  // Proposed input + show a lock icon.
  if (!line || (line.sheet_name || '') !== 'Gen & Admin') return false;
  return !PM_EDITABLE_GA_GLS.has(line.gl_code || '');
}

// FA dir 2026-05-17: GL-prefix → Gen & Admin sub-category fallback (mirrors
// budget_system/GL_Mapping.csv Sub-Category column). Used by PM_SHEET_CATEGORIES
// below when row_num=0 so YSL-imported GLs land in the right sub-section
// (e.g. 6145 → Insurance, 6315-001x → Taxes).
// FA dir 2026-05-19: 6315 (Real Estate Tax + credits) is now FA-only on the
// RE Taxes tab. Return null so PM portal G&A categorization skips them.
function _gaSubForGl(gl) {
  const p4 = (gl || '').slice(0, 4);
  if (p4 === '6315') return null;
  if (['6105','6110','6115','6120','6125','6126','6130','6135','6140','6145','6150','6195'].indexOf(p4) >= 0) return 'insurance';
  if (['6310','6320','6325','6330','6335','6395'].indexOf(p4) >= 0) return 'taxes';
  if (['6505','6510','6515','6520','6525','6535','6555','6585','6590'].indexOf(p4) >= 0) return 'prof_fees';
  if (p4.startsWith('69')) return 'financial';
  if (p4 >= '6700' && p4 <= '6799') return 'admin_other';
  return 'admin_other';
}

// PM Portal v2 — visual-hierarchy redesign (2026-05-17).
// Opt-in via ?ui=v2 (one-shot) or localStorage pm_ui=v2 (persistent).
// Adding ?ui=v2 to the URL also stickies the choice in localStorage so
// subsequent navigations keep v2. ?ui=v1 unsticks. Removes guesswork
// when sharing URLs to other PMs/FAs. Zero behavioral change.
(function _pmV2Init() {
  try {
    const q = new URLSearchParams(location.search).get('ui');
    if (q === 'v2') localStorage.setItem('pm_ui', 'v2');
    if (q === 'v1') localStorage.removeItem('pm_ui');
    const v2 = (q === 'v2') || (localStorage.getItem('pm_ui') === 'v2');
    if (v2) document.body.classList.add('pm-v2');
  } catch (_e) {}
})();

// Sheet tab config
let _pmActiveSheet = 'Repairs & Supplies';
const PM_SHEET_CATEGORIES = {
  'Repairs & Supplies': {
    cats: {supplies: [], repairs: [], maintenance: []},
    labels: {supplies: 'Supplies', repairs: 'Repairs', maintenance: 'Maintenance Contracts'},
    match: function(l) { return l.sheet_name === 'Repairs & Supplies'; },
    assign: function(l) { return l.category; },
    grandLabel: 'GRAND TOTAL R&M'
  },
  'Gen & Admin': {
    cats: {prof_fees: [], admin_other: [], insurance: [], taxes: [], financial: []},
    labels: {prof_fees: 'Professional Fees', admin_other: 'Administrative & Other', insurance: 'Insurance', taxes: 'Taxes', financial: 'Financial Expenses'},
    // FA dir 2026-05-19: 6315 (Real Estate Tax + credits) moved to FA-only
    // RE Taxes tab. Skip on PM portal G&A so PMs don't see RE tax lines.
    match: function(l) { return l.sheet_name === 'Gen & Admin' && !(l.gl_code || '').startsWith('6315'); },
    assign: function(l) {
      const r = l.row_num || 0;
      if (r >= 8 && r <= 16) return 'prof_fees';
      if (r >= 20 && r <= 49) return 'admin_other';
      if (r >= 53 && r <= 64) return 'insurance';
      if (r >= 68 && r <= 78) return 'taxes';
      if (r >= 82 && r <= 90) return 'financial';
      // FA dir 2026-05-17: row_num=0 → fall back to GL-prefix lookup so YSL-
      // imported GLs (not in approved-2026 template) land in the right bucket.
      const sub = _gaSubForGl(l.gl_code);
      if (sub === 'prof_fees')   return 'prof_fees';
      if (sub === 'insurance')   return 'insurance';
      if (sub === 'taxes')       return 'taxes';
      if (sub === 'financial')   return 'financial';
      return 'admin_other';   // ultimate fallback
    },
    grandLabel: 'GRAND TOTAL G&A'
  }
};

// Populate sub-tab count badges
(function() {
  const rs = LINES.filter(l => l.sheet_name === 'Repairs & Supplies').length;
  const ga = LINES.filter(l => l.sheet_name === 'Gen & Admin').length;
  const rsEl = document.getElementById('rsCount');
  const gaEl = document.getElementById('gaCount');
  if (rsEl && rs) rsEl.textContent = rs;
  if (gaEl && ga) gaEl.textContent = ga;
})();

function pmSwitchSheet(sheetName, tabEl) {
  _pmActiveSheet = sheetName;
  document.querySelectorAll('.pm-sheet-tab').forEach(t => t.classList.remove('active'));
  tabEl.classList.add('active');
  renderTable();
  updateZeroToggle();
  // FA directive 2026-05-11: progress strip hides on G&A tab, shows on R&M.
  if (typeof _pmRmUpdateProgress === 'function') _pmRmUpdateProgress();
  if (typeof _pmRmUpdateSubmitGate === 'function') _pmRmUpdateSubmitGate();
}

let saveTimer = null;
const indicator = document.getElementById('saveIndicator');

function showToast(msg, type='info') {
  const c = document.getElementById('toastContainer');
  if (!c) return;
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity 0.3s'; setTimeout(() => t.remove(), 300); }, 3000);
}

function fmt(n) {
    if (n == null || isNaN(n)) return '$0';
    return '$' + Math.round(n).toLocaleString();
}

/* ── Grid Viewport Fit (PM) — DISABLED 2026-05-18 ─────────────────────
   Was: dynamically resize .grid-container max-height on every window scroll
   so its inner scrollbar matched the viewport. Side-effect: nested scroll
   contexts (window + inner container) broke position:sticky on the thead,
   so column headers scrolled away as the PM moved down the row list.
   Fix: drop the inner scroll entirely and let the window be the single
   scroll context — sticky thead now anchors at top:48px below the nav.
   Function kept as a no-op so any cached call sites don't error. */
function pmFitGridToViewport() { /* intentional no-op */ }
// (removed: pmFitGridToViewport() call + resize/scroll listeners)

/* ── Column Auto-Sizer (PM) ───────────────────────────────────────── */
function autoSizeColumns(table) {
  if (!table) return;
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  ctx.font = '13px Arial';
  const cols = table.querySelectorAll('thead th');
  const colWidths = [];
  cols.forEach((th, ci) => {
    if (th.classList.contains('frozen')) { colWidths.push(null); return; }
    let maxPx = 0;
    table.querySelectorAll('tbody tr').forEach(tr => {
      const td = tr.children[ci];
      if (!td) return;
      const inp = td.querySelector('input');
      if (inp) {
        const w = Math.ceil(ctx.measureText(inp.value || '').width);
        if (w > maxPx) maxPx = w;
      } else {
        const span = td.querySelector('.sub-val') || td;
        const w = Math.ceil(ctx.measureText((span.textContent || '').trim()).width);
        if (w > maxPx) maxPx = w;
      }
    });
    colWidths.push(maxPx + 20);
  });
  table.querySelectorAll('tbody tr').forEach(tr => {
    cols.forEach((th, ci) => {
      if (!colWidths[ci]) return;
      const td = tr.children[ci];
      if (!td) return;
      const inp = td.querySelector('input');
      if (inp && !inp.classList.contains('cell-notes') && !inp.classList.contains('pm-cell-notes')) {
        inp.style.width = Math.max(colWidths[ci], 55) + 'px';
      }
    });
  });
}

function fmtAmt(n) {
    if (n == null || isNaN(n)) return '$0.00';
    const abs = Math.abs(n);
    const str = abs.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    return n < 0 ? '($' + str + ')' : '$' + str;
}

function pctFmt(n) {
    if (n == null || isNaN(n)) return '0.0%';
    return (n * 100).toFixed(1) + '%';
}

function parseDollar(s) {
    if (typeof s !== 'string') return parseFloat(s) || 0;
    const isNeg = /^\s*\(.*\)\s*$/.test(s);
    const val = parseFloat(s.replace(/[$,\s()]/g, '')) || 0;
    return isNeg ? -val : val;
}

function safeEvalFormula(expr) {
    let s = expr.trim();
    if (s.startsWith('=')) s = s.substring(1);
    s = s.replace(/([\d.]+)\s*%/g, '($1/100)');
    if (!/^[\d\s+\-*\/().]+$/.test(s)) return null;
    try {
        const result = new Function('return (' + s + ')')();
        if (typeof result !== 'number' || !isFinite(result)) return null;
        return result;
    } catch (e) { return null; }
}

function isFixedToBudgetLine(line) {
    const gl = (line && line.gl_code) || '';
    if (gl.indexOf('6315') === 0) return true;          // RE Tax (expense) — existing pin
    return !!(line && line.income_pinned);              // fully-collectible income (task #99)
}

// One-time annual fees — once YTD is posted the Mar-Dec estimate is zero.
// Kept in sync with Python ONE_TIME_FEE_GLS constant in workflow.py.
const PM_ONE_TIME_FEE_GLS = new Set(['6722-0000','6762-0000','6763-0000','6764-0000']);
function isOneTimeFeeBilled(line) {
    if (!line || !line.gl_code) return false;
    if (!PM_ONE_TIME_FEE_GLS.has(line.gl_code)) return false;
    const billed = (line.ytd_actual || 0) + (line.accrual_adj || 0) + (line.unpaid_bills || 0);
    return Math.abs(billed) > 0.01;
}

function computeEstimate(line) {
    if (line.estimate_override !== null && line.estimate_override !== undefined) return line.estimate_override;
    if (isFixedToBudgetLine(line)) {
        return (line.current_budget || 0) - (line.ytd_actual || 0);
    }
    if (isOneTimeFeeBilled(line)) return 0;
    // FA #18: Capital — no extrapolation
    if (line.sheet_name === 'Capital' || (line.category || '').toLowerCase() === 'capital') return 0;
    // 210 FA: RE-tax credit income (4105/4110/4115/4120/4125) — no May-Dec estimate.
    if (['4105','4110','4115','4120','4125'].indexOf((line.gl_code||'').slice(0,4)) >= 0) return 0;
    const ytd = line.ytd_actual || 0;
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    const base = ytd + accrual + unpaid;
    // FA #7 anomaly cap: don't extrapolate one-time refund/credit
    const prior = line.prior_year || 0;
    if (base < 0 && prior >= 0) return 0;
    // Formula: (YTD+Accrual+Unpaid) / YTD_MONTHS * REMAINING_MONTHS
    if (YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
    return 0;
}

function computeForecast(line) {
    if (line.forecast_override !== null && line.forecast_override !== undefined) return line.forecast_override;
    if (isFixedToBudgetLine(line)) {
        return line.current_budget || 0;
    }
    const ytd = line.ytd_actual || 0;
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    // FA directive 2026-06-10 (supersedes 2026-05-05 minus sign): Capital
    // forecast = YTD + accrual + unpaid.
    if (line.sheet_name === 'Capital' || (line.category || '').toLowerCase() === 'capital') {
        return ytd + accrual + unpaid;
    }
    return ytd + accrual + unpaid + computeEstimate(line);
}

function computeProposed(line) {
    // FA directive 2026-05-05: Capital lines have NO proposed budget. Always 0.
    if (line.sheet_name === 'Capital' || (line.category || '').toLowerCase() === 'capital') {
        return 0;
    }
    // FA 2026-06-17 (B1/B4): never-budgeted income (prepaid / dividend / messenger).
    if (line.no_budget) return 0;
    // FA directive 2026-05-18: PM portal moved to single-entry — proposed_budget
    // is the source of truth when set. Null/undefined means PM hasn't entered;
    // 0 means PM explicitly zeroed. Both render as "blank" in the PM input but
    // sum to 0 in subtotals.
    if (line.proposed_budget !== null && line.proposed_budget !== undefined && line.proposed_budget !== '') {
        return parseFloat(line.proposed_budget) || 0;
    }
    // Legacy fallback for old data that still has increase_pct / increase_dollar.
    const forecast = computeForecast(line);
    if (line.increase_dollar !== null && line.increase_dollar !== undefined && line.increase_dollar !== '') {
        return forecast + (parseFloat(line.increase_dollar) || 0);
    }
    return forecast * (1 + (line.increase_pct || 0));
}

// ── PM Cell Helper Functions ──────────────────────────────────────
let _pmCurrentCell = null;
let _pmEditMode = false;
let _pmOriginalFormula = '';
let _pmFormulaBarUndo = null;

// Editable $cell: formats value on blur, triggers cascade on change.
// FA directive 2026-05-11: handles the new `increase_dollar` field and
// enforces either-or with `increase_pct` — typing in one auto-clears the
// sibling both in memory (LINES array) and in the DOM (the other cell).
function pmCellBlur(el) {
    const gl = el.dataset.gl;
    const field = el.dataset.field;
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    // FA directive 2026-05-11 (Option A live mirror): refactored to a
    // single-exit pattern so _pmUpdateMirror always runs at the bottom,
    // even when the value didn't change. This matters because
    // pmCellFocus clears the mirror display when the PM focuses a
    // mirror cell — if they tab away without typing, we need to repaint
    // the mirror so it doesn't stay blank.
    let valueChanged = false;

    if (field === 'increase_pct') {
        const raw = (el.value || '').trim();
        if (raw === '' || raw === '—') {
            // Blank entry — clear %.
            el.dataset.raw = '';
            el.value = '';
            if (!(line.increase_pct === 0 || line.increase_pct === null)) {
                line.increase_pct = 0;
                valueChanged = true;
            }
        } else {
            const pctVal = parseFloat(raw) || 0;
            el.dataset.raw = pctVal.toFixed(1);
            el.value = pctVal.toFixed(1) + '%';
            const newPct = pctVal / 100;
            const pctChanged = line.increase_pct !== newPct;
            const dollarPresent = line.increase_dollar !== null && line.increase_dollar !== undefined;
            if (pctChanged || dollarPresent) {
                line.increase_pct = newPct;
                valueChanged = true;
                // Either-or: clear $ sibling. The DOM repaint of the
                // sibling happens in _pmUpdateMirror, not here.
                if (pctVal !== 0 && dollarPresent) {
                    line.increase_dollar = null;
                }
            }
        }
    } else if (field === 'increase_dollar') {
        const raw = (el.value || '').trim();
        if (raw === '' || raw === '—') {
            // Blank entry — clear $.
            el.dataset.raw = '';
            el.value = '';
            if (!(line.increase_dollar === null || line.increase_dollar === undefined)) {
                line.increase_dollar = null;
                valueChanged = true;
            }
        } else {
            const val = parseDollar(raw);
            el.dataset.raw = Math.round(val);
            el.value = fmtDollar(val);
            const wasSame = (line.increase_dollar !== null && line.increase_dollar !== undefined && Math.round(line.increase_dollar) === Math.round(val));
            const pctPresent = !!(line.increase_pct && line.increase_pct !== 0);
            if (!wasSame || pctPresent) {
                line.increase_dollar = val;
                valueChanged = true;
                // Either-or: clear % sibling. DOM repaint in _pmUpdateMirror.
                if (pctPresent) line.increase_pct = 0;
            }
        }
    } else {
        // Generic numeric field (accrual_adj, unpaid_bills, etc — all
        // currently locked in the PM grid but kept for forward-compat).
        const val = parseDollar(el.value);
        el.dataset.raw = Math.round(val);
        el.value = fmt(val);
        if (line[field] !== val) {
            line[field] = val;
            valueChanged = true;
        }
    }

    // Always repaint the mirror sibling for either-or fields, even when
    // the value didn't change (see comment at top of function).
    if (field === 'increase_pct' || field === 'increase_dollar') {
        _pmUpdateMirror(gl, line);
    }

    if (!valueChanged) return;

    // FA directive 2026-05-11: stamp PM review state on R&M lines when the
    // PM types in % or $. _pm_action is consumed by saveAll() → cleared on
    // success. line.pm_review_state is updated optimistically so the row
    // visually flips green without waiting for the server round-trip.
    if (line.sheet_name === 'Repairs & Supplies') {
        if (field === 'increase_pct') {
            line._pm_action = 'review_pct';
            line.pm_review_state = 'typed_pct';
        } else if (field === 'increase_dollar') {
            line._pm_action = 'review_dollar';
            line.pm_review_state = 'typed_dollar';
        }
        _pmRmUpdateRowState(gl, line);
        _pmRmUpdateProgress();
        _pmRmUpdateSubmitGate();
    }
    pmLineChanged(gl, field, null);
}

// FA directive 2026-05-11 (Option A): live-mirror helpers. When the PM
// enters a % the $ cell shows the equivalent dollar delta in dimmed
// italic ("≈ $1,000"), and vice versa. The mirror is a passive readout —
// the real stored value is still either-or, so the gate, save logic,
// and proposed-budget math all keep working unchanged. The PM sees both
// numbers without losing the "fill one OR the other" mental model.

function _pmUpdateMirror(gl, line) {
    if (!line) return;
    const pctEl = document.getElementById('pm_inc_' + gl);
    const dollarEl = document.getElementById('pm_incd_' + gl);
    if (!pctEl || !dollarEl) return;

    const fc = computeForecast(line);
    const pct = line.increase_pct || 0;
    const hasDollar = line.increase_dollar !== null && line.increase_dollar !== undefined && line.increase_dollar !== '';

    if (hasDollar) {
        // $ is the source of truth — show $ value, mirror % from forecast.
        dollarEl.value = fmtDollar(line.increase_dollar);
        dollarEl.dataset.raw = String(Math.round(line.increase_dollar));
        dollarEl.classList.remove('pm-cell-mirror');
        if (fc !== 0 && Math.abs(line.increase_dollar) > 0.01) {
            const mirrorPct = (line.increase_dollar / fc) * 100;
            pctEl.value = '≈ ' + mirrorPct.toFixed(1) + '%';
            pctEl.dataset.raw = '';
            pctEl.classList.add('pm-cell-mirror');
        } else {
            pctEl.value = '';
            pctEl.dataset.raw = '';
            pctEl.classList.remove('pm-cell-mirror');
        }
    } else if (pct !== 0) {
        // % is the source of truth — show % value, mirror $ delta.
        pctEl.value = (pct * 100).toFixed(1) + '%';
        pctEl.dataset.raw = (pct * 100).toFixed(1);
        pctEl.classList.remove('pm-cell-mirror');
        const delta = fc * pct;
        if (Math.abs(delta) > 0.01) {
            dollarEl.value = '≈ ' + fmtDollar(delta);
            dollarEl.dataset.raw = '';
            dollarEl.classList.add('pm-cell-mirror');
        } else {
            dollarEl.value = '';
            dollarEl.dataset.raw = '';
            dollarEl.classList.remove('pm-cell-mirror');
        }
    } else {
        // Both 0 / null. Mirror class off on both. Don't override the
        // value — pmRmNoChange / saveAll / initial render decide what
        // the field shows in this state (empty for unreviewed, "0.0%"
        // for no_change, etc).
        pctEl.classList.remove('pm-cell-mirror');
        dollarEl.classList.remove('pm-cell-mirror');
    }
    // v2 visual: glow green when the cell is the source-of-truth (has a
    // real, non-mirror value). No-op when body.pm-v2 is absent (the CSS
    // rules guard on body.pm-v2 anyway, but skipping the class write keeps
    // the DOM clean for legacy UI). 2026-05-17.
    try {
      const v2 = document.body && document.body.classList.contains('pm-v2');
      if (v2) {
        const _pctReal = pctEl.value && !pctEl.classList.contains('pm-cell-mirror');
        const _dollarReal = dollarEl.value && !dollarEl.classList.contains('pm-cell-mirror');
        pctEl.classList.toggle('pm-v2-filled', !!_pctReal);
        dollarEl.classList.toggle('pm-v2-filled', !!_dollarReal);
      }
    } catch (_e) {}
}

function pmCellFocus(el) {
    // Mirror cell: clear display so the PM can type their real value.
    // The mirror class is removed so the next blur treats the entry as
    // a fresh value, not a derived display.
    if (el.classList.contains('pm-cell-mirror')) {
        el.value = '';
        el.dataset.raw = '';
        el.classList.remove('pm-cell-mirror');
        return;
    }
    // Regular cell: existing behavior (strip the formatted % / $ for
    // raw numeric editing).
    el.value = el.dataset.raw || '';
}

// ── PM single-entry handlers (FA directive 2026-05-18) ────────────────
// The PM now types the 2027 Proposed Budget directly. Increase % and
// Increase $ become derived pills. These handlers parse the typed
// dollar amount, persist it to proposed_budget, and trigger a cascade
// that repaints the derived pills + subtotals + grand total.

function pmProposedFocus(el) {
    // Strip the formatted $ so the PM can edit the raw number.
    el.value = el.dataset.raw || '';
    el.select();
}

function pmProposedBlur(el) {
    const gl = el.dataset.gl;
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;
    const raw = (el.value || '').trim();
    let newVal;
    if (raw === '' || raw === '—') {
        // PM cleared the cell — treat as un-set.
        newVal = null;
        el.dataset.raw = '';
        el.value = '';
    } else {
        const parsed = parseDollar(raw);
        newVal = parsed;
        el.dataset.raw = String(Math.round(parsed));
        el.value = fmt(parsed);
    }
    const oldVal = line.proposed_budget;
    const changed = (oldVal === null || oldVal === undefined ? null : Math.round(oldVal))
                    !== (newVal === null ? null : Math.round(newVal));
    line.proposed_budget = newVal;
    // FA directive 2026-05-18: clear legacy fields so the source-of-truth
    // is unambiguous and saveAll doesn't carry stale data forward.
    if (newVal !== null) {
        line.increase_pct = 0;
        line.increase_dollar = null;
    }
    // Stamp R&M review state (single-entry equivalent of review_pct/dollar).
    if (line.sheet_name === 'Repairs & Supplies' && newVal !== null) {
        line._pm_action = 'review_proposed';
        line.pm_review_state = 'typed_proposed';
        _pmRmUpdateRowState(gl, line);
        _pmRmUpdateProgress();
        _pmRmUpdateSubmitGate();
    }
    if (changed) pmLineChanged(gl, 'proposed_budget', newVal);
}

// Update the derived Increase % and Increase $ pill displays for one row,
// based on the current proposed_budget vs current_budget. Called from
// pmLineChanged after a proposed-cell edit, and from pmRmNoChange.
function _pmUpdateDerivedPills(gl, line) {
    if (!line) return;
    const pctEl = document.getElementById('pm_inc_' + gl);
    const dollarEl = document.getElementById('pm_incd_' + gl);
    if (!pctEl && !dollarEl) return;
    const curr = parseFloat(line.current_budget || 0) || 0;
    const hasProposed = line.proposed_budget !== null && line.proposed_budget !== undefined && line.proposed_budget !== '';
    if (!hasProposed) {
        if (pctEl) { pctEl.textContent = '—'; pctEl.className = 'pm-pill flat'; }
        if (dollarEl) { dollarEl.textContent = '—'; dollarEl.className = 'pm-pill flat'; }
        return;
    }
    const proposed = parseFloat(line.proposed_budget) || 0;
    const delta = proposed - curr;
    const denom = Math.abs(curr);
    const pct = denom > 0.5 ? (delta / denom) * 100 : (delta === 0 ? 0 : null);
    const klass = Math.abs(delta) < 0.5 ? 'flat' : (delta > 0 ? 'up' : 'down');
    if (dollarEl) {
        const sign = delta > 0 ? '+' : (delta < 0 ? '-' : '');
        dollarEl.textContent = Math.abs(delta) < 0.5 ? '$0' : (sign + '$' + Math.abs(Math.round(delta)).toLocaleString());
        dollarEl.className = 'pm-pill ' + klass;
        dollarEl.title = '= ' + fmt(proposed) + ' − ' + fmt(curr);
    }
    if (pctEl) {
        if (pct === null) {
            pctEl.textContent = '—';
            pctEl.className = 'pm-pill flat';
            pctEl.title = 'Current budget is $0 — % delta undefined.';
        } else {
            const sign = pct > 0 ? '+' : '';
            pctEl.textContent = Math.abs(pct) < 0.05 ? '0.0%' : (sign + pct.toFixed(1) + '%');
            pctEl.className = 'pm-pill ' + klass;
            pctEl.title = '= (' + fmt(proposed) + ' − ' + fmt(curr) + ') / ' + fmt(denom);
        }
    }
}

// FA dir 2026-05-18: hide / show the inline "=" No-change button based on
// whether the PM has entered a proposed value. Once they take action
// (type a number OR click "="), the button disappears so it doesn't keep
// crowding the cell. If they clear the cell back to empty, the button
// comes back — they may want the shortcut again.
function _pmToggleNoChangeBtn(gl, line) {
    if (!line) return;
    const propEl = document.getElementById('pm_prop_' + gl);
    if (!propEl) return;
    const propTd = propEl.parentElement;
    if (!propTd) return;
    const hasProposed = line.proposed_budget !== null && line.proposed_budget !== undefined && line.proposed_budget !== '';
    const existing = propTd.querySelector('.pm-no-change-inline');
    // Button visibility tracks whether the cell has a value, NOT the review-state
    // machine. R&M's pm_review_state can stay 'typed_proposed' after a clear
    // (it's a server-saved audit signal). The PM expects the shortcut to come
    // back when they wipe the cell — they may want the "=" again.
    if (hasProposed) {
        if (existing) existing.remove();
        return;
    }
    if (!existing && (typeof CAN_EDIT === 'undefined' || CAN_EDIT)) {
        const btn = document.createElement('button');
        btn.className = 'pm-no-change-inline';
        btn.textContent = '=';
        btn.title = 'Set Proposed = Current Budget (' + fmt(line.current_budget || 0) + ')';
        btn.onclick = () => pmRmNoChange(gl);
        propTd.appendChild(btn);
    }
}

// ── PM R&M review-gate helpers (FA directive 2026-05-11) ────────────────
// Section-level gate forcing PMs to take an explicit action on every R&M
// line before submitting back to the FA. None of these functions touch
// G&A or any other sheet. _pm_action signals intent to the backend on
// the next saveAll; absent _pm_action means saveAll won't change
// pm_review_state for the line.

function _pmRmUpdateRowState(gl, line) {
    if (!line || line.sheet_name !== 'Repairs & Supplies') return;
    const tr = document.querySelector('#linesBody tr[data-gl="' + gl + '"]');
    if (!tr) return;
    tr.classList.remove('pm-row-rm-unreviewed', 'pm-row-rm-reviewed');
    tr.classList.add(line.pm_review_state ? 'pm-row-rm-reviewed' : 'pm-row-rm-unreviewed');
    // Show/hide the "No change" button inline with the % cell.
    const pctCell = document.getElementById('pm_inc_' + gl)?.parentElement;
    if (pctCell) {
        let btn = pctCell.querySelector('.pm-no-change-btn');
        if (line.pm_review_state) {
            if (btn) btn.remove();
        } else if (!btn && CAN_EDIT) {
            btn = document.createElement('button');
            btn.className = 'pm-no-change-btn';
            btn.textContent = 'No change';
            btn.title = 'Mark this line as no change for the 2027 budget';
            btn.onclick = () => pmRmNoChange(gl);
            pctCell.appendChild(btn);
        }
    }
}

function _pmRmCounts() {
    const rm = LINES.filter(l => l.sheet_name === 'Repairs & Supplies');
    const reviewed = rm.filter(l => !!l.pm_review_state).length;
    const unreviewed = rm.length - reviewed;
    const unreviewedDollars = rm
        .filter(l => !l.pm_review_state)
        .reduce((s, l) => s + (l.current_budget || 0), 0);
    return { total: rm.length, reviewed, unreviewed, unreviewedDollars };
}

function _pmRmUpdateProgress() {
    const strip = document.getElementById('pmRmProgressStrip');
    if (!strip) return;
    // Only show on R&M tab.
    if (_pmActiveSheet !== 'Repairs & Supplies') {
        strip.style.display = 'none';
        return;
    }
    const c = _pmRmCounts();
    if (c.total === 0) {
        strip.style.display = 'none';
        return;
    }
    strip.style.display = 'flex';
    const pct = Math.round((c.reviewed / c.total) * 100);
    const counter = document.getElementById('pmRmProgressCounter');
    const fill = document.getElementById('pmRmProgressFill');
    const jump = document.getElementById('pmRmJumpBtn');
    const bulk = document.getElementById('pmRmBulkBtn');
    const isComplete = c.unreviewed === 0;
    counter.textContent = c.reviewed + ' of ' + c.total + ' reviewed' + (isComplete ? ' ✓' : '');
    counter.classList.toggle('complete', isComplete);
    fill.style.width = pct + '%';
    fill.classList.toggle('complete', isComplete);
    strip.classList.toggle('complete', isComplete);
    strip.classList.toggle('incomplete', !isComplete);
    if (jump) jump.style.display = isComplete ? 'none' : '';
    if (bulk) bulk.style.display = isComplete ? 'none' : '';
}

function _pmRmUpdateSubmitGate() {
    const btn = document.getElementById('submitBtn');
    if (!btn) return;
    // If the budget is already in fa_review the PM is making tweaks — gate
    // doesn't apply (the save-only path runs regardless).
    if (BUDGET_STATUS === 'fa_review') {
        btn.classList.remove('submit-btn-blocked');
        btn.removeAttribute('data-blocker');
        return;
    }
    const c = _pmRmCounts();
    if (c.unreviewed > 0) {
        btn.classList.add('submit-btn-blocked');
        btn.setAttribute('data-blocker',
            c.unreviewed + ' R&M line' + (c.unreviewed === 1 ? '' : 's') +
            ' still need review');
    } else {
        btn.classList.remove('submit-btn-blocked');
        btn.removeAttribute('data-blocker');
    }
}

function pmRmJumpToNext() {
    const unreviewed = LINES.filter(l =>
        l.sheet_name === 'Repairs & Supplies' && !l.pm_review_state);
    if (!unreviewed.length) return;
    // Make sure we're on the R&M tab first.
    if (_pmActiveSheet !== 'Repairs & Supplies') {
        const tab = document.querySelector('.pm-sheet-tab[data-sheet="Repairs & Supplies"]');
        if (tab) pmSwitchSheet('Repairs & Supplies', tab);
    }
    // 2026-05-17 fix: when the next unreviewed line is a hidden zero-row
    // (display:none because _showZeroRows is false), scrollIntoView is a
    // silent no-op and the button appears broken. Auto-show zero rows so
    // the PM can see what they're reviewing. Also use instant scroll —
    // Chrome blocks programmatic smooth scroll without a strong user-gesture
    // chain, and the focus() call afterwards is sensitive to scroll timing.
    setTimeout(() => {
        const targetGl = unreviewed[0].gl_code;
        const el = document.getElementById('pm_inc_' + targetGl);
        if (!el) return;
        const tr = el.closest('tr');
        const isHidden = tr && (tr.style.display === 'none' || tr.classList.contains('zero-row') && !_showZeroRows);
        if (isHidden && typeof toggleZeroRows === 'function') {
            // Unhide and let the layout settle for one frame before scrolling.
            toggleZeroRows();
            setTimeout(() => _pmJumpScrollAndFocus(el), 40);
        } else {
            _pmJumpScrollAndFocus(el);
        }
    }, 60);
}

// Helper: scroll the page so the target input sits roughly in the
// vertical middle of the viewport, then focus it. Uses instant scroll
// (window.scrollTo) because smooth scroll is unreliable when the click
// chain includes a tab switch / row-unhide / closeDrawer handoff.
function _pmJumpScrollAndFocus(el) {
    if (!el) return;
    try {
        const rect = el.getBoundingClientRect();
        const targetY = Math.max(0, window.scrollY + rect.top - (window.innerHeight / 2));
        window.scrollTo(0, targetY);
    } catch (_e) {}
    try { el.focus({preventScroll: true}); } catch (_e) {
        try { el.focus(); } catch (_e2) {}
    }
}

async function pmRmNoChange(gl) {
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;
    if (!CAN_EDIT) return;
    const isRm = (line.sheet_name === 'Repairs & Supplies');
    // FA directive 2026-05-18: under single-entry, "No change" means
    // proposed_budget = current_budget (flat-line vs current). Legacy
    // increase_pct / increase_dollar are cleared. The R&M review-state
    // machine still stamps on `no_change` so the gate count still works.
    const oldState = line.pm_review_state;
    const oldProposed = line.proposed_budget;
    const flatProposed = parseFloat(line.current_budget || 0) || 0;
    line.proposed_budget = flatProposed;
    line.increase_pct = 0;
    line.increase_dollar = null;
    if (isRm) {
      line.pm_review_state = 'no_change';
      line._pm_action = 'no_change';
    } else {
      line._pm_action = 'no_change';
    }
    // Repaint cells optimistically.
    const propEl = document.getElementById('pm_prop_' + gl);
    if (propEl) {
        propEl.value = fmt(flatProposed);
        propEl.dataset.raw = String(Math.round(flatProposed));
    }
    pmLineChanged(gl, 'proposed_budget', flatProposed);
    if (isRm) {
      _pmRmUpdateRowState(gl, line);
      _pmRmUpdateProgress();
      _pmRmUpdateSubmitGate();
    }
    // Persist just this line so a refresh preserves the action.
    try {
        const resp = await fetch('/api/lines/' + ENTITY, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({lines: [{
                gl_code: gl, pm_action: 'no_change',
                proposed_budget: flatProposed,
                increase_pct: 0, increase_dollar: null,
            }]}),
        });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        line._pm_action = null;  // server consumed it
        showToast('Marked "No change"', 'success');
    } catch (e) {
        // Roll back optimistic update on failure.
        line.pm_review_state = oldState;
        line.proposed_budget = oldProposed;
        if (propEl) {
            const rb = (oldProposed !== null && oldProposed !== undefined) ? oldProposed : '';
            propEl.value = rb === '' ? '' : fmt(rb);
            propEl.dataset.raw = rb === '' ? '' : String(Math.round(rb));
        }
        if (isRm) {
            _pmRmUpdateRowState(gl, line);
            _pmRmUpdateProgress();
            _pmRmUpdateSubmitGate();
        }
        showToast('Save failed — try again', 'error');
    }
}

async function pmRmBulkNoChange() {
    if (!CAN_EDIT) return;
    const c = _pmRmCounts();
    if (c.unreviewed === 0) {
        showToast('All R&M lines already reviewed.', 'info');
        return;
    }
    const ok = confirm(
        'Mark ' + c.unreviewed + ' R&M line' +
        (c.unreviewed === 1 ? '' : 's') + ' as "No change" (0%)?\n\n' +
        'Total current budget for these lines: $' +
        Math.round(c.unreviewedDollars).toLocaleString() + '\n\n' +
        'They will count as reviewed. The FA can see in the audit trail ' +
        'that these were bulk-confirmed, not individually reviewed.'
    );
    if (!ok) return;
    try {
        const resp = await fetch('/api/pm/' + ENTITY + '/rm-bulk-no-change', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({confirm: true}),
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            showToast('Bulk action failed: ' + (err.error || 'HTTP ' + resp.status), 'error');
            return;
        }
        const body = await resp.json();
        // Update in-memory state for every R&M line now flagged bulk_no_change.
        LINES.forEach(l => {
            if (l.sheet_name === 'Repairs & Supplies' && !l.pm_review_state) {
                l.increase_pct = 0;
                l.increase_dollar = null;
                l.pm_review_state = 'bulk_no_change';
                l._pm_action = null;
            }
        });
        renderTable();
        _pmRmUpdateProgress();
        _pmRmUpdateSubmitGate();
        showToast('Marked ' + body.marked + ' R&M line(s) as no change.', 'success');
    } catch (e) {
        showToast('Bulk action failed — try again', 'error');
    }
}

// Format a dollar value like "$1,500" with comma + dollar sign.
// Negative values render as "-$500". Used by Increase $ column.
function fmtDollar(v) {
    if (v === null || v === undefined || v === '' || isNaN(v)) return '';
    const n = Math.round(v);
    if (n === 0) return '$0';
    const abs = Math.abs(n).toLocaleString();
    return (n < 0 ? '-$' : '$') + abs;
}

// Formula cell focus: opens formula bar for editing (if editable) or read-only display
function pmFxCellFocus(el) {
    const gl = el.dataset.gl;
    const field = el.dataset.field;
    const formula = el.dataset.formula || '';
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    // Clear undo if switching to a different cell
    if (_pmCurrentCell && _pmCurrentCell !== el && _pmFormulaBarUndo) {
        _pmFormulaBarUndo = null;
        const undoBtn = document.getElementById('pmFormulaUndo');
        if (undoBtn) undoBtn.style.display = 'none';
    }

    // Highlight cell
    if (_pmCurrentCell) _pmCurrentCell.style.outline = '';
    el.style.outline = '2px solid var(--blue)';
    el.style.outlineOffset = '-1px';
    _pmCurrentCell = el;

    const bar = document.getElementById('pmFormulaBar');
    const label = document.getElementById('pmFormulaLabel');
    const preview = document.getElementById('pmFormulaPreview');
    const accept = document.getElementById('pmFormulaAccept');
    const cancel = document.getElementById('pmFormulaCancel');
    const clear = document.getElementById('pmFormulaClear');

    label.textContent = gl + ' · ' + field.replace('_', ' ');
    label.style.display = '';

    // Check if field is editable (estimate, forecast, proposed) vs read-only (variance, pct_change)
    const isEditable = field === 'estimate' || field === 'forecast' || field === 'proposed' || field === 'prior_year' || field === 'ytd_actual' || field === 'accrual_adj' || field === 'unpaid_bills' || field === 'current_budget' || field === 'increase_pct';
    const isFormulaCell = field === 'estimate' || field === 'forecast' || field === 'proposed' || field === 'variance' || field === 'pct_change';

    if ((isFormulaCell && !isEditable) || !CAN_EDIT) {
        // Read-only: non-editable formula cells OR user lacks edit permission
        bar.value = formula;
        bar.disabled = true;
        bar.style.opacity = '0.6';
        bar.style.cursor = 'not-allowed';
        accept.style.display = 'none';
        cancel.style.display = 'none';
        clear.style.display = 'none';
        preview.style.display = 'none';
        _pmEditMode = false;
    } else {
        // Editable cells
        bar.value = el.dataset.proposedFormula || formula;
        bar.disabled = false;
        bar.style.opacity = '1';
        bar.style.cursor = 'text';
        accept.style.display = '';
        cancel.style.display = '';
        clear.style.display = el.dataset.proposedFormula ? '' : 'none';
        _pmEditMode = true;
        _pmOriginalFormula = bar.value;
    }
}

// Formula cell blur: remove outline
function pmFxCellBlur(el) {
    if (_pmCurrentCell === el) _pmCurrentCell.style.outline = '';
}

// Subtotal focus: show SUM formula (read-only)
// Excel-valid signed sum for PM formula bars: raw integers, ASCII +/-, single '='.
// Mirrors the Summary tab's sumExcelExpr (PM is a separate template/script, so it's
// defined locally). Folds each term's sign into the operator -> '=a-b', never '=a+-b'.
function pmXlExpr(nums) {
  const nz = nums.map(n => Math.round(Number(n) || 0)).filter(n => n !== 0);
  if (!nz.length) return '';
  let s = (nz[0] < 0 ? '-' + Math.abs(nz[0]) : '' + nz[0]);
  for (let i = 1; i < nz.length; i++) s += (nz[i] < 0 ? '-' + Math.abs(nz[i]) : '+' + nz[i]);
  return '=' + s;
}
function pmSubtotalFocus(td) {
    const col = td.dataset.col;
    const bar = document.getElementById('pmFormulaBar');
    const label = document.getElementById('pmFormulaLabel');
    const preview = document.getElementById('pmFormulaPreview');
    const accept = document.getElementById('pmFormulaAccept');
    const cancel = document.getElementById('pmFormulaCancel');
    const clear = document.getElementById('pmFormulaClear');

    // Which lines feed this subtotal? Category subtotal id = pm_subtotal_<col>_<cat>;
    // grand total id = pm_grandtotal_<col>. Rebuild the real numeric breakdown
    // (was a hard-coded "=SUM(...)" text placeholder that showed no numbers).
    const id = td.id || '';
    let glList = [], scope = 'Subtotal';
    if (id.indexOf('pm_grandtotal_') === 0) {
        glList = (window._pmAllGLs || []).slice();
        scope = 'Grand Total';
    } else {
        const m = id.match(/^pm_subtotal_[a-z]+_(.+)$/);
        const cat = m ? m[1] : null;
        glList = (cat && window._pmCatGLs && window._pmCatGLs[cat]) ? window._pmCatGLs[cat] : [];
    }
    const lines = glList.map(g => LINES.find(l => l.gl_code === g)).filter(Boolean);
    const valFor = (line, c) => {
        switch (c) {
            case 'prior':    return line.prior_year || 0;
            case 'ytd':      return line.ytd_actual || 0;
            case 'estimate': return computeEstimate(line);
            case 'forecast': return computeForecast(line);
            case 'budget':   return line.current_budget || 0;
            case 'proposed': return computeProposed(line);
            default:         return 0;
        }
    };

    let display;
    if (col === 'variance' || col === 'varpct') {
        // $ Inc / % Inc vs 12-Mo Forecast = Σproposed − Σforecast (matches catVar).
        const sp = lines.reduce((s, l) => s + computeProposed(l), 0);
        const sf = lines.reduce((s, l) => s + computeForecast(l), 0);
        if (col === 'variance') {
            display = pmXlExpr([sp, -sf]) || '=0';
        } else {
            display = sf ? ('=(' + (pmXlExpr([sp, -sf]) || '=0').slice(1) + ')/' + Math.round(sf)) : '';
        }
    } else {
        // Sum column — show each non-zero line value + the total.
        const nz = lines.map(l => valFor(l, col)).filter(v => Math.round(v) !== 0);
        const sum = lines.reduce((s, l) => s + valFor(l, col), 0);
        if (!nz.length) display = '=' + Math.round(parseFloat(td.dataset.raw) || 0);
        else display = pmXlExpr(nz) || '=0';
    }

    bar.value = display;
    bar.disabled = true;          // derived roll-up — PM edits the line cells, not the total
    bar.style.opacity = '0.85';
    label.textContent = scope + ' · ' + col;
    label.style.display = '';
    accept.style.display = 'none';
    cancel.style.display = 'none';
    clear.style.display = 'none';
    preview.style.display = 'none';
}

// Live preview while typing in formula bar
function pmFormulaBarPreview() {
    const bar = document.getElementById('pmFormulaBar');
    const preview = document.getElementById('pmFormulaPreview');
    if (!_pmEditMode) { preview.style.display = 'none'; return; }
    const typed = bar.value.trim();
    if (!typed || typed === _pmOriginalFormula) { preview.style.display = 'none'; return; }
    const result = safeEvalFormula(typed);
    if (result !== null) {
        preview.textContent = '= ' + fmt(result);
        preview.style.color = '#059669';
        preview.style.display = '';
    } else if (/^[\d$,.\-\s]+$/.test(typed)) {
        const num = parseDollar(typed);
        preview.textContent = '= ' + fmt(num);
        preview.style.color = '#2563eb';
        preview.style.display = '';
    } else {
        preview.textContent = 'Invalid formula';
        preview.style.color = 'var(--red)';
        preview.style.display = '';
    }
}

// Accept formula/value from formula bar
function pmFormulaBarAccept() {
    if (!_pmCurrentCell) return;
    const bar = document.getElementById('pmFormulaBar');
    const gl = _pmCurrentCell.dataset.gl;
    const field = _pmCurrentCell.dataset.field;
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    // Stash undo state before changing anything
    const badge = _pmCurrentCell.parentElement.querySelector('.pm-fx');
    _pmFormulaBarUndo = {
        gl: gl, field: field, cellId: _pmCurrentCell.id,
        formula: _pmOriginalFormula,
        raw: _pmCurrentCell.dataset.raw || '',
        value: _pmCurrentCell.value,
        lineSnapshot: {
            estimate_override: line.estimate_override,
            forecast_override: line.forecast_override,
            proposed_budget: line.proposed_budget,
            proposed_formula: line.proposed_formula,
        },
        badgeText: badge ? badge.textContent : '',
        badgeBg: badge ? badge.style.background : '',
        badgeColor: badge ? badge.style.color : '',
        badgeBorder: badge ? badge.style.borderColor : '',
    };

    const typed = bar.value.trim();
    const isFormula = typed.startsWith('=') || /[+\-*\/()]/.test(typed);
    const formulaResult = safeEvalFormula(typed);
    const numericVal = parseDollar(typed);

    let finalVal;
    if (isFormula && formulaResult !== null) {
        finalVal = formulaResult;
    } else if (!isNaN(numericVal)) {
        finalVal = numericVal;
    } else {
        showToast('Invalid formula or value', 'error');
        return;
    }

    // Set override on LINES object based on field
    if (field === 'estimate') {
        line.estimate_override = Math.round(finalVal);
    } else if (field === 'forecast') {
        line.forecast_override = Math.round(finalVal);
    } else if (field === 'proposed') {
        line.proposed_budget = Math.round(finalVal);
        if (isFormula && formulaResult !== null) {
            line.proposed_formula = typed.startsWith('=') ? typed : '=' + typed;
        } else {
            line.proposed_formula = null;
        }
    }

    _pmCurrentCell.dataset.formula = typed;
    _pmCurrentCell.value = fmt(finalVal);
    _pmCurrentCell.dataset.raw = Math.round(finalVal);

    // Update badge: fx for formula, ✎ for manual override
    // (badge already queried at top of function)
    if (badge) {
        if (isFormula && formulaResult !== null) {
            badge.textContent = 'fx';
            badge.style.background = '#dbeafe';
            badge.style.color = 'var(--blue)';
            badge.style.borderColor = 'var(--blue)';
        } else {
            badge.textContent = '✎';
            badge.style.background = '#f97316';
            badge.style.color = '#fff';
            badge.style.borderColor = '#ea580c';
        }
    }

    // Flash green confirmation
    _pmCurrentCell.style.outline = '2px solid var(--green)';
    setTimeout(() => { if (_pmCurrentCell) _pmCurrentCell.style.outline = ''; }, 1200);

    pmLineChanged(gl, field, null);
    pmFormulaBarCancel();
    // Show undo after cancel clears the editing UI
    const undoBtn = document.getElementById('pmFormulaUndo');
    if (undoBtn && _pmFormulaBarUndo) undoBtn.style.display = 'inline-block';
}

// Cancel formula bar edits
function pmFormulaBarCancel() {
    const bar = document.getElementById('pmFormulaBar');
    const label = document.getElementById('pmFormulaLabel');
    const preview = document.getElementById('pmFormulaPreview');
    const accept = document.getElementById('pmFormulaAccept');
    const cancel = document.getElementById('pmFormulaCancel');
    const clear = document.getElementById('pmFormulaClear');

    bar.value = '';
    label.style.display = 'none';
    preview.style.display = 'none';
    accept.style.display = 'none';
    cancel.style.display = 'none';
    clear.style.display = 'none';
    _pmEditMode = false;
}

// Clear formula/override, revert to auto-calc
function pmFormulaBarClear() {
    if (!_pmCurrentCell) return;
    const gl = _pmCurrentCell.dataset.gl;
    const field = _pmCurrentCell.dataset.field;
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    if (field === 'estimate' && line.estimate_override !== null && line.estimate_override !== undefined) {
        line.estimate_override = null;
    } else if (field === 'forecast' && line.forecast_override !== null && line.forecast_override !== undefined) {
        line.forecast_override = null;
    } else if (field === 'proposed' && line.proposed_formula) {
        line.proposed_formula = null;
    }

    // Recalculate and update cell
    let newVal;
    if (field === 'estimate') {
        newVal = computeEstimate(line);
    } else if (field === 'forecast') {
        newVal = computeForecast(line);
    } else if (field === 'proposed') {
        newVal = computeProposed(line);
    }

    _pmCurrentCell.value = fmt(newVal);
    _pmCurrentCell.dataset.raw = Math.round(newVal);

    const badge = _pmCurrentCell.parentElement.querySelector('.pm-fx');
    if (badge) {
        badge.textContent = 'fx';
        badge.style.background = 'var(--blue-light, #e1effe)';
        badge.style.color = 'var(--blue)';
        badge.style.borderColor = 'var(--blue)';
    }

    pmLineChanged(gl, field, newVal);
    pmFormulaBarCancel();
}

// Undo: revert the last accepted PM formula change
function pmFormulaBarUndo() {
    if (!_pmFormulaBarUndo) return;
    const u = _pmFormulaBarUndo;
    const el = document.getElementById(u.cellId);
    if (!el) { _pmFormulaBarUndo = null; return; }

    const line = LINES.find(l => l.gl_code === u.gl);
    if (line) {
        line.estimate_override = u.lineSnapshot.estimate_override;
        line.forecast_override = u.lineSnapshot.forecast_override;
        line.proposed_budget = u.lineSnapshot.proposed_budget;
        line.proposed_formula = u.lineSnapshot.proposed_formula;
    }

    el.value = u.value;
    el.dataset.raw = u.raw;
    el.dataset.formula = u.formula;

    const badge = el.parentElement.querySelector('.pm-fx');
    if (badge) {
        badge.textContent = u.badgeText;
        badge.style.background = u.badgeBg;
        badge.style.color = u.badgeColor;
        badge.style.borderColor = u.badgeBorder;
    }

    // Flash amber and save reverted state
    el.style.outline = '2px solid #c2410c';
    setTimeout(() => { el.style.outline = ''; }, 1200);

    pmLineChanged(u.gl, u.field, null);
    _pmFormulaBarUndo = null;
    const undoBtn = document.getElementById('pmFormulaUndo');
    if (undoBtn) undoBtn.style.display = 'none';
}

// Keyboard navigation in formula bar
function pmFormulaBarKeydown(e) {
    if (e.key === 'Enter') {
        pmFormulaBarAccept();
    } else if (e.key === 'Escape') {
        pmFormulaBarCancel();
    }
}

// ── PM-side Per-Tab Undo + History (FA dir 2026-05-19) ────────────────
// Mirrors the FA dashboard buttons. Uses _pmActiveSheet as the sheet filter
// and the same /api/recent-changes endpoint. Reverting any change refreshes
// the PM portal table so the restored value appears.

async function pmTabUndoLast() {
  const sheet = _pmActiveSheet || '';
  if (!sheet) { alert('No active sheet'); return; }
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(ENTITY) +
                              '?sheet=' + encodeURIComponent(sheet) + '&limit=20');
    if (!resp.ok) { alert('Could not load recent changes: ' + resp.status); return; }
    const data = await resp.json();
    const changes = data.changes || [];
    const target = changes.find(c => c.undoable);
    if (!target) {
      alert('No undoable changes on the ' + sheet + ' tab yet.');
      return;
    }
    const fieldLabel = target.field || target.action || 'change';
    if (!confirm('Undo the most recent change on ' + sheet + '?\n\n' +
                  (target.gl_code ? target.gl_code + ' · ' : '') +
                  fieldLabel + ': ' +
                  (target.old_value || '(empty)') + ' ← ' + (target.new_value || '(empty)'))) return;
    const undoResp = await fetch('/api/recent-changes/' + encodeURIComponent(ENTITY) + '/undo', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({revision_id: target.id}),
    });
    if (!undoResp.ok) {
      alert('Undo failed: ' + (await undoResp.text()).slice(0, 200));
      return;
    }
    // Reload PM lines so the change reflects in the grid
    await pmReloadLinesAndRender();
    showToast('Undid ' + fieldLabel + ' on ' + (target.gl_code || sheet), 'success');
  } catch (e) {
    alert('Undo error: ' + e.message);
  }
}

async function pmTabShowHistory() {
  const sheet = _pmActiveSheet || '';
  if (!sheet) { alert('No active sheet'); return; }
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(ENTITY) +
                              '?sheet=' + encodeURIComponent(sheet) + '&limit=50');
    if (!resp.ok) { alert('Could not load history: ' + resp.status); return; }
    const data = await resp.json();
    _pmTabRenderHistoryModal(sheet, data.changes || []);
  } catch (e) {
    alert('History error: ' + e.message);
  }
}

function _pmTabRenderHistoryModal(sheet, changes) {
  const existing = document.getElementById('pmTabHistoryRoot');
  if (existing) existing.remove();
  const fieldLabels = {
    proposed_budget: 'Proposed', increase_pct: 'Increase %', increase_dollar: 'Increase $',
    estimate_override: 'Estimate', forecast_override: 'Forecast',
    estimate_formula: 'Est. formula', forecast_formula: 'Fcst. formula', proposed_formula: 'Prop. formula',
    accrual_adj: 'Accrual', unpaid_bills: 'Unpaid', current_budget: 'Curr. Budget',
    prior_year: 'Prior Year', ytd_actual: 'YTD',
    notes: 'Notes', category: 'Category', pm_review_state: 'PM review',
    fa_proposed_status: 'FA decision', fa_proposed_note: 'FA note', fa_override_value: 'FA override',
  };
  const _esc = (s) => (s===null||s===undefined?'':String(s)).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  const _fmtV = (raw, field) => {
    if (raw === null || raw === undefined || raw === '') return '(empty)';
    const s = String(raw);
    const numericFields = ['proposed_budget','increase_dollar','estimate_override','forecast_override','accrual_adj','unpaid_bills','current_budget','prior_year','ytd_actual','fa_override_value'];
    if (numericFields.indexOf(field) >= 0) {
      const n = parseFloat(s);
      if (!isNaN(n)) return (n < 0 ? '-$' : '$') + Math.abs(Math.round(n)).toLocaleString();
    }
    if (field === 'increase_pct') {
      const n = parseFloat(s);
      if (!isNaN(n)) return (n * 100).toFixed(1) + '%';
    }
    if (s.length > 60) return s.slice(0, 57) + '…';
    return s;
  };
  let html = '';
  html += '<div id="pmTabHistoryOverlay" onclick="_pmTabCloseHistory()" style="position:fixed; inset:0; background:rgba(0,0,0,0.45); z-index:2000;"></div>';
  html += '<div id="pmTabHistoryModal" style="position:fixed; top:60px; left:50%; transform:translateX(-50%); width:720px; max-width:94vw; max-height:82vh; background:white; border-radius:12px; box-shadow:0 24px 60px rgba(0,0,0,0.3); z-index:2001; overflow:hidden; display:flex; flex-direction:column;">';
  html += '<div style="padding:14px 22px; border-bottom:1px solid var(--gray-200); display:flex; justify-content:space-between; align-items:center;">';
  html += '<h3 style="margin:0; font-size:15px; font-weight:700; color:var(--gray-900);">⏱ History · ' + _esc(sheet) + ' tab</h3>';
  html += '<button onclick="_pmTabCloseHistory()" style="border:none; background:transparent; font-size:20px; cursor:pointer; color:var(--gray-500); line-height:1;">×</button>';
  html += '</div>';
  html += '<div style="padding:8px 18px; font-size:11px; color:var(--gray-500); background:#fafbfc; border-bottom:1px solid var(--gray-200);">';
  html += changes.length + ' change' + (changes.length !== 1 ? 's' : '') + ' on this tab · newest first · Restore reverts a single field';
  html += '</div>';
  html += '<div style="overflow-y:auto; flex:1;">';
  if (!changes.length) {
    html += '<div style="padding:40px; text-align:center; color:var(--gray-500); font-size:13px;">No changes logged on this tab yet.</div>';
  } else {
    for (const c of changes) {
      const fieldLabel = fieldLabels[c.field] || c.field || c.action;
      const oldDisp = _fmtV(c.old_value, c.field);
      const newDisp = _fmtV(c.new_value, c.field);
      const ts = c.ts ? new Date(c.ts) : null;
      const tsLocal = ts ? ts.toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'}) : '';
      html += '<div style="padding:12px 22px; border-bottom:1px solid var(--gray-100); display:grid; grid-template-columns:1fr auto; gap:12px;">';
      html += '<div style="min-width:0;">';
      if (c.gl_code) html += '<div style="font:600 13px -apple-system,sans-serif; color:var(--gray-900); margin-bottom:3px;">' + _esc(c.gl_code) + (c.description ? ' · ' + _esc(c.description) : '') + '</div>';
      html += '<div style="font-size:12px; color:var(--gray-600); line-height:1.5;">';
      html += '<b style="color:var(--gray-900);">' + _esc(fieldLabel) + '</b>: ';
      html += '<span style="color:#94a3b8; text-decoration:line-through;">' + _esc(oldDisp) + '</span> → ';
      html += '<span style="color:var(--gray-900); font-weight:600;">' + _esc(newDisp) + '</span>';
      html += '</div>';
      html += '<div style="font-size:11px; color:var(--gray-400); margin-top:4px;">' + _esc(tsLocal);
      if (c.source) html += ' · ' + _esc(c.source);
      if (c.action === 'undo') html += ' · <span style="color:var(--blue);">UNDO</span>';
      html += '</div>';
      html += '</div>';
      if (c.undoable) {
        html += '<button onclick="_pmTabRestoreFromHistory(' + c.id + ', this)" style="align-self:center; padding:6px 14px; font:600 12px -apple-system,sans-serif; background:var(--blue, #1d4ed8); color:white; border:none; border-radius:6px; cursor:pointer; white-space:nowrap;">↺ Restore</button>';
      } else {
        html += '<span style="align-self:center; color:var(--gray-400); font-size:11px;">not undoable</span>';
      }
      html += '</div>';
    }
  }
  html += '</div>';
  html += '<div style="padding:10px 22px; background:var(--gray-50, #fafbfc); border-top:1px solid var(--gray-200); font-size:10px; color:var(--gray-500); text-align:right;">Last 50 changes shown.</div>';
  html += '</div>';
  const wrap = document.createElement('div');
  wrap.id = 'pmTabHistoryRoot';
  wrap.innerHTML = html;
  document.body.appendChild(wrap);
}

function _pmTabCloseHistory() {
  const r = document.getElementById('pmTabHistoryRoot');
  if (r) r.remove();
}

async function _pmTabRestoreFromHistory(revId, btn) {
  if (!confirm('Restore this version of the field?')) return;
  if (btn) { btn.disabled = true; btn.textContent = 'Restoring…'; }
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(ENTITY) + '/undo', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({revision_id: revId}),
    });
    if (!resp.ok) {
      alert('Restore failed: ' + (await resp.text()).slice(0, 200));
      if (btn) { btn.disabled = false; btn.textContent = '↺ Restore'; }
      return;
    }
    _pmTabCloseHistory();
    await pmReloadLinesAndRender();
    showToast('Restored', 'success');
  } catch (e) {
    alert('Restore error: ' + e.message);
    if (btn) { btn.disabled = false; btn.textContent = '↺ Restore'; }
  }
}

// Reload LINES from server then re-render the active sheet. Used after
// undo/restore so the grid shows the new (restored) values.
async function pmReloadLinesAndRender() {
  try {
    const r = await fetch('/api/lines/' + ENTITY + '?_=' + Date.now());
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    if (Array.isArray(data)) {
      LINES = data;
    } else if (data && Array.isArray(data.lines)) {
      LINES = data.lines;
    }
    if (typeof renderTable === 'function') renderTable();
    if (typeof populateMyChanges === 'function') populateMyChanges();
  } catch (e) {
    console.warn('Reload after undo failed:', e);
  }
}

// Get formula tooltip string for cell
function pmGetFormulaTooltip(line, type) {
    const ytd = line.ytd_actual || 0;
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    const base = ytd + accrual + unpaid;
    const prior = line.prior_year || 0;
    const estimate = computeEstimate(line);
    const forecast = computeForecast(line);
    const incPct = line.increase_pct || 0;

    if (type === 'estimate') {
        if (YTD_MONTHS > 0) return '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
        return '=0';
    }
    if (type === 'forecast') {
        const estExpr = (YTD_MONTHS > 0) ? '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0';
        return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + estExpr + ')';
    }
    if (type === 'proposed') {
        if (line.proposed_formula) return line.proposed_formula;
        const fcstExpr = ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + ((YTD_MONTHS > 0) ? '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0') + ')';
        return '=(' + fcstExpr + ')*(1+' + incPct.toFixed(4) + ')';
    }
    return '';
}

// Cascade recalculation when any cell changes
function pmLineChanged(gl, field, value) {
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    // Recalculate dependent fields
    const estimate = computeEstimate(line);
    const forecast = computeForecast(line);
    const proposed = computeProposed(line);
    // FA dir 2026-05-21: variance + pctChange now compare PROPOSED vs FORECAST
    // (was current_budget vs forecast). Column headers relabeled to
    // "% Inc vs 12-Mo Forecast" and "$ Inc vs 12-Mo Forecast".
    const variance = proposed - forecast;
    const pctChange = (forecast && isFinite(forecast)) ? ((proposed - forecast) / forecast) : 0;

    // Update cells in DOM
    const estEl = document.getElementById('pm_est_' + gl);
    const fcEl = document.getElementById('pm_fc_' + gl);
    const propEl = document.getElementById('pm_prop_' + gl);
    const varEl = document.getElementById('pm_var_' + gl);
    const pctEl = document.getElementById('pm_pct_' + gl);

    if (estEl && estEl.dataset.field === 'estimate') {
        estEl.value = fmt(estimate); estEl.dataset.raw = Math.round(estimate);
        if (!(line.estimate_override !== null && line.estimate_override !== undefined)) estEl.dataset.formula = pmGetFormulaTooltip(line, 'estimate');
    }
    if (fcEl && fcEl.dataset.field === 'forecast') {
        fcEl.value = fmt(forecast); fcEl.dataset.raw = Math.round(forecast);
        if (!(line.forecast_override !== null && line.forecast_override !== undefined)) fcEl.dataset.formula = pmGetFormulaTooltip(line, 'forecast');
    }
    if (propEl && propEl.dataset.field === 'proposed') {
        propEl.value = fmt(proposed); propEl.dataset.raw = Math.round(proposed);
        if (!line.proposed_formula) propEl.dataset.formula = pmGetFormulaTooltip(line, 'proposed');
    }
    // FA dir 2026-05-18: single-entry mode — proposed cell is now an editable
    // text input with data-field="proposed_budget". Skip the legacy formula-
    // cell path above and instead repaint the derived Increase % / $ pills.
    if (propEl && propEl.dataset.field === 'proposed_budget' && field !== 'proposed_budget') {
        // Don't clobber the PM's typed value here — only repaint when the
        // change was triggered by another field (e.g. current_budget). When
        // field === 'proposed_budget', pmProposedBlur already set the cell.
        propEl.value = (proposed || proposed === 0) ? fmt(proposed) : '';
        propEl.dataset.raw = proposed === null || proposed === undefined ? '' : String(Math.round(proposed));
    }
    _pmUpdateDerivedPills(gl, line);
    _pmToggleNoChangeBtn(gl, line);
    if (varEl) {
        varEl.value = fmt(variance); varEl.dataset.raw = Math.round(variance);
        varEl.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
        varEl.dataset.formula = pmXlExpr([proposed, -forecast]) || '=0';   // proposed - forecast (matches the displayed value)
        const varTd = varEl.parentElement; if (varTd) varTd.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
    }
    if (pctEl) {
        const pctDisp = isFinite(pctChange) ? (pctChange * 100).toFixed(1) : '0.0';
        pctEl.value = pctDisp + '%'; pctEl.dataset.raw = isFinite(pctChange) ? pctChange : 0;
        pctEl.dataset.formula = Math.round(forecast) ? ('=(' + (pmXlExpr([proposed, -forecast]) || '=0').slice(1) + ')/' + Math.round(forecast)) : '=0';
    }

    // Refresh material-variance note nudge
    if (typeof pmUpdateNoteWarn === 'function') pmUpdateNoteWarn(gl);

    // Update subtotals and grand totals
    pmUpdateTotals();

    // Debounced save
    if (saveTimer) clearTimeout(saveTimer);
    indicator.textContent = 'Unsaved changes...';
    indicator.className = 'save-indicator saving';
    saveTimer = setTimeout(saveAll, 800);
}

// Update all subtotal and grand total rows
function pmUpdateTotals() {
    const sheetCfg = PM_SHEET_CATEGORIES[_pmActiveSheet];
    const categories = {};
    Object.keys(sheetCfg.cats).forEach(k => categories[k] = []);
    const catLabels = sheetCfg.labels;
    LINES.forEach(l => {
        if (!sheetCfg.match(l)) return;
        const cat = sheetCfg.assign(l);
        if (categories[cat]) categories[cat].push(l);
    });

    let grandTotals = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};

    for (const [cat, catLines] of Object.entries(categories)) {
        if (catLines.length === 0) continue;

        let catTotals = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
        catLines.forEach(l => {
            catTotals.prior += (l.prior_year || 0);
            catTotals.ytd += (l.ytd_actual || 0);
            catTotals.accrual += (l.accrual_adj || 0);
            catTotals.unpaid += (l.unpaid_bills || 0);
            catTotals.estimate += computeEstimate(l);
            catTotals.forecast += computeForecast(l);
            catTotals.budget += (l.current_budget || 0);
            catTotals.proposed += computeProposed(l);
        });

        // Update subtotal cells
        const subPrior = document.getElementById('pm_subtotal_prior_' + cat);
        const subYtd = document.getElementById('pm_subtotal_ytd_' + cat);
        const subEstimate = document.getElementById('pm_subtotal_estimate_' + cat);
        const subForecast = document.getElementById('pm_subtotal_forecast_' + cat);
        const subBudget = document.getElementById('pm_subtotal_budget_' + cat);
        const subProposed = document.getElementById('pm_subtotal_proposed_' + cat);
        const subVar = document.getElementById('pm_subtotal_variance_' + cat);

        if (subPrior) subPrior.textContent = fmt(catTotals.prior);
        if (subYtd) subYtd.textContent = fmt(catTotals.ytd);
        if (subEstimate) subEstimate.textContent = fmt(catTotals.estimate);
        if (subForecast) subForecast.textContent = fmt(catTotals.forecast);
        if (subBudget) subBudget.textContent = fmt(catTotals.budget);
        if (subProposed) subProposed.textContent = fmt(catTotals.proposed);
        const catVar = catTotals.budget - catTotals.forecast;
        if (subVar) { subVar.textContent = fmt(catVar); subVar.style.color = catVar >= 0 ? 'var(--red)' : 'var(--green)'; }

        // FA dir 2026-05-18: live-update subtotal Inc% / Inc$ pills.
        const _subIncPctEl = document.getElementById('pm_subtotal_incpct_' + cat);
        const _subIncDolEl = document.getElementById('pm_subtotal_incdol_' + cat);
        if (_subIncPctEl || _subIncDolEl) {
            const _sd = catTotals.proposed - catTotals.budget;
            const _sn = Math.abs(catTotals.budget);
            const _sk = Math.abs(_sd) < 0.5 ? 'flat' : (_sd > 0 ? 'up' : 'down');
            const _sdTxt = Math.abs(_sd) < 0.5 ? '$0'
                : (_sd > 0 ? '+' : '-') + '$' + Math.abs(Math.round(_sd)).toLocaleString();
            const _spct = _sn > 0.5 ? (_sd / _sn) * 100 : null;
            const _spTxt = _spct === null ? '—'
                : (Math.abs(_spct) < 0.05 ? '0.0%' : (_spct > 0 ? '+' : '') + _spct.toFixed(1) + '%');
            if (_subIncPctEl) { _subIncPctEl.textContent = _spTxt; _subIncPctEl.className = 'pm-pill ' + _sk; }
            if (_subIncDolEl) { _subIncDolEl.textContent = _sdTxt; _subIncDolEl.className = 'pm-pill ' + _sk; }
        }

        Object.keys(grandTotals).forEach(k => grandTotals[k] += catTotals[k]);
    }

    // Update grand total cells
    const grandPrior = document.getElementById('pm_grandtotal_prior');
    const grandYtd = document.getElementById('pm_grandtotal_ytd');
    const grandEstimate = document.getElementById('pm_grandtotal_estimate');
    const grandForecast = document.getElementById('pm_grandtotal_forecast');
    const grandBudget = document.getElementById('pm_grandtotal_budget');
    const grandProposed = document.getElementById('pm_grandtotal_proposed');
    const grandVar = document.getElementById('pm_grandtotal_variance');
    const grandPct = document.getElementById('pm_grandtotal_pct');

    if (grandPrior) grandPrior.textContent = fmt(grandTotals.prior);
    if (grandYtd) grandYtd.textContent = fmt(grandTotals.ytd);
    if (grandEstimate) grandEstimate.textContent = fmt(grandTotals.estimate);
    if (grandForecast) grandForecast.textContent = fmt(grandTotals.forecast);
    if (grandBudget) grandBudget.textContent = fmt(grandTotals.budget);
    if (grandProposed) grandProposed.textContent = fmt(grandTotals.proposed);
    const gVar = grandTotals.budget - grandTotals.forecast;
    const gPct = grandTotals.forecast ? ((grandTotals.budget - grandTotals.forecast) / grandTotals.forecast) : 0;
    if (grandVar) { grandVar.textContent = fmt(gVar); grandVar.style.color = gVar >= 0 ? 'var(--red)' : 'var(--green)'; }
    if (grandPct) grandPct.textContent = (gPct * 100).toFixed(1) + '%';

    // FA dir 2026-05-18: live-update grand-total Inc% / Inc$ pills.
    const _gIncPctEl = document.getElementById('pm_grandtotal_incpct');
    const _gIncDolEl = document.getElementById('pm_grandtotal_incdol');
    if (_gIncPctEl || _gIncDolEl) {
        const _gd = grandTotals.proposed - grandTotals.budget;
        const _gn = Math.abs(grandTotals.budget);
        const _gk = Math.abs(_gd) < 0.5 ? 'flat' : (_gd > 0 ? 'up' : 'down');
        const _gdTxt = Math.abs(_gd) < 0.5 ? '$0'
            : (_gd > 0 ? '+' : '-') + '$' + Math.abs(Math.round(_gd)).toLocaleString();
        const _gp = _gn > 0.5 ? (_gd / _gn) * 100 : null;
        const _gpTxt = _gp === null ? '—'
            : (Math.abs(_gp) < 0.05 ? '0.0%' : (_gp > 0 ? '+' : '') + _gp.toFixed(1) + '%');
        if (_gIncPctEl) { _gIncPctEl.textContent = _gpTxt; _gIncPctEl.className = 'pm-pill ' + _gk; }
        if (_gIncDolEl) { _gIncDolEl.textContent = _gdTxt; _gIncDolEl.className = 'pm-pill ' + _gk; }
    }
}

function renderTable() {
    const tbody = document.getElementById('linesBody');
    tbody.innerHTML = '';

    // Group by category based on active sheet tab
    const sheetCfg = PM_SHEET_CATEGORIES[_pmActiveSheet];
    const categories = {};
    Object.keys(sheetCfg.cats).forEach(k => categories[k] = []);
    const catLabels = sheetCfg.labels;
    LINES.forEach(l => {
        if (!sheetCfg.match(l)) return;
        const cat = sheetCfg.assign(l);
        if (categories[cat]) categories[cat].push(l);
    });

    let grandTotals = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
    const NC = 15;
    // Maps for pmSubtotalFocus to rebuild the numeric breakdown shown in the
    // formula bar (which lines feed each category subtotal + the grand total).
    window._pmCatGLs = {};
    window._pmAllGLs = [];

    for (const [cat, catLines] of Object.entries(categories)) {
        if (catLines.length === 0) continue;
        window._pmCatGLs[cat] = catLines.map(l => l.gl_code);
        catLines.forEach(l => window._pmAllGLs.push(l.gl_code));

        const headerRow = document.createElement('tr');
        headerRow.className = 'category-header';
        headerRow.innerHTML = '<td class="frozen frozen-gl"></td><td class="frozen frozen-desc">' + catLabels[cat] + '</td><td colspan="' + (NC - 2) + '"></td>';
        tbody.appendChild(headerRow);

        let catTotals = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};

        catLines.forEach(line => {
            const estimate = computeEstimate(line);
            const forecast = computeForecast(line);
            const proposed = computeProposed(line);
            // FA dir 2026-05-21: $ Inc / % Inc vs 12-Mo Forecast = proposed - forecast
            const variance = proposed - forecast;
            const pctChange = forecast ? ((proposed - forecast) / forecast) : 0;

            catTotals.prior += (line.prior_year || 0);
            catTotals.ytd += (line.ytd_actual || 0);
            catTotals.estimate += estimate;
            catTotals.forecast += forecast;
            catTotals.budget += (line.current_budget || 0);
            catTotals.proposed += proposed;

            const reclassBadge = line.reclass_to_gl ? ' <span style="background:var(--orange-light); color:var(--orange); font-size:10px; padding:1px 5px; border-radius:8px;">Reclass</span>' : '';

            const isZero = !(line.prior_year || line.ytd_actual || line.accrual_adj || line.unpaid_bills || line.current_budget || (line.increase_pct && line.increase_pct !== 0));
            const tr = document.createElement('tr');
            if (isZero) { tr.classList.add('zero-row'); if (!_showZeroRows) tr.style.display = 'none'; }

            // FA directive 2026-05-11: R&M review-gate row state. Only R&M
            // rows get the red/green left border and the "No change" pill.
            // G&A rows stay edit-optional and unmarked.
            const isRm = (line.sheet_name === 'Repairs & Supplies');
            if (isRm) {
                tr.classList.add(line.pm_review_state ? 'pm-row-rm-reviewed' : 'pm-row-rm-unreviewed');
                tr.dataset.gl = line.gl_code;
            }

            const gl = line.gl_code;
            const estFormula = pmGetFormulaTooltip(line, 'estimate');
            const fcstFormula = pmGetFormulaTooltip(line, 'forecast');
            const propFormula = pmGetFormulaTooltip(line, 'proposed');

            // FA directive 2026-05-11: lock all cells except Increase % / Increase $ / Notes.
            // Even if CAN_EDIT is true, these 5 cells now ship `disabled`. Notes stays
            // editable so the PM can explain why they're proposing a change.
            tr.innerHTML = `
                <td class="frozen frozen-gl"><a href="#" onclick="toggleInvoices('${gl}', this); return false;" style="color:var(--blue); text-decoration:none; font-variant-numeric:tabular-nums;" title="Click to view invoices">${gl}</a>${reclassBadge}</td>
                <td class="frozen frozen-desc"><a href="#" onclick="toggleInvoices('${gl}', this); return false;" style="color:inherit; text-decoration:none; cursor:pointer;" title="Click to view expenses">${line.description} <span class="drill-arrow" style="font-size:10px; color:var(--gray-400); transition:transform 0.2s;">▶</span></a></td>
                <td class="number"><input id="pm_pr_${gl}" class="pm-cell" type="text" value="${fmt(line.prior_year)}" data-raw="${Math.round(line.prior_year || 0)}" data-gl="${gl}" data-field="prior_year" disabled title="Locked — only Increase % / Increase $ / Notes are editable"></td>
                <td class="number"><input id="pm_ytd_${gl}" class="pm-cell" type="text" value="${fmt(line.ytd_actual)}" data-raw="${Math.round(line.ytd_actual || 0)}" data-gl="${gl}" data-field="ytd_actual" disabled title="Locked — only Increase % / Increase $ / Notes are editable"></td>
                <td class="number"><input id="pm_acc_${gl}" class="pm-cell" type="text" value="${fmt(line.accrual_adj)}" data-raw="${Math.round(line.accrual_adj || 0)}" data-gl="${gl}" data-field="accrual_adj" disabled title="Locked — only Increase % / Increase $ / Notes are editable"></td>
                <td class="number"><input id="pm_unp_${gl}" class="pm-cell" type="text" value="${fmt(line.unpaid_bills)}" data-raw="${Math.round(line.unpaid_bills || 0)}" data-gl="${gl}" data-field="unpaid_bills" disabled title="Locked — only Increase % / Increase $ / Notes are editable"></td>
                <td class="number" style="position:relative; cursor:pointer;" onclick="pmFxCellFocus(document.getElementById('pm_est_${gl}'))">
                    <span class="pm-fx">fx</span>
                    <input id="pm_est_${gl}" class="pm-cell pm-cell-fx" type="text" readonly value="${fmt(estimate)}" data-raw="${Math.round(estimate)}" data-formula="${estFormula}" data-gl="${gl}" data-field="estimate" style="cursor:pointer; pointer-events:none;">
                </td>
                <td class="number" style="position:relative; cursor:pointer;" onclick="pmFxCellFocus(document.getElementById('pm_fc_${gl}'))">
                    <span class="pm-fx">fx</span>
                    <input id="pm_fc_${gl}" class="pm-cell pm-cell-fx" type="text" readonly value="${fmt(forecast)}" data-raw="${Math.round(forecast)}" data-formula="${fcstFormula}" data-gl="${gl}" data-field="forecast" style="cursor:pointer; pointer-events:none;">
                </td>
                <td class="number"><input id="pm_bud_${gl}" class="pm-cell" type="text" value="${fmt(line.current_budget)}" data-raw="${Math.round(line.current_budget || 0)}" data-gl="${gl}" data-field="current_budget" disabled title="Locked — only Increase % / Increase $ / Notes are editable"></td>
                ${(function(){
                  // FA directive 2026-05-18: SINGLE-ENTRY MODEL.
                  // PM types the 2027 Proposed Budget directly. Increase %
                  // and Increase $ become read-only derived pills computed
                  // from (proposed_budget - current_budget). When the PM
                  // hasn't entered a proposed value, both pills show "—".
                  const _hasProposed = line.proposed_budget !== null && line.proposed_budget !== undefined && line.proposed_budget !== '';
                  const _curr = parseFloat(line.current_budget || 0) || 0;
                  const _prop = _hasProposed ? (parseFloat(line.proposed_budget) || 0) : null;
                  let _pctTxt = '—', _dollarTxt = '—', _pillKlass = 'flat', _pctTitle = '', _dollarTitle = '';
                  if (_hasProposed) {
                    const _delta = _prop - _curr;
                    const _denom = Math.abs(_curr);
                    _pillKlass = Math.abs(_delta) < 0.5 ? 'flat' : (_delta > 0 ? 'up' : 'down');
                    if (Math.abs(_delta) < 0.5) {
                      _dollarTxt = '$0';
                    } else {
                      const _sign = _delta > 0 ? '+' : '-';
                      _dollarTxt = _sign + '$' + Math.abs(Math.round(_delta)).toLocaleString();
                    }
                    _dollarTitle = '= ' + fmt(_prop) + ' − ' + fmt(_curr);
                    if (_denom > 0.5) {
                      const _pct = (_delta / _denom) * 100;
                      _pctTxt = Math.abs(_pct) < 0.05 ? '0.0%' : ((_pct > 0 ? '+' : '') + _pct.toFixed(1) + '%');
                      _pctTitle = '= (' + fmt(_prop) + ' − ' + fmt(_curr) + ') / ' + fmt(_denom);
                    } else {
                      _pctTxt = (Math.abs(_delta) < 0.5) ? '0.0%' : '—';
                      _pctTitle = 'Current budget is $0 — % delta undefined.';
                    }
                  }
                  return `
                <td class="number">
                  <span id="pm_inc_${gl}" class="pm-pill ${_pillKlass}" data-gl="${gl}" data-field="increase_pct" title="${_pctTitle}">${_pctTxt}</span>
                </td>
                <td class="number">
                  <span id="pm_incd_${gl}" class="pm-pill ${_pillKlass}" data-gl="${gl}" data-field="increase_dollar" title="${_dollarTitle}">${_dollarTxt}</span>
                </td>
                  `;
                })()}
                ${(function(){
                  // FA dir 2026-05-21: G&A allowlist gate. For G&A rows whose
                  // GL is not in PM_EDITABLE_GA_GLS, lock the input and show
                  // a small lock icon so the PM sees why.
                  const _gaLocked = pmGaLineLocked(line);
                  const _lineCanEdit = CAN_EDIT && !_gaLocked;
                  const _propDisabledAttr = _lineCanEdit ? '' : 'disabled';
                  const _propTitle = _gaLocked
                    ? 'Locked — this G&A GL is managed by the FA. Contact your FA if a change is needed.'
                    : '2027 Proposed Budget — type a dollar amount. Increase $ and Increase % derive automatically.';
                  const _lockIcon = _gaLocked
                    ? '<span class="pm-ga-lock" title="FA-only G&A account" style="position:absolute; left:6px; top:50%; transform:translateY(-50%); font-size:11px; color:var(--gray-500); pointer-events:none;">🔒</span>'
                    : '';
                  const _noChangeBtn = (!line.pm_review_state && _lineCanEdit && (line.proposed_budget === null || line.proposed_budget === undefined || line.proposed_budget === ''))
                    ? `<button class="pm-no-change-inline" onclick="pmRmNoChange('${gl}')" title="Set Proposed = Current Budget (${fmt(line.current_budget || 0)})">=</button>`
                    : '';
                  return `
                <td class="number" style="position:relative; white-space:nowrap;">
                    ${_lockIcon}
                    <input id="pm_prop_${gl}" class="pm-cell pm-cell-proposed${_gaLocked ? ' pm-cell-ga-locked' : ''}" type="text" value="${(line.proposed_budget !== null && line.proposed_budget !== undefined && line.proposed_budget !== '') ? fmt(parseFloat(line.proposed_budget) || 0) : ''}" data-raw="${(line.proposed_budget !== null && line.proposed_budget !== undefined && line.proposed_budget !== '') ? String(Math.round(parseFloat(line.proposed_budget) || 0)) : ''}" data-gl="${gl}" data-field="proposed_budget" placeholder="${_gaLocked ? '' : 'Type proposed $...'}" onfocus="pmProposedFocus(this)" onblur="pmProposedBlur(this)" ${_propDisabledAttr} title="${_propTitle}" style="${_gaLocked ? 'padding-left:22px; background:var(--gray-100); color:var(--gray-500);' : ''}">
                    ${_noChangeBtn}
                </td>`;
                })()}
                <td class="number" style="position:relative; cursor:pointer; color:${variance >= 0 ? 'var(--red)' : 'var(--green)'};" onclick="pmFxCellFocus(document.getElementById('pm_pct_${gl}'))">
                    <span class="pm-fx">fx</span>
                    <input id="pm_pct_${gl}" class="pm-cell pm-cell-fx" type="text" readonly value="${(pctChange*100).toFixed(1)}%" data-raw="${pctChange}" data-formula="${Math.round(forecast) ? ('=(' + (pmXlExpr([proposed, -forecast]) || '=0').slice(1) + ')/' + Math.round(forecast)) : '=0'}" data-gl="${gl}" data-field="pct_change" style="cursor:pointer; pointer-events:none; color:${variance >= 0 ? 'var(--red)' : 'var(--green)'};">
                </td>
                <td class="number" style="position:relative; cursor:pointer; color:${variance >= 0 ? 'var(--red)' : 'var(--green)'};" onclick="pmFxCellFocus(document.getElementById('pm_var_${gl}'))">
                    <span class="pm-fx">fx</span>
                    <input id="pm_var_${gl}" class="pm-cell pm-cell-fx" type="text" readonly value="${fmt(variance)}" data-raw="${Math.round(variance)}" data-formula="${pmXlExpr([proposed, -forecast]) || '=0'}" data-gl="${gl}" data-field="variance" style="cursor:pointer; pointer-events:none; color:${variance >= 0 ? 'var(--red)' : 'var(--green)'};">
                </td>
                <td class="col-notes${(Math.abs(pctChange) > 0.10 && !(line.notes || '').trim()) ? ' needs-note' : ''}"><input type="text" value="${(line.notes || '').replace(/"/g, '&quot;')}" data-gl="${gl}" data-field="notes" oninput="onInput(this)" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'} placeholder="${(Math.abs(pctChange) > 0.10 && !(line.notes || '').trim()) ? 'Required at >10%' : 'Add context...'}" maxlength="500" style="min-width:140px; width:100%;"></td>
            `;
            tbody.appendChild(tr);
        });

        // Subtotal
        // FA dir 2026-05-21: catVar now = subtotal_proposed − subtotal_forecast
        const catVar = catTotals.proposed - catTotals.forecast;
        const catVarPct = catTotals.forecast ? (catVar / catTotals.forecast) : 0;
        // FA dir 2026-05-18 (visual cleanup v3): derive Inc% / Inc$ pills on
        // subtotal rows too, so the cells line up with data rows instead of
        // being empty white gaps. Pills compare sum_proposed vs sum_curr_budget.
        const _catDelta = catTotals.proposed - catTotals.budget;
        const _catDenom = Math.abs(catTotals.budget);
        const _catPct = _catDenom > 0.5 ? (_catDelta / _catDenom) * 100 : null;
        const _catKlass = Math.abs(_catDelta) < 0.5 ? 'flat' : (_catDelta > 0 ? 'up' : 'down');
        const _catDollarTxt = Math.abs(_catDelta) < 0.5 ? '$0'
            : (_catDelta > 0 ? '+' : '-') + '$' + Math.abs(Math.round(_catDelta)).toLocaleString();
        const _catPctTxt = _catPct === null ? '—'
            : (Math.abs(_catPct) < 0.05 ? '0.0%' : (_catPct > 0 ? '+' : '') + _catPct.toFixed(1) + '%');
        const subRow = document.createElement('tr');
        subRow.className = 'subtotal-row';
        subRow.innerHTML = `
            <td class="frozen frozen-gl"></td><td class="frozen frozen-desc">Total ${catLabels[cat]}</td>
            <td class="number pm-fx-td" id="pm_subtotal_prior_${cat}" style="position:relative; cursor:pointer;" data-col="prior" data-raw="${Math.round(catTotals.prior)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.prior)}</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_ytd_${cat}" style="position:relative; cursor:pointer;" data-col="ytd" data-raw="${Math.round(catTotals.ytd)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.ytd)}</span></td>
            <td></td><td></td>
            <td class="number pm-fx-td" id="pm_subtotal_estimate_${cat}" style="position:relative; cursor:pointer;" data-col="estimate" data-raw="${Math.round(catTotals.estimate)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.estimate)}</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_forecast_${cat}" style="position:relative; cursor:pointer;" data-col="forecast" data-raw="${Math.round(catTotals.forecast)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.forecast)}</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_budget_${cat}" style="position:relative; cursor:pointer;" data-col="budget" data-raw="${Math.round(catTotals.budget)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.budget)}</span></td>
            <td class="number"><span class="pm-pill ${_catKlass}" id="pm_subtotal_incpct_${cat}">${_catPctTxt}</span></td>
            <td class="number"><span class="pm-pill ${_catKlass}" id="pm_subtotal_incdol_${cat}">${_catDollarTxt}</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_proposed_${cat}" style="position:relative; cursor:pointer;" data-col="proposed" data-raw="${Math.round(catTotals.proposed)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.proposed)}</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_varpct_${cat}" style="position:relative; cursor:pointer; color:${catVar >= 0 ? 'var(--red)' : 'var(--green)'};" data-col="varpct" data-raw="${catVarPct}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${(catVarPct*100).toFixed(1)}%</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_variance_${cat}" style="position:relative; cursor:pointer; color:${catVar >= 0 ? 'var(--red)' : 'var(--green)'};" data-col="variance" data-raw="${Math.round(catVar)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catVar)}</span></td>
            <td class="col-notes"></td>
        `;
        tbody.appendChild(subRow);

        Object.keys(grandTotals).forEach(k => grandTotals[k] += catTotals[k]);
    }

    // Grand total
    // FA dir 2026-05-21: grandVar / grandPct now = grand_proposed − grand_forecast
    const grandVar = grandTotals.proposed - grandTotals.forecast;
    const grandPct = grandTotals.forecast ? ((grandTotals.proposed - grandTotals.forecast) / grandTotals.forecast) : 0;
    // FA dir 2026-05-18 (visual cleanup v3): grand-total Inc% / Inc$ pills
    // mirror the subtotal pattern. Compares total proposed vs total curr budget.
    const _gDelta = grandTotals.proposed - grandTotals.budget;
    const _gDenom = Math.abs(grandTotals.budget);
    const _gPct = _gDenom > 0.5 ? (_gDelta / _gDenom) * 100 : null;
    const _gKlass = Math.abs(_gDelta) < 0.5 ? 'flat' : (_gDelta > 0 ? 'up' : 'down');
    const _gDollarTxt = Math.abs(_gDelta) < 0.5 ? '$0'
        : (_gDelta > 0 ? '+' : '-') + '$' + Math.abs(Math.round(_gDelta)).toLocaleString();
    const _gPctTxt = _gPct === null ? '—'
        : (Math.abs(_gPct) < 0.05 ? '0.0%' : (_gPct > 0 ? '+' : '') + _gPct.toFixed(1) + '%');
    const grandRow = document.createElement('tr');
    grandRow.className = 'grand-total';
    grandRow.innerHTML = `
        <td class="frozen frozen-gl"></td><td class="frozen frozen-desc">${sheetCfg.grandLabel}</td>
        <td class="number pm-fx-td" id="pm_grandtotal_prior" style="position:relative; cursor:pointer;" data-col="prior" data-raw="${Math.round(grandTotals.prior)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.prior)}</span></td>
        <td class="number pm-fx-td" id="pm_grandtotal_ytd" style="position:relative; cursor:pointer;" data-col="ytd" data-raw="${Math.round(grandTotals.ytd)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.ytd)}</span></td>
        <td></td><td></td>
        <td class="number pm-fx-td" id="pm_grandtotal_estimate" style="position:relative; cursor:pointer;" data-col="estimate" data-raw="${Math.round(grandTotals.estimate)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.estimate)}</span></td>
        <td class="number pm-fx-td" id="pm_grandtotal_forecast" style="position:relative; cursor:pointer;" data-col="forecast" data-raw="${Math.round(grandTotals.forecast)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.forecast)}</span></td>
        <td class="number pm-fx-td" id="pm_grandtotal_budget" style="position:relative; cursor:pointer;" data-col="budget" data-raw="${Math.round(grandTotals.budget)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.budget)}</span></td>
        <td class="number"><span class="pm-pill ${_gKlass}" id="pm_grandtotal_incpct">${_gPctTxt}</span></td>
        <td class="number"><span class="pm-pill ${_gKlass}" id="pm_grandtotal_incdol">${_gDollarTxt}</span></td>
        <td class="number pm-fx-td" id="pm_grandtotal_proposed" style="position:relative; cursor:pointer;" data-col="proposed" data-raw="${Math.round(grandTotals.proposed)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.proposed)}</span></td>
        <td class="number" id="pm_grandtotal_pct" style="color:${grandVar >= 0 ? 'var(--red)' : 'var(--green)'};">${(grandPct * 100).toFixed(1)}%</td>
        <td class="number pm-fx-td" id="pm_grandtotal_variance" style="position:relative; cursor:pointer; color:${grandVar >= 0 ? 'var(--red)' : 'var(--green)'};" data-col="variance" data-raw="${Math.round(grandVar)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandVar)}</span></td>
        <td class="col-notes"></td>
    `;
    tbody.appendChild(grandRow);
    // Auto-size numeric columns after render
    autoSizeColumns(document.querySelector('#linesBody')?.closest('table'));

    // v2 visual: seed .pm-v2-filled on cells that already have a real
    // (non-mirror) value at initial render. _pmUpdateMirror runs the
    // toggle internally based on body.pm-v2; the call is cheap and the
    // function no-ops for lines without both cells. 2026-05-17.
    try {
      if (document.body.classList.contains('pm-v2')) {
        LINES.forEach(function (l) {
          if (l && l.gl_code) _pmUpdateMirror(l.gl_code, l);
        });
      }
    } catch (_e) {}
}

// ── Zero-row toggle ──────────────────────────────────────────────────
let _showZeroRows = false;

function countZeroRows() {
    return document.querySelectorAll('#linesBody .zero-row').length;
}

function updateZeroToggle() {
    const btn = document.getElementById('zeroToggle');
    if (!btn) return;
    const count = countZeroRows();
    if (count === 0) { btn.style.display = 'none'; return; }
    btn.style.display = '';
    btn.textContent = _showZeroRows ? 'Hide ' + count + ' Zero Rows' : 'Show ' + count + ' Hidden Zero Rows';
    btn.style.background = _showZeroRows ? 'var(--gray-200)' : 'var(--blue-light, #f5efe7)';
    btn.style.color = _showZeroRows ? 'var(--gray-600)' : 'var(--blue)';
    btn.style.borderColor = _showZeroRows ? 'var(--gray-300)' : 'var(--blue)';
}

function toggleZeroRows() {
    _showZeroRows = !_showZeroRows;
    document.querySelectorAll('#linesBody .zero-row').forEach(row => {
        row.style.display = _showZeroRows ? '' : 'none';
    });
    updateZeroToggle();
}

// Expense distribution drill-down
let _expenseCache = null;

async function fetchExpenseData() {
    if (_expenseCache !== null) return _expenseCache;
    try {
        const res = await fetch('/api/expense-dist/' + ENTITY);
        if (!res.ok) { _expenseCache = false; return null; }
        _expenseCache = await res.json();
        return _expenseCache;
    } catch(e) { _expenseCache = false; return null; }
}

async function toggleInvoices(glCode, linkEl) {
    const row = linkEl.closest('tr');
    const existingDetail = row.nextElementSibling;
    if (existingDetail && existingDetail.classList.contains('invoice-detail-row')) {
        existingDetail.remove();
        // Reset arrow indicators
        row.querySelectorAll('.drill-arrow').forEach(a => a.textContent = '▶');
        return;
    }

    // Set arrow to expanded
    row.querySelectorAll('.drill-arrow').forEach(a => a.textContent = '▼');

    const data = await fetchExpenseData();
    if (!data || !data.gl_groups) {
        const noData = document.createElement('tr');
        noData.className = 'invoice-detail-row';
        noData.innerHTML = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="13" style="padding:0;"><div class="drill-sticky" style="padding:12px 24px; background:#fef3c7; font-size:13px;">No expense distribution data uploaded yet. <a href="/pm/' + ENTITY + '/expenses" style="color:var(--blue);">Upload here</a></div></td>';
        row.after(noData);
        return;
    }

    const glGroup = data.gl_groups.find(g => g.gl_code === glCode);
    if (!glGroup || !glGroup.invoices || glGroup.invoices.length === 0) {
        const noInv = document.createElement('tr');
        noInv.className = 'invoice-detail-row';
        noInv.innerHTML = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="13" style="padding:0;"><div class="drill-sticky" style="padding:12px 24px; background:var(--gray-50); font-size:13px; color:var(--gray-500);">No invoices found for ' + glCode + '</div></td>';
        row.after(noInv);
        return;
    }

    // Build all GL codes for reclass dropdown
    const allGLs = LINES.map(l => l.gl_code).filter(g => g !== glCode);

    const detailRow = document.createElement('tr');
    detailRow.className = 'invoice-detail-row';
    let html = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="13" style="padding:0;"><div class="drill-sticky" style="padding:12px 16px 12px 24px; background:linear-gradient(to right, #f0f4ff, #f8faff); border-left:3px solid var(--blue); border-bottom:1px solid var(--gray-200);">';
    html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">';
    html += '<span style="font-weight:600; font-size:13px; color:var(--blue);">' + glCode + ' — ' + (glGroup.gl_name || '') + '</span>';
    html += '<span style="font-size:12px; color:var(--gray-500);">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' · ' + fmtAmt(glGroup.total || 0) + '</span>';
    html += '</div>';

    html += '<table style="width:auto; font-size:12px; border-collapse:separate; border-spacing:0; background:white; border-radius:6px; box-shadow:0 1px 2px rgba(0,0,0,0.05); overflow:hidden;">';
    html += '<thead style="position:static;"><tr style="position:static; background:var(--gray-100); color:var(--gray-600); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
    html += '<td style="padding:7px 16px; min-width:140px; border-bottom:2px solid var(--gray-300);">Payee</td><td style="padding:7px 16px; min-width:140px; border-bottom:2px solid var(--gray-300);">Description</td><td style="padding:7px 16px; min-width:70px; border-bottom:2px solid var(--gray-300);">Inv #</td><td style="padding:7px 16px; min-width:85px; border-bottom:2px solid var(--gray-300);">Date</td><td style="padding:7px 16px; min-width:100px; text-align:right; border-bottom:2px solid var(--gray-300);">Amount</td><td style="padding:7px 16px; min-width:90px; border-bottom:2px solid var(--gray-300);">Check #</td><td style="padding:7px 16px; min-width:90px; text-align:center; border-bottom:2px solid var(--gray-300);">Action</td></tr></thead>';

    glGroup.invoices.forEach(inv => {
        // FA dir 2026-06-05 (QA on 733): invoices are now grouped under their
        // EFFECTIVE GL (target when reclassed). So a reclassed invoice appears
        // HERE because it was moved INTO this GL from inv.gl_code — show it
        // normally with a "from <source>" tag + Undo, not greyed-out under the
        // old GL. This is the actual "move" the FA expects.
        const isIncoming = !!inv.reclass_to_gl && inv.reclass_to_gl !== inv.gl_code;
        html += '<tr>';
        html += '<td style="padding:7px 16px; font-size:12px; white-space:nowrap; border-bottom:1px solid var(--gray-200);">' + (inv.payee_name || inv.payee_code || '—') + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; color:var(--gray-600); border-bottom:1px solid var(--gray-200);">' + (inv.notes || '—') + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; font-family:monospace; border-bottom:1px solid var(--gray-200);">' + (inv.invoice_num || '—') + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; border-bottom:1px solid var(--gray-200);">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; text-align:right; font-size:12px; font-weight:600; font-variant-numeric:tabular-nums; border-bottom:1px solid var(--gray-200);">' + fmtAmt(inv.amount) + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; border-bottom:1px solid var(--gray-200);">' + (inv.check_num || '—') + '</td>';
        html += '<td style="padding:7px 16px; text-align:center; border-bottom:1px solid var(--gray-200);">';
        if (isIncoming) {
            html += '<span style="font-size:11px; color:#15803d;" title="Reclassed into this GL from ' + inv.gl_code + '">↩ from ' + inv.gl_code + '</span> ';
            html += '<button onclick="inlineUndoReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:#fef3c7; color:#92400e; border:1px solid #fcd34d; border-radius:4px; cursor:pointer;">Undo</button>';
        } else {
            html += '<span id="reclass_label_' + inv.id + '" style="font-size:11px; color:var(--gray-500); margin-right:4px;"></span>';
            html += '<input type="hidden" id="reclass_gl_' + inv.id + '" value="">';
            html += '<button onclick="openReclassModal(' + inv.id + ',\'' + glCode + '\',\'inline\')" style="font-size:11px; padding:2px 8px; background:var(--gray-100); color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer;">Reclass to…</button> ';
            html += '<button id="reclass_go_' + inv.id + '" onclick="inlineReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer; display:none;">Go</button>';
        }
        html += '</td></tr>';
    });

    html += '</table></div></td>';
    detailRow.innerHTML = html;
    row.after(detailRow);
}

async function inlineReclass(invoiceId, fromGL) {
    const select = document.getElementById('reclass_gl_' + invoiceId);
    if (!select || !select.value) { alert('Select a target GL code'); return; }
    try {
        const resp = await fetch('/api/expense-dist/reclass/' + invoiceId, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ reclass_to_gl: select.value, reclass_notes: 'Reclassed from PM budget review' })
        });
        if (resp.ok) {
            _expenseCache = null; // Clear cache to refresh
            // Re-toggle to refresh the detail view
            const glLink = document.querySelector('a[onclick*="' + fromGL + '"]');
            if (glLink) { toggleInvoices(fromGL, glLink); setTimeout(() => toggleInvoices(fromGL, glLink), 100); }
            showToast('Invoice reclassified to ' + select.value, 'success');
            // Re-apply YTD adjustments so totals reflect the reclass immediately
            await applyReclassAdjustments();
        } else { showToast('Reclass failed', 'error'); }
    } catch(e) { showToast('Reclass error: ' + e.message, 'error'); }
}

async function inlineUndoReclass(invoiceId, fromGL) {
    try {
        const resp = await fetch('/api/expense-dist/reclass/' + invoiceId, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ reclass_to_gl: '' })
        });
        if (resp.ok) {
            _expenseCache = null;
            const glLink = document.querySelector('a[onclick*="' + fromGL + '"]');
            if (glLink) { toggleInvoices(fromGL, glLink); setTimeout(() => toggleInvoices(fromGL, glLink), 100); }
            showToast('Reclass undone', 'success');
            // Re-apply YTD adjustments so totals reflect the undo immediately
            await applyReclassAdjustments();
        } else { showToast('Undo failed', 'error'); }
    } catch(e) { showToast('Undo error: ' + e.message, 'error'); }
}

// Legacy stub — now uses pmLineChanged for cascade system
function pmUpdateNoteWarn(gl) {
    const noteEl = document.querySelector('.col-notes input[data-gl="' + gl + '"]');
    if (!noteEl) return;
    const pctEl = document.getElementById('pm_pct_' + gl);
    const pct = pctEl ? (parseFloat(pctEl.dataset.raw) || 0) : 0;
    const hasNote = (noteEl.value || '').trim().length > 0;
    if (Math.abs(pct) > 0.10 && !hasNote) {
        noteEl.classList.add('note-warn');
    } else {
        noteEl.classList.remove('note-warn');
    }
}

function onInput(el) {
    const gl = el.dataset.gl;
    const field = el.dataset.field;
    const line = LINES.find(l => l.gl_code === gl);
    console.log('[onInput] gl=', gl, 'field=', field, 'value=', el.value, 'lineFound=', !!line);
    if (!line) { console.warn('[onInput] no line for gl', gl); return; }

    if (field === 'increase_pct') {
        line.increase_pct = parseFloat(el.value) / 100 || 0;
    } else if (field === 'accrual_adj') {
        line.accrual_adj = parseFloat(el.value) || 0;
    } else if (field === 'unpaid_bills') {
        line.unpaid_bills = parseFloat(el.value) || 0;
    } else if (field === 'notes') {
        line.notes = el.value;
        console.log('[onInput] notes set on line', gl, '→', line.notes);
        pmUpdateNoteWarn(gl);
        // FA dir 2026-05-18: refresh "My Notes" panel so the row's note shows
        // up there immediately (and the count updates). Debounce with a short
        // timer so we don't re-render on every keystroke.
        clearTimeout(window._pmMyNotesRefreshTimer);
        window._pmMyNotesRefreshTimer = setTimeout(() => {
            try { populateMyChanges(); } catch (e) { console.warn('populateMyChanges error', e); }
        }, 400);
    } else if (field === 'category') {
        line.category = el.value;
    }

    pmLineChanged(gl, field, el.value);
}

async function saveAll() {
    indicator.textContent = 'Saving...';
    indicator.className = 'save-indicator saving';
    try {
        const payload = LINES.map(l => {
            const item = {
                gl_code: l.gl_code,
                increase_pct: l.increase_pct || 0,
                // FA directive 2026-05-11: either-or with increase_pct. NULL means
                // PM is on the % path; a number means PM entered $.
                increase_dollar: (l.increase_dollar !== null && l.increase_dollar !== undefined && l.increase_dollar !== '')
                                  ? parseFloat(l.increase_dollar) : null,
                accrual_adj: l.accrual_adj || 0,
                unpaid_bills: l.unpaid_bills || 0,
                notes: l.notes || '',
                category: l.category || '',
                estimate_override: l.estimate_override !== null && l.estimate_override !== undefined ? l.estimate_override : null,
                forecast_override: l.forecast_override !== null && l.forecast_override !== undefined ? l.forecast_override : null,
                // FA dir 2026-05-18: single-entry — null means "PM hasn't entered",
                // distinguishable from explicit 0. Backend now accepts null/empty.
                proposed_budget: (l.proposed_budget !== null && l.proposed_budget !== undefined && l.proposed_budget !== '')
                                  ? parseFloat(l.proposed_budget) : null,
                proposed_formula: l.proposed_formula || null,
                prior_year: l.prior_year || 0,
                ytd_actual: l._db_ytd_actual !== undefined ? l._db_ytd_actual : (l.ytd_actual || 0),
                ytd_budget: l.ytd_budget || 0,
                current_budget: l.current_budget || 0
            };
            // FA directive 2026-05-11: only include pm_action when the PM
            // actually acted on this line. Backend stamps pm_review_state
            // ONLY when this key is present — without it, saveAll's blanket
            // increase_pct=0 would false-stamp every untouched line.
            if (l._pm_action) item.pm_action = l._pm_action;
            return item;
        });
        const linesWithNotes = payload.filter(p => p.notes && p.notes.trim().length > 0);
        console.log('[saveAll] PUT /api/lines/' + ENTITY + ' lines=' + payload.length + ' withNotes=' + linesWithNotes.length, linesWithNotes.map(l => ({gl: l.gl_code, notes: l.notes})));
        const resp = await fetch('/api/lines/' + ENTITY, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({lines: payload})
        });
        console.log('[saveAll] response status=', resp.status, resp.statusText);
        if (resp.ok) {
            const body = await resp.json().catch(() => ({}));
            console.log('[saveAll] response body=', body);
            // FA directive 2026-05-11: server consumed any pm_action signals,
            // so clear the per-line flags before the next save batch builds.
            LINES.forEach(l => { if (l._pm_action) l._pm_action = null; });
            indicator.textContent = 'Saved';
            indicator.className = 'save-indicator saved';
            indicator.onclick = null;
            setTimeout(() => { indicator.textContent = ''; indicator.className = 'save-indicator'; }, 2000);
        } else {
            const errBody = await resp.text().catch(() => '');
            console.error('[saveAll] FAILED status=', resp.status, 'body=', errBody);
            indicator.textContent = 'Save failed (' + resp.status + ') — click to retry';
            indicator.className = 'save-indicator failed';
            indicator.onclick = () => saveAll();
            alert('Save failed: HTTP ' + resp.status + '\n\n' + errBody);
        }
    } catch(e) {
        console.error('[saveAll] EXCEPTION', e);
        indicator.textContent = 'Save error — click to retry';
        indicator.className = 'save-indicator failed';
        indicator.onclick = () => saveAll();
        alert('Save error: ' + e.message);
    }
}

async function submitForReview() {
    // FA directive 2026-05-11: client-side R&M review gate.
    // Block submit if any R&M line is unreviewed. Server enforces the same
    // gate (defense in depth), but the client check gives instant feedback
    // and auto-jumps the PM to the first unreviewed line so they can act
    // without hunting.
    if (BUDGET_STATUS !== 'fa_review') {
        const c = _pmRmCounts();
        if (c.unreviewed > 0) {
            showToast(
                c.unreviewed + ' R&M line' +
                (c.unreviewed === 1 ? '' : 's') +
                ' still need review before submit. Each line needs a % ' +
                'or $ value (or click "No change").',
                'error');
            pmRmJumpToNext();
            return;
        }
    }

    // Save first
    await saveAll();

    // If the budget is already in fa_review (PM re-entered to tweak), just
    // save — no status transition needed and the server won't allow fa→fa.
    if (BUDGET_STATUS === 'fa_review') {
        if (!confirm('Save changes and return to portal? (Already submitted for FA review.)')) return;
        showToast('Changes saved.', 'success');
        setTimeout(() => { window.location.href = '/pm'; }, 800);
        return;
    }

    if (!confirm('Submit this budget for FA review?')) return;

    const resp = await fetch('/api/budgets/' + ENTITY + '/status', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({status: 'fa_review'})
    });
    if (resp.ok) {
        showToast('Submitted for FA review!', 'success');
        setTimeout(() => { window.location.href = '/pm'; }, 1000);
    } else if (resp.status === 422) {
        // Server-side gate caught what the client missed (race condition
        // where another tab cleared a state, or stale page state).
        const err = await resp.json().catch(() => ({}));
        showToast(err.message || 'R&M review incomplete — refresh and try again.', 'error');
    } else {
        const err = await resp.json();
        showToast('Error: ' + (err.error || 'Unknown'), 'error');
    }
}

// ── Searchable Reclass Modal ─────────────────────────────────────────
let _reclassCallback = null;

function openReclassModal(invoiceIdOrGl, fromGL, mode) {
    // mode: 'inline' (invoice-level) or 'line' (GL-level)
    _reclassCallback = { id: invoiceIdOrGl, fromGL: fromGL, mode: mode };

    // Build modal HTML
    let overlay = document.getElementById('reclassOverlay');
    if (overlay) overlay.remove();

    // Group ALL_GL_CODES by category, sorted by gl_code
    const cats = {};
    const catOrder = [];
    ALL_GL_CODES.filter(g => g.gl_code !== fromGL).forEach(g => {
        const cat = g.category || 'other';
        if (!cats[cat]) { cats[cat] = []; catOrder.push(cat); }
        cats[cat].push(g);
    });
    // Sort each category's GLs
    catOrder.forEach(c => cats[c].sort((a,b) => a.gl_code.localeCompare(b.gl_code)));
    catOrder.sort();

    // Build category label map
    const catLabels = {supplies:'Supplies',repairs:'Repairs',maintenance:'Maintenance',payroll:'Payroll',electric:'Electric',gas:'Gas',fuel:'Fuel',water:'Water & Sewer',sewer:'Water & Sewer',insurance:'Insurance',re_taxes:'Real Estate Taxes',professional:'Professional Fees',admin:'Administrative',financial:'Financial',income:'Income',other:'Other'};

    let listHtml = '';
    catOrder.forEach(cat => {
        listHtml += '<div class="rm-cat-header">' + (catLabels[cat] || cat) + '</div>';
        cats[cat].forEach(g => {
            listHtml += '<div class="rm-gl-row" data-gl="' + g.gl_code + '" data-desc="' + (g.description || '').toLowerCase() + '" data-cat="' + cat + '" onclick="selectReclassGL(\'' + g.gl_code + '\',\'' + g.description.replace(/'/g, "\\'") + '\')">';
            listHtml += '<span class="gl-code">' + g.gl_code + '</span>';
            listHtml += '<span class="gl-desc">' + (g.description || '') + '</span>';
            listHtml += '</div>';
        });
    });

    overlay = document.createElement('div');
    overlay.id = 'reclassOverlay';
    overlay.className = 'reclass-overlay';
    overlay.innerHTML = `
        <div class="reclass-modal">
            <div class="rm-header">
                <h3>Select Target GL Code</h3>
                <button onclick="document.getElementById('reclassOverlay').remove()" style="background:none; border:none; font-size:18px; cursor:pointer; color:var(--gray-500);">✕</button>
            </div>
            <div class="rm-search">
                <input type="text" id="reclassSearch" placeholder="Search by GL code, name, or category…" oninput="filterReclassModal(this.value)" autofocus>
            </div>
            <div class="rm-list" id="reclassListContainer">${listHtml}</div>
            <div class="rm-footer">
                <span style="font-size:12px; color:var(--gray-500);">${ALL_GL_CODES.length} GL codes available</span>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);

    // Close on overlay click
    overlay.addEventListener('click', function(e) { if (e.target === overlay) overlay.remove(); });

    // Focus search
    setTimeout(() => document.getElementById('reclassSearch').focus(), 50);
}

function filterReclassModal(q) {
    q = q.toLowerCase();
    const container = document.getElementById('reclassListContainer');
    const rows = container.querySelectorAll('.rm-gl-row');
    const catHeaders = container.querySelectorAll('.rm-cat-header');
    const catVisible = {};

    rows.forEach(r => {
        const gl = r.dataset.gl.toLowerCase();
        const desc = r.dataset.desc;
        const cat = r.dataset.cat;
        const match = !q || gl.includes(q) || desc.includes(q) || (cat && cat.includes(q));
        r.style.display = match ? '' : 'none';
        if (match) catVisible[cat] = true;
    });

    catHeaders.forEach(h => {
        const catName = h.textContent.toLowerCase();
        // Show cat header if any child matches
        const nextRows = [];
        let sib = h.nextElementSibling;
        while (sib && !sib.classList.contains('rm-cat-header')) { nextRows.push(sib); sib = sib.nextElementSibling; }
        const anyVisible = nextRows.some(r => r.style.display !== 'none');
        h.style.display = anyVisible ? '' : 'none';
    });
}

function selectReclassGL(glCode, glDesc) {
    if (!_reclassCallback) return;
    const cb = _reclassCallback;

    if (cb.mode === 'inline') {
        // Set hidden input and show label
        const hidden = document.getElementById('reclass_gl_' + cb.id);
        const label = document.getElementById('reclass_label_' + cb.id);
        const goBtn = document.getElementById('reclass_go_' + cb.id);
        if (hidden) hidden.value = glCode;
        if (label) { label.textContent = '→ ' + glCode; label.style.color = 'var(--blue)'; label.style.fontWeight = '600'; }
        if (goBtn) goBtn.style.display = '';
    } else if (cb.mode === 'line') {
        // Set the hidden input for line-level reclass
        const hidden = document.getElementById('reclass_target_' + cb.fromGL);
        const label = document.getElementById('reclass_target_label_' + cb.fromGL);
        if (hidden) hidden.value = glCode;
        if (label) { label.textContent = glCode + ' — ' + glDesc; label.style.color = 'var(--blue)'; label.style.fontWeight = '600'; }
    }

    document.getElementById('reclassOverlay').remove();
}

// Line-level reclass suggestion
function showReclass(glCode) {
    const line = LINES.find(l => l.gl_code === glCode);
    if (!line) return;

    const row = document.querySelector(`[data-gl="${glCode}"]`).closest('tr');
    const existing = row.nextElementSibling;
    if (existing && existing.classList.contains('reclass-form-row')) {
        existing.remove();
        return;
    }

    const formRow = document.createElement('tr');
    formRow.className = 'reclass-form-row';
    formRow.innerHTML = `
        <td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="13" style="padding:0;">
            <div class="drill-sticky" style="padding:12px 24px; background:var(--blue-light); border-left:3px solid var(--blue);"><div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
                <label style="font-size:12px; font-weight:600;">Suggest reclass to:</label>
                <input type="hidden" id="reclass_target_${glCode}" value="">
                <span id="reclass_target_label_${glCode}" style="font-size:12px; color:var(--gray-500);">No GL selected</span>
                <button onclick="openReclassModal('${glCode}','${glCode}','line')" style="font-size:12px; padding:4px 12px; background:var(--gray-100); color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer;">Choose GL…</button>
                <input type="number" id="reclass_amount_${glCode}" placeholder="Amount" step="1" value="${Math.round(line.current_budget || 0)}"
                       style="width:100px; font-size:12px; padding:4px 8px; border:1px solid var(--gray-300); border-radius:4px;">
                <input type="text" id="reclass_notes_${glCode}" placeholder="Notes for FA" value="${line.reclass_notes || ''}"
                       style="width:200px; font-size:12px; padding:4px 8px; border:1px solid var(--gray-300); border-radius:4px;">
                <button onclick="saveReclass('${glCode}')" style="font-size:12px; padding:4px 12px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer;">Save</button>
                <button onclick="this.closest('tr').remove()" style="font-size:12px; padding:4px 12px; background:var(--gray-200); border:none; border-radius:4px; cursor:pointer;">Cancel</button>
            </div></div>
        </td>
    `;
    row.after(formRow);
}

async function saveReclass(glCode) {
    const target = document.getElementById('reclass_target_' + glCode).value;
    const amount = document.getElementById('reclass_amount_' + glCode).value;
    const notes = document.getElementById('reclass_notes_' + glCode).value;

    if (!target) { alert('Select a target GL code'); return; }

    const resp = await fetch('/api/lines/' + ENTITY + '/reclass', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({gl_code: glCode, reclass_to_gl: target, reclass_amount: parseFloat(amount) || 0, reclass_notes: notes})
    });

    if (resp.ok) {
        // Update local data and re-render
        const line = LINES.find(l => l.gl_code === glCode);
        if (line) {
            line.reclass_to_gl = target;
            line.reclass_amount = parseFloat(amount) || 0;
            line.reclass_notes = notes;
        }
        renderTable();
        updateZeroToggle();
    } else {
        alert('Error saving reclass suggestion');
    }
}

// Disable submit button if not editable
if (!CAN_EDIT) {
    const btn = document.getElementById('submitBtn');
    if (btn) btn.disabled = true;
}

// Adjust LINES ytd_actual based on invoice-level reclasses (frontend only — DB unchanged until FA accepts)
// Store original DB values so saveAll() never writes adjusted figures back
LINES.forEach(l => { l._db_ytd_actual = l.ytd_actual || 0; });

async function applyReclassAdjustments() {
    try {
        const data = await fetchExpenseData();
        if (!data || !data.gl_groups) { renderTable(); updateZeroToggle(); return; }

        // Flatten all invoices and find reclassed ones
        const adjustments = {};  // gl_code -> net adjustment
        data.gl_groups.forEach(g => {
            if (!g.invoices) return;
            g.invoices.forEach(inv => {
                if (!inv.reclass_to_gl || inv.reclass_to_gl === inv.gl_code) return;
                const amt = inv.amount || 0;
                // Subtract from original GL
                adjustments[inv.gl_code] = (adjustments[inv.gl_code] || 0) - amt;
                // Add to target GL
                adjustments[inv.reclass_to_gl] = (adjustments[inv.reclass_to_gl] || 0) + amt;
            });
        });

        // Reset all to DB values first, then apply adjustments
        LINES.forEach(l => { l.ytd_actual = l._db_ytd_actual; });
        if (Object.keys(adjustments).length > 0) {
            LINES.forEach(l => {
                if (adjustments[l.gl_code]) {
                    l.ytd_actual = l._db_ytd_actual + adjustments[l.gl_code];
                }
            });
        }
    } catch(e) {
        // If expense data fails to load, just render with original figures
    }
    renderTable();
    updateZeroToggle();
    // FA directive 2026-05-11: initialize R&M progress strip + submit gate
    // on page load so the PM sees the state without clicking anything.
    if (typeof _pmRmUpdateProgress === 'function') _pmRmUpdateProgress();
    if (typeof _pmRmUpdateSubmitGate === 'function') _pmRmUpdateSubmitGate();
}
applyReclassAdjustments();

// ─── My Changes panel (read-only summary) ───────────────────────────────
function switchPmMcTab(button, tabId) {
  document.getElementById('pmMyNotesContent').style.display = 'none';
  document.getElementById('pmMyReclassContent').style.display = 'none';
  document.querySelectorAll('#pmMyChangesTabs .pm-mc-tab').forEach(t => {
    t.style.color = 'var(--gray-500)';
    t.style.borderBottom = '2px solid transparent';
    t.style.background = 'transparent';
  });
  document.getElementById(tabId).style.display = 'block';
  button.style.color = 'var(--blue)';
  button.style.borderBottom = '2px solid var(--blue)';
  button.style.background = 'white';
}

// FA dir 2026-05-18: was an IIFE that ran once on page load, so the "My Notes"
// panel stayed stale when the PM typed notes into rows. Now a named function
// that onInput() calls whenever a notes field changes (debounced through
// pmLineChanged's save timer cadence).
async function populateMyChanges() {
  let totalItems = 0;
  const panel = document.getElementById('pmMyChangesPanel');

  // Tab 1: My Notes
  const linesWithNotes = LINES.filter(l => l.notes && l.notes.trim().length > 0);
  const notesContainer = document.getElementById('pmMyNotesContainer');
  const notesEmpty = document.getElementById('pmMyNotesEmpty');
  const notesCount = document.getElementById('pmMyNotesCount');

  if (linesWithNotes.length > 0) {
    notesEmpty.style.display = 'none';
    notesCount.textContent = linesWithNotes.length;
    notesContainer.innerHTML = linesWithNotes.map(l => {
      // Split notes into PM notes vs FA responses
      const parts = (l.notes || '').split('\n');
      let pmHtml = '';
      let faHtml = '';
      parts.forEach(p => {
        if (p.match(/^\[FA (REJECTED|COMMENT|ACCEPTED)/)) {
          faHtml += '<div style="flex:1; font-size:12px; color:var(--gray-600); background:#f0f4ff; padding:6px 10px; border-radius:6px; border-left:3px solid var(--blue); margin-top:4px;">' +
            '<strong>FA Response:</strong> ' + p + '</div>';
        } else if (p.trim()) {
          pmHtml += (pmHtml ? '<br>' : '') + p;
        }
      });
      return '<div style="display:flex; align-items:flex-start; gap:12px; padding:10px 12px; border-radius:8px; margin-bottom:6px;" onmouseover="this.style.background=\'var(--gray-50)\'" onmouseout="this.style.background=\'\'">' +
        '<span style="font-family:monospace; font-size:12px; font-weight:600; color:var(--blue); background:var(--blue-light); padding:3px 8px; border-radius:4px; white-space:nowrap;">' + l.gl_code + '</span>' +
        '<span style="font-size:12px; color:var(--gray-500); min-width:140px;">' + (l.description || '') + '</span>' +
        '<div style="flex:1;">' +
          (pmHtml ? '<div style="font-size:13px; color:var(--gray-700); background:#fffbeb; padding:6px 10px; border-radius:6px; border-left:3px solid #fbbf24;">' + pmHtml + '</div>' : '') +
          faHtml +
        '</div>' +
      '</div>';
    }).join('');
    totalItems += linesWithNotes.length;
  } else {
    notesEmpty.style.display = '';
    notesContainer.innerHTML = '';
    notesCount.textContent = '0';
  }

  // Tab 2: My Reclasses
  const reclassCount = document.getElementById('pmMyReclassCount');
  const reclassBody = document.getElementById('pmMyReclassBody');
  const reclassEmpty = document.getElementById('pmMyReclassEmpty');

  const expData = await fetchExpenseData();
  if (expData && expData.gl_groups) {
    const allInvoices = [];
    expData.gl_groups.forEach(g => {
      if (g.invoices) g.invoices.forEach(inv => allInvoices.push(inv));
    });
    const reclassed = allInvoices.filter(inv => inv.reclass_to_gl);

    const reclassMap = {};
    reclassed.forEach(inv => {
      const key = inv.gl_code + '|' + inv.reclass_to_gl;
      if (!reclassMap[key]) {
        reclassMap[key] = { from_gl: inv.gl_code, to_gl: inv.reclass_to_gl, invoices: [], total: 0, notes: '' };
      }
      reclassMap[key].invoices.push(inv);
      reclassMap[key].total += inv.amount || 0;
      if (inv.reclass_notes && !reclassMap[key].notes) reclassMap[key].notes = inv.reclass_notes;
    });
    const groups = Object.values(reclassMap);

    if (groups.length > 0) {
      reclassEmpty.style.display = 'none';
      reclassCount.textContent = groups.length;
      reclassBody.innerHTML = '';
      groups.forEach((g, gi) => {
        const fromLine = LINES.find(l => l.gl_code === g.from_gl);
        const fromDesc = fromLine ? fromLine.description : '';
        const toLine = LINES.find(l => l.gl_code === g.to_gl);
        const toDesc = toLine ? toLine.description : '';
        const gid = 'pmrg_' + gi;

        // Determine FA status from the notes of the from_gl line
        let faStatus = '<span style="background:#fff7ed; color:#b45309; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">● Pending</span>';
        const fromNotes = fromLine ? (fromLine.notes || '') : '';
        if (fromNotes.includes('[FA ACCEPTED')) faStatus = '<span style="background:#dcfce7; color:#166534; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✓ Accepted</span>';

        const tr = document.createElement('tr');
        tr.style.cssText = 'transition:background 0.15s; cursor:pointer;';
        tr.onmouseover = function() { this.style.background='var(--gray-50)'; };
        tr.onmouseout = function() { this.style.background=''; };
        tr.onclick = function() { pmToggleReclassInv(gid); };
        tr.innerHTML =
          '<td style="padding:10px;"><span id="' + gid + '_arrow" style="display:inline-block; font-size:10px; color:var(--gray-500); transition:transform 0.2s; margin-right:6px;">▶</span><span style="font-family:monospace; font-size:12px; font-weight:700;">' + g.from_gl + '</span><div style="padding-left:20px; font-size:11px; color:var(--gray-500);">' + fromDesc + '</div></td>' +
          '<td style="padding:10px 4px; color:var(--orange); font-weight:700; font-size:16px;">→</td>' +
          '<td style="padding:10px;"><span style="font-family:monospace; font-size:12px; font-weight:700;">' + g.to_gl + '</span><div style="font-size:11px; color:var(--gray-500);">' + toDesc + '</div></td>' +
          '<td style="padding:10px;"><span style="font-size:11px; background:var(--orange-light); color:var(--orange); padding:2px 8px; border-radius:10px; font-weight:600;">' + g.invoices.length + ' invoice' + (g.invoices.length !== 1 ? 's' : '') + '</span></td>' +
          '<td style="padding:10px; text-align:right; font-weight:600; font-variant-numeric:tabular-nums;">' + fmt(g.total) + '</td>' +
          '<td style="padding:10px; font-size:12px; color:var(--gray-600); font-style:italic; max-width:200px;">' + (g.notes ? '"' + g.notes + '"' : '') + '</td>' +
          '<td style="padding:10px; text-align:center;">' + faStatus + '</td>';
        reclassBody.appendChild(tr);
        // Expandable invoice detail rows
        g.invoices.forEach(inv => {
          const itr = document.createElement('tr');
          itr.dataset.group = gid;
          itr.style.cssText = 'display:none; background:#fafbfc;';
          const invDate = inv.invoice_date || inv.date || '';
          const cleanDate = invDate ? invDate.split('T')[0] : '';
          const invNum = inv.invoice_num || inv.invoice_number || inv.ref || '';
          const invVendor = inv.payee_name || inv.vendor_name || inv.vendor || '';
          const invDesc = inv.notes || inv.description || '';
          const toGlName = (LINES.find(l => l.gl_code === inv.reclass_to_gl) || {}).description || inv.reclass_to_gl;
          itr.innerHTML =
            '<td colspan="7" style="padding:8px 10px 8px 44px; border-bottom:1px solid #f0f1f3;">' +
              '<div style="display:flex; align-items:center; gap:12px; font-size:12px; flex-wrap:wrap;">' +
                (invNum ? '<span style="font-family:monospace; font-size:11px; color:var(--gray-400); background:#f3f4f6; padding:1px 6px; border-radius:3px;">' + invNum + '</span>' : '') +
                '<span style="font-weight:600; color:var(--gray-700);">' + invVendor + '</span>' +
                (invDesc ? '<span style="color:var(--gray-500);">— ' + invDesc + '</span>' : '') +
                (cleanDate ? '<span style="font-size:11px; color:var(--gray-400);">' + cleanDate + '</span>' : '') +
                '<span style="font-size:11px; color:var(--orange);">→ ' + toGlName + '</span>' +
                '<span style="margin-left:auto; font-weight:600; font-variant-numeric:tabular-nums; text-align:right;">' + fmt(inv.amount || 0) + '</span>' +
                '<button onclick="event.stopPropagation(); undoSingleReclass(' + inv.id + ',\'' + g.from_gl + '\',\'' + g.to_gl + '\',this)" style="margin-left:8px; padding:2px 8px; font-size:10px; font-weight:600; border-radius:4px; cursor:pointer; background:white; color:var(--gray-500); border:1px solid var(--gray-300);">Undo</button>' +
              '</div>' +
            '</td>';
          reclassBody.appendChild(itr);
        });
      });
      totalItems += groups.length;
    } else {
      reclassEmpty.style.display = '';
      reclassBody.innerHTML = '';
      reclassCount.textContent = '0';
    }
  } else {
    reclassEmpty.style.display = '';
    reclassBody.innerHTML = '';
    reclassCount.textContent = '0';
  }

  // Show panel if there are items; hide otherwise so the panel doesn't linger
  // empty after a PM clears all notes.
  if (totalItems > 0) {
    panel.style.display = '';
    document.getElementById('pmMyChangesBadge').textContent = totalItems + ' item' + (totalItems !== 1 ? 's' : '');
  } else {
    panel.style.display = 'none';
    document.getElementById('pmMyChangesBadge').textContent = '';
  }
}
// Initial render on page load.
populateMyChanges();

async function undoSingleReclass(invId, fromGl, toGl, btn) {
  if (!confirm('Undo this invoice reclass?')) return;
  try {
    await fetch('/api/expense-dist/reclass/' + invId, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ reclass_to_gl: '' })
    });
    const row = btn.closest('tr');
    if (row) { row.style.opacity = '0.3'; row.style.pointerEvents = 'none'; }
    btn.textContent = 'Undone';
    btn.disabled = true;
    showToast('Invoice restored to ' + fromGl, 'success');
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

function pmToggleReclassInv(gid) {
  const rows = document.querySelectorAll('tr[data-group="' + gid + '"]');
  const arrow = document.getElementById(gid + '_arrow');
  if (!rows.length) return;
  const showing = rows[0].style.display !== 'none';
  rows.forEach(r => { r.style.display = showing ? 'none' : ''; });
  if (arrow) arrow.style.transform = showing ? '' : 'rotate(90deg)';
}
</script>
</body>
</html>
"""


# Board Presentation redesign (2026-07-01): client-facing template, rendered
# ONLY from a frozen PresentationSession.snapshot_data (see board_notice_view).
# Ported from the reviewed/approved design mockup (design-shotgun session,
# "Formal & Institutional" concept, plan: "Client Board Presentation").
