# Extracted from workflow.py 2026-07-05 (clean-architecture tranche 1).
# BYTE-IDENTICAL constant — template edits happen HERE now. Keep the
# string style unchanged (raw vs non-raw matters for JS escapes; see
# the wizard-template-js-escapes memory / check_template_js gate).

DASHBOARD_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FA Dashboard - Century Management</title>
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
    --red: #e02424;
    --red-light: #fde8e8;
    --yellow: #f59e0b;
    --yellow-light: #fef3c7;
    --orange: #f97316;
    --orange-light: #fed7aa;
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
  .top-nav {
    background: white;
    border-bottom: 1px solid var(--gray-200);
    padding: 0 20px;
    display: flex;
    align-items: center;
    height: 48px;
    position: sticky;
    top: 0;
    z-index: 100;
  }
  .top-nav .nav-brand {
    font-weight: 700;
    font-size: 15px;
    color: var(--blue);
    text-decoration: none;
    margin-right: 32px;
  }
  .top-nav .nav-links { display: flex; gap: 4px; }
  .top-nav .nav-link {
    padding: 6px 14px;
    font-size: 13px;
    font-weight: 500;
    color: var(--gray-500);
    text-decoration: none;
    border-radius: 6px;
    transition: all 0.15s;
  }
  .top-nav .nav-link:hover { background: var(--gray-100); color: var(--gray-900); }
  .top-nav .nav-link.active { background: var(--blue-light); color: var(--blue); }

  /* ── Toast notifications ── */
  .toast-container { position: fixed; top: 60px; right: 20px; z-index: 9999; display: flex; flex-direction: column; gap: 8px; }
  .toast {
    padding: 12px 20px;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 500;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    animation: slideIn 0.3s ease;
    max-width: 360px;
  }
  .toast-success { background: var(--green); color: white; }
  .toast-error { background: var(--red); color: white; }
  .toast-info { background: var(--blue); color: white; }
  @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }

  header {
    background: linear-gradient(135deg, var(--blue) 0%, var(--blue-dark) 100%);
    color: white;
    padding: 30px 20px;
  }
  header h1 {
    font-size: 28px;
    font-weight: 700;
    margin-bottom: 4px;
  }
  header p { font-size: 14px; opacity: 0.85; }
  .container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 32px 20px;
  }
  .status-summary {
    /* Design cleanup (2026-06-10, Jacob: "way too busy"): was a grid of six
       full-size cards (mostly zeros); now a single quiet line. */
    display: block;
    margin-bottom: 10px;
  }
  .status-card {
    background: white;
    border-radius: 12px;
    padding: 24px;
    border: 1px solid var(--gray-200);
    text-align: center;
  }
  .status-card .count {
    font-size: 32px;
    font-weight: 700;
    margin-bottom: 8px;
    color: var(--blue);
  }
  .status-card .label {
    font-size: 12px;
    color: var(--gray-500);
    text-transform: uppercase;
    font-weight: 600;
  }
  .section {
    background: white;
    border-radius: 12px;
    padding: 32px;
    border: 1px solid var(--gray-200);
  }
  .section h2 {
    font-size: 20px;
    font-weight: 600;
    margin-bottom: 24px;
    color: var(--blue);
  }
  table {
    width: 100%;
    border-collapse: collapse;
  }
  th {
    background: var(--gray-100);
    padding: 12px;
    text-align: left;
    font-weight: 600;
    font-size: 13px;
    border-bottom: 1px solid var(--gray-200);
  }
  td {
    padding: 12px;
    border-bottom: 1px solid var(--gray-200);
  }
  tr:hover {
    background: var(--gray-50);
  }
  .pill {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 600;
  }
  .pill-draft {
    background: var(--gray-100);
    color: var(--gray-700);
  }
  .pill-pm_pending {
    background: var(--yellow-light);
    color: #a16207;
  }
  .pill-pm_in_progress {
    background: var(--blue-light);
    color: var(--blue);
  }
  /* FA dir 2026-05-22 (B2): per-source mini-tile data status. */
  .ds-tiles {
    display: inline-flex;
    gap: 4px;
    align-items: center;
  }
  .ds-tile {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-width: 32px;
    height: 30px;
    padding: 2px 4px;
    border-radius: 5px;
    border: 1px solid transparent;
    text-decoration: none;
    font-family: 'Plus Jakarta Sans', sans-serif;
    cursor: pointer;
    line-height: 1;
    transition: transform 0.1s, box-shadow 0.1s;
  }
  .ds-tile:hover {
    transform: translateY(-1px);
    box-shadow: 0 2px 4px rgba(0,0,0,0.08);
  }
  .ds-tile .t-letter {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.5px;
  }
  .ds-tile .t-dt {
    font-size: 9px;
    margin-top: 1px;
    opacity: 0.75;
    font-variant-numeric: tabular-nums;
  }
  /* Tile glyph (2026-06-10, Jacob-approved design review fix): every state
     carries a non-color mark (✓ ! ◷ ✕ –) so tiles read without relying on
     hue alone — and the two ambers below stop being the same color with
     two opposite meanings. */
  .ds-tile .t-glyph {
    font-size: 10px;
    margin-left: 2px;
    font-weight: 700;
  }
  .ds-tile.ok {
    background: #def7ec;
    color: #065f46;
    border-color: #a7f3d0;
  }
  .ds-tile.miss {
    background: #fef2f2;
    color: #991b1b;
    border-color: #fecaca;
  }
  /* act = SOLID amber: a human action is pending (audit extract / confirm).
     ready = PALE OUTLINED amber: file arrived, the system loads it
     automatically — no action. Previously both rendered identical amber. */
  .ds-tile.act {
    background: #f59e0b;
    color: #451a03;
    border-color: #b45309;
  }
  .ds-tile.ready {
    background: #fffdf4;
    color: #92400e;
    border: 1.5px solid #d9a23b;
  }
  .ds-tile.setup {
    background: #f1f0ec;
    color: #7d7468;
    border-color: #e0dcd2;
  }
  /* Green tile, amber ring: in the budget BUT a newer file is in SharePoint
     than what was ingested (sub=newer_in_sp). Click to re-ingest. */
  .ds-tile.ok.stale {
    border: 1.5px solid #d97706;
  }
  .pill-fa_review {
    background: var(--orange-light);
    color: var(--orange);
  }
  .pill-approved {
    background: var(--green-light);
    color: var(--green);
  }
  .pill-returned {
    background: var(--red-light);
    color: var(--red);
  }
  /* ── Loading spinner ── */
  .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--gray-200); border-top-color: var(--blue); border-radius: 50%; animation: spin 0.6s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .loading-overlay { text-align: center; padding: 60px 20px; color: var(--gray-500); }

  /* ── Action buttons ── */
  .btn-action {
    padding: 6px 14px;
    border: none;
    border-radius: 6px;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.15s;
  }
  .btn-action:hover { filter: brightness(0.9); }
  .btn-blue { background: var(--blue); color: white; }
  .btn-green { background: var(--green); color: white; }
  .btn-orange { background: var(--yellow); color: white; }
  .action-menu { position: relative; display: inline-block; }
  .action-menu-btn { background: transparent; border: 1px solid var(--gray-300); border-radius: 6px; padding: 4px 10px; cursor: pointer; font-size: 16px; line-height: 1; color: var(--gray-500); }
  .action-menu-btn:hover { background: var(--gray-100); }
  .action-menu-items { display: none; position: absolute; right: 0; top: 100%; margin-top: 4px; background: white; border: 1px solid var(--gray-200); border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); min-width: 140px; z-index: 10; padding: 4px 0; }
  .action-menu-items button { display: block; width: 100%; text-align: left; padding: 8px 14px; border: none; background: none; cursor: pointer; font-size: 13px; }
  .action-menu-items button:hover { background: var(--gray-50); }
  .action-menu-items .del-item { color: var(--red); }
  .action-menu-items .del-item:hover { background: var(--red-light); }
  /* ── Option C "Refined Ledger" (Jacob 2026-07-08) ─────────────────────
     File bar: six fixed-order segments (B E Y A M Au) replace the letter
     tiles. Color = state, hover names the source + detail, click keeps the
     exact same jump targets the tiles had. Same shared source_states brain. */
  .fbar { display: inline-flex; gap: 2px; }
  .fbar .fseg { display: inline-flex; align-items: center; justify-content: center; min-width: 17px; height: 16px; border-radius: 3px; font-size: 8.5px; font-weight: 800; text-decoration: none; padding: 0 2px; }
  .fbar .fseg:hover { filter: brightness(0.93); }
  .fseg.fs-ok { background: #def7ec; color: #057a55; }
  .fseg.fs-ok.fs-stale { box-shadow: inset 0 0 0 1.5px #d97706; color: #b45309; }
  .fseg.fs-act { background: #fef3c7; color: #b45309; box-shadow: inset 0 0 0 1.5px #f59e0b; }
  .fseg.fs-ready { background: #fef3c7; color: #92400e; }
  .fseg.fs-miss { background: #fde8e8; color: #e02424; }
  .fseg.fs-fail { background: #e02424; color: #fff; }
  .fseg.fs-setup { background: #ede9e1; color: #a39a8b; }
  /* Tier group header rows — pure presentation over the readiness sort. */
  tr.grp-row td { font-size: 10px; font-weight: 800; letter-spacing: 0.06em; padding: 5px 12px; text-transform: uppercase; }
</style>
</head>
<body>

<!-- Global Nav -->
<nav class="top-nav">
  <a href="/" class="nav-brand">Century Management</a>
  <div class="nav-links">
    <a href="/" class="nav-link">Home</a>
    <a href="/wizard" class="nav-link" style="color:#f59e0b;font-weight:600;">⚡ Wizard</a>
    <a href="/dashboard" class="nav-link active">FA Dashboard</a>
    <a href="/pm" class="nav-link">PM Portal</a>
    <a href="/audited-financials" class="nav-link">Audited Financials</a>
    <a href="/admin/login?next=/dashboard" class="nav-link" style="margin-left:auto;font-size:12px;color:var(--gray-500);" title="Sign in with ADMIN_KEY to access admin endpoints">🔑 Admin</a>
  </div>
</nav>

<!-- Toast container -->
<div class="toast-container" id="toastContainer"></div>

<header>
  <h1>FA Dashboard</h1>
  <p>Review and manage building budgets</p>
</header>
<div class="container">
  <!-- Loading state -->
  <div class="loading-overlay" id="loadingState">
    <div class="spinner" style="width:32px; height:32px; border-width:3px; margin:0 auto 12px;"></div>
    <p>Loading budgets...</p>
  </div>

  <div id="dashboardContent" style="display:none;">
    <div class="status-summary" id="status-summary"></div>

    <div class="section">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
        <div onclick="toggleBuildingsCollapse()" style="display:flex; align-items:center; gap:10px; cursor:pointer; user-select:none;" title="Click to collapse/expand">
          <span id="buildingsChevron" style="display:inline-block; transition:transform 0.2s; font-size:12px; color:var(--gray-500);">&#9660;</span>
          <h2 style="margin-bottom:0;">All Buildings</h2>
        </div>
        <input type="text" id="budgetSearch" placeholder="Search buildings..." oninput="filterBudgetTable()"
          style="padding:8px 14px; border:1px solid var(--gray-200); border-radius:8px; font-size:14px; width:260px; outline:none;">
      </div>
      <!-- FA dir 2026-05-23: hero callout — surfaces actionable queue
           ("X ready to build now") at the top of the dashboard. Same as
           the wizard so both pages tell the FA the same story. -->
      <div id="readinessHero" style="display:none; margin-bottom:12px;"></div>
      <!-- Readiness-tier filter chips — clicking applies a filter on the table -->
      <div id="readinessChips" style="display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-bottom:12px;">
        <!-- Populated by renderReadinessChips() in JS (label dropped
             2026-06-10 — the chips are self-evident) -->
      </div>
      <!-- Design cleanup (2026-06-10, Jacob: "way too busy"): the legend is
           reference material, not daily reading — collapsed behind a toggle
           (remembered per browser). The boxed SharePoint scan bar shrank to
           the same quiet utility line. -->
      <div style="display:flex; align-items:center; gap:12px; font-size:12px; color:var(--gray-500); margin-bottom:10px; padding:0 2px;">
        <a href="javascript:void(0)" id="legendToggle" onclick="toggleTileLegend()" style="color:var(--gray-500); text-decoration:none; font-weight:600;">Tile legend &#9656;</a>
        <span style="flex:1"></span>
        <span id="spInventoryStatus">Loading…</span>
        <button id="spScanBtn" onclick="scanSharePoint()" style="font-size:11px; padding:3px 10px; border:1px solid var(--gray-300); background:white; color:var(--gray-700); border-radius:4px; cursor:pointer; font-weight:600;">⟳ Scan SharePoint</button>
      </div>
      <div id="tileLegend" style="display:none; align-items:center; gap:16px; flex-wrap:wrap; font-size:12px; color:var(--gray-700); padding:8px 12px; background:var(--gray-50); border:1px solid var(--gray-200); border-radius:8px; margin-bottom:12px;">
        <span style="font-weight:700; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.04em; font-size:11px;">Files</span>
        <span style="font-size:12px;">B 2026 Budget · E Expense Dist · Y YSL · A AP Aging · M Maint Proof · Au 2025 Audit</span>
        <span style="color:var(--gray-300); margin:0 2px;">·</span>
        <span style="display:inline-flex; align-items:center; gap:6px;"><span class="fseg fs-ok">B</span> <b>In the budget</b></span>
        <span style="display:inline-flex; align-items:center; gap:6px;"><span class="fseg fs-act">Au</span> <b>Act now</b> — audit needs you (extract / confirm)</span>
        <span style="display:inline-flex; align-items:center; gap:6px;"><span class="fseg fs-ready">E</span> <b>Arrived</b> — loads automatically</span>
        <span style="display:inline-flex; align-items:center; gap:6px;"><span class="fseg fs-ok fs-stale">B</span> <b>Newer file in SP</b> — click to re-ingest</span>
        <span style="display:inline-flex; align-items:center; gap:6px;"><span class="fseg fs-miss">Y</span> <b>Missing</b></span>
        <span style="display:inline-flex; align-items:center; gap:6px;"><span class="fseg fs-fail">A</span> <b>Failed in build</b></span>
        <span style="display:inline-flex; align-items:center; gap:6px;"><span class="fseg fs-setup">M</span> Setup — not started</span>
        <span style="color:var(--gray-500); margin-left:auto;">Hover a segment for detail · click to jump ↗</span>
      </div>
      <div id="buildingsTableWrap">
        <table id="budgets-table">
          <thead>
            <tr>
              <th data-sort="building_name" onclick="sortBuildings('building_name')" style="cursor:pointer; user-select:none; white-space:nowrap;">Building <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th data-sort="entity_code" onclick="sortBuildings('entity_code')" style="cursor:pointer; user-select:none; white-space:nowrap;">Entity <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th data-sort="pm_name" onclick="sortBuildings('pm_name')" style="cursor:pointer; user-select:none; white-space:nowrap;">PM <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th>Files</th>
              <th data-sort="status" onclick="sortBuildings('status')" style="cursor:pointer; user-select:none; white-space:nowrap;">Stage <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th data-sort="days" onclick="sortBuildings('days')" style="cursor:pointer; user-select:none; white-space:nowrap;">Days <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
const statusLabels = {
  'not_started': 'Not Started',
  'data_collection': 'Data Collection',
  'data_ready': 'Data Ready',
  'draft': 'Draft',
  'pm_pending': 'Pending PM',
  'pm_in_progress': 'PM In Progress',
  'fa_review': 'FA Review',
  'exec_review': 'Exec Review',
  'presentation': 'Presentation',
  'approved': 'Approved',
  'returned': 'Returned',
  'ar_pending': 'AR Pending',
  'ar_complete': 'AR Complete'
};
// Fallback: any unknown status gets snake_case → Title Case automatically
function formatStatus(s) {
  if (!s) return '';
  if (statusLabels[s]) return statusLabels[s];
  return s.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}

function showToast(msg, type='info') {
  const c = document.getElementById('toastContainer');
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity 0.3s'; setTimeout(() => t.remove(), 300); }, 3000);
}

const _pmByEntity = {};
let _budgetsCache = [];
// FA dir 2026-05-23: default sort = readiness desc so the most-actionable
// rows surface at the top, matching the wizard.
let _sortState = { column: 'readiness', direction: 'desc' };
// FA dir 2026-05-23: per-tier filter ("all" / "READY_TO_BUILD" / etc).
// FA dir 2026-05-24: seed from URL so /dashboard?tier=ready_to_build (the
// back-nav deep link) shows the filtered grid on first paint.
let _readinessFilter = (function () {
  try {
    return new URLSearchParams(window.location.search).get('tier') || 'all';
  } catch (e) { return 'all'; }
})();
const _pmStatusMap = {
  'draft': 'Not Sent',
  'pm_pending': 'Sent to PM',
  'pm_in_progress': 'PM Working',
  'fa_review': 'Submitted',
  'approved': 'Approved',
  'returned': 'Returned'
};

async function loadBudgets() {
  try {
    const [res, aRes] = await Promise.all([fetch('/api/budgets'), fetch('/api/assignments')]);
    const budgets = await res.json();
    try {
      const assignments = await aRes.json();
      assignments.forEach(a => { if (a.role === 'pm') _pmByEntity[a.entity_code] = a.user_name; });
    } catch(e) { console.warn('Assignments fetch failed:', e); }
    _budgetsCache = budgets;
    renderBudgets(budgets);
    renderStatusSummary(budgets);
    updateSortArrows();
    document.getElementById('loadingState').style.display = 'none';
    document.getElementById('dashboardContent').style.display = '';
    return budgets;
  } catch (err) {
    console.error('Failed to load budgets:', err);
    document.getElementById('loadingState').innerHTML = '<p style="color:var(--red);">Failed to load budgets. Please refresh.</p>';
    return [];
  }
}

function _getSortValue(b, col) {
  if (col === 'entity_code') return Number(b.entity_code) || 0;
  if (col === 'building_name') return (b.building_name || '').toLowerCase();
  if (col === 'pm_name') return (_pmByEntity[b.entity_code] || '').toLowerCase();
  if (col === 'status') return (b.status || '').toLowerCase();
  if (col === 'pm_review') return (_pmStatusMap[b.status] || b.status || '').toLowerCase();
  if (col === 'days') {
    const doneStatuses = ['approved','ar_pending','ar_complete'];
    if (doneStatuses.includes(b.status) || !b.updated_at) return -1;
    return Math.floor((Date.now() - new Date(b.updated_at).getTime()) / 86400000);
  }
  if (col === 'readiness') {
    // Server-supplied tier_order — higher = more actionable, so default
    // desc sort surfaces READY_TO_BUILD at the top.
    return (b.readiness && typeof b.readiness.tier_order === 'number')
      ? b.readiness.tier_order : 0;
  }
  return '';
}

function sortBuildings(col) {
  // Days defaults to desc (most stale first); others default to asc
  const defaultDir = col === 'days' ? 'desc' : 'asc';
  if (_sortState.column === col) {
    _sortState.direction = _sortState.direction === 'asc' ? 'desc' : 'asc';
  } else {
    _sortState.column = col;
    _sortState.direction = defaultDir;
  }
  renderBudgets(_budgetsCache);
  updateSortArrows();
  filterBudgetTable();
}

// FA dir 2026-05-22 (Phase 2): SharePoint inventory controls.
// On dashboard load, fetch the cache freshness; on button click, kick off
// the synchronous scan (5+ minutes for 147 entities) and re-render the
// dashboard so amber tiles light up where SP files were detected.
// Status UX Phase 3 (2026-06-09): auto-refresh on view. If the SP cache is
// stale (>15 min) when the dashboard opens, kick off a background rescan
// silently — same lazy pattern the wizard has had since 2026-05-24. The
// dashboard used to have NO auto-refresh, so its amber/red tile split could
// be days stale while the wizard checked live: the root of the two pages
// contradicting each other. Session-guarded so reloads during the ~5-min
// scan don't stack scans.
function maybeAutoScanSP(minsAgo) {
  try {
    const STALE_MIN = 15;
    if (minsAgo !== null && minsAgo <= STALE_MIN) return;
    const last = parseInt(sessionStorage.getItem('dash_auto_sp_scan') || '0', 10);
    if (Date.now() - last < 10 * 60000) return;
    sessionStorage.setItem('dash_auto_sp_scan', String(Date.now()));
    const status = document.getElementById('spInventoryStatus');
    if (status) status.innerHTML = '<span style="color:#1d4ed8; font-weight:600;">⟳ Auto-refreshing SharePoint in the background…</span>';
    fetch('/api/admin/sp-inventory/scan', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })
      .then(r => r.json())
      // Phase 3b: after every refresh, arrivals load themselves — sweep stages
      // any in-SP-not-staged sources server-side, then repaint with the result.
      .then(() => fetch('/api/admin/auto-load-arrivals', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' }).catch(() => {}))
      .then(() => { loadSpInventoryStatus(); loadBudgets(); })
      .catch(() => {});
  } catch (e) {}
}

function loadSpInventoryStatus() {
  fetch('/api/admin/sp-inventory/status')
    .then(r => r.ok ? r.json() : null)
    .then(d => {
      const el = document.getElementById('spInventoryStatus');
      if (!el) return;
      if (!d || !d.newest_scan) {
        el.innerHTML = '<span style="color:#92400e; font-weight:600;">Never scanned</span> — amber tiles inactive until first scan';
        maybeAutoScanSP(null);
        return;
      }
      // Server timestamps are naive UTC — without the 'Z' the browser parses
      // them as LOCAL time and the age goes NEGATIVE ("-65m ago", 2026-06-10).
      let iso = String(d.newest_scan);
      if (!/[zZ]$|[+-]\d\d:?\d\d$/.test(iso)) iso += 'Z';
      const newest = new Date(iso);
      let minsAgo = Math.floor((Date.now() - newest.getTime()) / 60000);
      if (minsAgo < 0) minsAgo = 0;
      maybeAutoScanSP(minsAgo);
      const tag = minsAgo < 1 ? 'just now'
                : minsAgo < 60 ? (minsAgo + 'm ago')
                : minsAgo < 1440 ? (Math.floor(minsAgo/60) + 'h ago')
                : (Math.floor(minsAgo/1440) + 'd ago');
      const color = minsAgo > 1440 ? '#b91c1c' : (minsAgo > 60 ? '#92400e' : '#065f46');
      el.innerHTML = '<span style="color:' + color + '; font-weight:600;">Last scan: ' + tag + '</span> · ' + d.entities_in_cache + ' entities cached';
    })
    .catch(() => {});
}

function scanSharePoint() {
  if (!confirm('Scan SharePoint folders for all 147 buildings? This takes ~5 minutes — do not close the tab.')) return;
  const btn = document.getElementById('spScanBtn');
  const status = document.getElementById('spInventoryStatus');
  if (btn) { btn.disabled = true; btn.textContent = 'Scanning…'; btn.style.opacity = '0.6'; }
  if (status) status.innerHTML = '<span style="color:#1d4ed8; font-weight:600;">⟳ Scanning all entities — ~5 min…</span>';
  fetch('/api/admin/sp-inventory/scan', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })
    .then(r => r.json())
    .then(d => {
      if (d.error) {
        showToast('Scan failed: ' + d.error, 'error');
      } else {
        showToast('Scan complete: ' + d.ok + '/' + d.total + ' entities (' + (d.errors||[]).length + ' errors)', 'success');
      }
      // Phase 3b: stage any newly-arrived files automatically, then repaint.
      return fetch('/api/admin/auto-load-arrivals', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })
        .then(r => r.json())
        .then(al => { if (al && al.loaded) showToast('Auto-loaded ' + al.loaded + ' newly arrived file(s)' + (al.failed ? (' · ' + al.failed + ' failed — see red tiles') : ''), 'success'); })
        .catch(() => {});
    })
    .then(() => {
      loadSpInventoryStatus();
      loadBudgets();  // re-render dashboard so amber tiles update
    })
    .catch(err => {
      showToast('Scan failed: ' + err, 'error');
    })
    .finally(() => {
      if (btn) { btn.disabled = false; btn.textContent = '⟳ Scan SharePoint now'; btn.style.opacity = '1'; }
    });
}

function updateSortArrows() {
  document.querySelectorAll('#budgets-table th[data-sort]').forEach(th => {
    const arrow = th.querySelector('.sort-arrow');
    if (!arrow) return;
    if (th.dataset.sort === _sortState.column) {
      arrow.innerHTML = _sortState.direction === 'asc' ? '&#9650;' : '&#9660;';
      arrow.style.opacity = '1';
    } else {
      arrow.innerHTML = '&#9650;';
      arrow.style.opacity = '0.25';
    }
  });
}

function toggleBuildingsCollapse() {
  const wrap = document.getElementById('buildingsTableWrap');
  const chevron = document.getElementById('buildingsChevron');
  const isCollapsed = wrap.style.display === 'none';
  if (isCollapsed) {
    wrap.style.display = '';
    chevron.style.transform = 'rotate(0deg)';
    localStorage.setItem('fa-dashboard-buildings-collapsed', 'false');
  } else {
    wrap.style.display = 'none';
    chevron.style.transform = 'rotate(-90deg)';
    localStorage.setItem('fa-dashboard-buildings-collapsed', 'true');
  }
}

// Design cleanup (2026-06-10): tile legend is reference material — collapsed
// by default, one click to expand, remembered per browser.
function toggleTileLegend() {
  const el = document.getElementById('tileLegend');
  const t = document.getElementById('legendToggle');
  if (!el) return;
  const open = el.style.display === 'none';
  el.style.display = open ? 'flex' : 'none';
  if (t) t.innerHTML = 'Tile legend ' + (open ? '&#9662;' : '&#9656;');
  try { localStorage.setItem('fa-tile-legend-open', open ? '1' : '0'); } catch (e) {}
}
try { if (localStorage.getItem('fa-tile-legend-open') === '1') toggleTileLegend(); } catch (e) {}

function renderStatusSummary(budgets) {
  // Design cleanup (2026-06-10, Jacob: "way too busy"): six full-size stat
  // cards (mostly zeros) became one quiet line showing only non-zero counts.
  // Lifecycle counts are secondary context — the readiness chips below are
  // the real navigation.
  const summary = document.getElementById('status-summary');
  if (!summary) return;
  const counts = {
    'draft': 0,
    'pm_pending': 0,
    'pm_in_progress': 0,
    'fa_review': 0,
    'approved': 0,
    'returned': 0
  };
  budgets.forEach(b => {
    if (counts.hasOwnProperty(b.status)) counts[b.status]++;
  });
  const parts = Object.entries(counts)
    .filter(([, c]) => c > 0)
    .map(([s, c]) => '<b style="color:var(--gray-700);">' + c + '</b> ' + formatStatus(s));
  summary.innerHTML = parts.length
    ? '<span style="font-size:12px; color:var(--gray-500);">Budgets in flight: ' + parts.join(' <span style="color:var(--gray-300);">·</span> ') + '</span>'
    : '';
}

// FA dir 2026-05-23: hero callout + readiness chips for the dashboard.
// Same affordances as the wizard so the FA can use either page.
function renderReadinessHero(budgets) {
  const hero = document.getElementById('readinessHero');
  if (!hero) return;
  let ready = 0, inProg = 0;
  budgets.forEach(b => {
    const t = (b.readiness && b.readiness.tier) || '';
    if (t === 'READY_TO_BUILD') ready += 1;
    else if (t === 'IN_PROGRESS') inProg += 1;
  });
  if (ready > 0) {
    hero.style.display = 'block';
    hero.innerHTML =
      '<div style="background:linear-gradient(90deg,#ecfdf5 0%,#f0fdf4 100%); border:1px solid #6ee7b7; border-radius:10px; padding:14px 18px; display:flex; align-items:center; gap:14px;">' +
        '<span style="font-size:22px;">🚀</span>' +
        '<div style="flex:1;">' +
          '<strong style="color:#065f46; font-size:14px;">' + ready + ' building' + (ready === 1 ? '' : 's') + ' ready to build right now</strong>' +
          '<div style="color:#047857; font-size:11px; margin-top:2px;">' +
            'All files staged, audit confirmed.' +
            (inProg > 0 ? ' ' + inProg + ' more in progress (audit review needed).' : '') +
          '</div>' +
        '</div>' +
        '<button onclick="buildAllReady()" style="background:#16a34a; color:#fff; border:none; padding:8px 16px; border-radius:6px; font-weight:700; font-size:13px; cursor:pointer;">Build all ' + ready + ' →</button>' +
      '</div>';
  } else if (inProg > 0) {
    hero.style.display = 'block';
    hero.innerHTML =
      '<div style="background:#fffbeb; border:1px solid #fcd34d; border-radius:10px; padding:12px 16px; display:flex; align-items:center; gap:12px;">' +
        '<span style="font-size:18px;">⏳</span>' +
        '<div style="flex:1;">' +
          '<strong style="color:#92400e; font-size:13px;">' + inProg + ' building' + (inProg === 1 ? '' : 's') + ' in progress</strong>' +
          '<div style="color:#92400e; font-size:11px; margin-top:1px;">Audit review or remaining source ingestion needed. Click "In progress" to focus.</div>' +
        '</div>' +
      '</div>';
  } else {
    hero.style.display = 'none';
    hero.innerHTML = '';
  }
}

function renderReadinessChips(budgets) {
  const row = document.getElementById('readinessChips');
  if (!row) return;
  const counts = {
    'all': budgets.length,
    'READY_TO_BUILD': 0,
    'IN_PROGRESS': 0,
    'NEEDS_AUDIT_EXTRACT': 0,
    'NEEDS_AUDIT': 0,
    'NEEDS_FILES': 0,
    'BUILT': 0,
  };
  budgets.forEach(b => {
    const t = (b.readiness && b.readiness.tier) || 'NEEDS_FILES';
    if (counts[t] !== undefined) counts[t] += 1;
  });
  const headerLabel = row.querySelector('span');
  row.innerHTML = '';
  if (headerLabel) row.appendChild(headerLabel);
  function makeChip(label, count, key, color) {
    const isActive = _readinessFilter === key;
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.innerHTML = label + ' <span style="opacity:0.7; font-weight:500;">' + count + '</span>';
    const bg = color || '#5a4a3f';
    btn.style.cssText =
      'font-size:12px; font-weight:' + (isActive ? '700' : '600') +
      '; padding:6px 12px; border-radius:14px; cursor:pointer; border:1px solid ' +
      (isActive ? bg : 'var(--gray-200)') +
      '; background:' + (isActive ? bg : 'white') +
      '; color:' + (isActive ? 'white' : 'var(--gray-700)') + ';';
    btn.onclick = function () { setReadinessFilter(key); };
    return btn;
  }
  row.appendChild(makeChip('All', counts.all, 'all'));
  row.appendChild(makeChip('Ready to build', counts.READY_TO_BUILD, 'READY_TO_BUILD', '#16a34a'));
  row.appendChild(makeChip('In progress',    counts.IN_PROGRESS,    'IN_PROGRESS',    '#d97706'));
  row.appendChild(makeChip('Audit ready to extract', counts.NEEDS_AUDIT_EXTRACT, 'NEEDS_AUDIT_EXTRACT', '#0369a1'));
  row.appendChild(makeChip('Waiting for audit', counts.NEEDS_AUDIT, 'NEEDS_AUDIT'));
  row.appendChild(makeChip('Waiting for files', counts.NEEDS_FILES, 'NEEDS_FILES'));
  row.appendChild(makeChip('Built', counts.BUILT, 'BUILT', '#065f46'));
}

function setReadinessFilter(tier) {
  _readinessFilter = tier;
  // FA dir 2026-05-24: push filter to URL so back-nav from /dashboard/<entity>
  // restores it. Without this, click chip → click building → back → all 147
  // rows show again. Use replaceState when clicking the active chip (no
  // history entry needed) and pushState on real filter changes.
  try {
    const url = new URL(window.location.href);
    if (tier === 'all') {
      url.searchParams.delete('tier');
    } else {
      url.searchParams.set('tier', tier);
    }
    const currentTier = new URL(window.location.href).searchParams.get('tier') || 'all';
    if (currentTier === tier) {
      window.history.replaceState({ tier: tier }, '', url.toString());
    } else {
      window.history.pushState({ tier: tier }, '', url.toString());
    }
  } catch (e) {}
  renderBudgets(_budgetsCache);
}

// FA dir 2026-05-24: popstate listener restores filter when FA hits browser
// back. Reads from URL since pushState writes there on every chip click.
window.addEventListener('popstate', function () {
  try {
    const tier = new URL(window.location.href).searchParams.get('tier') || 'all';
    _readinessFilter = tier;
    if (_budgetsCache) renderBudgets(_budgetsCache);
  } catch (e) {}
});

function buildAllReady() {
  let readyCount = 0;
  (_budgetsCache || []).forEach(function (b) {
    if (b.readiness && b.readiness.tier === 'READY_TO_BUILD') readyCount += 1;
  });
  if (readyCount === 0) {
    alert('No buildings are currently ready to build.');
    return;
  }
  if (!confirm('Build all ' + readyCount + ' ready building' + (readyCount === 1 ? '' : 's') + ' now?\n\nEach takes ~30-60 seconds. The page will stay open while it runs.')) return;
  const hero = document.getElementById('readinessHero');
  if (hero) hero.innerHTML = '<div style="background:#fffbeb; border:1px solid #fcd34d; border-radius:10px; padding:14px 18px; font-size:13px; color:#92400e;">⏳ Building ' + readyCount + ' budgets… this may take a few minutes. Do not close the tab.</div>';
  fetch('/api/admin/build-all-ready', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}' })
    .then(r => r.json())
    .then(data => {
      const ok = (data.results || []).filter(r => r.ok).length;
      const fail = (data.results || []).filter(r => !r.ok).length;
      showToast('Built ' + ok + ' of ' + readyCount + (fail > 0 ? ' (' + fail + ' failed — see table)' : ''), fail > 0 ? 'error' : 'success');
      loadBudgets();
    })
    .catch(err => {
      showToast('Bulk build failed: ' + err, 'error');
      loadBudgets();
    });
}

function renderBudgets(budgets) {
  const tbody = document.querySelector('#budgets-table tbody');
  tbody.innerHTML = '';

  // FA dir 2026-05-23: render hero + readiness chips up-top.
  // Counts come from the full unfiltered budget list — chips reflect what's
  // reachable in the whole portfolio, not the filtered view.
  renderReadinessHero(budgets);
  renderReadinessChips(budgets);

  // Apply readiness-tier filter (set by the chip click)
  let filtered = budgets;
  if (_readinessFilter && _readinessFilter !== 'all') {
    filtered = filtered.filter(b => (b.readiness && b.readiness.tier) === _readinessFilter);
  }

  // Sort by current sort state
  const col = _sortState.column;
  const dir = _sortState.direction === 'asc' ? 1 : -1;
  // Option C (Jacob 2026-07-08): built rows with a newer file in SP sort to
  // the top of their group — stale data is the one BUILT state that needs FA
  // attention.
  function _hasStale(bb) {
    const ss = bb.source_states || {};
    return Object.keys(ss).some(function (k) {
      const st = ss[k] || {};
      return st.sub === 'newer_in_sp' && st.state === 'in_budget';
    });
  }
  filtered.sort((a, b) => {
    const va = _getSortValue(a, col);
    const vb = _getSortValue(b, col);
    if (va < vb) return -1 * dir;
    if (va > vb) return 1 * dir;
    if (col === 'readiness') return (_hasStale(b) ? 1 : 0) - (_hasStale(a) ? 1 : 0);
    return 0;
  });

  // Option C: tier-grouped section headers. Pure presentation over the
  // readiness sort — headers only render on the default sort (tier_order is
  // monotonic there, so groups are contiguous), and empty groups never
  // appear because headers are emitted on boundary crossings.
  function _groupOf(bb) {
    const t = (bb.readiness && bb.readiness.tier) || '';
    if (t === 'BUILT') return 'built';
    if (t === 'NEEDS_AUDIT' || t === 'NEEDS_FILES') return 'waiting';
    return 'needs_you';
  }
  const GRP_META = {
    needs_you: { label: 'Needs you', style: 'background:#fdf0dd; color:#b45309;' },
    waiting:   { label: 'Waiting on others', style: 'background:#f1f0ec; color:#7d7468;' },
    built:     { label: 'Built', style: 'background:#def7ec; color:#065f46;' },
  };
  const grpCounts = { needs_you: 0, waiting: 0, built: 0 };
  filtered.forEach(function (bb) { grpCounts[_groupOf(bb)] += 1; });
  let lastGrp = null;

  filtered.forEach(b => {
    if (_sortState.column === 'readiness') {
      const g = _groupOf(b);
      if (g !== lastGrp) {
        lastGrp = g;
        const gtr = document.createElement('tr');
        gtr.className = 'grp-row';
        gtr.innerHTML = '<td colspan="7" style="' + GRP_META[g].style + '">' +
          GRP_META[g].label + ' · ' + grpCounts[g] + '</td>';
        tbody.appendChild(gtr);
      }
    }
    const tr = document.createElement('tr');
    // Display lifecycle stage if present (new vocabulary), fall back to legacy status label.
    // Pill color still keyed off status so existing CSS classes apply unchanged.
    const statusLabel = b.lifecycle_stage || formatStatus(b.status);
    const statusClass = `pill-${b.status}`;

    // Data completeness - compact inline format
    function dataIcon(ok) { return ok ? '<span style="color:var(--green);">&#10003;</span>' : '<span style="color:var(--gray-300);">&#10007;</span>'; }

    // PM review status pill
    const pmStatusMap = {
      'draft': 'Not Sent',
      'pm_pending': 'Sent to PM',
      'pm_in_progress': 'PM Working',
      'fa_review': 'Submitted',
      'approved': 'Approved',
      'returned': 'Returned'
    };
    const pmLabel = pmStatusMap[b.status] || formatStatus(b.status);

    // Option C: one tier-driven primary action per row. The backend already
    // computes next_action/next_url per tier (workflow.py readiness block) —
    // this renders it instead of making the FA infer it from tiles.
    let actionHtml = '';
    const rd = b.readiness || {};
    const isBuilt = rd.tier === 'BUILT';
    const rowStale = _hasStale(b);
    if (!isBuilt && rd.next_url) {
      if (rd.next_action === 'build') {
        actionHtml = `<a class="btn-action btn-green" href="${rd.next_url}" style="text-decoration:none;">Build →</a>`;
      } else if (rd.next_action === 'audit_review') {
        actionHtml = `<a class="btn-action btn-orange" href="${rd.next_url}" style="text-decoration:none;">Review audit →</a>`;
      } else {
        // Waiting tiers: no FA action exists yet — quiet wizard link (manual
        // upload escape hatch lives there). The chase workflow lands here later.
        actionHtml = `<a href="${rd.next_url}" style="font-size:12px; color:var(--gray-500); text-decoration:none;">Wizard ↗</a>`;
      }
    } else if (isBuilt && rowStale) {
      // Newer file in SP than the built budget — the detail page prompts the
      // update on open (source-freshness banner, shipped 2026-07-08).
      actionHtml = `<a class="btn-action btn-orange" href="/dashboard/${b.entity_code}" style="text-decoration:none;">Update figures →</a>`;
    }
    if (b.status === 'draft') {
      actionHtml += `<button class="btn-action btn-blue" onclick="changeStatus('${b.entity_code}', 'pm_pending')"${rowStale ? ' style="margin-left:4px;"' : ''}>Send to PM</button>`;
    } else if (b.status === 'fa_review') {
      actionHtml += `
        <button class="btn-action btn-green" onclick="approveStatus('${b.entity_code}')">Approve</button>
        <button class="btn-action btn-orange" onclick="returnTopm('${b.entity_code}')" style="margin-left: 4px;">Return</button>
      `;
    }
    if (b.status !== 'approved') {
      actionHtml += `<div class="action-menu" style="display:inline-block; margin-left:4px;">` +
        `<button class="action-menu-btn" onclick="toggleMenu(this)">&#8943;</button>` +
        `<div class="action-menu-items">` +
        `<button class="del-item" onclick='deleteBudget(${b.id}, ${JSON.stringify(b.building_name)}, ${b.version || 1})'>Delete budget</button>` +
        `</div></div>`;
    }

    // Status UX Phase 3 (2026-06-09): tiles render from the SHARED model
    // (b.source_states, computed once in /api/budgets) — the same brain as the
    // wizard, so the two pages cannot disagree. Jacob's rule: green = the file
    // is in a BUILT budget; amber = in SharePoint (date shown) or audit in
    // review; red = missing or failed during build; gray = not started.
    const ss = b.source_states || {};
    const au = b.audit || null;
    function fmtDt(iso) { if (!iso) return ''; const d = new Date(iso); return (d.getMonth()+1) + '/' + d.getDate(); }
    const TILE_ORDER = [
      ['approved_2026', 'B', 'Budget'], ['expense_distribution', 'E', 'Exp Dist'],
      ['ysl', 'Y', 'YSL'], ['ap_aging', 'A', 'AP Aging'],
      ['maint_proof', 'M', 'Maint Proof'], ['audit_2025', 'Au', 'Audit'],
    ];
    const tiles = TILE_ORDER.map(function (o) {
      const key = o[0], letter = o[1], label = o[2];
      const s = ss[key] || { state: 'missing' };
      let cls = 'fseg fs-miss', glyph = '✕', sub = '', tip = label + ' — not in SharePoint · chase the file';
      let href = '/wizard/' + b.entity_code + '?step=2&focus=' + key;
      if (s.state === 'in_budget') {
        cls = 'fseg fs-ok';
        glyph = '✓';
        sub = (key === 'audit_2025') ? 'conf' : (fmtDt(s.date) || '');
        tip = (key === 'audit_2025') ? ('Audit confirmed ' + (fmtDt(s.date) || ''))
            : (label + ' is in the built budget' + (fmtDt(s.date) ? ' (loaded ' + fmtDt(s.date) + ')' : ''));
        if (key === 'audit_2025' && au && au.id) href = '/audited-financials/review/' + au.id;
        // Stale-source flag (Jacob 2026-06-10, 733's ExpDist): a newer file
        // is in SharePoint than what was ingested — surface it instead of
        // silently running on the old file. Click lands on the wizard slot
        // where one click on the new file re-ingests (parse-on-click).
        if (s.sub === 'newer_in_sp') {
          cls = 'fseg fs-ok fs-stale';
          glyph = '↻';
          sub = 'new file';
          tip = label + ': a NEWER file (' + (fmtDt(s.sp_date) || 'recent') + ') is in SharePoint than the ingested data (' + (fmtDt(s.date) || '') + ') — click, then pick the new file to re-ingest';
        }
      } else if (s.state === 'needs_review') {
        cls = 'fseg fs-act';
        glyph = (s.sub === 'extracting') ? '⟳' : '!';
        sub = s.sub || 'review';
        tip = (s.sub === 'extracting') ? 'Extraction running — opens live progress'
            : (s.sub === 'extract') ? 'Audit PDF ready — click to extract'
            : 'Audit extracted — click to confirm the mapping';
        if (au && au.id) href = '/audited-financials/review/' + au.id;
      } else if (s.state === 'in_sp') {
        cls = 'fseg fs-ready';
        glyph = '◷';
        sub = (s.via === 'staged') ? 'staged ✓' : (fmtDt(s.date) ? ('SP ' + fmtDt(s.date)) : 'SP');
        tip = (s.via === 'staged')
            ? (label + ' is staged — data already loaded, turns green when the budget is built')
            : (label + ' in SharePoint' + (fmtDt(s.date) ? ' since ' + fmtDt(s.date) : '') + ' — loads automatically, turns green when the budget is built');
      } else if (s.state === 'failed') {
        cls = 'fseg fs-fail';
        glyph = '✕';
        sub = 'failed';
        tip = label + ' failed during build — fix the file and rebuild';
      } else if (s.state === 'setup') {
        cls = 'fseg fs-setup';
        glyph = '–';
        sub = '';
        tip = 'Not started';
      }
      // Option C: segment emission — letter only; state lives in the color,
      // detail in the tooltip. glyph/sub stay computed above because the
      // tips reference them and future surfaces may want them back.
      return '<a href="' + href + '" class="' + cls + '" title="' + tip.replace(/"/g, '&quot;') + '" data-focus="' + key + '">' + letter + '</a>';
    });
    const dataHtml = '<div class="fbar">' + tiles.join('') + '</div>';

    // Status UX Phase 3 (2026-06-09): Days unified with the wizard \u2014 same
    // anchor (last ACTIVITY = freshest of any load timestamp or budget edit)
    // and same thresholds (gray <14, amber >=14, red >=21). The two pages used
    // to measure staleness differently (7/14 vs 14/21, different anchors).
    const doneStatuses = ['approved','ar_pending','ar_complete'];
    let daysHtml = '<span style="color:var(--gray-300);">\u2014</span>';
    if (b.lifecycle_stage !== 'Setup' && !doneStatuses.includes(b.status)) {
      let last = b.updated_at ? new Date(b.updated_at).getTime() : 0;
      const tsAll = b.timestamps || {};
      Object.keys(tsAll).forEach(function (k) {
        if (tsAll[k]) { const t = new Date(tsAll[k]).getTime(); if (t > last) last = t; }
      });
      // Option C: queue-aware aging. For the two audit queues the honest
      // anchor is the audit row's own timestamp (how long it has sat waiting
      // for extract/confirm), not portfolio-wide activity. SLA thresholds
      // tighten for actionable rows (7/14) vs waiting rows (14/21); built
      // rows keep a quiet gray count.
      let tip = 'Days since last activity (file loads or edits)';
      if ((rd.tier === 'NEEDS_AUDIT_EXTRACT' || rd.tier === 'IN_PROGRESS') && b.audit && b.audit.ts) {
        const at = new Date(b.audit.ts).getTime();
        if (at > 0) { last = at; tip = 'Days this audit has waited in the ' + (rd.tier === 'IN_PROGRESS' ? 'confirm' : 'extract') + ' queue'; }
      }
      if (last > 0) {
        const days = Math.floor((Date.now() - last) / 86400000);
        const grp = _groupOf(b);
        const amberAt = grp === 'needs_you' ? 7 : 14;
        const redAt = grp === 'needs_you' ? 14 : 21;
        const color = grp === 'built' ? 'var(--gray-500)'
                    : days >= redAt ? 'var(--red)'
                    : days >= amberAt ? '#a16207' : 'var(--gray-500)';
        const hot = grp !== 'built' && days >= amberAt;
        daysHtml = `<span style="font-weight:${hot ? 700 : 600};color:${color};" title="${tip}">${days}d</span>`;
      }
    }

    // Phase 3: ONE Stage column (lifecycle words, stage-keyed colors) replaces
    // the old Status + PM Review double pill (both were derived from b.status \u2014
    // same fact rendered twice). The PM-phase detail (Sent/Working/Submitted)
    // lives in the pill tooltip instead of its own column.
    const stageStyles = {
      'Setup': 'background:#f1f0ec; color:#7d7468;',
      'Sources Collected': 'background:#f5efe7; color:#5a4a3f;',
      'Assumptions Confirmed': 'background:#f5efe7; color:#5a4a3f;',
      'Budget Built (draft)': 'background:#def7ec; color:#065f46;',
      'PM Review': 'background:#fef3c7; color:#a16207;',
      'Approved': 'background:#def7ec; color:#057a55; font-weight:700;',
    };
    const stageStyle = stageStyles[statusLabel] || '';
    const stageTip = (statusLabel === 'PM Review') ? ('PM Review \u2014 ' + pmLabel) : statusLabel;
    // Option C: phase-aware status. Pre-build the lifecycle words said
    // "Not Started" while the building was mid-pipeline — until a budget
    // exists, the readiness tier is the operational truth. Post-build the
    // lifecycle stage takes over (draft → PM → approved), unchanged.
    const tierPillStyles = {
      'READY_TO_BUILD': 'background:#def7ec; color:#057a55; font-weight:700;',
      'IN_PROGRESS': 'background:#fef3c7; color:#b45309;',
      'NEEDS_AUDIT_EXTRACT': 'background:#e5eff6; color:#22577e;',
      'NEEDS_AUDIT': 'background:#f1f0ec; color:#7d7468;',
      'NEEDS_FILES': 'background:#f1f0ec; color:#7d7468;',
    };
    let stagePill;
    if (!isBuilt && rd.tier && tierPillStyles[rd.tier]) {
      const tl = rd.tier_label || formatStatus(b.status);
      stagePill = `<span class="pill" style="${tierPillStyles[rd.tier]}" title="${tl.replace(/"/g, '&quot;')}">${tl}</span>`;
    } else {
      stagePill = stageStyle
        ? `<span class="pill" style="${stageStyle}" title="${stageTip.replace(/"/g, '&quot;')}">${statusLabel}</span>`
        : `<span class="pill ${statusClass}" title="${stageTip.replace(/"/g, '&quot;')}">${statusLabel}</span>`;
    }

    const pmName = _pmByEntity[b.entity_code] || '\u2014';
    tr.innerHTML = `
      <td><a href="/dashboard/${b.entity_code}" style="color: var(--blue); text-decoration: none; font-weight:500;">${b.building_name}</a></td>
      <td style="font-family:monospace; font-size:13px;">${b.entity_code}</td>
      <td style="font-size:12px; color:var(--gray-500); white-space:nowrap;">${pmName}</td>
      <td>${dataHtml}</td>
      <td>${stagePill}</td>
      <td style="text-align:center;">${daysHtml}</td>
      <td>${actionHtml}</td>
    `;
    tbody.appendChild(tr);
  });
}

function filterBudgetTable() {
  const query = document.getElementById('budgetSearch').value.toLowerCase();
  const rows = document.querySelectorAll('#budgets-table tbody tr');
  rows.forEach(row => {
    // Option C: group headers vanish during a search — a lookup result set
    // shouldn't carry section counts that no longer match what's visible.
    if (row.classList.contains('grp-row')) {
      row.style.display = query ? 'none' : '';
      return;
    }
    const text = row.textContent.toLowerCase();
    row.style.display = text.includes(query) ? '' : 'none';
  });
}

async function changeStatus(entity, newStatus) {
  if (!confirm(`Change status to ${formatStatus(newStatus)}?`)) return;
  try {
    await fetch(`/api/budgets/${entity}/status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: newStatus })
    });
    showToast('Status updated to ' + formatStatus(newStatus), 'success');
    await loadBudgets();
  } catch (err) {
    showToast('Failed to update status', 'error');
    console.error(err);
  }
}

async function approveStatus(entity) {
  if (!confirm('Approve this budget?')) return;
  await changeStatus(entity, 'approved');
}

async function returnTopm(entity) {
  const notes = prompt('Notes for returning to PM:');
  if (notes === null) return;
  try {
    await fetch(`/api/budgets/${entity}/status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: 'returned', fa_notes: notes })
    });
    showToast('Budget returned to PM', 'success');
    await loadBudgets();
  } catch (err) {
    showToast('Failed to return budget', 'error');
    console.error(err);
  }
}

function toggleMenu(btn) {
  const menu = btn.nextElementSibling;
  document.querySelectorAll('.action-menu-items').forEach(m => { if (m !== menu) m.style.display = 'none'; });
  menu.style.display = menu.style.display === 'block' ? 'none' : 'block';
}
document.addEventListener('click', e => {
  if (!e.target.closest('.action-menu')) document.querySelectorAll('.action-menu-items').forEach(m => m.style.display = 'none');
});

async function deleteBudget(budgetId, name, version) {
  if (!confirm(`Delete draft budget for ${name} (v${version})? This cannot be undone.`)) return;
  try {
    const resp = await fetch(`/api/budgets/${budgetId}`, { method: 'DELETE' });
    const data = await resp.json();
    if (resp.ok) {
      showToast(data.message, 'success');
      await loadBudgets();
    } else {
      showToast(data.error || 'Failed to delete', 'error');
    }
  } catch (err) {
    showToast('Failed to delete budget', 'error');
    console.error(err);
  }
}

// Initialize on page load
(async () => {
  // Restore collapse state before loading
  if (localStorage.getItem('fa-dashboard-buildings-collapsed') === 'true') {
    const wrap = document.getElementById('buildingsTableWrap');
    const chevron = document.getElementById('buildingsChevron');
    if (wrap) wrap.style.display = 'none';
    if (chevron) chevron.style.transform = 'rotate(-90deg)';
  }
  await loadBudgets();
  try { loadSpInventoryStatus(); } catch (e) {}
})();
</script>
</body>
</html>
"""

# ════════════════════════════════════════════════════════════════════════
# ACTION CENTER TEMPLATE
# ════════════════════════════════════════════════════════════════════════
# FA directive 2026-05-14 (Dashboard Phase 3): the landing page for every
# building. Shows status pipeline + KPI cards (shared with workbook view),
# then a single consolidated Action Center panel grouped by Blockers /
# Warnings / Complete. Pulls from /api/readiness, /api/wizard/<ec>/scan-
# findings, /api/summary/<ec>.warnings. Dedupes — each issue exists
# exactly once. Click-through actions all go somewhere useful.

