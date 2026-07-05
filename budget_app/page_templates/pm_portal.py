# Extracted from workflow.py 2026-07-05 (clean-architecture tranche 1).
# BYTE-IDENTICAL constant — template edits happen HERE now. Keep the
# string style unchanged (raw vs non-raw matters for JS escapes; see
# the wizard-template-js-escapes memory / check_template_js gate).

PM_PORTAL_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PM Portal - Century Management</title>
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

  header {
    background: linear-gradient(135deg, var(--blue) 0%, var(--blue-dark) 100%);
    color: white;
    padding: 30px 20px;
  }
  header h1 {
    font-size: 28px;
    font-weight: 700;
  }
  .container {
    max-width: 100%;
    margin: 0 auto;
    padding: 40px 40px;
  }
  .form-group {
    margin-bottom: 24px;
  }
  label {
    display: block;
    font-size: 14px;
    font-weight: 600;
    margin-bottom: 8px;
    color: var(--gray-700);
  }
  select {
    width: 100%;
    padding: 12px;
    border: 1px solid var(--gray-300);
    border-radius: 6px;
    font-size: 14px;
  }
  select:focus {
    outline: none;
    border-color: var(--blue);
    box-shadow: 0 0 0 3px var(--blue-light);
  }
  .summary-bar { display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap; }
  .summary-chip { padding: 6px 14px; border-radius: 20px; font-size: 12px; font-weight: 600; }
  .chip-action { background: #fef3c7; color: #92400e; }
  .chip-waiting { background: #e0e7ff; color: #3730a3; }
  .chip-done { background: #dcfce7; color: #166534; }
  .chip-total { background: var(--gray-100); color: var(--gray-700); }
  .buildings-list { display: flex; flex-direction: column; gap: 12px; margin-top: 16px; }
  .building-card {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 12px;
    padding: 18px 20px;
    text-decoration: none;
    color: var(--gray-900);
    transition: all 0.15s;
    border-left: 4px solid var(--gray-300);
  }
  .building-card:hover {
    border-color: var(--gray-200);
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
  }
  .building-card.card-action { border-left-color: #dc2626; }
  .building-card.card-waiting { border-left-color: #d97706; }
  .building-card.card-done { border-left-color: #16a34a; }
  .building-card.card-notready { border-left-color: var(--gray-300); opacity: 0.6; }
  .card-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
  .card-top h3 {
    font-size: 15px;
    font-weight: 600;
    color: var(--gray-900);
  }
  .card-top h3 span { font-size: 12px; font-weight: 400; color: var(--gray-500); margin-left: 8px; }
  .card-meta { display: flex; gap: 16px; font-size: 11px; color: var(--gray-500); margin-bottom: 8px; }
  .card-actions { display: flex; justify-content: flex-end; }
  .card-btn { font-size: 12px; padding: 5px 14px; border-radius: 6px; border: none; cursor: pointer; font-weight: 600; text-decoration: none; display: inline-block; }
  .card-btn-primary { background: var(--blue); color: white; }
  .card-btn-secondary { background: var(--gray-100); color: var(--gray-700); }
  .days-badge { font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 10px; }
  .days-red { background: #fef2f2; color: #dc2626; }
  .days-yellow { background: #fffbeb; color: #d97706; }
  .days-green { background: #f0fdf4; color: #16a34a; }
  .status-pill { display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 11px; font-weight: 600; }
  .pill-pm_pending { background: #fef3c7; color: #a16207; }
  .pill-pm_in_progress { background: var(--blue-light); color: var(--blue); }
  .pill-fa_review { background: #fed7aa; color: #f97316; }
  .pill-approved { background: #def7ec; color: #057a55; }
  .pill-returned { background: #fde8e8; color: #e02424; }
  .pill-draft { background: var(--gray-100); color: var(--gray-500); }
</style>
</head>
<body>

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

<header>
  <h1>PM Portal</h1>
  <p>Select your name and review assigned buildings</p>
</header>
<div class="container">
  <div class="form-group">
    <label style="display:flex;align-items:center;justify-content:space-between;gap:12px;">
      <span>Select Your Name</span>
      <!-- FA dir 2026-05-24: Monday sync status. Reads /api/sync-status on load,
           shows "Synced N min ago"; Refresh button forces a /api/sync-monday-now. -->
      <span id="pm-sync-status" style="font-size:11px;font-weight:500;color:#8b7b6b;display:inline-flex;align-items:center;gap:8px;text-transform:none;letter-spacing:0;">
        <span id="pm-sync-text">Checking Monday sync…</span>
        <button type="button" id="pm-sync-refresh" onclick="pmRefreshMondaySync()" title="Pull the latest Active Buildings (non-Lemle) list from Monday.com"
          style="padding:3px 10px;font-size:11px;font-weight:600;background:white;color:#4a3f35;border:1px solid #ddd5cc;border-radius:6px;cursor:pointer;">↻ Refresh</button>
      </span>
    </label>
    <select id="pm-select">
      <option value="">-- Choose your name --</option>
    </select>
  </div>

  <div id="pm-summary" class="summary-bar" style="display:none;"></div>
  <div class="buildings-list" id="buildings-grid" style="display: none;"></div>
</div>

<script>
// fa_review included so PM can re-enter a building after submitting for FA review
const editableStatuses = ['pm_pending', 'pm_in_progress', 'returned', 'fa_review'];
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

let allUsers = [];
let allAssignments = [];
let allBuildings = [];
let allBudgets = [];

async function loadInitialData() {
  try {
    const [usersRes, assignmentsRes, buildingsRes, budgetsRes] = await Promise.all([
      fetch('/api/users'),
      fetch('/api/assignments'),
      fetch('/api/buildings'),
      fetch('/api/budgets')
    ]);

    allUsers = await usersRes.json();
    allAssignments = await assignmentsRes.json();
    allBuildings = await buildingsRes.json();
    allBudgets = await budgetsRes.json();

    populatePMSelect();
    pmLoadSyncStatus();
  } catch (err) {
    console.error('Failed to load data:', err);
  }
}

// FA dir 2026-05-24: PM portal Monday-sync indicator. Reads /api/sync-status,
// shows "Synced N min ago" or last error. Refresh button forces a fresh pull.
async function pmLoadSyncStatus() {
  const txt = document.getElementById('pm-sync-text');
  if (!txt) return;
  try {
    const res = await fetch('/api/sync-status');
    const data = await res.json();
    if (data.error) {
      txt.textContent = '⚠ Last sync failed';
      txt.style.color = '#b91c1c';
      txt.title = data.error;
      return;
    }
    if (!data.last_synced_at) {
      txt.textContent = 'Never synced';
      txt.style.color = '#92400e';
      return;
    }
    const synced = new Date(data.last_synced_at);
    const mins = Math.max(0, Math.round((Date.now() - synced.getTime()) / 60000));
    let label;
    if (mins < 1) label = 'Synced just now';
    else if (mins < 60) label = 'Synced ' + mins + ' min ago';
    else if (mins < 1440) label = 'Synced ' + Math.round(mins / 60) + 'h ago';
    else label = 'Synced ' + Math.round(mins / 1440) + 'd ago';
    txt.textContent = '✓ ' + label + ' from Monday';
    txt.style.color = '#15803d';
    txt.title = 'Active Buildings (non-Lemle) group · last fetched ' + synced.toLocaleString();
  } catch (e) {
    txt.textContent = '? sync status unavailable';
    txt.style.color = '#8b7b6b';
  }
}

async function pmRefreshMondaySync() {
  const btn = document.getElementById('pm-sync-refresh');
  const txt = document.getElementById('pm-sync-text');
  if (btn) { btn.disabled = true; btn.textContent = '⟳ Syncing…'; btn.style.opacity = '0.7'; }
  if (txt) { txt.textContent = 'Pulling from Monday.com…'; txt.style.color = '#4a3f35'; }
  try {
    const res = await fetch('/api/sync-monday-now', {method: 'POST'});
    const data = await res.json();
    if (data.error) {
      if (txt) { txt.textContent = '⚠ Sync failed: ' + data.error; txt.style.color = '#b91c1c'; }
    } else {
      // Reload the local caches so the dropdown reflects the fresh Monday data.
      await loadInitialData();
    }
  } catch (e) {
    if (txt) { txt.textContent = '⚠ Sync error: ' + e.message; txt.style.color = '#b91c1c'; }
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '↻ Refresh'; btn.style.opacity = '1'; }
  }
}

function populatePMSelect() {
  const select = document.getElementById('pm-select');
  select.innerHTML = '<option value="">-- Choose your name --</option>';

  // FA dir 2026-05-24: only PMs in Monday's Active Buildings (non-Lemle) group
  // make it here, because _ensure_monday_fresh prunes BuildingAssignment rows
  // for inactive entities. But a PM can still exist as a User row with zero
  // current assignments (e.g., they moved on, or the building was archived).
  // Filter the dropdown to PMs with ≥1 active assignment whose budget is also
  // active (not archived_inactive). Sort alphabetically for usability.
  const activeBudgetCodes = new Set(
    allBudgets
      .filter(b => b.status !== 'archived_inactive')
      .map(b => b.entity_code)
  );
  const pmsWithActiveAssignment = new Set(
    allAssignments
      .filter(a => a.role === 'pm' && activeBudgetCodes.has(a.entity_code))
      .map(a => a.user_id)
  );
  const pmUsers = allUsers
    .filter(u => u.role === 'pm' && pmsWithActiveAssignment.has(u.id))
    .sort((a, b) => (a.name || '').localeCompare(b.name || ''));
  pmUsers.forEach(user => {
    const opt = document.createElement('option');
    opt.value = user.id;
    opt.textContent = user.name;
    select.appendChild(opt);
  });

  // Surface an empty-state if Monday says nobody's active — helps the FA notice
  // a botched sync vs. assuming the dropdown is just slow.
  if (pmUsers.length === 0) {
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = '(no PMs found — check Monday sync)';
    opt.disabled = true;
    select.appendChild(opt);
  }
}

function getBuildingName(entityCode) {
  const building = allBuildings.find(b => b.entity_code === entityCode);
  return building ? (building.building_name || building.name || entityCode) : entityCode;
}

function calcDays(updatedAt) {
  if (!updatedAt) return 0;
  return Math.floor((Date.now() - new Date(updatedAt).getTime()) / 86400000);
}

function renderBuildings(userId) {
  const grid = document.getElementById('buildings-grid');
  const summary = document.getElementById('pm-summary');
  const userAssignments = allAssignments.filter(a => a.user_id === userId && a.role === 'pm');

  // FA dir 2026-05-24: only show the PM's currently-assigned, NON-archived
  // buildings. Removed the prior "demo fallback" that exposed all 147 budgets
  // when a PM had zero assignments — that misled selectors into thinking they
  // managed the whole portfolio. archived_inactive = pruned from Monday's
  // Active Buildings (non-Lemle) group.
  let buildingList = userAssignments
    .map(a => {
      const budget = allBudgets.find(b => b.entity_code === a.entity_code);
      return { entity_code: a.entity_code, budget };
    })
    .filter(item => item.budget && item.budget.status !== 'archived_inactive');

  if (buildingList.length === 0) {
    // Empty state — different message depending on whether the PM has any
    // assignments at all (vs. all of them are archived).
    grid.style.display = 'none';
    summary.style.display = 'none';
    let emptyEl = document.getElementById('pm-empty-state');
    if (!emptyEl) {
      emptyEl = document.createElement('div');
      emptyEl.id = 'pm-empty-state';
      emptyEl.style.cssText = 'padding:32px 24px;text-align:center;color:#8b7b6b;background:#fffbeb;border:1px solid #fcd34d;border-radius:10px;margin-top:16px;font-size:14px;';
      const parent = grid.parentElement;
      if (parent) parent.appendChild(emptyEl);
    }
    emptyEl.style.display = 'block';
    emptyEl.innerHTML = userAssignments.length === 0
      ? 'No buildings assigned to this PM on Monday\'s Active Buildings (non-Lemle) list. If this looks wrong, ask your FA to refresh the Monday sync.'
      : 'All assigned buildings are archived. Ask your FA to check Monday assignments.';
    return;
  } else {
    const emptyEl = document.getElementById('pm-empty-state');
    if (emptyEl) emptyEl.style.display = 'none';
  }

  // Classify each building
  const actionStatuses = ['pm_pending', 'pm_in_progress', 'returned'];
  const doneStatuses = ['approved', 'ar_pending', 'ar_complete'];
  buildingList.forEach(item => {
    const s = item.budget ? item.budget.status : null;
    const days = item.budget ? calcDays(item.budget.updated_at) : 0;
    item.days = days;
    if (actionStatuses.includes(s)) { item.tier = 0; item.cardClass = days >= 14 ? 'card-action' : 'card-waiting'; }
    else if (s === 'fa_review') { item.tier = 1; item.cardClass = 'card-waiting'; }
    else if (doneStatuses.includes(s)) { item.tier = 2; item.cardClass = 'card-done'; }
    else { item.tier = 3; item.cardClass = 'card-notready'; }
  });
  // Sort: action items first (longest waiting), then done, then not-ready
  buildingList.sort((a, b) => a.tier - b.tier || b.days - a.days);

  // Summary chips
  const needReview = buildingList.filter(i => i.tier === 0).length;
  const awaitingFA = buildingList.filter(i => i.tier === 1).length;
  const done = buildingList.filter(i => i.tier === 2).length;
  summary.innerHTML = '<span class="summary-chip chip-total">' + buildingList.length + ' buildings</span>' +
    (needReview ? '<span class="summary-chip chip-action">' + needReview + ' need your review</span>' : '') +
    (awaitingFA ? '<span class="summary-chip chip-waiting">' + awaitingFA + ' awaiting FA</span>' : '') +
    (done ? '<span class="summary-chip chip-done">' + done + ' approved</span>' : '');
  summary.style.display = 'flex';

  grid.innerHTML = '';
  grid.style.display = 'flex';

  buildingList.forEach(item => {
    const buildingName = item.budget ? (item.budget.building_name || getBuildingName(item.entity_code)) : getBuildingName(item.entity_code);
    const budgetStatus = item.budget ? item.budget.status : null;
    const isEditable = editableStatuses.includes(budgetStatus);
    const statusLabel = budgetStatus ? formatStatus(budgetStatus) : 'No Budget';
    const pillClass = budgetStatus ? 'pill-' + budgetStatus : 'pill-draft';

    // Days badge
    let daysBadge = '';
    if (item.tier <= 1 && item.days > 0) {
      const dc = item.days >= 14 ? 'days-red' : item.days >= 7 ? 'days-yellow' : 'days-green';
      daysBadge = '<span class="days-badge ' + dc + '">' + item.days + 'd waiting</span>';
    }

    // Action button
    let btn = '';
    if (item.tier === 0 && budgetStatus === 'pm_pending') {
      btn = '<a href="/pm/' + item.entity_code + '" class="card-btn card-btn-primary">Start Review &rarr;</a>';
    } else if (item.tier === 0) {
      btn = '<a href="/pm/' + item.entity_code + '" class="card-btn card-btn-primary">Continue Review &rarr;</a>';
    } else if (item.tier === 1) {
      btn = '<a href="/pm/' + item.entity_code + '" class="card-btn card-btn-secondary">View &rarr;</a>';
    } else if (item.tier === 2) {
      btn = '<a href="/pm/' + item.entity_code + '" class="card-btn card-btn-secondary">View &rarr;</a>';
    } else {
      btn = '<span style="font-size:11px; color:var(--gray-500);">Not sent to PM yet</span>';
    }

    const card = document.createElement('div');
    card.className = 'building-card ' + item.cardClass;
    card.innerHTML =
      '<div class="card-top"><h3>' + buildingName + '<span>Entity ' + item.entity_code + '</span></h3>' +
        '<div style="display:flex; gap:8px; align-items:center;">' + daysBadge + '<span class="status-pill ' + pillClass + '">' + statusLabel + '</span></div></div>' +
      '<div class="card-actions">' + btn + '</div>';
    grid.appendChild(card);
  });
}

document.getElementById('pm-select').addEventListener('change', (e) => {
  const userId = parseInt(e.target.value);
  if (!userId) {
    document.getElementById('buildings-grid').style.display = 'none';
    return;
  }
  renderBuildings(userId);
});

// Initialize on page load
loadInitialData();
</script>
</body>
</html>
"""

