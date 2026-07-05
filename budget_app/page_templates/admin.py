# Extracted from workflow.py 2026-07-05 (clean-architecture tranche 1).
# BYTE-IDENTICAL constant — template edits happen HERE now. Keep the
# string style unchanged (raw vs non-raw matters for JS escapes; see
# the wizard-template-js-escapes memory / check_template_js gate).

ADMIN_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>User Management - Century Management</title>
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
    --blue-light: #f5efe7;
    --green: #057a55;
    --green-light: #def7ec;
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
  header {
    background: linear-gradient(135deg, #2c2825 0%, #3d322a 100%);
    color: white;
    padding: 30px 20px;
  }
  header a { color: white; text-decoration: none; font-size: 14px; }
  header a:hover { text-decoration: underline; }
  header h1 { font-size: 28px; font-weight: 700; }
  header p { font-size: 14px; opacity: 0.85; margin-top: 4px; }
  .container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 32px 20px;
  }
  .section {
    background: white;
    border-radius: 12px;
    padding: 28px;
    margin-bottom: 24px;
    border: 1px solid var(--gray-200);
  }
  .sync-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 12px;
  }
  .sync-bar h2 {
    font-size: 18px;
    font-weight: 600;
    color: var(--blue);
    margin: 0;
  }
  .sync-bar .meta {
    font-size: 13px;
    color: var(--gray-500);
  }
  .sync-right {
    display: flex;
    gap: 10px;
    align-items: center;
  }
  button {
    background: var(--blue);
    color: white;
    border: none;
    padding: 10px 20px;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.15s;
  }
  button:hover { background: #1542b8; }
  button:disabled { opacity: 0.6; cursor: not-allowed; }
  .btn-sync { background: #6366f1; }
  .btn-sync:hover { background: #4f46e5; }
  .search-input {
    width: 100%;
    padding: 10px 14px;
    border: 1px solid var(--gray-300);
    border-radius: 8px;
    font-size: 14px;
    margin-bottom: 16px;
  }
  .search-input:focus {
    outline: none;
    border-color: var(--blue);
    box-shadow: 0 0 0 3px var(--blue-light);
  }
  table {
    width: 100%;
    border-collapse: collapse;
  }
  th {
    background: var(--gray-100);
    padding: 10px 12px;
    text-align: left;
    font-weight: 600;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--gray-500);
    border-bottom: 1px solid var(--gray-200);
  }
  td {
    padding: 10px 12px;
    border-bottom: 1px solid var(--gray-200);
    font-size: 14px;
  }
  tr:hover { background: var(--gray-50); }
  .role-pill {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
  }
  .role-fa { background: #e0e7ff; color: #3730a3; }
  .role-pm { background: #fef3c7; color: #92400e; }
  .count-badge {
    display: inline-block;
    background: var(--gray-100);
    color: var(--gray-700);
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 13px;
    font-weight: 600;
    margin-left: 8px;
  }
  .empty-state {
    text-align: center;
    padding: 40px 20px;
    color: var(--gray-500);
  }
  .empty-state p { margin-bottom: 12px; }
  #syncResults {
    display: none;
    background: var(--gray-50);
    border-radius: 8px;
    padding: 14px 16px;
    font-size: 13px;
    margin-top: 16px;
  }
</style>
</head>
<body>
<header>
  <a href="/">← Home</a>
  <h1>User Management</h1>
  <p>Buildings, FAs, and PMs synced from Monday.com</p>
</header>
<div class="container">

  <div class="section" style="border-left: 4px solid #6366f1;">
    <div class="sync-bar">
      <div>
        <h2>Monday.com Sync</h2>
        <div class="meta">Pull building assignments from the Building Master List</div>
      </div>
      <div class="sync-right">
        <span id="syncStatus" style="font-size:13px; color:var(--gray-500);"></span>
        <button onclick="syncMonday()" id="syncBtn" class="btn-sync">Sync Now</button>
      </div>
    </div>
    <div id="syncResults"></div>
  </div>

  <div class="section">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
      <div style="display:flex; align-items:center;">
        <h2 style="font-size:18px; font-weight:600; color:var(--blue); margin:0;">Buildings</h2>
        <span class="count-badge" id="buildingCount">0</span>
      </div>
    </div>
    <input type="text" class="search-input" id="buildingSearch" placeholder="Search by entity code, address, FA, or PM..." oninput="filterBuildings()">
    <table id="buildings-table">
      <thead>
        <tr>
          <th>Entity</th>
          <th>Building Name</th>
          <th>Financial Analyst</th>
          <th>Property Manager</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
    <div id="emptyState" class="empty-state" style="display:none;">
      <p>No buildings synced yet.</p>
      <button onclick="syncMonday()" class="btn-sync">Sync from Monday.com</button>
    </div>
  </div>

</div>

<script>
const ROLE_LABELS = { fa: 'Financial Analyst', pm: 'Property Manager', admin: 'Admin' };

async function syncMonday() {
  const btn = document.getElementById('syncBtn');
  const status = document.getElementById('syncStatus');
  const results = document.getElementById('syncResults');

  btn.disabled = true;
  btn.textContent = 'Syncing...';
  status.textContent = 'Fetching from Monday.com...';

  try {
    const resp = await fetch('/api/sync-monday-fetch');
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'Fetch failed');

    status.textContent = `Syncing ${data.buildings.length} buildings...`;

    const syncResp = await fetch('/api/sync-monday', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data.buildings)
    });
    const syncData = await syncResp.json();
    if (!syncResp.ok) throw new Error(syncData.error || 'Sync failed');

    const s = syncData.stats;
    results.style.display = 'block';
    results.innerHTML = `
      <strong style="color:#057a55;">Sync complete</strong><br>
      Buildings: <strong>${s.buildings_synced}</strong> &nbsp;|&nbsp;
      Users created: <strong>${s.users_created}</strong> &nbsp;|&nbsp;
      Assignments updated: <strong>${s.assignments_created}</strong>
    `;
    status.textContent = '';
    loadBuildingView();
  } catch(e) {
    status.textContent = '';
    results.style.display = 'block';
    results.innerHTML = '<strong style="color:#e02424;">Error:</strong> ' + e.message;
  }

  btn.disabled = false;
  btn.textContent = 'Sync Now';
}

async function loadBuildingView() {
  try {
    const [buildingsRes, assignmentsRes] = await Promise.all([
      fetch('/api/buildings'),
      fetch('/api/assignments')
    ]);
    const buildings = await buildingsRes.json();
    const assignments = await assignmentsRes.json();
    renderBuildingTable(buildings, assignments);
  } catch (err) {
    console.error('Failed to load data:', err);
  }
}

function renderBuildingTable(buildings, assignments) {
  const tbody = document.querySelector('#buildings-table tbody');
  const emptyState = document.getElementById('emptyState');
  const countBadge = document.getElementById('buildingCount');
  tbody.innerHTML = '';

  // Build a lookup: entity_code -> { fa: name, pm: name }
  const assignMap = {};
  assignments.forEach(a => {
    if (!assignMap[a.entity_code]) assignMap[a.entity_code] = {};
    assignMap[a.entity_code][a.role] = a.user_name || '—';
  });

  if (buildings.length === 0) {
    emptyState.style.display = 'block';
    document.getElementById('buildings-table').style.display = 'none';
    countBadge.textContent = '0';
    return;
  }

  emptyState.style.display = 'none';
  document.getElementById('buildings-table').style.display = 'table';
  countBadge.textContent = buildings.length;

  buildings.sort((a, b) => (a.entity_code || '').localeCompare(b.entity_code || ''));

  buildings.forEach(b => {
    const ec = b.entity_code;
    const roles = assignMap[ec] || {};
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td style="font-weight:600; white-space:nowrap;">${ec}</td>
      <td>${b.building_name || '—'}</td>
      <td>${roles.fa ? '<span class="role-pill role-fa">' + roles.fa + '</span>' : '<span style="color:var(--gray-300);">—</span>'}</td>
      <td>${roles.pm ? '<span class="role-pill role-pm">' + roles.pm + '</span>' : '<span style="color:var(--gray-300);">—</span>'}</td>
    `;
    tbody.appendChild(tr);
  });
}

function filterBuildings() {
  const query = document.getElementById('buildingSearch').value.toLowerCase();
  const rows = document.querySelectorAll('#buildings-table tbody tr');
  let visible = 0;
  rows.forEach(row => {
    const text = row.textContent.toLowerCase();
    const show = text.includes(query);
    row.style.display = show ? '' : 'none';
    if (show) visible++;
  });
  document.getElementById('buildingCount').textContent = visible;
}

// Initialize
loadBuildingView();
</script>
</body>
</html>
"""

