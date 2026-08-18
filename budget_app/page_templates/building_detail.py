# Extracted from workflow.py 2026-07-05 (clean-architecture tranche 1).
# BYTE-IDENTICAL constant — template edits happen HERE now. Keep the
# string style unchanged (raw vs non-raw matters for JS escapes; see
# the wizard-template-js-escapes memory / check_template_js gate).

BUILDING_DETAIL_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Building Detail - Century Management</title>
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
  .top-nav { background: white; border-bottom: 1px solid var(--gray-200); padding: 0 20px; display: flex; align-items: center; height: 48px; position: sticky; top: 0; z-index: 100; }
  .top-nav .nav-brand { font-weight: 700; font-size: 15px; color: var(--blue); text-decoration: none; margin-right: 32px; }
  .top-nav .nav-links { display: flex; gap: 4px; }
  .top-nav .nav-link { padding: 6px 14px; font-size: 13px; font-weight: 500; color: var(--gray-500); text-decoration: none; border-radius: 6px; transition: all 0.15s; }
  .top-nav .nav-link:hover { background: var(--gray-100); color: var(--gray-900); }
  .top-nav .nav-link.active { background: var(--blue-light); color: var(--blue); }
  .top-nav .breadcrumb { margin-left: auto; font-size: 13px; color: var(--gray-500); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 300px; }
  .top-nav .breadcrumb a { color: var(--gray-500); text-decoration: none; }
  .top-nav .breadcrumb a:hover { color: var(--blue); }

  /* ── Toast ── */
  .toast-container { position: fixed; top: 60px; right: 20px; z-index: 9999; display: flex; flex-direction: column; gap: 8px; }
  .toast { padding: 12px 20px; border-radius: 8px; font-size: 14px; font-weight: 500; box-shadow: 0 4px 12px rgba(0,0,0,0.15); animation: slideIn 0.3s ease; max-width: 360px; }
  .toast-success { background: var(--green); color: white; }
  .toast-error { background: var(--red); color: white; }
  .toast-info { background: var(--blue); color: white; }
  @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }

  /* ── Status pipeline ── */
  /* FA directive 2026-05-11 Phase 1: status pipeline compressed from full
     banner to thin strip. Same content, ~60% less vertical space. */
  .status-pipeline { display: flex; align-items: center; gap: 0; background: white; border-radius: 6px; padding: 4px 12px; margin-bottom: 10px; border: 1px solid var(--gray-200); overflow-x: auto; }
  .pipeline-step { display: flex; align-items: center; gap: 5px; padding: 3px 10px; font-size: 11px; font-weight: 600; white-space: nowrap; color: var(--gray-400); }
  .pipeline-step.completed { color: var(--green); }
  .pipeline-step.current { color: var(--blue); background: var(--blue-light); border-radius: 4px; }
  .pipeline-arrow { color: var(--gray-300); font-size: 12px; margin: 0 2px; }

  header {
    background: linear-gradient(135deg, var(--blue) 0%, var(--blue-dark) 100%);
    color: white;
    padding: 24px 20px;
  }
  header h1 { font-size: 24px; font-weight: 700; }
  header p { font-size: 14px; opacity: 0.85; margin-top: 4px; }
  .container { max-width: 1760px; margin: 0 auto; padding: 16px 20px; }
  /* FA directive 2026-05-11 Phase 1: KPI cards compressed from 4 huge
     boxes (~120px tall) to a thin horizontal row (~52px tall). Same data,
     ~60% less vertical real estate. Variance/% Change keep their green
     accent so the headline numbers still pop. */
  .summary-cards {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 8px;
    margin-bottom: 10px;
  }
  .summary-card {
    background: white;
    border-radius: 8px;
    padding: 8px 14px;
    border: 1px solid var(--gray-200);
    text-align: left;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
  }
  .card-value { font-size: 18px; font-weight: 700; color: var(--blue); }
  .card-label { font-size: 10px; color: var(--gray-500); text-transform: uppercase; font-weight: 700; letter-spacing: 0.5px; }
  /* ── Context Strip (collapsible panels side by side) ── */
  .context-strip {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
    margin-bottom: 16px;
  }
  .panel {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 10px;
    overflow: hidden;
  }
  .panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 20px;
    cursor: pointer;
    user-select: none;
  }
  .panel-header:hover { background: var(--gray-50); }
  .panel-header h3 { font-size: 14px; font-weight: 600; color: var(--gray-700); margin: 0; }
  .panel-header .badge { font-size: 11px; font-weight: 500; padding: 2px 8px; border-radius: 10px; }
  .badge-blue { background: var(--blue-light); color: var(--blue); }
  @keyframes pmPulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
  .badge-green { background: var(--green-light); color: var(--green); }
  .badge-amber { background: var(--yellow-light); color: #d97706; }
  .badge-gray { background: var(--gray-100); color: var(--gray-500); }
  .panel-header .chevron { color: var(--gray-400); font-size: 16px; transition: transform 0.2s; }
  .panel-header .chevron.open { transform: rotate(180deg); }
  .panel-body { padding: 0 20px 16px; display: none; }
  .panel-body.open { display: block; }
  .panel-summary { font-size: 12px; color: var(--gray-500); margin-left: 8px; }
  /* ── Checklist items ── */
  .checklist-item { display: flex; align-items: flex-start; gap: 10px; padding: 8px 0; border-bottom: 1px solid var(--gray-100); }
  .checklist-item:last-child { border-bottom: none; }
  .check-icon { width: 20px; height: 20px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 11px; flex-shrink: 0; margin-top: 1px; }
  .check-done { background: var(--green-light); color: var(--green); }
  .check-pending { background: var(--gray-100); color: var(--gray-400); border: 1.5px solid var(--gray-300); }
  .checklist-label { font-size: 13px; font-weight: 500; color: var(--gray-700); }
  .checklist-detail { font-size: 11px; color: var(--gray-400); margin-top: 1px; }
  .section {
    background: white;
    border-radius: 12px;
    padding: 28px;
    border: 1px solid var(--gray-200);
    margin-bottom: 28px;
  }
  .section h2 { font-size: 18px; font-weight: 600; margin-bottom: 20px; color: var(--blue); }
  /* FA directive 2026-05-11 Phase 1: Budget Workbook section header
     compressed — the redundant "Budget Workbook" h2 title is hidden
     (the page IS the workbook). Action buttons collapse to a thin
     right-aligned row. Border still tight blue so the workbook visually
     dominates the page. */
  .workbook-section {
    background: white;
    border: 2px solid var(--blue);
    border-radius: 12px;
    overflow: clip;
    margin-bottom: 16px;
  }
  .workbook-section .workbook-header {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    padding: 6px 12px;
    border-bottom: 1px solid var(--gray-200);
    background: var(--gray-50);
  }
  .workbook-section .workbook-header h2 { display: none; }
  .workbook-section .workbook-header .btn { padding: 5px 12px !important; font-size: 11px !important; }
  table { width: 100%; border-collapse: collapse; }
  th {
    background: var(--gray-100);
    padding: 10px 12px;
    text-align: left;
    font-weight: 600;
    font-size: 12px;
    border-bottom: 1px solid var(--gray-200);
    text-transform: uppercase;
    color: var(--gray-500);
  }
  td { padding: 10px 12px; border-bottom: 1px solid var(--gray-200); font-size: 14px; }
  tr:hover { background: var(--gray-50); }
  .pill {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 600;
  }
  .pill-draft { background: var(--gray-100); color: var(--gray-700); }
  .pill-pm_pending { background: var(--yellow-light); color: #a16207; }
  .pill-pm_in_progress { background: var(--blue-light); color: var(--blue); }
  .pill-fa_review { background: var(--orange-light); color: var(--orange); }
  .pill-approved { background: var(--green-light); color: var(--green); }
  .pill-returned { background: var(--red-light); color: var(--red); }
  button {
    padding: 8px 16px;
    border: none;
    border-radius: 6px;
    font-weight: 600;
    font-size: 13px;
    cursor: pointer;
  }
  .sheet-tab {
    padding: 10px 18px;
    border: none;
    background: var(--gray-100);
    color: var(--gray-500);
    cursor: pointer;
    font-size: 13px;
    font-weight: 600;
    border-radius: 8px 8px 0 0;
    transition: all 0.15s;
  }
  .sheet-tab:hover { background: var(--gray-200); }
  .sheet-tab.active {
    background: white;
    color: var(--blue);
    box-shadow: 0 -2px 0 var(--blue) inset;
  }
  .btn {
    display: inline-block;
    padding: 8px 16px;
    border-radius: 6px;
    font-weight: 600;
    font-size: 13px;
    cursor: pointer;
    text-decoration: none;
  }
  @media (max-width: 768px) {
    .summary-cards { grid-template-columns: repeat(2, 1fr); }
    .context-strip { grid-template-columns: 1fr; }
  }
  /* ── Loading spinner ── */
  .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--gray-200); border-top-color: var(--blue); border-radius: 50%; animation: spin 0.6s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .loading-overlay { text-align: center; padding: 60px 20px; color: var(--gray-500); }

  /* ── Health Pill + Drawer (Variant A, FA directive 2026-05-14 Phase 4) ────
     Replaces the inline summary-cards + inline Readiness Inspector on the
     workbook surface. Trigger: small pill in the top nav. Drawer slides
     from right with KPIs + grouped gates (blockers / warnings / complete).
     The inline summary-cards element is kept in the DOM (CSS-hidden) so
     existing populator JS keeps running into it — the drawer mirrors the
     same data via populateHealthDrawerKpis / populateHealthDrawerActions. */
  .health-pill {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 6px 12px; background: var(--blue-light); color: var(--blue);
    border-radius: 999px; font-size: 12px; font-weight: 600;
    cursor: pointer; border: 1px solid var(--blue); font-family: inherit;
    transition: background 0.15s;
  }
  .health-pill:hover { background: white; }
  .health-pill .badge {
    background: var(--red); color: white;
    padding: 1px 7px; border-radius: 999px;
    font-size: 10px; font-weight: 700;
    min-width: 18px; text-align: center;
  }
  .health-pill .badge.zero { background: var(--green); }

  .drawer-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.3); display: none; z-index: 9998; }
  .drawer-overlay.open { display: block; }
  .drawer {
    position: fixed; top: 0; right: -540px; bottom: 0;
    width: 540px; max-width: 92vw;
    background: white; box-shadow: -8px 0 24px rgba(0,0,0,0.12);
    transition: right 0.28s ease;
    z-index: 9999; overflow-y: auto;
  }
  .drawer.open { right: 0; }
  .drawer-header {
    padding: 16px 22px;
    background: linear-gradient(to right, #fef3c7, white 35%);
    border-bottom: 1px solid var(--gray-200);
    display: flex; align-items: center; gap: 14px;
  }
  .drawer-header h2 { font-size: 18px; font-weight: 700; margin: 0; }
  .drawer-close {
    margin-left: auto; background: none; border: none;
    font-size: 22px; cursor: pointer; color: var(--gray-500);
    padding: 0 4px; line-height: 1;
  }
  .drawer-close:hover { color: var(--gray-900); }
  .drawer-kpis {
    display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px;
    padding: 14px 22px; background: #faf7f0;
  }
  .drawer-kpi {
    background: white; padding: 10px 12px; border-radius: 6px;
    border: 1px solid var(--gray-200);
  }
  .drawer-kpi.pos { background: var(--green-light); border-color: #86efac; }
  .drawer-kpi.warn { background: var(--yellow-light); border-color: #fde68a; }
  .drawer-kpi.bad { background: var(--red-light); border-color: #fca5a5; }
  .drawer-kpi .num { font-size: 16px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .drawer-kpi.pos .num { color: var(--green); }
  .drawer-kpi.warn .num { color: #92400e; }
  .drawer-kpi.bad .num { color: var(--red); }
  .drawer-kpi .lbl { font-size: 9px; color: var(--gray-500); text-transform: uppercase; letter-spacing: 0.04em; margin-top: 2px; }

  .ac-group { padding: 12px 22px; border-bottom: 1px solid var(--gray-100); }
  .ac-group-title { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 8px; }
  .ac-group.blockers .ac-group-title { color: var(--red); }
  .ac-group.warnings .ac-group-title { color: var(--yellow); }
  .ac-group.complete .ac-group-title { color: var(--green); cursor: pointer; user-select: none; }
  .ac-item { display: flex; align-items: flex-start; gap: 10px; padding: 8px 0; border-top: 1px solid var(--gray-100); font-size: 12px; }
  .ac-item:first-of-type { border-top: none; }
  .ac-icon {
    width: 22px; height: 22px; border-radius: 50%;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 11px; font-weight: 700; flex-shrink: 0;
  }
  .ac-icon.bad { background: var(--red-light); color: var(--red); }
  .ac-icon.warn { background: var(--yellow-light); color: #92400e; }
  .ac-icon.ok { background: var(--green-light); color: var(--green); }
  .ac-text { flex: 1; min-width: 0; }
  .ac-label { font-weight: 600; color: var(--gray-900); }
  .ac-detail { font-size: 10px; color: var(--gray-500); margin-top: 2px; line-height: 1.4; }
  .ac-btn {
    padding: 4px 10px; border-radius: 4px;
    font-size: 10px; font-weight: 600;
    background: var(--blue-light); color: var(--blue);
    border: 1px solid transparent; cursor: pointer; white-space: nowrap;
    text-decoration: none; display: inline-block;
  }
  .ac-btn.primary { background: var(--red); color: white; }
  .ac-btn:hover { opacity: 0.85; }
  .complete-collapsed .ac-item { display: none; }

  /* Inline-expand inside an ac-item — used by approved_file_labels to show
     the full list of unmapped labels without navigating away from the drawer.
     FA directive 2026-05-14 Phase 4.2. */
  .ac-expand { margin: 6px 0 0; border-radius: 6px; overflow: hidden; }
  .ac-expand table th, .ac-expand table td { font-family: inherit; }
  .ac-expand .ac-btn { font-size: 11px; padding: 5px 12px; }

  /* Per-row × Delete button on Summary data rows. CSS-only hover reveal
     (was JS onmouseenter handlers — those misfire when the cursor enters
     between cells). FA directive 2026-05-14 Phase 4.5. */
  tr[data-type="d"] .row-del-btn { opacity: 0; transition: opacity 0.15s; }
  tr[data-type="d"]:hover .row-del-btn { opacity: 1; }
  tr[data-type="d"] .row-del-btn:hover { color: var(--red); }

  /* Commercial tab — period table inline-edit affordances (Phase 5.2). */
  .comm-period-table tr:hover .comm-period-del { opacity: 1; }
  .comm-period-del:hover { color: var(--red) !important; }
  .comm-period-table input:hover { border-color: var(--gray-200) !important; background: white !important; }
  .comm-period-table input:focus { border-color: var(--blue) !important; outline: none; background: white !important; }
  /* All-fields inline-edit affordances (Phase 5.3) — every editable field on
     a tenant card hints on hover, focuses on click. Keeps the card quiet
     until you interact. */
  .comm-edit-input:hover { border-color: var(--gray-200) !important; background: white !important; cursor: text; }
  .comm-edit-input:focus { border-color: var(--blue) !important; outline: none; background: white !important; }
  .comm-period-year:hover { border-color: var(--gray-200) !important; background: white !important; }
  .comm-period-year:focus { border-color: var(--blue) !important; outline: none; background: white !important; }

  /* Hide every workbook element whose content is FULLY duplicated in the drawer.
     The populator JS for each one keeps running, but visually the workbook is
     clean. The drawer is the single surface for: KPIs + readiness gate summary.
     FA directive 2026-05-14 Phase 4 (Variant A: Quiet Pill).

     NOTE: #sumWarningsBanner is intentionally NOT hidden — it contains the
     "+ Add Row" buttons for unmapped GLs (the only place that action lives).
     Hiding it broke the FA's ability to add a summary row for an orphan GL.
     The drawer's "No orphan GLs" warning routes to #tab=Summary which scrolls
     here, so the duplication is intentional: drawer = "what's wrong",
     workbook banner = "here's the row-creation action you came for."
     FA directive 2026-05-14 Phase 4.1 (regression fix). */
  .summary-cards,
  #readinessInspector,
  #periodBanner,
  #auditStatusBanner,
  #unifiedStatusBlock { display: none !important; }
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
    <a href="/admin/login?next=/dashboard/{{ entity_code }}" class="nav-link" style="font-size:12px;color:var(--gray-500);" title="Sign in with ADMIN_KEY to access admin endpoints">🔑 Admin</a>
  </div>
  <!-- max-width:none overrides the .breadcrumb 300px cap from the global
       nav CSS — the cluster now holds the breadcrumb text + Health pill
       + Open in Wizard button and needs ~480px. FA directive 2026-05-14 Phase 4. -->
  <div class="breadcrumb" style="display:flex; align-items:center; gap:14px; max-width:none;">
    <span><a href="/dashboard">Dashboard</a> &rsaquo; <span id="breadcrumbName">Loading...</span></span>
    <span style="flex:1"></span>
    <!-- Variant A: Quiet Pill — opens the Health drawer (replaces the old
         "← Action Center" navigation link). FA directive 2026-05-14 Phase 4. -->
    <button class="health-pill" id="healthPill" onclick="openHealthDrawer()" title="Open Health drawer (blockers, warnings, KPIs)">
      <span>⚡ Health</span>
      <span class="badge" id="healthBadge">…</span>
    </button>
    <a href="/wizard/{{ entity_code }}" style="font-size:12px; padding:5px 12px; border:1px solid var(--blue); background:#eff6ff; color:var(--blue); border-radius:4px; text-decoration:none; font-weight:600;" title="Open this building in the Budget Wizard">Open in Wizard &rarr;</a>
  </div>
</nav>

<!-- Toast container -->
<div class="toast-container" id="toastContainer"></div>

<header>
  <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:16px;">
    <div style="flex:1; min-width:0;">
      <h1 id="buildingName">Loading...</h1>
      <p id="buildingMeta"></p>
    </div>
    <!-- FA identity chip — set via /api/whoami; required before edits. FA directive 2026-05-10. -->
    <div id="faIdentityChip" style="flex-shrink:0; display:flex; align-items:center; gap:8px; background:rgba(255,255,255,0.12); border:1px solid rgba(255,255,255,0.25); border-radius:18px; padding:6px 12px; font-size:12px; cursor:pointer;" onclick="faIdentityOpenPicker()" title="Click to identify or switch FA">
      <span id="faIdentityLabel">Identify yourself to edit ▾</span>
    </div>
  </div>
</header>

<!-- FA identity picker modal -->
<div id="faIdentityModal" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,0.45); z-index:9999; align-items:center; justify-content:center;">
  <div style="background:white; border-radius:10px; padding:24px; max-width:420px; width:90%; box-shadow:0 8px 32px rgba(0,0,0,0.2);">
    <h2 style="margin:0 0 6px; font-size:18px; font-weight:700; color:var(--gray-800);">Who are you working as?</h2>
    <p style="margin:0 0 14px; font-size:13px; color:var(--gray-500);">Pick yourself from the list. Saved for 90 days. You can switch anytime via the chip in the header.</p>
    <select id="faIdentitySelect" style="width:100%; padding:10px 12px; font-size:14px; border:1px solid var(--gray-300); border-radius:6px; background:white;">
      <option value="">— Select your name —</option>
    </select>
    <div style="display:flex; gap:8px; justify-content:flex-end; margin-top:16px;">
      <button onclick="faIdentityCancel()" style="padding:8px 14px; font-size:13px; background:transparent; color:var(--gray-700); border:1px solid var(--gray-300); border-radius:6px; cursor:pointer;">Cancel</button>
      <button onclick="faIdentitySave()" id="faIdentitySaveBtn" style="padding:8px 14px; font-size:13px; background:var(--blue); color:white; border:none; border-radius:6px; cursor:pointer; font-weight:600;">Continue</button>
    </div>
  </div>
</div>
<div class="container">
  <!-- Loading state -->
  <div class="loading-overlay" id="loadingState">
    <div class="spinner" style="width:32px; height:32px; border-width:3px; margin:0 auto 12px;"></div>
    <p>Loading building data...</p>
  </div>

  <div id="detailContent" style="display:none;">

  <!-- Status Pipeline -->
  <div class="status-pipeline" id="statusPipeline"></div>

  <!-- Summary Cards -->
  <div class="summary-cards" id="summaryCards"></div>

  <!-- FA directive 2026-05-14 (Dashboard Phase 2 — Action Center
       consolidation): the old Context Strip is replaced by a single
       compact "PM status row". The FA Completion Checklist panel is
       removed entirely — its job is done by the 9-gate Readiness
       Inspector inside the workbook section, which is more actionable
       (each gate has a click-through action button). Hidden divs below
       preserve element IDs so the existing JS populators (lines 9601-
       9672) keep working without defensive null-checks. -->
  <div class="pm-status-row" id="pmPanel" style="background:white; border:1px solid var(--gray-200); border-radius:8px; padding:10px 14px; margin-bottom:12px; display:flex; align-items:center; gap:10px; flex-wrap:wrap; font-size:12px;">
    <strong style="font-size:13px; color:var(--blue, #5a4a3f);">PM Expense Review</strong>
    <span class="badge badge-gray" id="pmBadge"></span>
    <span class="panel-summary" id="pmSummary" style="color:var(--gray-500);"></span>
    <span id="pmHeaderAction" style="margin-left:auto;"></span>
  </div>
  <!-- Detailed PM body content (collapsed by default, expandable). -->
  <details id="pmTrackDetails" style="margin-bottom:12px;">
    <summary style="font-size:11px; color:var(--gray-500); cursor:pointer; padding:4px 12px;">▾ PM detail</summary>
    <div class="panel-body" id="pmTrackContent" style="padding:8px 14px; background:white; border-radius:6px; border:1px solid var(--gray-200); margin-top:4px;"></div>
  </details>
  <!-- Hidden FA Completion Checklist (Readiness Inspector covers this). -->
  <div id="faPanel" style="display:none;" aria-hidden="true">
    <span id="faBadge"></span>
    <span id="faSummary"></span>
    <div id="assemblyContent"></div>
  </div>

  <!-- Pending Edits & Notes Panel — Notes + Invoice Reclasses + Budget Proposals.
       Renamed from "PM Review" because the items here aren't strictly PM-only:
       the FA also reviews + accepts/rejects each one, and Budget Proposals can
       originate from either side.
       FA directive 2026-05-14 (Dashboard Phase 2): reduced to a compact
       counter bar. The badge + label live in a single thin header that
       expands on click. The tabs/tables below are unchanged. -->
  <div class="panel" id="pmReviewPanel" style="display:none; margin-bottom:12px;">
    <div class="panel-header" style="background:linear-gradient(to right,#fefce8,#fef9c3); border-bottom:1px solid #fde68a; padding:8px 14px;" onclick="togglePanel(this)">
      <div style="display:flex; align-items:center; gap:10px;">
        <span style="font-size:12px; font-weight:600; color:var(--gray-700);" title="Items proposed by PM that need FA review/decision (notes, GL re-classifications, budget proposals)">⚠ Pending Edits &amp; Notes</span>
        <span id="pmReviewBadge" style="display:inline-flex; align-items:center; gap:4px; background:var(--orange); color:white; font-size:11px; font-weight:700; padding:2px 9px; border-radius:12px;"><span style="width:6px;height:6px;background:white;border-radius:50%;animation:pmPulse 1.5s infinite;"></span> <span id="pmReviewBadgeText"></span></span>
        <h3 id="pmReviewHiddenTitle" style="display:none;">Pending Edits &amp; Notes</h3>
      </div>
      <span class="chevron" style="font-size:11px;">▾</span>
    </div>
    <div class="panel-body" style="padding:0;">
      <div id="pmReviewTabs" style="display:flex; border-bottom:1px solid var(--gray-200); background:var(--gray-50);">
        <div class="pm-tab active" onclick="switchPmTab(this,'pmNotesContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--blue); cursor:pointer; border-bottom:2px solid var(--blue); background:white;">PM Notes <span id="pmNotesCount" style="background:var(--blue-light); color:var(--blue); font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
        <div class="pm-tab" onclick="switchPmTab(this,'pmReclassContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--gray-500); cursor:pointer; border-bottom:2px solid transparent;">Invoice Reclasses <span id="pmReclassCount" style="background:#fef3c7; color:#92400e; font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
        <div class="pm-tab" onclick="switchPmTab(this,'pmProposalsContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--gray-500); cursor:pointer; border-bottom:2px solid transparent;">Budget Proposals <span id="pmProposalsCount" style="background:#dbeafe; color:#1e40af; font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
      </div>
      <!-- Tab 1: PM Notes -->
      <div id="pmNotesContent" style="padding:16px 20px;">
        <div id="pmNotesEmpty" style="text-align:center; padding:20px; color:var(--gray-400); font-size:13px; display:none;">No PM notes yet.</div>
        <div id="pmNotesContainer"></div>
      </div>
      <!-- Tab 2: Invoice Reclasses -->
      <div id="pmReclassContent" style="padding:16px 20px; display:none;">
        <div id="pmReclassEmpty" style="text-align:center; padding:20px; color:var(--gray-400); font-size:13px; display:none;">No invoice reclasses pending.</div>
        <div id="pmReclassSummary" style="display:none; display:flex; gap:20px; padding:10px 12px; background:var(--gray-50); border-radius:8px; margin-bottom:14px; font-size:12px;"></div>
        <table id="pmReclassTable" style="width:100%; border-collapse:collapse; font-size:13px;">
          <thead><tr>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">From GL</th>
            <th style="font-size:11px; padding:6px 4px; border-bottom:1px solid var(--gray-200);"></th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">To GL</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Invoices</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Amount</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">PM Note</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Action</th>
          </tr></thead>
          <tbody id="pmReclassBody"></tbody>
        </table>
      </div>
      <!-- Tab 3: Budget Proposals -->
      <div id="pmProposalsContent" style="padding:16px 20px; display:none;">
        <div id="pmProposalsEmpty" style="text-align:center; padding:20px; color:var(--gray-400); font-size:13px; display:none;">No PM budget proposals to review.</div>
        <div id="pmProposalsSummary" style="display:none; gap:20px; padding:10px 12px; background:var(--gray-50); border-radius:8px; margin-bottom:14px; font-size:12px;"></div>
        <table id="pmProposalsTable" style="width:100%; border-collapse:collapse; font-size:13px;">
          <thead><tr>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">GL Code</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Description</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Current Budget</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">PM Proposed</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Change</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Method</th>
            <th style="text-align:center; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Status</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Action</th>
          </tr></thead>
          <tbody id="pmProposalsBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Reject/Comment Modal -->
  <div id="proposalModal" style="display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.5); z-index:9999; align-items:center; justify-content:center;">
    <div style="background:white; border-radius:12px; padding:24px; max-width:420px; width:90%; box-shadow:0 20px 60px rgba(0,0,0,0.3);">
      <h3 id="proposalModalTitle" style="margin:0 0 16px; font-size:16px;"></h3>
      <div id="proposalModalOverrideRow" style="margin-bottom:12px; display:none;">
        <label style="font-size:12px; font-weight:600; color:var(--gray-500);">Override Budget Figure ($)</label>
        <input id="proposalModalOverride" type="text" style="width:100%; padding:8px 12px; border:1px solid var(--gray-200); border-radius:6px; margin-top:4px; font-size:14px;" placeholder="Leave blank to revert to formula">
      </div>
      <div style="margin-bottom:16px;">
        <label style="font-size:12px; font-weight:600; color:var(--gray-500);">Note / Reason</label>
        <textarea id="proposalModalNote" rows="3" style="width:100%; padding:8px 12px; border:1px solid var(--gray-200); border-radius:6px; margin-top:4px; font-size:14px; resize:vertical;" placeholder="Add context for this decision..."></textarea>
      </div>
      <div style="display:flex; gap:8px; justify-content:flex-end;">
        <button onclick="closeProposalModal()" style="padding:8px 16px; border:1px solid var(--gray-200); border-radius:6px; background:white; cursor:pointer; font-size:13px;">Cancel</button>
        <button id="proposalModalSubmit" onclick="submitProposalReview()" style="padding:8px 16px; border:none; border-radius:6px; color:white; cursor:pointer; font-size:13px; font-weight:600;"></button>
      </div>
    </div>
  </div>

  <!-- FA directive 2026-05-14 (Dashboard Phase 2): Data Sources panel
       hidden entirely. The Readiness Inspector's "Source files found"
       gate covers the same information (audit upload status, source
       completeness) plus tells the FA whether the audit is also
       CONFIRMED. Hidden div preserves IDs so the existing populator
       JS (lines 8606-...) keeps working without defensive null-checks.
       To re-enable, change display:none to display:block on the
       outer wrapper. -->
  <div class="sources-section" style="display:none;" aria-hidden="true">
    <div onclick="toggleSourcesPanel()" id="sourcesPanelHeader" style="padding:10px 20px; cursor:pointer; display:flex; align-items:center; justify-content:space-between; background:#fafaf7; border-bottom:1px solid transparent;">
      <div style="display:flex; align-items:center; gap:10px;">
        <span style="font-size:14px; font-weight:600; color:var(--blue);">📂 Data Sources</span>
        <span id="sourcesSummary" style="font-size:12px; color:var(--gray-500);">Loading...</span>
      </div>
      <span id="sourcesChevron" style="font-size:12px; color:var(--gray-500); transition:transform 0.2s;">▶</span>
    </div>
    <div id="sourcesPanelBody" style="display:none; padding:12px 20px 16px;">
      <div style="font-size:11px; color:var(--gray-500); margin-bottom:8px;">Click <b>Replace</b> to re-upload an individual source. Other sources are unaffected.</div>
      <table style="width:100%; border-collapse:collapse; font-size:13px;">
        <thead>
          <tr style="border-bottom:1px solid var(--gray-200); text-align:left;">
            <th style="padding:8px 6px; font-size:11px; color:var(--gray-500); text-transform:uppercase; font-weight:600;">Source</th>
            <th style="padding:8px 6px; font-size:11px; color:var(--gray-500); text-transform:uppercase; font-weight:600;">Last Uploaded</th>
            <th style="padding:8px 6px; font-size:11px; color:var(--gray-500); text-transform:uppercase; font-weight:600;">File</th>
            <th style="padding:8px 6px; font-size:11px; color:var(--gray-500); text-transform:uppercase; font-weight:600; text-align:right;">Action</th>
          </tr>
        </thead>
        <tbody id="sourcesTableBody"></tbody>
      </table>
      <input type="file" id="sourcesFilePicker" accept=".xlsx,.xls" style="display:none;" onchange="sourcesOnFilePicked(event)">
      <div id="sourcesUploadStatus" style="margin-top:10px; font-size:12px;"></div>
    </div>
  </div>

  <!-- Budget Workbook (PROMOTED — blue border, primary visual element) -->
  <div class="workbook-section">
    <div class="workbook-header">
      <h2>Budget Workbook</h2>
      <div style="display:flex; gap:8px;">
        <button onclick="openBoardNoticeReview()" id="presLinkBtn" class="btn" style="background:#1e293b; color:white; border:none; font-size:13px; padding:8px 16px; border-radius:6px; cursor:pointer; display:flex; align-items:center; gap:6px;">📊 Board Presentation</button>
        <button onclick="openBuildingInfo()" id="buildingInfoBtn" class="btn" style="background:#fef9ef; color:var(--blue); border:1px solid var(--blue); font-size:13px; padding:8px 16px; border-radius:6px; cursor:pointer; display:flex; align-items:center; gap:6px;">🏢 Building Info</button>
        <a href="" id="downloadExcelBtn" class="btn" style="background:var(--green); color:white; text-decoration:none; font-size:13px; padding:8px 16px; border-radius:6px;">Download Excel</a>
      </div>
    </div>
    <!-- ─── Unified Status Block (FA directive 2026-05-10) ──────────────
         Wraps the 4 status signals (diff strip / readiness / period /
         audit) in a single bordered container so they read as one
         coherent block instead of 4 separate visual stripes. Each child
         keeps its own render function (no logic rewrites — those are
         proven), the wrapper just provides shared visual language.
         Hidden entirely when all 4 children are hidden. -->
    <div id="unifiedStatusBlock" style="background:var(--blue-light); border-bottom:1px solid var(--gray-200); margin:0;">
      <!-- Diff Strip: "what changed since last visit" — renders only when last visit >24h AND there's a delta. -->
      <div id="diffStrip" style="display:none;"></div>
      <!-- Newer-file check (Jacob 2026-07-08): on open, live-scan this building's
           SP folder; if a source file postdates the ingested data, ask the FA
           whether to update the budget with the new figures. -->
      <div id="sourceFreshnessBanner" style="display:none;"></div>
      <!-- Readiness Inspector: 9-gate inline checklist (full version on Action Center). -->
      <div id="readinessInspector" style="display:none;"></div>
      <!-- FA directive 2026-05-14 (Phase 3): periodBanner + auditStatusBanner hidden
           permanently because Action Center now covers them. IDs kept alive (display:none)
           so existing populator JS doesn't need defensive null-checks. The Readiness
           Inspector above still shows period/audit gates inline for quick reference. -->
      <div id="periodBanner" style="display:none !important;"></div>
      <div id="auditStatusBanner" style="display:none !important;"></div>
    </div>
    <div id="sheetTabs" style="display:flex; gap:4px; border-bottom:2px solid var(--gray-200); margin-bottom:0; flex-wrap:wrap; padding:0 24px; background:var(--gray-50);"></div>
    <div id="sheetContent" style="padding:0 24px;"></div>
    <div id="faSaveIndicator" style="font-size:12px; color:var(--green); margin-top:8px; padding:0 24px 12px;"></div>
  </div>

  </div><!-- end detailContent -->
</div>

<script>
const entityCode = '{{ entity_code }}';
const BY = {{ budget_year }};  // Budget year from server config
const BY1 = BY - 1, BY2 = BY - 2, BY3 = BY - 3;

// ─── Universal change-detection for auto-save handlers ─────────────────
// FA directive 2026-05-10: clicking into a cell to inspect it must NOT
// trigger an auto-save. Sites with onblur/onchange handlers that fire
// fetch/PUT calls were saving on every blur, even when the FA didn't
// change anything — flipping cells to "edited" / "OVR" state and
// polluting budget_revisions with no-op rows.
//
// Fix: a document-level focusin listener snapshots every input/textarea/
// select's value when it gains focus. Handlers call _isUnchangedInput(el)
// (or _isUnchangedValue(el, currentVal)) to short-circuit before saving.
// One helper, ~18 sites use it (cellBlur, pctCellBlur, savePrGLNote,
// savePrGLIncrease, _biCcUpd, _biMhUpd, _biAmUpd, assumAutoSave,
// payrollAssumptionChanged, wageIncreaseChanged, prRosterWageIncrChanged,
// updateBonusExtraField, updateBonusExtraAmount, ancUpdLine, prRosterChanged).
document.addEventListener('focusin', (e) => {
  const t = e.target;
  if (t && t.matches && t.matches('input, textarea, select')) {
    const v = t.value;
    t.dataset.focusedVal = (v === undefined || v === null) ? '' : String(v);
  }
}, true);

function _isUnchangedInput(el) {
  if (!el || !el.dataset) return false;
  const focused = el.dataset.focusedVal;
  if (focused === undefined) return false;  // no snapshot — assume changed
  return String(el.value || '') === String(focused);
}

// ─── Newer-file-in-SharePoint check (Jacob 2026-07-08) ──────────────────────
// On open, live-scan THIS building's SP folder (the server compares against
// the same ingest timestamps the dashboard tiles use). If a source file is
// newer than the ingested data, ask the FA whether to update the budget with
// it. Dismiss is per-session AND keyed to the newest file date, so a later
// arrival re-prompts even in the same session.
let _sfStale = [];
(function initSourceFreshness() {
  fetch(`/api/building/${entityCode}/source-freshness`)
    .then(r => r.ok ? r.json() : null)
    .then(d => {
      if (!d || !d.built || !(d.stale || []).length) return;
      _sfStale = d.stale;
      const newestKey = d.stale.map(s => s.sp_modified || '').sort().pop() || '';
      if (sessionStorage.getItem('sf_dismiss_' + entityCode) === newestKey) return;
      const el = document.getElementById('sourceFreshnessBanner');
      if (!el) return;
      const esc = (s) => (s === null || s === undefined ? '' : String(s)).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
      const fmtD = (iso) => { try { const dt = new Date(String(iso).replace(' ', 'T')); return isNaN(dt) ? '' : (dt.getMonth() + 1) + '/' + dt.getDate(); } catch (e) { return ''; } };
      let html = '<div style="display:flex; align-items:flex-start; gap:10px; padding:10px 24px; background:#fffbeb; border-bottom:1px solid #fde68a;">';
      html += '<span style="font-size:16px;">📄</span>';
      html += '<div style="flex:1; font-size:13px; color:#92400e;">';
      html += '<b>Newer file' + (d.stale.length > 1 ? 's' : '') + ' in SharePoint.</b> ';
      html += 'This budget was built from older data — update it with the new figures?';
      d.stale.forEach((s, i) => {
        html += '<div style="margin-top:6px; display:flex; align-items:center; gap:8px; flex-wrap:wrap;">';
        html += '<span style="font-weight:600;">' + esc(s.label) + ':</span>';
        html += '<span style="font-family:ui-monospace,monospace; font-size:12px;">' + esc(s.filename) + '</span>';
        html += '<span style="font-size:12px; color:#b45309;">modified ' + esc(fmtD(s.sp_modified)) + ' · budget loaded ' + esc(fmtD(s.loaded_at)) + '</span>';
        html += '<button type="button" data-sf-idx="' + i + '" style="font-size:12px; font-weight:600; padding:4px 10px; background:#b45309; color:white; border:none; border-radius:4px; cursor:pointer;">Update with new figures</button>';
        if (s.web_url) html += '<a href="' + esc(s.web_url) + '" target="_blank" rel="noopener" style="font-size:12px; color:var(--blue);">Open in SP ↗</a>';
        html += '</div>';
      });
      html += '</div>';
      html += '<button type="button" id="sfDismissBtn" style="font-size:12px; color:#92400e; background:none; border:1px solid #fde68a; border-radius:4px; padding:4px 10px; cursor:pointer; flex-shrink:0;">Dismiss</button>';
      html += '</div>';
      el.innerHTML = html;
      el.style.display = 'block';
      el.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-sf-idx]');
        if (btn) { _sfUpdateSource(parseInt(btn.getAttribute('data-sf-idx'), 10), btn); return; }
        if (e.target.closest('#sfDismissBtn')) {
          sessionStorage.setItem('sf_dismiss_' + entityCode, newestKey);
          el.style.display = 'none';
        }
      });
    })
    .catch(() => {});
})();

function _sfUpdateSource(idx, btn) {
  const s = _sfStale[idx];
  if (!s) return;
  let msg = 'Update ' + s.label + ' from "' + s.filename + '"?\n\n' +
            'This re-ingests the file and refreshes the figures it feeds in this budget.';
  if (s.source_type === 'ysl') {
    msg += '\n\nWARNING: a YSL refresh also RESETS FA adjustments (proposed budget, overrides, accruals, notes) on every GL line present in the new file.';
  } else if (s.source_type === 'approved_2026') {
    msg += '\n\nNote: this replaces the Summary tab prior-budget rows with the new file.';
  }
  if (!confirm(msg)) return;
  btn.disabled = true; btn.textContent = 'Updating…'; btn.style.opacity = '0.6';
  fetch(`/api/wizard/${entityCode}/use-sp-source`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ source_type: s.source_type, item_id: s.item_id,
                           filename: s.filename, web_url: s.web_url }),
  })
    .then(r => r.json().then(d => ({ ok: r.ok, d })))
    .then(({ ok, d }) => {
      if (ok && d && d.ok) {
        showToast(s.label + ' updated from ' + s.filename + ' — reloading…', 'success');
        setTimeout(() => window.location.reload(), 1200);
      } else {
        const err = (d && (d.parse_error || d.error)) || 'unknown error';
        showToast('Update failed: ' + err, 'error');
        btn.disabled = false; btn.textContent = 'Update with new figures'; btn.style.opacity = '1';
      }
    })
    .catch(err => {
      showToast('Update failed: ' + err, 'error');
      btn.disabled = false; btn.textContent = 'Update with new figures'; btn.style.opacity = '1';
    });
}

// ─── Wizard Sidebar (re-entry mode) ─────────────────────────────────────────
(function initWizardSidebar() {
  fetch(`/api/wizard/${entityCode}/status`)
    .then(r => r.json())
    .then(data => {
      if (!data.wizard_completed_at) return;  // Gate mode — no sidebar
      const steps = [
        {n: 1, label: 'Entity', done: true},
        {n: 2, label: 'Sources', done: data.wizard_step >= 2},
        {n: 3, label: 'Portfolio', done: data.wizard_step >= 3},
        {n: 4, label: 'Building', done: data.wizard_step >= 4},
        {n: 5, label: 'Generated', done: data.wizard_step >= 5},
        {n: 6, label: 'Dashboard', done: data.wizard_step >= 6},
      ];
      const ver = data.assumptions_version || 0;
      const doneCount = steps.filter(s => s.done).length;

      const sidebar = document.createElement('div');
      sidebar.id = 'wizardSidebar';
      sidebar.style.cssText = 'position:fixed;right:0;top:48px;width:180px;height:calc(100vh - 48px);background:white;border-left:1px solid var(--gray-200);padding:14px 12px;z-index:90;font-size:11px;transition:transform 0.3s;overflow-y:auto;';

      let html = '<div style="font-size:9px;font-weight:700;color:var(--gray-500);letter-spacing:1px;text-transform:uppercase;margin-bottom:8px;">Wizard</div>';
      html += `<div style="display:flex;align-items:center;gap:8px;padding:8px;border-radius:8px;background:var(--gray-50);border:1px solid var(--gray-200);margin-bottom:10px;"><span style="font-size:16px;font-weight:700;color:var(--green);">${doneCount}/6</span><span style="font-size:9px;color:var(--gray-500);line-height:1.2;">Steps<br>complete</span></div>`;

      steps.forEach(s => {
        const color = s.done ? 'var(--green)' : 'var(--gray-300)';
        const icon = s.done ? '&#10003;' : '&#9675;';
        const link = s.n <= 4 ? `/wizard/${entityCode}` : '#';
        html += `<a href="${link}" style="display:flex;align-items:center;gap:6px;padding:4px 6px;border-radius:4px;text-decoration:none;color:${s.done ? 'var(--green)' : 'var(--gray-500)'};font-size:10px;margin-bottom:2px;">${icon} ${s.label}</a>`;
      });

      if (ver > 0) {
        html += `<div style="margin-top:10px;display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:8px;font-size:9px;background:#f3e8ff;color:#7c3aed;font-weight:600;">Assumptions v${ver}</div>`;
      }

      // FA directive 2026-05-05: surface notable issues (e.g. orphaned
      // Interest Income GL 4800 with data but no summary row) as a Notes
      // section in the wizard sidebar so the FA sees them on every page load.
      const notes = Array.isArray(data.notes) ? data.notes : [];
      if (notes.length > 0) {
        html += `<div style="margin-top:12px;border-top:1px solid var(--gray-200);padding-top:10px;">`;
        html += `<div style="font-size:9px;font-weight:700;color:#92400e;letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">⚠️ Notes (${notes.length})</div>`;
        notes.forEach(n => {
          const titleEsc = (n.title || '').replace(/</g, '&lt;');
          const msgEsc = (n.message || '').replace(/</g, '&lt;');
          // Severity color mapping. Default to amber (medium) — most notes are
          // FA-actionable not error-state.
          const sev = n.severity || 'medium';
          const bg = sev === 'high' ? '#fef3c7' : '#fffbeb';
          const border = sev === 'high' ? '#fde68a' : '#fef3c7';
          const titleColor = sev === 'high' ? '#92400e' : '#a16207';
          const bodyColor = sev === 'high' ? '#78350f' : '#854d0e';
          html += `<div style="font-size:10px;background:${bg};border:1px solid ${border};padding:7px 9px;border-radius:5px;margin-bottom:6px;line-height:1.35;">`;
          html += `<div style="font-weight:700;color:${titleColor};margin-bottom:3px;">${titleEsc}</div>`;
          html += `<div style="color:${bodyColor};">${msgEsc}</div>`;
          // GL code chip list (when present) helps the FA find the GL fast
          if (Array.isArray(n.gl_codes) && n.gl_codes.length) {
            const chips = n.gl_codes.map(g => `<code style="background:rgba(0,0,0,0.05);padding:1px 4px;border-radius:3px;font-size:9px;">${g}</code>`).join(' ');
            html += `<div style="margin-top:4px;">${chips}</div>`;
          }
          html += `</div>`;
        });
        html += `</div>`;
      }

      // FA directive 2026-05-14: dedicated "Approved-budget label check"
      // card. Pulls /api/wizard/<ec>/scan-findings and shows the FA which
      // labels in her approved 2026 file won't aggregate to canonical
      // rows BEFORE she imports. If no scan exists yet, the endpoint
      // runs one inline (~3s) and caches it. The card placeholder
      // renders immediately; the data fills in when the fetch resolves.
      html += `<div id="scanFindingsCard" style="margin-top:12px;border-top:1px solid var(--gray-200);padding-top:10px;">`;
      html += `<div style="font-size:9px;font-weight:700;color:var(--gray-500);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Pre-import label check</div>`;
      html += `<div id="scanFindingsBody" style="font-size:10px;color:var(--gray-500);font-style:italic;">Loading...</div>`;
      html += `</div>`;

      html += `<div style="margin-top:12px;"><button onclick="document.getElementById(\'wizardSidebar\').style.transform=\'translateX(180px)\'" style="font-size:9px;border:none;background:var(--gray-100);color:var(--gray-500);padding:4px 8px;border-radius:4px;cursor:pointer;width:100%;">Collapse</button></div>`;

      sidebar.innerHTML = html;
      document.body.appendChild(sidebar);

      // FA dir 2026-06-10 (Jacob): the label-check card is now ACTIONABLE —
      // each flagged label shows its dollars at stake and offers Map /
      // Add row / Ignore (manual override). Resolved labels list how they
      // were resolved, with Undo on FA decisions. State lives in
      // window._slcLast; buttons reference items by index so labels with
      // quotes/parens never travel through onclick strings.
      window._slcAct = function(label, action, target) {
        fetch('/api/wizard/' + entityCode + '/label-action', {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({label: label, action: action, target: target || null})
        }).then(r => r.json()).then(d => {
          if (!d.success) { alert('Action failed: ' + (d.error || 'unknown')); return; }
          window._slcLoad(false);
        }).catch(e => alert('Action failed: ' + e));
      };
      window._slcActIdx = function(i, action) {
        const u = (window._slcLast.unmapped_labels || [])[i];
        if (u) window._slcAct(u.label, action, null);
      };
      window._slcUndoIdx = function(i) {
        const r = (window._slcLast.resolved_labels || [])[i];
        if (r) window._slcAct(r.label, 'clear', null);
      };
      window._slcMapOpen = function(i) {
        const el = document.getElementById('slcMap' + i);
        if (el) el.style.display = (el.style.display === 'none' ? 'block' : 'none');
      };
      window._slcMapApply = function(i) {
        const sel = document.getElementById('slcMapSel' + i);
        const u = (window._slcLast.unmapped_labels || [])[i];
        if (!sel || !sel.value || !u) return;
        window._slcAct(u.label, 'map', sel.value);
      };
      window._slcIgnoreAll = function() {
        const items = (window._slcLast.unmapped_labels || []);
        if (!items.length) return;
        if (!confirm('Override all ' + items.length + ' remaining labels? They will be excluded from this check (undoable per label).')) return;
        Promise.all(items.map(u => fetch('/api/wizard/' + entityCode + '/label-action', {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({label: u.label, action: 'ignore'})
        }))).then(() => window._slcLoad(false));
      };
      window._slcRender = function(sf) {
        window._slcLast = sf;
        const body = document.getElementById('scanFindingsBody');
        if (!body) return;
        const verdict = sf.verdict || 'unknown';
        const bg = verdict === 'clean' ? '#dcfce7' : (verdict === 'warn' ? '#fef3c7' : (verdict === 'fail' ? '#fee2e2' : '#f3f4f6'));
        const border = verdict === 'clean' ? '#86efac' : (verdict === 'warn' ? '#fde68a' : (verdict === 'fail' ? '#fca5a5' : '#e5e7eb'));
        const titleColor = verdict === 'clean' ? '#15803d' : (verdict === 'warn' ? '#a16207' : (verdict === 'fail' ? '#991b1b' : '#6b7280'));
        const icon = verdict === 'clean' ? '✓' : (verdict === 'warn' ? '⚠' : (verdict === 'fail' ? '✕' : '?'));
        const esc = s => String(s || '').replace(/</g, '&lt;');
        const fmtAmt = a => (a === null || a === undefined || isNaN(Number(a))) ? '' : (' · $' + Math.round(Number(a)).toLocaleString());
        let h = '';
        h += '<div style="background:' + bg + ';border:1px solid ' + border + ';padding:8px 10px;border-radius:5px;line-height:1.4;">';
        h += '<div style="font-weight:700;color:' + titleColor + ';font-size:11px;margin-bottom:4px;">' + icon + ' ' + verdict.toUpperCase() + '</div>';
        h += '<div style="color:' + titleColor + ';font-size:10px;">' + esc(sf.verdict_msg) + '</div>';
        const un = sf.unmapped_labels || [];
        if (un.length) {
          h += '<div style="margin-top:6px;font-size:9px;font-weight:600;color:' + titleColor + ';">Needs a decision:</div>';
          const opts = (sf.building_row_labels || []).map(l => '<option value="' + esc(l).replace(/"/g, '&quot;') + '">' + esc(l) + '</option>').join('');
          un.forEach((u, i) => {
            const sug = u.suggested ? ' <span style="color:#6b7280;">→ ' + esc(u.suggested) + '</span>' : '';
            h += '<div style="font-size:9px;color:' + titleColor + ';margin-top:4px;">• <code style="background:rgba(0,0,0,0.05);padding:0 3px;font-size:9px;">' + esc(u.label) + '</code>' + fmtAmt(u.amount) + sug + '</div>';
            h += '<div style="margin:2px 0 0 10px;">'
               + '<button onclick="window._slcMapOpen(' + i + ')" style="font-size:8px;border:1px solid #d1d5db;background:white;color:#374151;padding:1px 6px;border-radius:3px;cursor:pointer;" title="Point this file label at an existing summary row">Map to…</button> '
               + '<button onclick="window._slcActIdx(' + i + ',\'add_row\')" style="font-size:8px;border:1px solid #d1d5db;background:white;color:#374151;padding:1px 6px;border-radius:3px;cursor:pointer;" title="Create a row with this label on the summary so the amount has a home">Add row</button> '
               + '<button onclick="window._slcActIdx(' + i + ',\'ignore\')" style="font-size:8px;border:1px solid #d97706;background:#fffbeb;color:#92400e;padding:1px 6px;border-radius:3px;cursor:pointer;" title="Manual override — acknowledged, excluded from this check (undoable)">Ignore</button>'
               + '</div>';
            h += '<div id="slcMap' + i + '" style="display:none;margin:3px 0 0 10px;">'
               + '<select id="slcMapSel' + i + '" style="font-size:9px;max-width:150px;"><option value="">— pick a row —</option>' + opts + '</select> '
               + '<button onclick="window._slcMapApply(' + i + ')" style="font-size:8px;border:none;background:#0369a1;color:white;padding:2px 6px;border-radius:3px;cursor:pointer;">OK</button>'
               + '</div>';
          });
          if (un.length > 1) {
            h += '<div style="margin-top:6px;"><button onclick="window._slcIgnoreAll()" style="font-size:9px;border:1px solid #d97706;background:#fffbeb;color:#92400e;padding:2px 8px;border-radius:3px;cursor:pointer;font-weight:600;">Override all ' + un.length + ' (ignore)</button></div>';
          }
        }
        const res = sf.resolved_labels || [];
        if (res.length) {
          h += '<div style="margin-top:6px;font-size:9px;font-weight:600;color:#15803d;">✓ ' + res.length + ' resolved:</div>';
          res.forEach((r, i) => {
            const how = r.how === 'building_row' ? 'has its own row here' :
                        (r.how === 'ignored' ? 'overridden by FA' :
                         (r.how === 'mapped_by_fa' ? ('→ ' + esc(r.target) + ' (FA)') : esc(r.how)));
            const undo = (r.how === 'ignored' || r.how === 'mapped_by_fa')
              ? ' <a href="javascript:void(0)" onclick="window._slcUndoIdx(' + i + ')" style="color:#0369a1;font-size:8px;text-decoration:underline;">Undo</a>' : '';
            h += '<div style="font-size:9px;color:#15803d;margin-top:2px;">• <code style="background:rgba(0,0,0,0.05);padding:0 3px;font-size:9px;">' + esc(r.label) + '</code> <span style="color:#6b7280;">' + how + '</span>' + undo + '</div>';
          });
        }
        if (sf.file_name) h += '<div style="margin-top:6px;font-size:9px;color:#6b7280;">File: <code style="font-size:9px;">' + esc((sf.file_name || '').slice(0, 40)) + '</code></div>';
        if (sf.scanned_at) h += '<div style="margin-top:2px;font-size:9px;color:#9ca3af;">Scanned ' + new Date(sf.scanned_at).toLocaleString() + '</div>';
        h += '<div style="margin-top:6px;"><button onclick="window._slcLoad(true)" style="font-size:9px;border:1px solid #d1d5db;background:white;color:#374151;padding:3px 8px;border-radius:3px;cursor:pointer;">Re-scan</button></div>';
        h += '</div>';
        body.innerHTML = h;
      };
      window._slcLoad = function(refresh) {
        const body = document.getElementById('scanFindingsBody');
        if (!body) return;
        if (refresh) body.innerHTML = '<span style="font-style:italic;color:#6b7280;">Re-scanning…</span>';
        fetch('/api/wizard/' + entityCode + '/scan-findings' + (refresh ? '?refresh=1' : ''), refresh ? {method: 'POST'} : undefined)
          .then(r => r.json()).then(window._slcRender)
          .catch(() => { body.innerHTML = '<span style="color:#9ca3af;">scan unavailable</span>'; });
      };
      window._slcLoad(false);

      // Adjust main content + nav so the wizard sidebar doesn't overlap the
      // breadcrumb's "Open in Wizard" link (which was getting truncated).
      const adjustForSidebar = (open) => {
        const container = document.querySelector('.container');
        const nav = document.querySelector('nav');
        const margin = open ? '180px' : '0px';
        if (container) container.style.marginRight = margin;
        if (nav) nav.style.paddingRight = open ? '180px' : '0px';
      };

      // Persistent show/hide toggle. Restored from localStorage so the FA's
      // preference sticks across page loads. Default: visible.
      const STATE_KEY = 'wizardSidebarOpen.' + entityCode;
      const saved = localStorage.getItem(STATE_KEY);
      const startOpen = saved === null ? true : saved === '1';

      // Floating "Show Wizard" tab — always rendered, always clickable, sits
      // on the right edge so the FA can pop the sidebar back without hunting.
      const toggleTab = document.createElement('button');
      toggleTab.id = 'wizardSidebarToggle';
      toggleTab.style.cssText = 'position:fixed;right:0;top:120px;writing-mode:vertical-rl;transform:rotate(180deg);background:var(--blue);color:white;border:none;padding:10px 6px;border-radius:6px 0 0 6px;font-size:11px;font-weight:600;letter-spacing:1px;cursor:pointer;z-index:91;box-shadow:-2px 0 8px rgba(0,0,0,0.1);';
      toggleTab.textContent = 'WIZARD';
      const setOpen = (open) => {
        sidebar.style.transform = open ? 'translateX(0)' : 'translateX(180px)';
        toggleTab.style.display = open ? 'none' : 'block';
        adjustForSidebar(open);
        localStorage.setItem(STATE_KEY, open ? '1' : '0');
      };
      toggleTab.onclick = () => setOpen(true);
      document.body.appendChild(toggleTab);

      // Replace the existing in-sidebar "Collapse" button so it routes through
      // setOpen() (which also shows the toggle pill).
      setTimeout(() => {
        const collapseBtn = sidebar.querySelector('button');
        if (collapseBtn) collapseBtn.onclick = () => setOpen(false);
      }, 0);

      setOpen(startOpen);
    })
    .catch(() => {});  // Silently skip if wizard API fails
})();

function togglePanel(header) {
  const body = header.nextElementSibling;
  const chevron = header.querySelector('.chevron');
  body.classList.toggle('open');
  chevron.classList.toggle('open');
}

// ─── Sources Panel ─────────────────────────────────────────────────────────
let _sourcesData = null;
let _sourcesPendingKey = null;  // which source the user clicked Replace on

function toggleSourcesPanel() {
  const body = document.getElementById('sourcesPanelBody');
  const chev = document.getElementById('sourcesChevron');
  const open = body.style.display === 'none' || body.style.display === '';
  body.style.display = open ? 'block' : 'none';
  if (chev) chev.style.transform = open ? 'rotate(90deg)' : 'rotate(0deg)';
  if (open && !_sourcesData) loadSources();
}

function _fmtSourceDate(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    const m = d.getMonth() + 1, day = d.getDate(), y = String(d.getFullYear()).slice(-2);
    let h = d.getHours(), mm = String(d.getMinutes()).padStart(2, '0');
    const ap = h >= 12 ? 'p' : 'a';
    h = h % 12 || 12;
    return m + '/' + day + '/' + y + ' ' + h + ':' + mm + ap;
  } catch (e) { return '—'; }
}

async function loadSources() {
  const statusEl = document.getElementById('sourcesSummary');
  const body = document.getElementById('sourcesTableBody');
  try {
    const r = await fetch('/api/entity/' + entityCode + '/sources');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    _sourcesData = data;
    const rows = [
      { key: 'ysl',                 label: 'YSL Annual Budget',     hint: 'Yardi — creates GL lines' },
      { key: 'expense_distribution', label: 'Expense Distribution', hint: 'Yardi — accrual adjustments' },
      { key: 'ap_aging',             label: 'AP Aging',             hint: 'Yardi — unpaid bills' },
      { key: 'maint_proof',          label: 'Maintenance Proof',    hint: 'Yardi — unit-level maint' },
      { key: 'audited_financials',   label: 'Audited Financials',   hint: 'PDF — 2024 actual' }
    ];
    let loaded = 0;
    body.innerHTML = rows.map(r => {
      const s = data[r.key] || {};
      const date = _fmtSourceDate(s.last_uploaded);
      const file = s.filename ? (s.filename.length > 32 ? s.filename.slice(0, 29) + '...' : s.filename) : '—';
      const has = !!s.last_uploaded;
      if (has) loaded++;
      const isYslMergeAllowed = (r.key === 'ysl' && (entityCode === '204' || entityCode === '212'));
      let action;
      if (r.key === 'audited_financials') {
        action = '<a href="/audited-financials/bulk-upload" style="font-size:12px; color:var(--blue); text-decoration:none; padding:4px 10px; border:1px solid var(--blue); border-radius:4px;">Manage →</a>';
      } else if (isYslMergeAllowed) {
        action = '<button onclick="sourcesReplace(\'' + r.key + '\')" style="font-size:12px; color:var(--blue); background:#fff; border:1px solid var(--blue); padding:4px 10px; border-radius:4px; cursor:pointer;" title="Wipes edits, rebuilds from scratch">Replace</button>' +
                 ' <button onclick="yslMergeOpen()" style="font-size:12px; color:#fff; background:var(--blue); border:1px solid var(--blue); padding:4px 10px; border-radius:4px; cursor:pointer;" title="Refreshes prior-year/YTD only, preserves all user edits">Merge (Beta)</button>';
      } else {
        action = '<button onclick="sourcesReplace(\'' + r.key + '\')" style="font-size:12px; color:var(--blue); background:#fff; border:1px solid var(--blue); padding:4px 10px; border-radius:4px; cursor:pointer;">Replace</button>';
      }
      return '<tr style="border-bottom:1px solid var(--gray-100);">' +
        '<td style="padding:8px 6px;"><div style="font-weight:600;">' + r.label + '</div><div style="font-size:11px; color:var(--gray-500);">' + r.hint + '</div></td>' +
        '<td style="padding:8px 6px; font-size:12px; color:' + (has ? 'var(--text)' : 'var(--gray-400)') + ';">' + date + '</td>' +
        '<td style="padding:8px 6px; font-size:12px; color:var(--gray-500); font-family:ui-monospace,Consolas,monospace;" title="' + (s.filename || '') + '">' + file + '</td>' +
        '<td style="padding:8px 6px; text-align:right;">' + action + '</td>' +
      '</tr>';
    }).join('');
    statusEl.textContent = loaded + ' of 5 uploaded';
  } catch (e) {
    statusEl.textContent = 'Failed to load';
    body.innerHTML = '<tr><td colspan="4" style="padding:8px; color:var(--red); font-size:12px;">Error loading sources: ' + (e.message || e) + '</td></tr>';
  }
}

function sourcesReplace(key) {
  _sourcesPendingKey = key;
  const picker = document.getElementById('sourcesFilePicker');
  picker.value = '';
  picker.click();
}

async function sourcesOnFilePicked(evt) {
  const file = evt.target.files && evt.target.files[0];
  if (!file || !_sourcesPendingKey) return;
  const key = _sourcesPendingKey;
  _sourcesPendingKey = null;
  const statusEl = document.getElementById('sourcesUploadStatus');
  statusEl.style.color = 'var(--gray-500)';
  statusEl.textContent = 'Uploading ' + file.name + '...';
  try {
    const fd = new FormData();
    fd.append('files', file);
    const r = await fetch('/api/process', { method: 'POST', body: fd });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error('HTTP ' + r.status + ': ' + txt.slice(0, 200));
    }
    const data = await r.json();
    if (data.failed && data.failed.length) {
      statusEl.style.color = 'var(--red)';
      statusEl.textContent = 'Upload failed: ' + data.failed.join('; ');
    } else {
      statusEl.style.color = 'var(--green)';
      const names = (data.success || []).map(s => typeof s === 'string' ? s : (s.filename || 'file')).join(', ');
      statusEl.textContent = '✓ Uploaded ' + (names || file.name) + ' — reload to see changes';
      _sourcesData = null;
      loadSources();
    }
  } catch (e) {
    statusEl.style.color = 'var(--red)';
    statusEl.textContent = 'Upload error: ' + (e.message || e);
  }
}

// Populate summary on page load (without opening the panel)
setTimeout(() => { if (typeof loadSources === 'function' && !_sourcesData) loadSources(); }, 500);

// ─── YSL Merge (Beta) — 204/212 only ───────────────────────────────────
let _yslMergeFile = null;
let _yslMergeDiff = null;

function yslMergeOpen() {
  const modal = document.getElementById('yslMergeModal');
  if (!modal) { _yslMergeInjectModal(); }
  document.getElementById('yslMergeModal').style.display = 'flex';
  document.getElementById('yslMergeStep1').style.display = 'block';
  document.getElementById('yslMergeStep2').style.display = 'none';
  document.getElementById('yslMergeResult').style.display = 'none';
  document.getElementById('yslMergePicker').value = '';
  _yslMergeFile = null;
  _yslMergeDiff = null;
}

function yslMergeClose() {
  const m = document.getElementById('yslMergeModal');
  if (m) m.style.display = 'none';
}

function _yslMergeInjectModal() {
  const html = `
<div id="yslMergeModal" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,0.5); z-index:9999; align-items:center; justify-content:center;">
  <div style="background:#fff; border-radius:10px; max-width:720px; width:92%; max-height:85vh; overflow:hidden; display:flex; flex-direction:column;">
    <div style="padding:16px 20px; border-bottom:1px solid var(--gray-200); display:flex; justify-content:space-between; align-items:center;">
      <div>
        <div style="font-size:16px; font-weight:700;">Merge YSL (Beta) — Entity ${entityCode}</div>
        <div style="font-size:12px; color:var(--gray-500); margin-top:2px;">Refreshes prior-year & YTD only · preserves all FA/PM edits</div>
      </div>
      <button onclick="yslMergeClose()" style="background:none; border:none; font-size:24px; cursor:pointer; color:var(--gray-500);">×</button>
    </div>

    <div id="yslMergeStep1" style="padding:20px; overflow:auto;">
      <div style="background:#fff8e1; border:1px solid #f3d78a; border-radius:6px; padding:12px; margin-bottom:14px; font-size:13px;">
        <b>What this does:</b> Loads the new YSL file, shows you exactly which GL codes would have their Yardi numbers refreshed, and only commits after you click Apply. A full snapshot of the current state is saved before any writes — you can restore with one click if anything looks wrong.
      </div>
      <input type="file" id="yslMergePicker" accept=".xlsx,.xls" onchange="yslMergeOnFilePicked(event)" style="display:block; margin:0 auto;">
      <div id="yslMergeDryStatus" style="font-size:12px; color:var(--gray-500); margin-top:12px; text-align:center;"></div>
    </div>

    <div id="yslMergeStep2" style="display:none; padding:16px 20px; overflow:auto; flex:1;">
      <div id="yslMergeDiffSummary" style="margin-bottom:12px;"></div>
      <div id="yslMergeDiffDetail" style="font-size:12px; max-height:360px; overflow:auto; border:1px solid var(--gray-200); border-radius:6px; padding:10px; background:#fafafa;"></div>
    </div>

    <div id="yslMergeResult" style="display:none; padding:20px;"></div>

    <div style="padding:12px 20px; border-top:1px solid var(--gray-200); display:flex; justify-content:space-between; align-items:center; background:#fafafa;">
      <div id="yslMergeFooterLeft" style="font-size:12px; color:var(--gray-500);"></div>
      <div>
        <button onclick="yslMergeClose()" style="background:#fff; border:1px solid var(--gray-300); padding:6px 14px; border-radius:4px; cursor:pointer;">Close</button>
        <span id="yslMergeCommitPill" style="display:inline-block; background:#f5f5f5; color:var(--gray-500); border:1px dashed var(--gray-400); padding:6px 14px; border-radius:4px; margin-left:8px; font-size:12px;" title="Commit is disabled during beta — this is a preview-only feature.">Preview only · commit disabled</span>
      </div>
    </div>
  </div>
</div>`;
  document.body.insertAdjacentHTML('beforeend', html);
}

async function yslMergeOnFilePicked(evt) {
  const f = evt.target.files && evt.target.files[0];
  if (!f) return;
  _yslMergeFile = f;
  const statusEl = document.getElementById('yslMergeDryStatus');
  statusEl.style.color = 'var(--gray-500)';
  statusEl.textContent = 'Parsing ' + f.name + '...';
  try {
    const fd = new FormData();
    fd.append('file', f);
    fd.append('mode', 'dry_run');
    const r = await fetch('/api/ysl/merge/' + entityCode, { method: 'POST', body: fd });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || ('HTTP ' + r.status));
    _yslMergeDiff = data.diff;
    _yslMergeShowDiff(data);
  } catch (e) {
    statusEl.style.color = 'var(--red)';
    statusEl.textContent = 'Error: ' + (e.message || e);
  }
}

function _yslMergeShowDiff(data) {
  document.getElementById('yslMergeStep1').style.display = 'none';
  document.getElementById('yslMergeStep2').style.display = 'block';
  const diff = data.diff || {};
  const t = diff.totals || {};
  const summary = document.getElementById('yslMergeDiffSummary');
  summary.innerHTML =
    '<div style="display:grid; grid-template-columns:repeat(4,1fr); gap:8px;">' +
      '<div style="background:#fff8e1; border-radius:6px; padding:10px; text-align:center;"><div style="font-size:22px; font-weight:700;">' + (t.updated || 0) + '</div><div style="font-size:11px; text-transform:uppercase; color:var(--gray-500);">Updated</div></div>' +
      '<div style="background:#e8f5e9; border-radius:6px; padding:10px; text-align:center;"><div style="font-size:22px; font-weight:700;">' + (t.inserted || 0) + '</div><div style="font-size:11px; text-transform:uppercase; color:var(--gray-500);">New GLs</div></div>' +
      '<div style="background:#fafafa; border:1px solid var(--gray-200); border-radius:6px; padding:10px; text-align:center;"><div style="font-size:22px; font-weight:700; color:var(--gray-500);">' + (t.orphaned || 0) + '</div><div style="font-size:11px; text-transform:uppercase; color:var(--gray-500);">Orphaned</div></div>' +
      '<div style="background:#f5f5f5; border-radius:6px; padding:10px; text-align:center;"><div style="font-size:22px; font-weight:700;">' + (t.total_gls_in_file || 0) + '</div><div style="font-size:11px; text-transform:uppercase; color:var(--gray-500);">Total in file</div></div>' +
    '</div>' +
    '<div style="font-size:11px; color:var(--gray-500); margin-top:8px;">Orphaned = GLs in the current budget but not in the new YSL; these will be left untouched. User-edit columns (notes, overrides, increase %, PM reclasses) are never modified.</div>';

  const detail = document.getElementById('yslMergeDiffDetail');
  const parts = [];
  if ((diff.updated || []).length) {
    parts.push('<div style="font-weight:700; margin-bottom:4px;">Updated (' + diff.updated.length + ')</div>');
    parts.push('<table style="width:100%; border-collapse:collapse;"><thead><tr style="border-bottom:1px solid var(--gray-300); font-weight:600;"><td style="padding:4px;">GL</td><td style="padding:4px;">Description</td><td style="padding:4px;">Field</td><td style="padding:4px; text-align:right;">Old</td><td style="padding:4px; text-align:right;">New</td></tr></thead><tbody>');
    diff.updated.slice(0, 200).forEach(u => {
      (u.changes || []).forEach(c => {
        parts.push('<tr style="border-bottom:1px solid var(--gray-100);"><td style="padding:4px; font-family:ui-monospace,Consolas,monospace;">' + u.gl_code + '</td><td style="padding:4px;">' + (u.description || '') + '</td><td style="padding:4px; color:var(--gray-500);">' + c.field + '</td><td style="padding:4px; text-align:right; font-family:ui-monospace,Consolas,monospace;">' + Number(c.old).toLocaleString(undefined,{maximumFractionDigits:2}) + '</td><td style="padding:4px; text-align:right; font-family:ui-monospace,Consolas,monospace; font-weight:600;">' + Number(c.new).toLocaleString(undefined,{maximumFractionDigits:2}) + '</td></tr>');
      });
    });
    parts.push('</tbody></table>');
    if (diff.updated.length > 200) parts.push('<div style="font-size:11px; color:var(--gray-500); margin-top:4px;">(showing first 200)</div>');
  }
  if ((diff.inserted || []).length) {
    parts.push('<div style="font-weight:700; margin-top:12px; margin-bottom:4px;">New GLs (' + diff.inserted.length + ')</div>');
    parts.push('<div style="font-family:ui-monospace,Consolas,monospace; font-size:11px;">' + diff.inserted.map(i => i.gl_code + ' — ' + (i.description || '')).join('<br>') + '</div>');
  }
  if (!(diff.updated || []).length && !(diff.inserted || []).length) {
    parts.push('<div style="color:var(--gray-500); padding:12px; text-align:center;">No changes detected. The new YSL matches the current budget\'s Yardi columns.</div>');
  }
  detail.innerHTML = parts.join('');

  document.getElementById('yslMergeFooterLeft').textContent = data.filename + ' · dry-run preview (commit disabled)';
}

// yslMergeCommit / yslMergeRestore are intentionally omitted while the
// feature is in preview-only mode. Backend endpoints exist but return 403
// unless _YSL_MERGE_COMMIT_ENABLED is flipped on in workflow.py. When it is,
// restore the two functions and swap the footer "Preview only" pill for the
// Apply Merge button.

let allSheets = {};  // populated in loadDetail, used by Budget Summary
let YTD_MONTHS = 2;  // updated from API response
let REMAINING_MONTHS = 10;  // updated from API response

const MONTH_ABBR = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function estimateLabel() {
  // e.g. YTD_MONTHS=2 → "Mar-Dec Estimate", YTD_MONTHS=6 → "Jul-Dec Estimate"
  return MONTH_ABBR[YTD_MONTHS] + '-Dec';
}

// FA directive 2026-05-05: render the audit-status banner. Shows the latest
// AuditUpload's progression through Uploaded → Extracted → Mapped → Confirmed.
// Severity:
//   - confirmed: green chip with confirmed_at + by
//   - mapped: amber chip "needs FA confirmation"
//   - extracted: amber chip "needs review/mapping"
//   - uploaded: amber chip "extraction pending"
//   - missing audit_summary: muted "No audit on file" prompt
function renderAuditStatusBanner(data) {
  const banner = document.getElementById('auditStatusBanner');
  if (!banner) return;
  const a = (data && data.audit_summary) ? data.audit_summary : null;
  // Render four-step progression dots for visual scanability.
  const steps = [
    {key: 'uploaded',  label: 'Uploaded'},
    {key: 'extracted', label: 'Extracted'},
    {key: 'mapped',    label: 'Mapped'},
    {key: 'confirmed', label: 'Confirmed'},
  ];
  const stepIdx = (s) => steps.findIndex(x => x.key === s);
  if (!a) {
    banner.style.display = '';
    banner.style.background = '#f9fafb';
    banner.style.color = 'var(--gray-500)';
    banner.innerHTML =
      '<span style="font-weight:600;">📄 Audit:</span> no audited financial uploaded yet for this entity. ' +
      '<a href="/audited-financials/bulk-upload" style="color:var(--blue);text-decoration:none;font-weight:600;margin-left:6px;">Upload now →</a>';
    return;
  }
  const status = (a.status || '').toLowerCase();
  const idx = stepIdx(status);
  const isConfirmed = (status === 'confirmed');
  const fy = a.fiscal_year_end ? ('FY' + a.fiscal_year_end) : 'Audit';
  banner.style.display = '';
  banner.style.background = isConfirmed ? '#f0fdf4' : '#fffbeb';
  banner.style.color = isConfirmed ? '#166534' : '#92400e';
  banner.style.borderBottom = '1px solid ' + (isConfirmed ? '#bbf7d0' : '#fde68a');

  // Build dot row showing progression
  let dotsHtml = '<span style="display:inline-flex;align-items:center;gap:4px;">';
  steps.forEach((s, i) => {
    const reached = i <= idx;
    const isCurrent = (i === idx);
    const dotColor = reached ? (isConfirmed ? '#16a34a' : (isCurrent ? '#d97706' : '#16a34a')) : '#d1d5db';
    const labelColor = reached ? (isConfirmed ? '#166534' : (isCurrent ? '#92400e' : '#15803d')) : 'var(--gray-400)';
    dotsHtml += '<span style="display:inline-flex;align-items:center;gap:4px;">';
    dotsHtml += '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + dotColor + ';' + (isCurrent ? 'box-shadow:0 0 0 2px rgba(217,119,6,0.18);' : '') + '"></span>';
    dotsHtml += '<span style="font-size:11px;font-weight:600;color:' + labelColor + ';">' + s.label + '</span>';
    if (i < steps.length - 1) {
      dotsHtml += '<span style="color:var(--gray-300);font-size:10px;margin:0 2px;">→</span>';
    }
    dotsHtml += '</span>';
  });
  dotsHtml += '</span>';

  let actionHtml = '';
  if (isConfirmed) {
    const when = a.confirmed_at ? a.confirmed_at.slice(0, 10) : '';
    const by = a.confirmed_by ? (' by ' + a.confirmed_by) : '';
    actionHtml = '<span style="margin-left:10px;color:#15803d;">✓ Confirmed' + (when ? ' on ' + when : '') + by + '</span>';
    if (a.review_url) {
      actionHtml += ' &nbsp; <a href="' + a.review_url + '" style="color:var(--blue);text-decoration:none;font-size:11px;font-weight:600;">View →</a>';
    }
  } else {
    let cta = 'Open audit';
    if (status === 'uploaded')  cta = 'Run extraction';
    if (status === 'extracted') cta = 'Map + Confirm';
    if (status === 'mapped')    cta = 'Confirm';
    if (a.review_url) {
      actionHtml = ' &nbsp; <a href="' + a.review_url + '" style="color:#92400e;text-decoration:none;font-weight:700;font-size:11px;">' + cta + ' →</a>';
    }
  }

  banner.innerHTML =
    '<span style="font-weight:600;">📄 ' + fy + ' Audit:</span> &nbsp; ' + dotsHtml + actionHtml;
}

// ─── Readiness Inspector ──────────────────────────────────────────────
// FA directive 2026-05-09. Shows an at-a-glance 8-gate checklist at the
// top of every building's dashboard: Source files / Audit confirmed /
// Period set / Building type / No orphan GLs / No duplicate rows /
// Payroll reviewed / Generated. Each row is a colored dot + 1-line
// detail + click-through action button. Collapses to a single header
// line when all 8 are green.
// ─── Diff Strip + FA Identity ─────────────────────────────────────────
// FA directive 2026-05-10. Renders an amber strip at the top of the
// dashboard listing changes since the FA's last visit (>24h ago) with
// pill-tagged entries (AUTO / SYSTEM / FA). Auto-clears on navigation
// away. The Readiness Inspector keeps a "View recent changes" link to
// re-open the strip after dismissal.
let _faIdentity = null;        // { user_id, name, email, role } or null
let _faAllUsers = null;        // cached FA roster from /api/users (FA-role only)

async function faIdentityLoad() {
  try {
    const resp = await fetch('/api/whoami');
    if (!resp.ok) return null;
    const d = await resp.json();
    if (d && d.user_id) { _faIdentity = d; return d; }
    _faIdentity = null;
    return null;
  } catch (err) {
    _faIdentity = null;
    return null;
  }
}

function faIdentityRenderChip() {
  const chip = document.getElementById('faIdentityChip');
  const label = document.getElementById('faIdentityLabel');
  if (!chip || !label) return;
  if (_faIdentity && _faIdentity.user_id) {
    label.textContent = 'Working as: ' + _faIdentity.name + ' [change]';
    chip.style.background = 'rgba(34,197,94,0.18)';
    chip.style.borderColor = 'rgba(34,197,94,0.5)';
  } else {
    label.textContent = 'Identify yourself to edit ▾';
    chip.style.background = 'rgba(217,119,6,0.18)';
    chip.style.borderColor = 'rgba(251,191,36,0.6)';
  }
}

async function faIdentityFetchRoster() {
  if (_faAllUsers) return _faAllUsers;
  try {
    // active=1 excludes Lemle alumni and other stale FAs (only FAs with
    // BuildingAssignment rows on current-year budgets). Same filter the
    // wizard uses for its FA dropdown.
    const resp = await fetch('/api/users?role=fa&active=1');
    if (!resp.ok) return [];
    const d = await resp.json();
    const list = (d && d.users) ? d.users : (Array.isArray(d) ? d : []);
    _faAllUsers = list;
    return _faAllUsers;
  } catch (err) {
    return [];
  }
}

async function faIdentityOpenPicker() {
  const modal = document.getElementById('faIdentityModal');
  const sel = document.getElementById('faIdentitySelect');
  if (!modal || !sel) return;
  // Populate dropdown.
  sel.innerHTML = '<option value="">— Select your name —</option>';
  const list = await faIdentityFetchRoster();
  list.forEach(u => {
    const opt = document.createElement('option');
    opt.value = u.id;
    opt.textContent = u.name + (u.email ? ' (' + u.email + ')' : '');
    if (_faIdentity && _faIdentity.user_id === u.id) opt.selected = true;
    sel.appendChild(opt);
  });
  modal.style.display = 'flex';
}

function faIdentityCancel() {
  const modal = document.getElementById('faIdentityModal');
  if (modal) modal.style.display = 'none';
  // If a pending edit triggered this, clear it.
  _faPendingEditCallback = null;
}

async function faIdentitySave() {
  const sel = document.getElementById('faIdentitySelect');
  if (!sel) return;
  const uid = parseInt(sel.value, 10);
  if (!uid) {
    alert('Please pick your name.');
    return;
  }
  const btn = document.getElementById('faIdentitySaveBtn');
  if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
  try {
    const resp = await fetch('/api/whoami', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({user_id: uid}),
    });
    if (!resp.ok) {
      const j = await resp.json().catch(() => ({}));
      alert('Failed to save: ' + (j.error || resp.status));
      if (btn) { btn.disabled = false; btn.textContent = 'Continue'; }
      return;
    }
    _faIdentity = await resp.json();
    faIdentityRenderChip();
    document.getElementById('faIdentityModal').style.display = 'none';
    // If an edit was pending, retry it.
    if (typeof _faPendingEditCallback === 'function') {
      const cb = _faPendingEditCallback;
      _faPendingEditCallback = null;
      try { cb(); } catch (e) {}
    }
    // Refresh diff strip — now that we have identity, it might render.
    renderDiffStrip();
  } catch (err) {
    alert('Failed to save: ' + err.message);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Continue'; }
  }
}

// Edit-gate helper: call this before any mutation. If FA is identified,
// runs the callback immediately. If not, opens the picker and runs the
// callback after they pick.
let _faPendingEditCallback = null;
function faRequireIdentity(cb) {
  if (_faIdentity && _faIdentity.user_id) {
    if (typeof cb === 'function') cb();
    return true;
  }
  _faPendingEditCallback = cb || null;
  faIdentityOpenPicker();
  return false;
}

// ─── Diff Strip rendering ─────────────────────────────────────────────
async function renderDiffStrip(opts) {
  opts = opts || {};
  const el = document.getElementById('diffStrip');
  if (!el) return;
  const url = '/api/diff/' + entityCode + (opts.includeDismissed ? '?include_dismissed=1' : '');
  let data;
  try {
    const resp = await fetch(url);
    if (!resp.ok) { el.style.display = 'none'; return; }
    data = await resp.json();
  } catch (err) {
    el.style.display = 'none';
    return;
  }
  // Show/hide the "View recent changes" link in the readiness panel.
  // Visible when the FA has a prior visit row, regardless of whether
  // the strip itself renders.
  const recentLink = document.getElementById('readinessRecentChangesLink');
  if (recentLink) {
    recentLink.style.display = (data && data.has_prev_row) ? '' : 'none';
  }

  if (!data || !data.show) {
    el.style.display = 'none';
    return;
  }
  const pills = data.pills || [];
  if (!pills.length) { el.style.display = 'none'; return; }

  // Format the "Mon, May 5 · 4 days ago" header.
  let sinceLabel = '';
  if (data.since) {
    try {
      const sinceDt = new Date(data.since);
      const now = new Date();
      const diffMs = now - sinceDt;
      const days = Math.floor(diffMs / (1000 * 60 * 60 * 24));
      const dateStr = sinceDt.toLocaleDateString('en-US', {weekday:'short', month:'short', day:'numeric'});
      sinceLabel = '(' + dateStr + (days > 0 ? ' · ' + days + ' day' + (days !== 1 ? 's' : '') + ' ago' : ' · earlier today') + ')';
    } catch (e) {}
  }

  // Pill style by kind.
  const pillStyle = (kind) => {
    if (kind === 'auto')   return 'background:#d1fae5;color:#065f46;';
    if (kind === 'system') return 'background:#e0e7ff;color:#3730a3;';
    if (kind === 'fa')     return 'background:#fce7f3;color:#9d174d;';
    if (kind === 'alert')  return 'background:#fee2e2;color:#991b1b;';
    return 'background:#f3f4f6;color:#374151;';
  };
  const pillLabel = (kind) => ({
    auto:'AUTO', system:'SYSTEM', fa:'FA', alert:'ALERT'
  })[kind] || (kind || '').toUpperCase();

  let html = '';
  html += '<div style="background:#fffbeb; border-bottom:1px solid #fde68a; padding:14px 24px;">';
  html += '<div style="display:flex; align-items:center; justify-content:space-between; gap:16px; margin-bottom:8px;">';
  html += '<div style="display:flex; align-items:center; gap:10px; font-size:14px; font-weight:700; color:#92400e;">';
  html += '<span style="font-size:18px;">🔔</span>';
  html += '<span>Since you last opened this building <span style="font-weight:500; opacity:0.8;">' + sinceLabel + '</span></span>';
  html += '</div>';
  html += '<div style="display:flex; gap:8px;">';
  html += '<button onclick="diffStripDismiss()" style="padding:5px 12px; font-size:12px; font-weight:600; border-radius:5px; cursor:pointer; border:1px solid #d97706; background:#d97706; color:white;">Got it</button>';
  html += '<button onclick="diffStripDismiss()" title="Dismiss" style="background:transparent; border:none; cursor:pointer; color:#92400e; font-size:18px; line-height:1; padding:2px 6px;">×</button>';
  html += '</div>';
  html += '</div>';
  html += '<ul style="list-style:none; margin:0; padding:0; font-size:13px; color:var(--gray-700);">';
  pills.forEach(p => {
    html += '<li style="padding:3px 0 3px 22px; position:relative;">';
    html += '<span style="position:absolute; left:8px; color:#d97706; font-weight:700;">•</span>';
    html += '<span style="display:inline-block; padding:1px 7px; font-size:11px; font-weight:600; border-radius:9px; margin-right:6px; vertical-align:1px; ' + pillStyle(p.kind) + '">' + pillLabel(p.kind) + '</span>';
    html += (p.body || p.title || '').replace(/&/g, '&amp;').replace(/</g, '&lt;');
    html += '</li>';
  });
  html += '</ul>';
  html += '<div style="margin-top:8px; font-size:11px; color:#92400e; opacity:0.78;">';
  html += 'Auto-clears when you leave this page · You can re-open via "View recent changes" in the Readiness panel below.';
  html += '</div>';
  html += '</div>';

  el.innerHTML = html;
  el.style.display = '';
}

async function diffStripDismiss() {
  const el = document.getElementById('diffStrip');
  if (el) el.style.display = 'none';
  try {
    await fetch('/api/diff/' + entityCode + '/dismiss', {method: 'POST'});
  } catch (err) {}
}

// Hook beforeunload → fire-and-forget dismiss via sendBeacon (reliable
// across page-unload; fetch is not).
window.addEventListener('beforeunload', () => {
  const el = document.getElementById('diffStrip');
  // Only fire dismiss if the strip was actually showing.
  if (el && el.style.display !== 'none' && navigator.sendBeacon) {
    try {
      navigator.sendBeacon('/api/diff/' + entityCode + '/dismiss', '');
    } catch (e) {}
  }
});

// FA directive 2026-05-10: Unified Status Block — show/hide the wrapper
// container based on whether any of its 4 children (diff strip,
// readiness, period, audit) are visible. Avoids a stray colored stripe
// when the building has nothing status-worthy to show.
function _refreshUnifiedStatusBlock() {
  const wrap = document.getElementById('unifiedStatusBlock');
  if (!wrap) return;
  const children = ['diffStrip', 'readinessInspector', 'periodBanner', 'auditStatusBanner'];
  let anyVisible = false;
  for (const id of children) {
    const el = document.getElementById(id);
    if (el && el.style.display !== 'none' && el.offsetParent !== null) {
      anyVisible = true; break;
    }
  }
  wrap.style.display = anyVisible ? '' : 'none';
}

async function renderReadinessInspector() {
  const el = document.getElementById('readinessInspector');
  if (!el) return;
  let data;
  try {
    const resp = await fetch('/api/readiness/' + entityCode);
    if (!resp.ok) { el.style.display = 'none'; return; }
    data = await resp.json();
  } catch (err) {
    el.style.display = 'none';
    return;
  }
  if (!data || !data.gates) { el.style.display = 'none'; return; }
  const gates = data.gates;
  const s = data.summary || {};

  // Mirror gates into the Health drawer (Variant A, Phase 4). Runs every
  // time the readiness inspector refreshes, so the drawer + pill badge
  // stay in sync with the same data the inline panel used.
  try { populateHealthDrawerActions(gates, s); } catch (e) { console.warn('drawer actions populate failed', e); }
  const okCount = s.ok || 0;
  const total = s.total || gates.length;
  const allGreen = (s.fail === 0 && s.warn === 0);

  // Color mapping per status
  const statusColor = (st) => ({
    ok:   '#16a34a',
    warn: '#d97706',
    fail: '#dc2626',
    skip: '#9ca3af',
  })[st] || '#9ca3af';
  const statusBg = (st) => ({
    ok:   '#f0fdf4',
    warn: '#fffbeb',
    fail: '#fef2f2',
    skip: '#f9fafb',
  })[st] || '#f9fafb';
  const statusIcon = (st) => ({
    ok:   '✓',
    warn: '⚠',
    fail: '✕',
    skip: '–',
  })[st] || '·';

  // Default to expanded when not all green; collapsed when all green.
  const stateKey = '_readinessExpanded_' + entityCode;
  let expanded = sessionStorage.getItem(stateKey);
  if (expanded === null) expanded = allGreen ? '0' : '1';
  expanded = (expanded === '1');

  // Header
  const headerBg = allGreen ? '#f0fdf4' : (s.fail > 0 ? '#fef2f2' : '#fffbeb');
  const headerColor = allGreen ? '#166534' : (s.fail > 0 ? '#991b1b' : '#92400e');
  const headerIcon = allGreen ? '✅' : (s.fail > 0 ? '🛑' : '⚠️');
  const headerText = allGreen
    ? 'Building ready — all ' + total + ' gates green'
    : okCount + ' of ' + total + ' ready · ' +
      [
        s.fail > 0 ? (s.fail + ' blocker' + (s.fail !== 1 ? 's' : '')) : null,
        s.warn > 0 ? (s.warn + ' warning' + (s.warn !== 1 ? 's' : '')) : null,
      ].filter(Boolean).join(' · ');

  let html = '';
  html += '<div style="background:' + headerBg + '; border-bottom:1px solid var(--gray-200);">';
  html += '<div onclick="readinessToggle()" style="display:flex; align-items:center; justify-content:space-between; padding:10px 24px; cursor:pointer; user-select:none;">';
  html += '<div style="display:flex; align-items:center; gap:10px;">';
  html += '<span style="font-size:16px;">' + headerIcon + '</span>';
  html += '<span style="font-size:14px; font-weight:700; color:' + headerColor + ';">Readiness</span>';
  html += '<span style="font-size:13px; color:' + headerColor + ';">' + headerText + '</span>';
  html += '</div>';
  // Right-side cluster: "View recent changes" link (when there's a prior visit) + chevron.
  html += '<div style="display:flex; align-items:center; gap:14px;">';
  html += '<span id="readinessRecentChangesLink" style="display:none; font-size:11px; color:#92400e; text-decoration:underline; cursor:pointer;" onclick="event.stopPropagation(); renderDiffStrip({includeDismissed:true});">View recent changes ↗</span>';
  html += '<span id="readinessChevron" style="font-size:12px; color:' + headerColor + '; transition:transform 0.2s;">' + (expanded ? '▼' : '▶') + '</span>';
  html += '</div>';

  // Body
  if (expanded) {
    html += '<div style="padding:4px 24px 14px;">';
    html += '<table style="width:100%; border-collapse:collapse; font-size:13px;">';
    gates.forEach((g, i) => {
      const color = statusColor(g.status);
      const bg = statusBg(g.status);
      const icon = statusIcon(g.status);
      const isLast = (i === gates.length - 1);
      html += '<tr style="' + (isLast ? '' : 'border-bottom:1px solid rgba(0,0,0,0.04);') + '">';
      // dot + icon
      html += '<td style="padding:8px 8px 8px 0; width:28px; vertical-align:top;">';
      html += '<span style="display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; border-radius:50%; background:' + bg + '; color:' + color + '; font-weight:700; font-size:13px; border:1.5px solid ' + color + ';">' + icon + '</span>';
      html += '</td>';
      // label
      html += '<td style="padding:8px 12px; vertical-align:top; min-width:170px;">';
      html += '<div style="font-weight:600; color:var(--gray-700); font-size:13px;">' + (g.label || '') + '</div>';
      html += '</td>';
      // detail
      html += '<td style="padding:8px 12px; vertical-align:top; color:var(--gray-600); font-size:12px;">';
      html += (g.detail || '');
      html += '</td>';
      // action
      html += '<td style="padding:8px 0 8px 12px; vertical-align:top; text-align:right; white-space:nowrap;">';
      if (g.action_url && g.action_label) {
        const isHash = g.action_url.startsWith('#');
        if (isHash) {
          html += '<button onclick="readinessAction(\'' + g.action_url.replace(/\'/g, "\\'") + '\')" style="padding:4px 10px; font-size:11px; font-weight:600; background:transparent; color:' + color + '; border:1px solid ' + color + '; border-radius:4px; cursor:pointer;">' + g.action_label + ' →</button>';
        } else {
          html += '<a href="' + g.action_url + '" style="padding:4px 10px; font-size:11px; font-weight:600; background:transparent; color:' + color + '; border:1px solid ' + color + '; border-radius:4px; text-decoration:none; display:inline-block;">' + g.action_label + ' →</a>';
        }
      } else if (g.action_label) {
        // Action label without URL — soft hint, no link.
        html += '<span style="font-size:11px; color:' + color + '; font-weight:600;">' + g.action_label + '</span>';
      }
      html += '</td>';
      html += '</tr>';
    });
    html += '</table>';
    html += '</div>';
  }
  html += '</div>';

  el.innerHTML = html;
  el.style.display = '';
}

function readinessToggle() {
  const stateKey = '_readinessExpanded_' + entityCode;
  const cur = sessionStorage.getItem(stateKey);
  // If not yet stored, this is the first toggle from an auto-default, so flip
  // to the opposite of what's currently rendered.
  const chev = document.getElementById('readinessChevron');
  const wasExpanded = (chev && chev.textContent === '▼');
  sessionStorage.setItem(stateKey, wasExpanded ? '0' : '1');
  renderReadinessInspector();
}

// Handle hash-based action click-throughs from readiness inspector.
// Supported: '#tab=Summary', '#tab=Payroll', '#building-info', '#generate'.
function readinessAction(target) {
  if (!target) return;
  if (target === '#building-info') {
    if (typeof openBuildingInfo === 'function') openBuildingInfo();
    return;
  }
  if (target === '#generate') {
    const btn = document.getElementById('generateBudgetBtn');
    if (btn) { btn.scrollIntoView({behavior:'smooth', block:'center'}); btn.click(); }
    return;
  }
  // FA directive 2026-05-17: deep-link from Health drawer to the actual issue.
  // #sumOrphans → switch to Summary tab, scroll to the orphan-GL banner, flash it.
  // #sumDuplicateRows → switch to Summary tab, scroll to first duplicate, flash all duplicates.
  if (target === '#sumOrphans' || target === '#sumDuplicateRows') {
    _switchToSummaryTab();
    // renderBudgetSummary may finish painting AFTER our switch (it can fetch
    // /api/summary). Poll for the target to exist instead of guessing a
    // single timeout — up to 2s total, checking every 100ms.
    let attempts = 0;
    const maxAttempts = 20;
    const poll = function() {
      attempts++;
      if (target === '#sumOrphans') {
        const el = document.getElementById('sumOrphans');
        if (el) { scrollAndFlash(el); return; }
        if (attempts < maxAttempts) setTimeout(poll, 100);
        return;
      }
      // Duplicate-row case: gather labels from cached warnings, find rows.
      const dups = window._sumDuplicateWarnings || [];
      const labels = [];
      dups.forEach(function(w) {
        (w.labels || []).forEach(function(l) { if (l && labels.indexOf(l) < 0) labels.push(l); });
      });
      if (!labels.length) {
        if (attempts < maxAttempts) setTimeout(poll, 100);
        return;
      }
      const firstRow = _findSummaryRowByLabel(labels[0]);
      if (!firstRow) {
        if (attempts < maxAttempts) setTimeout(poll, 100);
        return;
      }
      // Got the rows — scroll once, flash each in sequence.
      // Use instant scroll (`behavior: 'auto'`) instead of smooth: Chrome
      // blocks smooth-scroll when not triggered by a direct user gesture,
      // and the close-drawer → readinessAction handoff can fall outside
      // the gesture window. Instant is also faster, which matches the
      // "find me the row" intent of a deep-link.
      try {
        const r0 = firstRow.getBoundingClientRect();
        const targetY = Math.max(0, window.scrollY + r0.top - (window.innerHeight / 2));
        window.scrollTo(0, targetY);
      } catch(e) {}
      labels.forEach(function(l, idx) {
        setTimeout(function() {
          const r = _findSummaryRowByLabel(l);
          if (r) scrollAndFlash(r, false /* don't re-scroll */);
        }, 100 + idx * 200);
      });
    };
    setTimeout(poll, 120);
    return;
  }
  if (target.indexOf('#tab=') === 0) {
    const sheetName = target.slice(5);
    const tab = document.querySelector('.sheet-tab[data-sheet="' + sheetName + '"]');
    if (tab) {
      document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      if (typeof renderSheet === 'function') renderSheet(sheetName, null, tab);
      tab.scrollIntoView({behavior:'smooth', block:'center'});
    }
    return;
  }
}

// Helper: switch to the Summary tab. Mirrors the #tab=Summary branch above
// but factored out so readinessAction's deep-link branches can call it
// before doing more specific scrolling.
function _switchToSummaryTab() {
  const tab = document.querySelector('.sheet-tab[data-sheet="Summary"]');
  if (!tab) return;
  document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
  tab.classList.add('active');
  if (typeof renderSheet === 'function') renderSheet('Summary', null, tab);
}

// Helper: find a Summary tab row by its label. Reads the data-label
// attribute set by renderBudgetSummary (2026-05-17). Linear scan keeps
// us safe from labels containing quotes or other CSS-selector specials.
// Returns null if the row isn't in the DOM yet — caller should retry.
function _findSummaryRowByLabel(label) {
  if (!label) return null;
  const rows = document.querySelectorAll('#sumTable tr[data-type="d"]');
  for (let i = 0; i < rows.length; i++) {
    if (rows[i].getAttribute('data-label') === label) return rows[i];
  }
  return null;
}

// Helper: flash a highlight on a DOM element so the FA's eye lands on it
// after a deep-link navigation. Uses a temporary inline boxShadow + scroll
// so we don't fight whatever bg the element already has. The flash decays
// over 2.5s. Called from readinessAction deep-link branches.
function scrollAndFlash(el, doScroll) {
  if (!el) return;
  if (doScroll !== false) {
    // Instant scroll — see comment in readinessAction for rationale.
    try {
      const rect = el.getBoundingClientRect();
      const targetY = Math.max(0, window.scrollY + rect.top - (window.innerHeight / 2));
      window.scrollTo(0, targetY);
    } catch(e) {}
  }
  // Stash original styles so we can restore them.
  const prevTransition = el.style.transition;
  const prevBoxShadow = el.style.boxShadow;
  const prevBackground = el.style.backgroundColor;
  el.style.transition = 'box-shadow 0.25s ease-out, background-color 0.25s ease-out';
  el.style.boxShadow = '0 0 0 3px #fbbf24, 0 0 16px rgba(251,191,36,0.7)';
  el.style.backgroundColor = 'rgba(254,243,199,0.6)';
  setTimeout(function() {
    el.style.boxShadow = prevBoxShadow || '';
    el.style.backgroundColor = prevBackground || '';
    // Restore transition after the fade completes so we don't leak it
    // into the element's normal styling.
    setTimeout(function() { el.style.transition = prevTransition || ''; }, 300);
  }, 2200);
}

// Render the period banner above the workbook tabs. Reads
// data.assumptions.budget_period ("MM/YYYY") and shows either:
//   - red "Period not set" warning + dropdown to set it, OR
//   - green "Actuals: Jan-Apr 2026 · Estimate: May-Dec 2026" + edit pencil
function renderPeriodBanner(data) {
  const banner = document.getElementById('periodBanner');
  if (!banner) return;
  const a = data.assumptions || {};
  const bp = a.budget_period || '';
  let mm = 0, yyyy = (BY - 1);
  if (bp && bp.indexOf('/') > 0) {
    const parts = bp.split('/');
    const m = parseInt(parts[0], 10);
    const y = parseInt(parts[1], 10);
    if (!isNaN(m) && m >= 1 && m <= 12) mm = m;
    if (!isNaN(y) && y > 1900) yyyy = y;
  }
  banner.style.display = '';
  if (!mm) {
    // Period not set — block of red, prompt to fix.
    banner.style.background = '#fef2f2';
    banner.style.borderBottom = '1px solid #fecaca';
    banner.style.color = '#991b1b';
    banner.innerHTML =
      '<span style="font-weight:700;">⚠ Period not set</span>' +
      ' &nbsp;·&nbsp; YTD/forecast formulas are using the default 2-month YTD.' +
      ' &nbsp; <button onclick="editPeriod()" style="margin-left:8px; padding:4px 10px; background:#dc2626; color:#fff; border:none; border-radius:4px; font-size:12px; cursor:pointer;">Set period</button>';
  } else {
    const actEnd = MONTH_ABBR[mm - 1];   // 1-indexed → 0-indexed
    const estStart = mm < 12 ? MONTH_ABBR[mm] : null;
    const actLabel = 'Jan–' + actEnd + ' ' + yyyy;
    const estLabel = estStart ? estStart + '–Dec ' + yyyy : '—';
    banner.style.background = '#f0fdf4';
    banner.style.borderBottom = '1px solid #bbf7d0';
    banner.style.color = '#166534';
    banner.innerHTML =
      '<span style="font-weight:700;">Period:</span>' +
      ' &nbsp; Actuals <strong>' + actLabel + '</strong>' +
      ' &nbsp;·&nbsp; Estimate <strong>' + estLabel + '</strong>' +
      ' &nbsp; <button onclick="editPeriod()" style="margin-left:8px; padding:2px 8px; background:transparent; color:#166534; border:1px solid #86efac; border-radius:4px; font-size:11px; cursor:pointer;">✎ Edit</button>';
  }
}

// Inline editor for the period banner — small dropdown overlay.
// Saves via PUT /api/budget-assumptions/<entity> which already accepts
// budget_period as a top-level key and recalculates downstream lines.
function editPeriod() {
  const banner = document.getElementById('periodBanner');
  if (!banner) return;
  const cur = (window._data && window._data.assumptions && window._data.assumptions.budget_period) || '';
  let curMM = 0;
  if (cur && cur.indexOf('/') > 0) {
    const m = parseInt(cur.split('/')[0], 10);
    if (!isNaN(m) && m >= 1 && m <= 12) curMM = m;
  }
  let opts = '<option value="0">— Select —</option>';
  for (let i = 1; i <= 12; i++) {
    opts += '<option value="' + i + '"' + (curMM === i ? ' selected' : '') + '>' + MONTH_ABBR[i - 1] + '</option>';
  }
  banner.innerHTML =
    '<span style="font-weight:700;">Actuals through:</span>' +
    ' &nbsp; <select id="periodMonthSel" style="padding:4px 8px; border:1px solid var(--gray-200); border-radius:4px; font-size:13px;">' + opts + '</select>' +
    ' &nbsp; <button onclick="savePeriod()" style="padding:4px 12px; background:var(--green); color:#fff; border:none; border-radius:4px; font-size:12px; cursor:pointer;">Save</button>' +
    ' &nbsp; <button onclick="renderPeriodBanner(window._data)" style="padding:4px 10px; background:transparent; color:var(--gray-700); border:1px solid var(--gray-200); border-radius:4px; font-size:12px; cursor:pointer;">Cancel</button>';
}

function savePeriod() {
  const sel = document.getElementById('periodMonthSel');
  if (!sel) return;
  const mm = parseInt(sel.value, 10) || 0;
  let value = '';
  if (mm >= 1 && mm <= 12) {
    value = String(mm).padStart(2, '0') + '/' + (BY - 1);
  }
  fetch('/api/budget-assumptions/' + entityCode, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({budget_period: value})
  })
    .then(r => r.json())
    .then(d => {
      if (d && d.error) {
        alert('Save failed: ' + d.error);
        return;
      }
      // Reload the dashboard to recompute everything against the new period.
      // Server-side recomputation already updates proposed_budget; client-side
      // YTD_MONTHS / REMAINING_MONTHS update via loadDetail.
      showToast('Period updated — recomputing forecasts', 'success');
      loadDetail();
    })
    .catch(err => alert('Save error: ' + err.message));
}

function showToast(msg, type='info') {
  const c = document.getElementById('toastContainer');
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity 0.3s'; setTimeout(() => t.remove(), 300); }, 3000);
}

async function loadDetail() {
  const res = await fetch('/api/dashboard/' + entityCode);
  if (!res.ok) {
    document.getElementById('loadingState').innerHTML = '<p style="color:var(--red);">Error loading building data.</p>';
    return;
  }
  const data = await res.json();
  document.getElementById('loadingState').style.display = 'none';
  document.getElementById('detailContent').style.display = '';
  renderDetail(data);
}

function fmt(n) {
  if (n === null || n === undefined) return '\u2014';
  return '$' + Math.round(n).toLocaleString();
}

function renderStatusPipeline(status) {
  const steps = [
    { key: 'draft', label: 'Draft' },
    { key: 'pm_pending', label: 'PM Review' },
    { key: 'fa_review', label: 'FA Review' },
    { key: 'approved', label: 'Approved' }
  ];
  const statusOrder = { draft: 0, pm_pending: 1, pm_in_progress: 1, fa_review: 2, exec_review: 2, approved: 3, returned: 1 };
  const currentIdx = statusOrder[status] || 0;

  const pipeline = document.getElementById('statusPipeline');
  pipeline.innerHTML = steps.map((s, i) => {
    let cls = '';
    if (i < currentIdx) cls = 'completed';
    else if (i === currentIdx) cls = 'current';
    const icon = i < currentIdx ? '\u2713 ' : (i === currentIdx ? '\u25CF ' : '');
    return (i > 0 ? '<span class="pipeline-arrow">\u203A</span>' : '') +
      '<div class="pipeline-step ' + cls + '">' + icon + s.label + '</div>';
  }).join('');
}

function renderDetail(data) {
  const b = data.budget;

  // Set dynamic YTD months from API
  YTD_MONTHS = data.ytd_months || 2;
  REMAINING_MONTHS = data.remaining_months || 10;

  // FA identity — load + render chip. Required before edits. Diff strip
  // also depends on this. FA directive 2026-05-10.
  faIdentityLoad().then(() => {
    faIdentityRenderChip();
    renderDiffStrip();
    _refreshUnifiedStatusBlock();
  });
  // Readiness inspector — single consolidated 8-gate checklist at the top
  // of every building's dashboard. FA directive 2026-05-09. Reads from
  // /api/readiness/<entity>; auto-collapses when all gates green.
  renderReadinessInspector();
  // Period banner — shows "Actuals: Jan-Apr 2026 · Estimate: May-Dec 2026"
  // or a red warning if the period was never set in the wizard.
  renderPeriodBanner(data);
  // Audit-status banner — shows where the latest AuditUpload is in the
  // extraction/mapping/confirm pipeline, with click-through to /audited-financials.
  // FA directive 2026-05-05 — uploaded → extracted → mapped → confirmed.
  renderAuditStatusBanner(data);
  // FA directive 2026-05-10: hide the unified wrapper if no children visible.
  _refreshUnifiedStatusBlock();

  // Header + breadcrumb
  document.getElementById('buildingName').textContent = b.building_name;
  document.getElementById('breadcrumbName').textContent = b.building_name;
  document.title = b.building_name + ' - Century Management';
  let meta = 'Entity ' + b.entity_code + ' | ' + b.year + ' Budget';
  if (data.assignments.fa) meta += ' | FA: ' + data.assignments.fa;
  if (data.assignments.pm) meta += ' | PM: ' + data.assignments.pm;
  document.getElementById('buildingMeta').textContent = meta;

  // Status Pipeline
  renderStatusPipeline(b.status);

  // Summary cards
  const lines = data.lines;
  let totalPrior = 0, totalBudget = 0, totalForecast = 0, totalPM = 0;
  lines.forEach(l => {
    totalPrior += l.prior_year || 0;
    totalBudget += l.current_budget || 0;
    const forecast = computeForecast(l);
    totalForecast += forecast;
    const proposed = forecast * (1 + (l.increase_pct || 0));
    totalPM += proposed;
  });

  // Variance/% Change compare Current Budget to Prior Year — matches the
  // "Prior Year" / "Current Budget" labels on the adjacent cards. Old code
  // computed against totalForecast (an invisible quantity), and when YTD
  // months was unset the forecast inflated 6× and made the cards show
  // wildly negative variances on year-over-year-flat budgets.
  const variance = totalBudget - totalPrior;
  const pctChange = totalPrior ? ((variance) / totalPrior * 100) : 0;
  const absPct = Math.abs(pctChange);
  const varColor = absPct > 10 ? 'var(--red)' : absPct > 5 ? '#d97706' : 'var(--green)';
  const varBg = absPct > 10 ? '#fef2f2' : absPct > 5 ? '#fffbeb' : '#f0fdf4';
  const varBorder = absPct > 10 ? '#fca5a5' : absPct > 5 ? '#fde68a' : '#86efac';
  const arrow = pctChange > 0 ? ' \u25B2' : pctChange < 0 ? ' \u25BC' : '';

  document.getElementById('summaryCards').innerHTML = `
    <div class="summary-card">
      <div class="card-value">${fmt(totalPrior)}</div>
      <div class="card-label">Prior Year</div>
    </div>
    <div class="summary-card">
      <div class="card-value">${fmt(totalBudget)}</div>
      <div class="card-label">Current Budget</div>
    </div>
    <div class="summary-card" style="background:${varBg};border-color:${varBorder};">
      <div class="card-value" style="color:${varColor};">${fmt(variance)}</div>
      <div class="card-label">Variance</div>
    </div>
    <div class="summary-card" style="background:${varBg};border-color:${varBorder};">
      <div class="card-value" style="color:${varColor};">${totalForecast ? pctChange.toFixed(1) + '%' + arrow : '\u2014'}</div>
      <div class="card-label">% Change</div>
    </div>
  `;

  // Mirror the same KPIs into the Health drawer (Variant A, Phase 4).
  // The inline .summary-cards element above is hidden via CSS; the drawer
  // is now the single surface for these numbers.
  try { populateHealthDrawerKpis(totalPrior, totalBudget, variance, pctChange, totalForecast); } catch (e) { console.warn('drawer kpi populate failed', e); }

  // PM Track — collapsible panel with badge.
  // Status mapping: 'not_started' is grouped with 'draft' for display because
  // post-wizard the budget is effectively a draft (wizard_completed_at set)
  // even if the raw status column hasn't flipped yet.
  const wizardDone = !!b.wizard_completed_at;
  const isPreSend = !b.status || ['not_started','data_collection','data_ready','draft'].includes(b.status);
  const pmStatusLabels = { draft: 'Not Sent', not_started: wizardDone ? 'Ready to Send' : 'Not Started', pm_pending: 'Sent to PM', pm_in_progress: 'PM Working', fa_review: 'Submitted for Review', approved: 'Approved', returned: 'Returned' };
  let pmStatus = pmStatusLabels[b.status] || (b.status ? b.status.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ') : '');
  // Once sent, append the date so FAs see "Sent on Apr 30" without expanding.
  if (b.pm_sent_at && ['pm_pending','pm_in_progress','fa_review','approved','returned'].includes(b.status)) {
    try {
      const dt = new Date(b.pm_sent_at);
      const mo = dt.toLocaleString('en-US', {month:'short'});
      pmStatus = 'Sent ' + mo + ' ' + dt.getDate() + ', ' + dt.getFullYear();
    } catch (e) {}
  }
  const pmBadgeClass = ['fa_review','approved'].includes(b.status) ? 'badge-green' : ['pm_pending','pm_in_progress'].includes(b.status) ? 'badge-amber' : (isPreSend && wizardDone ? 'badge-blue' : 'badge-gray');
  document.getElementById('pmBadge').className = 'badge ' + pmBadgeClass;
  document.getElementById('pmBadge').textContent = pmStatus;
  document.getElementById('pmSummary').textContent = data.assignments.pm ? data.assignments.pm : '';

  // Header inline action — visible without expanding the panel.
  const headerEl = document.getElementById('pmHeaderAction');
  if (headerEl) {
    if (isPreSend && wizardDone) {
      headerEl.innerHTML = '<button onclick="event.stopPropagation(); sendToPM();" style="background:var(--blue); color:white; padding:4px 12px; font-size:12px; border:none; border-radius:4px; cursor:pointer; font-weight:600;">Send to PM &rarr;</button>';
    } else {
      headerEl.innerHTML = '';
    }
  }

  let pmActions = '';
  if (isPreSend && wizardDone) {
    pmActions = '<button onclick="sendToPM()" style="background:var(--blue); color:white;">Send to PM for Review</button>';
  } else if (b.status === 'fa_review') {
    pmActions = '<button onclick="approvePM()" style="background:var(--green); color:white; margin-right:8px;">Approve PM Review</button>' +
      '<button onclick="returnPM()" style="background:var(--yellow); color:white;">Return to PM</button>';
  }
  if (b.fa_notes) {
    pmActions += '<div style="margin-top:12px; padding:10px; background:#fef3c7; border-radius:6px; font-size:13px;"><strong>FA Notes:</strong> ' + b.fa_notes + '</div>';
  }

  document.getElementById('pmTrackContent').innerHTML =
    '<div style="display:flex; align-items:center; gap:12px; margin-bottom:16px;">' +
      '<span class="pill pill-' + b.status + '">' + pmStatus + '</span>' +
      (data.assignments.pm ? '<span style="font-size:13px; color:var(--gray-500);">Assigned to: ' + data.assignments.pm + '</span>' : '') +
    '</div>' + pmActions;

  // FA Completion Checklist — guided workflow
  const assumptions = data.assumptions || {};
  const hasAssumptions = Object.keys(assumptions).length > 0;
  const hasBudgetPeriod = !!assumptions.budget_period;
  const hasEnergyRates = !!(assumptions.energy_rates && Object.keys(assumptions.energy_rates).length);
  const hasWaterRates = !!(assumptions.water_rates && Object.keys(assumptions.water_rates).length);
  const hasInsuranceInc = !!(assumptions.insurance_increase && assumptions.insurance_increase.percent);
  const hasWageInc = !!(assumptions.wage_increase && assumptions.wage_increase.percent);
  const anyAssumptions = hasBudgetPeriod || hasEnergyRates || hasWaterRates || hasInsuranceInc || hasWageInc;
  const linesWithProposed = lines.filter(l => l.proposed_budget && l.proposed_budget > 0).length;
  const pmDone = ['fa_review','approved'].includes(b.status);
  const pmSent = ['pm_pending','pm_in_progress','fa_review','approved'].includes(b.status);

  const reviewPct = lines.length ? Math.round(linesWithProposed / lines.length * 100) : 0;
  const checks = [
    { group: 'Data Collection', label: 'YSL Data Imported', done: lines.length > 0, detail: lines.length > 0 ? (lines.length + ' GL lines loaded') : 'No YSL data — run Build Budget' },
    { group: 'Data Collection', label: 'Expense Distribution', done: data.expenses.exists, detail: data.expenses.exists ? data.expenses.invoice_count + ' invoices (' + fmt(data.expenses.total_amount) + ')' : 'Upload via Data Collection' },
    { group: 'Data Collection', label: 'Audited Financials', done: data.audit.exists, detail: data.audit.exists ? Object.keys(data.audit.years || {}).length + ' years of history' : 'Upload via Data Collection' },
    { group: 'Configuration', label: 'Assumptions Configured', done: anyAssumptions, detail: hasBudgetPeriod ? 'Period: ' + assumptions.budget_period : 'Not set — click Assumptions tab', action: !anyAssumptions ? 'openAssumptions' : null },
    { group: 'Review', label: 'Review All Sheets', done: lines.length > 0 && linesWithProposed >= lines.length * 0.5, detail: lines.length === 0 ? 'No lines yet' : (linesWithProposed + ' of ' + lines.length + ' lines have proposed values (' + reviewPct + '%)'), progress: reviewPct },
    { group: 'Review', label: 'PM Review', done: pmDone, detail: pmDone ? 'PM review complete' : (pmSent ? 'Awaiting PM response' : 'Not yet sent'), action: !pmSent ? 'sendToPM' : null },
    { group: 'Approval', label: 'Final Approval', done: b.status === 'approved', detail: '', blocked: true }
  ];

  // Build missing-deps detail for Final Approval
  const missingDeps = [];
  if (!data.audit.exists) missingDeps.push('Audited Financials');
  if (!pmDone) missingDeps.push('PM Review');
  if (linesWithProposed < lines.length * 0.5) missingDeps.push('Review All Sheets');
  const approvalItem = checks[checks.length - 1];
  approvalItem.detail = approvalItem.done ? 'Budget approved' : (missingDeps.length ? 'Requires: ' + missingDeps.join(', ') : 'Ready for approval');
  if (!approvalItem.done && missingDeps.length === 0) approvalItem.blocked = false;

  const doneCount = checks.filter(c => c.done).length;
  const pct = Math.round(doneCount / checks.length * 100);
  const barColor = pct === 100 ? 'var(--green)' : pct >= 60 ? 'var(--blue)' : 'var(--yellow)';

  // Set FA badge and summary
  const faBadgeClass = pct === 100 ? 'badge-green' : pct >= 50 ? 'badge-blue' : 'badge-amber';
  document.getElementById('faBadge').className = 'badge ' + faBadgeClass;
  document.getElementById('faBadge').textContent = doneCount + ' / ' + checks.length;
  document.getElementById('faSummary').textContent = pct + '% complete';

  let assemblyHtml = '<div style="margin-bottom:12px;">' +
    '<div style="display:flex; justify-content:space-between; font-size:12px; color:var(--gray-500); margin-bottom:4px;"><span>' + doneCount + ' of ' + checks.length + ' complete</span><span>' + pct + '%</span></div>' +
    '<div style="height:6px; background:var(--gray-100); border-radius:3px; overflow:hidden;"><div style="height:100%; width:' + pct + '%; background:' + barColor + '; border-radius:3px; transition:width 0.3s;"></div></div></div>';

  let lastGroup = '';
  checks.forEach((c) => {
    // Group header
    if (c.group !== lastGroup) {
      assemblyHtml += '<div style="font-size:10px; font-weight:700; color:var(--gray-400); text-transform:uppercase; letter-spacing:0.5px; margin:10px 0 4px;">' + c.group + '</div>';
      lastGroup = c.group;
    }
    const iconClass = c.done ? 'check-done' : 'check-pending';
    const iconChar = c.done ? '✓' : '';
    const actionBtn = c.action ? ' <button onclick="' + c.action + '()" style="font-size:11px; padding:2px 8px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer; margin-left:8px;">Go</button>' : '';
    const blockedBadge = (c.blocked && !c.done) ? ' <span style="font-size:10px; padding:1px 6px; border-radius:8px; background:var(--gray-100); color:var(--gray-400); margin-left:6px;">Blocked</span>' : '';
    const dimStyle = (c.blocked && !c.done) ? ' opacity:0.5;' : '';
    // Mini progress bar for items with progress
    let progressBar = '';
    if (c.progress !== undefined && !c.done) {
      const pColor = c.progress >= 50 ? 'var(--blue)' : c.progress > 0 ? '#d97706' : 'var(--gray-300)';
      progressBar = '<div style="height:4px; background:var(--gray-100); border-radius:2px; margin-top:4px; width:120px;"><div style="height:100%; width:' + c.progress + '%; background:' + pColor + '; border-radius:2px;"></div></div>';
    }
    assemblyHtml += '<div class="checklist-item" style="' + dimStyle + '">' +
      '<div class="check-icon ' + iconClass + '">' + iconChar + '</div>' +
      '<div style="flex:1;"><div class="checklist-label">' + c.label + actionBtn + blockedBadge + '</div>' +
      '<div class="checklist-detail">' + c.detail + '</div>' + progressBar + '</div></div>';
  });

  document.getElementById('assemblyContent').innerHTML = assemblyHtml;

  // ── PM Review Panel: Notes + Invoice Reclasses ──────────────────────
  (async function populatePmReview() {
    let totalItems = 0;
    const panel = document.getElementById('pmReviewPanel');

    // Section 1: PM Notes
    const linesWithNotes = lines.filter(l => l.notes && l.notes.trim().length > 0);
    const notesContainer = document.getElementById('pmNotesContainer');
    const notesEmpty = document.getElementById('pmNotesEmpty');
    const notesCount = document.getElementById('pmNotesCount');

    if (linesWithNotes.length > 0) {
      notesEmpty.style.display = 'none';
      notesCount.textContent = linesWithNotes.length;
      notesContainer.innerHTML = linesWithNotes.map(l =>
        '<div style="display:flex; align-items:flex-start; gap:12px; padding:10px 12px; border-radius:8px; margin-bottom:6px;" onmouseover="this.style.background=\'var(--gray-50)\'" onmouseout="this.style.background=\'\'">' +
          '<span onclick="scrollToGlRow(\'' + l.gl_code + '\')" style="font-family:monospace; font-size:12px; font-weight:600; color:var(--blue); background:var(--blue-light); padding:3px 8px; border-radius:4px; white-space:nowrap; cursor:pointer;" title="Click to scroll to row">' + l.gl_code + '</span>' +
          '<span style="font-size:12px; color:var(--gray-500); min-width:140px;">' + (l.description || '') + '</span>' +
          '<div style="flex:1; font-size:13px; color:var(--gray-700); background:#fffbeb; padding:6px 10px; border-radius:6px; border-left:3px solid #fbbf24;">' + (l.notes || '') + '</div>' +
        '</div>'
      ).join('');
      totalItems += linesWithNotes.length;
    } else {
      notesEmpty.style.display = '';
      notesContainer.innerHTML = '';
      notesCount.textContent = '0';
    }

    // Section 2: Invoice Reclasses (aggregated from expense distribution data)
    const reclassCount = document.getElementById('pmReclassCount');
    const reclassBody = document.getElementById('pmReclassBody');
    const reclassEmpty = document.getElementById('pmReclassEmpty');
    const reclassSummary = document.getElementById('pmReclassSummary');

    const expData = await faFetchExpenseData();
    if (expData && expData.gl_groups) {
      // Flatten all invoices across GL groups and find reclassed ones
      const allInvoices = [];
      expData.gl_groups.forEach(g => {
        if (g.invoices) g.invoices.forEach(inv => allInvoices.push(inv));
      });
      const reclassed = allInvoices.filter(inv => inv.reclass_to_gl);

      // Aggregate by from_gl → to_gl
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
        const totalAmt = groups.reduce((s, g) => s + Math.abs(g.total), 0);
        reclassSummary.style.display = 'flex';
        reclassSummary.innerHTML =
          '<div><span style="color:var(--gray-500);">Invoices reclassed:</span> <span style="font-weight:700;">' + reclassed.length + '</span></div>' +
          '<div><span style="color:var(--gray-500);">Total amount moved:</span> <span style="font-weight:700;">' + fmt(totalAmt) + '</span></div>' +
          '<div><span style="color:var(--gray-500);">GL moves:</span> <span style="font-weight:700;">' + groups.length + '</span></div>';

        reclassBody.innerHTML = '';
        groups.forEach((g, gi) => {
          const fromDesc = (lines.find(l => l.gl_code === g.from_gl) || {}).description || '';
          const toDesc = (lines.find(l => l.gl_code === g.to_gl) || {}).description || '';
          const invIds = g.invoices.map(i => i.id).join(',');
          const gid = 'farg_' + gi;
          const tr = document.createElement('tr');
          tr.id = 'pmrc_' + g.from_gl + '_' + g.to_gl;
          tr.style.cssText = 'transition:background 0.15s; cursor:pointer;';
          tr.onmouseover = function() { this.style.background='var(--gray-50)'; };
          tr.onmouseout = function() { this.style.background=''; };
          tr.onclick = function(e) { if (e.target.tagName === 'BUTTON') return; toggleReclassInvDetail(gid); };
          tr.innerHTML =
            '<td style="padding:10px;"><span id="' + gid + '_arrow" style="display:inline-block; font-size:10px; color:var(--gray-400); transition:transform 0.2s; margin-right:6px;">▶</span><span style="font-family:monospace; font-size:12px; font-weight:700;">' + g.from_gl + '</span><div style="padding-left:20px; font-size:11px; color:var(--gray-400);">' + fromDesc + '</div></td>' +
            '<td style="padding:10px 4px; color:var(--orange); font-weight:700; font-size:16px;">→</td>' +
            '<td style="padding:10px;"><span style="font-family:monospace; font-size:12px; font-weight:700;">' + g.to_gl + '</span><div style="font-size:11px; color:var(--gray-400);">' + toDesc + '</div></td>' +
            '<td style="padding:10px;"><span style="font-size:11px; background:var(--orange-light); color:var(--orange); padding:2px 8px; border-radius:10px; font-weight:600;">' + g.invoices.length + ' invoice' + (g.invoices.length !== 1 ? 's' : '') + '</span></td>' +
            '<td style="padding:10px; text-align:right; font-weight:600; font-variant-numeric:tabular-nums;">' + fmt(g.total) + '</td>' +
            '<td style="padding:10px; font-size:12px; color:var(--gray-600); font-style:italic; max-width:200px;">' + (g.notes ? '"' + g.notes + '"' : '') + '</td>' +
            '<td style="padding:10px; text-align:right;" id="pmrc_action_' + g.from_gl + '_' + g.to_gl + '">' +
              '<button onclick="acceptPmReclass(\'' + g.from_gl + '\',\'' + g.to_gl + '\',' + g.total + ',\'' + invIds + '\')" style="padding:5px 12px; font-size:12px; font-weight:600; border-radius:6px; cursor:pointer; background:var(--green-light); color:var(--green); border:1px solid #86efac;">✓ Accept</button> ' +
              '<button onclick="undoPmReclass(\'' + g.from_gl + '\',\'' + g.to_gl + '\',\'' + invIds + '\')" style="padding:5px 12px; font-size:12px; font-weight:600; border-radius:6px; cursor:pointer; background:var(--gray-100); color:var(--gray-600); border:1px solid var(--gray-300); margin-left:6px;">Undo</button>' +
            '</td>';
          reclassBody.appendChild(tr);
          // Add expandable invoice detail rows (hidden by default)
          g.invoices.forEach(inv => {
            const itr = document.createElement('tr');
            itr.className = 'reclass-inv-detail';
            itr.dataset.group = gid;
            itr.style.cssText = 'display:none; background:#fafbfc;';
            const invDate = inv.invoice_date || inv.date || '';
            const cleanDate = invDate ? invDate.split('T')[0] : '';
            const invNum = inv.invoice_num || inv.invoice_number || inv.ref || '';
            const invVendor = inv.payee_name || inv.vendor_name || inv.vendor || '';
            const invDesc = inv.notes || inv.description || '';
            const toGlName = (lines.find(l => l.gl_code === inv.reclass_to_gl) || {}).description || inv.reclass_to_gl;
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
        reclassSummary.style.display = 'none';
        reclassBody.innerHTML = '';
        reclassCount.textContent = '0';
      }
    } else {
      reclassEmpty.style.display = '';
      reclassSummary.style.display = 'none';
      reclassBody.innerHTML = '';
      reclassCount.textContent = '0';
    }

    // Section 3: Budget Proposals (PM changes to budget figures)
    const proposalsCount = document.getElementById('pmProposalsCount');
    const proposalsBody = document.getElementById('pmProposalsBody');
    const proposalsEmpty = document.getElementById('pmProposalsEmpty');
    const proposalsSummary = document.getElementById('pmProposalsSummary');

    // Detect PM proposals: lines where PM changed the budget via increase_pct, override, or direct proposed_budget
    const proposals = lines.filter(l => {
      if (l.fa_proposed_status === 'accepted' || l.fa_proposed_status === 'rejected') return true;  // show resolved ones too
      const hasPct = (l.increase_pct || 0) !== 0;
      const hasOverride = l.estimate_override !== null && l.estimate_override !== undefined;
      const hasForecastOvr = l.forecast_override !== null && l.forecast_override !== undefined;
      const hasProposed = (l.proposed_budget || 0) !== 0 && Math.abs((l.proposed_budget || 0) - (l.current_budget || 0)) > 0.01;
      return hasPct || hasOverride || hasForecastOvr || hasProposed;
    });

    if (proposals.length > 0) {
      proposalsEmpty.style.display = 'none';
      proposalsCount.textContent = proposals.filter(l => !l.fa_proposed_status || l.fa_proposed_status === 'commented').length;
      const pending = proposals.filter(l => !l.fa_proposed_status || l.fa_proposed_status === 'commented').length;
      const accepted = proposals.filter(l => l.fa_proposed_status === 'accepted').length;
      const rejected = proposals.filter(l => l.fa_proposed_status === 'rejected').length;
      proposalsSummary.style.display = 'flex';
      proposalsSummary.innerHTML =
        '<div><span style="color:var(--gray-500);">Total proposals:</span> <span style="font-weight:700;">' + proposals.length + '</span></div>' +
        '<div><span style="color:var(--gray-500);">Pending:</span> <span style="font-weight:700; color:#b45309;">' + pending + '</span></div>' +
        '<div><span style="color:var(--gray-500);">Accepted:</span> <span style="font-weight:700; color:var(--green);">' + accepted + '</span></div>' +
        (rejected > 0 ? '<div><span style="color:var(--gray-500);">Rejected:</span> <span style="font-weight:700; color:var(--red);">' + rejected + '</span></div>' : '');

      proposalsBody.innerHTML = '';
      proposals.forEach(l => {
        const proposed = l.proposed_budget || 0;
        const current = l.current_budget || 0;
        const change = proposed - current;
        const pct = current !== 0 ? ((change / current) * 100).toFixed(1) : '—';
        let method = '';
        if ((l.increase_pct || 0) !== 0) method = (l.increase_pct > 0 ? '+' : '') + l.increase_pct.toFixed(1) + '% increase';
        else if (l.estimate_override !== null && l.estimate_override !== undefined) method = 'Manual override';
        else method = 'Direct edit';

        const status = l.fa_proposed_status || 'pending';
        let statusBadge = '';
        let actionHtml = '';
        if (status === 'accepted') {
          statusBadge = '<span style="background:#dcfce7; color:#166534; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✓ Accepted</span>';
          actionHtml = '<span style="color:var(--gray-400); font-size:11px;">Done</span>';
        } else if (status === 'rejected') {
          statusBadge = '<span style="background:#fef2f2; color:#991b1b; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✗ Rejected</span>';
          actionHtml = '<span style="color:var(--gray-400); font-size:11px;">Done</span>';
        } else if (status === 'commented') {
          statusBadge = '<span style="background:#fef3c7; color:#92400e; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">💬 Commented</span>';
          actionHtml = proposalActionButtons(l.gl_code);
        } else {
          statusBadge = '<span style="background:#fff7ed; color:#b45309; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">● Pending</span>';
          actionHtml = proposalActionButtons(l.gl_code);
        }

        const tr = document.createElement('tr');
        // FA dir 2026-06-04: prefix 'pendprop_' (NOT 'prop_') so these pending-
        // review rows don't collide with the budget-grid proposed input cells
        // (id 'prop_<gl>'). The duplicate id made getElementById('prop_'+gl)
        // return this <tr> instead of the <input>, which broke the Sheet Total
        // proposed FORMULA (fxSubtotalFocus read $0 parts) and the live total
        // recompute (sumGLs) + per-cell updates whenever proposals were pending.
        tr.id = 'pendprop_' + l.gl_code;
        tr.style.cssText = 'transition:background 0.15s;';
        tr.onmouseover = function() { this.style.background='var(--gray-50)'; };
        tr.onmouseout = function() { this.style.background=''; };
        const changeColor = change > 0 ? 'var(--red)' : change < 0 ? 'var(--green)' : 'var(--gray-500)';
        tr.innerHTML =
          '<td style="padding:10px;"><span onclick="scrollToGlRow(\'' + l.gl_code + '\')" style="font-family:monospace; font-size:12px; font-weight:700; color:var(--blue); cursor:pointer;">' + l.gl_code + '</span></td>' +
          '<td style="padding:10px; font-size:12px; color:var(--gray-600); max-width:180px;">' + (l.description || '') + '</td>' +
          '<td style="padding:10px; text-align:right; font-variant-numeric:tabular-nums; font-size:13px;">' + fmt(current) + '</td>' +
          '<td style="padding:10px; text-align:right; font-weight:700; font-variant-numeric:tabular-nums; font-size:13px;">' + fmt(proposed) + '</td>' +
          '<td style="padding:10px; text-align:right; font-variant-numeric:tabular-nums; font-size:13px; color:' + changeColor + ';">' + (change >= 0 ? '+' : '') + fmt(change) + ' (' + pct + '%)</td>' +
          '<td style="padding:10px; font-size:11px; color:var(--gray-500);">' + method + '</td>' +
          '<td style="padding:10px; text-align:center;">' + statusBadge + '</td>' +
          '<td style="padding:10px; text-align:right; white-space:nowrap;">' + actionHtml + '</td>';
        proposalsBody.appendChild(tr);
      });
      totalItems += pending;
    } else {
      proposalsEmpty.style.display = '';
      proposalsSummary.style.display = 'none';
      proposalsBody.innerHTML = '';
      proposalsCount.textContent = '0';
    }

    // Show/hide the panel
    if (totalItems > 0) {
      panel.style.display = '';
      document.getElementById('pmReviewBadgeText').textContent = totalItems + ' item' + (totalItems !== 1 ? 's' : '') + ' need review';
    }
  })();

  // Download Excel button
  // FA directive 2026-05-15 (Phase 6): point Download Excel at the new
  // /api/export-excel endpoint. Starts from the building's approved 2026
  // Excel and overlays product data. Old /api/download-budget kept for now
  // as a fallback path; will be removed in Pass 1b verification.
  document.getElementById('downloadExcelBtn').href = '/api/export-excel/' + entityCode;

  // Budget Workbook Tabs
  allSheets = data.sheets || {};  // global for Budget Summary access
  window._reTaxesData = data.re_taxes || null;  // RE Taxes tab data for co-ops
  window._data = data;  // Store data for renderBudgetSummary access to audit.summary_years
  const sheets = allSheets;
  const sheetOrder = data.sheet_order || Object.keys(sheets);
  const tabsDiv = document.getElementById('sheetTabs');
  const contentDiv = document.getElementById('sheetContent');
  tabsDiv.innerHTML = '';

  {
    // Summary tab is ALWAYS shown — even before detail lines exist
    // (BudgetSummaryRow data may be imported from approved Excel)
    const summaryTab = document.createElement('button');
    summaryTab.textContent = 'Summary';
    summaryTab.className = 'sheet-tab active';
    summaryTab.dataset.sheet = 'Summary';
    summaryTab.style.cssText = 'padding-left:20px; position:relative;';
    // green dot indicator
    const dot = document.createElement('span');
    dot.style.cssText = 'position:absolute;left:6px;top:50%;transform:translateY(-50%);width:6px;height:6px;background:#057a55;border-radius:50%;';
    summaryTab.prepend(dot);
    summaryTab.onclick = () => renderSheet('Summary', null, summaryTab);
    tabsDiv.appendChild(summaryTab);

    sheetOrder.forEach((sheetName) => {
      const tab = document.createElement('button');
      tab.textContent = sheetName;
      tab.className = 'sheet-tab';
      tab.dataset.sheet = sheetName;
      tab.onclick = () => renderSheet(sheetName, sheets[sheetName], tab);
      tabsDiv.appendChild(tab);
    });

    // Add Commercial Rent tab (FA directive 2026-05-14 Phase 5).
    // Shown for every building. If the building has no commercial rent rows
    // in its approved Excel, the tab shows a clean empty state with an
    // "Add tenant" affordance (Phase 2). Until Phase 2 lands, FAs viewing
    // an empty-state building can still confirm the system correctly
    // detected "no commercial rent here."
    const commTab = document.createElement('button');
    commTab.textContent = '\ud83c\udfe2 Commercial';
    commTab.className = 'sheet-tab';
    commTab.dataset.sheet = '__commercial__';
    commTab.style.background = '#fff7ed';
    commTab.style.color = '#9a3412';
    commTab.onclick = () => {
      document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
      commTab.classList.add('active');
      renderCommercialTab(contentDiv);
    };
    tabsDiv.appendChild(commTab);

    // CAM Allocation tab (condos / cond-ops only) — Schedule A-1 per-class split.
    // Gated on building_type so co-ops/rentals never see the noise.
    const _camBt = (data.building_type || '').toLowerCase();
    if (_camBt.indexOf('condo') !== -1 || _camBt.indexOf('cond-op') !== -1) {
      const camTab = document.createElement('button');
      camTab.textContent = '🏘 CAM';
      camTab.className = 'sheet-tab';
      camTab.dataset.sheet = '__cam__';
      camTab.style.background = '#eef2ff';
      camTab.style.color = '#3730a3';
      camTab.onclick = () => {
        document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
        camTab.classList.add('active');
        renderCamTab(contentDiv);
      };
      tabsDiv.appendChild(camTab);
    }

    // Add Assumptions tab
    const assumTab = document.createElement('button');
    assumTab.textContent = '\u2699 Assumptions';
    assumTab.className = 'sheet-tab';
    assumTab.style.marginLeft = 'auto';
    assumTab.style.background = 'var(--blue-light)';
    assumTab.style.color = 'var(--blue)';
    assumTab.onclick = () => {
      document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
      assumTab.classList.add('active');
      renderAssumptionsTab(data.assumptions || {}, contentDiv);
    };
    tabsDiv.appendChild(assumTab);

    // Add History tab
    const histTab = document.createElement('button');
    histTab.textContent = '\ud83d\udcdd History';
    histTab.className = 'sheet-tab';
    histTab.style.background = '#fef3c7';
    histTab.style.color = '#92400e';
    histTab.onclick = () => {
      document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
      histTab.classList.add('active');
      renderHistoryTab(contentDiv);
    };
    tabsDiv.appendChild(histTab);

    // FA dir 2026-05-24: read initial ?tab=X from URL so deep-links (or
    // back-nav from within the building) land on the right tab.
    let _initTab = 'Summary';
    try { _initTab = new URLSearchParams(window.location.search).get('tab') || 'Summary'; } catch (e) {}
    if (_initTab === 'Summary') {
      // skipPush so the initial render doesn't pollute history.
      renderSheet('Summary', null, summaryTab, { skipPush: true });
    } else {
      const targetTab = tabsDiv.querySelector('.sheet-tab[data-sheet="' + _initTab.replace(/"/g, '\\"') + '"]');
      if (targetTab) {
        // Trigger the tab's normal click flow but suppress the pushState
        // since the URL already has ?tab set.
        if (_initTab === '__commercial__' || _initTab === '__cam__') {
          targetTab.click();
        } else {
          renderSheet(_initTab, sheets[_initTab] || null, targetTab, { skipPush: true });
        }
      } else {
        renderSheet('Summary', null, summaryTab, { skipPush: true });
      }
    }
  }
}

// FA dir 2026-05-24: restore tab on browser back/forward. Finds the matching
// .sheet-tab and calls renderSheet through it (skipPush so we don't re-push).
window.addEventListener('popstate', function () {
  try {
    const tab = new URLSearchParams(window.location.search).get('tab') || 'Summary';
    const tabEl = document.querySelector('.sheet-tab[data-sheet="' + tab.replace(/"/g, '\\"') + '"]');
    if (!tabEl) return;
    if (tab === 'Summary') {
      renderSheet('Summary', null, tabEl, { skipPush: true });
    } else if (typeof sheets === 'object' && sheets[tab]) {
      renderSheet(tab, sheets[tab], tabEl, { skipPush: true });
    }
  } catch (e) {}
});

// ── Checklist Action Helpers ──
// ── Board Presentation: FA Review & Publish (2026-07-01) ──────────────
// Repurposes the Board Presentation button (was openBoardPresentation(), an
// internal-only overlay with no persistence — retired). The product drafts a
// client narrative from real budget data (_generate_client_narrative in
// workflow.py); an FA reviews/edits it here and must explicitly mark it
// reviewed before a client link can be published — mirrors the audit-review
// draft -> confirmed pattern already used elsewhere in this app.
async function openBoardNoticeReview() {
  const existing = document.getElementById('boardNoticeOverlay');
  if (existing) existing.remove();
  const overlay = document.createElement('div');
  overlay.id = 'boardNoticeOverlay';
  overlay.style.cssText = 'position:fixed; inset:0; z-index:9999; overflow-y:auto; background:#f6f5f2;';
  overlay.innerHTML = '<div style="padding:60px; text-align:center; color:#8a7e72;">Loading draft…</div>';
  document.body.appendChild(overlay);

  let data;
  try {
    const resp = await fetch('/api/board-notice/' + entityCode);
    data = await resp.json();
  } catch (e) {
    overlay.innerHTML = '<div style="padding:40px; color:#DE1C23;">Failed to load: ' + e.message + '</div>';
    return;
  }
  window._boardNoticeActive = data.active;
  renderBoardNoticeReview(overlay, data);
}

function renderBoardNoticeReview(overlay, data) {
  const n = data.active || {};
  const statusLabel = {draft: 'Draft — not yet reviewed', reviewed: 'Reviewed — ready to publish', published: 'Published — client link is live'}[data.status] || data.status;
  const statusColor = {draft: '#d97706', reviewed: '#3b82f6', published: '#16a34a'}[data.status] || '#666';

  let html = '<div style="max-width:760px; margin:0 auto; padding:32px 24px 80px; font-family:\'Plus Jakarta Sans\',-apple-system,sans-serif;">';
  html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">' +
    '<h2 style="margin:0; font-size:20px; color:#001721;">Board Presentation — Review &amp; Publish</h2>' +
    '<button onclick="document.getElementById(\'boardNoticeOverlay\').remove()" style="border:none; background:none; font-size:22px; cursor:pointer; color:#8a7e72;">&times;</button></div>';
  html += '<div style="display:flex; align-items:center; gap:8px; margin-bottom:20px; flex-wrap:wrap;"><span style="width:8px; height:8px; border-radius:50%; background:' + statusColor + ';"></span><span style="font-size:13px; font-weight:600; color:' + statusColor + ';">' + statusLabel + '</span>' +
    '<button onclick="boardNoticeRegenerate()" style="margin-left:auto; font-size:12px; padding:4px 10px; background:none; border:1px solid #ddd; border-radius:4px; cursor:pointer; color:#666;">Regenerate draft from current numbers</button></div>';

  html += '<p style="font-size:12px; color:#8a7e72; margin:0 0 20px;">The system drafted the sections below from the current budget — every dollar figure is real, but the "why" is only ever what you confirm or add here. Review each section, edit anything that needs a human touch (especially bracketed [FA: …] notes), then mark it reviewed. A client link can only be created after that.</p>';

  function field(label, key, value, rows) {
    return '<div style="margin-bottom:18px;">' +
      '<label style="display:block; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em; color:#666; margin-bottom:6px;">' + label + '</label>' +
      '<textarea data-key="' + key + '" rows="' + (rows || 3) + '" style="width:100%; font-family:inherit; font-size:14px; padding:10px 12px; border:1px solid #ddd; border-radius:4px; resize:vertical; box-sizing:border-box;">' + (value || '').replace(/</g,'&lt;') + '</textarea></div>';
  }

  html += field('Opening', 'opening', n.opening, 3);
  html += field('Driver summary', 'driver_summary', n.driver_summary, 2);
  html += field('Additional notes (e.g. reserve fund status — optional)', 'additional_notes', n.additional_notes, 2);

  function dateField(label, key, value) {
    return '<div style="margin-bottom:10px; display:inline-block; width:31%; min-width:150px; margin-right:2%; vertical-align:top;">' +
      '<label style="display:block; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em; color:#666; margin-bottom:6px;">' + label + '</label>' +
      '<input type="text" data-timeline="' + key + '" placeholder="e.g. February 5, 2027" value="' + (value || '').replace(/"/g,'&quot;') + '" style="width:100%; font-family:inherit; font-size:13px; padding:8px 10px; border:1px solid #ddd; border-radius:4px; box-sizing:border-box;"></div>';
  }
  const tl = n.timeline || {};
  html += '<div style="margin-bottom:18px;"><div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em; color:#666; margin-bottom:6px;">Key dates for the board</div>' +
    dateField('Review by', 'board_review_through', tl.board_review_through) +
    dateField('Board vote by', 'board_vote_by', tl.board_vote_by) +
    dateField('New charges effective', 'effective_date', tl.effective_date) +
    '</div>';

  html += '<div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em; color:#666; margin:24px 0 10px;">Anticipated questions</div>';
  (n.faq || []).forEach((item, i) => {
    html += '<div style="margin-bottom:14px; padding:12px 14px; background:#f8f8f6; border-radius:6px;">' +
      '<div style="font-size:13px; font-weight:600; margin-bottom:6px;">' + item.q.replace(/</g,'&lt;') + '</div>' +
      '<textarea data-faq="' + i + '" rows="2" style="width:100%; font-family:inherit; font-size:13px; padding:8px 10px; border:1px solid #ddd; border-radius:4px; resize:vertical; box-sizing:border-box;">' + (item.a || '').replace(/</g,'&lt;') + '</textarea></div>';
  });

  html += '<div style="display:flex; gap:10px; margin-top:28px; padding-top:20px; border-top:1px solid #eee; flex-wrap:wrap;">' +
    '<button onclick="boardNoticePreview()" style="padding:10px 18px; border:1px solid #4f46e5; background:#fff; color:#4f46e5; border-radius:4px; font-weight:600; cursor:pointer;">👁 Preview client view</button>' +
    '<button onclick="boardNoticeSave(false)" style="padding:10px 18px; border:1px solid #001721; background:#fff; color:#001721; border-radius:4px; font-weight:600; cursor:pointer;">Save draft</button>' +
    '<button onclick="boardNoticeSave(true)" style="padding:10px 18px; border:none; background:#001721; color:#fff; border-radius:4px; font-weight:600; cursor:pointer;">Save &amp; mark reviewed</button>';
  if (data.status === 'reviewed' || data.status === 'published') {
    html += '<button onclick="boardNoticePublish()" style="padding:10px 18px; border:none; background:#DE1C23; color:#fff; border-radius:4px; font-weight:600; cursor:pointer; margin-left:auto;">' + (data.status === 'published' ? 'Re-publish (regenerate link)' : 'Publish &amp; get client link') + '</button>';
  }
  html += '</div><div id="boardNoticeMsg" style="margin-top:14px; font-size:13px;"></div>';
  html += '</div>';
  overlay.innerHTML = html;
}

function _boardNoticeCollect() {
  const overlay = document.getElementById('boardNoticeOverlay');
  const narrative = JSON.parse(JSON.stringify(window._boardNoticeActive || {}));
  overlay.querySelectorAll('textarea[data-key]').forEach(t => { narrative[t.dataset.key] = t.value; });
  overlay.querySelectorAll('textarea[data-faq]').forEach(t => {
    const i = parseInt(t.dataset.faq, 10);
    if (narrative.faq && narrative.faq[i]) narrative.faq[i].a = t.value;
  });
  overlay.querySelectorAll('input[data-timeline]').forEach(inp => {
    if (!narrative.timeline) narrative.timeline = {};
    narrative.timeline[inp.dataset.timeline] = inp.value;
  });
  return narrative;
}

async function boardNoticeSave(markReviewed) {
  const msg = document.getElementById('boardNoticeMsg');
  msg.textContent = 'Saving…'; msg.style.color = '#666';
  const narrative = _boardNoticeCollect();
  const body = { narrative: narrative, mark_reviewed: !!markReviewed };
  if (markReviewed) body.reviewed_by = prompt('Your name (for the review record):') || '';
  try {
    const resp = await fetch('/api/board-notice/' + entityCode, { method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const d = await resp.json();
    if (!resp.ok) { msg.textContent = 'Error: ' + (d.error || resp.status); msg.style.color = '#DE1C23'; return; }
    msg.textContent = markReviewed ? 'Saved and marked reviewed.' : 'Draft saved.';
    msg.style.color = '#16a34a';
    const fresh = await (await fetch('/api/board-notice/' + entityCode)).json();
    window._boardNoticeActive = fresh.active;
    renderBoardNoticeReview(document.getElementById('boardNoticeOverlay'), fresh);
  } catch (e) { msg.textContent = 'Error: ' + e.message; msg.style.color = '#DE1C23'; }
}

async function boardNoticeRegenerate() {
  if (!confirm('Regenerate the draft from the current budget numbers? Any unreviewed edits will be replaced (a reviewed/published narrative starts a fresh review cycle).')) return;
  const resp = await fetch('/api/board-notice/' + entityCode + '?regenerate=1');
  const data = await resp.json();
  window._boardNoticeActive = data.active;
  renderBoardNoticeReview(document.getElementById('boardNoticeOverlay'), data);
}

async function boardNoticePreview() {
  // Save the on-screen edits as a draft first so the preview shows exactly
  // what the FA is looking at, then open the rendered client document in a
  // new tab. Saving here never advances status -- same as "Save draft".
  const msg = document.getElementById('boardNoticeMsg');
  msg.textContent = 'Saving draft & opening preview…'; msg.style.color = '#666';
  try {
    const narrative = _boardNoticeCollect();
    const resp = await fetch('/api/board-notice/' + entityCode, { method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ narrative: narrative }) });
    if (!resp.ok) { const d = await resp.json(); msg.textContent = 'Error: ' + (d.error || resp.status); msg.style.color = '#DE1C23'; return; }
    window._boardNoticeActive = narrative;
    msg.textContent = 'Draft saved — preview opened in a new tab.';
    msg.style.color = '#16a34a';
    window.open('/api/board-notice/' + entityCode + '/preview', '_blank');
  } catch (e) { msg.textContent = 'Error: ' + e.message; msg.style.color = '#DE1C23'; }
}

async function boardNoticePublish() {
  const msg = document.getElementById('boardNoticeMsg');
  msg.textContent = 'Publishing…'; msg.style.color = '#666';
  try {
    const resp = await fetch('/api/board-notice/' + entityCode + '/publish', { method: 'POST' });
    const d = await resp.json();
    if (!resp.ok) { msg.textContent = 'Error: ' + (d.error || resp.status); msg.style.color = '#DE1C23'; return; }
    const modal = document.createElement('div');
    modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);display:flex;align-items:center;justify-content:center;z-index:10000;';
    modal.innerHTML = '<div style="background:white;border-radius:12px;padding:32px;max-width:500px;width:90%;">' +
      '<h3 style="margin-bottom:12px;">Board Presentation link</h3>' +
      '<p style="font-size:13px;color:#64748b;margin-bottom:16px;">Share this link with the Board. It shows exactly what was reviewed and published just now — it will not change even if the budget is edited later.</p>' +
      '<input type="text" value="' + d.url + '" readonly style="width:100%;padding:10px;border:1px solid #e2e8f0;border-radius:6px;font-size:13px;margin-bottom:12px;box-sizing:border-box;" onclick="this.select()">' +
      '<div style="display:flex;gap:8px;justify-content:flex-end;">' +
      '<button onclick="navigator.clipboard.writeText(\'' + d.url + '\');this.textContent=\'Copied!\'" style="padding:8px 16px;background:#001721;color:white;border:none;border-radius:6px;cursor:pointer;">Copy link</button>' +
      '<button onclick="window.open(\'' + d.url + '\',\'_blank\')" style="padding:8px 16px;background:#16a34a;color:white;border:none;border-radius:6px;cursor:pointer;">Open</button>' +
      '<button onclick="this.closest(\'div\').parentElement.remove()" style="padding:8px 16px;background:#eee;border:none;border-radius:6px;cursor:pointer;">Close</button>' +
      '</div></div>';
    document.body.appendChild(modal);
    modal.onclick = (e) => { if (e.target === modal) modal.remove(); };
    const fresh = await (await fetch('/api/board-notice/' + entityCode)).json();
    window._boardNoticeActive = fresh.active;
    renderBoardNoticeReview(document.getElementById('boardNoticeOverlay'), fresh);
  } catch (e) { msg.textContent = 'Error: ' + e.message; msg.style.color = '#DE1C23'; }
}

function openAssumptions() {
  const tabs = document.querySelectorAll('.sheet-tab');
  const assumTab = Array.from(tabs).find(t => t.textContent.includes('Assumptions'));
  if (assumTab) assumTab.click();
  assumTab?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

// ── Assumptions Tab ──
let _assumSaveTimer = null;
function assumAutoSave(section, field, value, el) {
  // FA directive 2026-05-10: el is the input element. Skip when value
  // didn't change. Backwards-compatible: callers without el still work.
  if (el && _isUnchangedInput(el)) return;
  clearTimeout(_assumSaveTimer);
  const indicator = document.getElementById('faSaveIndicator');
  indicator.textContent = 'Saving assumptions...';
  _assumSaveTimer = setTimeout(async () => {
    const payload = {};
    payload[section] = {};
    payload[section][field] = value;
    const resp = await fetch('/api/budget-assumptions/' + entityCode, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
    const result = await resp.json();
    if (result.recalculated > 0) {
      indicator.textContent = 'Saved — ' + result.recalculated + ' lines recalculated';
      showToast(result.recalculated + ' budget lines recalculated', 'success');
      // Reload data so GL tabs show updated numbers
      setTimeout(() => loadDetail(), 500);
    } else {
      indicator.textContent = 'Assumptions saved';
    }
    setTimeout(() => { indicator.textContent = ''; }, 3000);
  }, 800);
}

function renderAssumptionsTab(assumptions, contentDiv) {
  const a = assumptions || {};
  const pt = a.payroll_tax || {};
  const ub = a.union_benefits || {};
  const wc = a.workers_comp || {};
  const wi = a.wage_increase || {};
  const ir = a.insurance_renewal || {};
  const en = a.energy || {};
  const ws = a.water_sewer || {};
  const rt = a.re_taxes_overrides || {};

  function pctVal(v) { return v ? (v * 100).toFixed(2) : '0'; }
  function numVal(v) { return v || 0; }

  // Inject scoped styles once
  if (!document.getElementById('asm-portal-style')) {
    const st = document.createElement('style');
    st.id = 'asm-portal-style';
    st.textContent =
      '.asm-portal { padding: 8px 0 40px; font-variant-numeric: tabular-nums; }' +
      '.asm-portal .asm-section { margin-bottom: 32px; }' +
      '.asm-portal .asm-section:last-child { margin-bottom: 0; }' +
      '.asm-portal .asm-section-header { display: flex; align-items: baseline; gap: 12px; margin-bottom: 14px; }' +
      '.asm-portal .asm-dot { width: 8px; height: 8px; border-radius: 50%; flex: 0 0 auto; background: var(--blue); display: inline-block; }' +
      '.asm-portal .asm-tag { font-size: 11px; font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase; color: var(--gray-700); flex: 0 0 auto; }' +
      '.asm-portal .asm-hint { font-size: 12px; color: var(--gray-500); font-weight: 400; flex: 0 0 auto; }' +
      '.asm-portal .asm-rule { flex: 1 1 auto; height: 1px; background: var(--gray-200); margin-left: 4px; }' +
      '.asm-portal .asm-section.payroll { --accent: var(--blue); }' +
      '.asm-portal .asm-section.operating { --accent: var(--green); }' +
      '.asm-portal .asm-section.taxes { --accent: var(--gray-700); }' +
      '.asm-portal .asm-section.operating .asm-dot { background: var(--green); }' +
      '.asm-portal .asm-section.taxes .asm-dot { background: var(--gray-700); }' +
      '.asm-portal .asm-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 16px; align-items: start; }' +
      '.asm-portal .asm-grid.single { grid-template-columns: minmax(340px, 600px); }' +
      '@media (max-width: 780px) { .asm-portal .asm-grid { grid-template-columns: 1fr; } }' +
      '.asm-portal .asm-card { background: #fff; border-radius: 10px; border: 1px solid var(--gray-200); border-left: 3px solid var(--accent, var(--blue)); padding: 18px 22px 16px; box-shadow: 0 1px 2px rgba(26, 23, 20, 0.03); }' +
      '.asm-portal .asm-card-title { font-size: 11px; font-weight: 700; letter-spacing: 0.09em; text-transform: uppercase; color: var(--gray-700); margin: 0 0 12px; display: flex; align-items: center; gap: 10px; }' +
      '.asm-portal .asm-card-title::before { content: \'\'; display: inline-block; width: 14px; height: 1.5px; background: var(--accent, var(--blue)); border-radius: 1px; flex: 0 0 auto; }' +
      '.asm-portal .asm-row { display: flex; align-items: center; justify-content: space-between; gap: 14px; padding: 6px 0; }' +
      '.asm-portal .asm-row label { font-size: 13px; color: var(--gray-500); font-weight: 400; flex: 1 1 auto; min-width: 0; }' +
      '.asm-portal .asm-input-wrap { display: inline-flex; align-items: center; gap: 6px; flex: 0 0 auto; }' +
      '.asm-portal .asm-unit { font-size: 11px; color: var(--gray-500); font-weight: 500; min-width: 8px; text-align: left; }' +
      '.asm-portal .asm-input { width: 104px; padding: 6px 10px; border: 1px solid var(--gray-200); border-radius: 6px; font-size: 13px; font-family: inherit; text-align: right; background: #fff; color: var(--gray-900); font-variant-numeric: tabular-nums; transition: border-color 120ms, box-shadow 120ms, background 120ms; -moz-appearance: textfield; }' +
      '.asm-portal .asm-input::-webkit-outer-spin-button, .asm-portal .asm-input::-webkit-inner-spin-button { -webkit-appearance: none; margin: 0; }' +
      '.asm-portal .asm-input:hover { border-color: var(--gray-300); }' +
      '.asm-portal .asm-input:focus { outline: none; border-color: var(--accent, var(--blue)); background: var(--gray-50); box-shadow: 0 0 0 3px rgba(90, 74, 63, 0.12); }' +
      '.asm-portal .asm-sub { font-size: 10px; font-weight: 700; letter-spacing: 0.10em; text-transform: uppercase; color: var(--gray-500); padding: 10px 0 2px; }' +
      '.asm-portal .asm-card .asm-sub:first-child { padding-top: 0; }';
    document.head.appendChild(st);
  }

  // Input helpers — preserve exact assumAutoSave field names & save semantics
  function pctF(section, key, val) {
    return '<div class="asm-input-wrap"><input class="asm-input" type="number" step="any" value="' + pctVal(val) +
      '" onchange="assumAutoSave(\'' + section + '\',\'' + key + '\', this.value/100, this)"><span class="asm-unit">%</span></div>';
  }
  function pctRawF(section, key, valStr) {
    return '<div class="asm-input-wrap"><input class="asm-input" type="number" step="any" value="' + valStr +
      '" onchange="assumAutoSave(\'' + section + '\',\'' + key + '\', this.value/100, this)"><span class="asm-unit">%</span></div>';
  }
  function numF(section, key, val, unit) {
    return '<div class="asm-input-wrap"><input class="asm-input" type="number" step="any" value="' + numVal(val) +
      '" onchange="assumAutoSave(\'' + section + '\',\'' + key + '\', parseFloat(this.value)||0, this)"><span class="asm-unit">' + (unit || '') + '</span></div>';
  }
  function txtF(section, key, val) {
    return '<div class="asm-input-wrap"><input class="asm-input" type="text" value="' + (val || '') +
      '" onchange="assumAutoSave(\'' + section + '\',\'' + key + '\', this.value, this)"><span class="asm-unit"></span></div>';
  }

  function row(label, inputHtml) {
    return '<div class="asm-row"><label>' + label + '</label>' + inputHtml + '</div>';
  }
  function sub(label) {
    return '<div class="asm-sub">' + label + '</div>';
  }
  function card(title, body) {
    return '<div class="asm-card"><h3 class="asm-card-title">' + title + '</h3><div class="asm-fields">' + body + '</div></div>';
  }
  function sectionWrap(cls, tag, hint, gridCls, cards) {
    return '<div class="asm-section ' + cls + '">' +
      '<div class="asm-section-header">' +
        '<span class="asm-dot"></span>' +
        '<span class="asm-tag">' + tag + '</span>' +
        '<span class="asm-hint">' + hint + '</span>' +
        '<span class="asm-rule"></span>' +
      '</div>' +
      '<div class="asm-grid' + (gridCls ? ' ' + gridCls : '') + '">' + cards + '</div>' +
    '</div>';
  }

  // ── Payroll ─────────────────────────────────────────
  const payrollTaxCard = card('Payroll Tax Rates',
    row('FICA', pctF('payroll_tax','FICA', pt.FICA)) +
    row('SUI', pctF('payroll_tax','SUI', pt.SUI)) +
    row('FUI', pctF('payroll_tax','FUI', pt.FUI)) +
    row('MTA', pctF('payroll_tax','MTA', pt.MTA)) +
    row('NYS Disability', pctF('payroll_tax','NYS_Disability', pt.NYS_Disability)) +
    row('PFL', pctF('payroll_tax','PFL', pt.PFL))
  );
  const unionCard = card('Union Benefits · 32BJ',
    row('Welfare · $/mo/man', numF('union_benefits','welfare_monthly', ub.welfare_monthly, '$')) +
    row('Pension · $/wk/man', numF('union_benefits','pension_weekly', ub.pension_weekly, '$')) +
    row('Supp Retirement · $/wk', numF('union_benefits','supp_retirement_weekly', ub.supp_retirement_weekly, '$')) +
    row('Legal · $/mo', numF('union_benefits','legal_monthly', ub.legal_monthly, '$')) +
    row('Training · $/mo', numF('union_benefits','training_monthly', ub.training_monthly, '$')) +
    row('Profit Sharing · $/qtr', numF('union_benefits','profit_sharing_quarterly', ub.profit_sharing_quarterly, '$'))
  );
  const wcWiCard = card('Workers Comp &amp; Wage Increase',
    sub('Workers Comp') +
    row('Workers Comp', pctF('workers_comp','percent', wc.percent)) +
    sub('Wage Increase') +
    row('Wage Increase', pctF('wage_increase','percent', wi.percent)) +
    row('Effective Week', txtF('wage_increase','effective_week', wi.effective_week || 'Wk 16')) +
    row('Pre-Increase Weeks', numF('wage_increase','pre_increase_weeks', wi.pre_increase_weeks, '')) +
    row('Post-Increase Weeks', numF('wage_increase','post_increase_weeks', wi.post_increase_weeks, ''))
  );
  const payrollSection = sectionWrap('payroll', 'Payroll', 'staffing, benefits, wage growth', '',
    payrollTaxCard + unionCard + wcWiCard);

  // ── Operating ───────────────────────────────────────
  const insCard = card('Insurance Renewal',
    row('Renewal Increase', pctF('insurance_renewal','increase_percent', ir.increase_percent)) +
    row('Effective Date', txtF('insurance_renewal','effective_date', ir.effective_date || ('Mar ' + BY))) +
    row('Pre-Renewal Months', numF('insurance_renewal','pre_renewal_months', ir.pre_renewal_months, '')) +
    row('Post-Renewal Months', numF('insurance_renewal','post_renewal_months', ir.post_renewal_months, ''))
  );
  const energyCard = card('Energy Rates',
    sub('Gas') +
    row('ESCO Rate · $/Therm', numF('energy','gas_esco_rate', en.gas_esco_rate, '$')) +
    row('Rate Increase', pctF('energy','gas_rate_increase', en.gas_rate_increase)) +
    sub('Electric') +
    row('ESCO Rate · $/KWH', numF('energy','electric_esco_rate', en.electric_esco_rate, '$')) +
    row('Rate Increase', pctF('energy','electric_rate_increase', en.electric_rate_increase)) +
    sub('Oil') +
    row('Price · $/gallon', numF('energy','oil_price_per_gallon', en.oil_price_per_gallon, '$')) +
    row('Rate Increase', pctF('energy','oil_rate_increase', en.oil_rate_increase))
  );
  const waterCard = card('Water &amp; Sewer',
    row('Rate Increase', pctF('water_sewer','rate_increase', ws.rate_increase))
  );
  const operatingSection = sectionWrap('operating', 'Operating', 'insurance, utilities, recurring costs', '',
    insCard + energyCard + waterCard);

  // ── Taxes ───────────────────────────────────────────
  const taxCard = card('Real Estate Taxes',
    row('Tax Rate', pctRawF('re_taxes_overrides','tax_rate', rt.tax_rate ? (rt.tax_rate * 100).toFixed(4) : '0')) +
    row('Est. Tax Rate', pctRawF('re_taxes_overrides','est_tax_rate', rt.est_tax_rate ? (rt.est_tax_rate * 100).toFixed(4) : '0'))
  );
  const taxesSection = sectionWrap('taxes', 'Taxes', 'real estate tax rate inputs', 'single', taxCard);

  contentDiv.innerHTML = '<div class="asm-portal">' + payrollSection + operatingSection + taxesSection + '</div>';
}

// ── Commercial Rent Tab (Phase 5.1 read-only viewer) ──
// Fetches /api/commercial/<ec> which auto-imports from the approved Excel
// on first call. Renders: tenant cards (one per commercial tenant) with
// rent periods + lease notes + escalation model badge.
// Phase 2 will add: edit buttons, escalation engine UI, Summary feed.
// ── CAM Allocation tab (condo Schedule A-1) ──────────────────────────────
// Matrix: operating-expense GLs (rows) × unit classes (columns). Each cell is
// the allocated $; the per-row code seeds the split (B/R/S/100%-class) and any
// typed cell overrides it. The server (_cam_compute) is the single source of
// the math, so every edit PUTs then re-fetches — the displayed matrix always
// equals the reconciled server result (no client-side drift).
async function renderCamTab(contentDiv) {
  contentDiv.innerHTML =
    '<div style="padding:40px 24px; text-align:center; color:var(--gray-500);">' +
    '<div style="display:inline-block; width:32px; height:32px; border:3px solid var(--gray-200); border-top-color:#4f46e5; border-radius:50%; animation:spin 0.6s linear infinite;"></div>' +
    '<p style="margin-top:12px; font-size:13px;">Loading CAM allocation&hellip;</p></div>';
  let data;
  try {
    const resp = await fetch('/api/cam/' + entityCode);
    data = await resp.json();
  } catch (err) {
    contentDiv.innerHTML = '<div style="padding:24px; color:var(--red);">Failed to load: ' + (err.message || err) + '</div>';
    return;
  }
  window._camData = data;
  // Required-increase is a separate, secondary calculation -- if it fails,
  // the main allocation matrix should still render.
  let reqIncrease = null;
  try {
    const riResp = await fetch('/api/cam/' + entityCode + '/required-increase');
    reqIncrease = await riResp.json();
    if (reqIncrease && reqIncrease.error) reqIncrease = null;
  } catch (err) { reqIncrease = null; }
  const classes = data.classes || [];
  const lines = data.lines || [];
  const fmt0 = (n) => '$' + Math.round(n || 0).toLocaleString();
  const pct = (s) => (Math.round((s || 0) * 1000) / 10).toFixed(1) + '%';

  let html = '<div style="padding:18px 24px;">';

  // Header + share-sum badge + enable toggle
  html += '<div style="display:flex; align-items:center; gap:14px; margin-bottom:6px; flex-wrap:wrap;">' +
    '<h2 style="font-size:18px; font-weight:700; margin:0;">🏘 CAM Allocation <span style="font-weight:500; color:var(--gray-500); font-size:13px;">(Schedule A-1)</span></h2>';
  const shareSum = data.share_sum || 0;
  const sharesOk = !!data.shares_ok;
  html += '<span style="background:' + (sharesOk ? '#dcfce7' : '#fef2f2') + '; color:' + (sharesOk ? '#166534' : '#991b1b') + '; padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600;">Shares: ' + pct(shareSum) + (sharesOk ? ' ✓' : ' — must total 100%') + '</span>';
  html += '<label style="margin-left:auto; font-size:12px; display:flex; align-items:center; gap:6px; cursor:pointer;">' +
    '<input type="checkbox" ' + (data.cam_enabled ? 'checked' : '') + ' onchange="camEnable(this.checked)"> ' +
    '<span style="font-weight:600; color:var(--gray-700);">CAM drives this budget</span></label></div>';
  html += '<p style="font-size:11px; color:var(--gray-500); margin:0 0 14px;">Split each operating-expense GL across unit classes by common-interest share. The code seeds each row (B = building-wide, R = residential 100%, S = shared, or 100% to one class); type a dollar amount in any cell to override. Rows always reconcile to the line total.</p>';

  // Classes editor
  html += '<div style="background:#eef2ff; border:1px solid #c7d2fe; border-radius:10px; padding:12px 14px; margin-bottom:16px;">';
  html += '<div style="font-size:11px; font-weight:700; text-transform:uppercase; color:#3730a3; margin-bottom:8px;">Unit Classes &amp; Common-Interest Shares</div>';
  if (classes.length === 0) {
    html += '<p style="font-size:12px; color:var(--gray-600); margin:0 0 8px;">No classes yet. Import them from the building\'s Schedule A-1 (SharePoint), or add manually below.</p>';
    html += '<button onclick="camImportFromExcel()" id="camImportBtn" style="font-size:12px; font-weight:600; padding:5px 12px; background:#4f46e5; color:#fff; border:none; border-radius:6px; cursor:pointer; margin-bottom:8px;">⇩ Import from SharePoint (Schedule A-1)</button>';
  } else {
    html += '<div style="display:flex; flex-wrap:wrap; gap:8px; margin-bottom:8px;">';
    classes.forEach(c => {
      const shareDisp = Math.round((c.share_pct || 0) * 1000000) / 10000;
      const safeName = (c.name || '').replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/"/g,'&quot;');
      html += '<div style="display:flex; align-items:center; gap:4px; background:#fff; border:1px solid #c7d2fe; border-radius:8px; padding:4px 6px;">' +
        '<input type="text" value="' + (c.name || '').replace(/"/g,'&quot;') + '" onblur="camUpdateClass(' + c.id + ',\'name\',this.value)" onkeydown="if(event.key===\'Enter\')this.blur()" style="width:120px; font-size:12px; font-weight:600; border:1px solid transparent; background:transparent; padding:2px 4px; border-radius:3px;">' +
        '<input type="number" step="0.0001" value="' + shareDisp + '" onblur="camUpdateClass(' + c.id + ',\'share_pct\',this.value)" onkeydown="if(event.key===\'Enter\')this.blur()" style="width:78px; font-size:12px; text-align:right; border:1px solid var(--gray-200); background:#fff; padding:2px 4px; border-radius:3px; font-variant-numeric:tabular-nums;">' +
        '<span style="font-size:11px; color:var(--gray-500);">%</span>' +
        '<button onclick="camEditNotes(' + c.id + ',\'' + safeName + '\')" title="' + (c.notes ? 'Edit note: ' + (c.notes || '').replace(/"/g,'&quot;') : 'Add a note (e.g. cite the offering plan)') + '" style="border:none; background:transparent; color:' + (c.notes ? '#4f46e5' : 'var(--gray-400)') + '; cursor:pointer; font-size:13px; line-height:1; padding:0 2px;">📝</button>' +
        '<button onclick="camEditSummaryLink(' + c.id + ',\'' + safeName + '\')" title="' + (c.summary_row_label ? 'Linked to Summary row: ' + (c.summary_row_label || '').replace(/"/g,'&quot;') : 'Link to a specific Summary row if this class isn\'t auto-matching one (needed for the required-increase calc)') + '" style="border:none; background:transparent; color:' + (c.summary_row_label ? '#4f46e5' : 'var(--gray-400)') + '; cursor:pointer; font-size:13px; line-height:1; padding:0 2px;">🔗</button>' +
        '<button onclick="camDeleteClass(' + c.id + ',\'' + safeName + '\')" title="Delete class" style="border:none; background:transparent; color:var(--red); cursor:pointer; font-size:15px; line-height:1; padding:0 2px;">×</button>' +
      '</div>';
    });
    html += '</div>';
  }
  html += '<div style="display:flex; flex-wrap:wrap; gap:6px; align-items:center;">';
  html += '<span style="font-size:11px; color:var(--gray-500);">Quick add:</span>';
  ['Residential','Retail','Garage','Inclusionary','Storage','Office','Commercial'].forEach(p => {
    html += '<button onclick="camAddClass(\'' + p + '\')" style="font-size:11px; padding:2px 8px; background:#fff; border:1px solid #c7d2fe; border-radius:10px; cursor:pointer; color:#3730a3;">+ ' + p + '</button>';
  });
  html += '<button onclick="camAddClass(null)" style="font-size:11px; padding:2px 8px; background:#4f46e5; color:#fff; border:none; border-radius:10px; cursor:pointer;">+ Custom…</button>';
  html += '<span style="font-size:10.5px; color:var(--gray-400); margin-left:4px;">Need more than one, e.g. two retail units? Add the same preset twice and rename each (\'Commercial 1\', \'Commercial 2\').</span>';
  html += '</div></div>';

  // Empty-state guards
  if (classes.length === 0 || lines.length === 0) {
    html += '<div style="padding:32px; text-align:center; color:var(--gray-500); font-size:13px; border:1px dashed var(--gray-200); border-radius:8px;">' +
      (classes.length === 0 ? 'Add at least one unit class to build the allocation matrix.' : 'No operating-expense lines to allocate yet — build the budget first.') +
      '</div></div>';
    contentDiv.innerHTML = html;
    return;
  }

  function codeOptions(sel) {
    const s = (sel || '').toLowerCase();
    const isSubset = s.startsWith('subset:');
    let o = '<option value=""' + (!sel ? ' selected' : '') + '>B – building-wide</option>';
    o += '<option value="R"' + (s === 'r' ? ' selected' : '') + '>R – residential 100%</option>';
    o += '<option value="S"' + (s === 's' ? ' selected' : '') + '>S – shared</option>';
    classes.forEach(c => {
      o += '<option value="' + (c.name || '').replace(/"/g,'&quot;') + '"' + (s === (c.name || '').toLowerCase() ? ' selected' : '') + '>100% ' + (c.name || '').replace(/</g,'&lt;') + '</option>';
    });
    if (isSubset) {
      // Show the CURRENT subset as its own selected option (e.g. "Subset:
      // Residential, Garage") so re-opening the dropdown doesn't look like
      // it silently reverted to B.
      const names = (sel.split(':')[1] || '').split('|').join(', ');
      o += '<option value="' + sel.replace(/"/g,'&quot;') + '" selected>Subset: ' + names.replace(/</g,'&lt;') + '</option>';
    }
    o += '<option value="__subset__">Subset (2+ classes)…</option>';
    return o;
  }

  const bySheet = {};
  lines.forEach(l => { (bySheet[l.sheet_name] = bySheet[l.sheet_name] || []).push(l); });
  const sheetSeq = ['Payroll','Energy','Water & Sewer','Repairs & Supplies','Gen & Admin'];

  html += '<div style="overflow-x:auto;">';
  html += '<table style="width:100%; border-collapse:collapse; font-size:12px; min-width:' + (540 + classes.length * 120) + 'px;">';
  html += '<thead><tr style="border-bottom:2px solid var(--gray-300);">' +
    '<th style="text-align:left; padding:6px 8px; font-size:10px; text-transform:uppercase; color:var(--gray-500);">Expense Line</th>' +
    '<th style="text-align:center; padding:6px 8px; font-size:10px; text-transform:uppercase; color:var(--gray-500);">Code</th>';
  classes.forEach(c => {
    html += '<th style="text-align:right; padding:6px 8px; font-size:10px; text-transform:uppercase; color:#3730a3;">' + (c.name || '').replace(/</g,'&lt;') + '<br><span style="font-weight:400; color:var(--gray-400);">' + pct(c.share_pct) + '</span></th>';
  });
  html += '<th style="text-align:right; padding:6px 8px; font-size:10px; text-transform:uppercase; color:var(--gray-500);">Line Total</th>';
  html += '</tr></thead><tbody>';

  sheetSeq.forEach(sn => {
    const rows = bySheet[sn];
    if (!rows || !rows.length) return;
    html += '<tr style="background:var(--gray-50);"><td colspan="' + (3 + classes.length) + '" style="padding:4px 8px; font-size:10px; font-weight:700; text-transform:uppercase; color:var(--gray-600);">' + sn + '</td></tr>';
    rows.forEach(l => {
      const cells = l.cells || {};
      html += '<tr style="border-bottom:1px solid var(--gray-100);" data-gl="' + l.gl_code + '">' +
        '<td style="padding:3px 8px;"><span style="font-family:monospace; color:var(--gray-400); font-size:10px;">' + l.gl_code + '</span> ' + (l.description || '').replace(/</g,'&lt;') + '</td>' +
        '<td style="padding:3px 8px; text-align:center;"><select onchange="camSetLineCode(\'' + l.gl_code + '\',this.value)" style="font-size:11px; padding:1px 2px; border:1px solid var(--gray-200); border-radius:3px; max-width:160px;">' + codeOptions(l.cam_code) + '</select></td>';
      classes.forEach(c => {
        const amt = cells[c.id] || 0;
        const share = l.total ? (amt / l.total) : 0;
        const sharePctDisp = Math.round(share * 100000) / 1000;
        html += '<td style="padding:3px 4px; text-align:right;">' +
          '<input type="number" step="0.01" value="' + (Math.round(amt * 100) / 100) + '" ' +
          'onblur="camSetCell(\'' + l.gl_code + '\',' + c.id + ',this.value)" onkeydown="if(event.key===\'Enter\')this.blur()" ' +
          'title="Dollar amount allocated to this class" ' +
          'style="width:86px; padding:2px 4px; text-align:right; border:1px solid transparent; background:transparent; font-variant-numeric:tabular-nums; font-size:11px; border-radius:3px;">' +
          '<div style="display:flex; align-items:center; justify-content:flex-end; gap:1px;">' +
          '<input type="number" step="0.001" value="' + sharePctDisp + '" ' +
          'onblur="camSetCellPct(\'' + l.gl_code + '\',' + c.id + ',this.value,' + (l.total || 0) + ')" onkeydown="if(event.key===\'Enter\')this.blur()" ' +
          'title="% of this line allocated to this class -- edit here instead of the $ amount if that\'s easier" ' +
          'style="width:60px; padding:1px 3px; text-align:right; border:1px solid transparent; background:transparent; font-variant-numeric:tabular-nums; font-size:9px; color:var(--gray-500); border-radius:3px;">' +
          '<span style="font-size:9px; color:var(--gray-400);">%</span></div></td>';
      });
      html += '<td style="padding:3px 8px; text-align:right; font-variant-numeric:tabular-nums; color:var(--gray-600);">' + fmt0(l.total) + '</td></tr>';
    });
  });

  const ct = data.column_totals || {};
  html += '<tr style="border-top:2px solid var(--gray-300); font-weight:700; background:#eef2ff;">' +
    '<td style="padding:6px 8px;">Allocated expense</td><td></td>';
  classes.forEach(c => {
    html += '<td style="padding:6px 8px; text-align:right; font-variant-numeric:tabular-nums; color:#3730a3;">' + fmt0(ct[c.id]) + '</td>';
  });
  html += '<td style="padding:6px 8px; text-align:right; font-variant-numeric:tabular-nums;">' + fmt0(data.grand_total) + '</td></tr>';
  html += '</tbody></table></div>';

  html += '<div style="margin-top:10px; font-size:12px; color:' + (data.reconciles ? 'var(--green)' : 'var(--red)') + ';">' +
    (data.reconciles
      ? '✓ Reconciled — class columns sum to total operating expense (' + fmt0(data.grand_total) + ').'
      : '⚠ Columns don\'t reconcile to the line totals — check your overrides.') + '</div>';
  // Summary feed indicator — each class's allocated expense → its common-charge
  // income row on the Summary (only when CAM drives this budget).
  const sync = data.summary_sync || {};
  if (sync.enabled) {
    html += '<div style="margin-top:14px; border-top:1px solid var(--gray-200); padding-top:10px;">';
    html += '<div style="font-size:11px; font-weight:700; text-transform:uppercase; color:#3730a3; margin-bottom:6px;">Summary feed — per-class common charges</div>';
    (sync.classes || []).forEach(s => {
      if (s.matched) {
        html += '<div style="background:#f0fdf4; border:1px solid #bbf7d0; border-radius:6px; padding:6px 10px; margin-bottom:6px; font-size:12px;">' +
          '<span style="color:var(--green); font-weight:700;">✓</span> <strong>' + (s.class_name || '').replace(/</g,'&lt;') + '</strong> → Summary row "' + (s.label || '').replace(/</g,'&lt;') + '" 2027 = <strong>' + fmt0(s.new_col7) + '</strong></div>';
      } else {
        html += '<div style="background:#fffbeb; border:1px solid #fde68a; border-radius:6px; padding:6px 10px; margin-bottom:6px; font-size:12px; color:#92400e;">' +
          '⚠ <strong>' + (s.class_name || '').replace(/</g,'&lt;') + '</strong> (' + fmt0(s.allocated_expense) + ') has no matching Summary common-charge row. Add a "Common Charges – ' + (s.class_name || '').replace(/</g,'&lt;') + '" row on the Summary tab (or name a class to match an existing row).</div>';
      }
    });
    html += '</div>';
  } else {
    html += '<p style="font-size:11px; color:var(--gray-400); margin-top:6px;">Turn on <strong>“CAM drives this budget”</strong> (top right) to feed each class\'s common charges into the Summary.</p>';
  }

  // Required increase — the FA's actual worksheet math: back out other
  // income, split what's left by share, compare to current common charges.
  html += '<div style="margin-top:18px; border-top:2px solid var(--gray-200); padding-top:14px;">';
  html += '<div style="font-size:13px; font-weight:700; color:var(--gray-800); margin-bottom:8px;">Required increase</div>';
  if (!reqIncrease) {
    html += '<p style="font-size:11px; color:var(--gray-400);">Could not compute the required increase for this building.</p>';
  } else {
    if (reqIncrease.warning) {
      html += '<div style="background:#fffbeb; border:1px solid #fde68a; border-radius:6px; padding:10px 12px; margin-bottom:10px; font-size:12px; color:#92400e;">⚠ ' +
        reqIncrease.warning.replace(/</g,'&lt;') + '</div>';
    }
    const hasTotals = reqIncrease.other_income !== null && reqIncrease.other_income !== undefined;
    if (hasTotals) {
      html += '<div style="display:flex; flex-wrap:wrap; gap:16px; margin-bottom:10px; font-size:12px;">' +
        '<div>Total allocated expense: <strong>' + fmt0(reqIncrease.grand_total_expense) + '</strong></div>' +
        '<div>Less: other income: <strong style="color:var(--green);">&minus;' + fmt0(reqIncrease.other_income) + '</strong></div>' +
        '<div>Amount to be covered by common charges: <strong>' + fmt0(reqIncrease.amount_to_be_covered) + '</strong></div>' +
        '</div>';
    }
    html += '<div style="overflow-x:auto;"><table style="width:100%; border-collapse:collapse; font-size:12px;">';
    html += '<thead><tr style="border-bottom:2px solid var(--gray-300);">' +
      '<th style="text-align:left; padding:6px 8px; font-size:10px; text-transform:uppercase; color:var(--gray-500);">Class</th>' +
      '<th style="text-align:right; padding:6px 8px; font-size:10px; text-transform:uppercase; color:var(--gray-500);">Current Common Charges</th>' +
      '<th style="text-align:right; padding:6px 8px; font-size:10px; text-transform:uppercase; color:var(--gray-500);">Required Common Charges</th>' +
      '<th style="text-align:right; padding:6px 8px; font-size:10px; text-transform:uppercase; color:var(--gray-500);">Increase</th></tr></thead><tbody>';
    (reqIncrease.classes || []).forEach(rc => {
      const warn = rc.matched_row_label ? '' :
        ' <span title="No matching Summary common-charge row found -- current common charges shown as $0 until one is matched." style="color:#d97706; cursor:help;">⚠</span>';
      const noData = rc.increase_dollar === null || rc.increase_dollar === undefined;
      const upOrDown = noData ? '' : (rc.increase_dollar >= 0 ? '+' : '');
      const increaseCell = noData ? '—' :
        upOrDown + fmt0(rc.increase_dollar) + ' (' + upOrDown + rc.increase_pct.toFixed(2) + '%)';
      html += '<tr style="border-bottom:1px solid var(--gray-100);">' +
        '<td style="padding:5px 8px;">' + (rc.class_name || '').replace(/</g,'&lt;') + warn + '</td>' +
        '<td style="padding:5px 8px; text-align:right; font-variant-numeric:tabular-nums;">' + fmt0(rc.current_common_charges) + '</td>' +
        '<td style="padding:5px 8px; text-align:right; font-variant-numeric:tabular-nums;">' + (noData ? '—' : fmt0(rc.required_common_charges)) + '</td>' +
        '<td style="padding:5px 8px; text-align:right; font-variant-numeric:tabular-nums; color:' + (noData ? 'var(--gray-400)' : (rc.increase_dollar >= 0 ? 'var(--red)' : 'var(--green)')) + ';">' +
        increaseCell + '</td></tr>';
    });
    html += '</tbody></table></div>';
    html += '<p style="font-size:10.5px; color:var(--gray-400); margin-top:6px;">"Current" comes from the Summary row matched to each class (via its name or Summary Row Label, below). "Other income" is every Income-sheet line except those matched common-charge rows.</p>';
  }
  html += '</div>';

  html += '</div>';
  contentDiv.innerHTML = html;
}

async function camRefresh() {
  const cd = document.getElementById('sheetContent');
  if (cd) await renderCamTab(cd);
}
async function _camFetch(path, body, method) {
  return fetch('/api/cam/' + entityCode + path, {
    method: method || 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: body ? JSON.stringify(body) : null,
  });
}
async function camEnable(on) {
  await _camFetch('/enable', {enabled: !!on});
  showToast(on ? 'CAM now drives this budget' : 'CAM disabled', 'info');
  await camRefresh();
}
async function camAddClass(preset) {
  let name = preset;
  if (!name) { name = prompt('Class name (e.g. Residential, Retail, Garage):'); if (!name) return; }
  await _camFetch('/class', {name: name, share_pct: 0}, 'POST');
  await camRefresh();
}
async function camUpdateClass(id, field, value) {
  const body = {};
  if (field === 'share_pct') {
    // The field always shows/accepts a PERCENT NUMBER (e.g. "76.5953" for
    // 76.5953%, "0.1304" for 0.1304%) -- always divide by 100, no guessing.
    // A "v > 1 ? /100 : v" heuristic here silently corrupted any legitimate
    // sub-1% entry (0.1304% got stored as 13.04%) -- see 343 incident.
    let v = parseFloat(value); if (isNaN(v)) v = 0;
    body.share_pct = v / 100;
    // Immediate client-side check (server enforces this too) -- catch the
    // over-100% case before a round trip instead of silently reverting.
    const others = ((window._camData || {}).classes || []).filter(c => c.id !== id);
    const prospective = others.reduce((s, c) => s + (c.share_pct || 0), 0) + body.share_pct;
    if (prospective > 1.0001) {
      alert('Classes would total ' + (Math.round(prospective * 10000) / 100).toFixed(2) +
            '% -- cannot exceed 100%.');
      await camRefresh();
      return;
    }
  } else { body[field] = value; }
  const resp = await _camFetch('/class/' + id, body);
  if (!resp.ok) {
    const d = await resp.json().catch(() => ({}));
    alert(d.error || ('Save failed (' + resp.status + ')'));
  }
  await camRefresh();
}
async function camEditNotes(id, name) {
  const c = ((window._camData || {}).classes || []).find(x => x.id === id);
  const current = (c && c.notes) || '';
  const next = prompt('Note for "' + (name || '') + '" (e.g. cite the offering plan section this share came from):', current);
  if (next === null) return;  // cancelled
  await camUpdateClass(id, 'notes', next);
}
async function camEditSummaryLink(id, name) {
  const c = ((window._camData || {}).classes || []).find(x => x.id === id);
  const current = (c && c.summary_row_label) || '';
  const next = prompt('Summary row label for "' + (name || '') + '" (type the EXACT label from the Summary tab, ' +
                      'e.g. "Common Charges - Residential") -- only needed if the Required Increase table below ' +
                      'shows a ⚠ warning for this class:', current);
  if (next === null) return;  // cancelled
  await camUpdateClass(id, 'summary_row_label', next);
}
async function camDeleteClass(id, name) {
  if (!confirm('Delete class "' + (name || '') + '"? Its override cells are removed too.')) return;
  await _camFetch('/class/' + id, null, 'DELETE');
  await camRefresh();
}
async function camSetLineCode(gl, code) {
  if (code === '__subset__') {
    const known = ((window._camData || {}).classes || []).map(c => c.name);
    const picked = prompt('Which classes share this line? Type the names separated by commas (from: ' +
                          known.join(', ') + '):');
    if (!picked) { await camRefresh(); return; }  // cancelled -- revert the dropdown
    const parts = picked.split(',').map(s => s.trim()).filter(Boolean);
    const knownLower = known.map(n => n.toLowerCase());
    const bad = parts.filter(p => !knownLower.includes(p.toLowerCase()));
    if (bad.length) {
      alert('Not a recognized class name: ' + bad.join(', ') + '. Use the exact names shown: ' + known.join(', '));
      await camRefresh();
      return;
    }
    if (parts.length < 2) {
      alert('Pick at least 2 classes for a subset split -- for a single class, choose it directly from the dropdown instead.');
      await camRefresh();
      return;
    }
    code = 'SUBSET:' + parts.join('|');
  }
  await _camFetch('/line-code', {gl_code: gl, cam_code: code || null});
  await camRefresh();
}
async function camSetCell(gl, classId, value) {
  let amt = (value === '' || value === null || value === undefined) ? null : parseFloat(value);
  if (amt !== null && isNaN(amt)) amt = null;
  await _camFetch('/cell', {gl_code: gl, cam_class_id: classId, amount: amt});
  await camRefresh();
}
async function camSetCellPct(gl, classId, value, lineTotal) {
  // Lets an FA think in % instead of $ for a per-line override (e.g. "17% of
  // elevator repairs to Residential" instead of computing the dollar figure
  // by hand) -- converts to the same $ override the backend already expects.
  if (value === '' || value === null || value === undefined) {
    await camSetCell(gl, classId, '');
    return;
  }
  const pctVal = parseFloat(value);
  if (isNaN(pctVal)) { await camRefresh(); return; }
  const amt = Math.round((pctVal / 100) * (lineTotal || 0) * 100) / 100;
  await camSetCell(gl, classId, String(amt));
}
async function camImportFromExcel(force) {
  const btn = document.getElementById('camImportBtn');
  if (btn) { btn.textContent = 'Importing…'; btn.disabled = true; }
  try {
    const resp = await _camFetch('/import' + (force ? '?force=1' : ''), null, 'POST');
    const d = await resp.json();
    if (d.status === 'imported') {
      showToast('Imported ' + (d.imported || 0) + ' unit classes from "' + (d.file_name || 'Excel') + '"', 'success');
      await camRefresh();
    } else if (d.status === 'exists') {
      showToast('Classes already exist — use Re-import to overwrite from Excel', 'info');
    } else if (d.status === 'no_file') {
      alert('No approved budget Excel found in SharePoint for this building.');
    } else if (d.status === 'not_found') {
      alert('Could not find a CAM Allocation / Schedule A-1 sheet in the Excel. Add classes manually instead.');
    } else {
      alert('Import failed: ' + (d.error || d.status));
    }
  } catch (e) {
    alert('Import error: ' + (e.message || e));
  } finally {
    if (btn) { btn.textContent = '⇩ Import from SharePoint (Schedule A-1)'; btn.disabled = false; }
  }
}
async function camReimport() {
  if (!confirm('Re-import overwrites all current classes + shares for this building with what is in the Excel. Per-cell overrides and per-line codes are kept (they key on GL + class id). Continue?')) return;
  await camImportFromExcel(true);
}

async function renderCommercialTab(contentDiv) {
  contentDiv.innerHTML =
    '<div style="padding:40px 24px; text-align:center; color:var(--gray-500);">' +
    '<div style="display:inline-block; width:32px; height:32px; border:3px solid var(--gray-200); border-top-color:var(--blue); border-radius:50%; animation:spin 0.6s linear infinite;"></div>' +
    '<p style="margin-top:12px; font-size:13px;">Loading commercial rent data&hellip; (first load imports from Excel)</p>' +
    '</div>';
  let data;
  try {
    const resp = await fetch('/api/commercial/' + entityCode);
    data = await resp.json();
  } catch (err) {
    contentDiv.innerHTML = '<div style="padding:24px; color:var(--red);">Failed to load: ' + (err.message || err) + '</div>';
    return;
  }

  const tenants = data.tenants || [];
  const impStatus = (data.import_result && data.import_result.status) || 'unknown';

  // Empty state
  if (tenants.length === 0) {
    let msg = 'No commercial rent set up for this building yet.';
    let sub = 'Add the building\'s commercial units manually below.';
    if (impStatus === 'no_file') {
      msg = 'No approved 2026 budget Excel found in SharePoint.';
      sub = 'Upload one and re-import, or check the file name pattern.';
    } else if (impStatus === 'error') {
      msg = 'Could not parse the approved Excel.';
      sub = (data.import_result.error || '').slice(0, 200);
    }
    contentDiv.innerHTML =
      '<div style="padding:48px 24px; text-align:center; max-width:520px; margin:24px auto; background:#fff7ed; border:1px solid #fed7aa; border-radius:12px;">' +
        '<div style="font-size:36px; margin-bottom:8px;">🏢</div>' +
        '<h3 style="margin:0 0 4px; font-size:16px; color:#9a3412;">' + msg + '</h3>' +
        '<p style="margin:4px 0 12px; font-size:12px; color:var(--gray-500);">' + sub + '</p>' +
        '<button onclick="commercialAddTenant()" style="font-size:13px; font-weight:600; padding:8px 16px; background:#9a3412; color:#fff; border:none; border-radius:6px; cursor:pointer;">+ Add Commercial Unit</button>' +
      '</div>';
    return;
  }

  // Compute escalation-model label
  const escLabels = {
    re_tax: '🏛 RE Tax Escalation',
    utility_billback: '⚡ Utility / Insurance Billback',
    opex: '💰 Operating Expense Escalation',
    none: '— No escalation —',
  };

  // Render tenant cards
  let html = '<div style="padding:18px 24px;">';

  // Header strip — show prior, current, projected totals
  const BUDGET_Y = {{ budget_year }};           // e.g. 2027
  const PRIOR_Y = BUDGET_Y - 1;                 // 2026 (approved budget year)
  function tenantAnnual(t, year) {
    return (t.rent_periods || []).filter(p => p.year === year)
      .reduce((s, p) => s + (p.annualized || 0), 0);
  }
  const totalPrior = tenants.reduce((s, t) => s + tenantAnnual(t, PRIOR_Y), 0);
  const totalBudget = tenants.reduce((s, t) => s + tenantAnnual(t, BUDGET_Y), 0);
  const tenantsWithoutProjection = tenants.filter(t => tenantAnnual(t, BUDGET_Y) === 0).length;
  const escModel = tenants[0] ? tenants[0].escalation_model : 'none';
  html += '<div style="display:flex; align-items:center; gap:14px; margin-bottom:14px; flex-wrap:wrap;">' +
    '<h2 style="font-size:18px; font-weight:700; margin:0;">Commercial Tenants</h2>' +
    '<span style="background:var(--blue-light); color:var(--blue); padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600;">' + tenants.length + ' active</span>' +
    '<button onclick="commercialAddTenant()" style="font-size:11px; font-weight:600; padding:3px 10px; background:#9a3412; color:#fff; border:none; border-radius:12px; cursor:pointer;">+ Add Unit</button>' +
    '<span style="font-size:11px; color:var(--gray-500);">Escalation:</span>' +
    '<span style="font-size:11px; font-weight:600; color:var(--gray-700);">' + (escLabels[escModel] || escModel) + '</span>' +
    '<div style="margin-left:auto; display:flex; align-items:center; gap:14px; font-size:11px;">' +
      '<div><span style="color:var(--gray-500);">' + PRIOR_Y + ' total:</span> <strong style="font-size:13px; color:var(--gray-700);">$' + Math.round(totalPrior).toLocaleString() + '</strong></div>' +
      '<div><span style="color:var(--gray-500);">' + BUDGET_Y + ' projected:</span> <strong style="font-size:13px; color:' + (totalBudget > 0 ? 'var(--green)' : 'var(--gray-400)') + ';">$' + Math.round(totalBudget).toLocaleString() + '</strong></div>' +
    '</div>' +
    '</div>';

  // Summary sync indicator — two Summary rows are auto-fed:
  //   row 4040 Commercial Rent (from rent periods)
  //   row 4520 Commercial Escalations (from per-tenant escalation calc)
  const sync = data.summary_sync || {};
  const rentSync = sync.rent;
  const escSync = sync.escalations;
  if (rentSync && rentSync.row_id) {
    const newCol7 = rentSync.new_col7 || 0;
    html += '<div style="background:#f0fdf4; border:1px solid #bbf7d0; border-radius:6px; padding:8px 12px; margin-bottom:8px; font-size:12px; display:flex; align-items:center; gap:10px;">' +
      '<span style="color:var(--green); font-weight:700; font-size:14px;">✓</span>' +
      '<span><strong>Summary row "' + (rentSync.label || '').replace(/</g,'&lt;') + '" (GL 4040)</strong> ' + BUDGET_Y + ' col7 = <strong>$' + Math.round(newCol7).toLocaleString() + '</strong> &mdash; auto-synced from rent periods.</span>' +
      '</div>';
  } else if (totalBudget > 0) {
    html += '<div style="background:#fffbeb; border:1px solid #fde68a; border-radius:6px; padding:8px 12px; margin-bottom:8px; font-size:12px; color:#92400e;">⚠ No "Commercial Rent" (4040) row on Summary tab. ' + BUDGET_Y + ' rent total won\'t flow until one is added.</div>';
  }
  if (escSync && escSync.row_id) {
    const newCol7 = escSync.new_col7 || 0;
    const newCol5 = escSync.new_col5 || 0;
    html += '<div style="background:#f0fdf4; border:1px solid #bbf7d0; border-radius:6px; padding:8px 12px; margin-bottom:8px; font-size:12px; display:flex; align-items:center; gap:10px;">' +
      '<span style="color:var(--green); font-weight:700; font-size:14px;">✓</span>' +
      '<span><strong>Summary row "' + (escSync.label || '').replace(/</g,'&lt;') + '" (GL 4520)</strong> ' + BUDGET_Y + ' col7 = <strong>$' + Math.round(newCol7).toLocaleString() + '</strong> &mdash; auto-synced from per-tenant escalation math.</span>' +
      '</div>';
    // FA dir 2026-06-03 (#1): 2026 escalation off the 2026 Budget RE-tax basis -> col5 (Forecast).
    html += '<div style="background:#f0fdf4; border:1px solid #bbf7d0; border-radius:6px; padding:8px 12px; margin-bottom:14px; font-size:12px; display:flex; align-items:center; gap:10px;">' +
      '<span style="color:var(--green); font-weight:700; font-size:14px;">✓</span>' +
      '<span><strong>Same row, ' + PRIOR_Y + ' (col5 / Forecast)</strong> = <strong>$' + Math.round(newCol5).toLocaleString() + '</strong> &mdash; ' + PRIOR_Y + ' escalation computed off the ' + PRIOR_Y + ' Budget RE-tax basis (same methodology); the imported ' + PRIOR_Y + ' budget (col6) is left untouched.</span>' +
      '</div>';
  } else if ((escSync === null || escSync === undefined) && tenants.some(t => t.escalation_model && t.escalation_model !== 'none')) {
    html += '<div style="background:#fffbeb; border:1px solid #fde68a; border-radius:6px; padding:8px 12px; margin-bottom:14px; font-size:12px; color:#92400e;">⚠ No Commercial Escalations (4520) row on Summary tab. Escalation totals won\'t flow until one is added.</div>';
  }

  // Top-of-tab projection toolbar removed (FA directive 2026-05-15) — the
  // per-tenant "Project from prior year" button on each card is enough.
  // Variable retained because the global commercialProjectAll() handler
  // still references it for future "Project all" workflows.
  void tenantsWithoutProjection;

  // Source attribution
  if (data.import_result && data.import_result.file_name) {
    html += '<div style="font-size:11px; color:var(--gray-500); margin-bottom:14px;">' +
      '📄 Imported from: <code style="background:rgba(0,0,0,0.05); padding:1px 6px; border-radius:3px; font-size:11px;">' + data.import_result.file_name.replace(/</g,'&lt;') + '</code></div>';
  }

  // Tenant grid
  html += '<div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(320px, 1fr)); gap:14px;">';

  tenants.forEach(t => {
    const periods = t.rent_periods || [];
    const years = [...new Set(periods.map(p => p.year))].sort();
    const annualByYear = {};
    years.forEach(y => {
      annualByYear[y] = periods.filter(p => p.year === y).reduce((s, p) => s + (p.annualized || 0), 0);
    });
    const latestYear = years.length ? years[years.length - 1] : null;
    const latestAnnual = latestYear ? annualByYear[latestYear] : 0;
    const escLabel = escLabels[t.escalation_model] || t.escalation_model;

    // Lease expiry warning
    let leaseWarning = '';
    if (t.lease_end) {
      try {
        const d = new Date(t.lease_end);
        const monthsLeft = (d.getFullYear() * 12 + d.getMonth()) - (new Date().getFullYear() * 12 + new Date().getMonth());
        if (monthsLeft < 12 && monthsLeft >= 0) {
          leaseWarning = '<div style="background:var(--red-light); color:var(--red); padding:3px 8px; border-radius:4px; font-size:10px; font-weight:600; margin-bottom:8px;">⚠ Lease ends ' + t.lease_end + '</div>';
        }
      } catch (e) {}
    }

    html += '<div style="background:white; border:1px solid var(--gray-200); border-radius:10px; padding:14px;">';
    html += '<div style="display:flex; align-items:flex-start; gap:10px; margin-bottom:8px;">';
    html += '<div style="flex:1; min-width:0;">';
    // Editable tenant name (FA can rename) — inline input, click to edit, blur to save.
    html += '<input type="text" value="' + (t.tenant_name || 'Unnamed').replace(/"/g,'&quot;') + '" ' +
      'onblur="commercialUpdateTenantField(' + t.id + ',\'tenant_name\',this.value)" ' +
      'onkeydown="if(event.key===\'Enter\')this.blur()" ' +
      'class="comm-edit-input" ' +
      'style="font-weight:700; font-size:14px; width:100%; padding:2px 4px; border:1px solid transparent; background:transparent; border-radius:3px;">';
    // Editable unit label
    html += '<input type="text" value="' + (t.unit_label || '').replace(/"/g,'&quot;') + '" ' +
      'placeholder="Unit (e.g. 1A, Ground floor, Garage)" ' +
      'onblur="commercialUpdateTenantField(' + t.id + ',\'unit_label\',this.value || null)" ' +
      'onkeydown="if(event.key===\'Enter\')this.blur()" ' +
      'class="comm-edit-input" ' +
      'style="font-size:11px; color:var(--gray-500); font-family:monospace; width:100%; padding:2px 4px; border:1px solid transparent; background:transparent; border-radius:3px; margin-top:2px;">';
    html += '</div>';
    if (t.imported_from_excel) {
      html += '<span style="background:var(--gray-100); color:var(--gray-600); font-size:9px; font-weight:600; padding:2px 6px; border-radius:3px;">FROM EXCEL</span>';
    }
    html += '</div>';

    // Editable lease dates (FA can set/change) — small row of two date inputs.
    html += '<div style="display:flex; gap:8px; margin-bottom:8px; font-size:11px;">';
    html += '<div style="flex:1;"><label style="display:block; color:var(--gray-500); font-size:10px; margin-bottom:1px;">Lease start</label>' +
      '<input type="date" value="' + (t.lease_start || '') + '" ' +
      'onblur="commercialUpdateTenantField(' + t.id + ',\'lease_start\',this.value || null)" ' +
      'class="comm-edit-input" ' +
      'style="width:100%; padding:3px 6px; border:1px solid var(--gray-200); background:white; border-radius:4px; font-size:11px;"></div>';
    html += '<div style="flex:1;"><label style="display:block; color:var(--gray-500); font-size:10px; margin-bottom:1px;">Lease end</label>' +
      '<input type="date" value="' + (t.lease_end || '') + '" ' +
      'onblur="commercialUpdateTenantField(' + t.id + ',\'lease_end\',this.value || null)" ' +
      'class="comm-edit-input" ' +
      'style="width:100%; padding:3px 6px; border:1px solid var(--gray-200); background:white; border-radius:4px; font-size:11px;"></div>';
    html += '</div>';

    html += leaseWarning;

    // Rent periods table — editable inputs (Phase 2). Click into a cell,
    // change value, blur or Enter saves via PUT. Add/Delete rows too.
    if (periods.length > 0) {
      html += '<table class="comm-period-table" style="width:100%; border-collapse:collapse; font-size:11px; margin-bottom:8px;">';
      html += '<thead><tr>' +
              '<th style="text-align:left; padding:4px 6px; color:var(--gray-500); font-weight:600; text-transform:uppercase; font-size:9px; border-bottom:1px solid var(--gray-200);">Year</th>' +
              '<th style="text-align:left; padding:4px 6px; color:var(--gray-500); font-weight:600; text-transform:uppercase; font-size:9px; border-bottom:1px solid var(--gray-200);">Period</th>' +
              '<th style="text-align:right; padding:4px 6px; color:var(--gray-500); font-weight:600; text-transform:uppercase; font-size:9px; border-bottom:1px solid var(--gray-200);">$/mo</th>' +
              '<th style="text-align:right; padding:4px 6px; color:var(--gray-500); font-weight:600; text-transform:uppercase; font-size:9px; border-bottom:1px solid var(--gray-200);">×</th>' +
              '<th style="text-align:right; padding:4px 6px; color:var(--gray-500); font-weight:600; text-transform:uppercase; font-size:9px; border-bottom:1px solid var(--gray-200);">Annual</th>' +
              '<th style="width:20px; border-bottom:1px solid var(--gray-200);"></th>' +
              '</tr></thead><tbody>';
      periods.forEach(p => {
        const isBudget = p.year === BUDGET_Y;
        const rowBg = isBudget ? 'background:#f0fdf4;' : '';
        const dataAttrs = 'data-tid="' + t.id + '" data-pid="' + p.id + '"';
        html += '<tr style="' + rowBg + '" ' + dataAttrs + '>' +
          '<td style="padding:2px 4px;">' +
            '<input type="number" min="2020" max="2040" step="1" value="' + p.year + '" ' +
            'onblur="commercialUpdatePeriod(' + t.id + ',' + p.id + ',\'year\',parseInt(this.value)||0, this)" ' +
            'onkeydown="if(event.key===\'Enter\')this.blur()" ' +
            'class="comm-period-year" ' +
            'style="width:54px; padding:2px 4px; border:1px solid transparent; background:transparent; font-size:11px; border-radius:3px;">' +
            (isBudget ? ' <span style="color:var(--green); font-size:9px; font-weight:700;">BUDGET</span>' : '') +
          '</td>' +
          '<td style="padding:2px 4px;">' +
            '<input type="text" value="' + (p.period_label || '').replace(/"/g,'&quot;') + '" ' +
            'onblur="commercialUpdatePeriod(' + t.id + ',' + p.id + ',\'period_label\',this.value, this)" ' +
            'onkeydown="if(event.key===\'Enter\')this.blur()" ' +
            'style="width:100%; padding:2px 4px; border:1px solid transparent; background:transparent; font-family:monospace; font-size:11px; border-radius:3px;">' +
          '</td>' +
          '<td style="padding:2px 4px; text-align:right;">' +
            '<input type="number" step="0.01" value="' + (p.monthly_rent || 0) + '" ' +
            'onblur="commercialUpdatePeriod(' + t.id + ',' + p.id + ',\'monthly_rent\',parseFloat(this.value)||0, this)" ' +
            'onkeydown="if(event.key===\'Enter\')this.blur()" ' +
            'style="width:80px; padding:2px 4px; border:1px solid transparent; background:transparent; text-align:right; font-variant-numeric:tabular-nums; font-size:11px; border-radius:3px;">' +
          '</td>' +
          '<td style="padding:2px 4px; text-align:right;">' +
            '<input type="number" min="1" max="12" step="1" value="' + (p.months_count || 12) + '" ' +
            'onblur="commercialUpdatePeriod(' + t.id + ',' + p.id + ',\'months_count\',parseInt(this.value)||12, this)" ' +
            'onkeydown="if(event.key===\'Enter\')this.blur()" ' +
            'style="width:36px; padding:2px 4px; border:1px solid transparent; background:transparent; text-align:right; font-size:11px; border-radius:3px;">' +
          '</td>' +
          '<td class="ann-cell" style="padding:4px 6px; text-align:right; font-weight:600; font-variant-numeric:tabular-nums;">$' + Math.round(p.annualized || 0).toLocaleString() + '</td>' +
          '<td style="text-align:center;">' +
            '<button onclick="commercialDeletePeriod(' + t.id + ',' + p.id + ',this)" ' +
            'class="comm-period-del" title="Delete period" ' +
            'style="background:transparent; border:none; color:var(--gray-400); cursor:pointer; font-size:14px; line-height:1; padding:0 4px; opacity:0; transition:opacity 0.15s;">×</button>' +
          '</td>' +
          '</tr>';
      });
      html += '</tbody></table>';
    }

    // Escalation panel — collapsed by default. Click to edit.
    const escConfigured = t.escalation_model && t.escalation_model !== 'none' && t.tenant_share_pct;
    const escAmount = (data.summary_sync && data.summary_sync.escalations && data.summary_sync.escalations.per_tenant
      ? (data.summary_sync.escalations.per_tenant.find(e => e.tenant_id === t.id) || {}).amount || 0
      : 0);
    // FA dir 2026-06-03 (#1): per-tenant 2026 escalation amount (off the 2026 Budget basis).
    const escAmount2026 = (data.summary_sync && data.summary_sync.escalations && data.summary_sync.escalations.per_tenant_2026
      ? (data.summary_sync.escalations.per_tenant_2026.find(e => e.tenant_id === t.id) || {}).amount || 0
      : 0);
    html += '<details style="margin:6px 0; border:1px solid var(--gray-200); border-radius:6px; background:#fafaf7;">';
    html += '<summary style="padding:6px 10px; cursor:pointer; font-size:11px; color:var(--gray-700); user-select:none; display:flex; align-items:center; gap:8px;">' +
      '<span>📈 Escalation config</span>' +
      (escConfigured
        ? '<span style="background:var(--green-light); color:var(--green); padding:1px 6px; border-radius:3px; font-weight:600;">' + escLabels[t.escalation_model] + '</span>' +
          '<span style="margin-left:auto; color:var(--gray-600); font-weight:600;">' + PRIOR_Y + ': $' + Math.round(escAmount2026).toLocaleString() + '</span>' +
          '<span style="color:var(--green); font-weight:700;">' + BUDGET_Y + ': $' + Math.round(escAmount).toLocaleString() + '</span>'
        : '<span style="color:var(--gray-400); font-style:italic;">Not configured</span>') +
      '</summary>';
    html += '<div style="padding:10px 12px;">';
    html += '<div style="display:grid; grid-template-columns:1fr 1fr; gap:10px; font-size:11px;">';

    // Escalation model select
    html += '<div><label style="display:block; color:var(--gray-500); margin-bottom:2px; font-weight:600;">Model</label>';
    html += '<select onchange="commercialUpdateTenantField(' + t.id + ',\'escalation_model\',this.value)" style="width:100%; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:11px;">';
    ['none', 're_tax', 'opex', 'utility_billback'].forEach(m => {
      const sel = (t.escalation_model === m) ? ' selected' : '';
      const lbl = {none: 'None', re_tax: 'RE Tax escalation', opex: 'Operating expense escalation', utility_billback: 'Utility / insurance billback (deferred)'}[m];
      html += '<option value="' + m + '"' + sel + '>' + lbl + '</option>';
    });
    html += '</select></div>';

    // Tenant share %
    const sharePct = t.tenant_share_pct ? (t.tenant_share_pct * 100).toFixed(3) : '';
    html += '<div><label style="display:block; color:var(--gray-500); margin-bottom:2px; font-weight:600;">Tenant share (%)</label>';
    html += '<input type="number" step="0.001" value="' + sharePct + '" placeholder="e.g. 1.040" ' +
      'onblur="commercialUpdateTenantField(' + t.id + ',\'tenant_share_pct\',this.value === \'\' ? null : parseFloat(this.value)/100)" ' +
      'style="width:100%; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:11px; text-align:right; font-variant-numeric:tabular-nums;"></div>';

    // Conditional base-year inputs
    if (t.escalation_model === 're_tax') {
      const baseRe = t.base_year_re_tax || '';
      html += '<div style="grid-column: 1 / -1;"><label style="display:block; color:var(--gray-500); margin-bottom:2px; font-weight:600;">Base year Real Estate Tax ($)</label>';
      html += '<input type="number" step="1" value="' + baseRe + '" placeholder="e.g. 1844323" ' +
        'onblur="commercialUpdateTenantField(' + t.id + ',\'base_year_re_tax\',this.value === \'\' ? null : parseFloat(this.value))" ' +
        'style="width:100%; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:11px; text-align:right; font-variant-numeric:tabular-nums;">';
      html += '<div style="font-size:10px; color:var(--gray-500); margin-top:2px;">From lease: the RE Tax amount in the year the lease was signed. Tenant pays a share of any increase since then.</div>';
      html += '</div>';
    } else if (t.escalation_model === 'opex') {
      const baseOp = t.base_year_opex || '';
      html += '<div style="grid-column: 1 / -1;"><label style="display:block; color:var(--gray-500); margin-bottom:2px; font-weight:600;">Base year operating expenses ($)</label>';
      html += '<input type="number" step="1" value="' + baseOp + '" placeholder="e.g. 852891" ' +
        'onblur="commercialUpdateTenantField(' + t.id + ',\'base_year_opex\',this.value === \'\' ? null : parseFloat(this.value))" ' +
        'style="width:100%; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:11px; text-align:right; font-variant-numeric:tabular-nums;">';
      html += '<div style="font-size:10px; color:var(--gray-500); margin-top:2px;">From lease: total operating expenses in the year the lease was signed. Tenant pays a share of any increase.</div>';
      html += '</div>';
    } else if (t.escalation_model === 'utility_billback') {
      html += '<div style="grid-column: 1 / -1; padding:8px; background:#fffbeb; border:1px solid #fde68a; border-radius:4px; font-size:10px; color:#92400e;">⏳ Utility / insurance billback (per-category base years) is deferred to Phase 3b.3. Existing Excel data is preserved; no auto-feed yet.</div>';
    }

    // Live math preview
    if (escConfigured) {
      const b = (data.summary_sync && data.summary_sync.escalations && data.summary_sync.escalations.per_tenant
        ? (data.summary_sync.escalations.per_tenant.find(e => e.tenant_id === t.id) || {}).breakdown || {}
        : {});
      // FA dir 2026-06-03 (#1): the 2026 breakdown (off the 2026 Budget basis).
      const b2026 = (data.summary_sync && data.summary_sync.escalations && data.summary_sync.escalations.per_tenant_2026
        ? (data.summary_sync.escalations.per_tenant_2026.find(e => e.tenant_id === t.id) || {}).breakdown || {}
        : {});
      if (t.escalation_model === 're_tax' && (b.current_re_tax || b2026.current_re_tax)) {
        html += '<div style="grid-column: 1 / -1; padding:8px; background:white; border:1px solid var(--gray-200); border-radius:4px; font-size:10px; font-family:monospace; line-height:1.7;">';
        if (b2026.current_re_tax) {
          html += '<div style="color:var(--gray-600);">' + PRIOR_Y + ' (2026 Budget basis): ($' + Math.round(b2026.current_re_tax).toLocaleString() + ' − $' + Math.round(b2026.base_year).toLocaleString() + ') × ' + ((b2026.share_pct || 0) * 100).toFixed(3) + '% = <strong style="font-family:inherit;">$' + Math.round(escAmount2026).toLocaleString() + '</strong></div>';
        }
        if (b.current_re_tax) {
          html += '<div style="color:var(--green);">' + BUDGET_Y + ' (proposed basis): ($' + Math.round(b.current_re_tax).toLocaleString() + ' − $' + Math.round(b.base_year).toLocaleString() + ') × ' + ((b.share_pct || 0) * 100).toFixed(3) + '% = <strong style="color:var(--green); font-family:inherit;">$' + Math.round(escAmount).toLocaleString() + '</strong></div>';
        }
        html += '</div>';
      } else if (t.escalation_model === 'opex' && (b.current_opex || b2026.current_opex)) {
        html += '<div style="grid-column: 1 / -1; padding:8px; background:white; border:1px solid var(--gray-200); border-radius:4px; font-size:10px; font-family:monospace; line-height:1.7;">';
        if (b2026.current_opex) {
          html += '<div style="color:var(--gray-600);">' + PRIOR_Y + ' (2026 Budget basis): ($' + Math.round(b2026.current_opex).toLocaleString() + ' − $' + Math.round(b2026.base_year).toLocaleString() + ') × ' + ((b2026.share_pct || 0) * 100).toFixed(3) + '% = <strong style="font-family:inherit;">$' + Math.round(escAmount2026).toLocaleString() + '</strong></div>';
        }
        if (b.current_opex) {
          html += '<div style="color:var(--green);">' + BUDGET_Y + ': ($' + Math.round(b.current_opex).toLocaleString() + ' − $' + Math.round(b.base_year).toLocaleString() + ') × ' + ((b.share_pct || 0) * 100).toFixed(3) + '% = <strong style="color:var(--green); font-family:inherit;">$' + Math.round(escAmount).toLocaleString() + '</strong></div>';
        }
        html += '</div>';
      }
    }

    html += '</div></div></details>';

    // Per-tenant actions: add period, project budget year, delete tenant.
    html += '<div style="display:flex; gap:6px; margin:6px 0 8px; flex-wrap:wrap;">';
    html += '<button onclick="commercialAddPeriodPrompt(' + t.id + ',' + BUDGET_Y + ')" style="font-size:11px; padding:4px 10px; background:white; color:var(--brown); border:1px dashed var(--gray-300); border-radius:4px; cursor:pointer; font-weight:600;">+ Add ' + BUDGET_Y + ' period</button>';
    if (tenantAnnual(t, BUDGET_Y) === 0 && tenantAnnual(t, PRIOR_Y) > 0) {
      html += '<button onclick="commercialProjectOne(' + t.id + ')" style="font-size:11px; padding:4px 10px; background:#9a3412; color:white; border:none; border-radius:4px; cursor:pointer; font-weight:600;">📅 Project ' + BUDGET_Y + ' from ' + PRIOR_Y + '</button>';
    }
    html += '<button onclick="commercialDeleteTenant(' + t.id + ',\'' + (t.tenant_name || '').replace(/\'/g,"\\'") + '\')" style="font-size:11px; padding:4px 10px; margin-left:auto; background:transparent; color:var(--red); border:1px solid transparent; border-radius:4px; cursor:pointer;">🗑 Delete tenant</button>';
    html += '</div>';

    // Lease notes — editable textarea. Always rendered (so FA can ADD notes
    // even to imported tenants that don't have any yet).
    {
      const notesText = (t.lease_notes || '').replace(/</g,'&lt;');
      const hasNotes = !!(t.lease_notes && t.lease_notes.trim());
      const noteCount = hasNotes ? t.lease_notes.split('\n').filter(l => l.trim()).length : 0;
      html += '<details style="margin-top:8px;"' + (hasNotes ? '' : '') + '><summary style="font-size:11px; color:var(--gray-500); cursor:pointer;">📝 Lease notes' + (noteCount > 0 ? ' <span style="color:var(--gray-400);">(' + noteCount + ')</span>' : ' <span style="color:var(--gray-400); font-style:italic;">— click to add —</span>') + '</summary>' +
        '<textarea ' +
          'placeholder="Lease term, renewal options, escalation clauses, contact info, ..." ' +
          'onblur="commercialUpdateTenantField(' + t.id + ',\'lease_notes\',this.value || null)" ' +
          'class="comm-edit-input" ' +
          'style="display:block; width:100%; min-height:80px; font-size:11px; color:var(--gray-700); margin-top:4px; padding:8px; background:#fafaf7; border:1px solid var(--gray-200); border-radius:6px; font-family:inherit; resize:vertical;">' +
            notesText +
        '</textarea></details>';
    }

    html += '</div>';
  });

  html += '</div>';  // end grid

  // Footer with status + actions
  html += '<div style="margin-top:20px; padding:14px; background:#fafaf7; border:1px solid var(--gray-200); border-radius:8px; font-size:12px; color:var(--gray-600); display:flex; align-items:center; gap:10px; flex-wrap:wrap;">' +
    '<span>Edit tenant cards inline, add rent periods, and configure escalations — changes auto-feed the Summary rows (Commercial Rent 4040 / Escalations 4520).</span>' +
    '<button onclick="commercialAddTenant()" style="margin-left:auto; padding:5px 12px; background:#9a3412; color:#fff; border:none; border-radius:4px; font-size:11px; font-weight:600; cursor:pointer;">+ Add Commercial Unit</button>' +
    '<button onclick="commercialReimport()" style="padding:4px 10px; border:1px solid var(--gray-300); background:white; border-radius:4px; font-size:11px; cursor:pointer;">↻ Re-import from Excel</button>' +
    '</div>';

  contentDiv.innerHTML = html;
}

// Update a single field on a tenant (escalation model, share %, base years,
// lease dates, notes, etc.). Calls PUT /api/commercial/<ec>/tenant/<id>.
// Phase 3b: triggers a Summary recompute server-side via the GET reload.
async function commercialUpdateTenantField(tenantId, field, value) {
  try {
    const resp = await fetch('/api/commercial/' + entityCode + '/tenant/' + tenantId, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[field]: value}),
    });
    if (!resp.ok) {
      const e = await resp.json().catch(() => ({}));
      alert('Save failed: ' + (e.error || resp.status));
      return;
    }
    // Debounced full re-render so escalation math + sync banner update.
    if (window._commRerenderTimer) clearTimeout(window._commRerenderTimer);
    window._commRerenderTimer = setTimeout(() => {
      renderCommercialTab(document.getElementById('sheetContent'));
    }, 600);
  } catch (e) {
    alert('Save error: ' + (e.message || e));
  }
}

// ── Commercial CRUD handlers (Phase 2) ──

// FA #30 (2026-06-16): add a new commercial unit / tenant. Prompts for a name,
// POSTs to create it, then re-renders the tab so the FA can fill in rent
// periods + escalation config on the new card.
async function commercialAddTenant() {
  const name = prompt('Name of the commercial unit / tenant (e.g. "Ground Floor Retail", "Garage"):', '');
  if (name === null) return;
  if (!name.trim()) { alert('A name is required.'); return; }
  try {
    const resp = await fetch('/api/commercial/' + entityCode + '/tenant', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({tenant_name: name.trim()}),
    });
    const data = await resp.json();
    if (!resp.ok || data.error) { alert('Could not add unit: ' + (data.error || resp.status)); return; }
    renderCommercialTab(document.getElementById('sheetContent'));
  } catch (e) {
    alert('Could not add unit: ' + (e.message || e));
  }
}

// Inline-edit a single field on a rent period. Called from onblur on the
// editable inputs in the period table. Saves to DB, recomputes the
// "Annual" cell, and updates the tenant card totals.
async function commercialUpdatePeriod(tenantId, periodId, field, value, inputEl) {
  if (inputEl) {
    inputEl.style.borderColor = 'var(--blue)';
  }
  try {
    const resp = await fetch('/api/commercial/' + entityCode + '/tenant/' + tenantId + '/period/' + periodId, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[field]: value}),
    });
    if (!resp.ok) {
      const e = await resp.json().catch(() => ({}));
      alert('Save failed: ' + (e.error || resp.status));
      return;
    }
    const updated = await resp.json();
    // Recompute the Annual cell in this row
    if (inputEl) {
      const row = inputEl.closest('tr');
      if (row) {
        const annCell = row.querySelector('.ann-cell');
        if (annCell) annCell.textContent = '$' + Math.round(updated.annualized || 0).toLocaleString();
      }
      inputEl.style.borderColor = 'transparent';
      // Flash green briefly to show save succeeded
      inputEl.style.background = '#dcfce7';
      setTimeout(() => { inputEl.style.background = 'transparent'; }, 300);
    }
    // Re-render the whole tab to refresh totals + budget/prior badges
    // (debounced — only refresh after no edits for 1.5s)
    if (window._commRerenderTimer) clearTimeout(window._commRerenderTimer);
    window._commRerenderTimer = setTimeout(() => {
      renderCommercialTab(document.getElementById('sheetContent'));
    }, 1500);
  } catch (err) {
    alert('Save error: ' + (err.message || err));
    if (inputEl) inputEl.style.borderColor = 'var(--red)';
  }
}

async function commercialDeletePeriod(tenantId, periodId, btnEl) {
  if (!confirm('Delete this rent period?')) return;
  try {
    const resp = await fetch('/api/commercial/' + entityCode + '/tenant/' + tenantId + '/period/' + periodId, {method: 'DELETE'});
    if (!resp.ok) {
      alert('Delete failed');
      return;
    }
    renderCommercialTab(document.getElementById('sheetContent'));
  } catch (e) { alert('Delete error: ' + e.message); }
}

async function commercialAddPeriodPrompt(tenantId, year) {
  const label = prompt('Period label (e.g. "Jan-Dec", "Jan-Feb"):', 'Jan-Dec');
  if (!label) return;
  const rentStr = prompt('Monthly rent ($):', '0');
  if (rentStr === null) return;
  const monthsStr = prompt('Number of months (1-12):', label.toLowerCase().includes('jan-dec') ? '12' : '1');
  if (monthsStr === null) return;
  try {
    const resp = await fetch('/api/commercial/' + entityCode + '/tenant/' + tenantId + '/period', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        year: year,
        period_label: label,
        monthly_rent: parseFloat(rentStr) || 0,
        months_count: parseInt(monthsStr) || 12,
      }),
    });
    if (!resp.ok) { alert('Add failed'); return; }
    renderCommercialTab(document.getElementById('sheetContent'));
  } catch (e) { alert('Add error: ' + e.message); }
}

async function commercialDeleteTenant(tenantId, name) {
  if (!confirm('Delete tenant "' + name + '" and all rent periods?')) return;
  try {
    const resp = await fetch('/api/commercial/' + entityCode + '/tenant/' + tenantId, {method: 'DELETE'});
    if (!resp.ok) { alert('Delete failed'); return; }
    renderCommercialTab(document.getElementById('sheetContent'));
  } catch (e) { alert('Delete error: ' + e.message); }
}

// Global "Project to BUDGET_YEAR" — applies bump % across all tenants.
async function commercialProjectAll() {
  const pctInput = document.getElementById('commProjectPct');
  const pct = (parseFloat(pctInput.value) || 0) / 100;
  const conf = pct === 0
    ? 'Copy ' + ({{ budget_year }} - 1) + ' rent periods unchanged into ' + {{ budget_year }} + ' for every tenant that doesn\'t already have a ' + {{ budget_year }} + ' projection?'
    : 'Copy ' + ({{ budget_year }} - 1) + ' rent periods × ' + (pct * 100).toFixed(1) + '% increase into ' + {{ budget_year }} + ' for every tenant that doesn\'t already have a projection?';
  if (!confirm(conf)) return;
  try {
    const resp = await fetch('/api/commercial/' + entityCode + '/project-year', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        from_year: {{ budget_year }} - 1,
        to_year: {{ budget_year }},
        increase_pct: pct,
        overwrite: false,
      }),
    });
    const d = await resp.json();
    if (!resp.ok) { alert('Projection failed: ' + (d.error || resp.status)); return; }
    showToast('Projected ' + d.periods_created + ' periods to ' + d.to_year, 'success');
    renderCommercialTab(document.getElementById('sheetContent'));
  } catch (e) { alert('Project error: ' + e.message); }
}

async function commercialProjectOne(tenantId) {
  // Per-tenant projection. Same default 3% bump; FA can adjust afterward.
  const pctStr = prompt('Annual increase % for this tenant?\n(Use 0 for flat, or enter lease-specific bump.)', '3.0');
  if (pctStr === null) return;
  const pct = (parseFloat(pctStr) || 0) / 100;
  try {
    const resp = await fetch('/api/commercial/' + entityCode + '/project-year', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        from_year: {{ budget_year }} - 1,
        to_year: {{ budget_year }},
        increase_pct: pct,
        tenant_ids: [tenantId],
        overwrite: false,
      }),
    });
    const d = await resp.json();
    if (!resp.ok) { alert('Projection failed: ' + (d.error || resp.status)); return; }
    renderCommercialTab(document.getElementById('sheetContent'));
  } catch (e) { alert('Project error: ' + e.message); }
}

async function commercialReimport() {
  if (!confirm('Re-import overwrites all current tenant data for this building with what is in the Excel. Continue?')) return;
  try {
    const resp = await fetch('/api/commercial/' + entityCode + '/import?force=1', {method: 'POST'});
    const d = await resp.json();
    if (d.status === 'imported' || d.status === 'exists') {
      showToast('Re-imported ' + (d.imported || 0) + ' tenants', 'success');
      renderCommercialTab(document.getElementById('sheetContent'));
    } else {
      alert('Re-import failed: ' + (d.error || d.status));
    }
  } catch (e) {
    alert('Re-import error: ' + (e.message || e));
  }
}

// ── History Tab ──
async function renderHistoryTab(contentDiv) {
  contentDiv.innerHTML = '<p style="padding:24px; color:var(--gray-500);">Loading change history...</p>';
  try {
    const resp = await fetch('/api/budget-history/' + entityCode);
    const data = await resp.json();
    const revs = data.revisions || [];

    if (revs.length === 0) {
      contentDiv.innerHTML = '<div style="padding:24px; text-align:center; color:var(--gray-400);">' +
        '<div style="font-size:32px; margin-bottom:8px;">\ud83d\udcdd</div>' +
        '<p>No changes recorded yet.</p>' +
        '<p style="font-size:12px;">Changes will appear here as you edit budget lines, update assumptions, and change statuses.</p></div>';
      return;
    }

    const actionLabels = {
      'update': 'Edited', 'status_change': 'Status Changed',
      'create': 'Created', 'reclass': 'Reclassified', 'presentation_edit': 'Presentation Edit'
    };
    const fieldLabels = {
      'increase_pct': 'Increase %', 'proposed_budget': 'Proposed Budget',
      'notes': 'Notes', 'status': 'Status', 'accrual_adj': 'Accrual Adj',
      'unpaid_bills': 'Unpaid Bills'
    };

    let html = '<table style="width:100%; border-collapse:collapse; font-size:13px;">' +
      '<thead><tr style="background:var(--gray-100); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
      '<th style="text-align:left; padding:8px;">When</th>' +
      '<th style="text-align:left; padding:8px;">Action</th>' +
      '<th style="text-align:left; padding:8px;">GL / Item</th>' +
      '<th style="text-align:left; padding:8px;">Field</th>' +
      '<th style="text-align:right; padding:8px;">Old Value</th>' +
      '<th style="text-align:right; padding:8px;">New Value</th>' +
      '<th style="text-align:left; padding:8px;">Source</th>' +
      '</tr></thead><tbody>';

    revs.forEach(r => {
      const when = r.created_at ? new Date(r.created_at).toLocaleString() : '';
      const action = actionLabels[r.action] || r.action;
      const gl = r.gl_code ? r.gl_code + ' — ' + (r.description || '') : (r.action === 'status_change' ? 'Budget' : '—');
      const field = fieldLabels[r.field_name] || r.field_name || '';
      const oldVal = r.field_name === 'proposed_budget' ? fmt(parseFloat(r.old_value) || 0) : r.old_value || '';
      const newVal = r.field_name === 'proposed_budget' ? fmt(parseFloat(r.new_value) || 0) : r.new_value || '';
      const actionColor = r.action === 'status_change' ? 'var(--blue)' : 'var(--gray-600)';

      html += '<tr style="border-bottom:1px solid var(--gray-100);">' +
        '<td style="padding:6px 8px; color:var(--gray-400); font-size:12px; white-space:nowrap;">' + when + '</td>' +
        '<td style="padding:6px 8px; color:' + actionColor + '; font-weight:500;">' + action + '</td>' +
        '<td style="padding:6px 8px; font-family:monospace; font-size:12px;">' + gl + '</td>' +
        '<td style="padding:6px 8px;">' + field + '</td>' +
        '<td style="text-align:right; padding:6px 8px; color:var(--red); text-decoration:line-through; font-size:12px;">' + oldVal + '</td>' +
        '<td style="text-align:right; padding:6px 8px; color:var(--green); font-weight:500;">' + newVal + '</td>' +
        '<td style="padding:6px 8px; font-size:11px; color:var(--gray-400);">' + (r.source || '') + '</td></tr>';
    });

    html += '</tbody></table>';
    contentDiv.innerHTML = html;
  } catch (err) {
    contentDiv.innerHTML = '<p style="padding:24px; color:var(--red);">Error loading history: ' + err.message + '</p>';
  }
}

// ──────────────────────────────────────────────────────────────────────
//  BUILDING INFO TAB — reference/illustrative data per entity.
//  Sections: Maintenance History, Amortization Schedule.
//  Zero impact on budget math. Designed to be extensible — add new
//  sections by extending _biData + adding a new _biRender* helper.
// ──────────────────────────────────────────────────────────────────────
let _biData = null;
let _biSaveTimer = null;

function _biEnsureStyles() {
  if (document.getElementById('biStyles')) return;
  const s = document.createElement('style');
  s.id = 'biStyles';
  s.textContent = ''
    + '.bi-page { max-width:1240px; margin:0 auto; }'
    + '.bi-card { background:white; border:1px solid var(--gray-200); border-radius:12px; margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.04); overflow:hidden; }'
    + '.bi-card-header { padding:14px 20px; background:var(--blue-light); border-bottom:1px solid var(--gray-200); display:flex; align-items:center; justify-content:space-between; }'
    + '.bi-card-header h2 { margin:0; font-size:13px; font-weight:700; color:var(--blue); text-transform:uppercase; letter-spacing:0.8px; }'
    + '.bi-illus-chip { font-size:10px; font-weight:700; color:#d97706; background:#fffbeb; border:1px solid #fde68a; padding:3px 8px; border-radius:4px; text-transform:uppercase; letter-spacing:0.5px; }'
    + '.bi-card-body { padding:20px; }'
    + '.bi-note { margin-top:14px; padding:10px 14px; background:var(--blue-light); border-left:3px solid var(--blue); border-radius:4px; font-size:12px; color:var(--blue); }'
    + '.bi-toolbar { display:flex; gap:8px; justify-content:flex-end; margin-top:12px; }'
    + '.bi-btn { padding:6px 14px; font-size:12px; font-weight:600; border-radius:6px; border:1px solid var(--gray-300); background:white; color:var(--gray-700); cursor:pointer; font-family:inherit; }'
    + '.bi-btn:hover { background:var(--gray-100); border-color:var(--gray-400); }'
    + '.bi-btn.primary { background:var(--blue); color:white; border-color:var(--blue); }'
    + '.bi-btn.primary:hover { background:#4a3d33; }'
    + '.bi-btn.ghost { border:1px solid transparent; color:var(--gray-500); }'
    + '.bi-btn.ghost:hover { background:var(--gray-100); color:var(--gray-700); }'
    /* Maint History table */
    + 'table.bi-mh { border-collapse:separate; border-spacing:0; width:100%; font-size:13px; }'
    + 'table.bi-mh thead th { padding:8px 10px; text-align:right; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); border-bottom:2px solid var(--gray-300); background:var(--gray-100); white-space:nowrap; }'
    + 'table.bi-mh thead th:first-child, table.bi-mh thead th:nth-child(2), table.bi-mh thead th:nth-child(3) { text-align:left; }'
    + 'table.bi-mh td { padding:6px 10px; border-bottom:1px solid var(--gray-200); font-variant-numeric:tabular-nums; text-align:right; }'
    + 'table.bi-mh td.gl { font-weight:700; color:var(--blue); font-size:12px; background:var(--gray-50); padding-left:16px; text-align:left; }'
    + 'table.bi-mh td.year-label { text-align:left; font-weight:600; color:var(--gray-700); }'
    + 'table.bi-mh td.label { text-align:left; }'
    + 'table.bi-mh tr.budget-row td { border-top:2px solid var(--blue); font-weight:700; background:#fef9ef; color:var(--blue); }'
    + 'table.bi-mh tr.budget-row td.year-label { color:var(--blue); }'
    + 'table.bi-mh input.bi-cell { width:110px; padding:5px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:inherit; font-variant-numeric:tabular-nums; text-align:right; background:#fbfaf4; }'
    + 'table.bi-mh input.bi-cell.small { width:80px; }'
    + 'table.bi-mh input.bi-cell.dec8 { width:130px; }'
    + 'table.bi-mh input.bi-cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px rgba(90,74,63,0.15); }'
    + 'table.bi-mh td.derived { color:var(--gray-700); background:#f0fdf4; font-weight:600; }'
    /* Amort params */
    + '.bi-amort-params { display:grid; grid-template-columns:repeat(4, 1fr); gap:16px; margin-bottom:20px; padding-bottom:20px; border-bottom:1px dashed var(--gray-200); }'
    + '.bi-amort-params .field label { display:block; font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); margin-bottom:4px; }'
    + '.bi-amort-params .field input, .bi-amort-params .field select { width:100%; padding:7px 10px; border:1px solid var(--gray-300); border-radius:6px; font-size:13px; font-family:inherit; background:#fbfaf4; }'
    + '.bi-amort-params .field input:focus, .bi-amort-params .field select:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px rgba(90,74,63,0.15); }'
    + '.bi-amort-summary { display:grid; grid-template-columns:repeat(4, 1fr); gap:16px; margin-bottom:20px; }'
    + '.bi-summary-card { padding:14px 16px; background:#f0fdf4; border:1px solid #bbf7d0; border-radius:8px; }'
    + '.bi-summary-card .label { font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; color:var(--green); }'
    + '.bi-summary-card .value { margin-top:4px; font-size:18px; font-weight:700; color:#14532d; font-variant-numeric:tabular-nums; }'
    + '.bi-amort-scroll { max-height:480px; overflow-y:auto; border:1px solid var(--gray-200); border-radius:8px; }'
    + '.bi-amort-scroll::-webkit-scrollbar { width:10px; }'
    + '.bi-amort-scroll::-webkit-scrollbar-track { background:var(--gray-100); }'
    + '.bi-amort-scroll::-webkit-scrollbar-thumb { background:#8b7355; border-radius:6px; }'
    + 'table.bi-amort { border-collapse:separate; border-spacing:0; width:100%; font-size:12px; }'
    + 'table.bi-amort thead th { position:sticky; top:0; z-index:10; padding:8px 10px; background:var(--gray-100); border-bottom:2px solid var(--gray-300); font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); text-align:right; white-space:nowrap; }'
    + 'table.bi-amort thead th:nth-child(1), table.bi-amort thead th:nth-child(2) { text-align:left; }'
    + 'table.bi-amort td { padding:5px 10px; border-bottom:1px solid var(--gray-100); font-variant-numeric:tabular-nums; text-align:right; }'
    + 'table.bi-amort td:nth-child(1), table.bi-amort td:nth-child(2) { text-align:left; }'
    + 'table.bi-amort tr:hover td { background:#f0f9ff; }'
    + 'table.bi-amort tr.year-end td { border-bottom:2px solid var(--gray-300); background:#fffbeb; }'
    + '.bi-empty { padding:32px; text-align:center; color:var(--gray-400); }'
    /* FA dir 2026-05-21: amortization solver mode pills + computed field styling */
    + '.bi-amort-mode-pill { padding:5px 12px; border:1px solid var(--gray-300); border-radius:999px; font-size:11px; font-weight:600; cursor:pointer; background:white; color:var(--gray-700); font-family:inherit; }'
    + '.bi-amort-mode-pill:hover { background:var(--gray-100); border-color:var(--blue); }'
    + '.bi-amort-mode-pill.active { background:var(--blue); color:white; border-color:var(--blue); }'
    + '.bi-amort-params .field input.computed { background:#dcfce7 !important; border-color:#16a34a !important; color:#15803d; font-weight:600; }'
    + '.bi-amort-tag { display:inline-block; padding:1px 6px; border-radius:3px; font-size:9px; font-weight:600; text-transform:uppercase; letter-spacing:0.4px; margin-left:6px; }'
    + '.bi-amort-tag.computed { background:#dcfce7; color:#15803d; }'
    + '.bi-amort-status { font-weight:500; }';
  document.head.appendChild(s);
}

function _biNum(v) {
  if (v == null) return 0;
  const n = parseFloat(String(v).replace(/[$,\s%]/g, ''));
  return isNaN(n) ? 0 : n;
}
function _biFmtInt(n)  { if (n == null || isNaN(n)) return '\u2014'; return Math.round(n).toLocaleString(); }
function _biFmtD(n)    { if (n == null || isNaN(n)) return '\u2014'; return '$' + Math.round(n).toLocaleString(); }
function _biFmtD2(n)   { if (n == null || isNaN(n)) return '\u2014'; return '$' + n.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}); }

function _biDefaultMaint() {
  const rows = [];
  for (let y = BY - 5; y <= BY; y++) {
    rows.push({ year: y, shares: 0, perShare: 0, increase: 0 });
  }
  return rows;
}
function _biDefaultAmort() {
  // FA directive 2026-05-11: io_months = number of initial interest-only
  // periods. 0 = standard amortization (default, no behavior change).
  // FA directive 2026-05-21: payment + balloon fields added. mode picks
  // which field the recalculator solves for:
  //   standard:   solves payment   (needs principal + rate + term)
  //   by_payment: solves term      (needs principal + rate + payment)
  //   by_balance: solves balloon   (needs principal + rate + term + payment)
  return {
    label: '', principal: 0, rate: 0, term: 0, start: '', freq: 12, io_months: 0,
    mode: 'standard',
    payment: 0,      // per-period payment ($)
    end_balance: 0,  // balloon at maturity ($)
  };
}

function _biSaveSoon() {
  if (_biSaveTimer) clearTimeout(_biSaveTimer);
  _biSaveTimer = setTimeout(_biSaveNow, 800);
}
async function _biSaveNow() {
  _biSaveTimer = null;
  if (!_biData) return;
  // FA dir 2026-05-18 (Item 10 follow-up): show save status. Previously the
  // indicator span sat empty so the FA had no idea changes had persisted —
  // some buildings even complained "amortization schedule doesn't work"
  // when it actually did, they just hadn't seen confirmation.
  const indicator = document.getElementById('biSaveIndicator');
  if (indicator) { indicator.textContent = 'Saving…'; indicator.style.color = 'var(--gray-500)'; }
  try {
    const resp = await fetch('/api/building-info/' + entityCode, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        maintenance_history: _biData.maintenance_history,
        amort_config: _biData.amort_config,
      }),
    });
    if (resp.ok && indicator) {
      indicator.textContent = '✓ Saved ' + new Date().toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
      indicator.style.color = 'var(--green, #16a34a)';
      setTimeout(() => {
        if (indicator.textContent.startsWith('✓ Saved')) indicator.textContent = '';
      }, 4000);
    } else if (indicator) {
      indicator.textContent = '✗ Save failed (' + resp.status + ')';
      indicator.style.color = 'var(--red, #dc2626)';
    }
  } catch (err) {
    console.warn('building-info save failed:', err);
    if (indicator) {
      indicator.textContent = '✗ Save error — check connection';
      indicator.style.color = 'var(--red, #dc2626)';
    }
  }
}

function openBuildingInfo() {
  // Deactivate all sheet tabs so none appear selected while viewing Building Info
  document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
  const contentDiv = document.getElementById('sheetContent');
  if (!contentDiv) return;
  renderBuildingInfoTab(contentDiv);
}

async function renderBuildingInfoTab(contentDiv) {
  _biEnsureStyles();
  contentDiv.innerHTML = '<p style="padding:24px; color:var(--gray-500);">Loading building info\u2026</p>';
  try {
    const resp = await fetch('/api/building-info/' + entityCode);
    _biData = await resp.json();
  } catch (err) {
    contentDiv.innerHTML = '<p style="padding:24px; color:var(--red);">Error loading building info: ' + err.message + '</p>';
    return;
  }
  if (!_biData.maintenance_history || !Array.isArray(_biData.maintenance_history) || _biData.maintenance_history.length === 0) {
    _biData.maintenance_history = _biDefaultMaint();
  }
  if (!_biData.amort_config || typeof _biData.amort_config !== 'object') {
    _biData.amort_config = _biDefaultAmort();
  }
  _biRenderAll(contentDiv);
}

function _biRenderAll(container) {
  // Condo? Show the Common Charges History card alongside (or instead of)
  // Maintenance History. We render Common Charges only when there's actually
  // data on the BuildingInfo row \u2014 populated by D1 from the Income tab's
  // GL 4020-0000 block.
  const ccRows = (_biData && Array.isArray(_biData.common_charges_history)) ? _biData.common_charges_history : null;
  const showCC = ccRows && ccRows.length > 0;
  container.innerHTML =
    '<div class="bi-page">' +
      '<div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px; gap:12px;">' +
        '<button onclick="_biCloseAndReturn()" style="padding:6px 14px; font-size:12px; font-weight:600; background:var(--blue-light, #f5efe7); color:var(--blue, #5a4a3f); border:1px solid var(--blue, #5a4a3f); border-radius:6px; cursor:pointer;">\u2190 Back to Summary</button>' +
        '<div style="display:flex; align-items:center; gap:10px;">' +
          // FA dir 2026-05-19: Undo + History buttons for snapshot recovery
          '<button id="biUndoBtn" onclick="_biUndoLast()" title="Restore the most recent saved state (your last edit will be undone)" style="padding:6px 12px; font-size:12px; font-weight:600; background:white; color:var(--gray-700, #4a4039); border:1px solid var(--gray-300, #d5cfc5); border-radius:6px; cursor:pointer;">\u21a9 Undo last change</button>' +
          '<button id="biHistoryBtn" onclick="_biOpenHistory()" title="Browse the last 20 saved versions" style="padding:6px 12px; font-size:12px; font-weight:600; background:white; color:var(--gray-700, #4a4039); border:1px solid var(--gray-300, #d5cfc5); border-radius:6px; cursor:pointer;">\u23f1 History\u2026</button>' +
          '<span id="biSaveIndicator" style="font-size:11px; color:var(--gray-500);"></span>' +
        '</div>' +
      '</div>' +
      _biRenderType() +
      _biRenderMaint() +
      (showCC ? _biRenderCommonCharges() : '') +
      _biRenderAmort() +
    '</div>';
  _biRecalcAmort();
  _biRefreshUndoButton();
}

// FA dir 2026-05-19: snapshot-based undo / history controls
async function _biRefreshUndoButton() {
  const btn = document.getElementById('biUndoBtn');
  if (!btn) return;
  try {
    const resp = await fetch('/api/building-info/' + entityCode + '/history');
    const data = await resp.json();
    const count = (data.snapshots || []).length;
    if (count === 0) {
      btn.disabled = true;
      btn.style.opacity = '0.45';
      btn.style.cursor = 'not-allowed';
      btn.title = 'No previous versions saved yet';
    } else {
      btn.disabled = false;
      btn.style.opacity = '1';
      btn.style.cursor = 'pointer';
      btn.title = 'Restore the most recent saved state (' + count + ' version' + (count !== 1 ? 's' : '') + ' available)';
    }
  } catch (_e) { /* leave button as-is */ }
}

async function _biUndoLast() {
  const btn = document.getElementById('biUndoBtn');
  if (btn && btn.disabled) return;
  if (!confirm('Restore the most recent saved version?\\n\\nYour current edits will be saved as a new version first, so you can redo if needed.')) return;
  if (btn) { btn.disabled = true; btn.textContent = '\u21a9 Restoring\u2026'; }
  try {
    const resp = await fetch('/api/building-info/' + entityCode + '/restore', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({snapshot_index: 0}),  // 0 = most recent in the reversed list
    });
    if (!resp.ok) {
      const err = await resp.text();
      alert('Restore failed: ' + err.slice(0, 200));
      if (btn) { btn.disabled = false; btn.textContent = '\u21a9 Undo last change'; }
      return;
    }
    const indicator = document.getElementById('biSaveIndicator');
    if (indicator) {
      indicator.textContent = '\u2713 Restored at ' + new Date().toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
      indicator.style.color = 'var(--green, #16a34a)';
    }
    // Reload Building Info to show restored state
    const contentDiv = document.getElementById('sheetContent');
    if (contentDiv) renderBuildingInfoTab(contentDiv);
  } catch (e) {
    alert('Restore error: ' + e.message);
    if (btn) { btn.disabled = false; btn.textContent = '\u21a9 Undo last change'; }
  }
}

async function _biOpenHistory() {
  try {
    const resp = await fetch('/api/building-info/' + entityCode + '/history');
    const data = await resp.json();
    const snaps = data.snapshots || [];
    // Build modal HTML
    let modalHtml = '<div id="biHistoryOverlay" onclick="_biCloseHistory()" style="position:fixed; inset:0; background:rgba(0,0,0,0.45); z-index:1000;"></div>';
    modalHtml += '<div id="biHistoryModal" style="position:fixed; top:80px; left:50%; transform:translateX(-50%); width:560px; max-width:92vw; max-height:80vh; background:white; border-radius:12px; box-shadow:0 20px 60px rgba(0,0,0,0.3); z-index:1001; overflow:hidden; display:flex; flex-direction:column;">';
    modalHtml += '<div style="padding:16px 22px; border-bottom:1px solid var(--gray-200); display:flex; justify-content:space-between; align-items:center;">';
    modalHtml += '<h3 style="margin:0; font-size:16px; font-weight:700; color:var(--gray-900);">Building Info \u2014 Version History</h3>';
    modalHtml += '<button onclick="_biCloseHistory()" style="border:none; background:transparent; font-size:20px; cursor:pointer; color:var(--gray-500); line-height:1;">\u00d7</button>';
    modalHtml += '</div>';
    modalHtml += '<div style="overflow-y:auto; flex:1;">';
    if (snaps.length === 0) {
      modalHtml += '<div style="padding:32px; text-align:center; color:var(--gray-500);">No saved versions yet. Your next edit will create the first snapshot.</div>';
    } else {
      for (const s of snaps) {
        const tsLocal = s.ts ? new Date(s.ts).toLocaleString() : '(unknown time)';
        const mhStr = s.maintenance_rows + ' maint row' + (s.maintenance_rows !== 1 ? 's' : '');
        const ccStr = s.common_charges_rows + ' CC row' + (s.common_charges_rows !== 1 ? 's' : '');
        const amStr = s.has_amort_config ? 'amort config' : 'no amort';
        modalHtml += '<div style="padding:14px 22px; border-bottom:1px solid var(--gray-100); display:flex; justify-content:space-between; align-items:center; gap:16px;">';
        modalHtml += '<div>';
        modalHtml += '<div style="font-size:13px; font-weight:600; color:var(--gray-900);">' + tsLocal + '</div>';
        modalHtml += '<div style="font-size:12px; color:var(--gray-500); margin-top:2px;">' + mhStr + ' \u00b7 ' + ccStr + ' \u00b7 ' + amStr + '</div>';
        modalHtml += '</div>';
        modalHtml += '<button onclick="_biRestoreSnapshot(' + s.index + ')" style="padding:6px 14px; font-size:12px; font-weight:600; background:var(--blue, #5a4a3f); color:white; border:none; border-radius:6px; cursor:pointer; white-space:nowrap;">Restore this</button>';
        modalHtml += '</div>';
      }
    }
    modalHtml += '</div>';
    modalHtml += '<div style="padding:12px 22px; background:var(--gray-50, #f4f1eb); border-top:1px solid var(--gray-200); font-size:11px; color:var(--gray-500);">Last 20 versions kept. Older versions are pruned automatically.</div>';
    modalHtml += '</div>';
    const div = document.createElement('div');
    div.id = 'biHistoryRoot';
    div.innerHTML = modalHtml;
    document.body.appendChild(div);
  } catch (e) {
    alert('Could not load history: ' + e.message);
  }
}

function _biCloseHistory() {
  const root = document.getElementById('biHistoryRoot');
  if (root) root.remove();
}

async function _biRestoreSnapshot(idx) {
  if (!confirm('Restore this version? Your current state will be saved as a new version first.')) return;
  try {
    const resp = await fetch('/api/building-info/' + entityCode + '/restore', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({snapshot_index: idx}),
    });
    if (!resp.ok) {
      alert('Restore failed: ' + await resp.text());
      return;
    }
    _biCloseHistory();
    const contentDiv = document.getElementById('sheetContent');
    if (contentDiv) renderBuildingInfoTab(contentDiv);
  } catch (e) {
    alert('Restore error: ' + e.message);
  }
}

// Building Type card \u2014 coop/condo/rental/mixed/other. Source of truth for
// is_coop() across the app. FA directive 2026-05-05.
function _biRenderType() {
  const bt = (_biData && _biData.building_type) ? String(_biData.building_type).toLowerCase() : '';
  const opts = [
    {v: '',       label: '\u2014 Not set \u2014'},
    {v: 'coop',   label: 'Co-op'},
    {v: 'condo',  label: 'Condo'},
    {v: 'rental', label: 'Rental'},
    {v: 'mixed',  label: 'Mixed-use'},
    {v: 'other',  label: 'Other'},
  ];
  let optHtml = '';
  opts.forEach(o => {
    const sel = (o.v === bt) ? ' selected' : '';
    optHtml += '<option value="' + o.v + '"' + sel + '>' + o.label + '</option>';
  });
  return ''
    + '<div class="bi-card">'
    +   '<div class="bi-card-header">'
    +     '<h2>Building Type</h2>'
    +   '</div>'
    +   '<div class="bi-card-body">'
    +     '<div style="display:flex; align-items:center; gap:14px; flex-wrap:wrap;">'
    +       '<label style="font-size:13px; font-weight:600; color:var(--gray-700);">Type:</label>'
    +       '<select id="biBuildingType" onchange="_biTypeUpd(this.value)" style="padding:6px 10px; font-size:13px; border:1px solid var(--gray-300); border-radius:6px; min-width:180px;">'
    +         optHtml
    +       '</select>'
    +       '<span id="biTypeStatus" style="font-size:11px; color:var(--gray-500);"></span>'
    +     '</div>'
    +     '<div class="bi-note">Drives Common Charges vs. Maintenance, summary-row math, and other coop/condo-specific behavior. Saved immediately.</div>'
    +   '</div>'
    + '</div>';
}

async function _biTypeUpd(val) {
  const v = (val || '').toLowerCase();
  if (!_biData) return;
  _biData.building_type = v;
  const status = document.getElementById('biTypeStatus');
  if (status) status.textContent = 'Saving\u2026';
  try {
    const resp = await fetch('/api/building-info/' + entityCode, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ building_type: v }),
    });
    if (!resp.ok) {
      const j = await resp.json().catch(() => ({}));
      if (status) status.textContent = 'Save failed: ' + (j.error || resp.status);
      return;
    }
    if (status) {
      status.textContent = 'Saved \u2713';
      setTimeout(() => { if (status) status.textContent = ''; }, 1800);
    }
  } catch (err) {
    if (status) status.textContent = 'Save failed: ' + err.message;
  }
}

// Common Charges History \u2014 condo equivalent of Maintenance History. Same
// table shape minus shares/perShare since condos don't use per-share rates.
function _biRenderCommonCharges() {
  const rows = _biData.common_charges_history || [];
  let body = '';
  rows.forEach((r, i) => {
    const isBudget = (r.year === BY);
    const rowCls = isBudget ? 'budget-row' : '';
    const yearLabel = r.year_label
        ? r.year_label
        : (isBudget ? (r.year + ' Budget') : r.year);
    body += '<tr class="' + rowCls + '">';
    body += '<td class="gl">' + (i === 0 ? '4020-0000' : '') + '</td>';
    body += '<td class="label">' + (i === 0 ? 'Common Charges' : '') + '</td>';
    body += '<td class="year-label">' + yearLabel + '</td>';
    body += '<td><input class="bi-cell" value="' + (r.monthly || 0).toLocaleString() + '" onblur="_biCcUpd(' + i + ',\'monthly\',this.value)" onfocus="this.select()"></td>';
    body += '<td><input class="bi-cell" value="' + (r.annual || 0).toLocaleString() + '" onblur="_biCcUpd(' + i + ',\'annual\',this.value)" onfocus="this.select()"></td>';
    body += '<td><input class="bi-cell small" value="' + (r.increase || 0).toFixed(2) + '%" onblur="_biCcUpd(' + i + ',\'increase\',this.value)" onfocus="this.select()"></td>';
    body += '</tr>';
  });
  return ''
    + '<div class="bi-card">'
    +   '<div class="bi-card-header">'
    +     '<h2>Common Charges History \u2014 4020-0000</h2>'
    +     '<span class="bi-illus-chip">Illustrative Only</span>'
    +   '</div>'
    +   '<div class="bi-card-body">'
    +     '<table class="bi-mh">'
    +       '<thead><tr>'
    +         '<th style="width:120px;">G/L</th>'
    +         '<th style="width:160px;">Label</th>'
    +         '<th style="width:140px;">Year</th>'
    +         '<th>Monthly</th>'
    +         '<th>Annual</th>'
    +         '<th>Increase</th>'
    +       '</tr></thead>'
    +       '<tbody>' + body + '</tbody>'
    +     '</table>'
    +     '<div class="bi-note">Auto-populated from the 2026 approved budget\u2019s Income tab (GL 4020-0000). Edits saved per-building \u2014 do not affect budget math.</div>'
    +   '</div>'
    + '</div>';
}

function _biCcUpd(i, field, val) {
  const n = _biNum(val);
  if (!_biData.common_charges_history || !_biData.common_charges_history[i]) return;
  // FA directive 2026-05-10: short-circuit when the value didn't change.
  // Prevents click-in/click-out from triggering re-render + save.
  if (_biData.common_charges_history[i][field] === n) return;
  _biData.common_charges_history[i][field] = n;
  // Re-render in place \u2014 find the Common Charges card by its header text.
  const cards = document.querySelectorAll('.bi-page .bi-card');
  cards.forEach(card => {
    const h = card.querySelector('h2');
    if (h && h.textContent.indexOf('Common Charges') >= 0) {
      card.outerHTML = _biRenderCommonCharges();
    }
  });
  _biSaveSoon();
}

// Flush any pending save, then re-activate Summary tab
function _biCloseAndReturn() {
  if (_biSaveTimer) { clearTimeout(_biSaveTimer); _biSaveTimer = null; _biSaveNow(); }
  const summaryTab = document.querySelector('.sheet-tab[data-sheet="Summary"]');
  if (summaryTab) {
    document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
    summaryTab.classList.add('active');
    renderSheet('Summary', null, summaryTab);
  }
}

// Flush pending Building Info save on page unload or visibility-change
window.addEventListener('beforeunload', () => {
  if (_biSaveTimer) { clearTimeout(_biSaveTimer); _biSaveTimer = null; _biSaveNow(); }
});

function _biRenderMaint() {
  const rows = _biData.maintenance_history;
  let body = '';
  rows.forEach((r, i) => {
    const monthly = (r.shares || 0) * (r.perShare || 0);
    const annual = monthly * 12;
    const isBudget = (r.year === BY);
    const rowCls = isBudget ? 'budget-row' : '';
    const yearLabel = isBudget ? (r.year + ' Budget') : r.year;
    body += '<tr class="' + rowCls + '">';
    body += '<td class="gl">' + (i === 0 ? '4010-0000' : '') + '</td>';
    body += '<td class="label">' + (i === 0 ? 'Maintenance' : '') + '</td>';
    body += '<td class="year-label">' + yearLabel + '</td>';
    body += '<td><input class="bi-cell small" value="' + (r.shares || 0).toLocaleString() + '" onblur="_biMhUpd(' + i + ',\'shares\',this.value)" onfocus="this.select()"></td>';
    body += '<td><input class="bi-cell dec8" value="' + (r.perShare || 0).toFixed(8) + '" onblur="_biMhUpd(' + i + ',\'perShare\',this.value)" onfocus="this.select()"></td>';
    body += '<td class="derived">' + _biFmtD2(monthly) + '</td>';
    body += '<td class="derived">' + _biFmtD(annual) + '</td>';
    body += '<td><input class="bi-cell small" value="' + (r.increase || 0).toFixed(2) + '%" onblur="_biMhUpd(' + i + ',\'increase\',this.value)" onfocus="this.select()"></td>';
    body += '</tr>';
  });
  return ''
    + '<div class="bi-card">'
    +   '<div class="bi-card-header">'
    +     '<h2>Maintenance History \u2014 4010-0000</h2>'
    +     '<span class="bi-illus-chip">Illustrative Only</span>'
    +   '</div>'
    +   '<div class="bi-card-body">'
    +     '<table class="bi-mh">'
    +       '<thead><tr>'
    +         '<th style="width:120px;">G/L</th>'
    +         '<th style="width:160px;">Label</th>'
    +         '<th style="width:110px;">Year</th>'
    +         '<th>Shares</th>'
    +         '<th>Mthly $/per sh</th>'
    +         '<th>Monthly</th>'
    +         '<th>Annual</th>'
    +         '<th>Increase</th>'
    +       '</tr></thead>'
    +       '<tbody>' + body + '</tbody>'
    +     '</table>'
    +     '<div class="bi-toolbar">'
    +       '<button class="bi-btn ghost" onclick="_biMhReset()">Reset rows</button>'
    +       '<button class="bi-btn" onclick="_biMhPropagate()">Apply Increase % forward \u2192</button>'
    +     '</div>'
    +     '<div class="bi-note">Changes here do <strong>not</strong> affect the budget. This is a reference panel only \u2014 showing the maintenance pattern for context when preparing the new budget. Saved automatically.</div>'
    +   '</div>'
    + '</div>';
}

function _biMhUpd(i, field, val) {
  const n = _biNum(val);
  // FA directive 2026-05-10: short-circuit when value didn't change.
  if (_biData.maintenance_history[i][field] === n) return;
  _biData.maintenance_history[i][field] = n;
  // Re-render just the Maintenance card. Find it by header text — using
  // querySelector('.bi-page .bi-card') would grab the FIRST card on the
  // page, which is now the Building Type card (added 2026-05-09). That
  // bug caused the Building Type card to be replaced with a duplicate
  // Maintenance card on every edit.
  const cards = document.querySelectorAll('.bi-page .bi-card');
  cards.forEach(card => {
    const h = card.querySelector('h2');
    if (h && h.textContent.indexOf('Maintenance History') >= 0) {
      card.outerHTML = _biRenderMaint();
    }
  });
  _biSaveSoon();
}

function _biMhReset() {
  _biData.maintenance_history = _biDefaultMaint();
  const container = document.querySelector('.bi-page');
  if (container) _biRenderAll(container.parentElement);
  _biSaveSoon();
}

function _biMhPropagate() {
  const rows = _biData.maintenance_history;
  for (let i = 1; i < rows.length; i++) {
    const prior = rows[i - 1].perShare || 0;
    rows[i].perShare = prior * (1 + (rows[i].increase || 0) / 100);
  }
  const container = document.querySelector('.bi-page');
  if (container) _biRenderAll(container.parentElement);
  _biSaveSoon();
}

function _biRenderAmort() {
  const a = _biData.amort_config || _biDefaultAmort();
  const mode = a.mode || 'standard';
  // FA directive 2026-05-21: 3 input modes + payment + end_balance fields.
  // Layout per mode:
  //   standard:   Principal, Rate, Term required; Payment = computed
  //   by_payment: Principal, Rate, Payment required; Term = computed
  //   by_balance: Principal, Rate, Term, Payment required; End Balance = computed
  // Field "input" or "computed" classes drive visual styling. The recalc
  // function safely handles any combination \u2014 never throws.
  function _modePill(val, label, hint) {
    const active = (mode === val);
    return '<button class="bi-amort-mode-pill' + (active ? ' active' : '') + '" onclick="_biAmModeSet(\'' + val + '\')" title="' + hint + '">' + label + '</button>';
  }
  const termClass     = (mode === 'by_payment') ? 'computed' : '';
  const paymentClass  = (mode === 'standard')   ? 'computed' : '';
  const balloonClass  = (mode === 'by_balance') ? 'computed' : '';
  const termReadOnly     = (mode === 'by_payment') ? 'readonly' : '';
  const paymentReadOnly  = (mode === 'standard')   ? 'readonly' : '';
  const balloonReadOnly  = (mode === 'by_balance') ? 'readonly' : '';
  return ''
    + '<div class="bi-card">'
    +   '<div class="bi-card-header">'
    +     '<h2>Underlying Mortgage \u2014 Amortization Schedule</h2>'
    +     '<span class="bi-illus-chip">Illustrative Only</span>'
    +   '</div>'
    +   '<div class="bi-card-body">'
    +     '<div class="bi-amort-modes" style="display:flex; gap:6px; flex-wrap:wrap; margin-bottom:12px; padding:8px; background:var(--gray-50, #f9fafb); border-radius:8px;">'
    +       '<span style="font-size:11px; font-weight:600; color:var(--gray-500); align-self:center; margin-right:6px; text-transform:uppercase; letter-spacing:0.5px;">Solver mode:</span>'
    +       _modePill('standard',   'Standard',   'Enter Principal + Rate + Term. System computes Payment.')
    +       _modePill('by_payment', 'By Payment', 'Enter Principal + Rate + Payment. System computes Term.')
    +       _modePill('by_balance', 'By Balance', 'Enter Principal + Rate + Term + Payment. System computes End Balance (balloon).')
    +     '</div>'
    +     '<div class="bi-amort-params">'
    +       '<div class="field"><label>Label</label><input type="text" id="biAmLabel" value="' + (a.label || '').replace(/"/g, '&quot;') + '" onchange="_biAmUpd()"></div>'
    +       '<div class="field"><label>Start Date</label><input type="month" id="biAmStart" value="' + (a.start || '') + '" onchange="_biAmUpd()"></div>'
    +       '<div class="field"><label>Original Principal</label><input type="text" id="biAmPrincipal" value="' + (a.principal ? Number(a.principal).toLocaleString() : '') + '" onchange="_biAmUpd()" onfocus="this.select()"></div>'
    +       '<div class="field"><label>Interest Rate (% annual)</label><input type="text" id="biAmRate" value="' + (a.rate || '') + '" onchange="_biAmUpd()" onfocus="this.select()"></div>'
    +       '<div class="field"><label>Term (years) ' + (termClass ? '<span class="bi-amort-tag computed">computed</span>' : '') + '</label><input type="text" id="biAmTerm" class="' + termClass + '" ' + termReadOnly + ' value="' + (a.term || '') + '" onchange="_biAmUpd()" onfocus="this.select()"></div>'
    +       '<div class="field"><label>Payment (per period) ' + (paymentClass ? '<span class="bi-amort-tag computed">computed</span>' : '') + '</label><input type="text" id="biAmPmt" class="' + paymentClass + '" ' + paymentReadOnly + ' value="' + (a.payment ? Number(a.payment).toLocaleString() : '') + '" onchange="_biAmUpd()" onfocus="this.select()"></div>'
    +       '<div class="field"><label>End Balance (balloon) ' + (balloonClass ? '<span class="bi-amort-tag computed">computed</span>' : '') + '</label><input type="text" id="biAmEndBal" class="' + balloonClass + '" ' + balloonReadOnly + ' value="' + (a.end_balance ? Number(a.end_balance).toLocaleString() : '0') + '" onchange="_biAmUpd()" onfocus="this.select()"></div>'
    +       '<div class="field"><label>Payment Frequency</label><select id="biAmFreq" onchange="_biAmUpd()">'
    +         '<option value="12"' + (Number(a.freq) === 12 ? ' selected' : '') + '>Monthly</option>'
    +         '<option value="4"'  + (Number(a.freq) === 4  ? ' selected' : '') + '>Quarterly</option>'
    +       '</select></div>'
    +       '<div class="field"><label>Interest-Only Period <span style="font-weight:400; color:var(--gray-500); text-transform:none; letter-spacing:0;">(periods)</span></label><input type="text" id="biAmIO" value="' + (Number(a.io_months) > 0 ? a.io_months : '') + '" placeholder="0" onchange="_biAmUpd()" onfocus="this.select()" title="Number of initial periods that are interest-only. 0 = standard amortization."></div>'
    +       '<div class="field" style="display:flex; align-items:flex-end;"><button class="bi-btn primary" style="width:100%;" onclick="_biRecalcAmort()">Recalculate</button></div>'
    +       '<div class="field" style="display:flex; align-items:flex-end;"><button class="bi-btn" style="width:100%;" onclick="_biExportAmort()">Export CSV</button></div>'
    +     '</div>'
    +     '<div id="biAmStatus" class="bi-amort-status" style="margin:8px 0; padding:8px 12px; border-radius:6px; font-size:12px; display:none;"></div>'
    +     '<div class="bi-amort-summary">'
    +       '<div class="bi-summary-card"><div class="label">Periodic Payment</div><div class="value" id="biSumPmt">\u2014</div></div>'
    +       '<div class="bi-summary-card"><div class="label">Annual Debt Service</div><div class="value" id="biSumAnnual">\u2014</div></div>'
    +       '<div class="bi-summary-card"><div class="label">Total Interest (life)</div><div class="value" id="biSumInt">\u2014</div></div>'
    +       '<div class="bi-summary-card"><div class="label">Maturity Date</div><div class="value" id="biSumMat">\u2014</div></div>'
    +     '</div>'
    +     '<div class="bi-amort-scroll">'
    +       '<table class="bi-amort">'
    +         '<thead><tr>'
    +           '<th>#</th><th>Date</th><th>Beginning Balance</th><th>Payment</th><th>Interest</th><th>Principal</th><th>Ending Balance</th>'
    +         '</tr></thead>'
    +         '<tbody id="biAmBody"></tbody>'
    +       '</table>'
    +     '</div>'
    +     '<div class="bi-note">Schedule auto-generates from the parameters above. Every 12 months is highlighted for easy year reference. All inputs save automatically.</div>'
    +   '</div>'
    + '</div>';
}

// FA directive 2026-05-21: mode setter \u2014 flips the solver mode and re-renders
// just the amortization card so the right fields show as "computed" vs "input".
function _biAmModeSet(newMode) {
  const a = _biData.amort_config || (_biData.amort_config = _biDefaultAmort());
  if (a.mode === newMode) return;
  a.mode = newMode;
  // Re-render only this card to reflect the new mode's field states
  const card = document.querySelector('.bi-card .bi-card-header h2');
  if (card && card.textContent && card.textContent.includes('Amortization Schedule')) {
    card.closest('.bi-card').outerHTML = _biRenderAmort();
  } else {
    // Fallback: full re-render
    if (typeof renderBuildingInfoTab === 'function') {
      const c = document.getElementById('sheetContent');
      if (c) renderBuildingInfoTab(c);
    }
  }
  _biRecalcAmort();
  _biSaveSoon();
}

function _biAmUpd() {
  const a = _biData.amort_config || (_biData.amort_config = _biDefaultAmort());
  // FA directive 2026-05-10: read the candidate values from the DOM, then
  // short-circuit if nothing changed. Avoids re-render + save when the FA
  // tabs through fields without editing.
  // FA dir 2026-05-21: also read payment + end_balance (new fields).
  const pmtEl = document.getElementById('biAmPmt');
  const endBalEl = document.getElementById('biAmEndBal');
  const cand = {
    label:     document.getElementById('biAmLabel').value || '',
    principal: _biNum(document.getElementById('biAmPrincipal').value),
    rate:      _biNum(document.getElementById('biAmRate').value),
    term:      _biNum(document.getElementById('biAmTerm').value),
    start:     document.getElementById('biAmStart').value || '',
    freq:      parseInt(document.getElementById('biAmFreq').value, 10) || 12,
    io_months: Math.max(0, Math.floor(_biNum(document.getElementById('biAmIO').value))),
    payment:     pmtEl ? _biNum(pmtEl.value) : (a.payment || 0),
    end_balance: endBalEl ? _biNum(endBalEl.value) : (a.end_balance || 0),
  };
  const eq = (a.label === cand.label && a.principal === cand.principal &&
              a.rate === cand.rate && a.term === cand.term &&
              a.start === cand.start && a.freq === cand.freq &&
              (Number(a.io_months) || 0) === cand.io_months &&
              (Number(a.payment) || 0) === cand.payment &&
              (Number(a.end_balance) || 0) === cand.end_balance);
  if (eq) return;
  Object.assign(a, cand);
  _biRecalcAmort();
  _biSaveSoon();
}

function _biRecalcAmort() {
  const a = _biData.amort_config || _biDefaultAmort();
  const body = document.getElementById('biAmBody');
  if (!body) return;
  const statusEl = document.getElementById('biAmStatus');
  function _setStatus(kind, msg) {
    if (!statusEl) return;
    if (!kind) { statusEl.style.display = 'none'; statusEl.innerHTML = ''; return; }
    statusEl.style.display = 'block';
    if (kind === 'ok') {
      statusEl.style.background = '#dcfce7'; statusEl.style.color = '#15803d';
      statusEl.innerHTML = '<strong>\u2713</strong> ' + msg;
    } else if (kind === 'need') {
      statusEl.style.background = '#fef3c7'; statusEl.style.color = '#92400e';
      statusEl.innerHTML = '<strong>!</strong> ' + msg;
    } else {
      statusEl.style.background = '#fee2e2'; statusEl.style.color = '#991b1b';
      statusEl.innerHTML = '<strong>\u26a0</strong> ' + msg;
    }
  }
  function _clearOutputs(msg) {
    body.innerHTML = '<tr><td colspan="7" class="bi-empty">' + msg + '</td></tr>';
    document.getElementById('biSumPmt').textContent    = '\u2014';
    document.getElementById('biSumAnnual').textContent = '\u2014';
    document.getElementById('biSumInt').textContent    = '\u2014';
    document.getElementById('biSumMat').textContent    = '\u2014';
  }

  const mode = (a.mode || 'standard');
  const principal = _biNum(a.principal);
  const annualRate = _biNum(a.rate) / 100;
  let termYears = _biNum(a.term);
  const freq = parseInt(a.freq, 10) || 12;
  const startStr = a.start || '';
  let payment = _biNum(a.payment);
  const endBalanceInput = _biNum(a.end_balance);
  const periodRate = (annualRate || 0) / freq;

  // FA dir 2026-05-21: solver picks which variable to compute from the mode.
  // ALWAYS returns a usable state \u2014 no throws. Insufficient info \u2192 yellow
  // "need more info" status, no schedule rendered.
  const needs = [];
  if (!principal)  needs.push('Original Principal');
  if (!annualRate) needs.push('Interest Rate');

  // Compute one variable based on mode
  let totalPeriods = 0;
  let endBalance = 0;
  let solverNote = '';
  if (mode === 'standard') {
    if (!termYears) needs.push('Term (years)');
    if (needs.length > 0) {
      _clearOutputs('Enter <strong>' + needs.join('</strong> and <strong>') + '</strong>.');
      _setStatus('need', 'Need: ' + needs.join(', '));
      return;
    }
    totalPeriods = Math.round(termYears * freq);
    endBalance = 0;
    // Payment computed by the existing engine below
    solverNote = 'Standard amortization \u2014 payment computed from principal, rate, term.';
  } else if (mode === 'by_payment') {
    if (!payment) needs.push('Payment (per period)');
    if (needs.length > 0) {
      _clearOutputs('Enter <strong>' + needs.join('</strong> and <strong>') + '</strong>.');
      _setStatus('need', 'Need: ' + needs.join(', '));
      return;
    }
    // n = log( M / (M - P*r) ) / log(1+r) \u2014 closed-form
    const interestOnly = principal * periodRate;
    if (payment <= interestOnly + 1e-6) {
      _clearOutputs('Payment is at or below interest-only. Loan never amortizes.');
      _setStatus('need', 'Payment must exceed interest-only of $' + interestOnly.toFixed(2) + '/period.');
      return;
    }
    const n_raw = Math.log(payment / (payment - principal * periodRate)) / Math.log(1 + periodRate);
    totalPeriods = Math.ceil(n_raw);
    termYears = totalPeriods / freq;
    a.term = Math.round(termYears * 100) / 100; // round to 2 decimals
    // Reflect computed term in the input
    const termEl = document.getElementById('biAmTerm');
    if (termEl) termEl.value = a.term;
    endBalance = 0;
    solverNote = 'Term computed from payment: ' + termYears.toFixed(2) + ' years (' + totalPeriods + ' periods).';
  } else if (mode === 'by_balance') {
    if (!termYears) needs.push('Term (years)');
    if (!payment) needs.push('Payment (per period)');
    if (needs.length > 0) {
      _clearOutputs('Enter <strong>' + needs.join('</strong> and <strong>') + '</strong>.');
      _setStatus('need', 'Need: ' + needs.join(', '));
      return;
    }
    totalPeriods = Math.round(termYears * freq);
    // B = P*(1+r)^n - M * ((1+r)^n - 1) / r
    if (periodRate > 0) {
      const growth = Math.pow(1 + periodRate, totalPeriods);
      endBalance = principal * growth - payment * (growth - 1) / periodRate;
    } else {
      endBalance = principal - payment * totalPeriods;
    }
    a.end_balance = Math.round(endBalance * 100) / 100;
    const endBalEl = document.getElementById('biAmEndBal');
    if (endBalEl) endBalEl.value = a.end_balance.toLocaleString();
    if (endBalance < -1) {
      _setStatus('warn', 'Payment is higher than needed \u2014 schedule overpays by $' + Math.abs(endBalance).toFixed(2) + ' at maturity.');
    } else {
      solverNote = 'End balance computed: $' + endBalance.toFixed(2) + ' balloon at maturity.';
    }
  } else {
    _clearOutputs('Unknown solver mode.');
    _setStatus('warn', 'Unknown solver mode: ' + mode);
    return;
  }

  // Need start date for schedule display. If missing, generate schedule but
  // use a placeholder start (today). FA can add real start later.
  let effectiveStart = startStr;
  if (!effectiveStart) {
    const now = new Date();
    effectiveStart = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
  }
  // Update a.start in memory only if user hasn't set one \u2014 don't override FA's choice
  const startStrUsed = effectiveStart;

  if (solverNote && statusEl && statusEl.style.display === 'none') {
    _setStatus('ok', solverNote);
  } else if (!solverNote) {
    _setStatus(null);
  }
  // FA directive 2026-05-11: I/O support. Clamp for safety.
  const ioPeriods = Math.max(0, Math.min(totalPeriods, Math.floor(Number(a.io_months) || 0)));
  const isPureIO = ioPeriods >= totalPeriods;
  const isHybrid = ioPeriods > 0 && ioPeriods < totalPeriods;
  const ioPmt = principal * periodRate;  // interest-only payment
  const amortPeriods = totalPeriods - ioPeriods;
  // FA dir 2026-05-21: amortPmt depends on solver mode.
  //   standard  : computed via PMT formula (end balance = 0)
  //   by_payment: user-supplied payment (term was just computed to fit it)
  //   by_balance: user-supplied payment (end balance was just computed)
  let amortPmt;
  if (mode === 'standard') {
    amortPmt = amortPeriods > 0
      ? principal * periodRate / (1 - Math.pow(1 + periodRate, -amortPeriods))
      : 0;
  } else {
    amortPmt = payment;
  }
  // Suppress the "force last period to zero" rounding-fix when there's a
  // legitimate balloon at maturity (by_balance mode with non-zero residual).
  const allowBalloonResidual = (mode === 'by_balance' && Math.abs(endBalance) > 0.5);
  const parts = startStrUsed.split('-');
  const sy = parseInt(parts[0], 10);
  const sm = parseInt(parts[1], 10);
  const startDate = new Date(sy, (sm || 1) - 1, 1);
  const monthsPerPeriod = 12 / freq;
  let balance = principal;
  let totalInterest = 0;
  let html = '';
  let lastDate = '';
  let firstAmortPmt = null;
  for (let i = 1; i <= totalPeriods; i++) {
    const beg = balance;
    const interest = beg * periodRate;
    const inIOPhase = i <= ioPeriods;
    let pmt, principalPaid;
    if (inIOPhase) {
      pmt = ioPmt;
      principalPaid = 0;
      // Balloon at maturity for pure I/O: last period repays full balance.
      if (isPureIO && i === totalPeriods) {
        principalPaid = beg;
        pmt = interest + beg;
      }
    } else {
      pmt = amortPmt;
      principalPaid = pmt - interest;
      // FA dir 2026-05-21: skip the rounding-fix on the last period when a
      // balloon residual is expected (by_balance mode). Otherwise force the
      // balance to zero to kill rounding drift.
      if (i === totalPeriods && !allowBalloonResidual) principalPaid = beg;
      if (firstAmortPmt === null) firstAmortPmt = pmt;
    }
    const end = beg - principalPaid;
    totalInterest += interest;
    const d = new Date(startDate);
    d.setMonth(d.getMonth() + (i - 1) * monthsPerPeriod);
    const dateStr = d.toLocaleDateString('en-US', { year: 'numeric', month: 'short' });
    lastDate = dateStr;
    const isYearEnd = (freq === 12 && i % 12 === 0) || (freq === 4 && i % 4 === 0);
    const isBalloon = isPureIO && i === totalPeriods;
    const isRecastFirst = isHybrid && i === ioPeriods + 1;
    let cls = isYearEnd ? 'year-end' : '';
    if (isBalloon) cls += ' balloon-row';
    if (isRecastFirst) cls += ' recast-row';
    html += '<tr class="' + cls.trim() + '">';
    html += '<td>' + i + (inIOPhase ? ' <span style="font-size:9px; color:var(--gray-500); font-weight:600;">I/O</span>' : '') + (isBalloon ? ' <span style="font-size:9px; color:#b45309; font-weight:700;">BALLOON</span>' : '') + (isRecastFirst ? ' <span style="font-size:9px; color:#0369a1; font-weight:700;">RECAST</span>' : '') + '</td>';
    html += '<td>' + dateStr + '</td>';
    html += '<td>' + _biFmtD(beg) + '</td>';
    html += '<td>' + _biFmtD2(pmt) + '</td>';
    html += '<td>' + _biFmtD2(interest) + '</td>';
    html += '<td>' + _biFmtD2(principalPaid) + '</td>';
    html += '<td>' + _biFmtD(end) + '</td>';
    html += '</tr>';
    balance = end;
    if (balance < 0.01) balance = 0;
  }
  body.innerHTML = html;
  // Summary card: show I/O payment + amortizing payment for hybrid, or
  // just one for the standard / pure-I/O cases.
  let pmtLabel;
  if (isPureIO) {
    pmtLabel = _biFmtD2(ioPmt) + ' <span style="font-size:10px; color:var(--gray-500); font-weight:500;">(I/O, balloon $' + Math.round(principal).toLocaleString() + ' at maturity)</span>';
  } else if (isHybrid) {
    pmtLabel = _biFmtD2(ioPmt) + ' <span style="font-size:10px; color:var(--gray-500); font-weight:500;">I/O for ' + ioPeriods + 'p, then ' + _biFmtD2(firstAmortPmt || amortPmt) + '/p</span>';
  } else {
    pmtLabel = _biFmtD2(amortPmt);
  }
  document.getElementById('biSumPmt').innerHTML      = pmtLabel;
  // Annual Debt Service: for pure I/O, amortPmt=0 (because amortPeriods=0),
  // so use ioPmt × freq instead. For hybrid, the amortizing payment (post-
  // recast) is the steady-state outflow most years. For standard amort,
  // both ioPmt and amortPmt would give similar numbers but amortPmt is
  // the right one.
  const annualPmt = isPureIO ? ioPmt : amortPmt;
  document.getElementById('biSumAnnual').textContent = _biFmtD(annualPmt * freq);
  document.getElementById('biSumInt').textContent    = _biFmtD(totalInterest);
  document.getElementById('biSumMat').textContent    = lastDate;
}

function _biExportAmort() {
  const a = _biData.amort_config || {};
  const body = document.getElementById('biAmBody');
  if (!body || body.rows.length === 0) { alert('Generate the schedule first.'); return; }
  const rows = [['#','Date','Beginning Balance','Payment','Interest','Principal','Ending Balance']];
  for (let i = 0; i < body.rows.length; i++) {
    const cells = body.rows[i].cells;
    if (cells.length !== 7) continue;
    const out = [];
    for (let j = 0; j < cells.length; j++) {
      out.push(cells[j].textContent.replace(/[$,]/g, ''));
    }
    rows.push(out);
  }
  const csv = rows.map(r => r.map(v => '"' + String(v).replace(/"/g, '""') + '"').join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = 'amort_' + entityCode + '_' + (a.label || 'mortgage').replace(/[^a-z0-9]+/gi, '_') + '.csv';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

// Parse a displayed dollar value like "$1,234" or "-$500" back to a number
function parseDollar(s) {
  if (typeof s !== 'string') return parseFloat(s) || 0;
  const isNeg = /^\s*\(.*\)\s*$/.test(s);
  const val = parseFloat(s.replace(/[$,\s()]/g, '')) || 0;
  return isNeg ? -val : val;
}

// cellBlur: user finished editing a regular dollar cell — reformat and save
function cellBlur(el) {
  // FA directive 2026-05-10: skip save when the FA only clicked in/out
  // without changing anything. Prevents stamping "edited" state + revision rows.
  if (_isUnchangedInput(el)) return;
  const raw = parseDollar(el.value);
  el.dataset.raw = Math.round(raw);
  el.value = fmt(raw);
  const gl = el.dataset.gl, field = el.dataset.field;
  faLineChanged(gl, field, raw);
}

// ── Excel-style Tab navigation ──────────────────────────────────────────
// Tab accepts the current cell (via blur → save), then moves focus to the
// next editable cell in document order within the active sheet. Shift+Tab
// moves backward. Stops at end-of-sheet (no wrap). Skips formula-display
// cells (.cell-fx) since those open the formula bar on focus.
function _gridTabNavigate(e) {
  if (e.key !== 'Tab') return;
  const t = e.target;
  if (!t || !t.matches) return;
  // Only hijack when focus is inside a grid data cell input
  if (!t.matches('input.cell, input.cell-pct, input.cell-notes, input.num-input')) return;
  // Don't interfere with formula-display cells (they open formula bar)
  if (t.classList.contains('cell-fx')) return;
  // Find the scroll container for the active grid
  const container = t.closest('.fa-grid-scroll, .prgl-scroll, .grid-container, #sumTable');
  if (!container) return;
  // Collect all eligible editable inputs in document order
  const nodes = container.querySelectorAll(
    'input.cell:not(.cell-fx):not([readonly]):not([disabled]),' +
    'input.cell-pct:not([readonly]):not([disabled]),' +
    'input.cell-notes:not([readonly]):not([disabled]),' +
    'input.num-input:not([readonly]):not([disabled])'
  );
  const all = [];
  for (let i = 0; i < nodes.length; i++) {
    const el = nodes[i];
    // Skip hidden (display:none ancestor or offsetParent null)
    if (el.offsetParent === null) continue;
    all.push(el);
  }
  const idx = all.indexOf(t);
  if (idx === -1) return;
  const dir = e.shiftKey ? -1 : 1;
  const nextIdx = idx + dir;
  // Stop at end-of-sheet (no wrap)
  if (nextIdx < 0 || nextIdx >= all.length) {
    e.preventDefault();
    return;
  }
  e.preventDefault();
  // Blur current (fires cellBlur / pctCellBlur / sumCellBlur → save)
  t.blur();
  const next = all[nextIdx];
  next.focus();
  if (typeof next.select === 'function') next.select();
}
document.addEventListener('keydown', _gridTabNavigate, true);

// QA fix 7 (2026-07-03): Excel muscle memory - Enter commits the cell edit
// (blur triggers the existing save path). Blur-only commits silently lost
// edits when users pressed Enter and navigated away.
function _gridEnterCommits(e) {
  if (e.key !== 'Enter') return;
  const t = e.target;
  if (!t || !t.matches) return;
  if (!t.matches('input.cell, input.cell-pct, input.cell-notes, input.num-input')) return;
  if (t.classList.contains('cell-fx') || t.readOnly || t.disabled) return;
  e.preventDefault();
  t.blur();
}
document.addEventListener('keydown', _gridEnterCommits, true);

// Track the currently selected formula cell
let _activeFxCell = null;
let _formulaBarOriginal = '';  // track original value to detect changes
let _formulaBarUndo = null;    // one-level undo: {gl, field, formula, raw, override, proposedFormula, value, badgeText, badgeBg, badgeColor, badgeBorder}

// ── Safe math evaluator (no eval) ──────────────────────────────────────
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

// ── Show/hide formula bar buttons ──────────────────────────────────────
function _showFormulaButtons(show, hasFormula) {
  const ids = ['faFormulaPreview','faFormulaAccept','faFormulaCancel'];
  ids.forEach(id => { const el = document.getElementById(id); if (el) el.style.display = show ? 'inline-block' : 'none'; });
  const clearBtn = document.getElementById('faFormulaClear');
  if (clearBtn) clearBtn.style.display = (show && hasFormula) ? 'inline-block' : 'none';
  // Show undo only when NOT in active edit mode and an undo is available
  const undoBtn = document.getElementById('faFormulaUndo');
  if (undoBtn) undoBtn.style.display = (!show && _formulaBarUndo) ? 'inline-block' : 'none';
}

// ── fxCellFocus: populate the formula bar when clicking a formula cell ─
function fxCellFocus(el) {
  // Clear undo if switching to a different cell
  if (_activeFxCell && _activeFxCell !== el && _formulaBarUndo) {
    _formulaBarUndo = null;
    const undoBtn = document.getElementById('faFormulaUndo');
    if (undoBtn) undoBtn.style.display = 'none';
  }
  _activeFxCell = el;
  const bar = document.getElementById('faFormulaBar');
  const label = document.getElementById('faFormulaLabel');
  if (!bar || !label) return;
  bar.readOnly = false;

  const field = el.dataset.field;
  const fieldLabel = field === 'proposed_budget' ? 'Proposed Budget' :
                     field === 'estimate_override' ? 'Estimate' :
                     field === 'forecast_override' ? 'Forecast' :
                     field === 'variance' ? '$ Variance' :
                     field === 'pct_change' ? '% Change' : field;
  label.textContent = el.dataset.gl + ' / ' + fieldLabel;
  label.style.display = 'inline';
  bar.style.display = 'block';

  if (field === 'proposed_budget' && el.dataset.proposedFormula) {
    bar.value = el.dataset.proposedFormula;
  } else if (el.dataset.userFormula) {
    // FA dir 2026-05-17: user typed a formula here previously (e.g.
    // "=300*12*4"). Show the formula again so they can edit it (change
    // 4 → 3) rather than retype. Separate from data-formula which holds
    // the auto-computed formula hint.
    bar.value = el.dataset.userFormula;
  } else if (el.dataset.override === 'true') {
    bar.value = el.dataset.raw || '';
  } else {
    bar.value = el.dataset.formula || '';
  }
  _formulaBarOriginal = bar.value;
  const isReadOnly = field === 'variance' || field === 'pct_change' || el.dataset.readonly === 'true';
  if (isReadOnly) {
    _showFormulaButtons(false, false);
    bar.readOnly = true;
  } else {
    const hasStoredFormula = !!(el.dataset.proposedFormula);
    _showFormulaButtons(true, hasStoredFormula);
    bar.readOnly = false;
  }
  formulaBarPreview();

  el.style.border = '2px solid var(--blue)';
  el.style.borderRadius = '4px';
  el.style.background = '#ecfdf5';

  if (!isReadOnly) {
    bar.focus({ preventScroll: true });
    bar.setSelectionRange(bar.value.length, bar.value.length);
  }
}

// fxCellBlur: just restore visual styling (editing now happens via Accept)
function fxCellBlur(el) {
  setTimeout(() => {
    const bar = document.getElementById('faFormulaBar');
    if (document.activeElement === bar) return;
    if (_activeFxCell === el) {
      el.style.border = '';
      el.style.borderRadius = '';
      el.style.background = '';
    }
  }, 100);
}

// ── fxSubtotalFocus: formula bar for subtotal/total row cells ──────────
// FA dir 2026-05-19: subtotal cells now editable like other formula tabs.
// Tracks the currently-focused subtotal cell so formulaBarAccept can route
// to the subtotal save path instead of the line-cell save path.
let _activeSubtotalCell = null;

function fxSubtotalFocus(td) {
  const bar = document.getElementById('faFormulaBar');
  const label = document.getElementById('faFormulaLabel');
  if (!bar || !label) return;
  // Clear any active GL-row fx cell
  if (_activeFxCell) {
    _activeFxCell.style.border = '';
    _activeFxCell.style.borderRadius = '';
    _activeFxCell.style.background = '';
    _activeFxCell = null;
  }
  _activeSubtotalCell = td;
  const row = td.closest('tr');
  const rowId = row ? row.id : '';
  const col = td.dataset.col;
  // Row label from first cell text
  let rowLabel = 'Total';
  if (row) { const fc = row.querySelector('td'); if (fc) rowLabel = fc.textContent.trim(); }
  const colLabels = {prior:'Prior Year', ytd:'YTD Actual', accrual:'Accrual Adj', unpaid:'Unpaid Bills', estimate:'Estimate', forecast:'12 Mo Forecast', budget:'Curr Budget', proposed:'Proposed', variance:'$ Variance', pctchange:'% Change'};
  label.textContent = rowLabel + ' / ' + (colLabels[col] || col);
  label.style.display = 'inline';
  bar.style.display = 'block';
  // Gather GL codes for this row
  const colPrefix = {prior:'pr_', ytd:'ytd_', accrual:'acc_', unpaid:'unp_', estimate:'est_', forecast:'fcst_', budget:'bud_', proposed:'prop_'};
  let glCodes = [];
  if (rowId === 'faSheetTotal') {
    document.querySelectorAll('tr[data-gl]').forEach(r => { if (r.style.display !== 'none') glCodes.push(r.dataset.gl); });
  } else if (rowId.startsWith('subtotal_')) {
    const key = rowId.replace('subtotal_', '');
    glCodes = (window._catGroupGLs || {})[key] || [];
  }
  const _fxDerived = (col === 'variance' || col === 'pctchange');
  if (_fxDerived) {
    // Derived columns must show the NUMBERS, not a text label. FA variance
    // semantics (2026-06): Proposed − Curr Budget, summed across this row's GLs.
    let pSum = 0, bSum = 0;
    glCodes.forEach(gl => {
      const pe = document.getElementById('prop_' + gl); if (pe) pSum += parseFloat(pe.dataset.raw) || 0;
      const be = document.getElementById('bud_' + gl);  if (be) bSum += parseFloat(be.dataset.raw) || 0;
    });
    const v = pSum - bSum;
    if (col === 'variance') {
      // Valid Excel: =Proposed-Budget (single =, ASCII -, no $/commas, no trailing result)
      bar.value = sumExcelExpr([pSum, -bSum]) || '=0';
    } else {
      // Valid Excel: =(Proposed-Budget)/Budget (ratio; the cell shows the % via format)
      bar.value = bSum ? ('=(' + (sumExcelExpr([pSum, -bSum]) || '=0').slice(1) + ')/' + String(Math.round(bSum))) : '';
    }
  } else {
    const pfx = colPrefix[col];
    if (pfx && glCodes.length) {
      // Valid Excel: =a+b+c (raw integers, ASCII +/-, no trailing total). Reuses the
      // shared sumExcelExpr so the FA tabs and Summary tab format identically.
      const vals = glCodes.map(gl => { const el = document.getElementById(pfx + gl); return el ? (parseFloat(el.dataset.raw) || 0) : 0; });
      bar.value = sumExcelExpr(vals) || ('=' + String(Math.round(parseFloat(td.dataset.raw) || 0)));
    } else {
      bar.value = '=' + String(Math.round(parseFloat(td.dataset.raw) || 0));
    }
  }
  _formulaBarOriginal = bar.value;
  // FA dir 2026-05-19: make subtotal cells editable like line cells. Was
  // bar.readOnly=true + buttons hidden, which meant the FA could only LOOK
  // at the formula. Now they can type an override (e.g. "=5800000") and
  // Accept to lock it in. Override stored per-entity in budget.assumptions_json
  // under "sheet_subtotal_overrides[rowId][col]".
  // If the cell currently has a saved override, show that instead of the
  // computed sum formula so the FA can edit their previous value.
  const savedOverride = td.dataset.overrideFormula || td.dataset.overrideValue;
  if (savedOverride) {
    bar.value = td.dataset.overrideFormula
      ? td.dataset.overrideFormula
      : ('=' + String(Math.round(parseFloat(td.dataset.overrideValue) || 0)));
  }
  if (_fxDerived) {
    // Variance / % change are computed from Proposed and Budget — read-only,
    // matching the line-cell variance behaviour. No override path for them.
    bar.readOnly = true;
    _showFormulaButtons(false, false);
  } else {
    bar.readOnly = false;
    _showFormulaButtons(true, !!savedOverride);
  }
  // Highlight clicked cell
  td.style.outline = '2px solid var(--blue)';
  td.style.outlineOffset = '-2px';
  td.style.borderRadius = '4px';
  // No auto-cleanup on outside click — the FA needs to explicitly Accept
  // or Cancel via the formula bar buttons. Auto-blur was eating typed
  // changes before the save fired.
}

// ── Accept handler for subtotal / sheet-total overrides (FA dir 2026-05-19)
// Mirrors formulaBarAccept's logic but routes to the subtotal save endpoint.
// Stores per-cell overrides in budget.assumptions_json under the key
// "sheet_subtotal_overrides[rowId][col]". Empty input clears the override.
async function subtotalAcceptFormula() {
  const td = _activeSubtotalCell;
  const bar = document.getElementById('faFormulaBar');
  if (!td || !bar) return;
  const typed = bar.value.trim();
  const row = td.closest('tr');
  const rowId = row ? row.id : '';
  const col = td.dataset.col;
  if (!rowId || !col) {
    alert('Cannot save: missing row id or column');
    return;
  }
  // Parse the typed value
  let value = null;
  let formula = null;
  if (typed === '' || typed === '—') {
    // Clear override — fall back to computed sum
    value = null;
    formula = null;
  } else if (typed.startsWith('=')) {
    const result = safeEvalFormula(typed);
    if (result === null) {
      alert('Invalid formula');
      return;
    }
    value = Math.round(result);
    formula = typed;
  } else {
    const num = parseFloat(typed.replace(/[$,]/g, ''));
    if (isNaN(num)) {
      alert('Invalid number');
      return;
    }
    value = Math.round(num);
    formula = null;
  }
  // Optimistic UI update
  const span = td.querySelector('.sub-val');
  if (value === null) {
    // Restore computed value from data-raw (the original sum)
    const computed = parseFloat(td.dataset.raw) || 0;
    if (span) span.textContent = fmt(computed);
    td.dataset.overrideValue = '';
    td.dataset.overrideFormula = '';
    td.style.outline = '';
    td.style.borderRadius = '';
    if (span) { span.style.color = ''; span.style.fontWeight = ''; }
  } else {
    if (span) {
      span.textContent = fmt(value);
      span.style.color = 'var(--blue, #1d4ed8)';
      span.style.fontWeight = '700';
      span.title = 'FA override (was ' + fmt(parseFloat(td.dataset.raw) || 0) + ')';
    }
    td.dataset.overrideValue = String(value);
    if (formula) td.dataset.overrideFormula = formula;
    else td.dataset.overrideFormula = '';
  }
  // Persist to backend
  try {
    const resp = await fetch('/api/sheet-subtotal-override/' + encodeURIComponent(entityCode), {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({row_id: rowId, col: col, value: value, formula: formula}),
    });
    if (!resp.ok) {
      const errText = await resp.text();
      alert('Save failed (' + resp.status + '): ' + errText.slice(0, 200));
      return;
    }
  } catch (e) {
    alert('Save error: ' + e.message);
  }
  // Close formula bar editing state
  bar.readOnly = true;
  _showFormulaButtons(false, false);
  td.style.outline = '';
  td.style.outlineOffset = '';
  td.style.borderRadius = '';
  _activeSubtotalCell = null;
}

// Cancel — revert formula bar to its original value, clear active state
function subtotalCancelFormula() {
  const td = _activeSubtotalCell;
  const bar = document.getElementById('faFormulaBar');
  if (td) {
    td.style.outline = '';
    td.style.outlineOffset = '';
    td.style.borderRadius = '';
  }
  if (bar) {
    bar.value = _formulaBarOriginal || '';
    bar.readOnly = true;
  }
  _showFormulaButtons(false, false);
  _activeSubtotalCell = null;
}

// ── Formula bar live preview ───────────────────────────────────────────
function formulaBarPreview() {
  const bar = document.getElementById('faFormulaBar');
  const preview = document.getElementById('faFormulaPreview');
  if (!bar || !preview || !_activeFxCell) return;

  const typed = bar.value.trim();
  if (!typed) {
    preview.style.display = 'none';
    const hadFormula = !!_activeFxCell.dataset.proposedFormula;
    _showFormulaButtons(hadFormula, hadFormula);
    return;
  }

  const result = safeEvalFormula(typed);
  const isChanged = typed !== _formulaBarOriginal;
  if (result !== null) {
    preview.textContent = '= ' + fmt(result);
    preview.style.color = isChanged ? '#059669' : 'var(--green)';
  } else if (/^[\d$,.\-\s]+$/.test(typed)) {
    const num = parseDollar(typed);
    preview.textContent = '= ' + fmt(num);
    preview.style.color = isChanged ? '#2563eb' : 'var(--blue)';
  } else {
    preview.textContent = 'Invalid formula';
    preview.style.color = 'var(--red)';
  }
  preview.style.display = 'inline-block';

  // QA fix 6 (2026-07-03): a Proposed cell can DISPLAY a stored value while
  // the bar previews the would-be formula (two different derivations).
  // Accepting would silently change the number - warn before it happens.
  var divergeEl = document.getElementById('faFxDivergeWarn');
  var showDiverge = false;
  if (_activeFxCell && _activeFxCell.dataset.field === 'proposed_budget' && !isChanged) {
    var rawStored = parseFloat(_activeFxCell.dataset.raw);
    if (result !== null && isFinite(rawStored) && Math.abs(result - rawStored) > 0.5) {
      if (!divergeEl) {
        divergeEl = document.createElement('span');
        divergeEl.id = 'faFxDivergeWarn';
        divergeEl.style.cssText = 'margin-left:10px;font-size:11px;font-weight:600;color:#92400e;background:#fef3c7;border:1px solid #fcd34d;border-radius:4px;padding:2px 8px;';
        preview.parentNode.insertBefore(divergeEl, preview.nextSibling);
      }
      divergeEl.textContent = 'Cell shows stored ' + fmt(rawStored) + '; Accept applies this formula = ' + fmt(result);
      divergeEl.style.display = 'inline-block';
      showDiverge = true;
    }
  }
  if (divergeEl && !showDiverge) divergeEl.style.display = 'none';

  const hasStoredFormula = !!_activeFxCell.dataset.proposedFormula;
  _showFormulaButtons(true, hasStoredFormula || isChanged);
}

// ── Accept: commit formula/value to cell and save ──────────────────────
function formulaBarAccept() {
  const bar = document.getElementById('faFormulaBar');
  if (!bar) return;

  // FA dir 2026-05-19: route to subtotal-save path when the active cell is
  // a subtotal/sheet-total cell instead of a per-line cell.
  if (_activeSubtotalCell) {
    return subtotalAcceptFormula();
  }

  if (!_activeFxCell) return;

  const el = _activeFxCell;
  const typed = bar.value.trim();
  const gl = el.dataset.gl, field = el.dataset.field;

  // Stash undo state before changing anything
  const badge = el.parentElement.querySelector('.fa-fx');
  _formulaBarUndo = {
    gl: gl, field: field, cellId: el.id,
    formula: _formulaBarOriginal,
    raw: el.dataset.raw || '',
    override: el.dataset.override || 'false',
    proposedFormula: el.dataset.proposedFormula || '',
    value: el.value,
    badgeText: badge ? badge.textContent : '',
    badgeBg: badge ? badge.style.background : '',
    badgeColor: badge ? badge.style.color : '',
    badgeBorder: badge ? badge.style.borderColor : '',
  };

  if (field === 'proposed_budget') {
    const formulaResult = safeEvalFormula(typed);
    if (formulaResult !== null && (typed.startsWith('=') || /[+\-*\/()]/.test(typed))) {
      const rounded = Math.round(formulaResult);
      el.dataset.raw = rounded;
      el.dataset.proposedFormula = typed.startsWith('=') ? typed : '=' + typed;
      el.dataset.override = 'true';
      el.value = fmt(formulaResult);
      const badge = el.parentElement.querySelector('.fa-fx');
      if (badge) { badge.textContent = 'fx'; badge.style.background = '#dbeafe'; badge.style.color = 'var(--blue)'; badge.style.borderColor = 'var(--blue)'; }
      faAutoSave(gl, 'proposed_budget', rounded);
      faAutoSave(gl, 'proposed_formula', el.dataset.proposedFormula);
      faRepaintVarPct(gl);   // FA #B6: refresh $ Var / % Chg immediately
      faUpdateSheetTotals();
    } else {
      const num = parseDollar(typed);
      if (!isNaN(num)) {
        el.dataset.raw = Math.round(num);
        el.dataset.override = 'true';
        el.dataset.proposedFormula = '';
        el.value = fmt(num);
        const badge = el.parentElement.querySelector('.fa-fx');
        if (badge) { badge.textContent = '✎'; badge.style.background = '#f97316'; badge.style.color = '#fff'; badge.style.borderColor = '#ea580c'; }
        faAutoSave(gl, 'proposed_budget', Math.round(num));
        faAutoSave(gl, 'proposed_formula', null);
        faRepaintVarPct(gl);   // FA #B6: refresh $ Var / % Chg immediately
        faUpdateSheetTotals();
      }
    }
  } else {
    const formulaResult = safeEvalFormula(typed);
    const numericVal = parseDollar(typed);
    if (formulaResult !== null && (typed.startsWith('=') || /[+\-*\/()]/.test(typed))) {
      const rounded = Math.round(formulaResult);
      el.dataset.raw = rounded;
      el.dataset.override = 'true';
      el.value = fmt(formulaResult);
      const formulaStr = typed.startsWith('=') ? typed : '=' + typed;
      // FA dir 2026-05-17: keep the auto-computed `data-formula` intact (used
      // as a hint when override is cleared). Store the FA's typed formula in
      // a separate attribute so fxCellFocus can repopulate the bar with it.
      el.dataset.userFormula = formulaStr;
      const badge = el.parentElement.querySelector('.fa-fx');
      if (badge) { badge.textContent = 'fx✎'; badge.style.background = '#dbeafe'; badge.style.color = 'var(--blue)'; badge.style.borderColor = 'var(--blue)'; }
      faLineChanged(gl, field, formulaResult);
      faAutoSave(gl, field, rounded);
      // FA dir 2026-05-17: persist the formula string so refresh / re-click
      // restores the expression. estimate → estimate_formula, forecast → forecast_formula.
      if (field === 'estimate' || field === 'estimate_override') {
        faAutoSave(gl, 'estimate_formula', formulaStr);
      } else if (field === 'forecast' || field === 'forecast_override') {
        faAutoSave(gl, 'forecast_formula', formulaStr);
      }
    } else if (typed !== '' && !isNaN(numericVal) && /^[\d$,.\-\s]+$/.test(typed)) {
      el.dataset.raw = Math.round(numericVal);
      el.dataset.override = 'true';
      el.value = fmt(numericVal);
      delete el.dataset.userFormula;   // plain number clears any saved formula
      const badge = el.parentElement.querySelector('.fa-fx');
      if (badge) { badge.textContent = '✎'; badge.style.background = '#f97316'; badge.style.color = '#fff'; badge.style.borderColor = '#ea580c'; }
      faLineChanged(gl, field, numericVal);
      faAutoSave(gl, field, Math.round(numericVal));
      // Plain number — clear any prior formula for this field.
      if (field === 'estimate' || field === 'estimate_override') {
        faAutoSave(gl, 'estimate_formula', null);
      } else if (field === 'forecast' || field === 'forecast_override') {
        faAutoSave(gl, 'forecast_formula', null);
      }
    } else if (typed === '' || typed.toLowerCase() === 'auto' || typed.toLowerCase() === 'formula') {
      el.dataset.override = 'false';
      delete el.dataset.userFormula;
      const badge = el.parentElement.querySelector('.fa-fx');
      if (badge) { badge.textContent = 'fx'; badge.style.background = ''; badge.style.color = ''; badge.style.borderColor = ''; }
      faLineChanged(gl, field === 'estimate_override' ? '__recalc_estimate' :
                         field === 'forecast_override' ? '__recalc_forecast' : field, null);
      faAutoSave(gl, field, null);
      // Empty / auto — clear formula too.
      if (field === 'estimate' || field === 'estimate_override') {
        faAutoSave(gl, 'estimate_formula', null);
      } else if (field === 'forecast' || field === 'forecast_override') {
        faAutoSave(gl, 'forecast_formula', null);
      }
    }
  }

  el.style.border = '2px solid var(--green)';
  el.style.background = '#ecfdf5';
  const preview = document.getElementById('faFormulaPreview');
  if (preview) {
    preview.textContent = '✓ Accepted';
    preview.style.color = 'var(--green)';
    preview.style.display = 'inline-block';
  }
  _showFormulaButtons(false, false);
  _formulaBarOriginal = bar.value.trim();
  // Show undo button
  const undoBtn = document.getElementById('faFormulaUndo');
  if (undoBtn && _formulaBarUndo) undoBtn.style.display = 'inline-block';

  // Payroll-specific hook: if cell is in prGLContent, sync _payrollGLLines + re-render
  if (el && typeof el.closest === 'function' && el.closest('#prGLContent') && typeof payrollCellEdited === 'function') {
    payrollCellEdited(el, gl, field);
  }

  setTimeout(() => {
    el.style.border = '';
    el.style.borderRadius = '';
    el.style.background = '';
    if (preview) preview.style.display = 'none';
  }, 1200);
}

// ── Cancel: revert formula bar to original ─────────────────────────────
function formulaBarCancel() {
  // FA dir 2026-05-19: also handle subtotal-cell cancel
  if (_activeSubtotalCell) {
    return subtotalCancelFormula();
  }
  const bar = document.getElementById('faFormulaBar');
  if (bar) bar.value = _formulaBarOriginal;
  _showFormulaButtons(false, false);
  const preview = document.getElementById('faFormulaPreview');
  if (preview) preview.style.display = 'none';
  if (_activeFxCell) {
    _activeFxCell.style.border = '';
    _activeFxCell.style.borderRadius = '';
    _activeFxCell.style.background = '';
  }
}

// ── Clear: remove formula, revert to auto-calc ─────────────────────────
function formulaBarClear() {
  if (!_activeFxCell) return;
  const el = _activeFxCell;
  const gl = el.dataset.gl, field = el.dataset.field;

  if (field === 'proposed_budget') {
    el.dataset.proposedFormula = '';
    el.dataset.override = 'false';
    const badge = el.parentElement.querySelector('.fa-fx');
    if (badge) { badge.textContent = 'fx'; badge.style.background = ''; badge.style.color = ''; badge.style.borderColor = ''; }
    faLineChanged(gl, '__recalc_proposed', null);
    faAutoSave(gl, 'proposed_formula', null);
  } else {
    el.dataset.override = 'false';
    const badge = el.parentElement.querySelector('.fa-fx');
    if (badge) { badge.textContent = 'fx'; badge.style.background = ''; badge.style.color = ''; badge.style.borderColor = ''; }
    faLineChanged(gl, field === 'estimate_override' ? '__recalc_estimate' :
                       field === 'forecast_override' ? '__recalc_forecast' : field, null);
    faAutoSave(gl, field, null);
  }

  const bar = document.getElementById('faFormulaBar');
  if (bar) bar.value = '';
  _showFormulaButtons(false, false);
  el.style.border = '';
  el.style.borderRadius = '';
  el.style.background = '';
}

// ── Undo: revert the last accepted formula change ────────────────────
function formulaBarUndo() {
  if (!_formulaBarUndo) return;
  const u = _formulaBarUndo;
  const el = document.getElementById(u.cellId);
  if (!el) { _formulaBarUndo = null; return; }

  // Restore cell dataset + display value
  el.value = u.value;
  el.dataset.raw = u.raw;
  el.dataset.override = u.override;
  if (u.field === 'proposed_budget') {
    el.dataset.proposedFormula = u.proposedFormula;
  }

  // Restore badge
  const badge = el.parentElement.querySelector('.fa-fx');
  if (badge) {
    badge.textContent = u.badgeText;
    badge.style.background = u.badgeBg;
    badge.style.color = u.badgeColor;
    badge.style.borderColor = u.badgeBorder;
  }

  // Restore formula bar display
  const bar = document.getElementById('faFormulaBar');
  if (bar) bar.value = u.formula;
  _formulaBarOriginal = u.formula;

  // Save the reverted value to the server
  if (u.field === 'proposed_budget') {
    const val = u.override === 'true' ? parseFloat(u.raw) : null;
    faAutoSave(u.gl, 'proposed_budget', val !== null ? Math.round(val) : 0);
    faAutoSave(u.gl, 'proposed_formula', u.proposedFormula || null);
    faUpdateSheetTotals();
  } else {
    const val = u.override === 'true' ? parseFloat(u.raw) : null;
    faLineChanged(u.gl, u.field, val);
    faAutoSave(u.gl, u.field, val !== null ? Math.round(val) : null);
  }

  // Flash amber to confirm undo
  el.style.border = '2px solid #c2410c';
  el.style.background = '#fff7ed';
  setTimeout(() => { el.style.border = ''; el.style.borderRadius = ''; el.style.background = ''; }, 1200);

  _formulaBarUndo = null;
  const undoBtn = document.getElementById('faFormulaUndo');
  if (undoBtn) undoBtn.style.display = 'none';
}

// formulaBarKeydown: Enter = Accept, Escape = Cancel
function formulaBarKeydown(e) {
  if (e.key === 'Enter') {
    e.preventDefault();
    formulaBarAccept();
  } else if (e.key === 'Escape') {
    e.preventDefault();
    formulaBarCancel();
  }
}

// pctCellBlur: user finished editing a percentage cell — reformat and save
function pctCellBlur(el) {
  // FA directive 2026-05-10: skip when value didn't change.
  if (_isUnchangedInput(el)) return;
  const raw = parseFloat(el.value) || 0;
  el.dataset.raw = raw.toFixed(1);
  el.value = raw.toFixed(1) + '%';
  const gl = el.dataset.gl;
  faLineChanged(gl, 'increase_pct', raw);
}

// When an input field changes, recalculate computed cells in that row and save
// FA 2026-06-17 (B6): repaint a row's $ Var + % Chg cells from the current
// proposed + budget. faLineChanged already does this for estimate/forecast/%
// edits, but a PROPOSED accept (formulaBarAccept) only touched the proposed
// cell + sheet total, so $ Var / % Chg stayed stale until the page was
// reopened. Calling this right after a proposed accept makes the whole row
// reflect the change immediately.
function faRepaintVarPct(gl) {
  const propEl = document.getElementById('prop_' + gl);
  if (!propEl) return;
  const budEl = document.getElementById('bud_' + gl);
  const proposed = parseFloat(propEl.dataset.raw) || 0;
  const budget = budEl ? (parseFloat(budEl.dataset.raw) || 0) : 0;
  const variance = proposed - budget;
  const pctChange = budget ? ((proposed - budget) / budget) : 0;
  const varEl = document.getElementById('var_' + gl);
  if (varEl) {
    varEl.value = fmt(variance);
    varEl.dataset.raw = Math.round(variance);
    varEl.dataset.formula = '= ' + fmt(proposed) + ' - ' + fmt(budget);
    varEl.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
    const varTd = varEl.closest('td');
    if (varTd) varTd.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
  }
  const pctEl = document.getElementById('pct_' + gl);
  if (pctEl) {
    pctEl.value = (pctChange * 100).toFixed(1) + '%';
    pctEl.dataset.raw = pctChange;
    pctEl.dataset.formula = '= (' + fmt(proposed) + ' - ' + fmt(budget) + ') / ' + fmt(budget);
  }
}

function faLineChanged(gl, field, value) {
  const getRaw = (id) => {
    const el = document.getElementById(id);
    return el ? parseFloat(el.dataset.raw) || 0 : 0;
  };

  if (field === 'increase_pct') {
    faAutoSave(gl, 'increase_pct', (parseFloat(value) || 0) / 100);
  } else if (field === '__recalc_estimate' || field === '__recalc_forecast' || field === '__recalc_proposed') {
    // Recalc triggers — no save needed, just recalculate below
  } else if (field === 'estimate_override' || field === 'forecast_override') {
    // Override saved by formulaBarAccept; just recalculate downstream here
  } else if (field && value !== null && value !== undefined) {
    faAutoSave(gl, field, Math.round(parseDollar(value)));
  }

  const row = document.querySelector('tr[data-gl="' + gl + '"]');
  if (!row) return;

  const ytd = getRaw('ytd_' + gl);
  const accrual = getRaw('acc_' + gl);
  const unpaid = getRaw('unp_' + gl);
  const prior = getRaw('pr_' + gl);
  const budget = getRaw('bud_' + gl);
  const incRaw = parseFloat(document.getElementById('inc_' + gl)?.dataset.raw) || 0;
  const incPct = incRaw / 100;
  const base = ytd + accrual + unpaid;

  let estimate, forecast;
  if (field === 'estimate_override' && value !== null) {
    estimate = parseFloat(value) || 0;
    forecast = ytd + accrual + unpaid + estimate;
  } else if (field === 'forecast_override' && value !== null) {
    forecast = parseFloat(value) || 0;
    estimate = forecast - (ytd + accrual + unpaid);
  } else {
    // Formula: (YTD+Accrual+Unpaid) / YTD_MONTHS * REMAINING_MONTHS
    if (YTD_MONTHS > 0) {
      estimate = (base / YTD_MONTHS) * REMAINING_MONTHS;
    } else {
      estimate = 0;
    }
    forecast = ytd + accrual + unpaid + estimate;
  }

  // Check if proposed has a user formula — if so, don't auto-recalc it
  const propEl = document.getElementById('prop_' + gl);
  const hasUserFormula = propEl && propEl.dataset.proposedFormula;
  let proposed;
  if (hasUserFormula && field !== '__recalc_proposed') {
    const evalResult = safeEvalFormula(propEl.dataset.proposedFormula);
    proposed = evalResult !== null ? evalResult : parseFloat(propEl.dataset.raw) || 0;
  } else {
    proposed = forecast * (1 + incPct);
  }

  const updateCell = (id, val, newFormula) => {
    const el = document.getElementById(id);
    if (el) {
      el.dataset.raw = Math.round(val);
      el.value = fmt(val);
      if (newFormula && el.dataset.override !== 'true') el.dataset.formula = newFormula;
    }
  };
  // Build updated formula strings: =(YTD+Accrual+Unpaid) / YTD_MONTHS * REMAINING_MONTHS
  let estFormula, estExpr;
  if (YTD_MONTHS > 0) {
    estFormula = '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
    estExpr = '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
  } else {
    estFormula = '=0';
    estExpr = '0';
  }
  const fcstFormula = '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + estExpr + ')';
  const propFormula = hasUserFormula && field !== '__recalc_proposed'
    ? propEl.dataset.proposedFormula
    : '=(' + ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + estExpr + '))*(1+' + incPct.toFixed(4) + ')';

  if (field !== 'estimate_override') updateCell('est_' + gl, estimate, estFormula);
  if (field !== 'forecast_override') updateCell('fcst_' + gl, forecast, fcstFormula);
  if (field !== 'proposed_budget') updateCell('prop_' + gl, proposed, propFormula);

  // Only auto-save proposed if there's no user formula (formula saves handled by Accept)
  if (!hasUserFormula || field === '__recalc_proposed') {
    faAutoSave(gl, 'proposed_budget', Math.round(proposed));
  }

  // FA dir 2026-06-05: $ Var = Proposed - Curr Budget; % Chg = (Proposed - Budget) / Budget.
  // (was Excel budget-vs-forecast parity — recomputed here on every cell edit.)
  const variance = proposed - budget;
  const pctChange = budget ? ((proposed - budget) / budget) : 0;
  const varEl = document.getElementById('var_' + gl);
  if (varEl) {
    varEl.value = fmt(variance);
    varEl.dataset.raw = Math.round(variance);
    varEl.dataset.formula = '= ' + fmt(proposed) + ' - ' + fmt(budget);
    varEl.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
    const varTd = varEl.closest('td');
    if (varTd) varTd.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
  }
  const pctEl = document.getElementById('pct_' + gl);
  if (pctEl) {
    pctEl.value = (pctChange * 100).toFixed(1) + '%';
    pctEl.dataset.raw = pctChange;
    pctEl.dataset.formula = '= (' + fmt(proposed) + ' - ' + fmt(budget) + ') / ' + fmt(budget);
  }

  // Recalculate sheet totals from live cell values
  faUpdateSheetTotals();
}

function faUpdateSheetTotals() {
  const raw = (id) => { const el = document.getElementById(id); return el ? parseFloat(el.dataset.raw) || 0 : 0; };

  function sumGLs(glCodes) {
    const t = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
    glCodes.forEach(gl => {
      const row = document.querySelector('tr[data-gl="' + gl + '"]');
      if (row && row.style.display === 'none') return;
      t.prior += raw('pr_' + gl);
      t.ytd += raw('ytd_' + gl);
      t.accrual += raw('acc_' + gl);
      t.unpaid += raw('unp_' + gl);
      t.estimate += raw('est_' + gl);
      t.forecast += raw('fcst_' + gl);
      t.budget += raw('bud_' + gl);
      t.proposed += raw('prop_' + gl);
    });
    return t;
  }

  function updateTotalRow(rowEl, t) {
    if (!rowEl) return;
    const v = t.proposed - t.budget;
    const p = t.budget ? ((t.proposed - t.budget) / t.budget) : 0;
    // Address cells by data-col, NOT by position. The total rows have TWO
    // leading cells (empty frozen-gl + label), but this used to assume a single
    // colspan label at cells[0], so every total was written one cell to the
    // LEFT — proposed landed in the inc% gap, the variance value landed in the
    // proposed cell, and the label got overwritten with the prior total. That
    // scrambled the row on every line edit. Every total cell carries data-col,
    // so target them directly and leave the label / spacer cells alone.
    const setCol = (col, val, isPct) => {
      const cell = rowEl.querySelector('[data-col="' + col + '"]');
      if (!cell) return null;
      const txt = isPct ? ((val * 100).toFixed(1) + '%') : fmt(val);
      const sp = cell.querySelector('.sub-val');
      if (sp) { sp.textContent = txt; cell.dataset.raw = (isPct ? val : Math.round(val)).toString(); }
      else { cell.textContent = txt; cell.dataset.raw = (isPct ? val : Math.round(val)).toString(); }
      return cell;
    };
    setCol('prior', t.prior);
    setCol('ytd', t.ytd);
    setCol('accrual', t.accrual);
    setCol('unpaid', t.unpaid);
    setCol('estimate', t.estimate);
    setCol('forecast', t.forecast);
    setCol('budget', t.budget);
    setCol('proposed', t.proposed);
    const vc = setCol('variance', v);
    if (vc) vc.style.color = v >= 0 ? 'var(--red)' : 'var(--green)';
    setCol('pctchange', p, true);
  }

  // Update category subtotal rows
  const groups = window._catGroupGLs || {};
  Object.keys(groups).forEach(key => {
    const subRow = document.getElementById('subtotal_' + key);
    if (subRow) updateTotalRow(subRow, sumGLs(groups[key]));
  });

  // Update sheet total row (all visible GL rows)
  const allGLs = [];
  document.querySelectorAll('tr[data-gl]').forEach(row => {
    if (row.style.display !== 'none') allGLs.push(row.dataset.gl);
  });
  updateTotalRow(document.getElementById('faSheetTotal'), sumGLs(allGLs));
}

let _faSavePending = {};
let _faSaveTimer = null;
// QA fix 4 (2026-07-03): the debounce body is now a named, awaitable flush so
// tab switches can flush pending edits BEFORE refetching (no lost-edit race),
// and every successful save marks the bootstrap cache dirty.
async function _faFlushSave() {
  clearTimeout(_faSaveTimer);
  _faSaveTimer = null;
  const entries = Object.entries(_faSavePending);
  if (!entries.length) return;
  const lines = entries.map(function(entry) {
    var obj = {gl_code: entry[0]};
    var fields = entry[1];
    for (var k in fields) { if (fields.hasOwnProperty(k)) obj[k] = fields[k]; }
    return obj;
  });
  _faSavePending = {};
  const indicator = document.getElementById('faSaveIndicator');
  if (indicator) indicator.textContent = 'Saving...';
  try {
    const resp = await fetch('/api/fa-lines/' + entityCode, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lines: lines})
    });
    if (!resp.ok) throw new Error('Save failed: ' + resp.status);
    window._faDataDirty = true;
    if (indicator) indicator.textContent = 'Saved';
  } catch(e) {
    if (indicator) {
      indicator.textContent = 'Save failed!';
      indicator.style.color = '#dc2626';
      setTimeout(function() { indicator.style.color = ''; }, 3000);
    }
    console.error('FA save error:', e);
  }
  setTimeout(function() { if (indicator) indicator.textContent = ''; }, 2000);
}
function faAutoSave(gl, field, value) {
  if (!_faSavePending[gl]) _faSavePending[gl] = {};
  _faSavePending[gl][field] = value;
  clearTimeout(_faSaveTimer);
  _faSaveTimer = setTimeout(function() { _faFlushSave(); }, 800);
}

// ── Ancillary Charges Backup Worksheet ───────────────────────────────
// Enabled for Income-tab GL codes in 4130-* (storage/locker/bike/gym/maids room),
// 4135-* (garage & parking), 4250-* (cable/appliances). Each GL can have its own
// "backup" worksheet of line items that justify the Col 6 number. Backup total is
// saved as BudgetLine.backup_json. Users can sync the backup total to Col 6 (clears
// any formula and writes proposed_budget) or keep them drifted.
const _ancExpanded = new Set();

function _isAncillaryGl(gl) {
  if (!gl) return false;
  return /^(4130|4135|4250)-/.test(gl);
}

function _ancGetLine(gl) {
  // Ancillary GLs always live on the Income sheet.
  const income = (typeof allSheets !== 'undefined' && allSheets && allSheets.Income) || [];
  return income.find(function(l) { return l.gl_code === gl; });
}

function _ancGetBackup(gl) {
  const line = _ancGetLine(gl);
  if (!line) return [];
  if (!Array.isArray(line.backup_json)) line.backup_json = [];
  return line.backup_json;
}

function _ancParseNum(s) {
  if (typeof s === 'number') return s;
  if (!s) return 0;
  const n = parseFloat(String(s).replace(/[$,\s%]/g, ''));
  return isNaN(n) ? 0 : n;
}

function _ancComputeLineTotal(line) {
  const qty = Number(line.qty) || 0;
  const rate = Number(line.rate) || 0;
  const annualRate = line.period === 'mo' ? rate * 12 : rate;
  const monthsFactor = (Number(line.monthsActive) || 12) / 12;
  const occFactor = (Number(line.occupancy) || 100) / 100;
  return qty * annualRate * monthsFactor * occFactor;
}

function _ancComputeBackupTotal(gl) {
  return _ancGetBackup(gl).reduce(function(s, l) { return s + _ancComputeLineTotal(l); }, 0);
}

function _ancFmtD(n) {
  if (n == null || isNaN(n)) return '$0';
  return (n < 0 ? '-$' : '$') + Math.abs(Math.round(n)).toLocaleString();
}

function _ancFmtD2(n) {
  if (n == null || isNaN(n)) return '$0.00';
  return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
}

function ancToggleDrawer(gl, ev) {
  if (ev) ev.stopPropagation();
  const row = document.querySelector('tr[data-anc-drawer="' + gl + '"]');
  if (!row) return;
  const icon = document.getElementById('anc_icon_' + gl);
  if (_ancExpanded.has(gl)) {
    _ancExpanded.delete(gl);
    row.style.display = 'none';
    row.querySelector('td').innerHTML = '';
    if (icon) icon.textContent = '+';
  } else {
    _ancExpanded.add(gl);
    row.style.display = '';
    row.querySelector('td').innerHTML = ancRenderDrawer(gl);
    if (icon) icon.textContent = '−';
  }
}

function ancRedrawDrawer(gl, focusField, focusIdx) {
  const row = document.querySelector('tr[data-anc-drawer="' + gl + '"]');
  if (!row) return;
  row.querySelector('td').innerHTML = ancRenderDrawer(gl);
  if (focusField != null && focusIdx != null) {
    const fieldMap = { label:0, qty:1, rate:2, period:3, monthsActive:4, occupancy:5 };
    const colIdx = fieldMap[focusField];
    if (colIdx != null) {
      const tr = row.querySelectorAll('table.anc-lines tbody tr')[focusIdx];
      if (tr) {
        const input = tr.children[colIdx] && tr.children[colIdx].querySelector('input, select');
        if (input) {
          input.focus();
          if (input.tagName === 'INPUT' && typeof input.setSelectionRange === 'function') {
            const len = input.value.length;
            input.setSelectionRange(len, len);
          }
        }
      }
    }
  }
}

function ancRenderDrawer(gl) {
  const line = _ancGetLine(gl);
  if (!line) return '<div class="anc-drawer"><em>Line not found.</em></div>';
  const items = _ancGetBackup(gl);
  const backupTotal = _ancComputeBackupTotal(gl);
  const col6 = Number(line.proposed_budget) || 0;
  const drift = col6 - backupTotal;
  const inSync = Math.abs(drift) < 1;
  const compareCls = inSync ? 'ok' : 'drift';
  const driftLabel = inSync
    ? '✓ In sync'
    : '⚠ Drift ' + _ancFmtD(Math.abs(drift)) + (drift > 0 ? ' (Col 6 high)' : ' (Col 6 low)');
  const syncBtn = inSync
    ? '<button class="anc-sync-btn in-sync" disabled>✓ In sync</button>'
    : '<button class="anc-sync-btn" onclick="ancSyncToCol6(\'' + gl + '\')">Sync Col 6 → ' + _ancFmtD(backupTotal) + '</button>';

  let linesHtml = '';
  if (items.length === 0) {
    linesHtml = '<tr><td colspan="8" style="text-align:center; color:var(--gray-500); padding:16px;"><em>No backup lines yet — click "+ Add line" to start.</em></td></tr>';
  } else {
    items.forEach(function(l, idx) {
      const lineTotal = _ancComputeLineTotal(l);
      const esc = (l.label || '').replace(/"/g, '&quot;');
      linesHtml += '<tr>' +
        '<td><input type="text" value="' + esc + '" onfocus="this.select()" oninput="ancUpdLine(\'' + gl + '\',' + idx + ',\'label\',this.value,false)"></td>' +
        '<td class="num"><input type="text" class="num-input" value="' + (Number(l.qty)||0) + '" onfocus="this.select()" oninput="ancUpdLine(\'' + gl + '\',' + idx + ',\'qty\',this.value,true)"></td>' +
        '<td class="num"><input type="text" class="num-input" value="' + (Number(l.rate)||0).toFixed(2) + '" onfocus="this.select()" oninput="ancUpdLine(\'' + gl + '\',' + idx + ',\'rate\',this.value,true)"></td>' +
        '<td><select onchange="ancUpdLine(\'' + gl + '\',' + idx + ',\'period\',this.value,false)">' +
          '<option value="mo"' + (l.period === 'mo' ? ' selected' : '') + '>Monthly</option>' +
          '<option value="yr"' + (l.period === 'yr' ? ' selected' : '') + '>Annual</option>' +
        '</select></td>' +
        '<td class="num"><input type="text" class="num-input" value="' + (Number(l.monthsActive)||12) + '" onfocus="this.select()" oninput="ancUpdLine(\'' + gl + '\',' + idx + ',\'monthsActive\',this.value,true)"></td>' +
        '<td class="num"><input type="text" class="num-input" value="' + (Number(l.occupancy)||100) + '" onfocus="this.select()" oninput="ancUpdLine(\'' + gl + '\',' + idx + ',\'occupancy\',this.value,true)"></td>' +
        '<td class="num anc-line-total">' + _ancFmtD2(lineTotal) + '</td>' +
        '<td><button class="anc-remove-btn" onclick="ancRemoveLine(\'' + gl + '\',' + idx + ')" title="Remove">✕</button></td>' +
      '</tr>';
    });
    linesHtml += '<tr class="anc-total-row"><td colspan="6" style="text-align:right;">Backup Total</td><td class="num">' + _ancFmtD2(backupTotal) + '</td><td></td></tr>';
  }

  const priorYear = Number(line.prior_year) || 0;
  const ytd = Number(line.ytd_actual) || 0;

  return '<div class="anc-drawer">' +
    '<h3>Backup Worksheet <span class="anc-gl-small">· ' + gl + ' ' + (line.description || '') + '</span></h3>' +
    '<div class="anc-compare-strip">' +
      '<div class="anc-compare-cell"><div class="anc-label">Prior Year Col 6</div><div class="anc-value">' + _ancFmtD(priorYear) + '</div></div>' +
      '<div class="anc-compare-cell"><div class="anc-label">YTD Actual</div><div class="anc-value">' + _ancFmtD(ytd) + '</div></div>' +
      '<div class="anc-compare-cell highlight"><div class="anc-label">Backup Total</div><div class="anc-value">' + _ancFmtD(backupTotal) + '</div></div>' +
      '<div class="anc-compare-cell ' + compareCls + '"><div class="anc-label">Current Col 6</div><div class="anc-value">' + _ancFmtD(col6) + '</div><div class="anc-hint">' + driftLabel + '</div></div>' +
    '</div>' +
    '<table class="anc-lines">' +
      '<colgroup><col style="width:28%"><col style="width:10%"><col style="width:13%"><col style="width:11%"><col style="width:10%"><col style="width:11%"><col style="width:14%"><col style="width:3%"></colgroup>' +
      '<thead><tr><th>Label</th><th class="num">Qty</th><th class="num">Rate</th><th>Period</th><th class="num">Months</th><th class="num">Occ %</th><th class="num">Annual Total</th><th></th></tr></thead>' +
      '<tbody>' + linesHtml + '</tbody>' +
    '</table>' +
    '<div class="anc-actions">' +
      '<button class="anc-add-btn" onclick="ancAddLine(\'' + gl + '\')">+ Add line</button>' +
      syncBtn +
    '</div>' +
    '<div class="anc-hint">Formula: <code>Qty × Rate × (Period: mo×12 | yr×1) × (MonthsActive / 12) × (Occupancy / 100)</code></div>' +
  '</div>';
}

function ancUpdLine(gl, idx, field, value, numeric) {
  const items = _ancGetBackup(gl);
  if (!items[idx]) return;
  // FA directive 2026-05-10: skip when value didn't change.
  const newVal = numeric ? _ancParseNum(value) : value;
  if (items[idx][field] === newVal) return;
  items[idx][field] = newVal;
  faAutoSave(gl, 'backup_json', items);
  // Only update derived cells — DO NOT rewrite the input the user is typing in,
  // otherwise value gets re-formatted (e.g. "1" → "1.00") and backspace breaks.
  ancUpdateDerived(gl);
}

function ancUpdateDerived(gl) {
  const row = document.querySelector('tr[data-anc-drawer="' + gl + '"]');
  if (!row) return;
  const line = _ancGetLine(gl);
  if (!line) return;
  const items = _ancGetBackup(gl);
  const backupTotal = _ancComputeBackupTotal(gl);
  const col6 = Number(line.proposed_budget) || 0;
  const drift = col6 - backupTotal;
  const inSync = Math.abs(drift) < 1;

  // 1) Update per-line totals
  const tbodyTrs = row.querySelectorAll('table.anc-lines tbody tr');
  items.forEach(function(l, idx) {
    const tr = tbodyTrs[idx];
    if (!tr) return;
    const totalCell = tr.querySelector('td.anc-line-total');
    if (totalCell) totalCell.textContent = _ancFmtD2(_ancComputeLineTotal(l));
  });

  // 2) Update backup-total row
  const totalRowCell = row.querySelector('tr.anc-total-row td.num');
  if (totalRowCell) totalRowCell.textContent = _ancFmtD2(backupTotal);

  // 3) Update compare strip — backup total + drift cell
  const highlightVal = row.querySelector('.anc-compare-cell.highlight .anc-value');
  if (highlightVal) highlightVal.textContent = _ancFmtD(backupTotal);

  const driftCell = row.querySelector('.anc-compare-cell.ok, .anc-compare-cell.drift');
  if (driftCell) {
    driftCell.className = 'anc-compare-cell ' + (inSync ? 'ok' : 'drift');
    const valEl = driftCell.querySelector('.anc-value');
    if (valEl) valEl.textContent = _ancFmtD(col6);
    const hintEl = driftCell.querySelector('.anc-hint');
    if (hintEl) {
      hintEl.textContent = inSync
        ? '✓ In sync'
        : '⚠ Drift ' + _ancFmtD(Math.abs(drift)) + (drift > 0 ? ' (Col 6 high)' : ' (Col 6 low)');
    }
  }

  // 4) Update sync button
  const actions = row.querySelector('.anc-actions');
  if (actions) {
    const oldBtn = actions.querySelector('.anc-sync-btn');
    if (oldBtn) {
      if (inSync) {
        oldBtn.outerHTML = '<button class="anc-sync-btn in-sync" disabled>✓ In sync</button>';
      } else {
        oldBtn.outerHTML = '<button class="anc-sync-btn" onclick="ancSyncToCol6(\'' + gl + '\')">Sync Col 6 → ' + _ancFmtD(backupTotal) + '</button>';
      }
    }
  }
}

function ancAddLine(gl) {
  const items = _ancGetBackup(gl);
  items.push({ label: 'New line', qty: 0, rate: 0, period: 'mo', monthsActive: 12, occupancy: 100 });
  faAutoSave(gl, 'backup_json', items);
  ancRedrawDrawer(gl);
}

function ancRemoveLine(gl, idx) {
  const items = _ancGetBackup(gl);
  items.splice(idx, 1);
  faAutoSave(gl, 'backup_json', items);
  ancRedrawDrawer(gl);
}

function ancSyncToCol6(gl) {
  const line = _ancGetLine(gl);
  if (!line) return;
  const total = Math.round(_ancComputeBackupTotal(gl));
  line.proposed_budget = total;
  line.proposed_formula = '';
  // Update the proposed input in the row (if visible)
  const propInput = document.getElementById('prop_' + gl);
  if (propInput) {
    propInput.value = fmt(total);
    propInput.dataset.raw = total;
    propInput.dataset.formula = '';
    propInput.dataset.override = 'false';
    if (propInput.hasAttribute('data-proposed-formula')) propInput.removeAttribute('data-proposed-formula');
    // Reset fx badge color (strip the user-formula blue styling)
    const badgeCell = propInput.parentElement;
    const badge = badgeCell && badgeCell.querySelector('.fa-fx');
    if (badge) {
      badge.style.background = '';
      badge.style.color = '';
      badge.style.borderColor = '';
      badge.textContent = 'fx';
    }
  }
  // Save both fields and recalc totals + drawer
  faAutoSave(gl, 'proposed_budget', total);
  faAutoSave(gl, 'proposed_formula', null);
  if (typeof recalcRow === 'function') recalcRow(gl);
  ancRedrawDrawer(gl);
}

async function dismissReclass(glCode) {
  await fetch('/api/lines/' + entityCode + '/reclass', {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({gl_code: glCode, reclass_to_gl: '', reclass_amount: 0, reclass_notes: ''})
  });
  loadDetail();
}

// ── PM Review Panel Functions ──────────────────────────────────────

function switchPmTab(button, tabId) {
  document.getElementById('pmNotesContent').style.display = 'none';
  document.getElementById('pmReclassContent').style.display = 'none';
  document.getElementById('pmProposalsContent').style.display = 'none';
  document.querySelectorAll('#pmReviewTabs .pm-tab').forEach(t => {
    t.style.color = 'var(--gray-500)';
    t.style.borderBottom = '2px solid transparent';
    t.style.background = 'transparent';
  });
  document.getElementById(tabId).style.display = 'block';
  button.style.color = 'var(--blue)';
  button.style.borderBottom = '2px solid var(--blue)';
  button.style.background = 'white';
}

function toggleReclassInvDetail(gid) {
  const rows = document.querySelectorAll('tr[data-group="' + gid + '"]');
  const arrow = document.getElementById(gid + '_arrow');
  if (!rows.length) return;
  const showing = rows[0].style.display !== 'none';
  rows.forEach(r => { r.style.display = showing ? 'none' : ''; });
  if (arrow) arrow.style.transform = showing ? '' : 'rotate(90deg)';
}

function scrollToGlRow(glCode) {
  const row = document.querySelector('tr[data-gl="' + glCode + '"]');
  if (row) {
    row.scrollIntoView({ behavior: 'smooth', block: 'center' });
    row.style.transition = 'background 0.3s';
    row.style.background = '#fef9c3';
    setTimeout(() => { row.style.background = ''; }, 3000);
  }
}

async function acceptPmReclass(fromGl, toGl, amount, invIdStr) {
  if (!confirm('Accept reclass of ' + fmt(amount) + ' from ' + fromGl + ' to ' + toGl + '?\\n\\nThis will move ' + fmt(amount) + ' of YTD Actual from ' + fromGl + ' to ' + toGl + ' and recalculate both lines. Any accrual adjustment on ' + fromGl + ' will move with it.')) return;

  try {
    const res = await fetch('/api/reclass/accept', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ entity_code: entityCode, from_gl: fromGl, to_gl: toGl, amount: amount })
    });
    if (!res.ok) { const err = await res.json(); throw new Error(err.error || 'Failed'); }

    // Update action cell
    const actionCell = document.getElementById('pmrc_action_' + fromGl + '_' + toGl);
    if (actionCell) actionCell.innerHTML = '<span style="color:var(--green); font-weight:700; font-size:12px;">✓ Accepted</span>';
    const row = document.getElementById('pmrc_' + fromGl + '_' + toGl);
    if (row) row.style.background = '#f0fdf4';

    // Highlight the affected GL rows in the spreadsheet
    const fromRow = document.querySelector('tr[data-gl="' + fromGl + '"]');
    const toRow = document.querySelector('tr[data-gl="' + toGl + '"]');
    if (fromRow) { fromRow.style.background = '#fef2f2'; setTimeout(() => { fromRow.style.background = ''; }, 4000); }
    if (toRow) { toRow.style.background = '#f0fdf4'; setTimeout(() => { toRow.style.background = ''; }, 4000); }

    showToast('Reclass accepted — ' + fmt(amount) + ' moved from ' + fromGl + ' to ' + toGl, 'success');

    // Refresh data to recalculate all numbers
    _faExpenseCache = null;
    loadDetail();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

async function undoPmReclass(fromGl, toGl, invIdStr) {
  if (!confirm('Undo reclass from ' + fromGl + ' to ' + toGl + '?\\n\\nThis will restore the invoices to their original GL code.')) return;

  try {
    const invIds = invIdStr.split(',').map(s => parseInt(s)).filter(n => n > 0);
    for (const invId of invIds) {
      await fetch('/api/expense-dist/reclass/' + invId, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ reclass_to_gl: '' })
      });
    }

    const actionCell = document.getElementById('pmrc_action_' + fromGl + '_' + toGl);
    if (actionCell) actionCell.innerHTML = '<span style="color:var(--gray-400); font-weight:600; font-size:12px;">Undone</span>';
    const row = document.getElementById('pmrc_' + fromGl + '_' + toGl);
    if (row) { row.style.background = 'var(--gray-50)'; row.style.opacity = '0.5'; }

    showToast('Reclass undone — invoices restored to ' + fromGl, 'success');

    _faExpenseCache = null;
    loadDetail();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

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
    _faExpenseCache = null;
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

// ─── Budget Proposals helpers ────────────────────────────────────────────
let _proposalModalGl = null;
let _proposalModalAction = null;

function proposalActionButtons(glCode) {
  return '<button onclick="acceptProposal(\'' + glCode + '\')" style="padding:4px 10px; font-size:11px; font-weight:600; border-radius:5px; cursor:pointer; background:#dcfce7; color:#166534; border:1px solid #86efac;">✓ Accept</button> ' +
    '<button onclick="openProposalModal(\'' + glCode + '\',\'rejected\')" style="padding:4px 10px; font-size:11px; font-weight:600; border-radius:5px; cursor:pointer; background:#fef2f2; color:#991b1b; border:1px solid #fca5a5; margin-left:4px;">✗ Reject</button> ' +
    '<button onclick="openProposalModal(\'' + glCode + '\',\'commented\')" style="padding:4px 10px; font-size:11px; font-weight:600; border-radius:5px; cursor:pointer; background:#fef3c7; color:#92400e; border:1px solid #fde68a; margin-left:4px;">💬</button>';
}

async function acceptProposal(glCode) {
  if (!confirm('Accept PM budget proposal for ' + glCode + '?')) return;
  try {
    const resp = await fetch('/api/budget-proposal/review', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ entity_code: entityCode, gl_code: glCode, action: 'accepted', note: '' })
    });
    const result = await resp.json();
    if (result.error) { showToast(result.error, 'error'); return; }

    // Update row in place
    const row = document.getElementById('pendprop_' + glCode);
    if (row) {
      const cells = row.querySelectorAll('td');
      cells[6].innerHTML = '<span style="background:#dcfce7; color:#166534; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✓ Accepted</span>';
      cells[7].innerHTML = '<span style="color:var(--gray-400); font-size:11px;">Done</span>';
    }
    showToast('Proposal accepted for ' + glCode, 'success');
    updateProposalBadge();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

function openProposalModal(glCode, action) {
  _proposalModalGl = glCode;
  _proposalModalAction = action;
  const modal = document.getElementById('proposalModal');
  const title = document.getElementById('proposalModalTitle');
  const overrideRow = document.getElementById('proposalModalOverrideRow');
  const submitBtn = document.getElementById('proposalModalSubmit');
  const noteEl = document.getElementById('proposalModalNote');
  const overrideEl = document.getElementById('proposalModalOverride');

  noteEl.value = '';
  overrideEl.value = '';

  if (action === 'rejected') {
    title.textContent = 'Reject Proposal — ' + glCode;
    overrideRow.style.display = '';
    submitBtn.textContent = 'Reject & Save';
    submitBtn.style.background = '#dc2626';
  } else {
    title.textContent = 'Comment on Proposal — ' + glCode;
    overrideRow.style.display = 'none';
    submitBtn.textContent = 'Save Comment';
    submitBtn.style.background = '#b45309';
  }
  modal.style.display = 'flex';
}

function closeProposalModal() {
  document.getElementById('proposalModal').style.display = 'none';
  _proposalModalGl = null;
  _proposalModalAction = null;
}

async function submitProposalReview() {
  const note = document.getElementById('proposalModalNote').value.trim();
  const overrideRaw = document.getElementById('proposalModalOverride').value.trim();
  const overrideValue = overrideRaw ? parseFloat(overrideRaw.replace(/[$,]/g, '')) : null;

  if (!note && _proposalModalAction === 'commented') {
    showToast('Please enter a comment', 'error');
    return;
  }

  try {
    const payload = {
      entity_code: entityCode,
      gl_code: _proposalModalGl,
      action: _proposalModalAction,
      note: note
    };
    if (_proposalModalAction === 'rejected' && overrideValue !== null && !isNaN(overrideValue)) {
      payload.override_value = overrideValue;
    }

    const resp = await fetch('/api/budget-proposal/review', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
    const result = await resp.json();
    if (result.error) { showToast(result.error, 'error'); return; }

    // Update row in place
    const row = document.getElementById('pendprop_' + _proposalModalGl);
    if (row) {
      const cells = row.querySelectorAll('td');
      if (_proposalModalAction === 'rejected') {
        cells[6].innerHTML = '<span style="background:#fef2f2; color:#991b1b; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✗ Rejected</span>';
        cells[7].innerHTML = '<span style="color:var(--gray-400); font-size:11px;">Done</span>';
        // If FA provided override, update the proposed column
        if (overrideValue !== null && !isNaN(overrideValue)) {
          cells[3].innerHTML = '<span style="color:var(--blue); font-weight:700;">' + fmt(overrideValue) + '</span> <span style="font-size:10px; color:var(--gray-400);">FA override</span>';
        }
      } else {
        cells[6].innerHTML = '<span style="background:#fef3c7; color:#92400e; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">💬 Commented</span>';
        // Keep action buttons for commented status
      }
    }

    closeProposalModal();
    const verb = _proposalModalAction === 'rejected' ? 'rejected' : 'comment saved on';
    showToast('Proposal ' + verb + ' ' + _proposalModalGl, 'success');
    updateProposalBadge();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

function updateProposalBadge() {
  // Recount pending proposals from the DOM
  const rows = document.querySelectorAll('#pmProposalsBody tr');
  let pending = 0;
  rows.forEach(r => {
    const statusCell = r.querySelectorAll('td')[6];
    if (statusCell && statusCell.textContent.includes('Pending')) pending++;
    if (statusCell && statusCell.textContent.includes('Commented')) pending++;
  });
  document.getElementById('pmProposalsCount').textContent = pending || '';
  // Update main badge
  const reclassCount = parseInt(document.getElementById('pmReclassCount').textContent) || 0;
  const notesCount = parseInt(document.getElementById('pmNotesCount').textContent) || 0;
  const total = notesCount + reclassCount + pending;
  document.getElementById('pmReviewBadgeText').textContent = total + ' item' + (total !== 1 ? 's' : '') + ' need review';
}

// FA dir 2026-05-17: GL-prefix → Gen & Admin sub-category. Mirrors the
// Sub-Category column in budget_system/GL_Mapping.csv. Used as fallback in
// every Gen & Admin sub-category match function when row_num=0 (the case for
// GLs imported from the YSL that weren't in the approved-2026 Excel template,
// e.g. GL 6145-0000 "Errors & Omissions Insurance"). Without this, those GLs
// land in "Administrative & Other" instead of their natural section.
function _gaSubForGl(gl) {
  const p4 = (gl || '').slice(0, 4);
  // FA dir 2026-05-19: 6315 (Real Estate Tax + credits) now lives on the
  // RE Taxes tab Section 3 only. Return null so G&A categorization treats
  // these lines as "skip" (combined with the explicit predicate filter
  // in SHEET_CATEGORIES so row_num-based imports also exclude them).
  if (p4 === '6315') return null;
  // Insurance (6105-6195)
  if (['6105','6110','6115','6120','6125','6126','6130','6135','6140','6145','6150','6195'].indexOf(p4) >= 0) return 'insurance';
  // Taxes (6310-6395) — excludes 6315, handled above
  if (['6310','6320','6325','6330','6335','6395'].indexOf(p4) >= 0) return 'taxes';
  // Professional Fees (6505-6590)
  if (['6505','6510','6515','6520','6525','6535','6555','6585','6590'].indexOf(p4) >= 0) return 'prof_fees';
  // Financial Expenses (6905-6970+, etc.)
  if (p4.startsWith('69')) return 'financial';
  // Administrative & Other (6700-6799) — Telephone, Software, Cable, Stationery, Misc, etc.
  if (p4 >= '6700' && p4 <= '6799') return 'admin_other';
  return 'admin_other';   // safe default
}

// Category grouping definitions per sheet
const SHEET_CATEGORIES = {
  'Repairs & Supplies': {
    groups: [
      {key: 'supplies', label: 'Supplies', match: l => l.category === 'supplies'},
      {key: 'repairs', label: 'Repairs', match: l => l.category === 'repairs'},
      {key: 'maintenance', label: 'Maintenance Contracts', match: l => l.category === 'maintenance'}
    ]
  },
  'Gen & Admin': {
    // FA dir 2026-05-17: GL-prefix fallback for row_num=0 GLs (YSL-imported
    // but not in the approved-2026 template). See _gaSubForGl() definition.
    groups: [
      {key: 'prof_fees', label: 'Professional Fees', match: l => (l.row_num >= 8 && l.row_num <= 16)  || (!l.row_num && _gaSubForGl(l.gl_code) === 'prof_fees')},
      {key: 'admin',     label: 'Administrative & Other', match: l => (l.row_num >= 20 && l.row_num <= 49) || (!l.row_num && _gaSubForGl(l.gl_code) === 'admin_other')},
      {key: 'insurance', label: 'Insurance', match: l => (l.row_num >= 53 && l.row_num <= 64) || (!l.row_num && _gaSubForGl(l.gl_code) === 'insurance')},
      {key: 'taxes',     label: 'Taxes', match: l => !(l.gl_code || '').startsWith('6315') && ((l.row_num >= 68 && l.row_num <= 78) || (!l.row_num && _gaSubForGl(l.gl_code) === 'taxes'))},
      {key: 'financial', label: 'Financial Expenses', match: l => (l.row_num >= 82 && l.row_num <= 90) || (!l.row_num && _gaSubForGl(l.gl_code) === 'financial')}
    ]
  }
};

// Budget Summary mapping (matches template_populator.py BUDGET_SUMMARY_MAPPING)
const SUMMARY_ROWS = [
  {label: 'Total Operating Income', sheet: 'Income', type: 'income'},
  {label: 'Payroll & Related', sheet: 'Payroll', type: 'expense'},
  {label: 'Energy', sheet: 'Energy', type: 'expense'},
  {label: 'Water & Sewer', sheet: 'Water & Sewer', type: 'expense'},
  {label: 'Repairs & Supplies', sheet: 'Repairs & Supplies', type: 'expense'},
  {label: 'Professional Fees', sheet: 'Gen & Admin', rowRange: [8,16], type: 'expense'},
  {label: 'Administrative & Other', sheet: 'Gen & Admin', rowRange: [20,49], type: 'expense'},
  {label: 'Insurance', sheet: 'Gen & Admin', rowRange: [53,64], type: 'expense'},
  {label: 'Taxes', sheet: 'Gen & Admin', rowRange: [68,78], type: 'expense'},
  {label: 'Financial Expenses', sheet: 'Gen & Admin', rowRange: [82,90], type: 'expense'}
];

// RE Taxes GL prefix 6315 — forecast is pinned to approved budget (current_budget),
// estimate back-solves from the pinned forecast minus YTD actual. Matches the Excel
// RE Taxes tab logic where Forecast = SUM(YTD, Remaining) and the effective result
// equals the gross tax budget. User-entered estimate/forecast overrides still win.
function faIsFixedToBudget(l) {
  const gl = (l && l.gl_code) || '';
  if (gl.indexOf('6315') === 0) return true;           // RE Tax (expense) — existing pin
  return faIsIncomePinned(l);                           // fully-collectible income (task #99)
}

// Task #99 (FA dir 2026-06-02): the server (/api/dashboard) stamps income lines
// with income_pinned=true when the Budget Summary pins that row's forecast to
// approved budget (Maintenance / Common Charges / Commercial / Operating
// Assessment, and only where an approved budget actually exists). Mirroring the
// Summary's own per-row decision is what makes the Income tab tie to the Summary
// instead of annualizing — and it's why buildings with no approved budget on a
// row (e.g. 500's maintenance) correctly keep annualizing on both tabs.
function faIsIncomePinned(l) {
  return !!(l && l.income_pinned);
}

// One-time annual fees: once YTD is posted, there is no additional billing
// for the rest of the year, so the Mar-Dec estimate must be zero.
// Kept in sync with Python ONE_TIME_FEE_GLS constant in workflow.py.
const ONE_TIME_FEE_GLS = new Set(['6722-0000','6762-0000','6763-0000','6764-0000']);
function faIsOneTimeFeeBilled(l) {
  if (!l || !l.gl_code) return false;
  if (!ONE_TIME_FEE_GLS.has(l.gl_code)) return false;
  const billed = (l.ytd_actual || 0) + (l.accrual_adj || 0) + (l.unpaid_bills || 0);
  return Math.abs(billed) > 0.01;
}

function faIsCapital(l) {
  return l && (l.sheet_name === 'Capital' || (l.category || '').toLowerCase() === 'capital');
}

function faComputeEstimate(l) {
  // Use override if FA set one
  if (l.estimate_override !== null && l.estimate_override !== undefined) return l.estimate_override;
  if (faIsFixedToBudget(l)) {
    const cb = l.current_budget || 0;
    const ytd = l.ytd_actual || 0;
    return cb - ytd;
  }
  // One-time fees with a YTD posted: no more projection
  if (faIsOneTimeFeeBilled(l)) return 0;
  // FA #18 + 2026-05-05 directive: Capital — no estimate at all.
  if (faIsCapital(l)) return 0;
  // 210 FA: RE-tax credit income (Abatement/STAR/Veteran/SCRIE/SCHE — GL
  // 4105/4110/4115/4120/4125) posts at year-end, not monthly — no May-Dec estimate.
  if (['4105','4110','4115','4120','4125'].indexOf((l.gl_code||'').slice(0,4)) >= 0) return 0;
  const ytd = l.ytd_actual || 0;
  const accrual = l.accrual_adj || 0;
  const unpaid = l.unpaid_bills || 0;
  const base = ytd + accrual + unpaid;
  // Formula: (YTD+Accrual+Unpaid) / YTD_MONTHS * REMAINING_MONTHS
  if (YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
  return 0;
}

function faComputeForecast(l) {
  // Use override if FA set one
  if (l.forecast_override !== null && l.forecast_override !== undefined) return l.forecast_override;
  if (faIsFixedToBudget(l)) {
    return l.current_budget || 0;
  }
  // FA directive 2026-06-10 (Jacob, 829 Cap-Doors: an accrual that zeros YTD
  // must zero the forecast too — supersedes 2026-05-05's minus sign, which
  // double-counted): Capital forecast = YTD + accrual + unpaid (no estimate).
  if (faIsCapital(l)) {
    return (l.ytd_actual || 0) + (l.accrual_adj || 0) + (l.unpaid_bills || 0);
  }
  return (l.ytd_actual || 0) + (l.accrual_adj || 0) + (l.unpaid_bills || 0) + faComputeEstimate(l);
}

function faGetFormulaTooltip(l, field) {
  const ytd = l.ytd_actual || 0;
  const accrual = l.accrual_adj || 0;
  const unpaid = l.unpaid_bills || 0;
  const estimate = faComputeEstimate(l);
  const forecast = faComputeForecast(l);
  const incPct = l.increase_pct || 0;

  if (field === 'estimate') {
    if (faIsFixedToBudget(l)) {
      const cb = l.current_budget || 0;
      return sumExcelExpr([cb, -ytd]) || '=0';   // estimate pinned: current budget - YTD (valid Excel)
    }
    if (faIsOneTimeFeeBilled(l)) {
      return '=0';   // one-time fee already billed YTD -> no estimate
    }
    if (faIsCapital(l)) {
      return '=0';   // Capital -> no estimate
    }
    if (YTD_MONTHS > 0) return '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
    return '=0';
  }
  if (field === 'forecast') {
    if (faIsFixedToBudget(l)) {
      const cb = l.current_budget || 0;
      return '=' + Math.round(cb);   // forecast pinned to current budget
    }
    if (faIsOneTimeFeeBilled(l)) {
      return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+0';   // one-time fee: no additional projection
    }
    if (faIsCapital(l)) {
      return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')';   // Capital: YTD net of accrual, plus unpaid (2026-06-10 sign fix)
    }
    const estExpr = (YTD_MONTHS > 0) ? '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0';
    return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + estExpr + ')';
  }
  if (field === 'proposed') {
    if (l.no_budget) return '=0';   // FA 2026-06-17 (B1/B4): never-budgeted line
    if (faIsCapital(l)) {
      return '=0';   // Capital -> no proposed budget
    }
    if (l.proposed_formula) return l.proposed_formula;
    // FA #25 (2026-06-15): Payroll Processing (5172) = 2026 budget × 1.03.
    if (l.gl_code === '5172-0000') {
      return '=' + Math.round(l.current_budget || 0) + '*1.03';
    }
    // FA #26 (2026-06-15): R+M and Gen&Admin propose off the 2026 budget, not
    // the 12-mo forecast.
    if (l.sheet_name === 'Repairs & Supplies' || l.sheet_name === 'Gen & Admin') {
      return '=' + Math.round(l.current_budget || 0) + '*(1+' + incPct.toFixed(4) + ')';
    }
    const fcstExpr = ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + ((YTD_MONTHS > 0) ? '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0') + ')';
    return '=(' + fcstExpr + ')*(1+' + incPct.toFixed(4) + ')';
  }
  return '';
}

function renderSheet(sheetName, sheetLines, tabEl, opts) {
  opts = opts || {};
  // Flush any pending Building Info save before switching sheets
  if (typeof _biSaveTimer !== 'undefined' && _biSaveTimer) {
    clearTimeout(_biSaveTimer); _biSaveTimer = null;
    try { _biSaveNow(); } catch (e) {}
  }
  document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
  tabEl.classList.add('active');
  // FA dir 2026-05-19: track active sheet so the per-tab Undo bar knows
  // which sheet's changes to load.
  window._activeFaSheet = sheetName;
  // QA fix 4 (2026-07-03): the tab buttons capture the bootstrap sheets
  // object in their onclick closures, so re-renders showed pre-edit numbers
  // (the edit looked lost while the server had it). Always re-resolve the
  // lines from the live allSheets cache instead of the closure argument.
  if (sheetName !== 'Summary' && typeof allSheets !== 'undefined' && allSheets && allSheets[sheetName]) {
    sheetLines = allSheets[sheetName];
  }
  // FA dir 2026-05-24: push the tab name to URL so browser-back returns to
  // the previous tab instead of exiting the building to /dashboard. opts.skipPush
  // is set on the initial Summary render + popstate-driven re-renders to avoid
  // history pollution. Summary is the default — clear ?tab when on it.
  if (!opts.skipPush) {
    try {
      const url = new URL(window.location.href);
      const current = url.searchParams.get('tab') || 'Summary';
      if (current !== sheetName) {
        if (sheetName === 'Summary') url.searchParams.delete('tab');
        else url.searchParams.set('tab', sheetName);
        window.history.pushState({ tab: sheetName }, '', url.toString());
      }
    } catch (e) {}
  }

  // QA fix 4 (2026-07-03): a committed edit marks the bootstrap cache dirty.
  // Flush any pending debounced save, re-pull /api/dashboard, swap the cache,
  // then re-enter. Tab switches can never show pre-edit numbers again.
  if (window._faDataDirty && !opts._refetched) {
    window._faDataDirty = false;
    var _flushP = (typeof _faFlushSave === 'function') ? _faFlushSave() : Promise.resolve();
    Promise.resolve(_flushP).then(function() {
      return fetch('/api/dashboard/' + entityCode);
    }).then(function(r) { return r.ok ? r.json() : null; }).then(function(d) {
      if (d && d.sheets) {
        allSheets = d.sheets;
        window._data = d;
        if (d.re_taxes) window._reTaxesData = d.re_taxes;
        // QA fix 8: repaint pending-review rows from the fresh lines so the
        // review queue can never offer stale amounts after an edit elsewhere.
        try {
          var _all = [];
          Object.keys(allSheets || {}).forEach(function(k) { _all = _all.concat(allSheets[k] || []); });
          document.querySelectorAll('tr[id^="pendprop_"]').forEach(function(trEl) {
            var g = trEl.id.replace('pendprop_', '');
            var ln = _all.find(function(x) { return x.gl_code === g; });
            if (!ln || trEl.children.length < 5) return;
            var cur = ln.current_budget || 0, prop = ln.proposed_budget || 0, ch = prop - cur;
            var pc = cur !== 0 ? ((ch / cur) * 100).toFixed(1) : '0.0';
            trEl.children[2].textContent = fmt(cur);
            trEl.children[3].textContent = fmt(prop);
            trEl.children[4].textContent = (ch >= 0 ? '+' : '') + fmt(ch) + ' (' + pc + '%)';
          });
        } catch (e) {}
      }
      renderSheet(sheetName, null, tabEl, Object.assign({}, opts, {skipPush: true, _refetched: true}));
    }).catch(function() {
      renderSheet(sheetName, null, tabEl, Object.assign({}, opts, {skipPush: true, _refetched: true}));
    });
    return;
  }

  const contentDiv = document.getElementById('sheetContent');

  // Handle Budget Summary tab
  if (sheetName === 'Summary') {
    renderBudgetSummary(contentDiv);
    return;
  }

  // Handle RE Taxes tab — custom calculation layout
  if (sheetName === 'RE Taxes') {
    renderRETaxesTab(contentDiv);
    return;
  }

  // Handle Payroll tab — enhanced with roster calc engine, assumptions, and GL grouping
  if (sheetName === 'Payroll') {
    renderPayrollTab(sheetLines, contentDiv);
    return;
  }

  if (!sheetLines || sheetLines.length === 0) {
    contentDiv.innerHTML = '<p style="padding:24px; color:var(--gray-500);">No data for this sheet.</p>';
    return;
  }

  // All sheets are editable for the FA — this is the budget workbench
  renderEditableSheet(sheetName, sheetLines, contentDiv);
  setTimeout(faUpdateZeroToggle, 50);
  // FA #B7 (2026-06-17): on the Unmapped tab, inject a per-row "Add to budget"
  // picker so an orphan GL can be mapped into a Summary line (→ tab + budget).
  if (sheetName === 'Unmapped') setTimeout(faEnhanceUnmappedTab, 60);
  // FA #B3 (2026-06-17): on the Income tab, add an above/below-the-line toggle
  // to the interest-income (4800) row so each building can place it as
  // operating or non-operating income.
  if (sheetName === 'Income') setTimeout(faEnhanceInterestToggle, 60);
}

// ── B7 (2026-06-17): "Add to budget" picker on the Unmapped tab ─────────
// An Unmapped GL line has no Summary mapping, so it never reaches the budget
// (the FA's 6744 Parking gripe). This injects a per-row picker of the
// building's Summary lines; choosing one calls map-to-summary (appends the GL
// to that row's prefixes + moves the line onto its detail tab), then reloads
// so the change shows everywhere (tab + Summary + totals). Decoupled from the
// render internals — runs after the Unmapped sheet paints.
let _b7SummaryRows = null;

async function faEnhanceUnmappedTab() {
  const host = document.getElementById('sheetContent');
  if (!host) return;
  const rows = host.querySelectorAll('tr[data-gl]');
  if (!rows.length) return;
  if (_b7SummaryRows === null) {
    try {
      const r = await fetch('/api/budget-summary-rows/' + entityCode);
      const d = await r.json();
      _b7SummaryRows = (d && d.rows) ? d.rows : [];
    } catch (e) { _b7SummaryRows = []; }
  }
  let optsHtml = '<option value="">➕ Add to budget…</option>';
  _b7SummaryRows.forEach(function (r) {
    const lab = (r.section ? (r.section + ' › ') : '') + r.label;
    optsHtml += '<option value="' + r.id + '">' + lab.replace(/</g, '&lt;') + '</option>';
  });
  rows.forEach(function (tr) {
    const gl = tr.getAttribute('data-gl');
    if (!gl) return;
    const descTd = tr.querySelector('td.frozen-desc') || tr.querySelector('td');
    if (!descTd || descTd.querySelector('.b7-map')) return;
    const sel = document.createElement('select');
    sel.className = 'b7-map';
    sel.dataset.gl = gl;
    sel.title = 'Add this account to a budget (Summary) line — puts it on that tab and into the budget total';
    sel.style.cssText = 'margin-left:8px; font-size:11px; max-width:220px; border:1px solid var(--blue); border-radius:4px; color:var(--blue); background:#eff6ff; cursor:pointer;';
    sel.innerHTML = optsHtml;
    sel.onchange = function () { faMapToBudget(this); };
    descTd.appendChild(sel);
  });
}

async function faMapToBudget(sel) {
  const gl = sel.dataset.gl;
  const rowId = sel.value;
  if (!rowId) return;
  const label = sel.options[sel.selectedIndex].text;
  if (!confirm('Add account ' + gl + ' to budget line:\n\n  ' + label +
               '\n\nThis puts it on that tab and rolls it into the budget total.')) {
    sel.value = '';
    return;
  }
  sel.disabled = true;
  try {
    const r = await fetch('/api/lines/' + entityCode + '/map-to-summary', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ gl_code: gl, summary_row_id: parseInt(rowId, 10) })
    });
    const d = await r.json();
    if (!r.ok || d.error) throw new Error(d.error || ('HTTP ' + r.status));
    location.reload();
  } catch (e) {
    alert('Could not add to budget: ' + e.message);
    sel.disabled = false;
    sel.value = '';
  }
}

// ── B3 (2026-06-17): interest income above/below-the-line toggle ─────────
// On the Income tab the interest-income (4800) row gets a toggle that flips
// the Summary "Interest Income" row between operating (above the line) and
// Non-Operating Income (below). Per-building placement, per FA B3.
async function faEnhanceInterestToggle() {
  const host = document.getElementById('sheetContent');
  if (!host) return;
  let tr = null;
  host.querySelectorAll('tr[data-gl]').forEach(function (x) {
    if ((x.getAttribute('data-gl') || '').indexOf('4800') === 0) tr = x;
  });
  if (!tr) return;
  const descTd = tr.querySelector('td.frozen-desc') || tr.querySelector('td');
  if (!descTd || descTd.querySelector('.b3-int')) return;
  let section = '';
  try {
    const r = await fetch('/api/budget-summary-rows/' + entityCode);
    const d = await r.json();
    const ir = (d.rows || []).find(function (x) { return /interest/i.test(x.label || ''); });
    section = (ir && ir.section) || '';
  } catch (e) {}
  const below = /non-?operating/i.test(section);
  const btn = document.createElement('button');
  btn.className = 'b3-int';
  btn.textContent = below ? '↧ Below the line (Non-Op) — move above' : '↥ Above the line (Operating) — move below';
  btn.title = 'Place interest income above (operating) or below (non-operating) the line on the Summary';
  btn.style.cssText = 'margin-left:8px; font-size:11px; padding:1px 7px; border:1px solid var(--blue); border-radius:4px; color:var(--blue); background:#eff6ff; cursor:pointer;';
  btn.onclick = function () { faToggleInterestPlacement(below ? 'operating' : 'non_operating'); };
  descTd.appendChild(btn);
}

async function faToggleInterestPlacement(placement) {
  const label = (placement === 'non_operating')
    ? 'BELOW the line (Non-Operating Income)' : 'ABOVE the line (Operating Income)';
  if (!confirm('Move Interest Income ' + label + ' on the Summary?')) return;
  try {
    const r = await fetch('/api/budget/' + entityCode + '/interest-placement', {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ placement: placement })
    });
    const d = await r.json();
    if (!r.ok || d.error) throw new Error(d.error || ('HTTP ' + r.status));
    location.reload();
  } catch (e) {
    alert('Could not move interest income: ' + e.message);
  }
}

// ── RE Taxes Tab — Custom Calculation Layout ──────────────────────────────
// ══════════════════════════════════════════════════════════════════════
// RE TAXES TAB — ported from re_taxes_template_preview.html (2026-04-15)
// ----------------------------------------------------------------------
// Replaces the legacy renderRETaxesTab. Backend contract unchanged:
//   GET  /api/re-taxes/<entity>  → populate cells via reTaxLoadFromBackend
//   PUT  /api/re-taxes/<entity>  → autosave via saveRETaxes (debounced)
// Cells: 71 registered (Section 1 + exemptions + per-GL budget lines).
// Formula bar: click any cell → see/edit value or override formula.
// ══════════════════════════════════════════════════════════════════════

function renderRETaxesTab(contentDiv) {
  const reTaxes = window._reTaxesData;
  if (!reTaxes) {
    contentDiv.innerHTML = '<div style="padding:40px; text-align:center; color:#64748b;">Loading RE Taxes data…</div>';
    return;
  }
  if (reTaxes.is_coop === false) {
    contentDiv.innerHTML = '<div style="padding:40px; text-align:center; color:#64748b;">This building is not configured as a co-op. RE Taxes tab is only available for co-op buildings.</div>';
    return;
  }
  const entityCode = reTaxes.entity_code;
  window._reActiveEntity = entityCode;
  contentDiv.innerHTML = RE_TAXES_TAB_HTML;
  // FA #14: rebuild GL_ROWS = the 7 fixed rows + this building's custom escalation
  // / adjustment lines (from the backend). Must run BEFORE initReTaxesTab() so
  // buildGlRows() + registerGlCellMeta() pick the custom rows up. Base case (no
  // custom rows) leaves GL_ROWS exactly equal to FIXED_GL_ROWS.
  GL_ROWS = FIXED_GL_ROWS.concat((reTaxes.custom_gl_rows || []).map(function (r) {
    return { gl: r.gl, label: r.label, custom: true };
  }));
  // Initialize the tab (builds GL rows, wires cell selection, loads from backend)
  try {
    initReTaxesTab();
    reTaxLoadFromBackend(reTaxes);
  } catch (err) {
    console.error('RE Taxes init error:', err);
    contentDiv.innerHTML = '<div style="padding:40px; text-align:center; color:#dc2626;">Failed to initialize RE Taxes tab: ' + err.message + '</div>';
  }
}

// FA #14: add a custom RE-tax escalation / adjustment line. Prompts the FA for a
// label + amount, creates a real 6315-xxxx budget line on the backend (so it also
// flows into Gen & Admin + the Summary via the 6315 prefix), then reloads back
// onto the RE Taxes tab so window._data refetches and the new row renders with
// its value (YTD from the line; E/F/H compute; it rolls into the Section-3 totals).
async function reTaxAddRow() {
  const ec = window._reActiveEntity;
  if (!ec) { alert('No active building loaded.'); return; }
  const label = prompt('Label for the new RE-tax line (e.g. "Assessment Appeal Adjustment"):', '');
  if (label === null) return;
  if (!label.trim()) { alert('A label is required.'); return; }
  const amtStr = prompt('Amount for "' + label.trim() + '" (annual $ — this is the YTD actual; E/F/H compute from it). Blank = 0:', '0');
  if (amtStr === null) return;
  const amount = parseDollar(amtStr);
  const btn = document.getElementById('reAddRowBtn');
  if (btn) { btn.disabled = true; btn.textContent = 'Adding…'; }
  try {
    const resp = await fetch('/api/re-taxes/' + ec + '/add-line', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label: label.trim(), amount: amount }),
    });
    const data = await resp.json();
    if (!resp.ok || data.error) { throw new Error(data.error || ('HTTP ' + resp.status)); }
    window.location.href = '/dashboard/' + ec + '?tab=' + encodeURIComponent('RE Taxes');
  } catch (e) {
    alert('Could not add row: ' + e.message);
    if (btn) { btn.disabled = false; btn.textContent = '+ Add Row'; }
  }
}

const RE_TAXES_TAB_HTML = `<style>

/* FA directive 2026-05-10: reskinned to match the dashboard palette.
   The original Excel-port used a cold blue/gray scheme (#f8fafc /
   #cbd5e1 / #2563eb) that felt like a different product from the rest
   of the app. These vars are scoped to .re-taxes-wrap so they don't
   leak to other tabs; the values now reference the dashboard's warm
   palette (--blue: #5a4a3f / --blue-light: #f5efe7) when available. */
.re-taxes-wrap {
    --bg: var(--gray-50, #f9fafb);
    --card: #ffffff;
    --border: var(--gray-200, #e5e7eb);
    --text: var(--gray-800, #1f2937);
    --muted: var(--gray-500, #6b7280);
    --accent: var(--blue, #5a4a3f);
    --accent-light: var(--blue-light, #f5efe7);
    --good: var(--green, #16a34a);
    --bad: var(--red, #dc2626);
    --warn: var(--amber, #d97706);
    --computed-bg: var(--gray-100, #f3f4f6);
    --input-bg: var(--blue-light, #f5efe7);
  }
.re-taxes-wrap * { box-sizing: border-box; }
.re-taxes-wrap {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    margin: 0;
    padding: 20px;
    font-size: 13px;
  }
.re-taxes-wrap .wrap { max-width: 1400px; margin: 0 auto; }
.re-taxes-wrap .header {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px 20px;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
  }
.re-taxes-wrap .header h1 { font-size: 18px; margin: 0; flex: 1; }
.re-taxes-wrap .header .subtitle { color: var(--muted); font-size: 13px; }
.re-taxes-wrap .header select, .re-taxes-wrap .header button {
    padding: 6px 12px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: white;
    font-size: 13px;
    cursor: pointer;
  }
.re-taxes-wrap .header button:hover { background: var(--accent-light); }
.re-taxes-wrap .layout {
    display: grid;
    grid-template-columns: 1fr 320px;
    gap: 16px;
  }
.re-taxes-wrap .card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px 20px;
    margin-bottom: 16px;
  }
.re-taxes-wrap .card h2 {
    font-size: 14px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--muted);
    margin: 0 0 12px 0;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
  }
.re-taxes-wrap .section-label {
    font-weight: 600;
    color: var(--text);
    margin: 14px 0 6px 0;
    font-size: 13px;
  }
.re-taxes-wrap table { width: 100%; border-collapse: collapse; font-size: 13px; }
.re-taxes-wrap th, .re-taxes-wrap td { padding: 6px 8px; text-align: left; vertical-align: middle; }
.re-taxes-wrap th {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    font-weight: 600;
  }
.re-taxes-wrap td.num, .re-taxes-wrap th.num { text-align: right; font-variant-numeric: tabular-nums; }
.re-taxes-wrap tr.total td {
    border-top: 2px solid var(--text);
    font-weight: 700;
    background: var(--bg);
  }
.re-taxes-wrap input[type="text"], .re-taxes-wrap input[type="number"] {
    width: 100%;
    padding: 5px 7px;
    border: 1px solid var(--border);
    border-radius: 4px;
    font-size: 13px;
    font-variant-numeric: tabular-nums;
    text-align: right;
    background: var(--input-bg);
    font-family: inherit;
  }
.re-taxes-wrap input[type="text"]:focus, .re-taxes-wrap input[type="number"]:focus {
    outline: none;
    border-color: var(--accent);
    box-shadow: 0 0 0 2px var(--accent-light);
  }
.re-taxes-wrap .computed {
    padding: 5px 7px;
    text-align: right;
    background: var(--computed-bg);
    border-radius: 4px;
    color: var(--text);
    font-variant-numeric: tabular-nums;
    border: 1px solid transparent;
  }
.re-taxes-wrap .computed.formula-clickable { cursor: pointer; transition: all 0.1s; }
.re-taxes-wrap .computed.formula-clickable:hover { background: var(--accent-light); border-color: var(--accent); }
.re-taxes-wrap #formulaPopover {
    position: fixed;
    z-index: 1000;
    background: #0f172a;
    color: #e2e8f0;
    border: 1px solid #334155;
    border-radius: 8px;
    padding: 12px 14px;
    font-family: "SF Mono", Consolas, monospace;
    font-size: 12px;
    max-width: 460px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.3);
    display: none;
    pointer-events: auto;
  }
.re-taxes-wrap #formulaPopover .title {
    font-family: -apple-system, sans-serif;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #94a3b8;
    margin-bottom: 6px;
  }
.re-taxes-wrap #formulaPopover .formula-row {
    padding: 3px 0;
    line-height: 1.6;
  }
.re-taxes-wrap #formulaPopover .formula-row .ref { color: #93c5fd; }
.re-taxes-wrap #formulaPopover .formula-row .val { color: #fde68a; }
.re-taxes-wrap #formulaPopover .formula-row .res { color: #86efac; font-weight: 600; }
.re-taxes-wrap #formulaPopover .close-x {
    position: absolute;
    top: 6px;
    right: 8px;
    cursor: pointer;
    color: #64748b;
    font-size: 14px;
  }
.re-taxes-wrap #formulaPopover .close-x:hover { color: #f1f5f9; }
.re-taxes-wrap #formulaPopover .note-text {
    margin-top: 6px;
    padding-top: 6px;
    border-top: 1px solid #334155;
    color: #94a3b8;
    font-family: -apple-system, sans-serif;
    font-size: 11px;
    font-style: italic;
  }
.re-taxes-wrap .pct-input { max-width: 90px; }
.re-taxes-wrap .av-input { max-width: 140px; }
.re-taxes-wrap .rate-input { max-width: 90px; }
.re-taxes-wrap .dollar-input { max-width: 120px; }
.re-taxes-wrap .gl-code { font-family: "SF Mono", Consolas, monospace; font-size: 12px; color: var(--muted); }
.re-taxes-wrap .gross-banner {
    background: linear-gradient(135deg, var(--blue-dark, #3d322a), var(--blue, #5a4a3f));
    color: white;
    padding: 12px 16px;
    border-radius: 6px;
    margin-top: 12px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
.re-taxes-wrap .gross-banner .label {
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    opacity: 0.85;
  }
.re-taxes-wrap .gross-banner .value {
    font-size: 24px;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
  }
.re-taxes-wrap .sidebar .diff-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 6px 0;
    border-bottom: 1px solid var(--border);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }
.re-taxes-wrap .sidebar .diff-row:last-child { border-bottom: none; }
.re-taxes-wrap .diff-row .label { color: var(--muted); }
.re-taxes-wrap .diff-row .excel { color: var(--text); }
.re-taxes-wrap .diff-row .status { margin-left: 6px; }
.re-taxes-wrap .diff-row .status.ok { color: var(--good); }
.re-taxes-wrap .diff-row .status.bad { color: var(--bad); }
.re-taxes-wrap .output-panel {
    background: #0f172a;
    color: #e2e8f0;
    padding: 12px;
    border-radius: 6px;
    font-family: "SF Mono", Consolas, monospace;
    font-size: 12px;
    white-space: pre-wrap;
    max-height: 280px;
    overflow-y: auto;
  }
.re-taxes-wrap .output-panel .key { color: #93c5fd; }
.re-taxes-wrap .output-panel .num { color: #86efac; }
.re-taxes-wrap button.copy-btn {
    margin-top: 8px;
    background: var(--accent);
    color: white;
    border: none;
    padding: 6px 12px;
    border-radius: 4px;
    font-size: 12px;
    cursor: pointer;
  }
.re-taxes-wrap button.copy-btn:hover { background: #1e40af; }
.re-taxes-wrap .note {
    font-size: 11px;
    color: var(--muted);
    margin-top: 4px;
  }
.re-taxes-wrap .flag-cell { background: var(--input-bg) !important; }
.re-taxes-wrap .right-col-layout td:first-child { width: 55%; }
.re-taxes-wrap .ysl-badge, .re-taxes-wrap .upload-badge {
    display: inline-block;
    padding: 1px 5px;
    font-size: 9px;
    border-radius: 3px;
    vertical-align: middle;
    font-weight: 600;
    text-transform: uppercase;
  }
.re-taxes-wrap .ysl-badge { background: #dcfce7; color: #166534; margin-left: 4px; }
.re-taxes-wrap .upload-badge { background: #fef3c7; color: #92400e; margin-left: 4px; }
.re-taxes-wrap /* ── Excel-style Formula Bar ───────────────────────────────────── */
  .formula-bar {
    position: sticky;
    top: 0;
    z-index: 900;
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 8px 12px;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 10px;
    box-shadow: 0 2px 8px rgba(15,23,42,0.04);
  }
.re-taxes-wrap .formula-bar .cell-ref {
    min-width: 220px;
    max-width: 320px;
    padding: 6px 12px;
    background: #f1f5f9;
    border: 1px solid var(--border);
    border-radius: 4px;
    font-family: inherit;
    font-size: 13px;
    font-weight: 600;
    color: var(--text);
    text-align: left;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
.re-taxes-wrap .formula-bar .cell-label {
    display: none;
  }
.re-taxes-wrap .formula-bar .fx-icon {
    font-style: italic;
    font-weight: 700;
    color: var(--accent);
    font-size: 14px;
    padding: 0 4px;
  }
.re-taxes-wrap .formula-bar input.fx-input {
    flex: 1;
    padding: 6px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    font-family: "SF Mono", Consolas, monospace;
    font-size: 13px;
    background: white;
    text-align: left;
    min-width: 200px;
  }
.re-taxes-wrap .formula-bar input.fx-input:focus {
    outline: none;
    border-color: var(--accent);
    box-shadow: 0 0 0 2px var(--accent-light);
  }
.re-taxes-wrap .formula-bar input.fx-input:disabled {
    background: var(--bg);
    color: var(--muted);
  }
.re-taxes-wrap .formula-bar .fx-result {
    min-width: 130px;
    padding: 5px 8px;
    background: #0f172a;
    color: #86efac;
    border-radius: 4px;
    font-family: "SF Mono", Consolas, monospace;
    font-size: 13px;
    text-align: right;
    font-weight: 600;
  }
.re-taxes-wrap .formula-bar button.fx-btn {
    padding: 5px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: white;
    font-size: 12px;
    cursor: pointer;
    white-space: nowrap;
  }
.re-taxes-wrap .formula-bar button.fx-btn:hover { background: var(--accent-light); }
.re-taxes-wrap .formula-bar button.fx-btn.revert {
    border-color: var(--warn);
    color: #92400e;
  }
.re-taxes-wrap .formula-bar button.fx-btn.revert:hover { background: #fef3c7; }
.re-taxes-wrap .formula-bar button.fx-btn.accept {
    border-color: #16a34a;
    color: #166534;
    font-weight: 700;
  }
.re-taxes-wrap .formula-bar button.fx-btn.accept:hover { background: #dcfce7; }
.re-taxes-wrap .formula-bar .override-badge {
    padding: 3px 7px;
    background: var(--warn);
    color: white;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
.re-taxes-wrap /* ── Selected / Overridden cell states ────────────────────────── */
  .cell-selected, .re-taxes-wrap input.cell-selected {
    outline: 2px solid var(--accent) !important;
    outline-offset: -1px;
  }
.re-taxes-wrap .cell-overridden {
    border-left: 3px solid var(--warn) !important;
    padding-left: 5px !important;
  }
.re-taxes-wrap input.cell-overridden {
    background: #fef3c7 !important;
  }
.re-taxes-wrap .computed.cell-overridden::before {
    content: "";
    display: inline-block;
    width: 6px;
    height: 6px;
    background: var(--warn);
    border-radius: 50%;
    margin-right: 5px;
    vertical-align: middle;
  }
.re-taxes-wrap /* All cells are now selectable */
  .computed, .re-taxes-wrap input[type="text"] { cursor: cell; }
.re-taxes-wrap input[type="text"] { cursor: text; }
.re-taxes-wrap /* ── Property card (BBL / DOF) ─────────────────────────────────── */
  .prop-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 14px 18px;
    margin-bottom: 16px;
  }
.re-taxes-wrap .prop-card .prop-title {
    font-size: 15px;
    font-weight: 600;
    color: var(--text);
    margin-bottom: 2px;
  }
.re-taxes-wrap .prop-card .prop-addr {
    color: var(--muted);
    font-size: 12px;
    margin-bottom: 10px;
  }
.re-taxes-wrap .prop-card .prop-row {
    display: flex;
    align-items: flex-end;
    gap: 12px;
    flex-wrap: wrap;
  }
.re-taxes-wrap .prop-card label {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: var(--muted);
    font-weight: 600;
    display: block;
    margin-bottom: 4px;
  }
.re-taxes-wrap .prop-card .prop-field {
    display: flex;
    flex-direction: column;
    gap: 0;
  }
.re-taxes-wrap .prop-card .prop-field.f-borough { width: 150px; }
.re-taxes-wrap .prop-card .prop-field.f-block   { width: 110px; }
.re-taxes-wrap .prop-card .prop-field.f-lot     { width: 90px; }
.re-taxes-wrap .prop-card .prop-field.f-bbl     { width: 160px; }
.re-taxes-wrap .prop-card select.prop-input,
.re-taxes-wrap .prop-card input.prop-input,
.re-taxes-wrap .prop-card div.prop-input {
    box-sizing: border-box;
    width: 100%;
    height: 32px;
    padding: 6px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    font-size: 13px;
    line-height: 18px;
    background: var(--bg);
    color: var(--text);
    font-family: inherit;
    display: flex;
    align-items: center;
  }
.re-taxes-wrap .prop-card .prop-input[disabled] {
    background: var(--computed-bg);
    color: var(--muted);
    cursor: not-allowed;
  }
.re-taxes-wrap .prop-card .prop-input:not([disabled]) { background: var(--input-bg); }
.re-taxes-wrap .prop-card .lock-btn {
    padding: 5px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: white;
    cursor: pointer;
    font-size: 12px;
  }
.re-taxes-wrap .prop-card .lock-btn:hover { background: var(--accent-light); }
.re-taxes-wrap .prop-card .dof-btn {
    padding: 6px 14px;
    border: 1px solid var(--accent);
    border-radius: 4px;
    background: var(--accent);
    color: white;
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
    text-decoration: none;
    display: inline-block;
  }
.re-taxes-wrap .prop-card .dof-btn:hover { background: #1e40af; }
.re-taxes-wrap .prop-card .config-note {
    font-size: 11px;
    color: var(--muted);
    font-style: italic;
    margin-left: 6px;
  }

.re-taxes-wrap .layout.re-single-col { grid-template-columns: 1fr !important; }

.re-taxes-wrap .re-save-status { font-size: 12px; margin-left: 12px; }

</style>
<div class="re-taxes-wrap">

  <!-- Excel-style Formula Bar (sticky) -->
  <div class="formula-bar" id="formulaBar">
    <div class="cell-ref" id="fxCellRef">—</div>
    <div class="cell-label" id="fxCellLabel">Click any cell</div>
    <span class="fx-icon">ƒx</span>
    <input type="text" class="fx-input" id="fxInput" placeholder="Select a cell to see or edit its formula" disabled>
    <div class="fx-result" id="fxResult">—</div>
    <span class="override-badge" id="fxOverrideBadge" style="display:none;">OVERRIDDEN</span>
    <span class="re-save-status" id="reTaxSaveStatus"></span><button class="fx-btn accept" id="fxAcceptBtn" onclick="commitFormulaBar(); autosaveReTaxes();" style="display:none;">✓ Accept</button><button class="fx-btn revert" id="fxRevertBtn" onclick="revertActiveCell()" style="display:none;">⟲ Revert</button>
  </div>

  <!-- Property / BBL card -->
  <div class="prop-card" id="propCard">
    <div class="prop-title" id="propBuildingName">—</div>
    <div class="prop-addr" id="propAddress">—</div>
    <div class="prop-row">
      <div class="prop-field f-borough">
        <label>Borough</label>
        <select class="prop-input" id="propBorough" disabled onchange="onPropFieldChange()">
          <option value="1">Manhattan</option>
          <option value="2">Bronx</option>
          <option value="3">Brooklyn</option>
          <option value="4">Queens</option>
          <option value="5">Staten Island</option>
        </select>
      </div>
      <div class="prop-field f-block">
        <label>Block</label>
        <input type="text" class="prop-input" id="propBlock" disabled onchange="onPropFieldChange()">
      </div>
      <div class="prop-field f-lot">
        <label>Lot</label>
        <input type="text" class="prop-input" id="propLot" disabled onchange="onPropFieldChange()">
      </div>
      <div class="prop-field f-bbl">
        <label>BBL</label>
        <div class="prop-input" id="propBbl" style="background:#f1f5f9; color:var(--muted); cursor:default;">—</div>
      </div>
      <button class="lock-btn" id="propLockBtn" onclick="togglePropLock()" title="Unlock to edit BBL fields">🔒 Locked</button>
      <a class="dof-btn" id="propDofLink" href="#" target="_blank" rel="noopener noreferrer">🔗 Verify on DOF</a>
      <span class="config-note" id="propConfigNote"></span>
    </div>
  </div>

  <div class="layout re-single-col">
    <div class="main">

      <!-- SECTION 1: TAX LIABILITY COMPUTATION -->
      <div class="card">
        <h2>1. Tax Liability Computation</h2>

        <div class="section-label">Current Year — 2026/2027 Assessed Valuation (Actual, July 2026)</div>
        <table class="right-col-layout">
          <tr>
            <td>Transitional AV</td>
            <td class="num"><input type="text" id="g11" class="av-input" value="21,633,840"></td>
            <td style="color:var(--muted);width:110px;">1st Half Tax</td>
            <td class="num"><div class="computed" id="i11">—</div></td>
          </tr>
          <tr>
            <td>Tax Rate — Actual</td>
            <td class="num"><input type="text" id="g12" class="rate-input" value="12.3780%"></td>
            <td></td>
            <td></td>
          </tr>
          <tr>
            <td>Tax Rate — Adjustment (1st & 2nd Qtr)</td>
            <td class="num"><input type="text" id="g13" class="rate-input" value="0.0000%"></td>
            <td style="color:var(--muted);">Adjustment</td>
            <td class="num"><div class="computed" id="i13">—</div></td>
          </tr>
          <tr>
            <td>Less: J-51 <span class="note">(enter as negative to reduce)</span></td>
            <td class="num"><input type="text" id="i15" class="dollar-input" value="0"></td>
            <td></td>
            <td></td>
          </tr>
        </table>

        <div class="section-label">Next Year — 2027/2028 Assessed Valuation (Estimated)</div>
        <table class="right-col-layout">
          <tr>
            <td>Transitional AV — Estimated Increase %</td>
            <td class="num"><input type="text" id="d17" class="pct-input" value="6.7332%"></td>
            <td style="color:var(--muted);">Trans AV (computed)</td>
            <td class="num"><div class="computed" id="g17">—</div></td>
          </tr>
          <tr>
            <td>Tax Rate — Estimated</td>
            <td class="num"><input type="text" id="g18" class="rate-input" value="12.4390%"></td>
            <td style="color:var(--muted);">2nd Half Tax</td>
            <td class="num"><div class="computed" id="i17">—</div></td>
          </tr>
          <tr>
            <td>Tax Rate — Estimated Increase <span class="note">(auto)</span></td>
            <td class="num"><div class="computed" id="d18">—</div></td>
            <td></td>
            <td></td>
          </tr>
        </table>

        <div class="gross-banner">
          <div>
            <div class="label">Full Year Tax Liability (Gross)</div>
            <div class="note" style="color:#bfdbfe;">= 1st Half + Adjustment + Less J-51 + 2nd Half</div>
          </div>
          <div class="value" id="i19">—</div>
        </div>
      </div>

      <!-- SECTION 2: TAX BENEFITS / EXEMPTIONS -->
      <div class="card">
        <h2>2. Tax Benefits (Exemptions)</h2>
        <div class="note" style="margin-bottom:8px;">
          Enter the base-year amount. Each forward year = prior × (1 + growth %).
          Default growth is 2% but each exemption has its own editable field.
        </div>
        <table>
          <thead>
            <tr>
              <th>Exemption</th>
              <th class="num">Base (2025/2026)</th>
              <th class="num">Growth %</th>
              <th class="num">2026/2027</th>
              <th class="num">2027/2028</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Veteran</td>
              <td class="num"><input type="text" id="f26" class="dollar-input" value="1,155.38"></td>
              <td class="num"><input type="text" id="gp26" class="pct-input" value="2.00%"></td>
              <td class="num"><div class="computed" id="g26">—</div></td>
              <td class="num"><div class="computed" id="h26">—</div></td>
            </tr>
            <tr>
              <td>Senior Citizen (SCHE)</td>
              <td class="num"><input type="text" id="f27" class="dollar-input" value="16,665.51"></td>
              <td class="num"><input type="text" id="gp27" class="pct-input" value="2.00%"></td>
              <td class="num"><div class="computed" id="g27">—</div></td>
              <td class="num"><div class="computed" id="h27">—</div></td>
            </tr>
            <tr>
              <td>STAR</td>
              <td class="num"><input type="text" id="f28" class="dollar-input" value="15,547.75"></td>
              <td class="num"><input type="text" id="gp28" class="pct-input" value="2.00%"></td>
              <td class="num"><div class="computed" id="g28">—</div></td>
              <td class="num"><div class="computed" id="h28">—</div></td>
            </tr>
            <tr>
              <td>Co-op Abatement</td>
              <td class="num"><input type="text" id="f29" class="dollar-input" value="355,307.04"></td>
              <td class="num"><input type="text" id="gp29" class="pct-input" value="2.00%"></td>
              <td class="num"><div class="computed" id="g29">—</div></td>
              <td class="num"><div class="computed" id="h29">—</div></td>
            </tr>
            <tr class="total">
              <td>Total</td>
              <td class="num"><div class="computed" id="f30">—</div></td>
              <td></td>
              <td class="num"><div class="computed" id="g30">—</div></td>
              <td class="num"><div class="computed" id="h30">—</div></td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- SECTION 3: GL BUDGET LINES -->
      <div class="card">
        <div style="display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap;">
          <h2 style="margin:0;">3. GL Budget Lines</h2>
          <label style="display:inline-flex; align-items:center; gap:6px; font-size:12px; color:var(--muted); cursor:pointer; user-select:none;" title="After 10/31, YSL actuals are the source of truth for remaining months. Set May-Dec estimates to zero so 12-Mo Forecast tracks YTD.">
            <input type="checkbox" id="reAfterOct31" style="margin:0;">
            After 10/31 (YSL has full year) — zero out remaining-months estimate
          </label>
        </div>
        <div class="note" style="margin-bottom:8px; margin-top:6px;">
          <span class="ysl-badge">YSL</span> YTD Actual is pulled from this building's Yardi Select Ledger postings.
          <span class="upload-badge">UPLOAD</span> Prior Year Budget comes from the imported approved budget.
        </div>
        <table>
          <thead>
            <tr>
              <th>GL Code</th>
              <th>Label</th>
              <th class="num"><span id="reYtdHdr">YTD Actual</span> <span class="ysl-badge">YSL</span></th>
              <th class="num"><span id="reEstHdr">Estimate</span></th>
              <th class="num">12 Mo Forecast</th>
              <th class="num">Prior Year Budget <span class="upload-badge">UPLOAD</span></th>
              <th class="num">Current Year Budget</th>
            </tr>
          </thead>
          <tbody id="glRows">
            <!-- injected by JS -->
          </tbody>
        </table>
        <!-- FA #14: add a custom RE-tax escalation / adjustment line -->
        <div style="margin-top:10px; display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
          <button type="button" id="reAddRowBtn" onclick="reTaxAddRow()" style="font-size:12px; font-weight:600; padding:6px 12px; border:1px solid var(--blue,#5a4a3f); background:#fff; color:var(--blue,#5a4a3f); border-radius:6px; cursor:pointer;">+ Add Row</button>
          <span style="font-size:11px; color:var(--muted,#64748b);">Add a custom escalation / adjustment line (e.g. assessment appeal, prior-year true-up). It rolls into the totals below and into Gen&nbsp;&amp;&nbsp;Admin.</span>
        </div>
      </div>

      <div class="card">
        <h2>4. Operating Assessment</h2>
        <div class="note" style="margin-bottom:10px;">
          Proposed operating assessment for the Budget Summary = <strong>first-half RE tax &times; 2 &times; the % below</strong>.
          Default 17.5%, editable per building. Flows to the Operating Assessment (GL 4200) income row on the Summary 2027 Budget column.
          A value typed directly on that Summary cell overrides this.
        </div>
        <table>
          <tbody>
            <tr>
              <td>First-Half Tax &times; 2</td>
              <td class="num" id="reOpAssessBase">&mdash;</td>
            </tr>
            <tr>
              <td>Assessment % of taxes</td>
              <td class="num"><input type="text" id="reOpAssessPct" value="17.50%" oninput="reOpAssessOnInput()" style="width:92px; text-align:right; padding:4px 6px; border:1px solid var(--border); border-radius:4px; background:var(--input-bg); font-size:13px;"></td>
            </tr>
            <tr style="font-weight:700; border-top:2px solid var(--border);">
              <td>Proposed Operating Assessment</td>
              <td class="num" id="reOpAssessProposed">&mdash;</td>
            </tr>
          </tbody>
        </table>
      </div>

    </div>

    </div>
  </div>

  
    <div id="popBody"></div>
  </div>

</div>`;


// ─── PROPERTY CONFIG (mirrors budget_app/dof_taxes.py PROPERTY_TAX_CONFIG) ──
// `configured` = has canonical BBL & address on file; BBL fields lock by default.
// Rates here are the Class 2 residential rate placeholders; in production these
// come from DOF (and remain overrideable via the formula bar).
const PROPERTY_DEFAULTS = {
  '212': {
    building_name: '221 East 36th Owners Corp.',
    address: '225 East 36th St, New York, NY 10016',
    borough: '1', block: '00917', lot: '0017', bbl: '1-00917-0017',
    configured: true,
    class2_rate: 0.123780
  },
  '204': {
    building_name: '444 East 86th Street Owners Corp.',
    address: '444 East 86th St, New York, NY 10028',
    borough: '1', block: '01565', lot: '0029', bbl: '1-01565-0029',
    configured: true,
    class2_rate: 0.096324
  },
  '148': {
    building_name: '130 E. 18 Owners Corp.',
    address: '130 East 18th St, New York, NY 10003',
    borough: '1', block: '00878', lot: '0048', bbl: '1-00878-0048',
    configured: true,
    class2_rate: 0.093128
  },
  '206': {
    building_name: '77 Bleecker Street Corp.',
    address: '77 Bleecker St, New York, NY 10012',
    borough: '1', block: '00532', lot: '0020', bbl: '1-00532-0020',
    configured: true,
    class2_rate: 0.094396
  },
  '106': {
    building_name: '5 West 14th Owners Corp.',
    address: '10 West 15th St, New York, NY 10011',
    borough: '1', block: '00821', lot: '0021', bbl: '1-00821-0021',
    configured: true,
    class2_rate: 0.123780
  }
};
function bblUrl(bbl) {
  if (!bbl) return '#';
  // DOF Property Portal uses a borough-block-lot format with no dashes
  const clean = String(bbl).replace(/[-\s]/g, '');
  return 'https://propertyinformationportal.nyc.gov/parcels/parcel/' + clean;
}

// FA directive 2026-05-10: dropped EXCEL_TRUTH_212 (~17 lines of 212-only
// canonical Excel values used by the legacy diff panel) and DEFAULTS_212
// (~22 lines of 212-only YTD/prior-budget hardcoded values used as a
// fallback before _reLookupGlData was wired). Both shipped to every
// browser session for every coop and were entity-212-specific debug aids.
// Now: real per-entity data flows via window._data.lines; missing data
// falls back to 0 cleanly. Net JS payload reduction: ~700 bytes per page.

let GL_ROWS = [
  { gl: '6315-0000', label: 'Real Estate Tax' },
  { gl: '6315-0010', label: 'Real Estate Tax Abatement' },
  { gl: '6315-0020', label: 'STAR Exemption' },
  { gl: '6315-0025', label: 'Veteran Exemption' },
  { gl: '6315-0030', label: 'SCRIE Credit' },
  { gl: '6315-0035', label: 'SCHE Credit' },
  { gl: '6315-0040', label: 'J-51 Credit' }
];
// FA #14 (2026-06-16): the 7 GLs above are the fixed base case, identical for
// every co-op. Any additional 6315-xxxx lines a building has (custom escalations
// / adjustments added via the Section-3 "+ Add Row" button) are appended to
// GL_ROWS at render time from the backend's custom_gl_rows (see renderRETaxesTab).
// Buildings with no custom rows keep GL_ROWS exactly equal to these 7 — zero change.
const FIXED_GL_ROWS = GL_ROWS.slice();

// ─── INPUT PARSING ───────────────────────────────────────────────
function parseDollar(s) {
  if (typeof s === 'number') return s;
  if (!s) return 0;
  s = String(s).replace(/[$,\s]/g, '');
  if (/^\(.+\)$/.test(s)) s = '-' + s.slice(1, -1);
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}
function parsePct(s) {
  if (typeof s === 'number') return s;
  if (!s) return 0;
  s = String(s).trim();
  s = s.replace(/%/g, '').replace(/,/g, '');
  const n = parseFloat(s);
  if (isNaN(n)) return 0;
  // FA 724 (2026-08-18): a %-formatted field ALWAYS means percent. The old
  // suffix-sniffing kept a bare "6" as 6.0 (=600%), which cascaded a x7
  // estimated AV and x3 benefit growth into the tax math. Same rule as CAM
  // shares: entry is in percent, stored as a fraction, no guessing.
  return n / 100;
}
function fmtDollar(n) {
  if (n == null || isNaN(n)) return '$0';
  const abs = Math.abs(n);
  const str = abs.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return n < 0 ? '($' + str + ')' : '$' + str;
}
function fmtDollarWhole(n) {
  if (n == null || isNaN(n)) return '$0';
  const abs = Math.abs(Math.round(n));
  const str = abs.toLocaleString();
  return n < 0 ? '($' + str + ')' : '$' + str;
}
function fmtPct(n) {
  if (n == null || isNaN(n)) return '0.0000%';
  return (n * 100).toFixed(4) + '%';
}
function val(id) {
  const el = document.getElementById(id);
  if (!el) return 0;
  return el.tagName === 'INPUT' ? el.value : el.textContent;
}
function setComputed(id, n, mode) {
  const el = document.getElementById(id);
  if (!el) return;
  if (mode === 'pct') el.textContent = fmtPct(n);
  else if (mode === 'dollarwhole') el.textContent = fmtDollarWhole(n);
  else el.textContent = fmtDollar(n);
}

// ─── CELL METADATA REGISTRY ──────────────────────────────────────
// Every cell in the tab has an entry here, so the formula bar can
// introspect it and FAs can override any value. `type: 'input'` =
// leaf input (user fills in). `type: 'computed'` = derived by a
// formula; overriding pins it to a hard number.
const CELL_META = {
  // Section 1 — Tax Liability
  g11: { label: 'Transitional AV (current year, 26/27)', type: 'input',    format: 'dollar', excel: null },
  g12: { label: 'Tax Rate — Actual',               type: 'input',    format: 'pct',    excel: null },
  g13: { label: 'Tax Rate — Adjustment',           type: 'input',    format: 'pct',    excel: null },
  i15: { label: 'Less: J-51',                      type: 'input',    format: 'dollar', excel: null },
  d17: { label: 'Trans AV Increase %',             type: 'input',    format: 'pct',    excel: null },
  g18: { label: 'Tax Rate — Estimated',            type: 'input',    format: 'pct',    excel: null },
  i11: { label: '1st Half Tax',                    type: 'computed', format: 'dollar', excel: '=G11*G12/2' },
  i13: { label: 'Adjustment',                      type: 'computed', format: 'dollar', excel: '=G11*G13/2' },
  g17: { label: 'Trans AV (current, computed)',    type: 'computed', format: 'dollar', excel: '=G11*D17+G11' },
  i17: { label: '2nd Half Tax',                    type: 'computed', format: 'dollar', excel: '=G17*G18/2' },
  d18: { label: 'Tax Rate Est Increase',           type: 'computed', format: 'pct',    excel: '=(G18-G12)/G12' },
  i19: { label: 'Full Year Tax Liability (Gross)', type: 'computed', format: 'dollar', excel: '=I11+I13+I15+I17' },
  // Section 2 — Exemptions (4 rows × F/GP/G/H). FA directive 2026-05-10:
  // labels advanced one fiscal year. Math unchanged: F = base, G = F×(1+growth), H = G×(1+growth).
  f26: { label: 'Veteran — Base 25/26',            type: 'input',    format: 'dollar', excel: null },
  gp26:{ label: 'Veteran — Growth %',              type: 'input',    format: 'pct',    excel: null },
  g26: { label: 'Veteran — 26/27',                 type: 'computed', format: 'dollar', excel: '=F26*(1+GP26)' },
  h26: { label: 'Veteran — 27/28',                 type: 'computed', format: 'dollar', excel: '=G26*(1+GP26)' },
  f27: { label: 'SCHE — Base 25/26',               type: 'input',    format: 'dollar', excel: null },
  gp27:{ label: 'SCHE — Growth %',                 type: 'input',    format: 'pct',    excel: null },
  g27: { label: 'SCHE — 26/27',                    type: 'computed', format: 'dollar', excel: '=F27*(1+GP27)' },
  h27: { label: 'SCHE — 27/28',                    type: 'computed', format: 'dollar', excel: '=G27*(1+GP27)' },
  f28: { label: 'STAR — Base 25/26',               type: 'input',    format: 'dollar', excel: null },
  gp28:{ label: 'STAR — Growth %',                 type: 'input',    format: 'pct',    excel: null },
  g28: { label: 'STAR — 26/27',                    type: 'computed', format: 'dollar', excel: '=F28*(1+GP28)' },
  h28: { label: 'STAR — 27/28',                    type: 'computed', format: 'dollar', excel: '=G28*(1+GP28)' },
  f29: { label: 'Co-op Abatement — Base 25/26',    type: 'input',    format: 'dollar', excel: null },
  gp29:{ label: 'Co-op Abatement — Growth %',      type: 'input',    format: 'pct',    excel: null },
  g29: { label: 'Co-op Abatement — 26/27',         type: 'computed', format: 'dollar', excel: '=F29*(1+GP29)' },
  h29: { label: 'Co-op Abatement — 27/28',         type: 'computed', format: 'dollar', excel: '=G29*(1+GP29)' },
  f30: { label: 'Exemptions Total — 25/26',        type: 'computed', format: 'dollar', excel: '=SUM(F26:F29)' },
  g30: { label: 'Exemptions Total — 26/27',        type: 'computed', format: 'dollar', excel: '=SUM(G26:G29)' },
  h30: { label: 'Exemptions Total — 27/28',        type: 'computed', format: 'dollar', excel: '=SUM(H26:H29)' },
  // Totals row 47. FA directive 2026-05-10: labels are dynamic — built
  // from YTD_MONTHS at render time so they read "Jan-Apr Actual" /
  // "May-Dec Estimate" for a building with budget_period=04, etc.
  d47: { label: 'Total YTD Actual',                type: 'computed', format: 'dollar', excel: '=SUM(D40:D46)' },
  e47: { label: 'Total Estimate',                  type: 'computed', format: 'dollar', excel: '=SUM(E40:E46)' },
  f47: { label: 'Total 12 Month Forecast',         type: 'computed', format: 'dollar', excel: '=SUM(F40:F46)' },
  g47: { label: 'Total Prior Year Budget',         type: 'computed', format: 'dollar', excel: '=SUM(G40:G46)' },
  h47: { label: 'Total Current Year Budget',       type: 'computed', format: 'dollar', excel: '=SUM(H40:H46)' }
};
// Section 3 per-GL cells — meta registered after GL_ROWS is defined below.
// Excel formulas for each per-GL computed cell.
// FA directive 2026-05-19 (148 RE Tax redesign): estimate column now derives
// from Sections 1 + 2 per-GL (see _reEstFormulaEntry for the human-readable
// explanation). 12-Mo Forecast = YTD + Estimate (unchanged).
// H column (Current Year Budget) formulas unchanged — they remain the
// "anchor" derived from sections 1+2 (gross tax minus exemptions).
const GL_EXCEL_FORMULAS = {
  '6315-0000': { e: '=I11/2',  h: '=+I19' },
  '6315-0010': { e: '=-F29/4', h: '=-G29/2 + -H29/2' },
  '6315-0020': { e: '=-F28/4', h: '=-G28/2 + -H28/2' },
  '6315-0025': { e: '=-F26/4', h: '=-G26/2 + -H26/2' },
  '6315-0030': { e: '0',       h: '0' },
  '6315-0035': { e: '=-F27/4', h: '=-G27/2 + -H27/2' },
  '6315-0040': { e: '0',       h: '0' }
};
// Helper: build period-aware labels for the GL Budget Lines period split.
// FA directive 2026-05-10: labels follow YTD_MONTHS (driven by budget_period).
// Examples: YTD_MONTHS=4 → "Jan-Apr Actual" / "May-Dec Estimate"
//           YTD_MONTHS=2 → "Jan-Feb Actual" / "Mar-Dec Estimate"
//           YTD_MONTHS=0 → "YTD Actual" / "Estimate" (degenerate fallback)
function _rePeriodLabels() {
  const M = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const m = (typeof YTD_MONTHS !== 'undefined' && YTD_MONTHS > 0) ? YTD_MONTHS : 0;
  if (m <= 0 || m >= 12) {
    return { actual: 'YTD Actual', estimate: 'Estimate' };
  }
  const actualEnd = M[m - 1];        // last actual month
  const estimateStart = M[m];        // first estimate month
  return {
    actual:   'Jan-' + actualEnd + ' Actual',
    estimate: estimateStart + '-Dec Estimate',
  };
}

function _reUpdatePeriodHeaders() {
  const lbl = _rePeriodLabels();
  const ytdHdr = document.getElementById('reYtdHdr');
  const estHdr = document.getElementById('reEstHdr');
  if (ytdHdr) ytdHdr.textContent = lbl.actual;
  if (estHdr) estHdr.textContent = lbl.estimate;
  // Also refresh CELL_META so the formula bar reads the dynamic labels.
  GL_ROWS.forEach(r => {
    if (CELL_META['ytd_' + r.gl]) {
      CELL_META['ytd_' + r.gl].label = r.label + ' · ' + lbl.actual + ' (YSL)';
    }
    if (CELL_META['e_' + r.gl]) {
      CELL_META['e_' + r.gl].label = r.label + ' · ' + lbl.estimate;
    }
  });
  if (CELL_META.d47) CELL_META.d47.label = 'Total ' + lbl.actual;
  if (CELL_META.e47) CELL_META.e47.label = 'Total ' + lbl.estimate;
}

function registerGlCellMeta() {
  const lbl = _rePeriodLabels();
  GL_ROWS.forEach(r => {
    // FA #14: custom escalation rows have no GL_EXCEL_FORMULAS entry — guard so
    // their E/H Excel-formula meta is null (the live recalc still computes the
    // values; the formula bar just shows no Excel string for those cells).
    const _xl = GL_EXCEL_FORMULAS[r.gl] || {};
    CELL_META['ytd_' + r.gl] = { label: r.label + ' · ' + lbl.actual + ' (YSL)', type: 'input',    format: 'dollar', excel: null };
    CELL_META['pb_'  + r.gl] = { label: r.label + ' · Prior Year Budget',    type: 'input',    format: 'dollar', excel: null };
    CELL_META['e_'   + r.gl] = { label: r.label + ' · ' + lbl.estimate,      type: 'computed', format: 'dollar', excel: _xl.e || null };
    CELL_META['f_'   + r.gl] = { label: r.label + ' · 12 Month Forecast',    type: 'computed', format: 'dollar', excel: '=SUM(D:E)' };
    CELL_META['h_'   + r.gl] = { label: r.label + ' · Current Year Budget',  type: 'computed', format: 'dollar', excel: _xl.h || null };
    ['ytd_','pb_','e_','f_','h_'].forEach(prefix => {
      const id = prefix + r.gl;
      if (!CELL_STATE[id]) CELL_STATE[id] = { value: 0, override: null };
    });
  });
}

// ─── CELL STATE (override tracking) ──────────────────────────────
// For inputs: `value` tracks the current value (for formula bar display).
// For computed: `override` null = use formula; number = pinned override.
const CELL_STATE = {};
Object.keys(CELL_META).forEach(id => {
  CELL_STATE[id] = { value: 0, override: null };
});

let activeCellId = null;

// Format a number based on a cell's format
function fmtForCell(id, n) {
  const meta = CELL_META[id];
  if (!meta) return String(n);
  if (meta.format === 'pct') return fmtPct(n);
  if (meta.format === 'dollar') return fmtDollar(n);
  return String(n);
}
function parseForCell(id, s) {
  const meta = CELL_META[id];
  if (!meta) return 0;
  if (meta.format === 'pct') return parsePct(s);
  return parseDollar(s);
}
// Return the "raw" current value of a cell (number) — used during recalc
function cellRaw(id) {
  const st = CELL_STATE[id];
  if (st && st.override != null) return st.override;
  // Inputs read from the DOM; computed get their value set during recalc
  const meta = CELL_META[id];
  if (!meta) return 0;
  if (meta.type === 'input') {
    const el = document.getElementById(id);
    if (!el) return 0;
    return parseForCell(id, el.value);
  }
  return (st && st.value) || 0;
}

// ─── BUILD GL ROWS ───────────────────────────────────────────────
// Tax-benefit GLs that should always show YTD = 0 (FA directive 2026-05-10).
// These credits/abatements are typically posted as one annual entry near
// year-end, NOT mid-year. Any small Yardi YTD postings on these GLs are
// noise (rounding, misposting) that confuse the FA. Display 0 instead.
// 6315-0000 (Real Estate Tax expense) is NOT in this list — it has real
// quarterly activity and YTD should reflect actual Yardi data.
const _RE_NO_YTD_GLS = new Set([
  '6315-0010',  // Real Estate Tax Abatement
  '6315-0020',  // STAR Exemption
  '6315-0025',  // Veteran Exemption
  '6315-0030',  // SCRIE Credit
  '6315-0035',  // SCHE Credit
  '6315-0040',  // J-51 Credit
]);

// Look up YTD + Prior Year Budget for a given GL prefix from this
// entity's actual budget_lines (loaded into window._data on dashboard
// mount). FA directive 2026-05-10:
//   • YTD column = real Jan-(actual_end) actual posted to this GL family,
//     EXCEPT for tax-benefit GLs (6315-0010..0040) which always show 0.
//     The FA explicitly said: "incorrect tax benefits are pulling into
//     the YTD column. There are currently no YTD figures for this category."
//   • Prior Year Budget = current_budget on this entity's BudgetLine
//     (still real per-entity data — these credits ARE budgeted annually,
//     just not posted to YTD until year-end).
//   • Replaces the hardcoded DEFAULTS_212 values that polluted every other
//     coop's RE Tax tab with entity 212's numbers.
function _reLookupGlData(glPrefix) {
  const lines = (window._data && Array.isArray(window._data.lines))
    ? window._data.lines : [];
  let ytd = 0, priorBudget = 0;
  for (const ln of lines) {
    const gl = String(ln.gl_code || '').trim();
    if (!gl) continue;
    // Match either exact (6315-0000) or prefix (6315-0010 matches 6315-0010-001 etc.)
    if (gl === glPrefix || gl.startsWith(glPrefix + '-') || gl.startsWith(glPrefix + '.')) {
      ytd += Number(ln.ytd_actual || 0);
      priorBudget += Number(ln.current_budget || 0);
    }
  }
  // FA directive: force 0 YTD for tax-benefit GLs even if Yardi has small
  // postings. Prior Year Budget stays as the real budgeted credit amount.
  if (_RE_NO_YTD_GLS.has(glPrefix)) {
    ytd = 0;
  }
  return { ytd, priorBudget };
}

function buildGlRows() {
  const tbody = document.getElementById('glRows');
  let html = '';
  GL_ROWS.forEach((r, i) => {
    const glKey = r.gl;
    const ytdId = 'ytd_' + glKey;
    const pbId  = 'pb_'  + glKey;
    const eId   = 'e_'   + glKey;
    const fId   = 'f_'   + glKey;
    const hId   = 'h_'   + glKey;
    // Pull live data from this entity's budget_lines (FA directive 2026-05-10).
    // Pre-2026-05-10 we fell back to DEFAULTS_212 hardcoded values when
    // window._data was unavailable; that's been removed (it was 212-only
    // debug data shipped to every coop). Now: 0 when no data.
    const live = _reLookupGlData(glKey);
    const ytdDefault = live.ytd;
    const pbDefault  = live.priorBudget;
    html += `<tr>
      <td class="gl-code">${glKey}</td>
      <td>${r.label}</td>
      <td class="num"><input type="text" id="${ytdId}" class="dollar-input" value="${ytdDefault.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}"></td>
      <td class="num"><div class="computed formula-clickable" id="${eId}" >—</div></td>
      <td class="num"><div class="computed formula-clickable" id="${fId}" >—</div></td>
      <td class="num"><input type="text" id="${pbId}" class="dollar-input" value="${pbDefault.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}"></td>
      <td class="num"><div class="computed formula-clickable" id="${hId}" >—</div></td>
    </tr>`;
  });
  html += `<tr class="total">
    <td colspan="2">Total</td>
    <td class="num"><div class="computed formula-clickable" id="d47" >—</div></td>
    <td class="num"><div class="computed formula-clickable" id="e47" >—</div></td>
    <td class="num"><div class="computed formula-clickable" id="f47" >—</div></td>
    <td class="num"><div class="computed formula-clickable" id="g47" >—</div></td>
    <td class="num"><div class="computed formula-clickable" id="h47" >—</div></td>
  </tr>`;
  tbody.innerHTML = html;
}

// Global state captured each recalc so the formula popover can read substituted values
const STATE = {};

// Helper used by recalc — if a computed cell has an override, return it;
// otherwise use the freshly-computed formula value. Also stores result on
// CELL_STATE so the formula bar can read it back.
function computedOrOverride(id, formulaVal) {
  const st = CELL_STATE[id] || (CELL_STATE[id] = { value: 0, override: null });
  const final = (st.override != null) ? st.override : formulaVal;
  st.value = final;
  return final;
}

// ─── CORE RECALC (mirrors 212 RE Taxes tab formulas exactly) ──────
function recalc() {
  // Inputs — read via cellRaw which respects overrides on input cells too
  const G11 = cellRaw('g11');
  const G12 = cellRaw('g12');
  const G13 = cellRaw('g13');
  const I15 = cellRaw('i15');
  const D17 = cellRaw('d17');
  const G18 = cellRaw('g18');

  // Keep input value in state so formula bar reflects typed values
  CELL_STATE.g11.value = G11;
  CELL_STATE.g12.value = G12;
  CELL_STATE.g13.value = G13;
  CELL_STATE.i15.value = I15;
  CELL_STATE.d17.value = D17;
  CELL_STATE.g18.value = G18;

  // Section 1 formulas
  const I11 = computedOrOverride('i11', (G11 * G12) / 2);           // =G11*G12/2
  const I13 = computedOrOverride('i13', (G11 * G13) / 2);           // =G11*G13/2
  const G17 = computedOrOverride('g17', G11 * D17 + G11);           // =G11*D17+G11
  const D18 = computedOrOverride('d18', G12 !== 0 ? (G18 - G12) / G12 : 0); // =(G18-G12)/G12
  const I17 = computedOrOverride('i17', (G17 * G18) / 2);           // =G17*G18/2
  const I19 = computedOrOverride('i19', I11 + I13 + I15 + I17);     // =SUM(I11:I18)

  setComputed('i11', I11);
  setComputed('i13', I13);
  setComputed('g17', G17, 'dollarwhole');
  setComputed('i17', I17);
  setComputed('d18', D18, 'pct');
  document.getElementById('i19').textContent = fmtDollar(I19);

  // Section 2 — exemptions (overrides honored on F, GP, G, H)
  const exemptions = [
    { base: 'f26', gp: 'gp26', g: 'g26', h: 'h26' },
    { base: 'f27', gp: 'gp27', g: 'g27', h: 'h27' },
    { base: 'f28', gp: 'gp28', g: 'g28', h: 'h28' },
    { base: 'f29', gp: 'gp29', g: 'g29', h: 'h29' }
  ];
  let F30 = 0, G30 = 0, H30 = 0;
  const ex = {};
  exemptions.forEach(e => {
    const F = cellRaw(e.base);          // input — override returns override
    const gp = cellRaw(e.gp);
    CELL_STATE[e.base].value = F;
    CELL_STATE[e.gp].value = gp;
    const G = computedOrOverride(e.g, F * (1 + gp));
    const H = computedOrOverride(e.h, G * (1 + gp));
    setComputed(e.g, G);
    setComputed(e.h, H);
    ex[e.base] = F; ex[e.g] = G; ex[e.h] = H;
    F30 += F; G30 += G; H30 += H;
  });
  F30 = computedOrOverride('f30', F30);
  G30 = computedOrOverride('g30', G30);
  H30 = computedOrOverride('h30', H30);
  setComputed('f30', F30);
  setComputed('g30', G30);
  setComputed('h30', H30);

  // Section 3 — GL lines.
  // FA directive 2026-05-10: H column (Current Year Budget) is the anchor
  // computed from sections 1+2. E column (Estimate) is period-independent
  // and equals H - YTD — meaning "the remaining months will close the gap
  // between what's posted and the full-year budget". F column (12-Mo
  // Forecast) = YTD + E = H. Old quarter-based formulas (=I11/2, =-G29/4)
  // were wired to entity 212's Jul-Sep / Oct-Dec calendar only and gave
  // wrong numbers for any other budget_period.
  const glHFormulas = {
    '6315-0000': I19,                                     // H = +I19 (gross)
    '6315-0010': -ex.g29 / 2 - ex.h29 / 2,                // H = -(G29+H29)/2
    '6315-0020': -ex.g28 / 2 - ex.h28 / 2,                // H = -(G28+H28)/2
    '6315-0025': -ex.g26 / 2 - ex.h26 / 2,                // H = -(G26+H26)/2
    '6315-0030': 0,                                       // SCRIE — always 0
    '6315-0035': -ex.g27 / 2 - ex.h27 / 2,                // H = -(G27+H27)/2
    '6315-0040': 0                                        // J-51 — always 0
  };

  // FA dir 2026-05-19 (148 RE Tax redesign): May-Dec estimate formulas now
  // come from Sections 1 + 2 directly instead of (H - YTD).
  //   6315-0000 Real Estate Tax    : Section 1 1st-half tax / 2 = I11 / 2
  //   6315-0010 Co-op Abatement    : -(Section 2 Co-op Abatement base / 4) = -F29/4
  //   6315-0020 STAR Exemption     : -F28/4
  //   6315-0025 Veteran Exemption  : -F26/4
  //   6315-0030 SCRIE Credit       : 0 (no Section 2 input — FA can override)
  //   6315-0035 SCHE Credit        : -F27/4
  //   6315-0040 J-51 Credit        : 0 (no Section 2 input — FA can override)
  // After-10/31 toggle (STATE.afterOct31): all estimates = 0, YSL actuals
  // are then the source of truth for remaining months.
  const afterOct31 = !!(STATE && STATE.afterOct31);
  const glEFormulas = afterOct31 ? {
    '6315-0000': 0, '6315-0010': 0, '6315-0020': 0, '6315-0025': 0,
    '6315-0030': 0, '6315-0035': 0, '6315-0040': 0,
  } : {
    '6315-0000': I11 / 2,
    '6315-0010': -(ex.f29 || 0) / 4,
    '6315-0020': -(ex.f28 || 0) / 4,
    '6315-0025': -(ex.f26 || 0) / 4,
    '6315-0030': 0,
    '6315-0035': -(ex.f27 || 0) / 4,
    '6315-0040': 0,
  };

  let D47 = 0, E47 = 0, F47 = 0, G47 = 0, H47 = 0;
  STATE.rows = {};
  GL_ROWS.forEach(r => {
    const glKey = r.gl;
    const D = cellRaw('ytd_' + glKey);            // input (YTD Actual from YSL)
    const G = cellRaw('pb_'  + glKey);            // input (Prior Year Budget)
    CELL_STATE['ytd_' + glKey].value = D;
    CELL_STATE['pb_'  + glKey].value = G;
    // FA #14: custom escalation rows have no glHFormulas/glEFormulas entry.
    // Default their current-year budget (H) to the YTD actual the FA enters, so
    // an added line flows straight into Forecast/Budget/Total (E = H - D = 0,
    // F = D + E = D). Fixed rows keep their tax-derived H formula unchanged.
    const _hFormula = (glHFormulas[glKey] !== undefined) ? glHFormulas[glKey] : D;
    const H = computedOrOverride('h_' + glKey, _hFormula);    // computed first
    // FA dir 2026-05-19: estimate now from Sections 1/2 (per glEFormulas)
    const eDefault = (glEFormulas[glKey] !== undefined) ? glEFormulas[glKey] : (H - D);
    const E = computedOrOverride('e_' + glKey, eDefault);
    const F = computedOrOverride('f_' + glKey, D + E);                 // forecast = ytd + estimate
    setComputed('e_' + glKey, E);
    setComputed('f_' + glKey, F);
    setComputed('h_' + glKey, H);
    D47 += D; E47 += E; F47 += F; G47 += G; H47 += H;
    STATE.rows[glKey] = { D, E, F, G, H, label: r.label };
  });
  D47 = computedOrOverride('d47', D47);
  E47 = computedOrOverride('e47', E47);
  F47 = computedOrOverride('f47', F47);
  G47 = computedOrOverride('g47', G47);
  H47 = computedOrOverride('h47', H47);
  STATE.I11 = I11; STATE.I17 = I17; STATE.I19 = I19;
  STATE.G26 = ex.g26; STATE.H26 = ex.h26;
  STATE.G27 = ex.g27; STATE.H27 = ex.h27;
  STATE.G28 = ex.g28; STATE.H28 = ex.h28;
  STATE.G29 = ex.g29; STATE.H29 = ex.h29;
  STATE.D47 = D47; STATE.E47 = E47; STATE.F47 = F47; STATE.G47 = G47; STATE.H47 = H47;
  setComputed('d47', D47);
  setComputed('e47', E47);
  setComputed('f47', F47);
  setComputed('g47', G47);
  setComputed('h47', H47);

  // GL output panel (preview-only debug panel, stripped in production port)

  // Live re-eval of any user-entered override formulas. Runs AFTER the main
  // formula pass so override cells see up-to-date dependency values. Updates
  // the DOM cell and CELL_STATE so subsequent reads are consistent.
  Object.keys(CELL_STATE).forEach(id => {
    const st = CELL_STATE[id];
    if (!st || !st.overrideSrc) return;
    try {
      const v = _reEvalFormula(st.overrideSrc);
      st.override = v;
      st.value = v;
      const meta = CELL_META[id];
      if (!meta) return;
      const el = document.getElementById(id);
      if (!el) return;
      if (meta.type === 'computed') {
        el.textContent = fmtForCell(id, v);
      } else if (el.tagName === 'INPUT') {
        el.value = fmtForCell(id, v);
      }
    } catch (e) {
      // Leave previous override in place if re-eval fails
    }
  });

  // Paint override indicators + refresh formula bar for the active cell
  paintOverrideIndicators();
  syncFormulaBar();

  // Diff panel
  renderDiff({
    i11: I11, i13: I13, i17: I17, i19: I19, g17: G17, d18: D18,
    f26: ex.f26, g26: ex.g26, h26: ex.h26,
    f27: ex.f27, g27: ex.g27, h27: ex.h27,
    f28: ex.f28, g28: ex.g28, h28: ex.h28,
    f29: ex.f29, g29: ex.g29, h29: ex.h29,
    f30: F30, g30: G30, h30: H30,
    h40: glHFormulas['6315-0000'],
    h41: glHFormulas['6315-0010'],
    h42: glHFormulas['6315-0020'],
    h43: glHFormulas['6315-0025'],
    h44: glHFormulas['6315-0030'],
    h45: glHFormulas['6315-0035'],
    h46: glHFormulas['6315-0040'],
    h47: H47, d47: D47, e47: E47, f47: F47, g47: G47
  });
}

// ─── FORMULA POPOVER (click on a Section 3 computed cell) ─────────
// FA directive 2026-05-19 (148 RE Tax redesign): estimate column now derives
// from Sections 1 + 2 per-GL. The popover shows the actual formula for each
// GL, and switches to "zero" messaging when the After-10/31 toggle is on.
// RE-Tax formula POPOVER builders removed 2026-06-08: the popover was replaced by
// the unified formula bar (CELL_META.excel + _xlNum, valid Excel). _reEstFormulaEntry /
// FORMULA_DEFS / fcastFormula / totalFormula were dead (FORMULA_DEFS was never read)
// and used Unicode operators. Nothing live referenced them.

function showFormula(ev, el, glKey, col) { /* popover removed — formula bar shows it */ }

function hideFormulaPopover() { /* removed */ }

// popover outside-click handler removed

function renderDiff(computed) { /* diff panel removed in production port */ }

function copyJson() { /* removed in production port */ }

function toggleDiff() { /* removed in production port */ }

function resetDefaults() { refreshDOFData(); }

// ─── PROPERTY / BBL CARD ─────────────────────────────────────────
let propLocked = true;   // BBL fields start locked when property is configured

function loadProperty() {
  const p = window._reActiveEntity || '204';
  const d = PROPERTY_DEFAULTS[p];
  const card = document.getElementById('propCard');
  document.getElementById('propBuildingName').textContent = d ? d.building_name : 'Unknown entity';
  document.getElementById('propAddress').textContent     = d ? d.address       : '—';
  const boroEl  = document.getElementById('propBorough');
  const blockEl = document.getElementById('propBlock');
  const lotEl   = document.getElementById('propLot');
  if (d) {
    boroEl.value  = d.borough;
    blockEl.value = d.block;
    lotEl.value   = d.lot;
  } else {
    boroEl.value = '1'; blockEl.value = ''; lotEl.value = '';
  }
  propLocked = !!(d && d.configured);
  applyPropLock();
  // Populate the estimated tax rate (Class 2 residential) into G18 if not yet overridden
  if (d && d.class2_rate && CELL_STATE.g18 && CELL_STATE.g18.override == null) {
    const g18el = document.getElementById('g18');
    if (g18el) {
      g18el.value = (d.class2_rate * 100).toFixed(4) + '%';
    }
  }
  onPropFieldChange();
  recalc();
}

function applyPropLock() {
  const locked = propLocked;
  ['propBorough','propBlock','propLot'].forEach(id => {
    document.getElementById(id).disabled = locked;
  });
  const btn = document.getElementById('propLockBtn');
  btn.textContent = locked ? '🔒 Locked' : '✏️ Editing';
  btn.title = locked ? 'Unlock to edit BBL fields' : 'Click to lock fields';
  document.getElementById('propConfigNote').textContent =
    locked ? 'from config — click the lock to edit' : 'unlocked — edits are local to this preview';
}

function togglePropLock() {
  propLocked = !propLocked;
  applyPropLock();
}

function onPropFieldChange() {
  const boro  = document.getElementById('propBorough').value;
  const block = document.getElementById('propBlock').value.trim();
  const lot   = document.getElementById('propLot').value.trim();
  // Build canonical BBL string (B-BBBBB-LLLL) when all parts present
  let bbl = '';
  if (boro && block && lot) {
    bbl = boro + '-' + block.padStart(5, '0') + '-' + lot.padStart(4, '0');
  }
  document.getElementById('propBbl').textContent = bbl || '—';
  const link = document.getElementById('propDofLink');
  if (bbl) {
    link.href = bblUrl(bbl);
    link.style.opacity = '1';
    link.style.pointerEvents = 'auto';
  } else {
    link.href = '#';
    link.style.opacity = '0.5';
    link.style.pointerEvents = 'none';
  }
}

// ─── EXCEL-STYLE FORMULA BAR ─────────────────────────────────────
function selectCell(id) {
  if (!CELL_META[id]) return;
  // Clear previous selection highlight
  if (activeCellId) {
    const prev = document.getElementById(activeCellId);
    if (prev) prev.classList.remove('cell-selected');
  }
  activeCellId = id;
  const el = document.getElementById(id);
  if (el) el.classList.add('cell-selected');
  syncFormulaBar();
}

// Format a raw numeric value according to a cell format type (no cell ID needed).
function _reFmtByFormat(val, format) {
  const n = (typeof val === 'number' && !isNaN(val)) ? val : 0;
  if (format === 'pct') return fmtPct(n);
  if (format === 'dollar') return fmtDollar(n);
  return String(n);
}

// Raw, Excel-SAFE number for substituting into a formula bar (no $, no commas, no %).
// Those characters make the formula unparseable ("Invalid formula") and break export.
// Dollars round to whole numbers; ratios/percents keep precision so the math stays exact.
function _xlNum(val, format) {
  const n = (typeof val === 'number' && !isNaN(val)) ? val : 0;
  return (format === 'dollar') ? String(Math.round(n)) : String(n);
}

// Replace cell references in an Excel-style formula with their current numeric
// values. Handles SUM(X1:X9) column ranges by summing the member cells.
// Unknown tokens are left literal so we don't corrupt the expression shape.
function _reSubstituteFormulaWithNumbers(formula) {
  if (!formula) return '';
  let s = String(formula);
  // 1. Expand SUM(X1:X9) → sum of member CELL_STATE.values (formatted)
  s = s.replace(/SUM\(([A-Z]+)(\d+):([A-Z]+)(\d+)\)/gi, (full, c1, n1, c2, n2) => {
    if (c1.toLowerCase() !== c2.toLowerCase()) return full;
    const start = parseInt(n1, 10);
    const end   = parseInt(n2, 10);
    if (!(end >= start)) return full;
    let sum = 0, fmt = 'dollar', anyFound = false;
    for (let i = start; i <= end; i++) {
      const id = c1.toLowerCase() + i;
      const st = CELL_STATE[id];
      if (st && typeof st.value === 'number') {
        sum += st.value;
        const m = CELL_META[id];
        if (m && m.format) fmt = m.format;
        anyFound = true;
      }
    }
    return anyFound ? _xlNum(sum, fmt) : full;
  });
  // 2. Replace individual cell tokens (case-insensitive) with formatted values
  s = s.replace(/\b([A-Z]{1,3}\d{1,3})\b/gi, (token) => {
    const id = token.toLowerCase();
    const st = CELL_STATE[id];
    const meta = CELL_META[id];
    if (!st || !meta) return token;
    const v = (typeof st.value === 'number') ? st.value : 0;
    return _xlNum(v, meta.format);
  });
  return s;
}

function syncFormulaBar() {
  const barRef    = document.getElementById('fxCellRef');
  const barLabel  = document.getElementById('fxCellLabel');
  const barInput  = document.getElementById('fxInput');
  const barResult = document.getElementById('fxResult');
  const badge     = document.getElementById('fxOverrideBadge');
  const revertBtn = document.getElementById('fxRevertBtn');
  const acceptBtn = document.getElementById('fxAcceptBtn');
  if (!activeCellId) {
    barRef.textContent = 'Click any cell';
    barLabel.textContent = '';
    barInput.value = '';
    barInput.disabled = true;
    barResult.textContent = '—';
    badge.style.display = 'none';
    revertBtn.style.display = 'none';
    if (acceptBtn) acceptBtn.style.display = 'none';
    return;
  }
  const meta = CELL_META[activeCellId];
  const st   = CELL_STATE[activeCellId];
  barRef.textContent = meta.label;
  barLabel.textContent = '';
  barInput.disabled = false;
  const overridden = (st && st.override != null);
  if (meta.type === 'input') {
    // Input cell: show the bare value (Excel-style, no $/commas) for dollar cells;
    // keep % formatting for percent cells (e.g. Tax Rate) so value entry isn't confusing.
    barInput.value = (meta.format === 'pct') ? fmtForCell(activeCellId, cellRaw(activeCellId)) : String(Math.round(cellRaw(activeCellId) || 0));
  } else {
    // Computed cell — show user override formula raw; numeric override formatted;
    // built-in formula with cell refs replaced by live numeric values.
    if (st && st.overrideSrc) {
      barInput.value = st.overrideSrc;
    } else if (overridden) {
      barInput.value = (meta.format === 'pct') ? fmtForCell(activeCellId, st.override) : String(Math.round(st.override || 0));
    } else if (activeCellId.charAt(0) === 'f' && activeCellId.charAt(1) === '_') {
      // 12-Month Forecast = YTD (D) + Estimate (E). These per-GL columns are
      // not A1-addressable, so the generic ref-substituter can't expand
      // "=SUM(D:E)" — build the numeric equation directly from STATE.rows so
      // the bar shows real numbers (e.g. "= $125,000 + $30,000"), not refs.
      const _gl = activeCellId.slice(2);
      const _r = (STATE.rows && STATE.rows[_gl]) ? STATE.rows[_gl] : null;
      barInput.value = _r ? sumExcelExpr([_r.D || 0, _r.E || 0])
                          : _reSubstituteFormulaWithNumbers(meta.excel || '');
    } else {
      barInput.value = _reSubstituteFormulaWithNumbers(meta.excel || '');
    }
  }
  barResult.textContent = fmtForCell(activeCellId, (st && st.value) || 0);
  badge.style.display = overridden ? 'inline-block' : 'none';
  revertBtn.style.display = overridden ? 'inline-block' : 'none';
  if (acceptBtn) acceptBtn.style.display = 'inline-block';
}

// ─── FORMULA EVALUATOR ─────────────────────────────────────────────────
// Supports: SUM, AVERAGE/AVG, MIN, MAX, COUNT, IF, ROUND, ABS
//           + arithmetic (+ - * / parens)
//           + comparisons (< > <= >= == !=)
//           + cell refs (G11, GP26, etc.)
//           + ranges (F26:F29)
//           + trailing % (5% → 0.05)
//           + nested function calls
//
// Strategy: pre-process ranges into JS array literals, substitute cell refs
// with their numeric values, then evaluate with the Function constructor and
// inject the supported functions by parameter (strict mode blocks unknown
// identifiers, so unsupported names throw cleanly).
const _RE_FN_NAMES = ['SUM','AVERAGE','AVG','MIN','MAX','COUNT','IF','ROUND','ABS'];

function _reMakeFnImpls() {
  const flat = (args) => args.flat(Infinity).map(Number).filter(n => !isNaN(n));
  return {
    SUM:     (...a) => flat(a).reduce((x, y) => x + y, 0),
    AVERAGE: (...a) => { const f = flat(a); return f.length ? f.reduce((x,y)=>x+y,0) / f.length : 0; },
    AVG:     (...a) => { const f = flat(a); return f.length ? f.reduce((x,y)=>x+y,0) / f.length : 0; },
    MIN:     (...a) => { const f = flat(a); return f.length ? Math.min.apply(null, f) : 0; },
    MAX:     (...a) => { const f = flat(a); return f.length ? Math.max.apply(null, f) : 0; },
    COUNT:   (...a) => flat(a).length,
    IF:      (c, x, y) => (c ? Number(x) : Number(y)),
    ROUND:   (x, n) => { const k = Math.pow(10, n || 0); return Math.round(Number(x) * k) / k; },
    ABS:     (x) => Math.abs(Number(x)),
  };
}

function _reEvalFormula(expr) {
  if (expr == null) throw new Error('Empty formula');
  let s = String(expr).trim();
  if (s.charAt(0) === '=') s = s.slice(1).trim();
  if (!s) throw new Error('Empty formula');

  // 1. Expand range refs X1:X9 → JS array literal [v1,v2,...]
  s = s.replace(/\b([A-Z]+)(\d+)\s*:\s*([A-Z]+)(\d+)\b/gi, (full, c1, n1, c2, n2) => {
    if (c1.toLowerCase() !== c2.toLowerCase()) throw new Error('Cross-column range not supported: ' + full);
    const col = c1.toLowerCase();
    const lo = Math.min(+n1, +n2), hi = Math.max(+n1, +n2);
    const vals = [];
    for (let i = lo; i <= hi; i++) {
      const st = CELL_STATE[col + i];
      vals.push(st && typeof st.value === 'number' ? st.value : 0);
    }
    return '[' + vals.join(',') + ']';
  });

  // 2. Substitute individual cell refs (skip tokens that match allowed function names)
  const fnSet = new Set(_RE_FN_NAMES);
  const unknown = [];
  s = s.replace(/\b([A-Z]{1,3}\d{1,3})\b/gi, (tok) => {
    const id = tok.toLowerCase();
    const st = CELL_STATE[id];
    if (!st || typeof st.value !== 'number') { unknown.push(tok); return '0'; }
    return '(' + st.value + ')';
  });
  if (unknown.length) throw new Error('Unknown cell(s): ' + unknown.join(', '));

  // 3. Trailing % on numbers → /100 (supports =G11*5%)
  s = s.replace(/(\d+(?:\.\d+)?)%/g, '($1/100)');

  // 4. Reject any stray alphabetic identifier that isn't one of the allowed functions
  const stripped = s.replace(/\b(SUM|AVERAGE|AVG|MIN|MAX|COUNT|IF|ROUND|ABS)\b/gi, '');
  const badIdent = stripped.match(/[A-Za-z_][A-Za-z_0-9]*/);
  if (badIdent) throw new Error('Unsupported: ' + badIdent[0]);

  // 5. Structural whitelist — block statement separators, block literals, etc.
  if (/[;{}`$\\]/.test(s)) throw new Error('Invalid characters');

  // 6. Evaluate with injected function implementations
  const impls = _reMakeFnImpls();
  let result;
  try {
    result = new Function(
      'SUM','AVERAGE','AVG','MIN','MAX','COUNT','IF','ROUND','ABS',
      '"use strict"; return (' + s + ');'
    )(impls.SUM, impls.AVERAGE, impls.AVG, impls.MIN, impls.MAX, impls.COUNT, impls.IF, impls.ROUND, impls.ABS);
  } catch (e) {
    throw new Error('Parse error');
  }
  if (typeof result !== 'number' || !isFinite(result)) throw new Error('Non-numeric result');
  return result;
}

// Show a transient error in the formula bar result area.
function _reShowFormulaError(msg) {
  const barResult = document.getElementById('fxResult');
  if (!barResult) return;
  const prevText = barResult.textContent;
  const prevColor = barResult.style.color;
  barResult.textContent = '✗ ' + msg;
  barResult.style.color = '#dc2626';
  setTimeout(() => {
    barResult.textContent = prevText;
    barResult.style.color = prevColor || '';
  }, 3000);
}

function commitFormulaBar() {
  if (!activeCellId) return;
  const meta = CELL_META[activeCellId];
  const st   = CELL_STATE[activeCellId];
  const raw  = document.getElementById('fxInput').value.trim();
  const isFormula = raw.charAt(0) === '=';

  if (meta.type === 'input') {
    // Input cell: formula → evaluate and stamp the numeric result; plain → write literal
    if (isFormula) {
      try {
        const v = _reEvalFormula(raw);
        const el = document.getElementById(activeCellId);
        if (el) el.value = String(v);
      } catch (e) {
        _reShowFormulaError(e.message);
        return;
      }
    } else {
      const el = document.getElementById(activeCellId);
      if (el) el.value = raw;
    }
    recalc();
    return;
  }

  // Computed cell
  if (raw === '' || raw === meta.excel) {
    st.override = null;
    st.overrideSrc = null;
  } else if (isFormula) {
    // Live formula override — stored as source so recalc() re-evaluates it
    try {
      const v = _reEvalFormula(raw);
      st.override = v;
      st.overrideSrc = raw;
    } catch (e) {
      _reShowFormulaError(e.message);
      return;
    }
  } else {
    // Numeric override
    st.override = parseForCell(activeCellId, raw);
    st.overrideSrc = null;
  }
  recalc();
}

function revertActiveCell() {
  if (!activeCellId) return;
  const meta = CELL_META[activeCellId];
  const st   = CELL_STATE[activeCellId];
  if (meta.type === 'computed') {
    st.override = null;
    st.overrideSrc = null;
  } else {
    // Revert an input: clear value to 0 — user can retype
    const el = document.getElementById(activeCellId);
    if (el) el.value = '0';
  }
  recalc();
}

// Attach click-to-select to every registered cell
function wireCellSelection() {
  Object.keys(CELL_META).forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    if (el.dataset.fxWired === '1') return;
    el.dataset.fxWired = '1';
    el.addEventListener('click', (e) => {
      e.stopPropagation();
      selectCell(id);
    });
    // Remove the stale formula-popover handler so we only select
    if (el.hasAttribute('onclick')) el.removeAttribute('onclick');
  });
}

// Update each cell's visual override indicator after recalc
function paintOverrideIndicators() {
  Object.keys(CELL_META).forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    const overridden = CELL_STATE[id] && CELL_STATE[id].override != null;
    el.classList.toggle('cell-overridden', !!overridden);
  });
}

// ─── INIT (called by renderRETaxesTab) ──
function initReTaxesTab() {
  buildGlRows();
  registerGlCellMeta();        // add Section 3 cells to CELL_META/CELL_STATE
  _reUpdatePeriodHeaders();    // FA directive 2026-05-10: dynamic period labels
  wireCellSelection();         // click any cell → formula bar

  // Input cells: recalc on edit + trigger debounced autosave
  document.querySelectorAll('.re-taxes-wrap input[type="text"]').forEach(el => {
    if (el.closest('.prop-card') || el.closest('.formula-bar')) return;
    el.addEventListener('input', () => { recalc(); autosaveReTaxes(); });
    el.addEventListener('blur', () => { recalc(); autosaveReTaxes(); });
  });

  // Formula bar: Enter commits; Escape cancels. Autosave on commit.
  const fxInputEl = document.getElementById('fxInput');
  if (fxInputEl) {
    fxInputEl.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); commitFormulaBar(); autosaveReTaxes(); }
      else if (e.key === 'Escape') { e.preventDefault(); syncFormulaBar(); fxInputEl.blur(); }
    });
    fxInputEl.addEventListener('blur', () => {
      if (activeCellId) { commitFormulaBar(); autosaveReTaxes(); }
    });
  }

  // FA dir 2026-05-19: After-10/31 toggle. When checked, Section 3 estimates
  // zero out (YSL actuals become the source of truth for remaining months).
  // Persisted via re_taxes_overrides.after_oct31 so it round-trips on reload.
  const afterOct31El = document.getElementById('reAfterOct31');
  if (afterOct31El) {
    afterOct31El.addEventListener('change', () => {
      STATE.afterOct31 = !!afterOct31El.checked;
      recalc();
      autosaveReTaxes();
    });
  }
}

// ─── BACKEND BRIDGE ──────────────────────────────────────────────
// Back-solve exemption base from current + growth, handling growth=-1 edge case.
function _reBaseFromCurrent(current, growth) {
  const g = parseFloat(growth) || 0;
  const c = parseFloat(current) || 0;
  if (g <= -0.999999) return c;
  return c / (1 + g);
}

// Populate all cells from the backend /api/re-taxes response.
function reTaxLoadFromBackend(re) {
  if (!re) return;
  _reSuppressAutosave = true;  // don't trigger autosave during initial load
  try {
    // Property card — populated from PROPERTY_DEFAULTS via the active entity
    _rePopulatePropertyCard(re.entity_code, re);

    // Section 1 — tax liability
    _reSetInput('g11', re.assessed_value || 0, 'dollar');
    _reSetInput('g12', re.tax_rate || 0, 'pct');
    _reSetInput('d17', re.transitional_av_increase || 0, 'pct');
    _reSetInput('g18', re.est_tax_rate || 0, 'pct');
    // g13 (rate adjustment) and i15 (J-51) — UI-only, default to 0 unless overrides exist
    _reSetInput('g13', re.rate_adjustment || 0, 'pct');
    _reSetInput('i15', re.j51_amount || 0, 'dollar');

    // Section 2 — exemptions (back-solve F column from backend's "current" = 25/26)
    const ex = re.exemptions || {};
    const pairs = [
      ['f26', 'gp26', ex.veteran],
      ['f27', 'gp27', ex.sche],
      ['f28', 'gp28', ex.star],
      ['f29', 'gp29', ex.coop_abatement],
    ];
    pairs.forEach(([fid, gpid, e]) => {
      if (!e) return;
      const g = e.growth_pct || 0;
      const c = e.current_year || 0;
      _reSetInput(fid, _reBaseFromCurrent(c, g), 'dollar');
      _reSetInput(gpid, g, 'pct');
    });

    // FA dir 2026-05-19: After-10/31 toggle round-trip.
    STATE.afterOct31 = !!re.after_oct31;
    const afterOct31El = document.getElementById('reAfterOct31');
    if (afterOct31El) afterOct31El.checked = STATE.afterOct31;

    // FA dir 2026-06-03 (#6): operating-assessment % + computed proposed.
    try { _reUpdateOpAssess(re); } catch (e) { console.error('op-assess load', e); }

    // Restore saved per-cell overrides (numeric values + user-typed formula sources).
    // Each entry is either a raw number (legacy) or {override, overrideSrc}.
    if (re.cell_overrides) {
      Object.entries(re.cell_overrides).forEach(([id, val]) => {
        if (!CELL_STATE[id]) return;
        if (val && typeof val === 'object') {
          CELL_STATE[id].override    = (val.override != null) ? val.override : null;
          CELL_STATE[id].overrideSrc = val.overrideSrc || null;
        } else {
          CELL_STATE[id].override    = val;
          CELL_STATE[id].overrideSrc = null;
        }
      });
    }

    recalc();
  } finally {
    _reSuppressAutosave = false;
  }
}

// Helper to set an input element's value, respecting format
function _reSetInput(id, val, format) {
  const el = document.getElementById(id);
  if (!el) return;
  if (format === 'pct') {
    el.value = (val * 100).toFixed(4) + '%';
  } else if (format === 'dollar') {
    el.value = Number(val).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  } else {
    el.value = String(val);
  }
  if (CELL_STATE[id]) CELL_STATE[id].value = val;
}

// Populate the property card from PROPERTY_DEFAULTS + backend response
function _rePopulatePropertyCard(entityCode, re) {
  const d = PROPERTY_DEFAULTS[entityCode];
  const nameEl = document.getElementById('propBuildingName');
  const addrEl = document.getElementById('propAddress');
  if (nameEl) nameEl.textContent = d ? d.building_name : (re.address || 'Unknown entity');
  if (addrEl) addrEl.textContent = d ? d.address : (re.address || '—');
  const boroEl = document.getElementById('propBorough');
  const blockEl = document.getElementById('propBlock');
  const lotEl = document.getElementById('propLot');
  // Prefer hardcoded PROPERTY_DEFAULTS (FA-curated). Fall back to backend
  // response (re.borough/block/lot are derived from BBL — works for any
  // building auto-resolved via NYC GeoSearch even without a CONFIG entry).
  const boro  = (d && d.borough)  || (re && re.borough)  || '1';
  const block = (d && d.block)    || (re && re.block)    || '';
  const lot   = (d && d.lot)      || (re && re.lot)      || '';
  if (boroEl) boroEl.value = boro;
  if (blockEl) blockEl.value = block;
  if (lotEl) lotEl.value = lot;
  propLocked = !!(d && d.configured);
  applyPropLock();
  onPropFieldChange();
}

// FA dir 2026-06-03 (#6): Operating Assessment proposed = first-half RE tax
// x 2 x the editable % on the RE Tax page. Drives the operating-assessment
// (GL 4200) proposed budget on the Budget Summary 2027 column.
function _reUpdateOpAssess(re) {
  re = re || window._reTaxesData || {};
  const fh = Number(re.first_half_tax || 0);
  const baseEl = document.getElementById('reOpAssessBase');
  if (baseEl) baseEl.textContent = fmtDollar(fh * 2);
  const pctEl = document.getElementById('reOpAssessPct');
  const pct = (re.operating_assessment_pct != null) ? Number(re.operating_assessment_pct) : 0.175;
  // Don't clobber the field while the FA is actively typing in it.
  if (pctEl && document.activeElement !== pctEl) pctEl.value = (pct * 100).toFixed(2) + '%';
  _reRenderOpAssessProposed();
}
function _reRenderOpAssessProposed() {
  const re = window._reTaxesData || {};
  const fh = Number(re.first_half_tax || 0);
  const pctEl = document.getElementById('reOpAssessPct');
  const pct = pctEl ? parsePct(pctEl.value) : 0.175;
  const el = document.getElementById('reOpAssessProposed');
  if (el) el.textContent = fmtDollar(fh * 2 * pct);
}
function reOpAssessOnInput() {
  _reRenderOpAssessProposed();
  autosaveReTaxes();
}

// Build the 12-key payload the backend expects.
// Sends DERIVED current-year values (G column) for exemptions, not base (F).
function reTaxBuildPayload() {
  // Serialize any per-cell overrides (both numeric and formula-source).
  // Stored in assumptions_json.re_taxes_overrides.cell_overrides for round-trip.
  const cellOverrides = {};
  Object.keys(CELL_STATE).forEach(id => {
    const st = CELL_STATE[id];
    if (!st) return;
    if (st.override != null || st.overrideSrc) {
      cellOverrides[id] = {
        override: (st.override != null) ? st.override : null,
        overrideSrc: st.overrideSrc || null,
      };
    }
  });
  return {
    first_half_av:     cellRaw('g11'),
    tax_rate:          cellRaw('g12'),
    second_half_av:    cellRaw('g17'),   // computed current transitional AV
    est_tax_rate:      cellRaw('g18'),
    transitional_av_increase: cellRaw('d17'),
    veteran_growth:    cellRaw('gp26'),
    veteran_current:   cellRaw('g26'),
    sche_growth:       cellRaw('gp27'),
    sche_current:      cellRaw('g27'),
    star_growth:       cellRaw('gp28'),
    star_current:      cellRaw('g28'),
    abatement_growth:  cellRaw('gp29'),
    abatement_current: cellRaw('g29'),
    // Extra fields — backend currently ignores these, persisted for UI round-trip
    rate_adjustment:   cellRaw('g13'),
    j51_amount:        cellRaw('i15'),
    // FA dir 2026-05-19: After-10/31 toggle (zeros out May-Dec estimate)
    after_oct31:       !!(STATE && STATE.afterOct31),
    // FA dir 2026-06-03 (#6): operating-assessment % → Summary 4200 proposed
    operating_assessment_pct: (function () {
      const el = document.getElementById('reOpAssessPct');
      return el ? parsePct(el.value) : 0.175;
    })(),
    // Per-cell overrides (numeric values + user-typed formula sources)
    cell_overrides:    cellOverrides,
  };
}

// ─── AUTOSAVE ────────────────────────────────────────────────────
let _reSaveTimer = null;
let _reSuppressAutosave = false;

function autosaveReTaxes() {
  if (_reSuppressAutosave) return;
  if (_reSaveTimer) clearTimeout(_reSaveTimer);
  _reSaveTimer = setTimeout(saveRETaxes, 500);
}

async function saveRETaxes() {
  const entity = window._reActiveEntity;
  if (!entity) return;
  const status = document.getElementById('reTaxSaveStatus');
  if (status) { status.textContent = 'Saving…'; status.style.color = '#64748b'; }
  try {
    const payload = reTaxBuildPayload();
    const resp = await fetch('/api/re-taxes/' + entity, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (!resp.ok || data.error) throw new Error(data.error || ('HTTP ' + resp.status));
    window._reTaxesData = data.re_taxes || window._reTaxesData;
    // FA dir 2026-06-03 (#6): refresh the operating-assessment base/proposed
    // from the backend's recomputed values after each save.
    try { _reUpdateOpAssess(window._reTaxesData); } catch (e) {}
    if (status) {
      status.textContent = 'Saved ✓';
      status.style.color = '#16a34a';
      setTimeout(() => { if (status) status.textContent = ''; }, 2000);
    }
  } catch (err) {
    console.error('saveRETaxes failed:', err);
    if (status) {
      status.textContent = 'Save failed: ' + err.message;
      status.style.color = '#dc2626';
    }
  }
}

// Refetch DOF data from backend and repopulate cells (no full re-render)
async function refreshDOFData() {
  const entity = window._reActiveEntity;
  if (!entity) return;
  const status = document.getElementById('reTaxSaveStatus');
  if (status) { status.textContent = 'Refreshing DOF…'; status.style.color = '#64748b'; }
  try {
    const resp = await fetch('/api/re-taxes/' + entity);
    const data = await resp.json();
    if (!resp.ok || data.error) throw new Error(data.error || ('HTTP ' + resp.status));
    window._reTaxesData = data.re_taxes;
    reTaxLoadFromBackend(data.re_taxes);
    if (status) {
      status.textContent = 'DOF refreshed ✓';
      status.style.color = '#16a34a';
      setTimeout(() => { if (status) status.textContent = ''; }, 2000);
    }
  } catch (err) {
    console.error('refreshDOFData failed:', err);
    if (status) {
      status.textContent = 'Refresh failed: ' + err.message;
      status.style.color = '#dc2626';
    }
  }
}




async function renderBudgetSummary(contentDiv) {
  const COLS = ['c1','c2','c3','c4','c5','c6','c7'];
  const COL_NAMES = {c1:'Col 1 \u00b7 '+BY3+' Actual',c2:'Col 2 \u00b7 '+BY2+' Actual',c3:'Col 3 \u00b7 '+BY1+' YTD',
    c4:'Col 4 \u00b7 '+BY1+' Est.',c5:'Col 5 \u00b7 '+BY1+' Forecast',c6:'Col 6 \u00b7 '+BY1+' Budget',c7:'Col 7 \u00b7 '+BY+' Budget'};
  const SUM_TAB_COLORS = {
    "Income":{bg:"rgba(76,175,80,0.15)",color:"#2e7d32"},"Payroll":{bg:"rgba(33,150,243,0.15)",color:"#1565c0"},
    "Energy":{bg:"rgba(255,152,0,0.15)",color:"#e65100"},"Water & Sewer":{bg:"rgba(0,188,212,0.15)",color:"#00838f"},
    "Repairs & Supplies":{bg:"rgba(121,85,72,0.15)",color:"#5d4037"},"Gen & Admin":{bg:"rgba(156,39,176,0.15)",color:"#7b1fa2"},
    "RE Taxes":{bg:"rgba(244,67,54,0.15)",color:"#c62828"},"Manual":{bg:"rgba(255,213,79,0.15)",color:"#f57f17"},
  };
  const SUM_TAB_SHORT = {"Income":"Income","Payroll":"Payroll","Energy":"Energy","Water & Sewer":"Water",
    "Repairs & Supplies":"R&S","Gen & Admin":"Gen&Admin","RE Taxes":"RE Tax","Manual":"Manual"};

  function sfmt(v) {
    if (v===null||v===undefined||v==='') return '\u2014';
    const n=Number(v); if(isNaN(n)||n===0) return '\u2014';
    const s=Math.abs(Math.round(n)).toLocaleString('en-US');
    return n<0?'('+s+')':s;
  }
  function schip(tab) {
    if(!tab) return '<span style="color:var(--gray-400);font-size:11px">\u2014</span>';
    const c=SUM_TAB_COLORS[tab]||{bg:'rgba(158,158,158,0.15)',color:'#757575'};
    return '<span style="display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:600;letter-spacing:0.3px;white-space:nowrap;background:'+c.bg+';color:'+c.color+'">'+(SUM_TAB_SHORT[tab]||tab)+'</span>';
  }

  // Fetch summary data from API
  let sumData = null;
  try {
    const res = await fetch('/api/summary/' + entityCode);
    if (res.ok) sumData = await res.json();
  } catch(e) {}

  if (!sumData || !sumData.rows || sumData.rows.length === 0) {
    contentDiv.innerHTML = '<div style="padding:40px; text-align:center; color:var(--gray-500);"><p style="font-size:16px; margin-bottom:8px;">No Budget Summary data imported yet.</p><p style="font-size:13px;">Import an approved budget Excel to populate the Summary tab.</p></div>';
    return;
  }

  // Build label-keyed lineage map for the inspector drill-down
  window._sumLineage = {};
  window._sumRowMap = {};
  sumData.rows.forEach(r => {
    if (r.lineage && r.label) window._sumLineage[r.label] = r.lineage;
    if (r.label) window._sumRowMap[r.label] = r;
  });

  // ── Duplicate-row warnings banner ─────────────────────────────────
  // FA directive 2026-05-05: surface duplicate-row warnings (e.g. Gas +
  // Gas Heating both pulling from [5250,5251,5252]) so the FA can review
  // and decide whether to consolidate or differentiate. The banner string
  // is prepended to the rendered html below (NOT inserted via DOM mutation)
  // because contentDiv.innerHTML is overwritten later in this function.
  // Server returns warnings in sumData.warnings.
  const warnings = Array.isArray(sumData.warnings) ? sumData.warnings : [];
  let warningsBannerHtml = '';
  if (warnings.length > 0) {
    const sevColor = (s) => s === 'high' ? '#b91c1c' : '#92400e';
    const sevBg    = (s) => s === 'high' ? '#fef2f2' : '#fffbeb';
    const sevBorder= (s) => s === 'high' ? '#fecaca' : '#fed7aa';
    // Stash orphan list on window so the "Add Row" buttons can read details
    // without round-tripping markup. Indexed by gl_code.
    window._sumOrphans = {};
    warningsBannerHtml = '<div id="sumWarningsBanner" style="margin:0 0 12px 0;border-radius:8px;overflow:hidden;border:1px solid #e5e7eb;">';
    warnings.forEach((w) => {
      // ── Orphan-GL banner ────────────────────────────────────
      // FA directive 2026-05-05: list each unmapped GL with its description
      // as the suggested label and a one-click "Add Row" button.
      // 2026-05-17: wrapper now has id=sumOrphans so the Health drawer's
      // "Review" button can scroll-and-flash to this banner.
      if (w.type === 'orphan_gls') {
        const orphans = Array.isArray(w.orphans) ? w.orphans : [];
        warningsBannerHtml += '<div id="sumOrphans" style="background:' + sevBg(w.severity) + ';border-bottom:1px solid ' + sevBorder(w.severity) + ';padding:10px 14px;">';
        warningsBannerHtml += '<div style="display:flex;align-items:flex-start;gap:10px;margin-bottom:8px;">';
        warningsBannerHtml += '<div style="font-size:18px;line-height:1;color:' + sevColor(w.severity) + ';">⚠️</div>';
        warningsBannerHtml += '<div style="flex:1;">';
        warningsBannerHtml += '<div style="font-weight:600;color:' + sevColor(w.severity) + ';font-size:13px;">' + (w.title || 'Unmapped GLs') + '</div>';
        warningsBannerHtml += '<div style="margin-top:2px;color:#374151;font-size:12px;">' + (w.message || '') + '</div>';
        warningsBannerHtml += '</div></div>';
        warningsBannerHtml += '<div style="background:white;border:1px solid var(--gray-200);border-radius:6px;overflow:hidden;">';
        warningsBannerHtml += '<table style="width:100%;border-collapse:collapse;font-size:12px;">';
        warningsBannerHtml += '<thead><tr style="background:var(--gray-50);color:var(--gray-700);text-align:left;">';
        warningsBannerHtml += '<th style="padding:6px 8px;font-weight:600;font-size:11px;text-transform:uppercase;color:var(--gray-500);border-bottom:1px solid var(--gray-200);">GL</th>';
        warningsBannerHtml += '<th style="padding:6px 8px;font-weight:600;font-size:11px;text-transform:uppercase;color:var(--gray-500);border-bottom:1px solid var(--gray-200);">Suggested Label</th>';
        warningsBannerHtml += '<th style="padding:6px 8px;font-weight:600;font-size:11px;text-transform:uppercase;color:var(--gray-500);border-bottom:1px solid var(--gray-200);text-align:right;">YTD</th>';
        warningsBannerHtml += '<th style="padding:6px 8px;font-weight:600;font-size:11px;text-transform:uppercase;color:var(--gray-500);border-bottom:1px solid var(--gray-200);text-align:right;">Current Bud</th>';
        warningsBannerHtml += '<th style="padding:6px 8px;font-weight:600;font-size:11px;text-transform:uppercase;color:var(--gray-500);border-bottom:1px solid var(--gray-200);">Section</th>';
        warningsBannerHtml += '<th style="padding:6px 8px;font-weight:600;font-size:11px;text-transform:uppercase;color:var(--gray-500);border-bottom:1px solid var(--gray-200);text-align:right;">Action</th>';
        warningsBannerHtml += '</tr></thead><tbody>';
        orphans.forEach((o, oi) => {
          window._sumOrphans[o.gl_code] = o;
          const ytdStr = (o.ytd === null || o.ytd === undefined || o.ytd === 0) ? '—' : '$' + Math.round(o.ytd).toLocaleString('en-US');
          const cbStr = (o.current_budget === null || o.current_budget === undefined || o.current_budget === 0) ? '—' : '$' + Math.round(o.current_budget).toLocaleString('en-US');
          warningsBannerHtml += '<tr>';
          warningsBannerHtml += '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);font-family:monospace;font-weight:600;color:var(--gray-700);">' + (o.gl_code || '') + '</td>';
          warningsBannerHtml += '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);">' + (o.suggested_label || o.description || '') + '</td>';
          warningsBannerHtml += '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);text-align:right;font-variant-numeric:tabular-nums;color:var(--gray-700);">' + ytdStr + '</td>';
          warningsBannerHtml += '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);text-align:right;font-variant-numeric:tabular-nums;color:var(--gray-700);">' + cbStr + '</td>';
          warningsBannerHtml += '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);"><span style="font-size:10px;color:var(--gray-500);background:var(--gray-100);padding:1px 6px;border-radius:3px;">' + (o.suggested_section || '—') + '</span></td>';
          warningsBannerHtml += '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);text-align:right;"><button onclick="sumAddOrphanRow(\'' + (o.gl_code || '').replace(/'/g, "\\'") + '\')" style="padding:3px 10px;background:var(--blue);color:white;border:none;border-radius:4px;font-size:11px;font-weight:600;cursor:pointer;">+ Add Row</button></td>';
          warningsBannerHtml += '</tr>';
        });
        warningsBannerHtml += '</tbody></table></div></div>';
        return;
      }
      // 2026-05-17: duplicate-row + audit-empty warnings used to render as
      // inline yellow banners here too. They now live ONLY in the Health
      // drawer (right side) — clicking Review there scrolls back to the
      // affected rows. Skipping inline rendering avoids the two-places
      // duplication the FA complained about. Any future warning type that
      // doesn't have a dedicated inline UI also falls through to "Health-only".
      // If you need an inline banner for a new type, add an explicit case above.
      return;
    });
    // Cache duplicate-row warnings on window so readinessAction can read
    // them for scroll+flash targeting without re-fetching the API.
    // Backend emits either 'duplicate_prefixes' (same GL prefix list, high
    // severity) or 'duplicate_values' (identical YTD, medium severity);
    // both carry a labels[] array we can flash. 'duplicate_rows' is kept
    // as a forward-compat name in case the backend ever consolidates.
    window._sumDuplicateWarnings = (warnings || []).filter(function(w) {
      return w && (w.type === 'duplicate_prefixes' ||
                   w.type === 'duplicate_values'   ||
                   w.type === 'duplicate_rows');
    });
    warningsBannerHtml += '</div>';
  }

  // Build section-aware data structure
  const rows = sumData.rows;
  const sections = {};
  const hasSectionHeaders = rows.some(r => r.row_type === 'section_header');

  if (hasSectionHeaders) {
    // Buildings WITH section headers: assign sections from header labels
    let currentSec = '';
    rows.forEach(r => {
      if (r.row_type === 'section_header') currentSec = r.label;
      r._sec = currentSec;
      const sk = currentSec.toLowerCase().includes('non') && currentSec.toLowerCase().includes('income') ? 'noi' :
                 currentSec.toLowerCase().includes('non') && currentSec.toLowerCase().includes('expense') ? 'noe' :
                 currentSec.toLowerCase() === 'income' ? 'income' :
                 currentSec.toLowerCase() === 'expenses' ? 'expenses' : '';
      r._sk = sk;
      if (r.row_type === 'data' && sk) {
        if (!sections[sk]) sections[sk] = [];
        sections[sk].push(r);
      }
    });
  } else {
    // Buildings WITHOUT section headers: infer sections from subtotal positions
    // Standard layout: income rows -> Total Income -> expense rows -> Total Expenses
    //   -> Net Operating -> NOI data -> Total NOI -> NOE data -> Total NOE -> Grand Total
    let inferredSk = 'income';
    rows.forEach(r => {
      if (r.row_type === 'subtotal') {
        const lbl = r.label.toLowerCase();
        if (lbl.includes('total income'))                                        { r._sk = 'income';   inferredSk = 'expenses'; }
        else if (lbl.includes('total expense') && !lbl.includes('non'))          { r._sk = 'expenses'; inferredSk = 'noi'; }
        else if (lbl.includes('net operating'))                                  { r._sk = '';          inferredSk = 'noi'; }
        else if (lbl.includes('total non') && lbl.includes('income'))            { r._sk = 'noi';      inferredSk = 'noe'; }
        else if (lbl.includes('total non') && lbl.includes('expense'))           { r._sk = 'noe';      inferredSk = ''; }
        else if (lbl.includes('total surplus') || lbl.includes('total deficit')) { r._sk = ''; }
        else                                                                     { r._sk = ''; }
      } else {
        r._sk = inferredSk;
      }
      r._sec = r._sk;
      if (r.row_type === 'data' && r._sk) {
        if (!sections[r._sk]) sections[r._sk] = [];
        sections[r._sk].push(r);
      }
    });
  }

  // Table
  const thS = 'text-align:right;padding:10px 10px;white-space:nowrap;font-weight:600;border-bottom:2px solid var(--gray-300);background:var(--gray-100);';
  let html = '<div style="background:white;border-radius:12px;border:1px solid var(--gray-200);">' +
    '<div id="sumFBar" style="display:flex;align-items:center;gap:12px;padding:10px 20px;background:white;border:1px solid var(--gray-200);border-radius:8px;margin:8px 8px 0;min-height:44px;transition:all .2s;position:sticky;top:48px;z-index:30;box-shadow:0 2px 4px rgba(0,0,0,0.04);">' +
    '<span style="font-size:11px;font-weight:800;color:white;background:var(--blue);padding:2px 8px;border-radius:4px;font-family:monospace;letter-spacing:1px;">fx</span>' +
    '<span id="sumFBLabel" style="font-size:11px;font-weight:700;color:var(--blue);text-transform:uppercase;white-space:nowrap;min-width:60px;">Click a cell\u2026</span>' +
    '<input id="sumFBInput" type="text" disabled placeholder="Select an editable cell to enter a value or formula\u2026" style="font-family:monospace;font-size:13px;color:var(--gray-700);flex:1;padding:4px 8px;background:var(--gray-50);border:1px solid var(--gray-200);border-radius:4px;outline:none;">' +
    '<span id="sumFBPreview" style="font-size:13px;color:var(--gray-500);font-family:monospace;min-width:100px;text-align:right;"></span>' +
    '<button id="sumFBAccept" style="display:none;padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:none;background:var(--green);color:white;">Accept</button>' +
    '<button id="sumFBCancel" style="display:none;padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid var(--gray-200);background:white;color:var(--gray-600);">Cancel</button>' +
    '<button id="sumFBClear" style="display:none;padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid rgba(224,36,36,0.3);background:white;color:var(--red);">Clear</button>' +
    '<button id="sumFBInspect" style="display:none;padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid #16a34a;background:#f0fdf4;color:#15803d;" title="Show how this number was calculated">\ud83d\udd0d Inspect</button>' +
    // FA dir 2026-05-19: Summary tab per-tab Undo + History (same pattern as other tabs)
    '<span style="display:inline-block;width:1px;height:22px;background:var(--gray-200);margin:0 4px;"></span>' +
    '<button onclick="sumTabUndoLast()" title="Restore the most recent change on the Summary tab" style="padding:4px 10px;font-size:11px;background:white;color:var(--gray-700);border:1px solid var(--gray-300);border-radius:4px;cursor:pointer;font-weight:600;white-space:nowrap;">\u21a9 Undo last</button>' +
    '<button onclick="sumTabShowHistory()" title="See the last 50 changes on the Summary tab" style="padding:4px 10px;font-size:11px;background:white;color:var(--gray-700);border:1px solid var(--gray-300);border-radius:4px;cursor:pointer;font-weight:600;white-space:nowrap;">\u23f1 History</button>' +
    '<button onclick="sumExportExcel()" title="Download this summary as a live Excel workbook (formulas recalculate in Excel)" style="padding:4px 10px;font-size:11px;background:#f0fdf4;color:#15803d;border:1px solid #16a34a;border-radius:4px;cursor:pointer;font-weight:600;white-space:nowrap;">\u2b07 Excel</button>' +
    '</div>' +
    // FA dir 2026-05-24: second-row formula breakdown \u2014 long expressions like
    // "= Income (47,900) \u2212 Expenses (35,200) = 12,700" need full-width space
    // and would otherwise squeeze the input or wrap awkwardly. Populated by
    // sumCellFocus on cell click; cleared by sumResetBar on blur.
    '<div id="sumFBBreakdown" style="display:none;margin:0 8px 0;padding:6px 20px;background:#f8fafc;border:1px solid var(--gray-200);border-top:none;border-radius:0 0 8px 8px;font-family:monospace;font-size:12px;color:var(--gray-700);min-height:24px;line-height:24px;overflow-x:auto;white-space:nowrap;"></div>' +
    '<div id="sumDrillPanel" style="display:none;margin:0 8px 8px;background:white;border:1px solid #bbf7d0;border-left:4px solid #16a34a;border-radius:8px;padding:14px 18px;font-size:13px;position:sticky;top:100px;z-index:29;box-shadow:0 4px 12px rgba(0,0,0,0.08);max-height:60vh;overflow-y:auto;"></div>' +
    '<table id="sumTable" style="border-collapse:separate;border-spacing:0;font-size:13px;width:100%;">' +
    '<thead style="position:sticky;top:94px;z-index:20;"><tr>' +
    '<th style="text-align:left;padding:10px;min-width:200px;max-width:240px;position:sticky;left:0;z-index:25;background:var(--gray-100);border-right:2px solid var(--gray-300);border-bottom:2px solid var(--gray-300);box-shadow:2px 0 8px rgba(90,74,63,0.08);">Line Item</th>' +
    '<th style="'+thS+'min-width:80px;">Tab</th>' +
    '<th style="'+thS+'min-width:120px;"><span style="font-size:10px;color:var(--gray-500);display:block;">Col 1</span>'+BY3+' Actual*</th>' +
    '<th style="'+thS+'min-width:120px;color:var(--gray-400);font-style:italic;"><span style="font-size:10px;display:block;">Col 2</span>'+BY2+' Actual</th>' +
    '<th style="'+thS+'min-width:120px;color:var(--gray-400);font-style:italic;"><span style="font-size:10px;display:block;">Col 3</span>'+BY1+' YTD</th>' +
    '<th style="'+thS+'min-width:120px;color:var(--gray-400);font-style:italic;"><span style="font-size:10px;display:block;">Col 4</span>'+BY1+' Est.</th>' +
    '<th style="'+thS+'min-width:120px;color:var(--gray-400);font-style:italic;"><span style="font-size:10px;display:block;">Col 5</span>'+BY1+' Forecast</th>' +
    '<th style="'+thS+'min-width:120px;"><span style="font-size:10px;color:var(--gray-500);display:block;">Col 6</span>'+BY1+' Budget</th>' +
    '<th style="'+thS+'min-width:130px;background:#fffbeb;"><span style="font-size:10px;color:var(--gray-500);display:block;">Col 7 \u270e</span>'+BY+' Budget</th>' +
    '<th style="'+thS+'min-width:80px;" title="Col 8 compares Col 7 Proposed to the Col 5 12-month Forecast. The Excel export % Var column compares to the Col 6 Budget instead."><span style="font-size:10px;color:var(--gray-500);display:block;">Col 8</span>% vs Fcst</th>' +
    '<th style="text-align:left;padding:10px;min-width:170px;border-bottom:2px solid var(--gray-300);background:var(--gray-100);">Notes</th>' +
    '</tr></thead><tbody id="sumBody">';

  function makeInput(val, label, col, bg, overrideInfo, savedFormula) {
    const raw = (val!==null&&val!==undefined&&val!==0) ? Math.round(val) : '';
    const disp = raw!=='' ? raw.toLocaleString('en-US') : '';
    // Cols c2-c5 are computed from sources (audit / GL lines). Mark them as inspectable
    // with a green left-stripe + data-fx flag so sumCellFocus can show the "Inspect" button.
    const isFx = (col === 'c2' || col === 'c3' || col === 'c4' || col === 'c5');
    // FA directive 2026-05-17: ALL numeric columns are now editable.
    //   c1 / c6: imported from approved-budget Excel \u2014 override column added 2026-05-17.
    //   c2:      computed from confirmed audit's mapped_data \u2014 override column added 2026-05-17.
    //   c3/c4/c5: GL-aggregation computed (since 2026-05-05).
    //   c7:      directly editable (Proposed Budget).
    // Each overridable column has its dedicated *_override field on the
    // BudgetSummaryRow model; right-click reverts the override (clears
    // the column back to imported / computed source).
    const isOverridable = (col === 'c1' || col === 'c2' || col === 'c3' ||
                            col === 'c4' || col === 'c5' || col === 'c6');
    const isReadOnly = !(col === 'c7' || isOverridable);
    const isOverridden = !!(overrideInfo && overrideInfo.is_overridden);
    const computedVal = (overrideInfo && overrideInfo.computed != null) ? Math.round(overrideInfo.computed) : '';
    const roAttr = isReadOnly ? ' readonly' : '';
    // Visual stripe: green for fx (inspectable), yellow when overridden.
    let stripe = '';
    if (isOverridden) {
      stripe = 'box-shadow:inset 3px 0 0 #d97706;color:#92400e;font-weight:700;';
    } else if (isFx) {
      stripe = 'box-shadow:inset 3px 0 0 #16a34a;color:#15803d;font-weight:600;';
    }
    const fxAttr = isFx ? ' data-fx="1"' : '';
    const ovrAttr = isOverridable ? (' data-overridden="' + (isOverridden ? '1' : '0') + '" data-computed="' + computedVal + '"') : '';
    // Read-only cells: no border, default cursor, transparent bg (cell bg shows through)
    // Editable c7 / c3 / c4 / c5: keeps gray border + text cursor so it reads as an input
    let editableBg = bg || '#fffbeb';
    if (isOverridden) editableBg = '#fef3c7'; // amber tint when override active
    const inputStyle = isReadOnly
      ? 'width:100px;padding:5px 8px;border:1px solid transparent;border-radius:4px;font-size:13px;text-align:right;background:transparent;font-variant-numeric:tabular-nums;font-family:inherit;cursor:default;color:var(--gray-700);'+stripe
      : 'width:100px;padding:5px 8px;border:1px solid var(--gray-300);border-radius:4px;font-size:13px;text-align:right;background:'+editableBg+';font-variant-numeric:tabular-nums;font-family:inherit;cursor:text;'+stripe;
    // FA directive 2026-05-17: Col 2 (audit actual) cells with lineage data
    // get a visible "\u24d8" badge in the corner so the FA can drill into the
    // auditor's source lines without hunting through the formula bar.
    // Click the badge \u2192 opens the Inspector drill panel directly. Tooltip
    // previews the matched audit category + line count. Only renders on c2
    // (audit data); c3/c4/c5 use OVR badge instead.
    const labelLineage = (window._sumLineage || {})[label];
    const c2Lineage = (col === 'c2' && labelLineage && labelLineage.c2) ? labelLineage.c2 : null;
    const c2SourceCount = c2Lineage && Array.isArray(c2Lineage.source_lines) ? c2Lineage.source_lines.length : 0;
    const c2HasData = c2Lineage && (c2SourceCount > 0 || (c2Lineage.matched_category && c2Lineage.value !== null));
    let c2BadgeTitle = '';
    if (c2HasData) {
      const mc = c2Lineage.matched_category || '(unmatched)';
      const mt = c2Lineage.match_type || '';
      c2BadgeTitle = 'Audit lineage: matched to "' + mc + '"' + (mt ? ' (' + mt + ')' : '') +
        '. ' + c2SourceCount + ' source line' + (c2SourceCount === 1 ? '' : 's') +
        ' from auditor. Click to inspect.';
    }
    // 2026-05-17: if c2 has an active OVR (the FA edited it manually), shift
    // the Inspector badge LEFT so it doesn't overlap the OVR badge. Clicking
    // the Inspector with OVR active still shows the original audit lineage —
    // useful for FA to compare their override to what the audit said.
    const c2HasOvr = (col === 'c2' && overrideInfo && overrideInfo.is_overridden);
    const c2InspectRight = c2HasOvr ? '24px' : '4px';
    const c2InspectBadge = c2HasData
      ? '<button type="button" class="sum-c2-inspect-badge" ' +
        // Use data-label + delegated handler instead of inline-encoded
        // params to avoid double-quoting hell with labels that contain
        // apostrophes (e.g., "Real Estate Tax Benefit Credits (Abatement, Star,etc)").
        'data-inspect-label="' + label.replace(/"/g, '&quot;') + '" ' +
        'onclick="sumC2BadgeClick(event, this)" ' +
        'title="' + c2BadgeTitle.replace(/"/g, '&quot;') + '" ' +
        'style="position:absolute;top:2px;right:' + c2InspectRight + ';width:16px;height:16px;padding:0;' +
        'background:#fef3c7;color:#92400e;border:1px solid #fcd34d;border-radius:50%;' +
        'cursor:pointer;font-size:10px;font-weight:700;line-height:14px;' +
        'font-family:Georgia,serif;font-style:italic;display:inline-flex;' +
        'align-items:center;justify-content:center;z-index:5;">i</button>'
      : '';

    const cellTitle = isOverridden
      ? 'Override active. Computed value was ' + (computedVal !== '' ? computedVal.toLocaleString('en-US') : '\u2014') + '. Right-click to revert.'
      : (isOverridable ? 'Click to override the computed value' : '');
    const titleAttr = cellTitle ? (' title="' + cellTitle.replace(/"/g, '&quot;') + '"') : '';
    const ovrBadge = isOverridden ? '<span class="sum-ovr-badge" style="position:absolute;top:2px;right:4px;font-size:8px;font-weight:700;color:#92400e;background:#fde68a;padding:1px 3px;border-radius:3px;letter-spacing:0.3px;pointer-events:none;">OVR</span>' : '';
    const ctxAttr = isOverridable ? ' oncontextmenu="return sumCellRevert(event, this)"' : '';
    // FA dir 2026-05-17: stamp data-formula when a saved formula string exists
    // for this cell. sumCellFocus uses it to populate the formula bar on click
    // (so the FA can edit the original "300*12*4" to "300*12*3" instead of
    // retyping). Escape quotes so the attribute renders cleanly.
    const formulaAttr = (savedFormula && typeof savedFormula === 'string' && savedFormula.length)
      ? ' data-formula="' + savedFormula.replace(/"/g, '&quot;') + '"'
      : '';
    return '<td class="number" style="background:'+(bg||'#fbfaf4')+';padding:4px 6px;font-variant-numeric:tabular-nums;text-align:right;position:relative;">' +
      '<input type="text" value="'+disp+'" placeholder="\u2014" data-label="'+label.replace(/"/g,'&quot;')+'" data-col="'+col+'" data-raw="'+raw+'"'+fxAttr+ovrAttr+roAttr+titleAttr+ctxAttr+formulaAttr+' ' +
      'onfocus="sumCellFocus(this)" onblur="sumCellBlur(this)" onkeydown="sumCellKey(event,this)" ' +
      'style="'+inputStyle+'">' + ovrBadge + c2InspectBadge + '</td>';
  }
  // FA dir 2026-05-24: subtotal rows become editable like data rows. Each
  // c1–c7 subtotal cell renders as an <input> that joins the existing
  // sumCellFocus / sumCellBlur / sumAcceptFormula / sumCellRevert pipeline.
  // The input's value is filled by sumRecalcTotals when there's no override;
  // when overridden, the saved override value shows + amber OVR badge appears.
  // Reuses col*_override fields on BudgetSummaryRow (no migration needed).
  function makeSubtotalInput(label, col, overrideInfo, isGrand, savedFormula) {
    const isOverridden = !!(overrideInfo && overrideInfo.is_overridden);
    // Backend serializes the saved value as .override (col1_override … col6_override).
    // For synthetic c7 (col7_proposed_budget acting as override), the caller
    // builds an object using the same .override key. value is read here.
    const ovrVal = isOverridden && overrideInfo.override != null ? Math.round(overrideInfo.override) : '';
    const raw = ovrVal !== '' ? ovrVal : '';
    const disp = raw !== '' ? raw.toLocaleString('en-US') : '';
    const computedVal = (overrideInfo && overrideInfo.computed != null) ? Math.round(overrideInfo.computed) : '';
    const ovrAttr = ' data-overridden="' + (isOverridden ? '1' : '0') + '" data-computed="' + computedVal + '"';
    const formulaAttr = (savedFormula && typeof savedFormula === 'string' && savedFormula.length)
      ? ' data-formula="' + savedFormula.replace(/"/g, '&quot;') + '"'
      : '';
    // Subtotal styling: green-fill for sectional + Net Operating, dark blue
    // for Grand Total. Amber tint when overridden (mirrors data-row OVR look).
    let cellBg, textColor, stripe;
    if (isGrand) {
      cellBg = '#1e3a5f';
      textColor = isOverridden ? '#fbbf24' : '#86efac';
      stripe = isOverridden ? 'box-shadow:inset 3px 0 0 #fbbf24;' : '';
    } else {
      cellBg = isOverridden ? '#fef3c7' : '#f0fdf4';
      textColor = isOverridden ? '#92400e' : '#16a34a';
      stripe = isOverridden ? 'box-shadow:inset 3px 0 0 #d97706;' : '';
    }
    const inputStyle = 'width:100px;padding:5px 8px;border:1px solid transparent;border-radius:4px;font-size:13px;font-weight:700;text-align:right;color:' + textColor + ';background:' + cellBg + ';font-variant-numeric:tabular-nums;font-family:inherit;cursor:text;' + stripe;
    const cellTitle = isOverridden
      ? 'Override active. Computed total was ' + (computedVal !== '' ? computedVal.toLocaleString('en-US') : '—') + '. Right-click to revert.'
      : 'Click to override the computed subtotal';
    const ovrBadge = isOverridden ? '<span class="sum-ovr-badge" style="position:absolute;top:2px;right:4px;font-size:8px;font-weight:700;color:#92400e;background:#fde68a;padding:1px 3px;border-radius:3px;letter-spacing:0.3px;pointer-events:none;">OVR</span>' : '';
    return '<td class="number" data-sum-col="' + col + '" style="text-align:right;padding:4px 6px;font-variant-numeric:tabular-nums;position:relative;background:' + cellBg + ';">' +
      '<input type="text" value="' + disp + '" placeholder="—" data-label="' + label.replace(/"/g, '&quot;') + '" data-col="' + col + '" data-raw="' + raw + '" data-subtotal="1"' + ovrAttr + formulaAttr +
      ' title="' + cellTitle.replace(/"/g, '&quot;') + '"' +
      ' oncontextmenu="return sumCellRevert(event, this)"' +
      ' onfocus="sumCellFocus(this)" onblur="sumCellBlur(this)" onkeydown="sumCellKey(event,this)"' +
      ' style="' + inputStyle + '">' + ovrBadge + '</td>';
  }
  const _fxBadge = '<span class="sum-fx" style="display:inline-block;background:#4ade80;color:#fff;font-size:8px;font-weight:700;padding:1px 3px;border-radius:3px;margin-left:4px;vertical-align:middle;">fx</span>';
  function sumTd(col) {
    return '<td class="number" data-sum-col="'+col+'" style="text-align:right;padding:8px 10px;font-weight:700;font-variant-numeric:tabular-nums;cursor:pointer;" onclick="sumSubtotalClick(this)"><span class="sub-val">\u2014</span>'+_fxBadge+'</td>';
  }
  function noteIn(label) {
    return '<td style="padding:4px 6px;"><input type="text" placeholder="Add note\u2026" data-note-label="'+label.replace(/"/g,'&quot;')+'" ' +
      'style="width:100%;padding:5px 8px;border:1px solid var(--gray-200);border-radius:4px;font-size:12px;background:white;font-family:inherit;color:var(--gray-700);"></td>';
  }

  rows.forEach((r, idx) => {
    if (r.row_type === 'section_header') {
      html += '<tr data-sec="'+r._sk+'" style="background:var(--blue-light);">' +
        '<td colspan="11" style="font-weight:700;color:var(--blue);font-size:14px;padding:10px;border-bottom:2px solid var(--blue);position:sticky;left:0;background:var(--blue-light);">' +
        r.label + ' <button onclick="sumShowInsert(\''+r._sk+'\',\''+r.label.replace(/'/g,"\\'")+'\')" style="margin-left:12px;display:inline-flex;align-items:center;gap:4px;padding:3px 10px;border:1px dashed var(--gray-300);border-radius:6px;font-size:11px;font-weight:600;color:var(--gray-500);background:transparent;cursor:pointer;vertical-align:middle;">+ Add Row</button>' +
        '</td></tr>';
    } else if (r.row_type === 'data') {
      const fn = r.footnote_marker ? '<span style="color:var(--gray-500);font-size:11px;font-weight:600;vertical-align:super;margin-left:2px;">'+r.footnote_marker+'</span>' : '';
      // Per-row × Delete button (FA directive 2026-05-14 Phase 4.3, hover
      // CSS-ified in Phase 4.5). Click → confirm dialog → /api/admin/delete-
      // summary-row. The endpoint refuses to delete subtotal/section_header
      // rows and imported rows (col6 set) — those failures bubble up as
      // toasts. Hover-reveal is handled by CSS rules in the style block.
      const delBtn = '<button onclick="sumDeleteRow(this,\''+r.label.replace(/'/g,"\\'").replace(/"/g,'&quot;')+'\')" class="row-del-btn" title="Delete this row" style="margin-left:6px;background:transparent;border:none;color:var(--gray-400);cursor:pointer;font-size:14px;line-height:1;padding:0 4px;vertical-align:middle;">&times;</button>';
      // FA directive 2026-05-17: data-label on <tr> so deep-links from the
      // Health drawer (e.g. duplicate-row Review) can find the right row in
      // O(1) by attribute, instead of fragile textContent matching that
      // breaks on footnote markers and the × delete button.
      html += '<tr data-sec="'+r._sk+'" data-type="d" data-order="'+r.display_order+'" data-label="'+r.label.replace(/"/g,'&quot;')+'">' +
        '<td style="padding:8px 10px;border-bottom:1px solid var(--gray-200);position:sticky;left:0;z-index:15;background:white;min-width:200px;max-width:240px;border-right:2px solid var(--gray-300);box-shadow:2px 0 8px rgba(90,74,63,0.08);">'+r.label+fn+delBtn+'</td>' +
        '<td style="text-align:right;padding:8px 10px;border-bottom:1px solid var(--gray-200);">'+schip(r.source_tab)+'</td>' +
        // FA dir 2026-05-17: pass override info for c1/c2/c6 too (newly editable).
        // Also pass any saved formula string via r.formulas so makeInput can
        // stamp data-formula on the input — sumCellFocus reads that to show
        // the formula in the formula bar on re-click.
        makeInput(r.col1, r.label, 'c1', '#fbfaf4', (r.overrides && r.overrides.col1), (r.formulas && r.formulas.col1)) +
        makeInput(r.col2, r.label, 'c2', '#f9f9f7', (r.overrides && r.overrides.col2), (r.formulas && r.formulas.col2)) +
        makeInput(r.col3, r.label, 'c3', '#f9f9f7', (r.overrides && r.overrides.col3), (r.formulas && r.formulas.col3)) +
        makeInput(r.col4, r.label, 'c4', '#f9f9f7', (r.overrides && r.overrides.col4), (r.formulas && r.formulas.col4)) +
        makeInput(r.col5, r.label, 'c5', '#f9f9f7', (r.overrides && r.overrides.col5), (r.formulas && r.formulas.col5)) +
        makeInput(r.col6, r.label, 'c6', '#fbfaf4', (r.overrides && r.overrides.col6), (r.formulas && r.formulas.col6)) +
        makeInput(r.col7, r.label, 'c7', '#fffbeb', null,                                (r.formulas && r.formulas.col7)) +
        // FA dir 2026-05-17: per-row % Variance (Col 8) \u2014 formula = (col7-col5)/|col5|.
        // Used to be hardcoded as "\u2014"; now renders the computed value AND
        // gets recomputed on every cell blur via _sumRowRecalcVariance().
        // Marked with data-row-col so sumRecalcTotals can find it.
        (function(){
          const c5n = (typeof r.col5 === 'number') ? r.col5 : null;
          const c7n = (typeof r.col7 === 'number') ? r.col7 : null;
          let html = '\u2014';
          let color = 'var(--gray-400)';
          if (c5n !== null && c5n !== 0 && c7n !== null) {
            const pct = ((c7n - c5n) / Math.abs(c5n)) * 100;
            color = pct > 0 ? 'var(--green)' : (pct < 0 ? 'var(--red)' : 'var(--gray-400)');
            html = (pct > 0 ? '+' : '') + pct.toFixed(1) + '%';
          }
          return '<td data-row-col="c8" data-label="'+r.label.replace(/"/g,'&quot;')+'" style="text-align:right;padding:8px 10px;border-bottom:1px solid var(--gray-200);color:'+color+';font-variant-numeric:tabular-nums;font-weight:500;">'+html+'</td>';
        })() +
        noteIn(r.label) + '</tr>';
    } else if (r.row_type === 'subtotal') {
      const isNet = r.label.toLowerCase().includes('net operating');
      const isGrand = r.label.toLowerCase().includes('total surplus') || r.label.toLowerCase().includes('total deficit');
      const calcAttr = isGrand ? 'data-calc="grand"' : isNet ? 'data-calc="income-expenses"' : 'data-sums="'+r._sk+'"';
      const bgStyle = isGrand ? 'background:#1e3a5f;color:white;' : isNet ? 'background:#f0f4f8;border-top:2px solid var(--gray-400);border-bottom:2px solid var(--gray-400);' : 'background:var(--gray-100);border-top:2px solid var(--gray-300);';
      const tdFrozen = isGrand ? 'background:#1e3a5f;color:white;' : isNet ? 'background:#f0f4f8;' : 'background:var(--gray-100);';

      // Append manual "+ Add Row" button to sectional subtotals so every
      // building (with or without section_header rows) gets row-creation
      // affordance. Skip Net Operating + Total Surplus — those are computed,
      // not editable. FA directive 2026-05-14 Phase 4.3.
      const addRowBtn = (!isNet && !isGrand && r._sk)
        ? ' <button onclick="event.stopPropagation();sumShowInsert(\''+r._sk+'\',\''+r.label.replace(/'/g,"\\'")+'\')" style="margin-left:10px;padding:3px 10px;border:1px dashed var(--gray-400);border-radius:6px;font-size:11px;font-weight:600;color:var(--gray-700);background:white;cursor:pointer;vertical-align:middle;">+ Add Row</button>'
        : '';
      // FA dir 2026-05-24: subtotal rows are now editable like data rows.
      // Pass display_order via data-order so sumCellBlur / sumAcceptFormula can
      // find the row, and data-type="s" lets recalc filter subtotal rows
      // (vs "d" = data rows). Each cell renders as an <input> so it joins the
      // same focus/blur/accept/revert pipeline as data cells. Overrides hit
      // the existing col*_override fields on BudgetSummaryRow (no migration).
      html += '<tr '+calcAttr+' data-sec="'+r._sk+'" data-type="s" data-order="'+r.display_order+'" data-label="'+r.label.replace(/"/g,'&quot;')+'" style="'+bgStyle+'">' +
        '<td style="padding:8px 10px;font-weight:700;position:sticky;left:0;z-index:15;'+tdFrozen+'min-width:200px;max-width:240px;border-right:2px solid var(--gray-300);box-shadow:2px 0 8px rgba(90,74,63,0.08);">'+r.label+addRowBtn+'</td>' +
        '<td style="'+(isGrand?'background:#1e3a5f;':'')+'"></td>';
      COLS.forEach(c => {
        // overrides dict from /api/summary is keyed "col1"…"col6" (no col7).
        // c7 on a subtotal stores into col7_proposed_budget — synthesize an
        // override-shaped object so makeSubtotalInput can render the OVR badge
        // when col7_proposed_budget is non-null.
        const colKey = 'col' + c.substring(1);
        let ovr = (r.overrides && r.overrides[colKey]) || null;
        if (c === 'c7') {
          // FA dir 2026-06-03 (#3): subtotal c7 (proposed total) is a LIVE
          // computed roll-up of the data-row proposed budgets — never an
          // override. A subtotal-level c7 was never persisted server-side
          // anyway (GET recomputes it as the sum of the data rows), so the old
          // "non-null => overridden" synth just painted a misleading OVR badge
          // and, worse, froze the cell so it stopped tracking data-row edits.
          // Mark it computed: sumRecalcTotals keeps it in sync and no badge.
          const c7v = (typeof r.col7 === 'number') ? r.col7 : null;
          ovr = {is_overridden: false, override: null, computed: c7v};
        }
        const fmla = (r.formulas && r.formulas[colKey]) || null;
        html += makeSubtotalInput(r.label, c, ovr, isGrand, fmla);
      });
      // Option F: green fill for computed cells (subtotal + net), light-green text on dark blue for grand total. No fx badge.
      const computedCellStyle = isGrand
        ? 'background:#1e3a5f;color:#86efac;'
        : 'background:#f0fdf4;color:#16a34a;';
      // c8 (% Variance) stays view-only \u2014 user directive 2026-05-24 (Skip Col 8).
      html += '<td data-sum-col="c8" style="text-align:right;padding:8px 10px;font-weight:700;font-variant-numeric:tabular-nums;cursor:pointer;'+computedCellStyle+'" onclick="sumSubtotalClick(this)"><span class="sub-val">\u2014</span></td>';
      html += '<td style="'+(isGrand?'background:#1e3a5f;':'')+'padding:4px 6px;">'+(isGrand?'':'<input type="text" placeholder="Add note\u2026" style="width:100%;padding:5px 8px;border:1px solid var(--gray-200);border-radius:4px;font-size:12px;background:white;font-family:inherit;">')+'</td>';
      html += '</tr>';
    }
  });

  html += '</tbody></table></div>';
  // Prepend duplicate-row warnings banner (built above) so it survives the
  // innerHTML overwrite and renders above the workbook table.
  contentDiv.innerHTML = warningsBannerHtml + html;

  // Recalculate totals
  sumRecalcTotals();
}

// ── Summary tab: recalculate all subtotals ──
function sumRecalcTotals() {
  const COLS = ['c1','c2','c3','c4','c5','c6','c7'];
  const tbody = document.getElementById('sumBody');
  if (!tbody) return;

  const secs = {income:[], expenses:[], noi:[], noe:[]};
  tbody.querySelectorAll('tr[data-type="d"]').forEach(tr => {
    const sec = tr.dataset.sec;
    const skMap = {'Income':'income','Expenses':'expenses','Non-Operating Income':'noi','Non-Operating Expense':'noe'};
    const sk = skMap[sec] || tr.closest('[data-sec]')?.dataset.sec || '';
    if (!secs[sk]) return;
    const vals = {};
    COLS.forEach(c => {
      const inp = tr.querySelector('input[data-col="'+c+'"]');
      vals[c] = inp ? (parseFloat(inp.dataset.raw) || 0) : 0;
    });
    secs[sk].push(vals);

    // FA dir 2026-05-17: refresh per-row Col 8 (% Var) as part of recalc.
    // Formula matches the backend: (col7 - col5) / |col5| × 100.
    // Empty c7 OR zero c5 → render "—" so the FA doesn't see misleading values.
    const c8td = tr.querySelector('td[data-row-col="c8"]');
    if (c8td) {
      // Use the live input raw to detect empty (rather than `vals[c]` which
      // coerces empty to 0 and would compute -100% / 100% spuriously).
      const c7inp = tr.querySelector('input[data-col="c7"]');
      const c5inp = tr.querySelector('input[data-col="c5"]');
      const c7raw = c7inp && c7inp.dataset.raw;
      const c5raw = c5inp && c5inp.dataset.raw;
      const c7null = (c7raw === '' || c7raw === undefined || c7raw === null);
      const c5null = (c5raw === '' || c5raw === undefined || c5raw === null);
      const c5v = parseFloat(c5raw);
      const c7v = parseFloat(c7raw);
      if (c7null || c5null || !isFinite(c5v) || c5v === 0 || !isFinite(c7v)) {
        c8td.textContent = '—';
        c8td.style.color = 'var(--gray-400)';
      } else {
        const pct = ((c7v - c5v) / Math.abs(c5v)) * 100;
        c8td.textContent = (pct > 0 ? '+' : '') + pct.toFixed(1) + '%';
        c8td.style.color = pct > 0 ? 'var(--green)' : (pct < 0 ? 'var(--red)' : 'var(--gray-400)');
      }
    }
  });

  function sumSec(key) {
    const t = {}; COLS.forEach(c => t[c] = 0);
    (secs[key]||[]).forEach(v => { COLS.forEach(c => t[c] += v[c]); });
    return t;
  }
  const inc = sumSec('income'), exp = sumSec('expenses'), noi = sumSec('noi'), noe = sumSec('noe');

  // FA dir 2026-05-24: subtotal c1\u2013c7 cells are now <input>s (editable).
  // writeSum writes computed totals to input.value/dataset.raw UNLESS the
  // input has data-overridden="1" \u2014 overrides win. c8 stays a <span> view-only.
  function writeSum(sel, totals) {
    const tr = tbody.querySelector(sel);
    if (!tr) return;
    COLS.forEach(c => {
      const td = tr.querySelector('[data-sum-col="'+c+'"]');
      if (!td) return;
      const inp = td.querySelector('input[data-col="'+c+'"]');
      const v = totals[c];
      const isEmpty = (!v && v !== 0);
      const isZero = Math.round(v||0) === 0;
      if (inp) {
        // Override active \u2192 don't overwrite the user's value.
        if (inp.dataset.overridden === '1') return;
        if (isEmpty || isZero) {
          inp.dataset.raw = '';
          inp.value = '';
        } else {
          inp.dataset.raw = v;
          inp.value = Math.round(v).toLocaleString('en-US');
        }
        // Refresh data-computed so right-click revert tooltip shows the latest.
        inp.dataset.computed = isEmpty ? '' : Math.round(v);
      } else {
        // Fallback for any subtotal that wasn't converted (defensive).
        const sv = td.querySelector('.sub-val');
        const txt = (isEmpty || isZero) ? '\u2014' : (v < 0 ? '(' + Math.abs(Math.round(v)).toLocaleString('en-US') + ')' : Math.round(v).toLocaleString('en-US'));
        if (sv) sv.textContent = txt; else td.textContent = txt;
      }
    });
    const c8 = tr.querySelector('[data-sum-col="c8"]');
    if (c8) {
      const sv8 = c8.querySelector('.sub-val');
      if (totals.c7 && totals.c5 && totals.c5 !== 0) {
        const pct = ((totals.c7 - totals.c5) / Math.abs(totals.c5)) * 100;
        const pctHtml = '<span style="color:'+(pct>0?'var(--green)':pct<0?'var(--red)':'var(--gray-400)')+'">'+(pct>0?'+':'')+pct.toFixed(1)+'%</span>';
        if (sv8) sv8.innerHTML = pctHtml; else c8.innerHTML = pctHtml;
      } else {
        if (sv8) sv8.textContent = '\u2014'; else c8.textContent = '\u2014';
      }
    }
  }

  // FA dir 2026-05-24: for cascading totals (Net Operating, Grand), read the
  // EFFECTIVE value of each section subtotal \u2014 override wins over computed.
  // Without this, FA can override Income c5 = $500K but Net Operating still
  // shows the sum-of-data-rows value, creating a visible inconsistency.
  function effectiveSubtotals(sel, computed) {
    const tr = tbody.querySelector(sel);
    if (!tr) return computed;
    const out = {}; COLS.forEach(c => out[c] = computed[c]);
    COLS.forEach(c => {
      const inp = tr.querySelector('input[data-col="'+c+'"]');
      if (inp && inp.dataset.overridden === '1') {
        const v = parseFloat(inp.dataset.raw);
        if (!isNaN(v)) out[c] = v;
      }
    });
    return out;
  }

  writeSum('tr[data-sums="income"]', inc);
  writeSum('tr[data-sums="expenses"]', exp);
  writeSum('tr[data-sums="noi"]', noi);
  writeSum('tr[data-sums="noe"]', noe);

  const incEff = effectiveSubtotals('tr[data-sums="income"]', inc);
  const expEff = effectiveSubtotals('tr[data-sums="expenses"]', exp);
  const noiEff = effectiveSubtotals('tr[data-sums="noi"]', noi);
  const noeEff = effectiveSubtotals('tr[data-sums="noe"]', noe);

  // Net Operating = Income - Expenses (uses effective sectional subtotals)
  const net = {}; COLS.forEach(c => net[c] = incEff[c] - expEff[c]);
  writeSum('tr[data-calc="income-expenses"]', net);

  // Grand = Net + NOI - NOE (uses effective Net + effective NOI/NOE).
  // Net Op may itself be overridden \u2014 read it through effectiveSubtotals too.
  const netEff = effectiveSubtotals('tr[data-calc="income-expenses"]', net);
  const grand = {}; COLS.forEach(c => grand[c] = netEff[c] + noiEff[c] - noeEff[c]);
  writeSum('tr[data-calc="grand"]', grand);
}

// ── Summary tab: subtotal fx click ──
let _activeSumSubtotal = null;
function sumSubtotalClick(td) {
  // Clear previous highlight
  if (_activeSumSubtotal && _activeSumSubtotal !== td) {
    _activeSumSubtotal.style.outline = '';
  }
  _activeSumSubtotal = td;
  td.style.outline = '2px solid var(--blue)';
  td.style.outlineOffset = '-2px';

  const col = td.dataset.sumCol;
  const tr = td.closest('tr');
  const COL_NAMES = {c1:BY3+' Actual',c2:BY2+' Actual',c3:BY1+' YTD',c4:BY1+' Est.',c5:BY1+' Forecast',c6:BY1+' Budget',c7:BY+' Budget',c8:'% Var'};
  const rowLabel = tr ? (tr.querySelector('td')?.textContent || 'Total') : 'Total';
  const colLabel = COL_NAMES[col] || col;

  // Build formula from component data rows
  const tbody = document.getElementById('sumBody');
  let formula = '';
  if (col === 'c8') {
    // % Var = (c7 - c5) / |c5|
    formula = '= (Col 7 - Col 5) / |Col 5|';
  } else if (tr.dataset.sums) {
    // Section subtotal: sum all data rows in this section
    const secKey = tr.dataset.sums;
    const vals = [];
    tbody.querySelectorAll('tr[data-type="d"][data-sec="'+secKey+'"]').forEach(dr => {
      const inp = dr.querySelector('input[data-col="'+col+'"]');
      if (inp) { const v = parseFloat(inp.dataset.raw) || 0; if (v !== 0) vals.push(Math.round(v)); }
    });
    formula = vals.length <= 10 ? '= ' + (vals.length ? vals.join(' + ') : '0') : '= SUM of ' + vals.length + ' lines = ' + vals.reduce((a,b)=>a+b,0).toLocaleString();
  } else if (tr.dataset.calc === 'income-expenses') {
    // Net Operating = Income - Expenses
    const incTr = tbody.querySelector('tr[data-sums="income"]');
    const expTr = tbody.querySelector('tr[data-sums="expenses"]');
    const incVal = incTr ? (incTr.querySelector('[data-sum-col="'+col+'"] .sub-val')?.textContent || '0') : '0';
    const expVal = expTr ? (expTr.querySelector('[data-sum-col="'+col+'"] .sub-val')?.textContent || '0') : '0';
    formula = '= Income (' + incVal + ') - Expenses (' + expVal + ')';
  } else if (tr.dataset.calc === 'grand') {
    formula = '= Net Operating + Non-Op Income - Non-Op Expenses';
  }

  // Show in formula bar
  const bar = document.getElementById('sumFBar');
  if (bar) bar.style.borderColor = 'var(--blue)';
  const lbl = document.getElementById('sumFBLabel');
  if (lbl) lbl.textContent = rowLabel.trim() + ' \u2192 ' + colLabel;
  const inp = document.getElementById('sumFBInput');
  if (inp) { inp.disabled = true; inp.value = formula; inp.style.opacity = '0.85'; inp.placeholder = ''; }
  ['sumFBAccept','sumFBCancel','sumFBClear'].forEach(id => { const b = document.getElementById(id); if(b) b.style.display='none'; });
  const prev = document.getElementById('sumFBPreview');
  if (prev) prev.textContent = '';
}
// Clear subtotal highlight on click-away
document.addEventListener('click', function(e) {
  if (!_activeSumSubtotal) return;
  if (_activeSumSubtotal.contains(e.target)) return;
  const bar = document.getElementById('sumFBar');
  if (bar && bar.contains(e.target)) return;
  _activeSumSubtotal.style.outline = '';
  _activeSumSubtotal = null;
  sumResetBar();
});

// ── Summary tab: cell editing ──
let _sumActiveCell = null;

// Build an Excel-style numerical formula string for a read-only summary cell
// ── Excel-valid formula for the formula BAR (Step 1 toward dynamic .xlsx export) ──
// Emits a REAL single-'=' Excel formula: raw integers (no thousands-commas, no
// parens), ASCII operators (+ - * /), no trailing '= result', no word labels. So a
// cell exported to Excel is a working formula. The human-readable explanation
// (= 39,730 + 79,460 = 119,190) stays in the breakdown row + Inspector below.
// sumExcelExpr folds a term's sign into the operator, so we emit '=a-b', never '=a+-b'.
function sumExcelExpr(nums) {
  const nz = nums.map(n => Math.round(Number(n) || 0)).filter(n => n !== 0);
  if (!nz.length) return '';
  let s = (nz[0] < 0 ? '-' + Math.abs(nz[0]) : '' + nz[0]);
  for (let i = 1; i < nz.length; i++) s += (nz[i] < 0 ? '-' + Math.abs(nz[i]) : '+' + nz[i]);
  return '=' + s;
}
function sumBuildExcelFormula(label, col, lineage, raw) {
  const v = (n) => String(Math.round(Number(n) || 0));
  // c1 / c6 are direct Excel imports — plain values, not formulas (no leading '=').
  if (col === 'c1' || col === 'c6') return raw ? v(raw) : '';
  if (!lineage) return raw ? v(raw) : '';
  if (col === 'c2') {
    const c2 = lineage.c2 || {};
    if (!c2.has_audit || !c2.matched_category) return '';
    return '=' + v(c2.value);
  }
  const gl = lineage.gl || {};
  const ff = lineage.fixed_forecast || {};
  const rowData = (window._sumRowMap || {})[label] || {};
  if (ff.applied && col === 'c5') return '=' + v(rowData.col6);
  if (ff.applied && col === 'c4') return sumExcelExpr([rowData.col5, -(Number(rowData.col3) || 0)]);
  const allLines = gl.lines || [];
  const ytdM = gl.ytd_months || 0;
  const remM = gl.remaining_months || 0;
  if (col === 'c3') {
    const lines = allLines.filter(l => Math.round(Number(l.ytd) || 0) !== 0);
    return sumExcelExpr(lines.map(l => l.ytd));
  }
  if (col === 'c4') {
    const tY = allLines.reduce((s, l) => s + (Number(l.ytd) || 0), 0);
    if (!ytdM || !remM) return tY ? ('=' + v(tY)) : '';
    return '=' + v(tY) + '/' + ytdM + '*' + remM;
  }
  if (col === 'c5') {
    const tY = allLines.reduce((s, l) => s + (Number(l.ytd) || 0), 0);
    const tA = allLines.reduce((s, l) => s + (Number(l.accrual) || 0), 0);
    const tU = allLines.reduce((s, l) => s + (Number(l.unpaid) || 0), 0);
    const tE = allLines.reduce((s, l) => s + (Number(l.estimate) || 0), 0);
    return sumExcelExpr([tY, tA, tU, tE]);
  }
  return '';
}
function sumBuildExcelSubtotal(el) {
  const col = el.dataset.col;
  const tr = el.closest('tr');
  if (!tr) return '';
  const tbody = document.getElementById('sumBody');
  if (!tbody) return '';
  const v = (n) => String(Math.round(Number(n) || 0));
  if (col === 'c8') {
    const c7i = tr.querySelector('input[data-col="c7"]');
    const c5i = tr.querySelector('input[data-col="c5"]');
    const c7v = c7i ? (parseFloat(c7i.dataset.raw) || 0) : 0;
    const c5v = c5i ? (parseFloat(c5i.dataset.raw) || 0) : 0;
    if (!Math.round(c5v)) return '';
    const num = sumExcelExpr([c7v, -c5v]);   // '=c7-c5'
    return '=(' + num.slice(1) + ')/' + v(Math.abs(c5v));
  }
  if (tr.dataset.sums) {
    const secKey = tr.dataset.sums;
    const vals = [];
    tbody.querySelectorAll('tr[data-type="d"][data-sec="' + secKey + '"]').forEach(dr => {
      const inp = dr.querySelector('input[data-col="' + col + '"]');
      if (inp) { const x = parseFloat(inp.dataset.raw); if (!isNaN(x) && Math.round(x) !== 0) vals.push(Math.round(x)); }
    });
    return sumExcelExpr(vals);
  }
  if (tr.dataset.calc === 'income-expenses') {
    const incInp = tbody.querySelector('tr[data-sums="income"] input[data-col="' + col + '"]');
    const expInp = tbody.querySelector('tr[data-sums="expenses"] input[data-col="' + col + '"]');
    const incV = incInp ? parseFloat(incInp.dataset.raw) || 0 : 0;
    const expV = expInp ? parseFloat(expInp.dataset.raw) || 0 : 0;
    return sumExcelExpr([incV, -expV]);
  }
  if (tr.dataset.calc === 'grand') {
    const netInp = tbody.querySelector('tr[data-calc="income-expenses"] input[data-col="' + col + '"]');
    const noiInp = tbody.querySelector('tr[data-sums="noi"] input[data-col="' + col + '"]');
    const noeInp = tbody.querySelector('tr[data-sums="noe"] input[data-col="' + col + '"]');
    const netV = netInp ? parseFloat(netInp.dataset.raw) || 0 : 0;
    const noiV = noiInp ? parseFloat(noiInp.dataset.raw) || 0 : 0;
    const noeV = noeInp ? parseFloat(noeInp.dataset.raw) || 0 : 0;
    return sumExcelExpr([netV, noiV, -noeV]);
  }
  return '';
}

function sumBuildFormulaText(label, col, lineage, raw) {
  const fmt = (n) => {
    if (n === null || n === undefined || isNaN(n)) return '0';
    const r = Math.round(Number(n));
    return r < 0 ? '(' + Math.abs(r).toLocaleString('en-US') + ')' : r.toLocaleString('en-US');
  };
  // c1 and c6 are direct Excel imports — no formula, just the number
  if (col === 'c1' || col === 'c6') {
    return raw ? Math.round(Number(raw)).toLocaleString('en-US') : '';
  }
  if (!lineage) return raw ? Math.round(Number(raw)).toLocaleString('en-US') : '';
  if (col === 'c2') {
    const c2 = lineage.c2 || {};
    if (!c2.has_audit || !c2.matched_category) return '';
    return '= ' + fmt(c2.value);
  }
  // Cols 3-5: GL aggregation
  const gl = lineage.gl || {};
  const ff = lineage.fixed_forecast || {};
  const rowData = (window._sumRowMap || {})[label] || {};
  // Fixed-forecast override (Maintenance / Common Charges / Commercial Rent)
  if (ff.applied && col === 'c5') {
    return '= ' + fmt(rowData.col6);
  }
  if (ff.applied && col === 'c4') {
    return '= ' + fmt(rowData.col5) + ' - ' + fmt(rowData.col3) + ' = ' + fmt(rowData.col4);
  }
  const allLines = gl.lines || [];
  const ytdM = gl.ytd_months || 0;
  const remM = gl.remaining_months || 0;
  if (col === 'c3') {
    // Sum of per-line YTD values (non-zero only)
    const lines = allLines.filter(l => Math.round(Number(l.ytd)||0) !== 0);
    if (!lines.length) return '';
    if (lines.length === 1) return '= ' + fmt(lines[0].ytd);
    const total = lines.reduce((s,l) => s + (Number(l.ytd)||0), 0);
    return '= ' + lines.map(l => fmt(l.ytd)).join(' + ') + ' = ' + fmt(total);
  }
  if (col === 'c4') {
    // Estimate = (Σ YTD ÷ ytd_months) × remaining_months
    const totalYtd = allLines.reduce((s,l) => s + (Number(l.ytd)||0), 0);
    if (!ytdM || !remM) return '= ' + fmt(totalYtd);
    const totalEst = (totalYtd / ytdM) * remM;
    return '= ' + fmt(totalYtd) + ' / ' + ytdM + ' * ' + remM + ' = ' + fmt(totalEst);
  }
  if (col === 'c5') {
    // Forecast = YTD + Accrual + Unpaid + Estimate (totals)
    const tY = allLines.reduce((s,l) => s + (Number(l.ytd)||0), 0);
    const tA = allLines.reduce((s,l) => s + (Number(l.accrual)||0), 0);
    const tU = allLines.reduce((s,l) => s + (Number(l.unpaid)||0), 0);
    const tE = allLines.reduce((s,l) => s + (Number(l.estimate)||0), 0);
    const tF = tY + tA + tU + tE;
    // Drop any zero terms to keep it readable
    const parts = [];
    if (Math.round(tY) !== 0) parts.push(fmt(tY));
    if (Math.round(tA) !== 0) parts.push(fmt(tA));
    if (Math.round(tU) !== 0) parts.push(fmt(tU));
    if (Math.round(tE) !== 0) parts.push(fmt(tE));
    if (!parts.length) return '';
    if (parts.length === 1) return '= ' + parts[0];
    return '= ' + parts.join(' + ') + ' = ' + fmt(tF);
  }
  return '';
}

function sumExportExcel() {
  // Download the dynamic .xlsx (live formulas) for the building currently open.
  window.location.href = '/api/summary/' + encodeURIComponent(entityCode) + '/export.xlsx';
}
function sumCellFocus(el) {
  // Clear any subtotal highlight
  if (_activeSumSubtotal) { _activeSumSubtotal.style.outline = ''; _activeSumSubtotal = null; }
  _sumActiveCell = el;
  // Snapshot the value at focus so we can detect whether the FA actually
  // changed anything before saving on blur. FA directive 2026-05-10:
  // clicking into a cell without editing must NOT create an override.
  el.dataset.focusedRaw = (el.dataset.raw === undefined) ? '' : String(el.dataset.raw);
  const bar = document.getElementById('sumFBar');
  if (bar) bar.style.borderColor = 'var(--blue)';
  const COL_NAMES = {c1:'Col 1 \u00b7 '+BY3+' Actual',c2:'Col 2 \u00b7 '+BY2+' Actual',c3:'Col 3 \u00b7 '+BY1+' YTD',
    c4:'Col 4 \u00b7 '+BY1+' Est.',c5:'Col 5 \u00b7 '+BY1+' Forecast',c6:'Col 6 \u00b7 '+BY1+' Budget',c7:'Col 7 \u00b7 '+BY+' Budget'};
  const cl = COL_NAMES[el.dataset.col] || el.dataset.col;
  const lbl = document.getElementById('sumFBLabel');
  // FA dir 2026-05-17: every numeric column (c1-c7) is now editable. The
  // formula bar reads the cell's actual readOnly attribute (set by makeInput
  // based on the same isOverridable check) so it stays in sync. Previously
  // hardcoded "only c7 is editable" which made the formula bar disabled for
  // c1-c6 even after I made the cells themselves editable. Bug-fix.
  const isReadOnly = !!el.readOnly;
  if (lbl) lbl.textContent = el.dataset.label + ' \u2192 ' + cl;
  const inp = document.getElementById('sumFBInput');
  if (inp) {
    // 2026-06-07: bring the Summary formula bar to the SAME Excel standard as
    // every other tab — the bar shows the EQUATION (all the numbers adding
    // together) for computed cells, not a bare single figure. Section
    // subtotals + Net Operating / grand totals use the subtotal breakdown;
    // GL-aggregated data cells (c2-c5) use the line-level breakdown.
    const lineage = (window._sumLineage || {})[el.dataset.label];
    const isSub = el.dataset.subtotal === '1';
    const isOvr = el.dataset.overridden === '1';
    const isFxData = el.dataset.fx === '1';
    let eq = '';
    if (!isOvr) {
      // The BAR shows a valid Excel formula (=A+B); the breakdown row + Inspector
      // below keep the readable explanation. Same numbers, two renderings.
      if (isSub) eq = sumBuildExcelSubtotal(el);
      else if (isFxData) eq = sumBuildExcelFormula(el.dataset.label, el.dataset.col, lineage, el.dataset.raw);
    }
    if (isReadOnly) {
      inp.value = eq || (el.dataset.raw || '');
      inp.disabled = true;
      inp.style.opacity = '0.85';
      inp.placeholder = '';
      delete el.dataset._fxeq;
    } else {
      // FA dir 2026-05-17: if the FA previously saved a formula here, show the
      // formula text so they can edit it. Otherwise show the computed equation
      // (stored as a baseline in _fxeq so Accept-without-edit is a no-op and
      // never turns the computed value into a spurious override), else raw.
      const savedFormula = el.dataset.formula;
      if (savedFormula) {
        inp.value = savedFormula; delete el.dataset._fxeq;
      } else if (eq) {
        inp.value = eq; el.dataset._fxeq = eq;
      } else {
        inp.value = el.dataset.raw || el.value || ''; delete el.dataset._fxeq;
      }
      inp.disabled = false;
      inp.style.opacity = '1';
      inp.placeholder = 'Enter value or formula (e.g. =9384324*1.035)';
    }
  }
  // Show Accept/Cancel/Clear for every editable column.
  ['sumFBAccept','sumFBCancel','sumFBClear'].forEach(id => {
    const b = document.getElementById(id);
    if (b) b.style.display = isReadOnly ? 'none' : '';
  });
  // Show Inspect button only for computed cols (c2-c5) when lineage exists for this row
  const isFx = el.dataset.fx === '1';
  const lineage = (window._sumLineage || {})[el.dataset.label];
  const inspBtn = document.getElementById('sumFBInspect');
  if (inspBtn) inspBtn.style.display = (isFx && lineage) ? '' : 'none';
  // FA dir 2026-05-24: surface the cell's underlying formula in the breakdown
  // row below the formula bar. The input itself stays editable (with the raw
  // value or saved formula), but the FA can now SEE what the cell is summing
  // when they click into it — same insight the old sumSubtotalClick provided
  // for subtotals, plus GL-aggregation breakdown for data-row c3/c4/c5.
  const bdEl = document.getElementById('sumFBBreakdown');
  if (bdEl) {
    let breakdown = '';
    const isSubtotal = el.dataset.subtotal === '1';
    const isOverridden = (el.dataset.overridden === '1');
    if (isSubtotal && !isOverridden) {
      breakdown = sumBuildSubtotalBreakdown(el);
    } else if (isFx && !isOverridden) {
      // Data-row c2-c5: GL aggregation / audit lineage breakdown.
      breakdown = sumBuildFormulaText(el.dataset.label, el.dataset.col, lineage, el.dataset.raw);
    } else if (isOverridden) {
      // Override active — show the computed value the override is hiding.
      const computed = el.dataset.computed;
      if (computed && computed !== '') {
        breakdown = 'Override active. Computed value: ' + Number(computed).toLocaleString('en-US') + ' (right-click cell to revert)';
      }
    }
    if (breakdown) {
      bdEl.textContent = breakdown;
      bdEl.style.display = 'block';
    } else {
      bdEl.textContent = '';
      bdEl.style.display = 'none';
    }
  }
  // Don't strip formatting on read-only cells (no editing happens there)
  if (!isReadOnly) el.value = el.dataset.raw || '';
}

// FA dir 2026-05-24: compute the "this subtotal is X + Y + Z" breakdown text
// for a subtotal cell — used by sumCellFocus to populate the formula preview
// when the FA clicks a subtotal so they SEE what's rolling up. Mirrors the
// formula text the old sumSubtotalClick built (pre-refactor).
function sumBuildSubtotalBreakdown(el) {
  const col = el.dataset.col;
  const tr = el.closest('tr');
  if (!tr) return '';
  const tbody = document.getElementById('sumBody');
  if (!tbody) return '';
  const fmt = (n) => {
    if (n === null || n === undefined || isNaN(n)) return '0';
    const r = Math.round(Number(n));
    return r < 0 ? '(' + Math.abs(r).toLocaleString('en-US') + ')' : r.toLocaleString('en-US');
  };
  if (col === 'c8') {
    const c7i = tr.querySelector('input[data-col="c7"]');
    const c5i = tr.querySelector('input[data-col="c5"]');
    const c7v = c7i ? (parseFloat(c7i.dataset.raw) || 0) : 0;
    const c5v = c5i ? (parseFloat(c5i.dataset.raw) || 0) : 0;
    const pct = c5v ? ((c7v - c5v) / Math.abs(c5v)) * 100 : 0;
    return '= (' + fmt(c7v) + ' − ' + fmt(c5v) + ') / ' + fmt(Math.abs(c5v)) + ' × 100 = ' + pct.toFixed(1) + '%';
  }
  if (tr.dataset.sums) {
    // Section subtotal: list all non-zero data-row values that roll up.
    const secKey = tr.dataset.sums;
    const vals = [];
    tbody.querySelectorAll('tr[data-type="d"][data-sec="' + secKey + '"]').forEach(dr => {
      const inp = dr.querySelector('input[data-col="' + col + '"]');
      if (inp) {
        const v = parseFloat(inp.dataset.raw);
        if (!isNaN(v) && Math.round(v) !== 0) vals.push(Math.round(v));
      }
    });
    if (!vals.length) return '= 0';
    const total = vals.reduce((a, b) => a + b, 0);
    // 2026-06-07: show ALL the numbers adding together (no "sum of N lines"
    // collapse) — the FA wants every component visible in the formula bar.
    return '= ' + vals.map(fmt).join(' + ') + ' = ' + fmt(total);
  }
  if (tr.dataset.calc === 'income-expenses') {
    // Net Operating = Income − Expenses (read live from the subtotal inputs).
    const incInp = tbody.querySelector('tr[data-sums="income"] input[data-col="' + col + '"]');
    const expInp = tbody.querySelector('tr[data-sums="expenses"] input[data-col="' + col + '"]');
    const incV = incInp ? parseFloat(incInp.dataset.raw) || 0 : 0;
    const expV = expInp ? parseFloat(expInp.dataset.raw) || 0 : 0;
    return '= Income (' + fmt(incV) + ') − Expenses (' + fmt(expV) + ') = ' + fmt(incV - expV);
  }
  if (tr.dataset.calc === 'grand') {
    // Total Surplus = Net Op + Non-Op Income − Non-Op Expenses.
    const netInp = tbody.querySelector('tr[data-calc="income-expenses"] input[data-col="' + col + '"]');
    const noiInp = tbody.querySelector('tr[data-sums="noi"] input[data-col="' + col + '"]');
    const noeInp = tbody.querySelector('tr[data-sums="noe"] input[data-col="' + col + '"]');
    const netV = netInp ? parseFloat(netInp.dataset.raw) || 0 : 0;
    const noiV = noiInp ? parseFloat(noiInp.dataset.raw) || 0 : 0;
    const noeV = noeInp ? parseFloat(noeInp.dataset.raw) || 0 : 0;
    return '= Net Op (' + fmt(netV) + ') + Non-Op Income (' + fmt(noiV) + ') − Non-Op Expenses (' + fmt(noeV) + ') = ' + fmt(netV + noiV - noeV);
  }
  return '';
}

// FA directive 2026-05-17: delegated click handler for the c2 Inspector
// badge. Reads the label from a data attribute (avoids inline-quoting
// issues with labels that contain apostrophes / quotes), opens the
// drill panel, and scrolls it into view so the FA sees the result.
function sumC2BadgeClick(evt, btn) {
  if (evt && evt.stopPropagation) evt.stopPropagation();
  if (evt && evt.preventDefault) evt.preventDefault();
  const label = btn && btn.getAttribute('data-inspect-label');
  if (!label) return;
  if (typeof sumRenderDrillPanel === 'function') sumRenderDrillPanel(label, 'c2');
  const panel = document.getElementById('sumDrillPanel');
  if (panel) {
    try { panel.scrollIntoView({behavior:'smooth', block:'center'}); } catch(e) {}
  }
}

// ── Summary inspector: render lineage drill-down for a c2-c5 cell ──
function sumRenderDrillPanel(label, col) {
  console.log('[inspector] render', {label: label, col: col, hasPanel: !!document.getElementById('sumDrillPanel'), hasLineageMap: !!window._sumLineage, lineageKeys: Object.keys(window._sumLineage||{}).length});
  const panel = document.getElementById('sumDrillPanel');
  if (!panel) { console.warn('[inspector] panel element not found'); return; }
  const lineage = (window._sumLineage || {})[label];
  if (!lineage) {
    // Show visible fallback so user sees something rather than silent failure
    panel.innerHTML = '<div style="color:#92400e;background:#fffbeb;padding:10px 12px;border-radius:6px;">No lineage data for "<b>' + label + '</b>". Available keys: ' + Object.keys(window._sumLineage||{}).slice(0,5).join(', ') + '\u2026</div>';
    panel.style.display = 'block';
    return;
  }
  const fmt = (n) => {
    if (n === null || n === undefined || isNaN(n)) return '\u2014';
    const r = Math.round(Number(n));
    return r < 0 ? '(' + Math.abs(r).toLocaleString('en-US') + ')' : r.toLocaleString('en-US');
  };
  const COL_TITLES = {c2:'Col 2 \u00b7 '+(typeof BY2!=='undefined'?BY2:'')+' Actual',
    c3:'Col 3 \u00b7 '+(typeof BY1!=='undefined'?BY1:'')+' YTD',
    c4:'Col 4 \u00b7 '+(typeof BY1!=='undefined'?BY1:'')+' Est.',
    c5:'Col 5 \u00b7 '+(typeof BY1!=='undefined'?BY1:'')+' Forecast'};
  let html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">' +
    '<div style="font-weight:700;color:#15803d;font-size:14px;">\ud83d\udd0d Inspector \u00b7 ' + label + ' \u2192 ' + (COL_TITLES[col]||col) + '</div>' +
    '<button onclick="document.getElementById(\'sumDrillPanel\').style.display=\'none\'" style="background:transparent;border:none;cursor:pointer;color:var(--gray-500);font-size:18px;line-height:1;">\u00d7</button>' +
    '</div>';

  if (col === 'c2') {
    const c2 = lineage.c2 || {};
    if (!c2.has_audit) {
      html += '<div style="background:#fffbeb;padding:10px 12px;border-radius:6px;color:#92400e;">No confirmed audited financials for FY ' + (c2.audit_year || '?') + '. Upload + confirm an audit on the Audited Financials tab to populate Col 2.</div>';
    } else if (c2.matched_category) {
      html += '<div style="display:grid;grid-template-columns:auto 1fr;gap:6px 14px;font-size:12px;margin-bottom:10px;">' +
        '<div style="color:var(--gray-500);">Source:</div><div><b>Confirmed Audited Financials</b> \u00b7 FY ' + (c2.audit_year || '?') + '</div>' +
        '<div style="color:var(--gray-500);">Matched category:</div><div><code style="background:var(--gray-100);padding:1px 6px;border-radius:3px;">' + c2.matched_category + '</code> <span style="color:var(--gray-500);font-size:11px;">(' + (c2.match_type||'') + ')</span></div>' +
        '<div style="color:var(--gray-500);">Confirmed by:</div><div>' + (c2.audit_confirmed_by || '\u2014') + ' on ' + (c2.audit_confirmed_at ? c2.audit_confirmed_at.slice(0,10) : '\u2014') + '</div>' +
        '<div style="color:var(--gray-500);">Source file:</div><div>' + (c2.audit_filename || '\u2014') + '</div>' +
        '</div>' +
        '<div style="background:#f0fdf4;border:1px solid #bbf7d0;padding:10px 12px;border-radius:6px;font-family:monospace;font-size:13px;">' +
        '<b>= ' + fmt(c2.value) + '</b> <span style="color:var(--gray-500);">(rolled up from audit lines below)</span></div>';
      // Per-auditor-line breakdown \u2014 Phase 2 (editable). FA can edit amounts,
      // move lines to a different summary row, delete, or add manually. Each
      // mutation goes through /api/af/uploads/<id>/source-line and re-fetches
      // the summary to refresh the totals.
      const sourceLines = Array.isArray(c2.source_lines) ? c2.source_lines : [];
      const auditId = c2.audit_id;
      // List of all valid summary labels for the move dropdown \u2014 populated
      // from the Inspector's parent rowMap, which has every data row label.
      const moveTargets = Object.keys(window._sumRowMap || {}).filter(
        k => (window._sumRowMap[k] || {}).row_type === 'data' && k !== label
      ).sort();
      const moveOptionsHtml = moveTargets.map(t =>
        '<option value="' + t.replace(/"/g, '&quot;') + '">' + t + '</option>'
      ).join('');
      let lineSubtotal = 0;
      let rowsHtml = '';
      sourceLines.forEach((sl, sliIdx) => {
        const desc = (sl.auditor_desc || '').replace(/[<>]/g, '');
        const amt = Number(sl.amount) || 0;
        lineSubtotal += amt;
        const auditCat = (sl.audit_category || '').replace(/[<>]/g, '');
        const catBadge = auditCat && auditCat !== c2.matched_category && auditCat !== label
          ? '<span style="display:inline-block;font-size:10px;color:var(--gray-500);margin-left:6px;background:var(--gray-100);padding:1px 6px;border-radius:8px;">via ' + auditCat + '</span>'
          : '';
        const userBadge = sl.user_added
          ? '<span style="display:inline-block;font-size:10px;color:#7c3aed;margin-left:6px;background:#f3e8ff;padding:1px 6px;border-radius:8px;">added</span>'
          : '';
        const lineId = (sl.id || '').replace(/"/g, '&quot;');
        const dataAttrs = 'data-audit-id="' + auditId + '" data-line-id="' + lineId + '" data-summary-label="' + label.replace(/"/g, '&quot;') + '"';
        rowsHtml += '<tr id="audrow-' + sliIdx + '">'
          + '<td style="padding:4px 8px;font-size:12px;border-bottom:1px solid var(--gray-100);">' + desc + catBadge + userBadge + '</td>'
          + '<td style="padding:4px 8px;font-size:12px;border-bottom:1px solid var(--gray-100);text-align:right;font-family:monospace;">$' + fmt(amt) + '</td>'
          + '<td style="padding:4px 8px;font-size:12px;border-bottom:1px solid var(--gray-100);text-align:right;white-space:nowrap;">'
          + '<button title="Edit amount" ' + dataAttrs + ' data-line-amount="' + amt + '" data-line-desc="' + desc.replace(/"/g,'&quot;') + '" onclick="auditLineEdit(this)" style="border:1px solid var(--gray-300);background:white;color:var(--gray-700);padding:2px 6px;border-radius:4px;cursor:pointer;font-size:11px;margin-right:4px;">\u270f\ufe0f</button>'
          + '<button title="Move to another category" ' + dataAttrs + ' onclick="auditLineMove(this)" style="border:1px solid var(--gray-300);background:white;color:var(--gray-700);padding:2px 6px;border-radius:4px;cursor:pointer;font-size:11px;margin-right:4px;">\u2197</button>'
          + '<button title="Delete this line" ' + dataAttrs + ' onclick="auditLineDelete(this)" style="border:1px solid #fecaca;background:white;color:#b91c1c;padding:2px 6px;border-radius:4px;cursor:pointer;font-size:11px;">\ud83d\uddd1\ufe0f</button>'
          + '</td>'
          + '</tr>';
      });
      const subtotalDelta = Math.round(lineSubtotal - (Number(c2.value) || 0));
      const reconcileNote = Math.abs(subtotalDelta) < 1
        ? '<span style="color:var(--green);">\u2713 reconciles</span>'
        : '<span style="color:var(--orange);">\u26a0 differs by $' + fmt(Math.abs(subtotalDelta)) + '</span>';
      html += '<div style="margin-top:14px;">'
        + '<div style="display:flex;align-items:center;justify-content:space-between;font-size:11px;font-weight:600;color:var(--gray-700);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;">'
        + '<span>Audit Breakdown \u00b7 ' + sourceLines.length + ' line' + (sourceLines.length === 1 ? '' : 's') + '</span>'
        + '<span style="text-transform:none;letter-spacing:0;font-weight:500;">' + reconcileNote + '</span>'
        + '</div>'
        + '<div style="border:1px solid var(--gray-200);border-radius:6px;overflow:hidden;">'
        + '<table style="width:100%;border-collapse:collapse;">'
        + '<thead style="background:var(--gray-50);">'
        + '<tr><th style="padding:6px 8px;font-size:11px;font-weight:600;color:var(--gray-500);text-align:left;text-transform:uppercase;">Auditor description</th>'
        + '<th style="padding:6px 8px;font-size:11px;font-weight:600;color:var(--gray-500);text-align:right;text-transform:uppercase;">Amount</th>'
        + '<th style="padding:6px 8px;font-size:11px;font-weight:600;color:var(--gray-500);text-align:right;text-transform:uppercase;">Actions</th></tr>'
        + '</thead>'
        + '<tbody>' + rowsHtml + '</tbody>'
        + '<tfoot><tr style="background:var(--gray-50);font-weight:600;">'
        + '<td style="padding:6px 8px;font-size:12px;">Subtotal</td>'
        + '<td style="padding:6px 8px;font-size:12px;text-align:right;font-family:monospace;">$' + fmt(lineSubtotal) + '</td>'
        + '<td style="padding:6px 8px;text-align:right;">'
        + (auditId ? '<button onclick="auditLineAdd(\'' + (auditId) + '\', \'' + label.replace(/"/g,'&quot;').replace(/'/g,"\\'") + '\')" style="border:1px solid var(--blue);background:#eff6ff;color:var(--blue);padding:2px 8px;border-radius:4px;cursor:pointer;font-size:11px;font-weight:600;">+ Add line</button>' : '')
        + '</td>'
        + '</tr></tfoot>'
        + '</table></div>'
        + '<div style="margin-top:8px;font-size:11px;color:var(--gray-500);font-style:italic;">Edit amounts in place, move lines to a different summary row, or add manual entries. Saves to this building only.</div>'
        + '</div>'
        + '<datalist id="auditMoveTargets">' + moveOptionsHtml + '</datalist>';
      // Stash move-target list and audit_id on window so handlers can reach
      // them without rebuilding markup.
      window._auditMoveTargets = moveTargets;
    } else {
      html += '<div style="background:#fffbeb;padding:10px 12px;border-radius:6px;color:#92400e;">Audit is confirmed for FY ' + (c2.audit_year || '?') + ', but no category matched the row label "<b>' + label + '</b>". Add an alias in <code>_LABEL_ALIASES</code> (workflow.py).</div>';
    }
  } else {
    // Cols 3-5: GL aggregation breakdown
    const gl = lineage.gl || {};
    const allLines = gl.lines || [];
    const ytdM = gl.ytd_months || 0;
    const remM = gl.remaining_months || 0;
    // Filter out lines where the inspected column is zero
    const hi = (col === 'c3') ? 'ytd' : (col === 'c4') ? 'estimate' : 'forecast';
    const lines = allLines.filter(l => Math.round(Number(l[hi]) || 0) !== 0);
    const hiddenCount = allLines.length - lines.length;
    // Fixed-forecast override banner (Maintenance / Common Charges / Commercial Rent)
    const ff = lineage.fixed_forecast || {};
    if (ff.applied && (col === 'c4' || col === 'c5')) {
      const rowData = (window._sumRowMap || {})[label] || {};
      const c3v = Number(rowData.col3 || 0);
      const c5v = Number(rowData.col5 || 0);
      const c6v = Number(rowData.col6 || 0);
      const c4v = Number(rowData.col4 || 0);
      const formulaLine = (col === 'c5')
        ? 'Col 5 = Col 6 (Approved Budget) = <b>' + fmt(c6v) + '</b>'
        : 'Col 4 = Col 5 \u2212 Col 3 = ' + fmt(c5v) + ' \u2212 ' + fmt(c3v) + ' = <b>' + fmt(c4v) + '</b>';
      html += '<div style="background:#eff6ff;border:1px solid #bfdbfe;padding:10px 12px;border-radius:6px;margin-bottom:10px;font-size:12px;color:#1e40af;">' +
        '<div style="font-weight:700;margin-bottom:4px;">\ud83d\udccc Forecast pinned to Approved Budget</div>' +
        '<div>This row matches the Maintenance / Common Charges / Commercial Rent rule (GL 4010 / 4020 / 4030 / 4040). Forecast is locked to the approved budget rather than aggregated from YTD.</div>' +
        '<div style="margin-top:6px;font-family:monospace;">' + formulaLine + '</div>' +
        '<div style="margin-top:4px;color:#475569;">GL breakdown below is shown for reference only.</div>' +
        '</div>';
    }
    if (!lines.length) {
      html += '<div style="background:#f4f1eb;padding:10px 12px;border-radius:6px;color:var(--gray-700);">No GL prefixes mapped for this row, or no budget_lines data found. Map GL prefixes in the Budget Setup configuration to populate Cols 3-5.</div>';
    } else {
      const totalYtd = lines.reduce((s,l) => s + l.ytd, 0);
      const totalAcc = lines.reduce((s,l) => s + l.accrual, 0);
      const totalUnp = lines.reduce((s,l) => s + l.unpaid, 0);
      const totalEst = lines.reduce((s,l) => s + l.estimate, 0);
      const totalFc  = lines.reduce((s,l) => s + l.forecast, 0);
      // COL 4 (Estimate) as displayed on the summary tab = accrual + unpaid + remaining-months projection.
      // (Per FA directive 2026-05-05: COL 3 is raw YTD; accrual/unpaid + projection live in COL 4.)
      const totalCol4 = totalAcc + totalUnp + totalEst;
      // Helper: render a signed term as " + 5,073" or " \u2212 5,073" so accruals
      // (which are stored as negatives in the DB) read like the FA's mental model:
      //   "ytd minus accrual, plus unpaid"  (with accrual < 0, becomes a subtraction)
      const fmtTerm = (n) => {
        const v = Number(n) || 0;
        if (v < 0) return ' \u2212 ' + Math.abs(Math.round(v)).toLocaleString('en-US');
        return ' + ' + Math.round(v).toLocaleString('en-US');
      };
      html += '<div style="display:grid;grid-template-columns:auto 1fr;gap:6px 14px;font-size:12px;margin-bottom:10px;">' +
        '<div style="color:var(--gray-500);">Source:</div><div><b>Budget Lines</b> (GL aggregation)</div>' +
        '<div style="color:var(--gray-500);">GL prefixes:</div><div><code style="background:var(--gray-100);padding:1px 6px;border-radius:3px;">' + (gl.prefixes || []).join(', ') + '</code></div>' +
        '<div style="color:var(--gray-500);">YTD period:</div><div>' + ytdM + ' months actual + ' + remM + ' months projected</div>' +
        '</div>';
      // Math box \u2014 show the actual formula with proper signs
      let mathLabel, mathBody, mathTotal;
      if (col === 'c3') {
        mathLabel = 'COL 3 \u00b7 YTD Actual (raw posted; no accrual/unpaid)';
        mathBody  = '<b>\u03a3 ytd = ' + fmt(totalYtd) + '</b>';
        mathTotal = totalYtd;
      } else if (col === 'c4') {
        // Step-by-step breakdown so it's clear where each number comes from.
        const projBase = totalYtd + totalAcc + totalUnp;
        const baseExpr = fmt(totalYtd) + fmtTerm(totalAcc) + fmtTerm(totalUnp);
        const projExpr = (ytdM > 0)
          ? '(' + baseExpr + ') \u00f7 ' + ytdM + ' \u00d7 ' + remM
          : '0';
        const adjExpr  = fmtTerm(totalAcc).trim() + fmtTerm(totalUnp);
        mathLabel = 'COL 4 \u00b7 Estimate = projection (remaining ' + remM + ' mo) + accrual/unpaid adjustment';
        mathBody = ''
          + '<div>Projection base = ytd + accrual + unpaid = ' + baseExpr + ' = ' + fmt(projBase) + '</div>'
          + '<div>Projection (' + remM + ' months) = ' + projExpr + ' = <b>' + fmt(totalEst) + '</b></div>'
          + '<div>Accrual + Unpaid adjustment = ' + adjExpr + ' = <b>' + fmt(totalAcc + totalUnp) + '</b></div>'
          + '<div style="margin-top:4px;border-top:1px solid #bbf7d0;padding-top:4px;"><b>COL 4 = ' + fmt(totalEst) + fmtTerm(totalAcc + totalUnp) + ' = ' + fmt(totalCol4) + '</b></div>';
        mathTotal = totalCol4;
      } else {
        // c5 = Forecast = COL 3 + COL 4
        mathLabel = 'COL 5 \u00b7 Forecast = YTD + Estimate (= COL 3 + COL 4)';
        mathBody = ''
          + '<div>YTD (COL 3) = ' + fmt(totalYtd) + '</div>'
          + '<div>Estimate (COL 4) = ' + fmt(totalCol4) + '</div>'
          + '<div style="margin-top:4px;border-top:1px solid #bbf7d0;padding-top:4px;"><b>COL 5 = ' + fmt(totalYtd) + fmtTerm(totalCol4) + ' = ' + fmt(totalFc) + '</b></div>';
        mathTotal = totalFc;
      }
      html += '<div style="background:#f0fdf4;border:1px solid #bbf7d0;padding:10px 12px;border-radius:6px;font-family:monospace;font-size:13px;margin-bottom:10px;">' +
        '<div style="color:var(--gray-500);font-size:11px;text-transform:uppercase;letter-spacing:0.3px;margin-bottom:6px;">' + mathLabel + '</div>' +
        mathBody +
        '</div>';
      // Per-line table
      html += '<table style="width:100%;border-collapse:collapse;font-size:12px;">' +
        '<thead><tr style="background:var(--gray-100);color:var(--gray-700);">' +
        '<th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--gray-200);">GL</th>' +
        '<th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Description</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">YTD Actual</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Accrual</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Unpaid</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Estimate</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Forecast</th>' +
        '</tr></thead><tbody>';
      lines.forEach(l => {
        const cell = (key) => {
          const v = l[key];
          const bg = (key === hi) ? 'background:#f0fdf4;font-weight:700;color:#15803d;' : '';
          return '<td style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-100);font-variant-numeric:tabular-nums;'+bg+'">' + fmt(v) + '</td>';
        };
        html += '<tr>' +
          '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);font-family:monospace;">' + (l.gl||'') + '</td>' +
          '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);color:var(--gray-700);">' + (l.desc||'') + '</td>' +
          cell('ytd') + cell('accrual') + cell('unpaid') + cell('estimate') + cell('forecast') +
          '</tr>';
      });
      const tcell = (key, val) => {
        const bg = (key === hi) ? 'background:#f0fdf4;color:#15803d;font-weight:700;' : 'color:var(--gray-500);';
        return '<td style="text-align:right;padding:6px 8px;border-top:2px solid var(--gray-300);font-variant-numeric:tabular-nums;'+bg+'">' + fmt(val) + '</td>';
      };
      const hiddenNote = hiddenCount > 0 ? ' <span style="color:var(--gray-400);font-weight:400;font-size:11px;">(' + hiddenCount + ' zero hidden)</span>' : '';
      html += '<tr style="background:var(--gray-50);">' +
        '<td colspan="2" style="padding:6px 8px;border-top:2px solid var(--gray-300);color:var(--gray-500);">Total (' + lines.length + ' lines)' + hiddenNote + '</td>' +
        tcell('ytd', totalYtd) + tcell('accrual', totalAcc) + tcell('unpaid', totalUnp) +
        tcell('estimate', totalEst) + tcell('forecast', totalFc) +
        '</tr>';
      html += '</tbody></table>';
    }
  }
  panel.innerHTML = html;
  panel.style.display = 'block';
}

// ── Phase 2: per-line audit drill-down mutations ─────────────────────────
// All four mutations use the same backend endpoint:
//   PATCH /api/af/uploads/<upload_id>/source-line  for edit/move/delete
//   POST  /api/af/uploads/<upload_id>/source-line  for add
// On success, re-fetch the summary to refresh col2 totals + the Inspector
// panel, since the FA's edit may have moved value between summary rows.

async function _auditLinePatch(uploadId, body) {
  const resp = await fetch('/api/af/uploads/' + uploadId + '/source-line', {
    method: 'PATCH',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  });
  return resp.json();
}

async function _auditLinePost(uploadId, body) {
  const resp = await fetch('/api/af/uploads/' + uploadId + '/source-line', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  });
  return resp.json();
}

// Reload summary + replay the drill panel for the same row so the FA stays
// in context after a mutation. The summary tab uses renderBudgetSummary
// (not loadSummary, which doesn't exist on this page); calling it refetches
// /api/summary and rebuilds the table + window._sumLineage so the next
// sumRenderDrillPanel call sees fresh source_lines + IDs.
async function _auditAfterMutation(rowLabel) {
  const contentDiv = document.getElementById('sheetContent');
  if (contentDiv && typeof renderBudgetSummary === 'function') {
    try { await renderBudgetSummary(contentDiv); } catch (e) { console.warn('renderBudgetSummary failed', e); }
  }
  // Re-render the drill panel for the same label so the FA can keep editing.
  if (rowLabel) {
    try { sumRenderDrillPanel(rowLabel, 'c2'); } catch (e) { console.warn('drill replay failed', e); }
  }
}

function _auditBtnContext(btn) {
  return {
    auditId: btn.getAttribute('data-audit-id'),
    lineId: btn.getAttribute('data-line-id'),
    summaryLabel: btn.getAttribute('data-summary-label'),
  };
}

function auditLineEdit(btn) {
  const ctx = _auditBtnContext(btn);
  const oldAmt = Number(btn.getAttribute('data-line-amount')) || 0;
  const desc = btn.getAttribute('data-line-desc') || '';
  const inputStr = window.prompt(
    'Edit amount for "' + desc + '":',
    String(Math.round(oldAmt))
  );
  if (inputStr === null) return;
  const newAmt = parseFloat(inputStr.replace(/[,$\s]/g, ''));
  if (isNaN(newAmt)) {
    alert('Invalid amount: "' + inputStr + '"');
    return;
  }
  if (Math.abs(newAmt - oldAmt) < 0.005) return;  // no-op
  _auditLinePatch(ctx.auditId, {
    summary_label: ctx.summaryLabel,
    line_id: ctx.lineId,
    action: 'edit',
    new_amount: newAmt,
  }).then(j => {
    if (j && j.success) {
      _auditAfterMutation(ctx.summaryLabel);
    } else {
      alert('Edit failed: ' + (j && j.error || 'unknown error'));
    }
  }).catch(e => alert('Edit error: ' + e.message));
}

function auditLineMove(btn) {
  const ctx = _auditBtnContext(btn);
  const targets = window._auditMoveTargets || [];
  if (!targets.length) {
    alert('No other summary rows available to move to.');
    return;
  }
  const desc = btn.closest('tr')?.querySelector('td')?.textContent?.trim() || 'this line';
  // Lightweight modal: backdrop + centered card with a real <select> dropdown
  // and Move/Cancel buttons. No window.prompt typing — FA picks from a list.
  // Group options by section (Income / Expenses / etc.) when section info is
  // available on window._sumRowMap so the picker matches the summary layout.
  const rowMap = window._sumRowMap || {};
  const groups = {};  // {section: [labels]}
  targets.forEach(t => {
    const sec = (rowMap[t] && rowMap[t].section) || 'Other';
    (groups[sec] = groups[sec] || []).push(t);
  });
  const sectionOrder = ['Income', 'Expenses', 'Non-Operating Income', 'Non-Operating Expenses', 'Other'];
  let optsHtml = '';
  sectionOrder.forEach(sec => {
    if (!groups[sec]) return;
    optsHtml += '<optgroup label="' + sec + '">';
    groups[sec].forEach(t => {
      optsHtml += '<option value="' + t.replace(/"/g, '&quot;') + '">' + t + '</option>';
    });
    optsHtml += '</optgroup>';
  });
  // Pick up any sections not in our preset order (defensive)
  Object.keys(groups).forEach(sec => {
    if (sectionOrder.includes(sec)) return;
    optsHtml += '<optgroup label="' + sec + '">';
    groups[sec].forEach(t => {
      optsHtml += '<option value="' + t.replace(/"/g, '&quot;') + '">' + t + '</option>';
    });
    optsHtml += '</optgroup>';
  });

  const overlay = document.createElement('div');
  overlay.id = 'auditMoveOverlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(15,23,42,0.45);display:flex;align-items:center;justify-content:center;z-index:1000;';
  overlay.innerHTML =
    '<div style="background:white;border-radius:10px;box-shadow:0 20px 50px rgba(0,0,0,0.25);width:420px;max-width:90vw;padding:20px 22px;">' +
      '<div style="font-size:11px;font-weight:700;color:var(--gray-500);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;">Move audit line</div>' +
      '<div style="font-size:14px;font-weight:600;color:var(--gray-800);margin-bottom:4px;">' + desc.replace(/[<>]/g, '') + '</div>' +
      '<div style="font-size:12px;color:var(--gray-500);margin-bottom:14px;">From <b>' + ctx.summaryLabel + '</b> to:</div>' +
      '<select id="auditMoveSelect" style="width:100%;padding:8px 10px;font-size:13px;border:1px solid var(--gray-300);border-radius:6px;background:white;cursor:pointer;">' + optsHtml + '</select>' +
      '<div style="display:flex;justify-content:flex-end;gap:8px;margin-top:18px;">' +
        '<button id="auditMoveCancel" style="padding:6px 14px;font-size:13px;background:white;color:var(--gray-700);border:1px solid var(--gray-300);border-radius:6px;cursor:pointer;">Cancel</button>' +
        '<button id="auditMoveConfirm" style="padding:6px 14px;font-size:13px;background:var(--blue);color:white;border:none;border-radius:6px;cursor:pointer;font-weight:600;">Move</button>' +
      '</div>' +
    '</div>';
  document.body.appendChild(overlay);

  const closeOverlay = () => { try { document.body.removeChild(overlay); } catch (e) {} };
  document.getElementById('auditMoveCancel').onclick = closeOverlay;
  // Click outside the card → cancel.
  overlay.addEventListener('click', (e) => { if (e.target === overlay) closeOverlay(); });
  // ESC → cancel.
  const escHandler = (e) => { if (e.key === 'Escape') { closeOverlay(); document.removeEventListener('keydown', escHandler); } };
  document.addEventListener('keydown', escHandler);

  document.getElementById('auditMoveConfirm').onclick = () => {
    const sel = document.getElementById('auditMoveSelect');
    const tgt = sel ? sel.value : '';
    if (!tgt) { alert('Pick a target row.'); return; }
    closeOverlay();
    document.removeEventListener('keydown', escHandler);
    _auditLinePatch(ctx.auditId, {
      summary_label: ctx.summaryLabel,
      line_id: ctx.lineId,
      action: 'move',
      new_summary_label: tgt,
    }).then(j => {
      if (j && j.success) {
        _auditAfterMutation(ctx.summaryLabel);
      } else {
        alert('Move failed: ' + (j && j.error || 'unknown error'));
      }
    }).catch(e => alert('Move error: ' + e.message));
  };
  // Auto-focus the select for immediate keyboard navigation.
  setTimeout(() => { const s = document.getElementById('auditMoveSelect'); if (s) s.focus(); }, 0);
}

function auditLineDelete(btn) {
  const ctx = _auditBtnContext(btn);
  if (!confirm('Delete this audit line? It will be removed from this building only — original audit extraction is preserved.')) return;
  _auditLinePatch(ctx.auditId, {
    summary_label: ctx.summaryLabel,
    line_id: ctx.lineId,
    action: 'delete',
  }).then(j => {
    if (j && j.success) {
      _auditAfterMutation(ctx.summaryLabel);
    } else {
      alert('Delete failed: ' + (j && j.error || 'unknown error'));
    }
  }).catch(e => alert('Delete error: ' + e.message));
}

function auditLineAdd(uploadId, summaryLabel) {
  const desc = window.prompt('New audit line description (e.g. "Late fee adjustment"):', '');
  if (!desc) return;
  const amtStr = window.prompt('Amount for "' + desc + '":', '0');
  if (amtStr === null) return;
  const amt = parseFloat(String(amtStr).replace(/[,$\s]/g, ''));
  if (isNaN(amt)) {
    alert('Invalid amount: "' + amtStr + '"');
    return;
  }
  _auditLinePost(uploadId, {
    summary_label: summaryLabel,
    auditor_desc: desc,
    amount: amt,
  }).then(j => {
    if (j && j.success) {
      _auditAfterMutation(summaryLabel);
    } else {
      alert('Add failed: ' + (j && j.error || 'unknown error'));
    }
  }).catch(e => alert('Add error: ' + e.message));
}

function sumCellBlur(el) {
  // FA dir 2026-05-17: track whether the user's input was a formula so we
  // can persist the formula string alongside the result. typedFormula is
  // set to the raw "=..." string when applicable, else null.
  // FA dir 2026-05-24: skip the bg/border tweaks on subtotal cells — the
  // save-callback (below) handles their styling, and the data-row colors
  // would briefly flash through the green-fill/dark-blue subtotal look.
  const isSubtotalCell = el.dataset.subtotal === '1';
  let typedFormula = null;
  if (el.value && !el.value.startsWith('=')) {
    const num = parseFloat(el.value.replace(/,/g, ''));
    if (!isNaN(num)) {
      el.dataset.raw = num;
      el.value = num.toLocaleString('en-US', {maximumFractionDigits:0});
      if (!isSubtotalCell) el.style.background = el.dataset.col === 'c7' ? '#fffbeb' : '#fbfaf4';
    }
  } else if (el.value.startsWith('=')) {
    typedFormula = el.value.trim();
    try {
      const clean = typedFormula.slice(1).replace(/,/g, '');
      const r = Function('"use strict"; return (' + clean + ')')();
      el.dataset.raw = r;
      if (!isSubtotalCell) {
        el.style.background = '#f0fdf4';
        el.style.borderColor = '#bbf7d0';
      }
    } catch(e) { typedFormula = null; }
  } else { el.dataset.raw = ''; }
  // FA #17 (2026-06-13): editing the 2026 Estimate (c4) or YTD (c3) re-derives
  // the 2026 Forecast (c5 = c3 + c4) LIVE, unless the forecast was separately
  // overridden. The backend recomputes the same on reload (api_summary GET);
  // this just reflects it immediately so the forecast reacts to the estimate.
  (function(){
    var _col = el.dataset.col, _tr = el.closest('tr');
    if (!_tr || (_col !== 'c4' && _col !== 'c3')) return;
    var c5 = _tr.querySelector('[data-col="c5"]');
    if (!c5 || c5.dataset.overridden === '1') return;
    var c3el = _tr.querySelector('[data-col="c3"]');
    var c4el = _tr.querySelector('[data-col="c4"]');
    var v3 = parseFloat(c3el && c3el.dataset.raw); if (isNaN(v3)) v3 = 0;
    var v4 = parseFloat(c4el && c4el.dataset.raw); if (isNaN(v4)) v4 = 0;
    var v5 = Math.round((v3 + v4) * 100) / 100;
    c5.dataset.raw = v5;
    c5.value = v5.toLocaleString('en-US', {maximumFractionDigits:0});
  })();
  sumRecalcTotals();
  // Auto-save edits — col7 (proposed) goes to col7; c3/c4/c5 land in *_override
  // FA directive 2026-05-05: editable green cells.
  const col = el.dataset.col;
  const tr = el.closest('tr');
  const order = tr ? tr.dataset.order : null;
  if (!order) return;
  // FA dir 2026-05-17: c1/c2/c6 are now editable too (via *_override fields).
  const editable = (col === 'c7') ||
                   (col === 'c1') || (col === 'c2') || (col === 'c3') ||
                   (col === 'c4') || (col === 'c5') || (col === 'c6');
  if (!editable) return;
  // FA directive 2026-05-10: short-circuit when the FA didn't actually
  // change the value during this focus session. Without this guard, just
  // clicking into a cell and clicking out would write an "override" equal
  // to the computed value — flipping the cell to OVR with no real change.
  // Compare numerically so display-formatting drift doesn't matter
  // (e.g., "60112" vs "60,112" parse to the same number).
  const focusedRawStr = (el.dataset.focusedRaw === undefined) ? '' : String(el.dataset.focusedRaw);
  const currentRawStr = (el.dataset.raw === undefined) ? '' : String(el.dataset.raw);
  const focusedNumNorm = (focusedRawStr === '' || focusedRawStr === 'null') ? null : parseFloat(focusedRawStr);
  const currentNumNorm = (currentRawStr === '' || currentRawStr === 'null') ? null : parseFloat(currentRawStr);
  const unchanged = (
    (focusedNumNorm === null && currentNumNorm === null) ||
    (focusedNumNorm !== null && currentNumNorm !== null &&
     !isNaN(focusedNumNorm) && !isNaN(currentNumNorm) &&
     focusedNumNorm === currentNumNorm)
  );
  if (unchanged) return;
  // Build the edit payload depending on which column was touched.
  // Empty input → null (clear override / proposed)
  const rawNum = (el.dataset.raw === '' || el.dataset.raw === undefined) ? null : parseFloat(el.dataset.raw);
  const edit = { display_order: parseInt(order) };
  if (col === 'c7')      edit.col7 = rawNum;
  else if (col === 'c1') edit.col1_override = rawNum;
  else if (col === 'c2') edit.col2_override = rawNum;
  else if (col === 'c3') edit.col3_override = rawNum;
  else if (col === 'c4') edit.col4_override = rawNum;
  else if (col === 'c5') edit.col5_override = rawNum;
  else if (col === 'c6') edit.col6_override = rawNum;
  // FA dir 2026-05-17: persist formula string. typedFormula is set when the
  // user entered a "=" expression. A plain-number edit clears any prior
  // formula (since the number isn't tied to a formula anymore).
  edit[col + '_formula'] = typedFormula;
  if (typedFormula) el.dataset.formula = typedFormula;
  else delete el.dataset.formula;
  fetch('/api/summary/' + entityCode, {method:'PUT', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({edits:[edit]})
  }).then(r => r.json()).then(data => {
    // Reflect override state visually after save (no full reload).
    // 2026-05-17: c1/c2/c6 also get the OVR badge treatment.
    // 2026-05-24: subtotal c7 also OVR-badges (col7_proposed_budget acts as
    // an override of the computed sum on subtotal rows). data-subtotal="1"
    // is stamped by makeSubtotalInput.
    const isSubtotal = el.dataset.subtotal === '1';
    const ovrEligible = (col === 'c1' || col === 'c2' || col === 'c3' || col === 'c4' || col === 'c5' || col === 'c6') ||
                        (isSubtotal && col === 'c7');
    if (ovrEligible) {
      const isNowOverridden = (rawNum !== null && !isNaN(rawNum));
      el.dataset.overridden = isNowOverridden ? '1' : '0';
      // Toggle stripe + amber tint
      if (isNowOverridden) {
        el.style.background = '#fef3c7';
        el.style.boxShadow = 'inset 3px 0 0 #d97706';
        el.style.color = '#92400e';
        el.style.fontWeight = '700';
        // Inject OVR badge if not already present
        const td = el.parentElement;
        if (td && !td.querySelector('.sum-ovr-badge')) {
          const badge = document.createElement('span');
          badge.className = 'sum-ovr-badge';
          badge.style.cssText = 'position:absolute;top:2px;right:4px;font-size:8px;font-weight:700;color:#92400e;background:#fde68a;padding:1px 3px;border-radius:3px;letter-spacing:0.3px;pointer-events:none;';
          badge.textContent = 'OVR';
          td.appendChild(badge);
        }
        el.title = 'Override active. Right-click to revert.';
      } else {
        // FA dir 2026-05-24: subtotal cells get their green-fill/dark-blue
        // styling back when override is cleared, not the data-row green-stripe.
        if (isSubtotal) {
          const isGrandRow = !!el.closest('tr[data-calc="grand"]');
          el.style.background = isGrandRow ? '#1e3a5f' : '#f0fdf4';
          el.style.color = isGrandRow ? '#86efac' : '#16a34a';
          el.style.boxShadow = '';
          el.style.fontWeight = '700';
        } else {
          el.style.background = '#f9f9f7';
          el.style.boxShadow = 'inset 3px 0 0 #16a34a';
          el.style.color = '#15803d';
          el.style.fontWeight = '600';
        }
        const td = el.parentElement;
        if (td) { const badge = td.querySelector('.sum-ovr-badge'); if (badge) badge.remove(); }
        el.title = 'Click to override the computed value';
      }
    }
  }).catch(()=>{});
}

// FA directive 2026-05-05: right-click on c3/c4/c5 to revert to computed value.
// 2026-05-17: extended to c1/c2/c6 (which revert to imported / audit-computed source).
// Sends {col*_override: null} which clears the override; server returns the
// row to GL-aggregation / import behavior on next render.
function sumCellRevert(event, el) {
  event.preventDefault();
  if (el.dataset.overridden !== '1') return false;
  const col = el.dataset.col;
  const computed = el.dataset.computed;
  const tr = el.closest('tr');
  const order = tr ? tr.dataset.order : null;
  if (!order) return false;
  if (!confirm('Revert ' + (el.dataset.label || '') + ' ' + col.toUpperCase() + ' to computed value (' + (computed || '—') + ')?')) return false;
  const edit = { display_order: parseInt(order) };
  const isSubtotal = el.dataset.subtotal === '1';
  if (col === 'c1') edit.col1_override = null;
  else if (col === 'c2') edit.col2_override = null;
  else if (col === 'c3') edit.col3_override = null;
  else if (col === 'c4') edit.col4_override = null;
  else if (col === 'c5') edit.col5_override = null;
  else if (col === 'c6') edit.col6_override = null;
  // FA dir 2026-05-24: subtotal c7 revert clears col7_proposed_budget so the
  // subtotal goes back to computed (sum of data-row col7s). Data-row c7
  // revert is still blocked — c7 IS the value on data rows.
  else if (col === 'c7' && isSubtotal) edit.col7 = null;
  else return false;
  // FA dir 2026-05-17: clear any saved formula too — reverting wipes both
  // the override value and the formula expression. The cell falls back to
  // its computed/imported source.
  edit[col + '_formula'] = null;
  delete el.dataset.formula;
  fetch('/api/summary/' + entityCode, {method:'PUT', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({edits:[edit]})
  }).then(r => r.json()).then(data => {
    // Restore computed value display
    el.dataset.overridden = '0';
    el.dataset.raw = computed || '';
    el.value = (computed && computed !== '' && computed !== '0')
      ? Number(computed).toLocaleString('en-US') : '';
    // FA dir 2026-05-24: subtotal cells revert to green-fill/dark-blue, not
    // the data-row green-stripe look.
    if (isSubtotal) {
      const isGrandRow = !!el.closest('tr[data-calc="grand"]');
      el.style.background = isGrandRow ? '#1e3a5f' : '#f0fdf4';
      el.style.color = isGrandRow ? '#86efac' : '#16a34a';
      el.style.boxShadow = '';
      el.style.fontWeight = '700';
    } else {
      el.style.background = '#f9f9f7';
      el.style.boxShadow = 'inset 3px 0 0 #16a34a';
      el.style.color = '#15803d';
      el.style.fontWeight = '600';
    }
    const td = el.parentElement;
    if (td) { const badge = td.querySelector('.sum-ovr-badge'); if (badge) badge.remove(); }
    el.title = 'Click to override the computed value';
    sumRecalcTotals();
    showToast('Reverted to computed', 'success');
  }).catch(e => alert('Revert failed: ' + e.message));
  return false;
}

function sumCellKey(e, el) {
  if (e.key === 'Enter') { el.blur(); sumAcceptFormula(); }
  else if (e.key === 'Escape') { sumCancelFormula(); }
}

// ── Summary tab Per-tab Undo + History (FA dir 2026-05-19) ────────────
// Reuses /api/recent-changes/<entity>?sheet=Summary + same undo endpoint.
// After a successful undo, re-renders the Summary tab so the change shows.

async function sumTabUndoLast() {
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) + '?sheet=Summary&limit=20');
    if (!resp.ok) { alert('Could not load recent changes: ' + resp.status); return; }
    const data = await resp.json();
    const changes = data.changes || [];
    const target = changes.find(c => c.undoable);
    if (!target) {
      alert('No undoable changes on the Summary tab yet.');
      return;
    }
    const fieldLabel = target.field || 'cell';
    const lbl = target.summary_label || target.description || '';
    const batchNote = (target.batch_size && target.batch_size > 1)
      ? ('\n\nThis edit changed ' + target.batch_size + ' fields; they revert together.') : '';
    if (!confirm('Undo the most recent change on the Summary tab?\n\n' +
                  (lbl ? lbl + ' · ' : '') + fieldLabel + ': ' +
                  (target.old_value || '(empty)') + ' ← ' + (target.new_value || '(empty)') + batchNote)) return;
    const undoResp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) + '/undo', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({revision_id: target.id}),
    });
    if (!undoResp.ok) {
      alert('Undo failed: ' + (await undoResp.text()).slice(0, 200));
      return;
    }
    // Re-render the Summary tab
    const tab = document.querySelector('.sheet-tab[data-sheet="Summary"]');
    if (tab) tab.click();
  } catch (e) {
    alert('Undo error: ' + e.message);
  }
}

async function sumTabShowHistory() {
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) + '?sheet=Summary&limit=50');
    if (!resp.ok) { alert('Could not load history: ' + resp.status); return; }
    const data = await resp.json();
    _sumTabRenderHistoryModal(data.changes || []);
  } catch (e) {
    alert('History error: ' + e.message);
  }
}

function _sumTabRenderHistoryModal(changes) {
  const existing = document.getElementById('sumTabHistoryRoot');
  if (existing) existing.remove();
  const COL_LABELS = {col7:'2027 Proposed', col1_override:'Col 1 (Actual BY-3)', col2_override:'Col 2 (Actual BY-2)',
                       col3_override:'Col 3 (YTD)', col4_override:'Col 4 (Estimate)',
                       col5_override:'Col 5 (Forecast)', col6_override:'Col 6 (Curr Budget)'};
  const _esc = (s) => (s===null||s===undefined?'':String(s)).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  const _fmtV = (raw, field) => {
    if (raw === null || raw === undefined || raw === '') return '(empty)';
    const n = parseFloat(String(raw));
    if (!isNaN(n)) return (n < 0 ? '-$' : '$') + Math.abs(Math.round(n)).toLocaleString();
    return String(raw).slice(0, 60);
  };
  let html = '';
  html += '<div id="sumTabHistoryOverlay" onclick="_sumTabCloseHistory()" style="position:fixed;inset:0;background:rgba(0,0,0,0.45);z-index:1000;"></div>';
  html += '<div id="sumTabHistoryModal" style="position:fixed;top:60px;left:50%;transform:translateX(-50%);width:720px;max-width:94vw;max-height:82vh;background:white;border-radius:12px;box-shadow:0 24px 60px rgba(0,0,0,0.3);z-index:1001;overflow:hidden;display:flex;flex-direction:column;">';
  html += '<div style="padding:14px 22px;border-bottom:1px solid var(--gray-200);display:flex;justify-content:space-between;align-items:center;">';
  html += '<h3 style="margin:0;font-size:15px;font-weight:700;color:var(--gray-900);">⏱ History · Summary tab</h3>';
  html += '<button onclick="_sumTabCloseHistory()" style="border:none;background:transparent;font-size:20px;cursor:pointer;color:var(--gray-500);line-height:1;">×</button>';
  html += '</div>';
  html += '<div style="padding:8px 18px;font-size:11px;color:var(--gray-500);background:#fafbfc;border-bottom:1px solid var(--gray-200);">';
  html += changes.length + ' change' + (changes.length !== 1 ? 's' : '') + ' · newest first · Restore reverts one cell';
  html += '</div>';
  html += '<div style="overflow-y:auto;flex:1;">';
  if (!changes.length) {
    html += '<div style="padding:40px;text-align:center;color:var(--gray-500);font-size:13px;">No edits logged on the Summary tab yet.</div>';
  } else {
    for (const c of changes) {
      const colLabel = COL_LABELS[c.field] || c.field || '';
      const lbl = c.summary_label || c.description || '(unknown row)';
      const oldDisp = _fmtV(c.old_value, c.field);
      const newDisp = _fmtV(c.new_value, c.field);
      const ts = c.ts ? new Date(c.ts) : null;
      const tsLocal = ts ? ts.toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'}) : '';
      html += '<div style="padding:12px 22px;border-bottom:1px solid var(--gray-100);display:grid;grid-template-columns:1fr auto;gap:12px;">';
      html += '<div style="min-width:0;">';
      html += '<div style="font:600 13px -apple-system,sans-serif;color:var(--gray-900);margin-bottom:3px;">' + _esc(lbl) + '</div>';
      html += '<div style="font-size:12px;color:var(--gray-600);line-height:1.5;">';
      html += '<b style="color:var(--gray-900);">' + _esc(colLabel) + '</b>: ';
      html += '<span style="color:#94a3b8;text-decoration:line-through;">' + _esc(oldDisp) + '</span> → ';
      html += '<span style="color:var(--gray-900);font-weight:600;">' + _esc(newDisp) + '</span>';
      html += '</div>';
      html += '<div style="font-size:11px;color:var(--gray-400);margin-top:4px;">' + _esc(tsLocal);
      if (c.source) html += ' · ' + _esc(c.source);
      html += '</div>';
      html += '</div>';
      if (c.undoable) {
        html += '<button onclick="_sumTabRestoreFromHistory(' + c.id + ', this)" style="align-self:center;padding:6px 14px;font:600 12px -apple-system,sans-serif;background:var(--blue, #1d4ed8);color:white;border:none;border-radius:6px;cursor:pointer;white-space:nowrap;">↺ Restore</button>';
      } else {
        html += '<span style="align-self:center;color:var(--gray-400);font-size:11px;">not undoable</span>';
      }
      html += '</div>';
    }
  }
  html += '</div>';
  html += '<div style="padding:10px 22px;background:#fafbfc;border-top:1px solid var(--gray-200);font-size:10px;color:var(--gray-500);text-align:right;">Last 50 changes shown.</div>';
  html += '</div>';
  const wrap = document.createElement('div');
  wrap.id = 'sumTabHistoryRoot';
  wrap.innerHTML = html;
  document.body.appendChild(wrap);
}

function _sumTabCloseHistory() {
  const r = document.getElementById('sumTabHistoryRoot');
  if (r) r.remove();
}

async function _sumTabRestoreFromHistory(revId, btn) {
  if (!confirm('Restore this version of the cell?')) return;
  if (btn) { btn.disabled = true; btn.textContent = 'Restoring…'; }
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) + '/undo', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({revision_id: revId}),
    });
    if (!resp.ok) {
      alert('Restore failed: ' + (await resp.text()).slice(0, 200));
      if (btn) { btn.disabled = false; btn.textContent = '↺ Restore'; }
      return;
    }
    _sumTabCloseHistory();
    const tab = document.querySelector('.sheet-tab[data-sheet="Summary"]');
    if (tab) tab.click();
  } catch (e) {
    alert('Restore error: ' + e.message);
    if (btn) { btn.disabled = false; btn.textContent = '↺ Restore'; }
  }
}

function sumAcceptFormula() {
  if (!_sumActiveCell) return;
  const val = document.getElementById('sumFBInput').value;
  // 2026-06-07: if the bar still shows the computed equation we pre-filled
  // (FA only looked at the breakdown, didn't edit it), do NOT turn it into an
  // override — just close. Prevents a spurious freeze on computed cells.
  if (_sumActiveCell.dataset._fxeq !== undefined && val === _sumActiveCell.dataset._fxeq) {
    return;
  }
  let parsed = null;
  if (val === '' || val === null || val === undefined) {
    // Empty input \u2192 clear value / revert override
    _sumActiveCell.dataset.raw = '';
    _sumActiveCell.value = '';
    parsed = null;
  } else if (val.trim().startsWith('=')) {
    // Formula path: evaluate as JS expression. Strip commas first so the
    // FA can paste numbers like 4,654,828*1.035 from a spreadsheet without
    // having to clean them up manually.
    try {
      const clean = val.trim().slice(1).replace(/,/g, '');
      const r = Function('"use strict"; return (' + clean + ')')();
      if (!isFinite(r)) throw new Error('non-finite result');
      parsed = r;
      _sumActiveCell.dataset.raw = r;
      _sumActiveCell.value = Math.round(r).toLocaleString('en-US');
      // FA dir 2026-05-24: skip the green-tint flash on subtotal cells — the
      // save callback below sets the correct subtotal styling.
      if (_sumActiveCell.dataset.subtotal !== '1') {
        _sumActiveCell.style.background = '#f0fdf4';
        _sumActiveCell.style.borderColor = '#bbf7d0';
      }
      const prev = document.getElementById('sumFBPreview');
      if (prev) prev.textContent = '= ' + Math.round(r).toLocaleString('en-US');
    } catch(e) {
      const prev = document.getElementById('sumFBPreview');
      if (prev) prev.textContent = '\u26a0 Error';
      return;   // don't recalc/persist on bad formula
    }
  } else {
    // Plain number path: parse, format, store
    const num = parseFloat(String(val).replace(/[$,]/g, ''));
    if (isNaN(num)) {
      const prev = document.getElementById('sumFBPreview');
      if (prev) prev.textContent = '\u26a0 Invalid number';
      return;
    }
    parsed = num;
    _sumActiveCell.dataset.raw = num;
    _sumActiveCell.value = Math.round(num).toLocaleString('en-US');
  }
  // FA dir 2026-05-17: persist override to backend. Previously the formula
  // bar Accept updated the cell visually but never called the save endpoint,
  // so refresh would wipe the change. Now we route through the same PUT path
  // sumCellBlur uses, with the column \u2192 override-field mapping kept in sync.
  // ALSO sends col*_formula so re-clicks restore the original expression for
  // editing (e.g. change "300*12*4" \u2192 "300*12*3" without retyping).
  const cell = _sumActiveCell;
  const col = cell.dataset.col;
  const tr = cell.closest('tr');
  const order = tr ? tr.dataset.order : null;
  const editable = (col === 'c7') || (col === 'c1') || (col === 'c2') ||
                   (col === 'c3') || (col === 'c4') || (col === 'c5') || (col === 'c6');
  // The original input (might be a formula starting with "="). If formula,
  // save its string form so the FA can edit it later. Otherwise clear any
  // previously-saved formula since a plain number doesn't have one.
  const inputWasFormula = (typeof val === 'string' && val.trim().startsWith('='));
  const savedFormulaToPersist = inputWasFormula ? val.trim() : null;
  // Reflect immediately on the cell so re-focus shows the formula without a refetch.
  if (savedFormulaToPersist) cell.dataset.formula = savedFormulaToPersist;
  else delete cell.dataset.formula;
  if (order && editable) {
    const edit = { display_order: parseInt(order, 10) };
    const rawNum = (parsed === null || parsed === undefined || isNaN(parsed)) ? null : parsed;
    if (col === 'c7')      edit.col7 = rawNum;
    else if (col === 'c1') edit.col1_override = rawNum;
    else if (col === 'c2') edit.col2_override = rawNum;
    else if (col === 'c3') edit.col3_override = rawNum;
    else if (col === 'c4') edit.col4_override = rawNum;
    else if (col === 'c5') edit.col5_override = rawNum;
    else if (col === 'c6') edit.col6_override = rawNum;
    // Persist the formula string (or null to clear).
    edit[col + '_formula'] = savedFormulaToPersist;
    fetch('/api/summary/' + entityCode, {
      method: 'PUT', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({edits: [edit]})
    }).then(r => r.json()).then(data => {
      if (data && data.error) {
        if (typeof showToast === 'function') showToast('Save failed: ' + data.error, 'error');
        return;
      }
      // Flip OVR styling on the cell for cols 1-6 (col7 is just an input, no OVR concept).
      // FA dir 2026-05-24: subtotal c7 ALSO gets OVR badging (col7_proposed_budget
      // acts as an override of the computed sum on subtotal rows).
      const isSubtotalCell = cell.dataset.subtotal === '1';
      if (col !== 'c7' || isSubtotalCell) {
        const isNowOverridden = (rawNum !== null && !isNaN(rawNum));
        cell.dataset.overridden = isNowOverridden ? '1' : '0';
        if (isNowOverridden) {
          cell.style.background = '#fef3c7';
          cell.style.boxShadow = 'inset 3px 0 0 #d97706';
          cell.style.color = '#92400e';
          cell.style.fontWeight = '700';
          const td = cell.parentElement;
          if (td && !td.querySelector('.sum-ovr-badge')) {
            const badge = document.createElement('span');
            badge.className = 'sum-ovr-badge';
            badge.style.cssText = 'position:absolute;top:2px;right:4px;font-size:8px;font-weight:700;color:#92400e;background:#fde68a;padding:1px 3px;border-radius:3px;letter-spacing:0.3px;pointer-events:none;';
            badge.textContent = 'OVR';
            td.appendChild(badge);
          }
          cell.title = 'Override active. Right-click to revert.';
        } else if (isSubtotalCell) {
          // Subtotal cell with cleared override → restore green-fill/dark-blue look.
          const isGrandRow = !!cell.closest('tr[data-calc="grand"]');
          cell.style.background = isGrandRow ? '#1e3a5f' : '#f0fdf4';
          cell.style.color = isGrandRow ? '#86efac' : '#16a34a';
          cell.style.boxShadow = '';
          cell.style.fontWeight = '700';
          const td = cell.parentElement;
          if (td) { const badge = td.querySelector('.sum-ovr-badge'); if (badge) badge.remove(); }
          cell.title = 'Click to override the computed subtotal';
        }
      }
      if (typeof showToast === 'function') showToast('Saved', 'success');
    }).catch(err => {
      if (typeof showToast === 'function') showToast('Save failed: ' + err.message, 'error');
    });
  }
  sumRecalcTotals();
  sumResetBar();
}

function sumCancelFormula() {
  if (_sumActiveCell) {
    _sumActiveCell.value = _sumActiveCell.dataset.raw ? parseFloat(_sumActiveCell.dataset.raw).toLocaleString('en-US',{maximumFractionDigits:0}) : '';
  }
  sumResetBar();
}

function sumResetBar() {
  const bar = document.getElementById('sumFBar');
  if (bar) bar.style.borderColor = 'var(--gray-200)';
  const inp = document.getElementById('sumFBInput');
  if (inp) { inp.disabled = true; inp.value = ''; inp.style.opacity = '1'; }
  const lbl = document.getElementById('sumFBLabel');
  if (lbl) lbl.textContent = 'Click a cell\u2026';
  const prev = document.getElementById('sumFBPreview');
  if (prev) prev.textContent = '';
  // FA dir 2026-05-24: also clear the formula breakdown row.
  const bdEl = document.getElementById('sumFBBreakdown');
  if (bdEl) { bdEl.textContent = ''; bdEl.style.display = 'none'; }
  ['sumFBAccept','sumFBCancel','sumFBClear','sumFBInspect'].forEach(id => { const b = document.getElementById(id); if(b) b.style.display='none'; });
  _sumActiveCell = null;
}

// Wire formula bar buttons (called after render)
document.addEventListener('click', function(e) {
  if (e.target.id === 'sumFBAccept') sumAcceptFormula();
  if (e.target.id === 'sumFBCancel') sumCancelFormula();
  if (e.target.id === 'sumFBInspect' || (e.target.closest && e.target.closest('#sumFBInspect'))) {
    console.log('[inspector] Inspect clicked, _sumActiveCell:', _sumActiveCell);
    try {
      if (_sumActiveCell) {
        sumRenderDrillPanel(_sumActiveCell.dataset.label, _sumActiveCell.dataset.col);
      } else {
        const panel = document.getElementById('sumDrillPanel');
        if (panel) {
          panel.innerHTML = '<div style="color:#92400e;background:#fffbeb;padding:10px 12px;border-radius:6px;">No active cell. Click a Col 2-5 cell first, then click Inspect.</div>';
          panel.style.display = 'block';
        }
      }
    } catch (err) {
      console.error('[inspector] render error', err);
      const panel = document.getElementById('sumDrillPanel');
      if (panel) {
        panel.innerHTML = '<div style="color:#991b1b;background:#fee2e2;padding:10px 12px;border-radius:6px;">Inspector error: ' + (err.message || err) + '</div>';
        panel.style.display = 'block';
      }
    }
    return;
  }
  if (e.target.id === 'sumFBClear') {
    if (_sumActiveCell) { _sumActiveCell.value=''; _sumActiveCell.dataset.raw=''; _sumActiveCell.style.background=_sumActiveCell.dataset.col==='c7'?'#fffbeb':'#fbfaf4'; _sumActiveCell.style.borderColor='var(--gray-300)'; }
    sumRecalcTotals(); sumResetBar();
  }
});
document.addEventListener('input', function(e) {
  if (e.target.id === 'sumFBInput') {
    if (_sumActiveCell) _sumActiveCell.value = e.target.value;
    if (e.target.value.startsWith('=')) {
      try { const r = Function('"use strict"; return (' + e.target.value.slice(1) + ')')(); document.getElementById('sumFBPreview').textContent = '= ' + Math.round(r).toLocaleString('en-US'); } catch(ex) { document.getElementById('sumFBPreview').textContent = ''; }
    } else { const p = document.getElementById('sumFBPreview'); if(p) p.textContent = ''; }
  }
});

// ── Summary tab: insert row ──
// Tier 4 universal-row support (2026-05-03): pick from a canonical dropdown
// of SUMMARY_ROW_MAP labels (Commercial Rent, Cable TV, Interest Income, etc).
// On submit we POST to /api/admin/add-summary-row, then run alias-resolve so
// the new row inherits the right gl_prefix and starts pulling data on the
// next render. Falls back to "Custom" free-text for one-off building-specific lines.
let _sumRowOptionsCache = null;

async function sumLoadRowOptions() {
  if (_sumRowOptionsCache) return _sumRowOptionsCache;
  try {
    const r = await fetch('/api/admin/summary-row-options');
    if (r.ok) _sumRowOptionsCache = await r.json();
  } catch (e) { /* ignore */ }
  return _sumRowOptionsCache || {};
}

function _secKeyToApiSection(secKey) {
  if (secKey === 'income') return 'income';
  if (secKey === 'expenses') return 'expenses';
  if (secKey === 'noi') return 'non_operating_income';
  if (secKey === 'noe') return 'non_operating_expense';
  return 'expenses';
}
function _secKeyToServerSection(secKey) {
  if (secKey === 'income') return 'Income';
  if (secKey === 'expenses') return 'Expenses';
  if (secKey === 'noi') return 'Non-Operating Income';
  if (secKey === 'noe') return 'Non-Operating Expenses';
  return 'Expenses';
}

async function sumShowInsert(secKey, secLabel) {
  let modal = document.getElementById('sumInsertModal');
  let overlay = document.getElementById('sumInsertOverlay');
  if (!modal) {
    overlay = document.createElement('div'); overlay.id = 'sumInsertOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.3);z-index:99;';
    overlay.onclick = sumCloseInsert;
    document.body.appendChild(overlay);
    modal = document.createElement('div'); modal.id = 'sumInsertModal';
    modal.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:white;border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,0.2);padding:24px;z-index:100;width:480px;max-height:85vh;overflow-y:auto;';
    document.body.appendChild(modal);
  }
  overlay.style.display = 'block'; modal.style.display = 'block';

  // Filter canonical labels to those NOT already on this entity's summary
  const opts = await sumLoadRowOptions();
  const apiSec = _secKeyToApiSection(secKey);
  const candidates = (opts[apiSec] || []).slice();
  const existingLabels = new Set(Object.keys(window._sumRowMap || {}));
  const available = candidates.filter(l => !existingLabels.has(l));

  let dropdownHtml = '<select id="sumInsCanonical" style="width:100%;padding:8px 10px;border:1px solid var(--gray-200);border-radius:6px;font-size:13px;font-family:inherit;">' +
    '<option value="">— Pick a row to add —</option>';
  available.forEach(l => {
    dropdownHtml += '<option value="'+l.replace(/"/g,'&quot;')+'">'+l+'</option>';
  });
  dropdownHtml += '</select>';

  // Three-mode radio picker (FA directive 2026-05-05):
  //   1) canonical row (default — uses SUMMARY_ROW_MAP entry)
  //   2) custom name (one-off, no GL auto-pull)
  //   3) specific GL (search the building's own GL codes)
  modal.innerHTML = '<h3 style="font-size:15px;font-weight:700;color:var(--blue-dark);margin-bottom:6px;">Add Row to '+secLabel+'</h3>' +
    '<div style="font-size:12px;color:var(--gray-500);margin-bottom:12px;">Pick how to identify the new row. Standard rows pull GL data automatically; specific GLs let you target a single GL code; custom names are manual-entry only.</div>' +
    '<div style="display:flex;gap:6px;margin-bottom:12px;font-size:12px;background:var(--gray-100);padding:4px;border-radius:8px;">' +
      '<label style="flex:1;text-align:center;padding:6px 8px;border-radius:6px;cursor:pointer;background:white;font-weight:600;border:1px solid transparent;" id="sumInsModeLbl_canonical"><input type="radio" name="sumInsMode" value="canonical" checked style="margin-right:4px;">Standard row</label>' +
      '<label style="flex:1;text-align:center;padding:6px 8px;border-radius:6px;cursor:pointer;border:1px solid transparent;" id="sumInsModeLbl_gl"><input type="radio" name="sumInsMode" value="gl" style="margin-right:4px;">Specific GL</label>' +
      '<label style="flex:1;text-align:center;padding:6px 8px;border-radius:6px;cursor:pointer;border:1px solid transparent;" id="sumInsModeLbl_custom"><input type="radio" name="sumInsMode" value="custom" style="margin-right:4px;">Custom name</label>' +
    '</div>' +
    // Mode 1: standard row dropdown (the system\'s pre-built list of summary
    // row labels — formerly called "canonical". Renamed for FA clarity.)
    '<div id="sumInsCanonicalBlock">' +
      '<label style="display:block;font-size:12px;font-weight:600;color:var(--gray-600);margin-bottom:4px;">Pick from the standard row list</label>' +
      dropdownHtml +
      '<div style="font-size:11px;color:var(--gray-500);margin-top:4px;">'+available.length+' standard label(s) available for this section. The new row will auto-pull GL data from the standard mapping.</div>' +
    '</div>' +
    // Mode 2: specific GL search
    '<div id="sumInsGLBlock" style="display:none;">' +
      '<label style="display:block;font-size:12px;font-weight:600;color:var(--gray-600);margin-bottom:4px;">Search GL code or description</label>' +
      '<input id="sumInsGLSearch" type="text" placeholder="Type 4800, &quot;interest&quot;, etc." autocomplete="off" style="width:100%;padding:8px 10px;border:1px solid var(--gray-200);border-radius:6px;font-size:13px;font-family:inherit;">' +
      '<div id="sumInsGLResults" style="max-height:240px;overflow-y:auto;border:1px solid var(--gray-200);border-radius:6px;margin-top:6px;background:white;display:none;"></div>' +
      '<div id="sumInsGLPicked" style="margin-top:8px;padding:8px 10px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:6px;font-size:12px;display:none;"></div>' +
      '<div style="font-size:11px;color:var(--gray-500);margin-top:6px;">Picks one GL. The new row aggregates that GL\'s data via its 4-digit prefix.</div>' +
    '</div>' +
    // Mode 3: custom name
    '<div id="sumInsCustomBlock" style="display:none;">' +
      '<label style="display:block;font-size:12px;font-weight:600;color:var(--gray-600);margin-bottom:4px;">Custom Label</label>' +
      '<input id="sumInsCustomLabel" type="text" placeholder="e.g. Lobby Renovation" style="width:100%;padding:8px 10px;border:1px solid var(--gray-200);border-radius:6px;font-size:13px;font-family:inherit;">' +
      '<div style="font-size:11px;color:var(--gray-500);margin-top:4px;">Custom rows do not auto-pull GL data — manual-entry only.</div>' +
    '</div>' +
    '<div style="display:flex;gap:8px;margin-top:16px;justify-content:flex-end;">' +
    '<button onclick="sumCloseInsert()" style="padding:6px 16px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:1px solid var(--gray-200);background:white;">Cancel</button>' +
    '<button id="sumInsSaveBtn" onclick="sumDoInsert(\''+secKey+'\')" style="padding:6px 16px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:none;background:var(--blue);color:white;">Add Row</button></div>';

  // Wire mode radios — show/hide each block
  const radios = document.getElementsByName('sumInsMode');
  const blocks = {
    canonical: document.getElementById('sumInsCanonicalBlock'),
    gl:        document.getElementById('sumInsGLBlock'),
    custom:    document.getElementById('sumInsCustomBlock'),
  };
  const lbls = {
    canonical: document.getElementById('sumInsModeLbl_canonical'),
    gl:        document.getElementById('sumInsModeLbl_gl'),
    custom:    document.getElementById('sumInsModeLbl_custom'),
  };
  function _refreshMode() {
    const mode = Array.from(radios).find(r => r.checked).value;
    Object.keys(blocks).forEach(k => { if (blocks[k]) blocks[k].style.display = (k === mode ? 'block' : 'none'); });
    Object.keys(lbls).forEach(k => {
      if (!lbls[k]) return;
      if (k === mode) {
        lbls[k].style.background = 'white';
        lbls[k].style.fontWeight = '600';
        lbls[k].style.borderColor = 'var(--gray-200)';
      } else {
        lbls[k].style.background = 'transparent';
        lbls[k].style.fontWeight = '400';
        lbls[k].style.borderColor = 'transparent';
      }
    });
    if (mode === 'canonical') setTimeout(() => document.getElementById('sumInsCanonical').focus(), 30);
    if (mode === 'gl')        setTimeout(() => document.getElementById('sumInsGLSearch').focus(), 30);
    if (mode === 'custom')    setTimeout(() => document.getElementById('sumInsCustomLabel').focus(), 30);
  }
  Array.from(radios).forEach(r => r.addEventListener('change', _refreshMode));

  // Wire GL search — typeahead against the building's own budget_lines.
  // Source = window._data.lines (loaded by loadDetail at dashboard mount).
  // De-dup by gl_code so each GL appears once even if backend returns dupes.
  const allLines = (window._data && Array.isArray(window._data.lines)) ? window._data.lines : [];
  const seenGL = new Set();
  const glIndex = [];
  allLines.forEach(l => {
    const gl = (l.gl_code || '').trim();
    if (!gl || seenGL.has(gl)) return;
    seenGL.add(gl);
    glIndex.push({
      gl,
      gl_base: gl.split('-')[0],
      desc: (l.description || '').trim(),
      tab: l.sheet_name || '',
      cat: (l.category || '').toLowerCase(),
      ytd: l.ytd_actual || 0,
    });
  });
  const searchEl = document.getElementById('sumInsGLSearch');
  const resultsEl = document.getElementById('sumInsGLResults');
  const pickedEl = document.getElementById('sumInsGLPicked');
  function _renderGLResults(q) {
    q = (q || '').trim().toLowerCase();
    if (!q) { resultsEl.style.display = 'none'; resultsEl.innerHTML = ''; return; }
    const matches = glIndex.filter(g =>
      g.gl.toLowerCase().includes(q) || g.desc.toLowerCase().includes(q)
    ).slice(0, 50);
    if (matches.length === 0) {
      resultsEl.style.display = 'block';
      resultsEl.innerHTML = '<div style="padding:10px 12px;color:var(--gray-500);font-size:12px;">No matching GLs.</div>';
      return;
    }
    resultsEl.style.display = 'block';
    resultsEl.innerHTML = matches.map((g, i) =>
      '<div class="sum-ins-gl-row" data-gl="' + g.gl.replace(/"/g,'&quot;') + '" data-base="' + g.gl_base + '" data-desc="' + g.desc.replace(/"/g,'&quot;') + '" data-cat="' + g.cat + '" data-tab="' + g.tab + '" style="padding:7px 10px;border-bottom:1px solid var(--gray-100);cursor:pointer;display:flex;align-items:center;gap:10px;font-size:12px;">' +
      '<span style="font-family:monospace;color:var(--gray-700);font-weight:600;min-width:90px;">' + g.gl + '</span>' +
      '<span style="flex:1;color:var(--gray-700);">' + (g.desc || '<i style="color:var(--gray-400)">(no description)</i>') + '</span>' +
      '<span style="font-size:10px;color:var(--gray-500);background:var(--gray-100);padding:1px 6px;border-radius:3px;">' + g.tab + '</span>' +
      '<span style="font-variant-numeric:tabular-nums;color:var(--gray-500);">YTD: ' + Math.round(g.ytd).toLocaleString('en-US') + '</span>' +
      '</div>'
    ).join('');
    Array.from(resultsEl.querySelectorAll('.sum-ins-gl-row')).forEach(el => {
      el.onmouseover = () => el.style.background = '#eff6ff';
      el.onmouseout  = () => el.style.background = 'transparent';
      el.onclick = () => {
        // Stash picked GL on the modal for sumDoInsert to read
        window._sumInsPickedGL = {
          gl: el.dataset.gl,
          base: el.dataset.base,
          desc: el.dataset.desc,
          cat: el.dataset.cat,
          tab: el.dataset.tab,
        };
        searchEl.value = el.dataset.gl;
        resultsEl.style.display = 'none';
        pickedEl.style.display = 'block';
        // FA dir 2026-05-21: scope toggle. Default to 8-digit (exact) since the
        // FA wants Add Row to capture the specific GL, not aggregate the whole
        // 4-digit family. 4-digit option still available when the FA actually
        // wants all sub-accounts to roll up.
        const gl8 = el.dataset.gl;
        const gl4 = el.dataset.base;
        pickedEl.innerHTML = '<div><b>Picked: </b><code>' + gl8 + '</code> · ' + (el.dataset.desc || '(no desc)') + '</div>' +
          '<div style="margin-top:4px;font-size:11px;color:var(--gray-600);">Row label will be: <input id="sumInsGLPickedLabel" type="text" value="' + (el.dataset.desc || gl8).replace(/"/g, '&quot;') + '" style="margin-left:4px;padding:3px 6px;border:1px solid var(--gray-300);border-radius:3px;font-size:12px;width:55%;"></div>' +
          '<div style="margin-top:8px;display:flex;gap:6px;font-size:11px;">' +
            '<label style="flex:1;padding:6px 8px;border:1px solid var(--blue);background:var(--blue-light, #f5efe7);border-radius:6px;cursor:pointer;display:block;">' +
              '<input type="radio" name="sumInsScope" value="exact" checked style="margin-right:4px;">' +
              '<b>Just this GL</b> <code>' + gl8 + '</code><br>' +
              '<span style="color:var(--gray-600);">Matches only this exact account. Recommended.</span>' +
            '</label>' +
            '<label style="flex:1;padding:6px 8px;border:1px solid var(--gray-300);background:white;border-radius:6px;cursor:pointer;display:block;">' +
              '<input type="radio" name="sumInsScope" value="prefix" style="margin-right:4px;">' +
              '<b>All sub-accounts</b> <code>' + gl4 + '-XXXX</code><br>' +
              '<span style="color:var(--gray-600);">Aggregates the whole 4-digit family.</span>' +
            '</label>' +
          '</div>';
        // Visual feedback for the radios
        const scopeRadios = pickedEl.querySelectorAll('input[name="sumInsScope"]');
        scopeRadios.forEach(r => {
          r.addEventListener('change', () => {
            scopeRadios.forEach(rr => {
              const lbl = rr.parentElement;
              if (rr.checked) {
                lbl.style.borderColor = 'var(--blue)';
                lbl.style.background = 'var(--blue-light, #f5efe7)';
              } else {
                lbl.style.borderColor = 'var(--gray-300)';
                lbl.style.background = 'white';
              }
            });
          });
        });
      };
    });
  }
  if (searchEl) {
    window._sumInsPickedGL = null;
    searchEl.addEventListener('input', e => _renderGLResults(e.target.value));
    searchEl.addEventListener('focus', e => { if (e.target.value) _renderGLResults(e.target.value); });
  }

  _refreshMode();
}

function sumCloseInsert() {
  const m = document.getElementById('sumInsertModal');
  const o = document.getElementById('sumInsertOverlay');
  if (m) m.style.display = 'none';
  if (o) o.style.display = 'none';
}

// Delete a summary row. Calls /api/admin/delete-summary-row (FA-callable,
// no admin auth required since 2026-05-11). The endpoint:
//   - refuses subtotal/section_header rows (returns 403)
//   - refuses imported rows with col6_approved_budget set unless merge_into_label given
//   - succeeds for FA-created data rows
// On 403 or any error, show a toast and leave the row in place.
// FA directive 2026-05-14 Phase 4.3.
async function sumDeleteRow(btnEl, label) {
  if (!label) return;
  if (!confirm('Delete row "' + label + '"? This cannot be undone (re-import the approved file to restore).')) return;
  if (btnEl) { btnEl.disabled = true; btnEl.textContent = '…'; }
  try {
    const resp = await fetch('/api/admin/delete-summary-row', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({entity_code: entityCode, label: label})
    });
    const data = await resp.json();
    if (!resp.ok || data.error) {
      const msg = data.error || ('HTTP ' + resp.status);
      // Endpoint returns 403 with "Cannot delete a row imported..." when the row has
      // imported budget values (col6_approved_budget set). Offer the merge path.
      const isImportedRowRefusal = resp.status === 403 && /imported|approved/i.test(msg);
      if (isImportedRowRefusal) {
        const target = prompt(
          'This row was imported from the approved budget file and has values that would be lost on delete.\n\n' +
          'Type the EXACT label of another row to merge this one INTO (its values get added to that row, then this row is removed). Or click Cancel.'
        );
        if (target && target.trim()) {
          const merged = await fetch('/api/admin/delete-summary-row', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({entity_code: entityCode, label: label, merge_into_label: target.trim()})
          });
          const md = await merged.json();
          if (!merged.ok || md.error) {
            alert('Merge failed: ' + (md.error || merged.status));
            if (btnEl) { btnEl.disabled = false; btnEl.textContent = '×'; }
            return;
          }
          showToast('Merged "' + label + '" into "' + target.trim() + '"', 'success');
          loadDetail();
          return;
        }
        if (btnEl) { btnEl.disabled = false; btnEl.textContent = '×'; }
        return;
      }
      alert('Delete failed: ' + msg);
      if (btnEl) { btnEl.disabled = false; btnEl.textContent = '×'; }
      return;
    }
    showToast('Row "' + label + '" deleted', 'success');
    loadDetail();
  } catch (e) {
    alert('Delete error: ' + (e.message || e));
    if (btnEl) { btnEl.disabled = false; btnEl.textContent = '×'; }
  }
}

// FA directive 2026-06-01: the orphan "+ Add Row" button now ASKS instead of
// auto-creating a row. Two paths:
//   1) Create its own row  — preserves the original behavior (new data line,
//      label = GL description, aggregates the GL's 4-digit family).
//   2) Add to an existing row — folds this GL into a row the building already
//      has so its money rolls into that subtotal. Defaults to the EXACT
//      8-digit GL (safe, can never double-count an orphan) with a 4-digit
//      family option. A live guard (mirrored server-side) blocks a family
//      merge that would double-count against another row.
function sumOrphanClose() {
  var m = document.getElementById('sumOrphanModal');
  var o = document.getElementById('sumOrphanOverlay');
  if (m) m.style.display = 'none';
  if (o) o.style.display = 'none';
}

// prefix-vs-prefix overlap helpers — BROWSER MIRROR of the canonical server
// logic in budget_app/gl_logic.py. There is no build step transpiling Python
// to JS, so this is a hand mirror: it MUST return the same answers as the
// 'overlap' cases in budget_app/gl_test_vectors.json. If you change the rule,
// change gl_logic.py, the vector file, AND this. A bare token matches
// gl_base.startsWith(token); a dashed token matches gl_full.startsWith(token).
// So a short catch-all family like '7' (Capital Expenses) correctly overlaps
// '7120'. (indexOf(x)===0 is startsWith.)
function _sumOrphanFam(x){ return String(x).split('-')[0]; }
function _sumOrphanExact(x){ return String(x).indexOf('-') !== -1; }
function _sumOrphanOverlap(a, b){
  a = String(a); b = String(b);
  var ea = a.indexOf('-') !== -1, eb = b.indexOf('-') !== -1;
  if (!ea && !eb) return a.indexOf(b) === 0 || b.indexOf(a) === 0; // both bare
  if (ea && eb)   return a.indexOf(b) === 0 || b.indexOf(a) === 0; // both dashed
  if (ea && !eb)  return _sumOrphanFam(a).indexOf(b) === 0;        // a dashed
  return _sumOrphanFam(b).indexOf(a) === 0;                        // b dashed
}

function sumOrphanRecompute() {
  var st = window._sumOrphanState; if (!st) return;
  var confirmBtn = document.getElementById('sumOrphanConfirm');
  if (st.mode === 'new') { if (confirmBtn) confirmBtn.disabled = false; return; }
  var sel = document.getElementById('sumOrphanTarget');
  var row = sel ? sel.value : '';
  var token = (st.scope === 'family') ? st.base : st.gl;
  var pv = document.getElementById('sumOrphanPreview');
  var warn = document.getElementById('sumOrphanWarn');
  if (!row) {
    if (pv) pv.innerHTML = 'Pick a row above to see what happens.';
    if (warn) warn.style.display = 'none';
    if (confirmBtn) confirmBtn.disabled = true;
    return;
  }
  // Overlap check against every OTHER data row's prefixes.
  var conflict = null;
  var rowMap = window._sumRowMap || {};
  Object.keys(rowMap).forEach(function(lbl){
    if (lbl === row || conflict) return;
    var r = rowMap[lbl] || {};
    if ((r.row_type || 'data') !== 'data') return;
    var toks = Array.isArray(r.gl_prefixes) ? r.gl_prefixes : [];
    toks.forEach(function(t){ if (!conflict && _sumOrphanOverlap(token, t)) conflict = lbl; });
  });
  if (conflict) {
    if (warn) {
      warn.style.display = 'block';
      warn.innerHTML = '⚠️ <b>' + conflict + '</b> already aggregates GL family ' + _sumOrphanFam(token) + '. Adding it here too would double-count. Switch to <b>Just this GL</b> or pick another row.';
    }
    if (pv) pv.innerHTML = '';
    if (confirmBtn) confirmBtn.disabled = true;
  } else {
    if (warn) warn.style.display = 'none';
    if (pv) pv.innerHTML = 'Adds GL <code>' + token + '</code> to <b>' + row + '</b> — its data now rolls into that row’s subtotal.';
    if (confirmBtn) confirmBtn.disabled = false;
  }
}

function sumOrphanPick(mode) {
  var st = window._sumOrphanState; if (!st) return;
  st.mode = mode;
  var cNew = document.getElementById('sumOrphanChoiceNew');
  var cEx  = document.getElementById('sumOrphanChoiceExist');
  if (cNew) { cNew.style.borderColor = (mode==='new')?'var(--blue)':'var(--gray-200)'; cNew.style.background = (mode==='new')?'#f4f7ff':'white'; }
  if (cEx)  { cEx.style.borderColor  = (mode==='exist')?'var(--blue)':'var(--gray-200)'; cEx.style.background  = (mode==='exist')?'#f4f7ff':'white'; }
  var rN = document.getElementById('sumOrphanRadioNew');   if (rN) rN.checked = (mode==='new');
  var rE = document.getElementById('sumOrphanRadioExist'); if (rE) rE.checked = (mode==='exist');
  var dN = document.getElementById('sumOrphanDetailNew');   if (dN) dN.style.display = (mode==='new')?'block':'none';
  var dE = document.getElementById('sumOrphanDetailExist'); if (dE) dE.style.display = (mode==='exist')?'block':'none';
  sumOrphanRecompute();
}

function sumOrphanScope(scope) {
  var st = window._sumOrphanState; if (!st) return;
  st.scope = scope;
  var oE = document.getElementById('sumOrphanScopeExact');
  var oF = document.getElementById('sumOrphanScopeFamily');
  if (oE) { oE.style.borderColor = (scope==='exact')?'var(--blue)':'var(--gray-300)'; oE.style.background = (scope==='exact')?'#f4f7ff':'white'; }
  if (oF) { oF.style.borderColor = (scope==='family')?'var(--blue)':'var(--gray-300)'; oF.style.background = (scope==='family')?'#f4f7ff':'white'; }
  var rE = document.getElementById('sumOrphanScopeRadioExact');  if (rE) rE.checked = (scope==='exact');
  var rF = document.getElementById('sumOrphanScopeRadioFamily'); if (rF) rF.checked = (scope==='family');
  sumOrphanRecompute();
}

async function sumAddOrphanRow(glCode) {
  var orphan = (window._sumOrphans || {})[glCode];
  if (!orphan) { alert('Could not find GL data for ' + glCode); return; }
  var label = (orphan.suggested_label || orphan.description || glCode).slice(0, 100);
  var section = orphan.suggested_section || 'Expenses';
  var base = orphan.suggested_prefix || (glCode.split('-')[0]);
  var amt = orphan.current_budget || orphan.ytd || 0;
  window._sumOrphanState = {gl: glCode, base: base, label: label, section: section, mode: 'exist', scope: 'exact'};

  var modal = document.getElementById('sumOrphanModal');
  var overlay = document.getElementById('sumOrphanOverlay');
  if (!modal) {
    overlay = document.createElement('div'); overlay.id = 'sumOrphanOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.3);z-index:99;';
    overlay.onclick = sumOrphanClose;
    document.body.appendChild(overlay);
    modal = document.createElement('div'); modal.id = 'sumOrphanModal';
    modal.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:white;border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,0.2);padding:22px;z-index:100;width:520px;max-width:94vw;max-height:88vh;overflow-y:auto;';
    document.body.appendChild(modal);
  }
  overlay.style.display = 'block'; modal.style.display = 'block';

  // Dropdown: every data row on this building (any section), ordered.
  var rowMap = window._sumRowMap || {};
  var dataRows = Object.keys(rowMap)
    .filter(function(l){ return (rowMap[l] || {}).row_type === 'data'; })
    .map(function(l){ return {label: l, section: (rowMap[l]||{}).section || '', order: (rowMap[l]||{}).display_order || 0}; })
    .sort(function(a,b){ return a.order - b.order; });
  var optHtml = '<option value="">— choose a summary row —</option>';
  dataRows.forEach(function(r){
    optHtml += '<option value="' + r.label.replace(/"/g,'&quot;') + '">' + r.label + (r.section ? ('  ·  ' + r.section) : '') + '</option>';
  });

  // Section dropdown for the create-new path (default = suggested section).
  var SECTIONS = ['Income','Expenses','Non-Operating Income','Non-Operating Expense'];
  if (SECTIONS.indexOf(section) === -1) SECTIONS.unshift(section);
  var secHtml = '';
  SECTIONS.forEach(function(s){ secHtml += '<option value="' + s.replace(/"/g,'&quot;') + '"' + (s===section?' selected':'') + '>' + s + '</option>'; });

  var amtStr = (amt && Math.abs(amt) >= 0.5) ? ('$' + Math.round(Math.abs(amt)).toLocaleString('en-US')) : '—';

  modal.innerHTML =
    '<div style="font-size:15px;font-weight:700;color:var(--blue-dark);">Add summary row</div>' +
    '<div style="font-size:12.5px;color:var(--gray-500);margin:3px 0 14px;">GL <b style="font-family:monospace;color:var(--gray-700);">' + glCode + '</b> · “' + label + '” · ' + amtStr + '</div>' +

    '<div id="sumOrphanChoiceNew" onclick="sumOrphanPick(\'new\')" style="display:flex;gap:10px;align-items:flex-start;border:1.5px solid var(--gray-200);border-radius:10px;padding:11px 12px;cursor:pointer;margin-bottom:9px;">' +
      '<input type="radio" id="sumOrphanRadioNew" name="sumOrphanMode" style="margin-top:2px;">' +
      '<div><div style="font-weight:600;">Create its own row</div><div style="font-size:12px;color:var(--gray-500);margin-top:1px;">Adds a new line item. (What the button did before.)</div></div>' +
    '</div>' +

    '<div id="sumOrphanChoiceExist" onclick="sumOrphanPick(\'exist\')" style="display:flex;gap:10px;align-items:flex-start;border:1.5px solid var(--blue);background:#f4f7ff;border-radius:10px;padding:11px 12px;cursor:pointer;margin-bottom:10px;">' +
      '<input type="radio" id="sumOrphanRadioExist" name="sumOrphanMode" checked style="margin-top:2px;">' +
      '<div><div style="font-weight:600;">Add to an existing row</div><div style="font-size:12px;color:var(--gray-500);margin-top:1px;">Folds this GL into a row you already have, so it rolls into that subtotal.</div></div>' +
    '</div>' +

    '<div id="sumOrphanDetailNew" style="display:none;border-top:1px dashed var(--gray-200);padding-top:13px;">' +
      '<label style="display:block;font-size:11px;font-weight:600;color:var(--gray-500);text-transform:uppercase;letter-spacing:.03em;margin-bottom:4px;">New row label</label>' +
      '<input id="sumOrphanNewLabel" type="text" value="' + label.replace(/"/g,'&quot;') + '" style="width:100%;padding:8px 10px;border:1px solid var(--gray-200);border-radius:7px;font-size:13.5px;margin-bottom:11px;">' +
      '<label style="display:block;font-size:11px;font-weight:600;color:var(--gray-500);text-transform:uppercase;letter-spacing:.03em;margin-bottom:4px;">Section</label>' +
      '<select id="sumOrphanNewSection" style="width:100%;padding:8px 10px;border:1px solid var(--gray-200);border-radius:7px;font-size:13.5px;">' + secHtml + '</select>' +
      '<div style="background:#f3f6ff;border:1px solid #cdddff;border-radius:8px;padding:9px 11px;font-size:12px;color:#234;margin-top:11px;">Creates a new line aggregating the whole <code>' + base + '</code> family.</div>' +
    '</div>' +

    '<div id="sumOrphanDetailExist" style="border-top:1px dashed var(--gray-200);padding-top:13px;">' +
      '<label style="display:block;font-size:11px;font-weight:600;color:var(--gray-500);text-transform:uppercase;letter-spacing:.03em;margin-bottom:4px;">Add into which row?</label>' +
      '<select id="sumOrphanTarget" onchange="sumOrphanRecompute()" style="width:100%;padding:8px 10px;border:1px solid var(--gray-200);border-radius:7px;font-size:13.5px;margin-bottom:11px;">' + optHtml + '</select>' +
      '<label style="display:block;font-size:11px;font-weight:600;color:var(--gray-500);text-transform:uppercase;letter-spacing:.03em;margin-bottom:4px;">How much of the GL to pull in</label>' +
      '<div style="display:flex;gap:8px;margin-bottom:10px;">' +
        '<div id="sumOrphanScopeExact" onclick="sumOrphanScope(\'exact\')" style="flex:1;border:1.5px solid var(--blue);background:#f4f7ff;border-radius:8px;padding:8px 10px;cursor:pointer;font-size:12px;">' +
          '<input type="radio" id="sumOrphanScopeRadioExact" name="sumOrphanScope" checked style="margin-right:4px;"><b>Just this GL</b><br><span style="font-family:monospace;color:var(--gray-600);">' + glCode + '</span> <span style="color:var(--gray-500);">· recommended</span>' +
        '</div>' +
        '<div id="sumOrphanScopeFamily" onclick="sumOrphanScope(\'family\')" style="flex:1;border:1.5px solid var(--gray-300);background:white;border-radius:8px;padding:8px 10px;cursor:pointer;font-size:12px;">' +
          '<input type="radio" id="sumOrphanScopeRadioFamily" name="sumOrphanScope" style="margin-right:4px;"><b>Whole family</b><br><span style="font-family:monospace;color:var(--gray-600);">' + base + '-XXXX</span>' +
        '</div>' +
      '</div>' +
      '<div id="sumOrphanWarn" style="display:none;background:#fdecec;border:1px solid #f3b4b4;color:#b91c1c;border-radius:8px;padding:9px 11px;font-size:12px;margin-bottom:8px;"></div>' +
      '<div id="sumOrphanPreview" style="background:#f3f6ff;border:1px solid #cdddff;border-radius:8px;padding:9px 11px;font-size:12px;color:#234;">Pick a row above to see what happens.</div>' +
    '</div>' +

    '<div style="display:flex;gap:8px;margin-top:16px;justify-content:flex-end;">' +
      '<button onclick="sumOrphanClose()" style="padding:6px 16px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:1px solid var(--gray-200);background:white;">Cancel</button>' +
      '<button id="sumOrphanConfirm" onclick="sumOrphanConfirm()" style="padding:6px 16px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:none;background:var(--blue);color:white;">Confirm</button>' +
    '</div>';

  sumOrphanPick('exist');
  sumOrphanScope('exact');
  sumOrphanRecompute();
}

async function sumOrphanConfirm() {
  var st = window._sumOrphanState; if (!st) return;
  var btn = document.getElementById('sumOrphanConfirm');
  if (btn) { btn.disabled = true; btn.textContent = 'Saving...'; }
  try {
    if (st.mode === 'new') {
      // ── Create its own row (preserves the original orphan behavior) ──
      var lblEl = document.getElementById('sumOrphanNewLabel');
      var secEl = document.getElementById('sumOrphanNewSection');
      var newLabel = (lblEl && lblEl.value.trim()) || st.label;
      var newSection = (secEl && secEl.value) || st.section;
      var after_label = null;
      try {
        var rows = (window._sumRowMap && Object.values(window._sumRowMap)) || [];
        var sectionRows = rows
          .filter(function(r){ return r.section === newSection && r.row_type === 'data' && r.label !== newLabel; })
          .sort(function(a,b){ return (a.display_order||0)-(b.display_order||0); });
        if (sectionRows.length > 0) after_label = sectionRows[sectionRows.length-1].label;
      } catch (e) { /* fall back to end */ }
      var resp = await fetch('/api/admin/add-summary-row', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({entity_code: entityCode, label: newLabel, section: newSection, after_label: after_label, gl_prefixes: [st.base]})
      });
      var data = await resp.json();
      if (!resp.ok || data.error) { alert('Add Row failed: ' + (data.error || 'unknown')); if (btn){btn.disabled=false;btn.textContent='Confirm';} return; }
      var existed = (data.noop || '').includes('already exists');
      if (existed) {
        try { await fetch('/api/admin/resolve-summary-aliases/' + entityCode, {method:'POST', headers:{'Content-Type':'application/json'}}); } catch(e){}
        showToast('“' + newLabel + '” already exists — refreshed its GL mapping', 'info');
      } else {
        showToast('Added new row: ' + newLabel, 'success');
      }
    } else {
      // ── Add to an existing row (append prefix; server re-guards) ──
      var sel = document.getElementById('sumOrphanTarget');
      var target = sel ? sel.value : '';
      if (!target) { alert('Pick a row to add this GL into.'); if (btn){btn.disabled=false;btn.textContent='Confirm';} return; }
      var token = (st.scope === 'family') ? st.base : st.gl;
      var resp2 = await fetch('/api/admin/append-summary-prefix', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({entity_code: entityCode, label: target, prefix: token})
      });
      var d2 = await resp2.json();
      if (resp2.status === 409 && d2 && d2.overlap) {
        var warn = document.getElementById('sumOrphanWarn');
        if (warn) { warn.style.display = 'block'; warn.innerHTML = '⚠️ ' + (d2.error || 'This would double-count against another row.'); }
        if (btn){btn.disabled=false;btn.textContent='Confirm';}
        return;
      }
      if (!resp2.ok || d2.error) { alert('Add to row failed: ' + (d2.error || 'unknown')); if (btn){btn.disabled=false;btn.textContent='Confirm';} return; }
      if (d2.noop) {
        showToast(token + ' was already on “' + target + '”', 'info');
      } else {
        showToast('Added GL ' + token + ' into “' + target + '”', 'success');
      }
    }
    sumOrphanClose();
    var sheetContent = document.getElementById('sheetContent');
    if (sheetContent && typeof renderBudgetSummary === 'function') renderBudgetSummary(sheetContent);
  } catch (e) {
    alert('Add Row error: ' + (e.message || e));
    if (btn){btn.disabled=false;btn.textContent='Confirm';}
  }
}

async function sumDoInsert(secKey) {
  // Mode-aware: read the active radio + the corresponding input
  const radios = document.getElementsByName('sumInsMode');
  const mode = (Array.from(radios).find(r => r.checked) || {}).value || 'canonical';
  let label = '';
  let isCanonical = false;
  let gl_prefixes = null;
  if (mode === 'canonical') {
    const sel = document.getElementById('sumInsCanonical');
    const choice = sel ? sel.value : '';
    if (!choice) { alert('Pick a standard row from the dropdown.'); return; }
    label = choice;
    isCanonical = true;
  } else if (mode === 'gl') {
    const picked = window._sumInsPickedGL;
    if (!picked) { alert('Search and select a GL from the results.'); return; }
    // FA can rename the auto-suggested label
    const labelEl = document.getElementById('sumInsGLPickedLabel');
    label = (labelEl && labelEl.value.trim()) || picked.desc || picked.gl;
    // FA dir 2026-05-21: scope picker. "exact" = 8-digit specific GL only
    // (default), "prefix" = 4-digit base aggregates all sub-accounts.
    // The matcher (gl_matches_prefixes) treats strings with "-" as exact
    // and without "-" as 4-digit prefix automatically.
    const scopeRadios = document.getElementsByName('sumInsScope');
    const scope = (Array.from(scopeRadios).find(r => r.checked) || {}).value || 'exact';
    gl_prefixes = (scope === 'prefix') ? [picked.base] : [picked.gl];
  } else if (mode === 'custom') {
    const ci = document.getElementById('sumInsCustomLabel');
    label = (ci ? ci.value : '').trim();
    if (!label) { alert('Enter a custom row name.'); return; }
  }
  if (!label) { alert('Provide a label for the new row.'); return; }

  const serverSection = _secKeyToServerSection(secKey);
  const btn = document.getElementById('sumInsSaveBtn');
  if (btn) { btn.disabled = true; btn.textContent = 'Saving...'; }

  // Compute after_label: insert AFTER the last data row in this section,
  // which means BEFORE the section's subtotal. Without this, the row gets
  // appended to display_order=max+1, landing AFTER Total Income (or whichever
  // section's subtotal) and rendering in the wrong bucket.
  let after_label = null;
  try {
    const rows = (window._sumRowMap && Object.values(window._sumRowMap)) || [];
    const sectionRows = rows
      .filter(r => r._sk === secKey && r.row_type === 'data' && r.label !== label)
      .sort((a, b) => (a.display_order || 0) - (b.display_order || 0));
    if (sectionRows.length > 0) after_label = sectionRows[sectionRows.length - 1].label;
  } catch (e) { /* fall back to end-of-table */ }

  try {
    // Persist via the existing endpoint. gl_prefixes is only sent for GL mode;
    // canonical mode lets alias-resolve attach prefixes server-side.
    const body = {entity_code: entityCode, label: label, section: serverSection, after_label: after_label};
    if (gl_prefixes && gl_prefixes.length) body.gl_prefixes = gl_prefixes;
    const resp = await fetch('/api/admin/add-summary-row', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok || data.error) {
      alert('Add Row failed: ' + (data.error || 'unknown'));
      if (btn) { btn.disabled = false; btn.textContent = 'Add Row'; }
      return;
    }

    // FA directive 2026-05-13: when the row already exists (server returns
    // {ok:true, noop:"row already exists"}), the FA used to see a misleading
    // "Added" toast with no visible change. Now we explicitly tell them the
    // row was already present + that we re-ran resolve-aliases on it (which
    // is the actual fix for an empty-prefix existing row — e.g. 148's
    // "Commercial Rent" or "Bicycle Storage").
    const rowAlreadyExisted = (data.noop || '').includes('already exists');

    // For canonical labels OR when the row already existed, run alias-resolve
    // so existing rows with empty gl_prefixes_json pick up their canonical
    // prefix list. (For GL mode the prefix was already attached at insert.)
    if (isCanonical || rowAlreadyExisted) {
      try {
        await fetch('/api/admin/resolve-summary-aliases/' + entityCode,
                    {method: 'POST', headers: {'Content-Type': 'application/json'}});
      } catch (e) { /* non-fatal */ }
    }

    sumCloseInsert();
    if (rowAlreadyExisted) {
      showToast('"' + label + '" already exists — refreshed its GL mapping from canonical', 'info');
    } else {
      showToast('Added: ' + label, 'success');
    }
    // Reload the summary tab so the new row + its data appear
    const sheetContent = document.getElementById('sheetContent');
    if (sheetContent && typeof renderBudgetSummary === 'function') {
      renderBudgetSummary(sheetContent);
    }
  } catch (e) {
    alert('Add Row error: ' + e.message);
    if (btn) { btn.disabled = false; btn.textContent = 'Add Row'; }
  }
}

function renderReadOnlySheet(sheetName, sheetLines, contentDiv) {
  const thStyle = 'text-align:right; padding:8px; white-space:nowrap;';
  let html = '<table style="width:100%; border-collapse:collapse; font-size:13px;">' +
    '<thead><tr style="background:var(--gray-100); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
    '<th style="text-align:left; padding:8px;">GL Code</th>' +
    '<th style="text-align:left; padding:8px;">Description</th>' +
    '<th style="' + thStyle + '">Prior Year<br>Actual</th>' +
    '<th style="' + thStyle + '">YTD<br>Actual</th>' +
    '<th style="' + thStyle + '">Approved<br>Budget</th>' +
    '<th style="' + thStyle + '">Variance</th>' +
    '</tr></thead><tbody>';

  let totals = {prior:0, ytd:0, budget:0};
  sheetLines.forEach(l => {
    const prior = l.prior_year || 0;
    const ytd = l.ytd_actual || 0;
    const budget = l.current_budget || 0;
    const variance = budget - prior;
    totals.prior += prior; totals.ytd += ytd; totals.budget += budget;
    const varColor = variance >= 0 ? 'var(--red)' : 'var(--green)';

    html += '<tr style="border-bottom:1px solid var(--gray-100);">' +
      '<td style="font-family:monospace; font-size:12px; padding:6px 8px;">' + l.gl_code + '</td>' +
      '<td style="padding:6px 8px;">' + l.description + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(prior) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(ytd) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(budget) + '</td>' +
      '<td style="text-align:right; padding:6px 8px; color:' + varColor + ';">' + fmt(variance) + '</td></tr>';
  });

  const totalVar = totals.budget - totals.prior;
  html += '<tr style="font-weight:700; background:var(--gray-100);"><td style="padding:8px;" colspan="2">Sheet Total</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.prior) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.ytd) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.budget) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totalVar) + '</td></tr>';
  html += '</tbody></table>';
  contentDiv.innerHTML = html;
}

// ── FA Expense drill-down ────────────────────────────────────────────
let _faExpenseCache = null;

async function faFetchExpenseData() {
  if (_faExpenseCache !== null) return _faExpenseCache;
  try {
    const res = await fetch('/api/expense-dist/' + entityCode);
    if (!res.ok) { _faExpenseCache = false; return null; }
    _faExpenseCache = await res.json();
    return _faExpenseCache;
  } catch(e) { _faExpenseCache = false; return null; }
}

async function faToggleInvoices(glCode, el) {
  const row = el.closest('tr');
  const next = row.nextElementSibling;
  if (next && next.classList.contains('fa-invoice-detail')) {
    next.remove();
    row.querySelectorAll('.fa-drill-arrow').forEach(a => a.textContent = '▶');
    return;
  }
  row.querySelectorAll('.fa-drill-arrow').forEach(a => a.textContent = '▼');

  const data = await faFetchExpenseData();
  if (!data || !data.gl_groups) {
    const noData = document.createElement('tr');
    noData.className = 'fa-invoice-detail';
    noData.innerHTML = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="12" style="padding:0;"><div class="drill-sticky" style="padding:12px 24px; background:#fef3c7; font-size:13px;">No expense data uploaded yet.</div></td>';
    row.after(noData);
    return;
  }

  const glGroup = data.gl_groups.find(g => g.gl_code === glCode);
  if (!glGroup || !glGroup.invoices || glGroup.invoices.length === 0) {
    const noInv = document.createElement('tr');
    noInv.className = 'fa-invoice-detail';
    noInv.innerHTML = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="12" style="padding:0;"><div class="drill-sticky" style="padding:12px 24px; background:var(--gray-50); font-size:13px; color:var(--gray-500);">No invoices for ' + glCode + '</div></td>';
    row.after(noInv);
    return;
  }

  const detailRow = document.createElement('tr');
  detailRow.className = 'fa-invoice-detail';
  let html = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="12" style="padding:0;"><div class="drill-sticky" style="padding:12px 16px 12px 24px; background:linear-gradient(to right, #f0f4ff, #f8faff); border-left:3px solid var(--blue); border-bottom:1px solid var(--gray-200);">';
  html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">';
  html += '<span style="font-weight:600; font-size:13px; color:var(--blue);">' + glCode + ' — ' + (glGroup.gl_name || '') + '</span>';
  html += '<span style="font-size:12px; color:var(--gray-500);">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' · $' + Math.round(glGroup.total || 0).toLocaleString() + '</span>';
  html += '</div>';
  html += '<table style="width:auto; font-size:12px; border-collapse:separate; border-spacing:0; background:white; border-radius:6px; box-shadow:0 1px 2px rgba(0,0,0,0.05); overflow:hidden;">';
  html += '<thead><tr style="background:var(--gray-100); color:var(--gray-600); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
  html += '<td style="padding:7px 16px; min-width:140px; border-bottom:2px solid var(--gray-300);">Payee</td><td style="padding:7px 16px; min-width:140px; border-bottom:2px solid var(--gray-300);">Description</td><td style="padding:7px 16px; min-width:70px; border-bottom:2px solid var(--gray-300);">Inv #</td><td style="padding:7px 16px; min-width:85px; border-bottom:2px solid var(--gray-300);">Date</td><td style="padding:7px 16px; min-width:100px; text-align:right; border-bottom:2px solid var(--gray-300);">Amount</td><td style="padding:7px 16px; min-width:90px; border-bottom:2px solid var(--gray-300);">Check #</td><td style="padding:7px 16px; min-width:90px; text-align:center; border-bottom:2px solid var(--gray-300);">Action</td></tr></thead>';

  glGroup.invoices.forEach(inv => {
    const isReclassed = !!inv.reclass_to_gl;
    html += '<tr style="border-top:1px solid var(--gray-200);' + (isReclassed ? ' opacity:0.5; text-decoration:line-through;' : '') + '">';
    html += '<td style="padding:7px 16px; font-size:12px; white-space:nowrap; border-bottom:1px solid var(--gray-200);">' + (inv.payee_name || inv.payee_code || '—') + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; color:var(--gray-600); border-bottom:1px solid var(--gray-200);">' + (inv.notes || '—') + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; font-family:monospace; border-bottom:1px solid var(--gray-200);">' + (inv.invoice_num || '—') + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; border-bottom:1px solid var(--gray-200);">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; text-align:right; font-size:12px; font-weight:600; font-variant-numeric:tabular-nums; border-bottom:1px solid var(--gray-200);">$' + Math.round(inv.amount).toLocaleString() + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; border-bottom:1px solid var(--gray-200);">' + (inv.check_num || '—') + '</td>';
    html += '<td style="padding:7px 16px; text-align:center; border-bottom:1px solid var(--gray-200);">';
    if (isReclassed) {
      html += '<span style="font-size:11px; color:var(--orange);">→ ' + inv.reclass_to_gl + '</span> ';
      html += '<button onclick="faUndoReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:#fef3c7; color:#92400e; border:1px solid #fcd34d; border-radius:4px; cursor:pointer;">Undo</button>';
    } else {
      html += '<span id="fa_reclass_label_' + inv.id + '" style="font-size:11px; color:var(--gray-500); margin-right:4px;"></span>';
      html += '<input type="hidden" id="fa_reclass_gl_' + inv.id + '" value="">';
      html += '<button onclick="faOpenReclassModal(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:var(--gray-100); color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer;">Reclass to…</button> ';
      html += '<button id="fa_reclass_go_' + inv.id + '" onclick="faInlineReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer; display:none;">Go</button>';
    }
    html += '</td></tr>';
  });
  html += '</table></div></td>';
  detailRow.innerHTML = html;
  row.after(detailRow);
}

async function faInlineReclass(invoiceId, fromGL) {
  const select = document.getElementById('fa_reclass_gl_' + invoiceId);
  if (!select || !select.value) { alert('Select a target GL code'); return; }
  try {
    const resp = await fetch('/api/expense-dist/reclass/' + invoiceId, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ reclass_to_gl: select.value, reclass_notes: 'Reclassed from FA workbook' })
    });
    if (resp.ok) {
      _faExpenseCache = null;
      const el = document.querySelector('a[onclick*="faToggleInvoices"][onclick*="' + fromGL + '"]');
      if (el) { faToggleInvoices(fromGL, el); setTimeout(() => faToggleInvoices(fromGL, el), 100); }
      showToast('Reclassified to ' + select.value, 'success');
    } else { showToast('Reclass failed', 'error'); }
  } catch(e) { showToast('Error: ' + e.message, 'error'); }
}

async function faUndoReclass(invoiceId, fromGL) {
  try {
    const resp = await fetch('/api/expense-dist/reclass/' + invoiceId, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ reclass_to_gl: '' })
    });
    if (resp.ok) {
      _faExpenseCache = null;
      const el = document.querySelector('a[onclick*="faToggleInvoices"][onclick*="' + fromGL + '"]');
      if (el) { faToggleInvoices(fromGL, el); setTimeout(() => faToggleInvoices(fromGL, el), 100); }
      showToast('Reclass undone', 'success');
    } else { showToast('Undo failed', 'error'); }
  } catch(e) { showToast('Error: ' + e.message, 'error'); }
}

// ── FA Searchable Reclass Modal (matches PM dashboard) ──────────────
let _faReclassCallback = null;

function faOpenReclassModal(invoiceId, fromGL) {
  _faReclassCallback = { id: invoiceId, fromGL: fromGL };

  let overlay = document.getElementById('faReclassOverlay');
  if (overlay) overlay.remove();

  // Build ALL_GL_CODES from allSheets
  const allGLs = [];
  const seen = {};
  Object.keys(allSheets).forEach(sheet => {
    (allSheets[sheet] || []).forEach(l => {
      if (!seen[l.gl_code]) {
        seen[l.gl_code] = true;
        allGLs.push({ gl_code: l.gl_code, description: l.description || '', category: l.category || 'other' });
      }
    });
  });
  allGLs.sort((a, b) => a.gl_code.localeCompare(b.gl_code));

  // Group by category
  const cats = {};
  const catOrder = [];
  allGLs.filter(g => g.gl_code !== fromGL).forEach(g => {
    const cat = g.category || 'other';
    if (!cats[cat]) { cats[cat] = []; catOrder.push(cat); }
    cats[cat].push(g);
  });
  catOrder.sort();

  const catLabels = {supplies:'Supplies',repairs:'Repairs',maintenance:'Maintenance Contracts',payroll:'Payroll',electric:'Electric',gas:'Gas',fuel:'Fuel',water:'Water & Sewer',sewer:'Water & Sewer',insurance:'Insurance',re_taxes:'Real Estate Taxes',professional:'Professional Fees',admin:'Administrative',financial:'Financial',income:'Income',other:'Other'};

  let listHtml = '';
  catOrder.forEach(cat => {
    listHtml += '<div class="rm-cat-header">' + (catLabels[cat] || cat) + '</div>';
    cats[cat].forEach(g => {
      listHtml += '<div class="rm-gl-row" data-gl="' + g.gl_code + '" data-desc="' + (g.description || '').toLowerCase() + '" data-cat="' + cat + '" onclick="faSelectReclassGL(\'' + g.gl_code + '\',\'' + g.description.replace(/'/g, "\\'") + '\')">';
      listHtml += '<span class="gl-code">' + g.gl_code + '</span>';
      listHtml += '<span class="gl-desc">' + (g.description || '') + '</span>';
      listHtml += '</div>';
    });
  });

  overlay = document.createElement('div');
  overlay.id = 'faReclassOverlay';
  overlay.className = 'fa-reclass-overlay';
  overlay.innerHTML =
    '<div class="fa-reclass-modal">' +
      '<div class="rm-header"><h3>Select Target GL Code</h3>' +
        '<button onclick="document.getElementById(\'faReclassOverlay\').remove()" style="background:none; border:none; font-size:18px; cursor:pointer; color:var(--gray-500);">✕</button></div>' +
      '<div class="rm-search"><input type="text" id="faReclassSearch" placeholder="Search by GL code, name, or category…" oninput="faFilterReclassModal(this.value)" autofocus></div>' +
      '<div class="rm-list" id="faReclassListContainer">' + listHtml + '</div>' +
      '<div class="rm-footer"><span style="font-size:12px; color:var(--gray-500);">' + allGLs.length + ' GL codes available</span></div>' +
    '</div>';
  document.body.appendChild(overlay);

  overlay.addEventListener('click', function(e) { if (e.target === overlay) overlay.remove(); });
  setTimeout(() => { const s = document.getElementById('faReclassSearch'); if (s) s.focus(); }, 50);
}

function faFilterReclassModal(q) {
  q = q.toLowerCase();
  const container = document.getElementById('faReclassListContainer');
  const rows = container.querySelectorAll('.rm-gl-row');
  const catHeaders = container.querySelectorAll('.rm-cat-header');

  rows.forEach(r => {
    const gl = r.dataset.gl.toLowerCase();
    const desc = r.dataset.desc;
    const cat = r.dataset.cat;
    const match = !q || gl.includes(q) || desc.includes(q) || (cat && cat.includes(q));
    r.style.display = match ? '' : 'none';
  });

  catHeaders.forEach(h => {
    let sib = h.nextElementSibling;
    let anyVisible = false;
    while (sib && !sib.classList.contains('rm-cat-header')) {
      if (sib.style.display !== 'none') anyVisible = true;
      sib = sib.nextElementSibling;
    }
    h.style.display = anyVisible ? '' : 'none';
  });
}

function faSelectReclassGL(glCode, glDesc) {
  if (!_faReclassCallback) return;
  const cb = _faReclassCallback;
  const hidden = document.getElementById('fa_reclass_gl_' + cb.id);
  const label = document.getElementById('fa_reclass_label_' + cb.id);
  const goBtn = document.getElementById('fa_reclass_go_' + cb.id);
  if (hidden) hidden.value = glCode;
  if (label) { label.textContent = '→ ' + glCode; label.style.color = 'var(--blue)'; label.style.fontWeight = '600'; }
  if (goBtn) goBtn.style.display = '';
  document.getElementById('faReclassOverlay').remove();
}

// ── FA Zero-row toggle ───────────────────────────────────────────────
let _faShowZeroRows = false;

function faUpdateZeroToggle() {
  const btn = document.getElementById('faZeroToggle');
  if (!btn) return;
  const count = document.querySelectorAll('.fa-grid .zero-row').length;
  if (count === 0) { btn.style.display = 'none'; return; }
  btn.style.display = '';
  btn.textContent = _faShowZeroRows ? 'Hide ' + count + ' Zero Rows' : 'Show ' + count + ' Hidden Rows';
  btn.style.background = _faShowZeroRows ? 'var(--gray-200)' : 'var(--blue-light, #dbeafe)';
  btn.style.color = _faShowZeroRows ? 'var(--gray-600)' : 'var(--blue)';
  btn.style.borderColor = _faShowZeroRows ? 'var(--gray-300)' : 'var(--blue)';
}

function faToggleZeroRows() {
  _faShowZeroRows = !_faShowZeroRows;
  document.querySelectorAll('.fa-grid .zero-row').forEach(row => {
    row.style.display = _faShowZeroRows ? '' : 'none';
  });
  faUpdateZeroToggle();
}

// ═══════════════════════════════════════════════════════════════════════════
// PAYROLL TAB — Enhanced with Assumptions, Roster Calc, GL Grouping
// ═══════════════════════════════════════════════════════════════════════════

let _payrollAssumptions = {};
// FA directive 2026-05-17: per-cell overrides for tax/benefit totals on the
// Payroll tab. Keyed by cell key (e.g., "welfare", "pension", "fica"). When a
// key has a numeric value, recalcPayroll substitutes it for the computed total
// at render time. Right-click an overridden cell to revert (clear the key).
let _payrollOverrides = {};
// Tracks the last-rendered totals per cell so flash-on-change can detect deltas.
let _payrollPrevTotals = {};
let _payrollPositions = [];
let _payrollGLLines = [];

// ── Payroll Zero-Row Toggle (mirrors faShowZeroRows pattern) ─────────────
let _prShowZeroRows = false;

function prUpdateZeroToggleBtn() {
  const btn = document.getElementById('prZeroToggle');
  if (!btn) return;
  const count = document.querySelectorAll('#prGLContent .prgl-zero-row').length;
  if (count === 0) { btn.style.display = 'none'; return; }
  btn.style.display = '';
  btn.textContent = _prShowZeroRows ? 'Hide ' + count + ' Zero Rows' : 'Show ' + count + ' Hidden Rows';
  btn.style.background = _prShowZeroRows ? 'var(--gray-200)' : 'var(--blue-light, #dbeafe)';
  btn.style.color = _prShowZeroRows ? 'var(--gray-600)' : 'var(--blue)';
  btn.style.borderColor = _prShowZeroRows ? 'var(--gray-300)' : 'var(--blue)';
}

function prToggleZeroRows(ev) {
  if (ev) ev.stopPropagation();
  _prShowZeroRows = !_prShowZeroRows;
  document.querySelectorAll('#prGLContent .prgl-zero-row').forEach(row => {
    row.classList.toggle('prgl-zero-show', _prShowZeroRows);
  });
  prUpdateZeroToggleBtn();
}

// True if a Payroll GL line is "all zero" — matches bpIsZero but without
// the accrual_adj/unpaid_bills terms (those are no longer shown on Payroll).
function prGlIsZero(l) {
  return !l.prior_year && !l.ytd_actual && !l.current_budget && !l.increase_pct
    && (l.estimate_override === null || l.estimate_override === undefined)
    && (l.forecast_override === null || l.forecast_override === undefined)
    && !l.proposed_budget && !l.proposed_formula;
}

// Auto-size editable cells inside the payroll tab to fit their values.
// Sets the HTML `size` attribute (character count) on every input.
function prAutoSizeAll() {
  const sel = '#prGLContent input.cell, #prGLContent input.cell-fx, #prGLContent input.cell-pct, #prRosterTable input';
  document.querySelectorAll(sel).forEach(el => {
    const v = el.value || '';
    el.size = Math.max(2, v.length + 1);
  });
}

// Payroll-only compute helpers — no accrual/unpaid in the math.
// Kept local so renderFASheet (other tabs) keeps using faComputeEstimate/Forecast.
// Hardcoded Payroll-tab forecast rules (manual overrides still win):
//   5105-0035 (Bonus) → forecast pinned to current budget
//   5150-0000 / 5155-0000 / 5160-0000 → forecast zeroed out (Option A: est=0, fcst=0)
function prFaGetForcedOverride(l) {
  const gl = (l.gl_code || '').trim();
  if (gl === '5105-0035') {
    const cb = l.current_budget || 0;
    const ytd = l.ytd_actual || 0;
    return { estimate: cb - ytd, forecast: cb };
  }
  if (gl === '5150-0000' || gl === '5155-0000' || gl === '5160-0000') {
    return { estimate: 0, forecast: 0 };
  }
  return null;
}
function prFaComputeEstimate(l) {
  if (l.estimate_override !== null && l.estimate_override !== undefined) return l.estimate_override;
  const forced = prFaGetForcedOverride(l);
  if (forced) return forced.estimate;
  if (typeof faIsFixedToBudget === 'function' && faIsFixedToBudget(l)) {
    return (l.current_budget || 0) - (l.ytd_actual || 0);
  }
  const ytd = l.ytd_actual || 0;
  if (typeof YTD_MONTHS !== 'undefined' && YTD_MONTHS > 0) {
    return (ytd / YTD_MONTHS) * REMAINING_MONTHS;
  }
  return 0;
}
function prFaComputeForecast(l) {
  if (l.forecast_override !== null && l.forecast_override !== undefined) return l.forecast_override;
  const forced = prFaGetForcedOverride(l);
  if (forced) return forced.forecast;
  if (typeof faIsFixedToBudget === 'function' && faIsFixedToBudget(l)) {
    return l.current_budget || 0;
  }
  return (l.ytd_actual || 0) + prFaComputeEstimate(l);
}

async function renderPayrollTab(sheetLines, contentDiv) {
  _payrollGLLines = sheetLines || [];
  const ec = entityCode;

  // Load assumptions and positions in parallel
  const [aResp, pResp] = await Promise.all([
    fetch('/api/payroll/assumptions/' + ec).then(r => r.json()),
    fetch('/api/payroll/positions/' + ec).then(r => r.json())
  ]);
  _payrollAssumptions = aResp.assumptions || {};
  // 2026-05-17: pick up per-cell overrides if the assumptions row has any.
  _payrollOverrides = (aResp.overrides && typeof aResp.overrides === 'object') ? aResp.overrides : {};
  _payrollPrevTotals = {};   // fresh tab load — no prior totals to flash against
  _payrollPositions = pResp || [];

  // If no positions saved yet, seed with 2 placeholder rows
  if (_payrollPositions.length === 0) {
    _payrollPositions = [
      {position_name: 'Resident Manager', employee_count: 0, hourly_rate: 0, bonus_per_employee: 0, effective_week_override: null, sort_order: 0},
      {position_name: 'Handyman', employee_count: 0, hourly_rate: 0, bonus_per_employee: 0, effective_week_override: null, sort_order: 1}
    ];
  }

  const a = _payrollAssumptions;
  const fmtD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const fmtPct = v => (v * 100).toFixed(2) + '%';
  const fmtPctInput = v => (v * 100).toFixed(3);

  // Scrollable wrapper so sticky formula bar has a scroll context (matches R&S behavior)
  let html = '<div style="max-width:100%; margin:0 auto; max-height:calc(100vh - 220px); overflow-y:auto; padding-right:8px;">';

  // Inject Payroll-specific CSS — FA design language (.fa-grid tokens),
  // scoped to #prGLContent and #prRosterTable so other tabs are unaffected.
  html += '<style>' +
    // ── GL Detail: .fa-grid parity (frozen GL Code + Description, navy total, cream cat-hdr) ──
    '#prGLContent { background:white; border-radius:10px; border:1px solid var(--gray-200); overflow:hidden; }' +
    '#prGLContent .prgl-scroll { overflow-x:auto; max-height:75vh; overflow-y:auto; }' +
    '#prGLContent .prgl-scroll::-webkit-scrollbar { width:10px; height:12px; }' +
    '#prGLContent .prgl-scroll::-webkit-scrollbar-track { background:var(--gray-100); border-radius:6px; }' +
    '#prGLContent .prgl-scroll::-webkit-scrollbar-thumb { background:#8b7355; border-radius:6px; min-height:40px; }' +
    '#prGLContent .prgl-scroll::-webkit-scrollbar-thumb:hover { background:#6b5740; }' +
    '#prGLContent table { border-collapse:separate; border-spacing:0; font-size:13px; width:100%; }' +
    '#prGLContent thead { position:sticky; top:0; z-index:20; }' +
    '#prGLContent th { padding:8px 8px; text-align:left; font-weight:600; border-bottom:2px solid var(--gray-300); white-space:nowrap; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); background:var(--gray-100); }' +
    '#prGLContent th.num { text-align:right; }' +
    '#prGLContent td, #prGLContent th { white-space:nowrap; width:1px; }' +
    '#prGLContent td { padding:6px 8px; border-bottom:1px solid var(--gray-200); }' +
    '#prGLContent td.num { text-align:right; font-variant-numeric:tabular-nums; position:relative; }' +
    // num-box: wrap class that mirrors .cell input outer dimensions exactly.
    // Wrapping plain text in <span class="num-box"> aligns its right edge with
    // input.cell text inside body rows, regardless of browser quirks.
    '#prGLContent .num-box { display:inline-block; padding:4px 6px; border:1px solid transparent; box-sizing:content-box; text-align:right; font-variant-numeric:tabular-nums; font-family:inherit; font-size:inherit; line-height:inherit; min-width:50px; }' +
    '#prGLContent tbody tr:hover td { background:#eef2ff; }' +
    '#prGLContent tbody tr:hover td.frozen { background:#ede5d8; }' +
    '#prGLContent th.frozen, #prGLContent td.frozen { position:sticky; z-index:15; background:white; }' +
    '#prGLContent thead th.frozen { z-index:25; background:var(--gray-100); }' +
    '#prGLContent .frozen-gl { left:0; width:115px; min-width:115px; max-width:115px; }' +
    '#prGLContent .frozen-desc { left:115px; width:240px; min-width:240px; max-width:240px; border-right:2px solid var(--gray-300); box-shadow:2px 0 8px rgba(90,74,63,0.08); }' +
    '#prGLContent .cat-hdr td { background:#f5efe7; font-weight:700; color:#5a4a3f; font-size:14px; padding:10px 12px; border-bottom:2px solid #5a4a3f; cursor:pointer; user-select:none; }' +
    '#prGLContent .sub-row td { background:var(--gray-100); font-weight:700; border-top:2px solid var(--gray-300); }' +
    '#prGLContent .sub-row td.frozen { background:var(--gray-100); }' +
    '#prGLContent .total-row td { background:#1e3a5f; color:white; font-weight:700; font-size:14px; padding:10px 8px; }' +
    '#prGLContent .total-row td.frozen { background:#1e3a5f; color:white; }' +
    // Match R&S/Gen&Admin/.fa-grid cell visual: bordered cream box for editable
    // cells, transparent bg with green left bar for fx (formula) cells. Keeps
    // every numeric cell visually distinct and easy to scan. font:inherit avoids
    // browser-default font on form controls.
    '#prGLContent .cell { min-width:50px; width:100%; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font:inherit; font-size:13px; text-align:right; background:#fbfaf4; cursor:text; font-variant-numeric:tabular-nums; box-sizing:border-box; line-height:inherit; }' +
    '#prGLContent .cell:hover { border-color:#a8a29e; }' +
    '#prGLContent .cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px #e1effe; }' +
    // Formula cells: transparent bg, subtle border, green inset left bar as
    // the "formula" indicator (matches .fa-grid .cell-fx).
    '#prGLContent .cell-fx { background:transparent; border-color:#e5e1d8; box-shadow:inset 3px 0 0 #16a34a; color:#15803d; }' +
    '#prGLContent .cell-fx:hover { border-color:#a8a29e; }' +
    '#prGLContent .cell-fx:focus { background:#ecfdf5; }' +
    '#prGLContent .cell-fx-linked { background:#eff6ff !important; border-color:transparent !important; box-shadow:inset 3px 0 0 #2563eb !important; color:#1e40af !important; font-weight:700; }' +
    '#prGLContent .cell-fx-linked:hover { border-color:#93c5fd !important; }' +
    '#prGLContent .cell-pct { width:auto; min-width:45px; font:inherit; font-size:13px; font-variant-numeric:tabular-nums; }' +
    '#prGLContent .cell-pct[disabled] { background:#fbfaf4; color:#6b7280; cursor:not-allowed; opacity:1; -webkit-text-fill-color:#6b7280; }' +
    '#prGLContent .cell-notes { text-align:left; min-width:120px; width:auto; font-size:12px; background:white; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-family:inherit; }' +
    '#prGLContent .fa-fx { display:none !important; }' +
    '#prGLContent tr.prgl-zero-row { display:none; }' +
    '#prGLContent tr.prgl-zero-row.prgl-zero-show { display:table-row; }' +
    // ── Roster: FA tokens (gray-100 header, cream inputs, gray-200 borders) ──
    '#prRosterTable { width:100%; border-collapse:separate; border-spacing:0; font-size:13px; }' +
    '#prRosterTable thead th { padding:8px 8px; font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); background:var(--gray-100); border-bottom:2px solid var(--gray-300); white-space:nowrap; text-align:left; }' +
    '#prRosterTable thead th.r { text-align:right; }' +
    '#prRosterTable tbody td { padding:6px 8px; border-bottom:1px solid var(--gray-200); font-size:13px; font-variant-numeric:tabular-nums; }' +
    '#prRosterTable tbody tr:hover td { background:#eef2ff; }' +
    '#prRosterTable input { padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; background:#fbfaf4; font-size:13px; font-family:inherit; font-variant-numeric:tabular-nums; box-sizing:content-box; text-align:right; }' +
    '#prRosterTable input:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px #e1effe; }' +
    '#prRosterTable .pr-pos-name { text-align:left; }' +
    '#prRosterTable th.filler, #prRosterTable td.filler { width:100%; padding:0 !important; background:transparent; }' +
    '#prRosterTable tfoot td { padding:8px 8px; border-top:2px solid var(--gray-300); border-bottom:2px solid var(--gray-200); background:var(--gray-100); font-weight:700; font-variant-numeric:tabular-nums; }' +
    '</style>';

  // Formula bar — Excel-style with live preview + Accept/Cancel (same as other tabs)
  // Sticky positioning so it stays visible as user scrolls through GL detail
  html += '<div id="faFormulaBarWrap" style="position:sticky; top:0; z-index:50; display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200); border-radius:8px; margin-bottom:12px; box-shadow:0 2px 4px rgba(0,0,0,0.04);">' +
    '<span style="font-size:11px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>' +
    '<span id="faFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:var(--gray-600); white-space:nowrap; min-width:100px;"></span>' +
    '<input id="faFormulaBar" type="text" placeholder="Click a green formula cell to view its formula..." style="display:block; flex:1; padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:monospace; background:white;" oninput="formulaBarPreview()" onkeydown="formulaBarKeydown(event)">' +
    '<span id="faFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:var(--green); white-space:nowrap; min-width:80px; text-align:right;"></span>' +
    '<button id="faFormulaAccept" style="display:none; padding:4px 14px; font-size:12px; font-weight:600; background:var(--green); color:white; border:none; border-radius:4px; cursor:pointer;" onclick="formulaBarAccept()">Accept</button>' +
    '<button id="faFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:var(--gray-200); color:var(--gray-700); border:none; border-radius:4px; cursor:pointer;" onclick="formulaBarCancel()">Cancel</button>' +
    '<button id="faFormulaClear" style="display:none; padding:4px 10px; font-size:11px; background:#fef2f2; color:var(--red); border:1px solid #fecaca; border-radius:4px; cursor:pointer;" onclick="formulaBarClear()" title="Remove formula, revert to auto-calc">Clear</button>' +
    '<button id="faFormulaUndo" style="display:none; padding:4px 10px; font-size:11px; background:#fff7ed; color:#c2410c; border:1px solid #fed7aa; border-radius:4px; cursor:pointer;" onclick="formulaBarUndo()" title="Undo the last accepted formula change">↶ Undo</button>' +
    // FA dir 2026-05-19: per-tab Undo + History controls. Visible on every
    // sheet tab. Scoped to the active sheet via `sheet=` query param.
    '<span style="display:inline-block; width:1px; height:22px; background:var(--gray-300); margin:0 4px;"></span>' +
    '<button class="fa-tab-undo-btn" onclick="faTabUndoLast()" title="Restore the most recent change on this tab" style="padding:4px 10px; font-size:11px; background:white; color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer; font-weight:600; white-space:nowrap;">↩ Undo last</button>' +
    '<button class="fa-tab-hist-btn" onclick="faTabShowHistory()" title="See the last 50 changes on this tab" style="padding:4px 10px; font-size:11px; background:white; color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer; font-weight:600; white-space:nowrap;">⏱ History</button>' +
    '</div>';

  // ── Section 0: Payroll Assumptions (Editable) ──────────────────────────
  html += `
  <div id="payrollAssumptionsSection" style="background:white; border-radius:10px; border:1px solid var(--gray-200); margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
    <div onclick="togglePayrollSection('prAssump')" style="display:flex; align-items:center; justify-content:space-between; padding:12px 20px; background:#f5efe7; border-bottom:1px solid #e5e0d5; border-radius:10px 10px 0 0; cursor:pointer; user-select:none;">
      <h3 style="font-size:13px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin:0;">Payroll Assumptions <span style="font-size:9px; font-weight:800; color:white; background:#5a4a3f; border-radius:3px; padding:1px 5px; margin-left:6px; vertical-align:middle;">EDITABLE</span></h3>
      <div style="display:flex; align-items:center; gap:12px;">
        <span style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#f5efe7; color:#5a4a3f; border:1px solid #d5cfc5;">Changes flow through all sections below</span>
        <span style="font-size:12px; color:var(--gray-400);" id="prAssumpChev">▾</span>
      </div>
    </div>
    <div id="prAssump">
      <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:0;">

        <!-- Column 1: Wage & Schedule -->
        <div style="padding:14px 20px; border-right:1px solid var(--gray-200);">
          <div style="font-size:10px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:10px; padding-bottom:4px; border-bottom:1px solid #f5efe7;">Wage & Schedule</div>
          ${prAssumpWageIncreaseRow(a)}
          ${prAssumpRow('Effective Week', 'effective_week', a.effective_week || '16', '')}
          ${prAssumpRow('Pre-Incr Weeks', 'pre_increase_weeks', a.pre_increase_weeks || 15, '')}
          ${prAssumpRow('Post-Incr Weeks', 'post_increase_weeks', a.post_increase_weeks || 37, '')}
          ${prAssumpRow('OT Factor %', 'ot_factor', ((a.ot_factor || 0.002) * 100).toFixed(1), '%')}
          ${prAssumpRow('Vac/Sick/Hol %', 'vac_sick_hol_factor', ((a.vac_sick_hol_factor || 0.10) * 100).toFixed(1), '%')}
          <div style="margin-top:8px; font-size:10px; color:var(--gray-400); font-style:italic; padding-top:6px; border-top:1px dashed var(--gray-200);">Changing Effective Week auto-updates Pre/Post weeks — you can also edit them directly</div>
        </div>

        <!-- Column 2: Payroll Tax Rates -->
        <div style="padding:14px 20px; border-right:1px solid var(--gray-200);">
          <div style="font-size:10px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:10px; padding-bottom:4px; border-bottom:1px solid #f5efe7;">Payroll Tax Rates</div>
          ${prAssumpRow('FICA', 'fica', fmtPctInput(a.fica || 0), '%')}
          ${prAssumpRow('SUI', 'sui', fmtPctInput(a.sui || 0), '%')}
          ${prAssumpRow('FUI', 'fui', fmtPctInput(a.fui || 0), '%')}
          ${prAssumpRow('MTA', 'mta', fmtPctInput(a.mta || 0), '%')}
          ${prAssumpRow('NYS Disability', 'nys_disability', fmtPctInput(a.nys_disability || 0), '%')}
          ${prAssumpRow('Paid Family Leave', 'pfl', fmtPctInput(a.pfl || 0), '%')}
          ${prAssumpRow('Workers Comp', 'workers_comp', fmtPctInput(a.workers_comp || 0), '%')}
          <div style="margin-top:8px; font-size:10px; color:var(--gray-400); font-style:italic; padding-top:6px; border-top:1px dashed var(--gray-200);">SUI base: $12,000 · FUI base: $7,000</div>
        </div>

        <!-- Column 3: Union Benefits -->
        <div style="padding:14px 20px;">
          <div style="font-size:10px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:10px; padding-bottom:4px; border-bottom:1px solid #f5efe7;">Union Benefits (32BJ)</div>
          ${prAssumpRow('Welfare ($/mo)', 'welfare_monthly', (a.welfare_monthly || 0).toFixed(2), '$')}
          ${prAssumpRow('Pension ($/wk)', 'pension_weekly', (a.pension_weekly || 0).toFixed(2), '$')}
          ${prAssumpRow('Supp Retirement ($/wk)', 'supp_retirement_weekly', (a.supp_retirement_weekly || 0).toFixed(2), '$')}
          ${prAssumpRow('Legal ($/mo)', 'legal_monthly', (a.legal_monthly || 0).toFixed(2), '$')}
          ${prAssumpRow('Training ($/mo)', 'training_monthly', (a.training_monthly || 0).toFixed(2), '$')}
          ${prAssumpRow('Profit Sharing ($/qtr)', 'profit_sharing_quarterly', (a.profit_sharing_quarterly || 0).toFixed(2), '$')}
          <div style="margin-top:8px; font-size:10px; color:var(--gray-400); font-style:italic; padding-top:6px; border-top:1px dashed var(--gray-200);">Rates × headcount × period multiplier = total</div>
        </div>
      </div>
      <div style="padding:8px 20px; background:#f5efe7; border-top:1px solid #e5e0d5; display:flex; align-items:center; gap:12px; border-radius:0 0 10px 10px;">
        <span id="prAssumpStatus" style="font-size:11px; color:#5a4a3f; font-weight:600;">Seeded from Assumptions tab</span>
      </div>
    </div>
  </div>`;

  // ── Section 1: Employee Roster & Wage Calculation ──────────────────────
  html += `
  <div style="background:white; border-radius:10px; border:1px solid var(--gray-200); margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
    <div onclick="togglePayrollSection('prRoster')" style="display:flex; align-items:center; justify-content:space-between; padding:12px 20px; background:var(--gray-50); border-bottom:1px solid var(--gray-200); border-radius:10px 10px 0 0; cursor:pointer; user-select:none;">
      <h3 style="font-size:13px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin:0;">Employee Roster & Wage Calculation</h3>
      <div style="display:flex; align-items:center; gap:12px;">
        <span id="prRosterBadge" style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#eff6ff; color:#2563eb;">0 employees</span>
        <span id="prRosterTotal" style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#dcfce7; color:#16a34a;">Total: $0</span>
        <span style="font-size:12px; color:var(--gray-400);" id="prRosterChev">▾</span>
      </div>
    </div>
    <div id="prRoster">
      <div id="prRosterInfo" style="padding:10px 20px 6px; display:flex; gap:16px; align-items:center; background:#fafbfc; border-bottom:1px solid var(--gray-200);"></div>
      <div style="overflow-x:auto;">
        <table id="prRosterTable" style="width:100%; border-collapse:collapse; font-size:12px;">
          <thead>
            <tr style="background:var(--gray-50);">
              <th style="text-align:left; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Position</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">#</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Hourly Rate</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Bonus $/Emp</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);" title="Override the global Effective Week for this position only. Leave blank to use global.">Eff Wk Override</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);" title="Per-position wage increase % override. Leave blank to inherit the global rate.">Incr %</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);" title="Per-position wage increase $/hr override. Leave blank to inherit the global rate.">Incr $/hr</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Weekly Pay</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Pre-Incr Wages</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Post-Incr Rate</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Post-Incr Wages</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Annual Base</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); min-width:75px;">OT</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Vac/Sick/Hol</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); font-weight:800;">Total Comp</th>
              <th class="filler"></th>
            </tr>
          </thead>
          <tbody id="prRosterBody"></tbody>
          <tfoot id="prRosterFoot"></tfoot>
        </table>
      </div>
    </div>
  </div>`;

  // ── Payroll Lineage Drill Panel (sticky above the tax/benefits table) ──
  // FA directive 2026-05-17: click any green tax/benefit total → drill panel
  // appears here showing the formula + contributing inputs + override status.
  // Same pattern as the Summary tab's audit Inspector. Right-click an OVR'd
  // cell to revert. Double-click to inline-edit.
  html += '<div id="payrollDrillPanel" style="display:none;margin:0 0 14px;background:white;border:1px solid #c9b89a;border-left:4px solid #5a4a3f;border-radius:8px;padding:14px 18px;font-size:13px;position:sticky;top:100px;z-index:29;box-shadow:0 4px 12px rgba(0,0,0,0.08);max-height:60vh;overflow-y:auto;"></div>';

  // ── Section 2: Payroll Taxes, Workers Comp & Union Benefits ────────────
  html += `
  <div style="background:white; border-radius:10px; border:1px solid var(--gray-200); margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
    <div onclick="togglePayrollSection('prTaxes')" style="display:flex; align-items:center; justify-content:space-between; padding:12px 20px; background:var(--gray-50); border-bottom:1px solid var(--gray-200); border-radius:10px 10px 0 0; cursor:pointer; user-select:none;">
      <h3 style="font-size:13px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin:0;">Payroll Taxes, Workers Comp & Union Benefits</h3>
      <div style="display:flex; align-items:center; gap:12px;">
        <span style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#fff7ed; color:#ea580c;">Auto-calculated from Assumptions + Roster</span>
        <span id="prTaxTotal" style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#dcfce7; color:#16a34a;">Total: $0</span>
        <span style="font-size:12px; color:var(--gray-400);" id="prTaxesChev">▾</span>
      </div>
    </div>
    <div id="prTaxes">
      <table style="width:100%; border-collapse:collapse; font-size:12px;">
        <thead>
          <tr style="background:var(--gray-50);">
            <th style="text-align:left; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); width:200px;">Category</th>
            <th style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); width:80px;">Rate</th>
            <th style="text-align:left; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); width:220px;">Basis</th>
            <th style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); width:120px;">Calculated Total</th>
          </tr>
        </thead>
        <tbody id="prTaxBody"></tbody>
        <tfoot id="prTaxFoot"></tfoot>
      </table>
    </div>
  </div>`;

  // ── Section 3: GL Detail with expandable sub-categories ────────────────
  html += `
  <div style="background:white; border-radius:10px; border:1px solid var(--gray-200); margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
    <div onclick="togglePayrollSection('prGL')" style="display:flex; align-items:center; justify-content:space-between; padding:12px 20px; background:var(--gray-50); border-bottom:1px solid var(--gray-200); border-radius:10px 10px 0 0; cursor:pointer; user-select:none;">
      <h3 style="font-size:13px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin:0;">GL Detail — Yardi Actuals & Budget</h3>
      <div style="display:flex; align-items:center; gap:12px;">
        <span style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#f5efe7; color:#5a4a3f;">${_payrollGLLines.length} GL lines in 4 groups</span>
        <button id="prZeroToggle" onclick="prToggleZeroRows(event)" style="display:none; font-size:11px; padding:4px 12px; background:var(--blue-light, #dbeafe); color:var(--blue); border:1px solid var(--blue); border-radius:4px; cursor:pointer;"></button>
        <span style="font-size:12px; color:var(--gray-400);" id="prGLChev">▾</span>
      </div>
    </div>
    <div id="prGL">
      <div id="prGLContent"></div>
      <div id="prTieOut"></div>
    </div>
  </div>`;

  html += '</div>';
  contentDiv.innerHTML = html;

  // Now populate dynamic sections
  recalcPayroll();
  renderPayrollGL();
}

// ── Assumption row helpers ────────────────────────────────────────────────

function prAssumpRow(label, key, val, suffix) {
  return '<div style="display:flex; justify-content:space-between; align-items:center; padding:4px 0; font-size:12px;">' +
    '<span style="color:var(--gray-600);">' + label + '</span>' +
    '<div style="display:flex; align-items:center; gap:2px;">' +
    '<input class="pr-assump-input" data-key="' + key + '" value="' + val + '" onchange="payrollAssumptionChanged(this)" style="width:90px; padding:3px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:12px; text-align:right; background:#fbfaf4; font-variant-numeric:tabular-nums; font-family:inherit;">' +
    '<span style="font-size:11px; color:var(--gray-400); width:12px; display:inline-block;">' + (suffix || '') + '</span>' +
    '</div></div>';
}

// Dual-cell wage increase row (global): % and $/hr linked. Edit either,
// the other auto-updates using the roster's weighted-avg hourly rate as basis.
// Mode flag tracks which cell is the driver so downstream math stays stable.
function prAssumpWageIncreaseRow(a) {
  const mode = a.wage_increase_mode || 'pct';
  const val  = (a.wage_increase_value != null) ? a.wage_increase_value : (a.wage_increase_pct || 0);
  const avgRate = prBlendedHourlyRate();
  // Compute derived value for the non-driver cell
  let pctDisplay, dollarDisplay;
  if (mode === 'dollar') {
    dollarDisplay = (val || 0).toFixed(2);
    pctDisplay = avgRate > 0 ? ((val / avgRate) * 100).toFixed(2) : '0.00';
  } else {
    pctDisplay = ((val || 0) * 100).toFixed(2);
    dollarDisplay = (avgRate * (val || 0)).toFixed(2);
  }
  const activeBorder = '#16a34a';
  const inactiveBorder = 'var(--gray-300)';
  const pctBorder = (mode === 'pct') ? activeBorder : inactiveBorder;
  const dollarBorder = (mode === 'dollar') ? activeBorder : inactiveBorder;
  return '<div style="display:flex; justify-content:space-between; align-items:center; padding:4px 0; font-size:12px;" title="Enter either a % or a $/hr amount. The other cell updates automatically using the weighted-avg hourly rate. The cell you edit becomes the driver for downstream math.">' +
    '<span style="color:var(--gray-600);">Wage Increase</span>' +
    '<div style="display:flex; align-items:center; gap:4px;">' +
    '<input class="pr-assump-wage-pct" data-mode="pct" value="' + pctDisplay + '" onchange="wageIncreaseChanged(this,\'pct\')" style="width:54px; padding:3px 6px; border:1.5px solid ' + pctBorder + '; border-radius:4px; font-size:12px; text-align:right; background:#fbfaf4; font-variant-numeric:tabular-nums; font-family:inherit;">' +
    '<span style="font-size:11px; color:var(--gray-400); width:8px;">%</span>' +
    '<span style="font-size:10px; color:var(--gray-400);">or</span>' +
    '<span style="font-size:11px; color:var(--gray-400); width:6px;">$</span>' +
    '<input class="pr-assump-wage-dollar" data-mode="dollar" value="' + dollarDisplay + '" onchange="wageIncreaseChanged(this,\'dollar\')" style="width:54px; padding:3px 6px; border:1.5px solid ' + dollarBorder + '; border-radius:4px; font-size:12px; text-align:right; background:#fbfaf4; font-variant-numeric:tabular-nums; font-family:inherit;">' +
    '<span style="font-size:10px; color:var(--gray-400);">/hr</span>' +
    '</div></div>';
}

// Weighted-average hourly rate across roster, used as basis for global %↔$ conversion.
// Weighted by employee_count. Returns 0 if roster is empty.
function prBlendedHourlyRate() {
  if (!Array.isArray(_payrollPositions) || _payrollPositions.length === 0) return 0;
  let totalCount = 0, totalPay = 0;
  for (const p of _payrollPositions) {
    const c = p.employee_count || 0;
    const r = p.hourly_rate || 0;
    totalCount += c;
    totalPay += c * r;
  }
  return totalCount > 0 ? (totalPay / totalCount) : 0;
}

// Handler for the global dual-cell wage increase. Updates mode + value in state,
// refreshes the other cell in place, triggers recalc, and debounces save.
function wageIncreaseChanged(el, editedMode) {
  // FA directive 2026-05-10: skip when value didn't change.
  if (_isUnchangedInput(el)) return;
  const raw = parseFloat((el.value || '').replace(/[^0-9.\-]/g, '')) || 0;
  _payrollAssumptions.wage_increase_mode = editedMode;
  _payrollAssumptions.wage_increase_value = (editedMode === 'pct') ? (raw / 100) : raw;
  // Keep legacy field in sync when mode=pct so old consumers keep working.
  if (editedMode === 'pct') {
    _payrollAssumptions.wage_increase_pct = raw / 100;
  }
  // Update the other cell's display in place
  const pctInput = document.querySelector('.pr-assump-wage-pct');
  const dollarInput = document.querySelector('.pr-assump-wage-dollar');
  const avgRate = prBlendedHourlyRate();
  if (editedMode === 'pct' && dollarInput) {
    dollarInput.value = (avgRate * (raw / 100)).toFixed(2);
  } else if (editedMode === 'dollar' && pctInput) {
    pctInput.value = avgRate > 0 ? ((raw / avgRate) * 100).toFixed(2) : '0.00';
  }
  // Highlight the driver border
  if (pctInput) pctInput.style.borderColor = (editedMode === 'pct') ? '#16a34a' : 'var(--gray-300)';
  if (dollarInput) dollarInput.style.borderColor = (editedMode === 'dollar') ? '#16a34a' : 'var(--gray-300)';
  recalcPayroll();
  clearTimeout(_prAssumpSaveTimer);
  _prAssumpSaveTimer = setTimeout(savePayrollAssumptions, 800);
}

function prAssumpRowCalc(label, val) {
  return '<div style="display:flex; justify-content:space-between; align-items:center; padding:4px 0; font-size:12px;">' +
    '<span style="color:var(--gray-600);">' + label + '</span>' +
    '<span style="font-size:12px; font-weight:600; color:#16a34a; background:#f0fdf4; border:1px solid #bbf7d0; border-radius:4px; padding:3px 8px; width:90px; text-align:right; display:inline-block;">' + val + '</span>' +
    '</div>';
}

function togglePayrollSection(id) {
  const el = document.getElementById(id);
  const chev = document.getElementById(id + 'Chev');
  if (!el) return;
  if (el.style.display === 'none') {
    el.style.display = '';
    if (chev) chev.textContent = '▾';
  } else {
    el.style.display = 'none';
    if (chev) chev.textContent = '▸';
  }
}

// ── Assumption change handler ─────────────────────────────────────────────

let _prAssumpSaveTimer = null;
function payrollAssumptionChanged(el) {
  // FA directive 2026-05-10: skip when value didn't change.
  if (_isUnchangedInput(el)) return;
  const key = el.dataset.key;
  let val = el.value.trim();

  // Parse value depending on type
  if (key === 'effective_week') {
    _payrollAssumptions[key] = val;
    // Auto-calc pre/post weeks
    const wk = parseInt(val) || 16;
    _payrollAssumptions.pre_increase_weeks = Math.max(wk - 1, 0);
    _payrollAssumptions.post_increase_weeks = 52 - _payrollAssumptions.pre_increase_weeks;
  } else if (['fica','sui','fui','mta','nys_disability','pfl','workers_comp','ot_factor','vac_sick_hol_factor'].includes(key)) {
    _payrollAssumptions[key] = parseFloat(val) / 100 || 0;
  } else if (key === 'pre_increase_weeks' || key === 'post_increase_weeks') {
    _payrollAssumptions[key] = parseInt(val) || 0;
  } else {
    _payrollAssumptions[key] = parseFloat(val) || 0;
  }

  recalcPayroll();

  // Debounced auto-save
  clearTimeout(_prAssumpSaveTimer);
  _prAssumpSaveTimer = setTimeout(savePayrollAssumptions, 800);
}

async function savePayrollAssumptions() {
  const ec = entityCode;
  try {
    await fetch('/api/payroll/assumptions/' + ec, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({assumptions: _payrollAssumptions})
    });
    const st = document.getElementById('prAssumpStatus');
    if (st) st.textContent = 'Saved ✓ — ' + new Date().toLocaleTimeString();
  } catch(e) { console.error('Failed to save payroll assumptions:', e); }
}

// ── Roster change & save ──────────────────────────────────────────────────

function prRosterChanged(el) {
  // FA directive 2026-05-10: skip when value didn't change. el is the
  // input element from the onchange handler; backwards-compatible if
  // called with no arg (re-renders + saves unconditionally).
  if (el && _isUnchangedInput(el)) return;
  // Snapshot the prior _payrollPositions so we can preserve per-row overrides
  // (wage_increase_mode / wage_increase_value) which live in state but not in
  // easily-readable form on the main roster inputs.
  const prior = Array.isArray(_payrollPositions) ? _payrollPositions.slice() : [];
  // Read all rows from DOM
  const rows = document.querySelectorAll('#prRosterBody tr');
  _payrollPositions = [];
  rows.forEach((tr, i) => {
    const nameInput = tr.querySelector('.pr-pos-name');
    const countInput = tr.querySelector('.pr-pos-count');
    const rateInput = tr.querySelector('.pr-pos-rate');
    const bonusInput = tr.querySelector('.pr-pos-bonus');
    const effWkInput = tr.querySelector('.pr-pos-effwk');
    if (!nameInput) return;
    const effWkRaw = effWkInput ? effWkInput.value.trim() : '';
    const priorRow = prior[i] || {};
    _payrollPositions.push({
      position_name: nameInput.value.trim(),
      employee_count: parseInt(countInput.value) || 0,
      hourly_rate: parseFloat(rateInput.value.replace(/[^0-9.]/g, '')) || 0,
      bonus_per_employee: bonusInput ? (parseFloat(bonusInput.value.replace(/[^0-9.]/g, '')) || 0) : 0,
      effective_week_override: effWkRaw === '' ? null : (parseFloat(effWkRaw) || null),
      wage_increase_mode: priorRow.wage_increase_mode || null,
      wage_increase_value: (priorRow.wage_increase_value != null) ? priorRow.wage_increase_value : null,
      extra_bonuses: Array.isArray(priorRow.extra_bonuses) ? priorRow.extra_bonuses : [],
      sort_order: i
    });
  });
  recalcPayroll();

  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 800);
}

// Handler for per-position wage-increase override cells.
// Editing either cell sets the mode and value; clearing both reverts to global.
function prRosterWageIncrChanged(el, idx, mode) {
  if (!_payrollPositions[idx]) return;
  // FA directive 2026-05-10: skip when value didn't change.
  if (_isUnchangedInput(el)) return;
  const raw = (el.value || '').trim();
  if (raw === '') {
    // Check the sibling cell — if it's also empty, clear the override entirely.
    const tr = el.closest('tr');
    const sibling = tr ? tr.querySelector(mode === 'pct' ? '.pr-pos-wage-incr-dollar' : '.pr-pos-wage-incr-pct') : null;
    const siblingVal = sibling ? (sibling.value || '').trim() : '';
    if (siblingVal === '') {
      _payrollPositions[idx].wage_increase_mode = null;
      _payrollPositions[idx].wage_increase_value = null;
    }
    // If sibling still has a value, leave current override state alone.
  } else {
    const num = parseFloat(raw.replace(/[^0-9.\-]/g, '')) || 0;
    _payrollPositions[idx].wage_increase_mode = mode;
    _payrollPositions[idx].wage_increase_value = (mode === 'pct') ? (num / 100) : num;
  }
  recalcPayroll();
  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 800);
}

let _prRosterSaveTimer = null;
async function savePayrollPositions() {
  const ec = entityCode;
  try {
    await fetch('/api/payroll/positions/' + ec, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({positions: _payrollPositions})
    });
  } catch(e) { console.error('Failed to save payroll positions:', e); }
}

function addPayrollPosition() {
  _payrollPositions.push({position_name: '', employee_count: 0, hourly_rate: 0, bonus_per_employee: 0, effective_week_override: null, wage_increase_mode: null, wage_increase_value: null, extra_bonuses: [], benefit_adjustments: null, sort_order: _payrollPositions.length});
  renderPayrollRoster();
  recalcPayroll();
}

function removePayrollPosition(idx) {
  _payrollPositions.splice(idx, 1);
  renderPayrollRoster();
  recalcPayroll();
  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 400);
}

// ── Core Recalculation ────────────────────────────────────────────────────

// Apply a wage increase to an hourly rate based on mode + value.
// mode='pct' → rate * (1 + value);  mode='dollar' → rate + value
function applyWageIncrease(rate, mode, value) {
  const m = (mode === 'dollar') ? 'dollar' : 'pct';
  const v = value || 0;
  return (m === 'dollar') ? (rate + v) : (rate * (1 + v));
}

function recalcPayroll() {
  const a = _payrollAssumptions;
  // Resolve global wage increase: prefer new fields, fall back to legacy wage_increase_pct
  const globalMode  = a.wage_increase_mode || 'pct';
  const globalValue = (a.wage_increase_value != null) ? a.wage_increase_value : (a.wage_increase_pct || 0);
  const wageInc = globalValue; // legacy name kept for formula display when mode=pct
  const preWks = a.pre_increase_weeks || 15;
  const postWks = a.post_increase_weeks || 37;
  const otFactor = a.ot_factor || 0.002;
  const vshFactor = a.vac_sick_hol_factor || 0.10;

  let totalEmployees = 0;
  let totalAnnualBase = 0;
  let totalOT = 0;
  let totalVSH = 0;
  let totalComp = 0;
  let totalBonus = 0;

  // Calculate per-position wages
  const posCalcs = _payrollPositions.map(p => {
    const count = p.employee_count || 0;
    const rate = p.hourly_rate || 0;
    const bonusPerEmp = p.bonus_per_employee || 0;
    // Per-position effective week override (e.g. one Resident Manager getting a late raise)
    let posPreWks = preWks, posPostWks = postWks;
    if (p.effective_week_override && p.effective_week_override > 0) {
      posPreWks = Math.max(p.effective_week_override - 1, 0);
      posPostWks = 52 - posPreWks;
    }
    // Per-position wage increase override: falls back to global when null
    const posWiMode  = p.wage_increase_mode || globalMode;
    const posWiValue = (p.wage_increase_value != null) ? p.wage_increase_value : globalValue;
    const weeklyPay = rate * 40;
    const preIncrWages = weeklyPay * posPreWks * count;
    const postIncrRate = applyWageIncrease(rate, posWiMode, posWiValue);
    const postIncrWages = (postIncrRate * 40) * posPostWks * count;
    const annualBase = preIncrWages + postIncrWages;
    const ot = annualBase * otFactor;
    const vsh = annualBase * vshFactor;
    // Base bonus cell + optional stacked extra bonus lines (per_emp / lump / pct_wages)
    let bonus = bonusPerEmp * count;
    const extras = Array.isArray(p.extra_bonuses) ? p.extra_bonuses : [];
    for (const e of extras) {
      const amt = e.amount || 0;
      if (e.basis === 'per_emp')        bonus += amt * count;
      else if (e.basis === 'lump')      bonus += amt;
      else if (e.basis === 'pct_wages') bonus += amt * annualBase;
    }
    const comp = annualBase + ot + vsh;

    totalEmployees += count;
    totalAnnualBase += annualBase;
    totalOT += ot;
    totalVSH += vsh;
    totalComp += comp;
    totalBonus += bonus;

    return { count, rate, bonusPerEmp, posPreWks, posPostWks, posWiMode, posWiValue, weeklyPay, preIncrWages, postIncrRate, postIncrWages, annualBase, ot, vsh, bonus, comp };
  });

  // Calculate taxes & benefits
  const grossWages = totalAnnualBase + totalOT + totalVSH;
  const ficaAmt = grossWages * (a.fica || 0);
  const suiAmt = 12000 * (a.sui || 0) * totalEmployees;
  const fuiAmt = 7000 * (a.fui || 0) * totalEmployees;
  const mtaAmt = grossWages * (a.mta || 0);
  const nysDisAmt = (a.nys_disability || 0) * totalEmployees;
  const pflAmt = grossWages * (a.pfl || 0);
  const totalPayrollTax = ficaAmt + suiAmt + fuiAmt + mtaAmt + nysDisAmt + pflAmt;
  const wcAmt = (a.workers_comp || 0) * grossWages;

  // FA directive 2026-05-05: per-position benefit adjustments stack on top
  // of the building defaults. Each position can flag "N of M employees have
  // an extra rate × periods on welfare/pension/etc." Math is additive.
  // Example: 6 doormen, default pension = $82.50 × 52 = $4,290 each = $25,740.
  // 1 of 6 has +$82.50 × 30 wks. Adjustment total = $82.50 × 30 × 1 = $2,475.
  // Position pension total = $25,740 + $2,475 = $28,215.
  let adjWelfare = 0, adjPension = 0, adjSuppRet = 0,
      adjLegal = 0,   adjTraining = 0, adjProfitShare = 0;
  let adjPositionsCount = 0;
  for (const p of _payrollPositions) {
    const adj = p.benefit_adjustments;
    if (!adj || !adj.benefits) continue;
    // Clamp adjusted_count to position's employee_count.
    const cnt = Math.min(
      Math.max(parseInt(adj.adjusted_count || 0, 10) || 0, 0),
      parseInt(p.employee_count || 0, 10) || 0
    );
    if (cnt <= 0) continue;
    adjPositionsCount++;
    const b = adj.benefits || {};
    const addBlock = (block) => {
      if (!block) return 0;
      const r = parseFloat(block.rate) || 0;
      const pp = parseFloat(block.periods) || 0;
      return r * pp * cnt;
    };
    adjWelfare     += addBlock(b.welfare);
    adjPension     += addBlock(b.pension);
    adjSuppRet     += addBlock(b.supp_retirement);
    adjLegal       += addBlock(b.legal);
    adjTraining    += addBlock(b.training);
    adjProfitShare += addBlock(b.profit_sharing);
  }

  const welfareAmt = (a.welfare_monthly || 0) * totalEmployees * 12 + adjWelfare;
  const pensionAmt = (a.pension_weekly || 0) * totalEmployees * 52 + adjPension;
  const suppRetAmt = (a.supp_retirement_weekly || 0) * totalEmployees * 52 + adjSuppRet;
  const legalAmt = (a.legal_monthly || 0) * totalEmployees * 12 + adjLegal;
  const trainingAmt = (a.training_monthly || 0) * totalEmployees * 12 + adjTraining;
  const profitShareAmt = (a.profit_sharing_quarterly || 0) * totalEmployees * 4 + adjProfitShare;
  const totalUnion = welfareAmt + pensionAmt + suppRetAmt + legalAmt + trainingAmt + profitShareAmt;
  // Stash adjustment totals so the UI can display them in the badge tooltip
  // and (later) in a "what changed vs default" panel.
  window._payrollAdjustments = {
    welfare: adjWelfare, pension: adjPension, supp_retirement: adjSuppRet,
    legal: adjLegal, training: adjTraining, profit_sharing: adjProfitShare,
    positions_with_adjustments: adjPositionsCount,
  };

  // ── FA-set overrides for tax/benefit totals (2026-05-17) ──────────────
  // _payrollOverrides is a {cell_key: value} map. When a key is present and
  // numeric, substitute it for the computed total. Keep the COMPUTED values
  // in _payrollComputed so the lineage panel + revert flow can show the
  // delta. Final variables (`ficaAmt` etc.) below this block are the
  // FA-authoritative numbers used for rendering + downstream sums.
  const _computed = {
    fica: ficaAmt, sui: suiAmt, fui: fuiAmt, mta: mtaAmt,
    nys_disability: nysDisAmt, pfl: pflAmt,
    workers_comp: wcAmt,
    welfare: welfareAmt, pension: pensionAmt, supp_retirement: suppRetAmt,
    legal: legalAmt, training: trainingAmt, profit_sharing: profitShareAmt,
    // FA dir 2026-06-04: section totals (pre-override) so the lineage panel and
    // revert-to-computed flow show the correct computed value when an FA has
    // typed a hard override onto a total cell.
    total_payroll_tax: ficaAmt + suiAmt + fuiAmt + mtaAmt + nysDisAmt + pflAmt,
    total_union: welfareAmt + pensionAmt + suppRetAmt + legalAmt + trainingAmt + profitShareAmt,
    total_labor: grossWages + (ficaAmt + suiAmt + fuiAmt + mtaAmt + nysDisAmt + pflAmt) + wcAmt
                 + (welfareAmt + pensionAmt + suppRetAmt + legalAmt + trainingAmt + profitShareAmt),
  };
  const _applyOv = (key, fallback) => {
    const ov = _payrollOverrides[key];
    return (ov !== null && ov !== undefined && isFinite(parseFloat(ov)))
      ? parseFloat(ov) : fallback;
  };
  const ovFica = _applyOv('fica', ficaAmt);
  const ovSui = _applyOv('sui', suiAmt);
  const ovFui = _applyOv('fui', fuiAmt);
  const ovMta = _applyOv('mta', mtaAmt);
  const ovNysDis = _applyOv('nys_disability', nysDisAmt);
  const ovPfl = _applyOv('pfl', pflAmt);
  const ovWc = _applyOv('workers_comp', wcAmt);
  const ovWelfare = _applyOv('welfare', welfareAmt);
  const ovPension = _applyOv('pension', pensionAmt);
  const ovSuppRet = _applyOv('supp_retirement', suppRetAmt);
  const ovLegal = _applyOv('legal', legalAmt);
  const ovTraining = _applyOv('training', trainingAmt);
  const ovProfitShare = _applyOv('profit_sharing', profitShareAmt);
  // Recompute subtotals using overrides so they cascade through.
  // FA dir 2026-06-04: section totals are themselves overridable (type a hard
  // number on the total cell). Default = sum of the (possibly-overridden) line
  // items; an explicit total override wins and cascades into Total Labor.
  const ovTotalPayrollTax = _applyOv('total_payroll_tax', ovFica + ovSui + ovFui + ovMta + ovNysDis + ovPfl);
  const ovTotalUnion = _applyOv('total_union', ovWelfare + ovPension + ovSuppRet + ovLegal + ovTraining + ovProfitShare);

  window._payrollComputed = _computed;   // pre-override values for lineage panel

  const totalLaborCalc = _applyOv('total_labor', grossWages + ovTotalPayrollTax + ovWc + ovTotalUnion);

  // Store for tie-out
  window._payrollCalcTotal = totalLaborCalc;

  // Publish component breakdown for GL linkage. Use FA-overridden values
  // so downstream GL lines reflect manual corrections (2026-05-17).
  window._payrollComponents = {
    annual_base: totalAnnualBase,
    ot: totalOT,
    vsh_vacation: totalVSH / 3,
    vsh_holiday: totalVSH / 3,
    vsh_sick: totalVSH / 3,
    bonus: totalBonus,
    employer_taxes: ovFica + ovSui + ovFui + ovMta,
    workers_comp: ovWc,
    nys_disability: ovNysDis,
    pfl: ovPfl,
    welfare: ovWelfare,
    pension: ovPension,
    supp_retirement: ovSuppRet,
    legal_fund: ovLegal,
    training_fund: ovTraining,
    profit_sharing: ovProfitShare
  };

  // FA dir 2026-06-04: expose per-position calcs so the roster TOTAL row's
  // click-to-inspect cells can show a per-position breakdown of each column.
  window._payrollPosCalcs = posCalcs;

  // Render roster (pass assumption values for formula strings)
  renderPayrollRoster(posCalcs, totalEmployees, totalAnnualBase, totalOT, totalVSH, totalComp,
    {preWks, postWks, wageInc, otFactor, vshFactor});

  // Render taxes — pass OVERRIDDEN values for display + lineage panel.
  // The renderer also reads window._payrollComputed + window._payrollOverrides
  // to decide OVR badges and lineage formulas (2026-05-17).
  renderPayrollTaxes({
    ficaAmt: ovFica, suiAmt: ovSui, fuiAmt: ovFui, mtaAmt: ovMta,
    nysDisAmt: ovNysDis, pflAmt: ovPfl, totalPayrollTax: ovTotalPayrollTax,
    wcAmt: ovWc,
    welfareAmt: ovWelfare, pensionAmt: ovPension, suppRetAmt: ovSuppRet,
    legalAmt: ovLegal, trainingAmt: ovTraining, profitShareAmt: ovProfitShare,
    totalUnion: ovTotalUnion, totalLaborCalc, grossWages, totalEmployees,
  });

  // Flash-on-change (2026-05-17): compare new totals to last render and pulse
  // any cell whose value changed. Skipped on first render (no prior baseline).
  if (window._payrollPrevTotals && Object.keys(_payrollPrevTotals).length > 0) {
    const newTotals = {
      fica: ovFica, sui: ovSui, fui: ovFui, mta: ovMta,
      nys_disability: ovNysDis, pfl: ovPfl, workers_comp: ovWc,
      welfare: ovWelfare, pension: ovPension, supp_retirement: ovSuppRet,
      legal: ovLegal, training: ovTraining, profit_sharing: ovProfitShare,
      total_payroll_tax: ovTotalPayrollTax, total_union: ovTotalUnion,
      total_labor: totalLaborCalc,
    };
    Object.keys(newTotals).forEach(function(k) {
      const prev = _payrollPrevTotals[k];
      const next = newTotals[k];
      if (prev !== undefined && Math.abs(prev - next) > 0.5) {
        // Use setTimeout so the DOM has rendered the new value before we flash
        setTimeout(function() { _payrollFlashCell(k); }, 30);
      }
    });
    _payrollPrevTotals = newTotals;
  } else {
    _payrollPrevTotals = {
      fica: ovFica, sui: ovSui, fui: ovFui, mta: ovMta,
      nys_disability: ovNysDis, pfl: ovPfl, workers_comp: ovWc,
      welfare: ovWelfare, pension: ovPension, supp_retirement: ovSuppRet,
      legal: ovLegal, training: ovTraining, profit_sharing: ovProfitShare,
      total_payroll_tax: ovTotalPayrollTax, total_union: ovTotalUnion,
      total_labor: totalLaborCalc,
    };
  }

  // Push roster-derived component values to linked GL lines
  pushRosterToGL();

  // Update tie-out
  renderPayrollTieOut(totalLaborCalc);

  // Update info bar
  const infoDiv = document.getElementById('prRosterInfo');
  if (infoDiv) {
    infoDiv.innerHTML =
      '<div style="font-size:11px;"><span style="color:var(--gray-500);">Wage Increase:</span> <strong style="color:#5a4a3f;">' + ((globalMode === 'dollar') ? ('+$' + (globalValue || 0).toFixed(2) + '/hr') : (((globalValue || 0) * 100).toFixed(1) + '%')) + '</strong> <span style="font-size:8px; font-weight:800; color:#5a4a3f; background:#f5efe7; border:1px solid #5a4a3f; border-radius:3px; padding:0 3px; vertical-align:super;">from assumptions</span></div>' +
      '<div style="font-size:11px;"><span style="color:var(--gray-500);">Effective:</span> <strong>Wk ' + (a.effective_week || '16') + '</strong></div>' +
      '<div style="font-size:11px;"><span style="color:var(--gray-500);">Pre-Incr Weeks:</span> <strong>' + preWks + '</strong></div>' +
      '<div style="font-size:11px;"><span style="color:var(--gray-500);">Post-Incr Weeks:</span> <strong>' + postWks + '</strong></div>';
  }
}

// ── Bonus Cell Shell + Popover ────────────────────────────────────────────
// The common-case bonus stays as a plain number input. Additional bonus lines
// (label + amount + basis) live behind a "+" affordance that opens a popover.

function prBonusCellHTML(p, idx, c) {
  const extras = Array.isArray(p.extra_bonuses) ? p.extra_bonuses : [];
  const hasExtras = extras.length > 0;
  const base = p.bonus_per_employee || 0;
  // Build hover tooltip showing the breakdown
  const count = p.employee_count || 0;
  const annualBase = (c && c.annualBase) || 0;
  const tipLines = [];
  if (base) tipLines.push('Base: $' + base + '/emp × ' + count + ' = $' + Math.round(base * count).toLocaleString());
  extras.forEach(e => {
    const amt = e.amount || 0;
    let v = 0, desc = '';
    if (e.basis === 'per_emp')        { v = amt * count; desc = '$' + amt + '/emp × ' + count; }
    else if (e.basis === 'lump')      { v = amt;         desc = 'lump sum'; }
    else if (e.basis === 'pct_wages') { v = amt * annualBase; desc = (amt * 100).toFixed(2) + '% of wages'; }
    tipLines.push((e.label || '(unnamed)') + ': ' + desc + ' = $' + Math.round(v).toLocaleString());
  });
  const tooltip = (tipLines.join('\n') || 'Single bonus line').replace(/"/g, '&quot;');
  const cellBg = hasExtras ? '#fefce8' : '#fbfaf4';
  const cellBorder = hasExtras ? '#fde68a' : '#d1d5db';
  const badge = hasExtras
    ? '<span style="font-size:9px; font-weight:700; color:#2563eb; background:#eff6ff; border:1px solid #bfdbfe; border-radius:3px; padding:0 3px; margin-right:2px;">+' + extras.length + '</span>'
    : '';
  return '<span class="pr-bonus-cell" title="' + tooltip + '" style="display:inline-flex; align-items:center; gap:3px; padding:1px 2px 1px 6px; border:1px solid ' + cellBorder + '; border-radius:4px; background:' + cellBg + ';">' +
    '<input class="pr-pos-bonus" type="text" value="' + base + '" onchange="prRosterChanged(this)" ' +
    'style="border:none; outline:none; background:transparent; width:62px; padding:2px 0; font-size:12px; text-align:right; font-variant-numeric:tabular-nums; font-family:inherit;">' +
    badge +
    '<button type="button" onclick="openBonusPopover(event,' + idx + ')" title="Add additional bonus lines" ' +
    'style="width:18px; height:18px; border:none; background:transparent; color:#9ca3af; cursor:pointer; font-size:14px; line-height:1; border-radius:3px; padding:0;">+</button>' +
    '</span>';
}

let _prBonusPopoverIdx = null;

function closeBonusPopover() {
  const pop = document.getElementById('prBonusPopover');
  if (pop) pop.remove();
  document.removeEventListener('mousedown', _prBonusOutsideClick, true);
  _prBonusPopoverIdx = null;
}

function _prBonusOutsideClick(e) {
  const pop = document.getElementById('prBonusPopover');
  if (!pop) return;
  if (pop.contains(e.target)) return;
  // Ignore clicks on any "+" button or within a bonus cell (re-open will rebuild)
  if (e.target.closest && e.target.closest('.pr-bonus-cell')) return;
  closeBonusPopover();
}

function openBonusPopover(evt, idx) {
  if (evt && evt.stopPropagation) evt.stopPropagation();
  closeBonusPopover();
  const p = _payrollPositions[idx];
  if (!p) return;
  if (!Array.isArray(p.extra_bonuses)) p.extra_bonuses = [];
  const pop = document.createElement('div');
  pop.id = 'prBonusPopover';
  pop.style.cssText = 'position:absolute; z-index:1000; background:white; border:1px solid #d1d5db; border-radius:8px; box-shadow:0 8px 24px rgba(0,0,0,0.12); padding:12px; min-width:380px; font-size:12px;';
  pop.innerHTML = buildBonusPopoverHTML(idx);
  document.body.appendChild(pop);
  // Position near the click target, anchored to the button
  const anchor = (evt && evt.target) ? evt.target.getBoundingClientRect() : { left: 100, bottom: 100 };
  const popWidth = 380;
  let leftPx = anchor.left + window.scrollX - popWidth + 20;
  if (leftPx < 10) leftPx = 10;
  pop.style.left = leftPx + 'px';
  pop.style.top  = (anchor.bottom + window.scrollY + 6) + 'px';
  _prBonusPopoverIdx = idx;
  setTimeout(() => document.addEventListener('mousedown', _prBonusOutsideClick, true), 0);
}

function buildBonusPopoverHTML(idx) {
  const p = _payrollPositions[idx];
  if (!p) return '';
  const extras = p.extra_bonuses || [];
  const count = p.employee_count || 0;
  // Compute a live preview total (base + extras). Uses the cached annualBase for pct_wages.
  let annualBase = 0;
  try {
    // Derive annualBase on the fly (mirrors recalcPayroll math, without the full side effects)
    const a = _payrollAssumptions || {};
    const preWks = a.pre_increase_weeks || 15;
    const postWks = a.post_increase_weeks || 37;
    const gm = a.wage_increase_mode || 'pct';
    const gv = (a.wage_increase_value != null) ? a.wage_increase_value : (a.wage_increase_pct || 0);
    const pm = p.wage_increase_mode || gm;
    const pv = (p.wage_increase_value != null) ? p.wage_increase_value : gv;
    const rate = p.hourly_rate || 0;
    const effOv = p.effective_week_override;
    const preW = (effOv && effOv > 0) ? Math.max(effOv - 1, 0) : preWks;
    const postW = (effOv && effOv > 0) ? (52 - preW) : postWks;
    const postRate = applyWageIncrease(rate, pm, pv);
    annualBase = (rate * 40 * preW * count) + (postRate * 40 * postW * count);
  } catch (e) {}
  let total = (p.bonus_per_employee || 0) * count;
  extras.forEach(e => {
    const amt = e.amount || 0;
    if (e.basis === 'per_emp')        total += amt * count;
    else if (e.basis === 'lump')      total += amt;
    else if (e.basis === 'pct_wages') total += amt * annualBase;
  });
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  let rowsHtml = '';
  extras.forEach((e, ei) => {
    const displayAmt = (e.basis === 'pct_wages') ? ((e.amount || 0) * 100).toFixed(2) : (e.amount || 0);
    rowsHtml +=
      '<input type="text" placeholder="Label (e.g. Performance)" value="' + (e.label || '').replace(/"/g, '&quot;') + '" ' +
      'onchange="updateBonusExtraField(' + idx + ',' + ei + ',\'label\',this.value)" ' +
      'style="padding:4px 6px; border:1px solid #d1d5db; border-radius:4px; font-size:12px; background:#fbfaf4; font-family:inherit;">' +
      '<input type="text" value="' + displayAmt + '" ' +
      'onchange="updateBonusExtraAmount(' + idx + ',' + ei + ',this.value)" ' +
      'style="padding:4px 6px; border:1px solid #d1d5db; border-radius:4px; font-size:12px; text-align:right; font-variant-numeric:tabular-nums; background:#fbfaf4; font-family:inherit;">' +
      '<select onchange="updateBonusExtraField(' + idx + ',' + ei + ',\'basis\',this.value)" ' +
      'style="padding:4px 6px; border:1px solid #d1d5db; border-radius:4px; font-size:12px; background:#fbfaf4; font-family:inherit;">' +
      '<option value="per_emp"' + (e.basis === 'per_emp' ? ' selected' : '') + '>$ per emp</option>' +
      '<option value="lump"' + (e.basis === 'lump' ? ' selected' : '') + '>$ lump sum</option>' +
      '<option value="pct_wages"' + (e.basis === 'pct_wages' ? ' selected' : '') + '>% of wages</option>' +
      '</select>' +
      '<button type="button" onclick="removeBonusExtra(' + idx + ',' + ei + ')" title="Remove" ' +
      'style="background:none; border:none; color:#dc2626; cursor:pointer; font-size:14px; padding:0;">✕</button>';
  });
  const emptyMsg = (extras.length === 0)
    ? '<span style="grid-column:1/-1; color:#9ca3af; font-style:italic; padding:8px 0; text-align:center; font-size:11px;">No extra bonus lines yet.</span>'
    : '';
  return (
    '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; padding-bottom:6px; border-bottom:1px solid #f3f4f6;">' +
      '<strong style="font-size:11px; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.4px;">' + (p.position_name || 'Position') + ' — Additional Bonuses</strong>' +
      '<button type="button" onclick="closeBonusPopover()" style="background:none; border:none; color:#9ca3af; cursor:pointer; font-size:14px; padding:2px 6px; border-radius:3px;">✕</button>' +
    '</div>' +
    '<div style="display:grid; grid-template-columns: 1fr 90px 110px 24px; gap:6px; align-items:center;">' +
      '<span style="font-size:10px; color:#6b7280; text-transform:uppercase; font-weight:700;">Label</span>' +
      '<span style="font-size:10px; color:#6b7280; text-transform:uppercase; font-weight:700; text-align:right;">Amount</span>' +
      '<span style="font-size:10px; color:#6b7280; text-transform:uppercase; font-weight:700;">Basis</span>' +
      '<span></span>' +
      rowsHtml + emptyMsg +
    '</div>' +
    '<button type="button" onclick="addBonusExtra(' + idx + ')" ' +
    'style="margin-top:8px; padding:5px 10px; font-size:11px; font-weight:600; background:white; color:#2563eb; border:1px solid #2563eb; border-radius:4px; cursor:pointer;">+ Add Bonus Line</button>' +
    '<div style="margin-top:10px; padding-top:8px; border-top:1px dashed #e5e7eb; display:flex; justify-content:space-between; font-size:11px;">' +
      '<span style="color:#6b7280;">Total bonus (base + extras):</span>' +
      '<strong style="font-size:13px; color:#15803d; font-variant-numeric:tabular-nums;">' + fD(total) + '</strong>' +
    '</div>' +
    '<div style="margin-top:6px; font-size:10px; color:#9ca3af; font-style:italic;">Base $/emp cell stays as-is for the simple case. Lines added here stack on top.</div>'
  );
}

function refreshBonusPopover() {
  if (_prBonusPopoverIdx == null) return;
  const pop = document.getElementById('prBonusPopover');
  if (!pop) return;
  pop.innerHTML = buildBonusPopoverHTML(_prBonusPopoverIdx);
}

function addBonusExtra(idx) {
  const p = _payrollPositions[idx];
  if (!p) return;
  if (!Array.isArray(p.extra_bonuses)) p.extra_bonuses = [];
  p.extra_bonuses.push({label: '', amount: 0, basis: 'per_emp'});
  refreshBonusPopover();
  recalcPayroll();
  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 800);
}

function removeBonusExtra(idx, ei) {
  const p = _payrollPositions[idx];
  if (!p || !Array.isArray(p.extra_bonuses)) return;
  p.extra_bonuses.splice(ei, 1);
  refreshBonusPopover();
  recalcPayroll();
  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 800);
}

function updateBonusExtraField(idx, ei, field, value) {
  const p = _payrollPositions[idx];
  if (!p || !p.extra_bonuses || !p.extra_bonuses[ei]) return;
  // FA directive 2026-05-10: skip when value didn't change.
  if (p.extra_bonuses[ei][field] === value) return;
  p.extra_bonuses[ei][field] = value;
  refreshBonusPopover();
  recalcPayroll();
  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 800);
}

function updateBonusExtraAmount(idx, ei, raw) {
  const p = _payrollPositions[idx];
  if (!p || !p.extra_bonuses || !p.extra_bonuses[ei]) return;
  const num = parseFloat((raw || '').replace(/[^0-9.\-]/g, '')) || 0;
  const e = p.extra_bonuses[ei];
  // For pct_wages, user enters a whole-percent (2 for 2%), store as decimal (0.02)
  const newAmount = (e.basis === 'pct_wages') ? (num / 100) : num;
  // FA directive 2026-05-10: skip when value didn't change.
  if (e.amount === newAmount) return;
  e.amount = newAmount;
  refreshBonusPopover();
  recalcPayroll();
  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 800);
}

// ── Render Roster Table ───────────────────────────────────────────────────

function renderPayrollRoster(posCalcs, totalEmp, totalBase, totalOT, totalVSH, totalComp, assumpCtx) {
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const body = document.getElementById('prRosterBody');
  const foot = document.getElementById('prRosterFoot');
  if (!body) return;

  // If no calcs passed, just render empty inputs
  if (!posCalcs) posCalcs = _payrollPositions.map(() => ({count:0,rate:0,weeklyPay:0,preIncrWages:0,postIncrRate:0,postIncrWages:0,annualBase:0,ot:0,vsh:0,comp:0}));

  const ctx = assumpCtx || {preWks:15, postWks:37, wageInc:0, otFactor:0.002, vshFactor:0.10};
  const cs = 'padding:7px 10px; border-bottom:1px solid #f3f4f6;';
  const ns = cs + 'text-align:right; font-variant-numeric:tabular-nums; font-size:12px;';
  const gs = 'color:#16a34a; font-weight:600;';
  const is = 'padding:4px 8px; border:1px solid #d1d5db; border-radius:4px; font-size:12px; text-align:right; background:#fbfaf4; box-sizing:content-box;';

  // fx cell helper for roster calculated fields (click to view formula, read-only)
  // Matches FA `.cell-fx` pattern: transparent bg + inset green left-border + dark green text.
  const rosterFx = (id, field, val, formula, posIdx, bgColor, fontWeight) => {
    const displayVal = (field === 'postIncrRate') ? '$' + val.toFixed(2) : fD(val);
    const tdStyle = 'padding:6px 8px; border-bottom:1px solid var(--gray-200); text-align:right; position:relative; cursor:pointer;';
    const inputStyle = 'cursor:pointer; pointer-events:none; width:100%; padding:4px 6px 4px 9px; border:1px solid #e5e1d8; border-radius:4px; background:transparent; box-shadow:inset 3px 0 0 #16a34a; text-align:right; font-family:inherit; font-size:13px; font-variant-numeric:tabular-nums; box-sizing:border-box; ' + (bgColor || 'color:#15803d;') + ' ' + (fontWeight || 'font-weight:600;');
    return '<td style="' + tdStyle + '" onclick="fxCellFocus(document.getElementById(\'' + id + '\'))">' +
      '<input id="' + id + '" type="text" readonly ' +
        'data-readonly="true" ' +
        'data-gl="Roster[' + posIdx + ']" ' +
        'data-field="' + field + '" ' +
        'data-raw="' + Math.round(val) + '" ' +
        'data-formula="' + formula.replace(/"/g, '&quot;') + '" ' +
        'value="' + displayVal + '" ' +
        'onblur="fxCellBlur(this)" ' +
        'style="' + inputStyle + '">' +
      '</td>';
  };

  let rows = '';
  _payrollPositions.forEach((p, i) => {
    const c = posCalcs[i] || {};
    const count = p.employee_count || 0;
    const rate = p.hourly_rate || 0;
    // Build formulas as parseable math strings with literal values (safeEvalFormula compatible)
    // Uses per-position week overrides if set, otherwise global ctx values
    const usedPreWks = c.posPreWks !== undefined ? c.posPreWks : ctx.preWks;
    const usedPostWks = c.posPostWks !== undefined ? c.posPostWks : ctx.postWks;
    const fWeekly = '=' + rate + '*40';
    const fPreWages = '=' + (c.weeklyPay||0) + '*' + usedPreWks + '*' + count;
    // Formula reflects whichever mode is actually in effect for THIS position
    // (per-position override or global fallback — already resolved in posCalcs).
    const posWiMode = (c.posWiMode === 'dollar') ? 'dollar' : 'pct';
    const posWiValue = c.posWiValue || 0;
    const fPostRate = (posWiMode === 'dollar')
      ? ('=' + rate + '+' + posWiValue.toFixed(2))
      : ('=' + rate + '*(1+' + posWiValue.toFixed(4) + ')');
    const fPostWages = '=' + (c.postIncrRate||0).toFixed(4) + '*40*' + usedPostWks + '*' + count;
    const fAnnualBase = '=' + (c.preIncrWages||0) + '+' + (c.postIncrWages||0);
    const fOT = '=' + (c.annualBase||0) + '*' + ctx.otFactor.toFixed(4);
    const fVSH = '=' + (c.annualBase||0) + '*' + ctx.vshFactor.toFixed(4);
    const fComp = '=' + (c.annualBase||0) + '+' + (c.ot||0) + '+' + (c.vsh||0);

    // Per-position wage-increase override display (empty = inheriting global)
    const posHasOverride = (p.wage_increase_mode != null);
    let incrPctDisplay = '', incrDollarDisplay = '';
    if (posHasOverride) {
      const mv = p.wage_increase_value || 0;
      if (p.wage_increase_mode === 'dollar') {
        incrDollarDisplay = mv.toFixed(2);
        incrPctDisplay = (rate > 0) ? ((mv / rate) * 100).toFixed(2) : '';
      } else {
        incrPctDisplay = (mv * 100).toFixed(2);
        incrDollarDisplay = (rate * mv).toFixed(2);
      }
    }
    // Light blue tint when row is overriding global; otherwise same cream as other inputs
    const overrideIs = 'padding:4px 8px; border:1px solid ' + (posHasOverride ? '#60a5fa' : '#d1d5db') + '; border-radius:4px; font-size:12px; text-align:right; background:' + (posHasOverride ? '#eff6ff' : '#fbfaf4') + '; box-sizing:content-box;';

    rows += '<tr>' +
      '<td style="' + cs + '"><input class="pr-pos-name" type="text" value="' + (p.position_name || '') + '" onchange="prRosterChanged(this)" style="padding:4px 8px; border:1px solid #d1d5db; border-radius:4px; font-size:12px; background:#fbfaf4; box-sizing:content-box;"></td>' +
      '<td style="' + ns + '"><input class="pr-pos-count" type="number" value="' + (p.employee_count || 0) + '" onchange="prRosterChanged(this)" style="' + is + '" min="0"></td>' +
      '<td style="' + ns + '"><input class="pr-pos-rate" type="text" value="' + (p.hourly_rate || 0) + '" onchange="prRosterChanged(this)" style="' + is + '"></td>' +
      '<td style="' + ns + '">' + prBonusCellHTML(p, i, c) + '</td>' +
      '<td style="' + ns + '"><input class="pr-pos-effwk" type="number" min="1" max="52" placeholder="—" value="' + (p.effective_week_override || '') + '" onchange="prRosterChanged(this)" title="Override global Effective Week for this position only" style="' + is + '"></td>' +
      '<td style="' + ns + '"><input class="pr-pos-wage-incr-pct" type="text" placeholder="—" value="' + incrPctDisplay + '" onchange="prRosterWageIncrChanged(this,' + i + ',\'pct\')" title="Override wage increase % for this position (leave blank to inherit global)" style="' + overrideIs + '"></td>' +
      '<td style="' + ns + '"><input class="pr-pos-wage-incr-dollar" type="text" placeholder="—" value="' + incrDollarDisplay + '" onchange="prRosterWageIncrChanged(this,' + i + ',\'dollar\')" title="Override wage increase $/hr for this position (leave blank to inherit global)" style="' + overrideIs + '"></td>' +
      rosterFx('pr_rost_wk_'+i, 'weeklyPay', c.weeklyPay||0, fWeekly, i) +
      rosterFx('pr_rost_pre_'+i, 'preIncrWages', c.preIncrWages||0, fPreWages, i) +
      rosterFx('pr_rost_pr_'+i, 'postIncrRate', c.postIncrRate||0, fPostRate, i) +
      rosterFx('pr_rost_post_'+i, 'postIncrWages', c.postIncrWages||0, fPostWages, i) +
      rosterFx('pr_rost_base_'+i, 'annualBase', c.annualBase||0, fAnnualBase, i, 'color:#1f2937;', 'font-weight:700;') +
      rosterFx('pr_rost_ot_'+i, 'ot', c.ot||0, fOT, i) +
      rosterFx('pr_rost_vsh_'+i, 'vsh', c.vsh||0, fVSH, i) +
      rosterFx('pr_rost_comp_'+i, 'comp', c.comp||0, fComp, i, 'color:#1e40af;', 'font-weight:800;') +
      // Per-position adjust + remove buttons. Adjust opens the benefit-adjustments
      // modal; the gear shows an amber badge "N/M" when an adjustment is active.
      _renderPayrollAdjustCell(p, i) +
      '</tr>';
  });
  body.innerHTML = rows;

  // Footer totals
  // FA dir 2026-06-04: roster TOTAL row cells are click-to-inspect — clicking
  // opens the lineage drill panel showing the per-position breakdown of that
  // column. Mirrors the per-line cells; totals re-sum from the positions above.
  const _b = 'border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;';
  const _sumPC = f => _payrollPositions.reduce((s,p,i)=> s + ((posCalcs[i] && posCalcs[i][f]) || 0), 0);
  const rosterCell = (key, label, val, extra) => '<td data-payroll-cell="' + key +
    '" data-payroll-label="' + String(label).replace(/"/g, '&quot;') +
    '" style="padding:8px 10px; text-align:right; ' + _b + ' cursor:pointer; position:relative; ' + (extra || '') + '"' +
    ' onclick="payrollShowLineage(event, this)" title="Click to see the per-position breakdown">' +
    '<span class="pr-total-val">' + val + '</span>' +
    ' <span style="font-size:8px; color:#16a34a; border:1px solid #86efac; border-radius:3px; padding:0 2px; vertical-align:middle; font-weight:700;">fx</span></td>';
  foot.innerHTML =
    '<tr style="background:var(--gray-50); font-weight:700;">' +
    '<td style="padding:8px 10px; ' + _b + '">TOTAL</td>' +
    '<td style="padding:8px 10px; text-align:right; ' + _b + ' font-weight:700;">' + (totalEmp || 0) + '</td>' +
    '<td style="' + _b + '"></td>' +
    rosterCell('roster_bonus', 'Total Bonus', fD(_sumPC('bonus')), 'font-weight:700;') +
    '<td style="' + _b + '"></td>' +
    '<td style="' + _b + '"></td>' +
    '<td style="' + _b + '"></td>' +
    rosterCell('roster_weekly', 'Total Weekly Pay', fD(_sumPC('weeklyPay'))) +
    rosterCell('roster_preincr', 'Total Pre-Incr Wages', fD(_sumPC('preIncrWages'))) +
    '<td style="' + _b + '"></td>' +
    rosterCell('roster_postincr', 'Total Post-Incr Wages', fD(_sumPC('postIncrWages'))) +
    rosterCell('roster_base', 'Total Annual Base', fD(totalBase || 0), 'font-weight:800;') +
    rosterCell('roster_ot', 'Total OT', fD(totalOT || 0)) +
    rosterCell('roster_vsh', 'Total VSH', fD(totalVSH || 0)) +
    rosterCell('roster_comp', 'Total Annual Comp', fD(totalComp || 0), 'font-weight:800; font-size:13px; color:#1e40af;') +
    '<td style="' + _b + '"></td>' +
    '</tr>' +
    '<tr><td colspan="16" style="padding:8px 10px;">' +
    '<button onclick="addPayrollPosition()" style="padding:4px 12px; font-size:11px; font-weight:600; border-radius:5px; cursor:pointer; background:white; color:#2563eb; border:1px solid #2563eb;">+ Add Position</button>' +
    '<span style="margin-left:12px; font-size:10px; color:var(--gray-400); font-style:italic;">Flexible positions — each building can have different roles</span>' +
    '</td></tr>';

  // Update badges
  const badge = document.getElementById('prRosterBadge');
  const totBadge = document.getElementById('prRosterTotal');
  if (badge) badge.textContent = (totalEmp || 0) + ' employees';
  if (totBadge) totBadge.textContent = 'Total: ' + fD(totalComp || 0);

  // Auto-size all roster inputs to their content width
  if (typeof prAutoSizeAll === 'function') prAutoSizeAll();
}

// ── Per-Position Benefit Adjustments (FA directive 2026-05-05) ────────────
// Lets the FA flag "N of M employees in this position have an extra rate ×
// periods block on welfare/pension/supp_retirement/legal/training/profit_sharing".
// Math is additive to the building default; e.g. 6 doormen with 1 getting
// +$82.50 × 30 wks of pension on top of the standard $82.50 × 52.

const PR_ADJ_BENEFITS = [
  {key: 'welfare',         label: 'Welfare',          unit: '$/mo',  periodLabel: 'mo',  periodsPerYear: 12, defaultKey: 'welfare_monthly'},
  {key: 'pension',         label: 'Pension',          unit: '$/wk',  periodLabel: 'wk',  periodsPerYear: 52, defaultKey: 'pension_weekly'},
  {key: 'supp_retirement', label: 'Supp Retirement',  unit: '$/wk',  periodLabel: 'wk',  periodsPerYear: 52, defaultKey: 'supp_retirement_weekly'},
  {key: 'legal',           label: 'Legal',            unit: '$/mo',  periodLabel: 'mo',  periodsPerYear: 12, defaultKey: 'legal_monthly'},
  {key: 'training',        label: 'Training',         unit: '$/mo',  periodLabel: 'mo',  periodsPerYear: 12, defaultKey: 'training_monthly'},
  {key: 'profit_sharing',  label: 'Profit Sharing',   unit: '$/qtr', periodLabel: 'qtr', periodsPerYear: 4,  defaultKey: 'profit_sharing_quarterly'},
];

// Render the trailing actions cell for a roster row: amber badge when an
// adjustment is active + ⚙️ Adjust button + ✕ remove button.
function _renderPayrollAdjustCell(p, i) {
  const adj = p && p.benefit_adjustments;
  const cnt = adj && adj.adjusted_count ? Math.max(parseInt(adj.adjusted_count, 10) || 0, 0) : 0;
  const hasAdj = (cnt > 0 && adj && adj.benefits && Object.keys(adj.benefits).length > 0);
  const empCount = parseInt(p.employee_count || 0, 10) || 0;
  const tdStyle = 'padding:7px 4px; border-bottom:1px solid #f3f4f6; white-space:nowrap;';
  const badgeStyle = 'display:inline-block;font-size:9px;font-weight:700;color:#92400e;background:#fde68a;padding:2px 5px;border-radius:3px;letter-spacing:0.3px;margin-right:4px;';
  let badge = '';
  if (hasAdj) {
    const tooltip = _payrollAdjTooltip(p);
    badge = '<span style="' + badgeStyle + '" title="' + tooltip.replace(/"/g, '&quot;') + '">' + cnt + '/' + empCount + '</span>';
  }
  const adjBtn = '<button onclick="prShowAdjustModal(' + i + ')" title="Per-employee benefit adjustments (e.g. double pension for 1 of N)" style="padding:2px 6px;font-size:10px;cursor:pointer;background:' + (hasAdj ? '#fef3c7' : '#f9fafb') + ';color:#92400e;border:1px solid ' + (hasAdj ? '#fde68a' : '#e5e7eb') + ';border-radius:4px;margin-right:3px;">⚙️</button>';
  const remBtn = '<button onclick="removePayrollPosition(' + i + ')" style="padding:2px 6px;font-size:10px;cursor:pointer;background:#fef2f2;color:#dc2626;border:1px solid #fecaca;border-radius:4px;">✕</button>';
  return '<td style="' + tdStyle + '">' + badge + adjBtn + remBtn + '</td>';
}

// Build a tooltip describing the active benefit adjustments on a position.
function _payrollAdjTooltip(p) {
  const adj = p && p.benefit_adjustments;
  if (!adj || !adj.benefits) return '';
  const cnt = adj.adjusted_count || 0;
  const lines = [adj.label ? ('— ' + adj.label) : ('Adjustment for ' + cnt + ' of ' + (p.employee_count || 0) + ' employees')];
  PR_ADJ_BENEFITS.forEach(b => {
    const v = adj.benefits[b.key];
    if (v && v.rate && v.periods) {
      const sign = v.rate >= 0 ? '+' : '';
      lines.push('  ' + b.label + ': ' + sign + v.rate.toFixed(2) + ' ' + b.unit + ' × ' + v.periods + ' ' + b.periodLabel + (v.label ? ' (' + v.label + ')' : ''));
    }
  });
  return lines.join('\n');
}

// Open the per-position adjustment modal. Reads the building's payroll
// assumption defaults from window._payrollAssumptions for "Default" display.
function prShowAdjustModal(idx) {
  const p = _payrollPositions[idx];
  if (!p) return;
  const a = (typeof _payrollAssumptions !== 'undefined' && _payrollAssumptions) ? _payrollAssumptions : {};
  const empCount = parseInt(p.employee_count || 0, 10) || 0;
  const adj = p.benefit_adjustments || {};
  const benefits = adj.benefits || {};
  const adjustedCount = Math.min(parseInt(adj.adjusted_count || 0, 10) || 0, empCount);

  let modal = document.getElementById('prAdjustModal');
  let overlay = document.getElementById('prAdjustOverlay');
  if (!modal) {
    overlay = document.createElement('div');
    overlay.id = 'prAdjustOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.35);z-index:120;';
    overlay.onclick = prCloseAdjustModal;
    document.body.appendChild(overlay);
    modal = document.createElement('div');
    modal.id = 'prAdjustModal';
    modal.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:white;border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,0.25);padding:0;z-index:121;width:680px;max-width:95vw;max-height:90vh;display:flex;flex-direction:column;';
    document.body.appendChild(modal);
  }
  overlay.style.display = 'block';
  modal.style.display = 'flex';

  let html = '';
  // Header
  html += '<div style="padding:18px 20px;border-bottom:1px solid var(--gray-200);">';
  html += '<div style="font-size:15px;font-weight:700;color:var(--blue-dark);">⚙️ Adjust Benefits — ' + (p.position_name || '(unnamed position)') + '</div>';
  html += '<div style="font-size:12px;color:var(--gray-500);margin-top:4px;">Position has <b>' + empCount + '</b> employee' + (empCount === 1 ? '' : 's') + '. Use this when only some of them need a different benefit (e.g. one tenured doorman gets double pension). Adjustment math: <i>rate × periods × adjusted_count</i> stacks on top of the building default. Use the <b>Quick set</b> buttons to remove a benefit (nets to $0 for the adjusted employees, e.g. someone not getting pension) or add a building default in one click.</div>';
  html += '</div>';
  // Body (scrollable)
  html += '<div style="padding:14px 20px;overflow-y:auto;flex:1;">';
  // Adjusted count + label
  html += '<div style="display:flex;gap:14px;align-items:flex-end;margin-bottom:14px;padding:10px 12px;background:#fffbeb;border:1px solid #fde68a;border-radius:8px;">';
  html += '<div><label style="display:block;font-size:11px;font-weight:600;color:var(--gray-600);margin-bottom:3px;">How many employees adjusted?</label>';
  html += '<input id="prAdjCount" type="number" min="0" max="' + empCount + '" value="' + adjustedCount + '" style="padding:6px 10px;border:1px solid var(--gray-300);border-radius:6px;font-size:13px;width:80px;text-align:right;"> <span style="color:var(--gray-500);font-size:12px;margin-left:4px;">of ' + empCount + '</span></div>';
  html += '<div style="flex:1;"><label style="display:block;font-size:11px;font-weight:600;color:var(--gray-600);margin-bottom:3px;">Label (optional)</label>';
  html += '<input id="prAdjLabel" type="text" placeholder="e.g. Tenure exception" value="' + (adj.label || '').replace(/"/g, '&quot;') + '" style="padding:6px 10px;border:1px solid var(--gray-300);border-radius:6px;font-size:13px;width:100%;"></div>';
  html += '</div>';

  // Per-benefit rows
  html += '<table style="width:100%;border-collapse:collapse;font-size:12px;">';
  html += '<thead><tr style="background:var(--gray-100);color:var(--gray-700);text-align:left;">' +
    '<th style="padding:6px 8px;border-bottom:1px solid var(--gray-300);">Benefit</th>' +
    '<th style="padding:6px 8px;border-bottom:1px solid var(--gray-300);text-align:right;">Building Default</th>' +
    '<th style="padding:6px 8px;border-bottom:1px solid var(--gray-300);text-align:right;">Adj Rate</th>' +
    '<th style="padding:6px 8px;border-bottom:1px solid var(--gray-300);text-align:right;">Adj Periods</th>' +
    '<th style="padding:6px 8px;border-bottom:1px solid var(--gray-300);">Label</th>' +
    '<th style="padding:6px 8px;border-bottom:1px solid var(--gray-300);text-align:right;">Δ / yr / employee</th>' +
    '<th style="padding:6px 8px;border-bottom:1px solid var(--gray-300);">Quick set</th>' +
    '</tr></thead><tbody>';
  PR_ADJ_BENEFITS.forEach((b, bIdx) => {
    const def = parseFloat(a[b.defaultKey] || 0) || 0;
    const defAnnual = def * b.periodsPerYear;
    const cur = benefits[b.key] || {};
    const curRate = cur.rate != null ? cur.rate : '';
    const curPeriods = cur.periods != null ? cur.periods : '';
    const curLabel = (cur.label || '').replace(/"/g, '&quot;');
    html += '<tr>';
    html += '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);font-weight:600;">' + b.label + ' <span style="color:var(--gray-500);font-weight:400;">(' + b.unit + ')</span></td>';
    html += '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);text-align:right;color:var(--gray-500);font-variant-numeric:tabular-nums;">' + def.toFixed(2) + ' × ' + b.periodsPerYear + ' = $' + defAnnual.toFixed(2) + '</td>';
    html += '<td style="padding:4px 6px;border-bottom:1px solid var(--gray-100);text-align:right;"><input class="pr-adj-rate" data-key="' + b.key + '" type="text" value="' + curRate + '" placeholder="—" oninput="prAdjRecalc()" style="width:80px;padding:4px 6px;border:1px solid var(--gray-300);border-radius:4px;font-size:12px;text-align:right;"></td>';
    html += '<td style="padding:4px 6px;border-bottom:1px solid var(--gray-100);text-align:right;"><input class="pr-adj-periods" data-key="' + b.key + '" type="text" value="' + curPeriods + '" placeholder="' + b.periodLabel + '" oninput="prAdjRecalc()" style="width:60px;padding:4px 6px;border:1px solid var(--gray-300);border-radius:4px;font-size:12px;text-align:right;"></td>';
    html += '<td style="padding:4px 6px;border-bottom:1px solid var(--gray-100);"><input class="pr-adj-label" data-key="' + b.key + '" type="text" value="' + curLabel + '" placeholder="(optional)" style="width:100%;padding:4px 6px;border:1px solid var(--gray-300);border-radius:4px;font-size:12px;"></td>';
    html += '<td id="pr-adj-delta-' + b.key + '" style="padding:6px 8px;border-bottom:1px solid var(--gray-100);text-align:right;font-variant-numeric:tabular-nums;font-weight:600;color:#92400e;">$0.00</td>';
    // FA dir 2026-06-03 (#4): one-click add/remove of this benefit for the
    // adjusted employees. "Remove" fills the NEGATIVE building default so the
    // benefit nets to $0 (e.g. an employee who does not get pension). "+Def"
    // adds another building default (e.g. double). "Clr" wipes the row. Numeric
    // index args keep the onclick free of nested string quotes.
    html += '<td style="padding:4px 6px;border-bottom:1px solid var(--gray-100);white-space:nowrap;">' +
      '<button type="button" onclick="prAdjRemove(' + bIdx + ')" title="Remove this benefit for the adjusted employees (nets to $0)" style="padding:2px 6px;font-size:10px;cursor:pointer;background:#fef2f2;color:#dc2626;border:1px solid #fecaca;border-radius:4px;margin-right:3px;">&minus; Remove</button>' +
      '<button type="button" onclick="prAdjAddDef(' + bIdx + ')" title="Add another building default of this benefit" style="padding:2px 6px;font-size:10px;cursor:pointer;background:#f0fdf4;color:#15803d;border:1px solid #bbf7d0;border-radius:4px;margin-right:3px;">&plus; Def</button>' +
      '<button type="button" onclick="prAdjClear(' + bIdx + ')" title="Clear this benefit row" style="padding:2px 6px;font-size:10px;cursor:pointer;background:#f9fafb;color:#6b7280;border:1px solid #e5e7eb;border-radius:4px;">Clr</button>' +
      '</td>';
    html += '</tr>';
  });
  html += '</tbody></table>';
  // Total impact
  html += '<div style="margin-top:14px;padding:10px 12px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;display:flex;justify-content:space-between;align-items:center;">';
  html += '<span style="font-weight:600;color:#15803d;">Total adjustment impact (this row)</span>';
  html += '<span id="prAdjTotalImpact" style="font-weight:700;color:#15803d;font-size:14px;font-variant-numeric:tabular-nums;">$0.00 / yr</span>';
  html += '</div>';
  html += '</div>';
  // Footer
  html += '<div style="padding:12px 20px;border-top:1px solid var(--gray-200);display:flex;justify-content:space-between;align-items:center;background:var(--gray-50);">';
  html += '<button onclick="prClearAdjustment(' + idx + ')" style="padding:7px 14px;border:1px solid var(--gray-300);background:white;color:var(--red);border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;">Clear adjustment</button>';
  html += '<div style="display:flex;gap:8px;">';
  html += '<button onclick="prCloseAdjustModal()" style="padding:7px 14px;border:1px solid var(--gray-300);background:white;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;">Cancel</button>';
  html += '<button onclick="prSaveAdjustment(' + idx + ')" style="padding:7px 16px;border:none;background:var(--blue);color:white;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;">Save Adjustment</button>';
  html += '</div></div>';

  modal.innerHTML = html;
  prAdjRecalc();
}

function prCloseAdjustModal() {
  const m = document.getElementById('prAdjustModal');
  const o = document.getElementById('prAdjustOverlay');
  if (m) m.style.display = 'none';
  if (o) o.style.display = 'none';
}

// Live total/per-row impact calc as the FA types.
function prAdjRecalc() {
  const cntEl = document.getElementById('prAdjCount');
  const cnt = parseInt((cntEl && cntEl.value) || 0, 10) || 0;
  let total = 0;
  PR_ADJ_BENEFITS.forEach(b => {
    const rateEl = document.querySelector('.pr-adj-rate[data-key="' + b.key + '"]');
    const periodsEl = document.querySelector('.pr-adj-periods[data-key="' + b.key + '"]');
    const r = parseFloat((rateEl && rateEl.value) || '') || 0;
    const pp = parseFloat((periodsEl && periodsEl.value) || '') || 0;
    const delta = r * pp * cnt;
    total += delta;
    const td = document.getElementById('pr-adj-delta-' + b.key);
    if (td) {
      const sign = delta >= 0 ? '+' : '−';
      td.textContent = (delta === 0) ? '$0.00' : (sign + '$' + Math.abs(delta).toFixed(2));
      td.style.color = delta >= 0 ? '#92400e' : '#15803d';
    }
  });
  const totEl = document.getElementById('prAdjTotalImpact');
  if (totEl) {
    const sign = total >= 0 ? '+' : '−';
    totEl.textContent = (total === 0) ? '$0.00 / yr' : (sign + '$' + Math.abs(total).toFixed(2) + ' / yr');
  }
}

// FA dir 2026-06-03 (#4): one-click add/remove of a benefit for the adjusted
// employees, in the ⚙️ gear box. "Remove" fills the negative of the building
// default (rate × periods cancels the default so the benefit nets to $0 for
// those employees — e.g. a doorman who does not get pension). "Add" fills the
// positive default (e.g. double pension). "Clear" wipes the row. Works against
// the existing benefit_adjustments model — no new fields.
function _prAdjApplyQuick(bIdx, mode) {
  const b = PR_ADJ_BENEFITS[bIdx];
  if (!b) return;
  const a = (typeof _payrollAssumptions !== 'undefined' && _payrollAssumptions) ? _payrollAssumptions : {};
  const def = parseFloat(a[b.defaultKey] || 0) || 0;
  const rateEl = document.querySelector('.pr-adj-rate[data-key="' + b.key + '"]');
  const periodsEl = document.querySelector('.pr-adj-periods[data-key="' + b.key + '"]');
  const labelEl = document.querySelector('.pr-adj-label[data-key="' + b.key + '"]');
  if (mode === 'remove') {
    if (rateEl) rateEl.value = (-def).toFixed(2);
    if (periodsEl) periodsEl.value = String(b.periodsPerYear);
    if (labelEl && !labelEl.value) labelEl.value = 'No ' + b.label;
  } else if (mode === 'add') {
    if (rateEl) rateEl.value = def.toFixed(2);
    if (periodsEl) periodsEl.value = String(b.periodsPerYear);
  } else {
    if (rateEl) rateEl.value = '';
    if (periodsEl) periodsEl.value = '';
    if (labelEl) labelEl.value = '';
  }
  prAdjRecalc();
}
function prAdjRemove(bIdx) { _prAdjApplyQuick(bIdx, 'remove'); }
function prAdjAddDef(bIdx) { _prAdjApplyQuick(bIdx, 'add'); }
function prAdjClear(bIdx) { _prAdjApplyQuick(bIdx, 'clear'); }

// Save the adjustment back to the position object + persist + recalc payroll.
function prSaveAdjustment(idx) {
  const p = _payrollPositions[idx];
  if (!p) return;
  const empCount = parseInt(p.employee_count || 0, 10) || 0;
  const cntEl = document.getElementById('prAdjCount');
  let cnt = parseInt((cntEl && cntEl.value) || 0, 10) || 0;
  if (cnt < 0) cnt = 0;
  if (cnt > empCount) cnt = empCount;
  const labelEl = document.getElementById('prAdjLabel');
  const adjLabel = (labelEl && labelEl.value || '').trim();

  const benefits = {};
  PR_ADJ_BENEFITS.forEach(b => {
    const rateEl = document.querySelector('.pr-adj-rate[data-key="' + b.key + '"]');
    const periodsEl = document.querySelector('.pr-adj-periods[data-key="' + b.key + '"]');
    const labelE = document.querySelector('.pr-adj-label[data-key="' + b.key + '"]');
    const r = parseFloat((rateEl && rateEl.value) || '') || 0;
    const pp = parseFloat((periodsEl && periodsEl.value) || '') || 0;
    if (Math.abs(r) > 1e-9 && Math.abs(pp) > 1e-9) {
      benefits[b.key] = { rate: r, periods: pp, label: ((labelE && labelE.value) || '').trim().slice(0, 80) };
    }
  });

  // Persist only if there's something meaningful; else clear.
  if (cnt > 0 && Object.keys(benefits).length > 0) {
    p.benefit_adjustments = { adjusted_count: cnt, label: adjLabel.slice(0, 120), benefits: benefits };
  } else {
    p.benefit_adjustments = null;
  }
  prCloseAdjustModal();
  renderPayrollRoster(); // re-render to update the badge
  recalcPayroll();
  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 400);
}

// Wipe the adjustment for this row.
function prClearAdjustment(idx) {
  const p = _payrollPositions[idx];
  if (!p) return;
  if (!p.benefit_adjustments) { prCloseAdjustModal(); return; }
  if (!confirm('Clear the benefit adjustment on this position?')) return;
  p.benefit_adjustments = null;
  prCloseAdjustModal();
  renderPayrollRoster();
  recalcPayroll();
  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 400);
}

// ── Render Taxes/Benefits Table ───────────────────────────────────────────

function renderPayrollTaxes(t) {
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const fP = v => (v * 100).toFixed(3) + '%';
  const body = document.getElementById('prTaxBody');
  const foot = document.getElementById('prTaxFoot');
  if (!body) return;

  const a = _payrollAssumptions;
  const cs = 'padding:7px 10px; border-bottom:1px solid #f3f4f6;';
  const ns = cs + 'text-align:right; font-variant-numeric:tabular-nums;';
  const gs = 'color:#16a34a; font-weight:600;';
  const ps = 'color:#5a4a3f;';
  const catHdr = 'background:#f5efe7; font-weight:700; color:#5a4a3f; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; padding:8px 10px; border-bottom:2px solid #e5e7eb;';
  const subRow = 'background:var(--gray-50); font-weight:700; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb; padding:8px 10px;';

  // FA dir 2026-06-04: total rows are click-to-inspect — clicking opens the
  // lineage drill panel showing the component breakdown, exactly like the
  // per-line tax/benefit cells. Totals are NOT directly editable: a total is
  // the sum of its line items (recalc forces total = sum of the overridable
  // lines), so you edit the lines above and the total re-sums (Excel SUM
  // semantics). The little "fx" tag signals the cell is inspectable.
  const inspectTotal = (key, label, val, extraStyle) => {
    const _ov = !!(_payrollOverrides && _payrollOverrides[key] !== undefined && _payrollOverrides[key] !== null);
    const _num = parseFloat(String(val).replace(/[$,]/g, '')) || 0;
    const ovrStyle = _ov ? 'background:#fef3c7; color:#92400e;' : '';
    const badge = _ov
      ? '<span style="position:absolute; top:2px; right:3px; font-size:8px; font-weight:700; color:#92400e; background:#fde68a; padding:1px 3px; border-radius:3px; letter-spacing:0.3px; pointer-events:none;">OVR</span>'
      : ' <span style="font-size:8px; color:#16a34a; border:1px solid #86efac; border-radius:3px; padding:0 3px; vertical-align:middle; font-weight:700;">fx</span>';
    return '<td data-payroll-cell="' + key + '" data-payroll-label="' + String(label).replace(/"/g, '&quot;') +
      '" style="' + (extraStyle || subRow) + ' text-align:right; font-weight:800; cursor:cell; position:relative; ' + ovrStyle + '"' +
      ' onclick="payrollShowLineage(event, this)" oncontextmenu="return payrollRevertOverride(event, this)"' +
      ' title="' + (_ov ? 'Override active — right-click to revert to computed' : 'Click for breakdown · double-click to type an override') + '">' +
      '<span class="pr-total-val">' + val + '</span>' + badge +
      '<input type="text" class="pr-total-edit" data-payroll-cell="' + key + '" data-raw="' + _num + '" value="' + val + '"' +
      ' style="display:none; width:90%; padding:2px 4px; border:1px solid #d97706; border-radius:3px; text-align:right; font-variant-numeric:tabular-nums; background:white;"' +
      ' onblur="payrollOverrideSave(this)"' +
      ' onkeydown="if(event.key===\'Enter\'){this.blur();} else if(event.key===\'Escape\'){this.value=this.dataset.raw;this.blur();}">' +
      '</td>';
  };

  let html = '';
  // Payroll Taxes
  html += '<tr><td colspan="4" style="' + catHdr + '">Payroll Taxes</td></tr>';
  html += taxRow('FICA', fP(a.fica||0), 'Gross Wages × Rate', fD(t.ficaAmt), 'fica');
  html += taxRow('SUI', fP(a.sui||0), '$12,000 × Rate × ' + t.totalEmployees + ' emp', fD(t.suiAmt), 'sui');
  html += taxRow('FUI', fP(a.fui||0), '$7,000 × Rate × ' + t.totalEmployees + ' emp', fD(t.fuiAmt), 'fui');
  html += taxRow('MTA', fP(a.mta||0), 'Gross Wages × Rate', fD(t.mtaAmt), 'mta');
  html += taxRow('NYS Disability', fP(a.nys_disability||0), 'Per employee/year', fD(t.nysDisAmt), 'nys_disability');
  html += taxRow('Paid Family Leave', fP(a.pfl||0), 'Gross Wages × Rate', fD(t.pflAmt), 'pfl');
  html += '<tr><td colspan="3" style="' + subRow + '">Total Payroll Taxes</td>' + inspectTotal('total_payroll_tax', 'Total Payroll Taxes', fD(t.totalPayrollTax)) + '</tr>';

  // Workers Comp — now click-to-inspect + editable like the other rows.
  html += '<tr style="height:8px;"><td colspan="4"></td></tr>';
  html += taxRow('Workers Compensation', fP(a.workers_comp||0), 'Gross Wages × Rate', fD(t.wcAmt), 'workers_comp');

  // Union Benefits
  html += '<tr style="height:8px;"><td colspan="4"></td></tr>';
  html += '<tr><td colspan="4" style="' + catHdr + '">Union Benefits (32BJ)</td></tr>';
  html += taxRow('Welfare', '$' + (a.welfare_monthly||0).toFixed(2) + '/mo', '$' + (a.welfare_monthly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 12 mo', fD(t.welfareAmt), 'welfare');
  html += taxRow('Pension', '$' + (a.pension_weekly||0).toFixed(2) + '/wk', '$' + (a.pension_weekly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 52 wk', fD(t.pensionAmt), 'pension');
  html += taxRow('Supp. Retirement', '$' + (a.supp_retirement_weekly||0).toFixed(2) + '/wk', '$' + (a.supp_retirement_weekly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 52 wk', fD(t.suppRetAmt), 'supp_retirement');
  html += taxRow('Legal Fund', '$' + (a.legal_monthly||0).toFixed(2) + '/mo', '$' + (a.legal_monthly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 12 mo', fD(t.legalAmt), 'legal');
  html += taxRow('Training Fund', '$' + (a.training_monthly||0).toFixed(2) + '/mo', '$' + (a.training_monthly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 12 mo', fD(t.trainingAmt), 'training');
  html += taxRow('Profit Sharing', '$' + (a.profit_sharing_quarterly||0).toFixed(2) + '/qtr', '$' + (a.profit_sharing_quarterly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 4 qtr', fD(t.profitShareAmt), 'profit_sharing');
  html += '<tr><td colspan="3" style="' + subRow + '">Total Union Benefits</td>' + inspectTotal('total_union', 'Total Union Benefits', fD(t.totalUnion)) + '</tr>';

  body.innerHTML = html;

  // Grand total footer
  foot.innerHTML = '<tr style="background:#f5efe7; font-weight:800; font-size:13px;">' +
    '<td colspan="3" style="border-top:3px double #5a4a3f; padding:10px;">TOTAL LABOR & RELATED (calculated)</td>' +
    inspectTotal('total_labor', 'Total Labor & Related', fD(t.totalLaborCalc), 'border-top:3px double #5a4a3f; padding:10px; font-size:14px;') + '</tr>';

  // Update badge
  const badge = document.getElementById('prTaxTotal');
  if (badge) badge.textContent = 'Total: ' + fD(t.totalPayrollTax + t.wcAmt + t.totalUnion);
}

function taxRow(label, rate, basis, total, cellKey) {
  // FA directive 2026-05-17: each tax/benefit row's total is now click-to-inspect
  // AND editable. cellKey is the override storage key (e.g., "welfare").
  // - Click: opens the lineage drill panel for this cell
  // - Edit: types a new value → saves as override → OVR badge
  // - Right-click on OVR'd cell: reverts to computed
  const cs = 'padding:7px 10px; border-bottom:1px solid #f3f4f6;';
  const ns = cs + 'text-align:right; font-variant-numeric:tabular-nums;';
  const isOv = cellKey && _payrollOverrides &&
               _payrollOverrides[cellKey] !== undefined &&
               _payrollOverrides[cellKey] !== null;
  // Pull the numeric "total" by stripping the `$` and commas. We need this
  // for the editable input's data-raw so blur-detection can compare.
  const totalNum = parseFloat(String(total).replace(/[$,]/g, '')) || 0;
  let totalCell;
  if (cellKey) {
    // OVR badge + amber styling when override active
    const ovrStyle = isOv
      ? 'background:#fef3c7;color:#92400e;font-weight:700;border:1px dashed #d97706;'
      : 'color:#16a34a;font-weight:600;';
    const ovrBadge = isOv
      ? '<span style="position:absolute;top:2px;right:4px;font-size:8px;font-weight:700;color:#92400e;background:#fde68a;padding:1px 3px;border-radius:3px;letter-spacing:0.3px;pointer-events:none;">OVR</span>'
      : '';
    totalCell = '<td data-payroll-cell="' + cellKey + '" data-payroll-label="' +
      label.replace(/"/g, '&quot;') + '" style="' + ns + ' position:relative; cursor:cell; ' +
      ovrStyle + '" ' +
      'onclick="payrollShowLineage(event, this)" ' +
      'oncontextmenu="return payrollRevertOverride(event, this)" ' +
      'title="' + (isOv ? 'Override active — right-click to revert' : 'Click to inspect lineage. Double-click to edit.') + '">' +
      '<span class="pr-total-val">' + total + '</span>' + ovrBadge +
      // Hidden input used for inline edit on double-click.
      '<input type="text" class="pr-total-edit" data-payroll-cell="' + cellKey +
      '" data-raw="' + totalNum + '" value="' + total + '" ' +
      'style="display:none; width:90%; padding:2px 4px; border:1px solid #d97706; border-radius:3px; text-align:right; font-variant-numeric:tabular-nums; background:white;" ' +
      'onblur="payrollOverrideSave(this)" ' +
      'onkeydown="if(event.key===\'Enter\'){this.blur();} else if(event.key===\'Escape\'){this.value=this.dataset.raw;this.blur();}">' +
      '</td>';
    // Double-click swaps span↔input. Attach via the click handler with detail check.
  } else {
    totalCell = '<td style="' + ns + ' color:#16a34a; font-weight:600;">' + total + '</td>';
  }
  return '<tr><td style="' + cs + '">' + label + '</td>' +
    '<td style="' + ns + ' color:#5a4a3f;">' + rate + '</td>' +
    '<td style="' + cs + ' font-size:10px; color:var(--gray-400); font-style:italic;">' + basis + '</td>' +
    totalCell + '</tr>';
}

// FA directive 2026-05-17: opens the Payroll Lineage drill panel for a clicked
// tax/benefit total cell. Reads the cell key from data-payroll-cell and
// composes a breakdown using _payrollComputed, _payrollOverrides, the current
// _payrollAssumptions, and _payrollAdjustments (per-position deltas).
// Double-click on the cell starts inline edit instead of opening the panel.
function payrollShowLineage(evt, td) {
  if (!td || !td.dataset) return;
  // Double-click → enter edit mode on the hidden input.
  if (evt && evt.detail === 2) {
    const inp = td.querySelector('.pr-total-edit');
    const span = td.querySelector('.pr-total-val');
    if (inp) {
      if (span) span.style.display = 'none';
      inp.style.display = 'inline-block';
      inp.value = inp.dataset.raw || '';
      inp.focus();
      inp.select();
    }
    return;
  }
  const key = td.dataset.payrollCell;
  const label = td.dataset.payrollLabel || key;
  const panel = document.getElementById('payrollDrillPanel');
  if (!panel || !key) return;
  const a = _payrollAssumptions || {};
  const computed = (window._payrollComputed || {})[key];
  const override = (_payrollOverrides || {})[key];
  const isOv = (override !== null && override !== undefined && isFinite(parseFloat(override)));
  const displayed = isOv ? parseFloat(override) : computed;
  const adj = window._payrollAdjustments || {};
  const totalEmp = (window._payrollPrevTotals && window._payrollPrevTotals._totalEmp) || 0;
  const fD = function(v) { const n = Math.round(v||0); return (n<0?'-$':'$') + Math.abs(n).toLocaleString(); };

  // Per-cell formula builder. Each branch returns an array of {label, value}
  // breakdown lines that explain how `computed` was reached.
  const breakdown = [];
  // Compute totalEmp from positions live (the snapshot above is unreliable on first render)
  let _emp = 0;
  (_payrollPositions || []).forEach(function(p){ _emp += parseInt(p.employee_count||0,10) || 0; });
  const grossW = (window._payrollComponents)
    ? ((window._payrollComponents.annual_base||0) + (window._payrollComponents.ot||0) + (window._payrollComponents.vsh_vacation||0) + (window._payrollComponents.vsh_holiday||0) + (window._payrollComponents.vsh_sick||0))
    : 0;
  // FA dir 2026-06-04: helpers for the section-total breakdowns. Each component
  // shows its applied (override-or-computed) value so the total ties out.
  const _appliedComp = (k) => {
    const ov = (_payrollOverrides || {})[k];
    if (ov !== null && ov !== undefined && isFinite(parseFloat(ov))) return parseFloat(ov);
    return (window._payrollComputed || {})[k] || 0;
  };
  const _sumComp = (keys) => keys.reduce((s, k) => s + _appliedComp(k), 0);
  const TAX_KEYS = ['fica','sui','fui','mta','nys_disability','pfl'];
  const UNION_KEYS = ['welfare','pension','supp_retirement','legal','training','profit_sharing'];
  const COMP_LABELS = {fica:'FICA', sui:'SUI', fui:'FUI', mta:'MTA', nys_disability:'NYS Disability', pfl:'Paid Family Leave', workers_comp:'Workers Compensation', welfare:'Welfare', pension:'Pension', supp_retirement:'Supp. Retirement', legal:'Legal Fund', training:'Training Fund', profit_sharing:'Profit Sharing'};
  switch (key) {
    case 'fica':
      breakdown.push({l: 'Gross Wages',     v: fD(grossW)});
      breakdown.push({l: '× FICA Rate',     v: ((a.fica||0)*100).toFixed(3) + '%'});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'sui':
      breakdown.push({l: '$12,000 base × ' + _emp + ' employees', v: fD(12000 * _emp)});
      breakdown.push({l: '× SUI Rate',      v: ((a.sui||0)*100).toFixed(3) + '%'});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'fui':
      breakdown.push({l: '$7,000 base × ' + _emp + ' employees', v: fD(7000 * _emp)});
      breakdown.push({l: '× FUI Rate',      v: ((a.fui||0)*100).toFixed(3) + '%'});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'mta':
      breakdown.push({l: 'Gross Wages',     v: fD(grossW)});
      breakdown.push({l: '× MTA Rate',      v: ((a.mta||0)*100).toFixed(3) + '%'});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'nys_disability':
      breakdown.push({l: 'Per-employee rate', v: fD(a.nys_disability||0)});
      breakdown.push({l: '× ' + _emp + ' employees', v: ''});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'pfl':
      breakdown.push({l: 'Gross Wages',     v: fD(grossW)});
      breakdown.push({l: '× PFL Rate',      v: ((a.pfl||0)*100).toFixed(3) + '%'});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'workers_comp':
      breakdown.push({l: 'Gross Wages',     v: fD(grossW)});
      breakdown.push({l: '× Workers Comp Rate', v: ((a.workers_comp||0)*100).toFixed(3) + '%'});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'welfare':
      breakdown.push({l: '$' + (a.welfare_monthly||0).toFixed(2) + '/mo × ' + _emp + ' emp × 12 mo', v: fD((a.welfare_monthly||0) * _emp * 12)});
      if (adj.welfare) breakdown.push({l: '+ Per-position adjustments', v: fD(adj.welfare)});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'pension':
      breakdown.push({l: '$' + (a.pension_weekly||0).toFixed(2) + '/wk × ' + _emp + ' emp × 52 wk', v: fD((a.pension_weekly||0) * _emp * 52)});
      if (adj.pension) breakdown.push({l: '+ Per-position adjustments', v: fD(adj.pension)});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'supp_retirement':
      breakdown.push({l: '$' + (a.supp_retirement_weekly||0).toFixed(2) + '/wk × ' + _emp + ' emp × 52 wk', v: fD((a.supp_retirement_weekly||0) * _emp * 52)});
      if (adj.supp_retirement) breakdown.push({l: '+ Per-position adjustments', v: fD(adj.supp_retirement)});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'legal':
      breakdown.push({l: '$' + (a.legal_monthly||0).toFixed(2) + '/mo × ' + _emp + ' emp × 12 mo', v: fD((a.legal_monthly||0) * _emp * 12)});
      if (adj.legal) breakdown.push({l: '+ Per-position adjustments', v: fD(adj.legal)});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'training':
      breakdown.push({l: '$' + (a.training_monthly||0).toFixed(2) + '/mo × ' + _emp + ' emp × 12 mo', v: fD((a.training_monthly||0) * _emp * 12)});
      if (adj.training) breakdown.push({l: '+ Per-position adjustments', v: fD(adj.training)});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    case 'profit_sharing':
      breakdown.push({l: '$' + (a.profit_sharing_quarterly||0).toFixed(2) + '/qtr × ' + _emp + ' emp × 4 qtr', v: fD((a.profit_sharing_quarterly||0) * _emp * 4)});
      if (adj.profit_sharing) breakdown.push({l: '+ Per-position adjustments', v: fD(adj.profit_sharing)});
      breakdown.push({l: '= Computed',      v: fD(computed)});
      break;
    // FA dir 2026-06-04: section-total breakdowns (sum of the line items above).
    case 'total_payroll_tax':
      TAX_KEYS.forEach(k => breakdown.push({l: COMP_LABELS[k], v: fD(_appliedComp(k))}));
      breakdown.push({l: '= Total Payroll Taxes', v: fD(_sumComp(TAX_KEYS))});
      break;
    case 'total_union':
      UNION_KEYS.forEach(k => breakdown.push({l: COMP_LABELS[k], v: fD(_appliedComp(k))}));
      breakdown.push({l: '= Total Union Benefits', v: fD(_sumComp(UNION_KEYS))});
      break;
    case 'total_labor':
      breakdown.push({l: 'Gross Wages (base + OT + VSH)', v: fD(grossW)});
      breakdown.push({l: '+ Total Payroll Taxes', v: fD(_sumComp(TAX_KEYS))});
      breakdown.push({l: '+ Workers Compensation', v: fD(_appliedComp('workers_comp'))});
      breakdown.push({l: '+ Total Union Benefits', v: fD(_sumComp(UNION_KEYS))});
      breakdown.push({l: '= Total Labor & Related', v: fD(grossW + _sumComp(TAX_KEYS) + _appliedComp('workers_comp') + _sumComp(UNION_KEYS))});
      break;
    // Roster column totals — breakdown by position (uses window._payrollPosCalcs).
    case 'roster_comp': case 'roster_base': case 'roster_ot': case 'roster_vsh':
    case 'roster_weekly': case 'roster_preincr': case 'roster_postincr': case 'roster_bonus': {
      const fieldMap = {roster_comp:'comp', roster_base:'annualBase', roster_ot:'ot', roster_vsh:'vsh', roster_weekly:'weeklyPay', roster_preincr:'preIncrWages', roster_postincr:'postIncrWages', roster_bonus:'bonus'};
      const f = fieldMap[key];
      const pc = window._payrollPosCalcs || [];
      let tot = 0;
      (_payrollPositions || []).forEach((p, i) => {
        const v = (pc[i] && (pc[i][f] || 0)) || 0;
        if (Math.abs(v) > 0.5) { breakdown.push({l: (p.position_name || 'Position ' + (i+1)) + (p.employee_count ? ' (' + p.employee_count + ')' : ''), v: fD(v)}); tot += v; }
      });
      breakdown.push({l: '= ' + label, v: fD(tot)});
      break;
    }
    default:
      breakdown.push({l: 'Formula', v: '(no breakdown defined for this cell)'});
  }
  // Render the drill panel HTML
  let html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">' +
    '<div style="font-weight:700;color:#5a4a3f;font-size:14px;">🔍 Lineage · ' + label + '</div>' +
    '<button onclick="document.getElementById(\'payrollDrillPanel\').style.display=\'none\'" style="background:transparent;border:none;cursor:pointer;color:var(--gray-500);font-size:18px;line-height:1;">×</button>' +
    '</div>';
  html += '<div style="background:white;border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;font-size:12px;">';
  html += '<table style="width:100%;border-collapse:collapse;">';
  breakdown.forEach(function(b, i) {
    const isLast = (i === breakdown.length - 1);
    html += '<tr style="' + (isLast ? 'background:#f5efe7;font-weight:700;' : '') + '">' +
      '<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;">' + b.l + '</td>' +
      '<td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;text-align:right;font-variant-numeric:tabular-nums;">' + b.v + '</td>' +
      '</tr>';
  });
  html += '</table></div>';
  if (isOv) {
    html += '<div style="margin-top:10px;padding:8px 10px;background:#fef3c7;border-left:3px solid #d97706;border-radius:4px;font-size:12px;color:#92400e;">' +
      '<strong>OVR active:</strong> the FA manually set this to <strong>' + fD(override) + '</strong> ' +
      '(computed was ' + fD(computed) + '). Right-click the cell to revert.' +
      '</div>';
  }
  // Show per-position adjustment detail when relevant
  const adjVal = adj[key];
  if (adjVal && Math.abs(adjVal) > 0.5) {
    const adjPositions = [];
    (_payrollPositions || []).forEach(function(p) {
      const pa = p.benefit_adjustments;
      if (!pa || !pa.benefits || !pa.benefits[key]) return;
      const block = pa.benefits[key];
      const cnt = Math.min(parseInt(pa.adjusted_count||0,10)||0, parseInt(p.employee_count||0,10)||0);
      if (cnt <= 0) return;
      const r = parseFloat(block.rate) || 0;
      const pp = parseFloat(block.periods) || 0;
      adjPositions.push({pos: p.position_name, cnt: cnt, rate: r, periods: pp, total: r * pp * cnt});
    });
    if (adjPositions.length) {
      html += '<div style="margin-top:10px;padding:8px 10px;background:#f5efe7;border-left:3px solid #5a4a3f;border-radius:4px;font-size:12px;">' +
        '<strong>Per-position adjustments contributing:</strong>' +
        '<table style="width:100%;margin-top:6px;border-collapse:collapse;">';
      adjPositions.forEach(function(ap) {
        html += '<tr><td style="padding:3px 0;">' + ap.pos + ' (' + ap.cnt + ' adj)</td>' +
          '<td style="padding:3px 0;text-align:right;">$' + ap.rate.toFixed(2) + ' × ' + ap.periods + ' = ' + fD(ap.total) + '</td></tr>';
      });
      html += '</table></div>';
    }
  }
  panel.innerHTML = html;
  panel.style.display = 'block';
  try { panel.scrollIntoView({behavior: 'smooth', block: 'center'}); } catch (_e) {}
}

// FA directive 2026-05-17: blur on a payroll override input → POST the new value.
// Empty input → clears the override (reverts to computed).
async function payrollOverrideSave(inp) {
  if (!inp || !inp.dataset) return;
  const td = inp.closest('td');
  const span = td ? td.querySelector('.pr-total-val') : null;
  // Restore span↔input visibility
  if (span) span.style.display = '';
  inp.style.display = 'none';
  const key = inp.dataset.payrollCell;
  if (!key) return;
  const raw = (inp.value || '').trim().replace(/[$,]/g, '');
  const num = raw === '' ? null : parseFloat(raw);
  if (raw !== '' && !isFinite(num)) {
    showToast('Invalid number — not saved', 'error');
    return;
  }
  // Optimistic local update so the UI flips before the server round-trip.
  if (num === null) {
    delete _payrollOverrides[key];
  } else {
    _payrollOverrides[key] = num;
  }
  // Re-run recalc so all dependent subtotals + flash effects fire.
  if (typeof recalcPayroll === 'function') recalcPayroll();
  try {
    const resp = await fetch('/api/payroll/assumptions/' + entityCode, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({override: {key: key, value: num}}),
    });
    const data = await resp.json();
    if (!resp.ok || data.error) {
      showToast('Save failed: ' + (data.error || resp.status), 'error');
    } else {
      showToast(num === null ? 'Reverted ' + key : 'Override saved · ' + key, 'success');
    }
  } catch (e) {
    showToast('Save failed: ' + e.message, 'error');
  }
}

// FA directive 2026-05-17: right-click on an OVR'd payroll cell → revert.
function payrollRevertOverride(evt, td) {
  if (evt) evt.preventDefault();
  if (!td || !td.dataset) return false;
  const key = td.dataset.payrollCell;
  if (!key) return false;
  const ov = _payrollOverrides[key];
  if (ov === null || ov === undefined) return false;   // not overridden
  const computed = (window._payrollComputed || {})[key];
  const fD = function(v) { const n = Math.round(v||0); return (n<0?'-$':'$') + Math.abs(n).toLocaleString(); };
  if (!confirm('Revert ' + (td.dataset.payrollLabel || key) + ' to computed value (' + fD(computed) + ')?')) return false;
  delete _payrollOverrides[key];
  if (typeof recalcPayroll === 'function') recalcPayroll();
  fetch('/api/payroll/assumptions/' + entityCode, {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({override: {key: key, value: null}}),
  }).then(function(r){ return r.json(); }).then(function(d){
    if (d.error) showToast('Revert save failed: ' + d.error, 'error');
    else showToast('Reverted ' + key, 'success');
  }).catch(function(e){ showToast('Revert save failed: ' + e.message, 'error'); });
  return false;
}

// FA directive 2026-05-17: flash a payroll cell amber when its value changed
// after a roster edit. Used to make data-flow visible: edit employee_count
// and the dependent tax/benefit cells pulse to show "this is what changed."
function _payrollFlashCell(cellKey) {
  const td = document.querySelector('td[data-payroll-cell="' + cellKey + '"]');
  if (!td) return;
  const prevBg = td.style.backgroundColor || '';
  const prevTrans = td.style.transition || '';
  td.style.transition = 'background-color 0.18s ease-out';
  td.style.backgroundColor = '#fef3c7';   // amber flash
  setTimeout(function() {
    td.style.backgroundColor = prevBg;
    setTimeout(function() { td.style.transition = prevTrans; }, 250);
  }, 800);
}

// ── Render GL Detail with Expandable Groups ───────────────────────────────

const PAYROLL_GL_GROUPS = [
  {key: 'wages', label: 'Wages', glPrefixes: ['5105']},
  // FA #23 (2026-06-13): PFL (5168) belongs under Payroll Taxes, not Other Payroll.
  {key: 'payroll_taxes', label: 'Payroll Taxes', glPrefixes: ['5140','5145','5168']},
  {key: 'benefits', label: 'Benefits', glPrefixes: ['5150','5155','5160']},
  {key: 'other_payroll', label: 'Other Payroll', glPrefixes: ['5162','5165','5166','5172']}
];

// Maps GL codes to roster/assumption calc components. Mapped GLs have their
// proposed_budget driven automatically by Section 1-2 calculations; unmapped
// GLs retain the manual flat-% behavior.
const PAYROLL_COMPONENT_MAP = {
  '5105-0000': 'annual_base',      // Gross Payroll
  '5105-0010': 'ot',               // Overtime Pay
  '5105-0015': 'vsh_vacation',     // Vacation Pay (1/3 of VSH)
  '5105-0020': 'vsh_holiday',      // Holiday Pay (1/3 of VSH)
  '5105-0025': 'vsh_sick',         // Sick Pay (1/3 of VSH)
  '5105-0035': 'bonus',            // Bonus (flat $/employee × count, per position)
  '5145-0000': 'employer_taxes',   // Employer Payroll Taxes (FICA+SUI+FUI+MTA)
  '5165-0000': 'workers_comp',     // Workers Comp Insurance
  '5166-0000': 'nys_disability',   // Disability Insurance
  '5168-0000': 'pfl',              // Paid Family Leave
  // FA #24 (2026-06-15): the union-welfare calc drives HEALTH FUND (5160-0015),
  // not Health Insurance (5155-0015). 5155-0015 is now manual/unmapped.
  '5160-0015': 'welfare',          // Health Fund (union welfare calc)
  '5160-0010': 'pension',          // Pension Fund
  '5160-0020': 'supp_retirement',  // Annuity Fund
  '5160-0025': 'legal_fund',       // Legal Fund
  '5160-0030': 'training_fund',    // Training Fund
  '5160-0035': 'profit_sharing'    // Profit Sharing
};

function getPayrollGroup(glCode) {
  const prefix = (glCode || '').split('-')[0];
  for (const g of PAYROLL_GL_GROUPS) {
    if (g.glPrefixes.includes(prefix)) return g.key;
  }
  return 'other_payroll';
}

function renderPayrollGL() {
  const contentDiv = document.getElementById('prGLContent');
  if (!contentDiv) return;

  const lines = _payrollGLLines;
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const fP = v => ((v||0) * 100).toFixed(1) + '%';
  const estLbl = typeof estimateLabel === 'function' ? estimateLabel() : 'Sep-Dec Est';

  // Group lines
  const grouped = {};
  PAYROLL_GL_GROUPS.forEach(g => { grouped[g.key] = []; });
  lines.forEach(l => {
    const gk = getPayrollGroup(l.gl_code);
    grouped[gk].push(l);
  });

  let html = '<div class="prgl-scroll"><table><thead><tr>' +
    '<th class="frozen frozen-gl">GL Code</th>' +
    '<th class="frozen frozen-desc">Description</th>' +
    '<th class="num"><span class="num-box">Prior Year</span></th>' +
    '<th class="num"><span class="num-box">YTD Actual</span></th>' +
    '<th class="num"><span class="num-box">' + estLbl + '</span></th>' +
    '<th class="num"><span class="num-box">12 Mo Forecast</span></th>' +
    '<th class="num"><span class="num-box">Curr Budget</span></th>' +
    '<th class="num"><span class="num-box">Inc %</span></th>' +
    '<th class="num"><span class="num-box">Proposed</span></th>' +
    '<th class="num"><span class="num-box">$ Var</span></th>' +
    '<th class="num"><span class="num-box">% Chg</span></th>' +
    '<th>Notes</th>' +
    '</tr></thead><tbody>';

  let grandTotals = {prior:0, ytd:0, estimate:0, forecast:0, currBudget:0, proposed:0};

  PAYROLL_GL_GROUPS.forEach(g => {
    const gLines = grouped[g.key];
    if (gLines.length === 0) return;

    // Category header (clickable, spans full width, scrolls with content)
    html += '<tr class="cat-hdr" onclick="togglePrGLGroup(\'' + g.key + '\')">' +
      '<td colspan="12">' +
      '<span id="prgl_' + g.key + '_arrow" style="display:inline-block; transition:transform 0.2s; margin-right:6px; font-size:10px;">▶</span>' +
      g.label + '<span style="font-size:10px; font-weight:500; color:var(--gray-400); margin-left:8px; text-transform:none; letter-spacing:0;">' + gLines.length + ' GL lines</span>' +
      '</td></tr>';

    // Individual GL lines (hidden by default, except wages)
    let subTotals = {prior:0, ytd:0, estimate:0, forecast:0, currBudget:0, proposed:0};

    gLines.forEach(l => {
      const est = prFaComputeEstimate(l);
      const fc = prFaComputeForecast(l);
      const prop = float(l.proposed_budget || 0);
      const curr = float(l.current_budget || 0);
      const varD = prop - curr;
      const varP = curr !== 0 ? varD / curr : 0;

      subTotals.prior += float(l.prior_year);
      subTotals.ytd += float(l.ytd_actual);
      subTotals.estimate += est;
      subTotals.forecast += fc;
      subTotals.currBudget += curr;
      subTotals.proposed += prop;

      const hidden = g.key !== 'wages' ? ' style="display:none;"' : '';

      // Linked rows are auto-driven by roster — show 🔗 icon, lock Inc%, highlight Proposed
      const isLinked = !!l._linked;
      const linkIcon = isLinked ? '<span title="Driven by roster calculation" style="color:#2563eb; font-size:11px; margin-right:3px;">🔗</span>' : '';
      const pctDisabled = isLinked ? ' disabled title="Locked — driven by roster calculation"' : '';

      // Build human-readable formulas (Payroll uses simplified base — no accrual/unpaid)
      const pyr = float(l.prior_year), yta = float(l.ytd_actual);
      let estFormula;
      if (YTD_MONTHS > 0) {
        estFormula = '=' + yta + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
      } else {
        estFormula = '=0';
      }
      const fcstFormula = '=' + yta + '+' + Math.round(est);
      const componentKey = PAYROLL_COMPONENT_MAP[l.gl_code];
      // Proposed = Forecast * (1 + Increase%). Emit a REAL Excel formula with raw
      // numbers (forecast = yta + est here) instead of the word-token placeholder.
      // Linked rows show a plain descriptive tooltip (title attr only, not a formula).
      const propFormulaDisplay = isLinked
        ? ('Roster-linked (' + componentKey + ')')
        : ('=(' + yta + '+' + Math.round(est) + ')*(1+' + (l.increase_pct || 0).toFixed(4) + ')');

      // Determine override states
      const estOverride = l.estimate_override !== null && l.estimate_override !== undefined;
      const fcstOverride = l.forecast_override !== null && l.forecast_override !== undefined;
      const propHasFormula = !!(l.proposed_formula && l.proposed_formula !== 'manual');
      const propManualOverride = l.proposed_formula === 'manual';

      // Cell IDs
      const estId = 'pr_est_' + l.gl_code;
      const fcstId = 'pr_fcst_' + l.gl_code;
      const propId = 'pr_prop_' + l.gl_code;

      // Helper: build fx cell input matching R&S style (class="cell cell-fx" + top-right fx badge)
      const fxInput = (id, val, formula, field, overrideFlag, extraAttr, linkedFlag) => {
        const cellClass = linkedFlag ? 'cell cell-fx cell-fx-linked' : 'cell cell-fx';
        return '<input id="' + id + '" class="' + cellClass + '" type="text" readonly' +
          ' value="' + fD(val) + '"' +
          ' data-raw="' + Math.round(val) + '"' +
          ' data-formula="' + formula.replace(/"/g, '&quot;') + '"' +
          ' data-override="' + (overrideFlag ? 'true' : 'false') + '"' +
          (extraAttr || '') +
          ' data-gl="' + l.gl_code + '" data-field="' + field + '"' +
          ' onblur="fxCellBlur(this)"' +
          ' style="cursor:pointer; pointer-events:none;">';
      };

      // Estimate cell — always editable via formula bar.
      // FA dir 2026-05-17: stamp data-user-formula when a saved estimate_formula
      // exists so fxCellFocus repopulates the bar with the FA's expression
      // (e.g. "=300*12*4") on re-click instead of the raw number.
      const estUserFormulaAttr = (l.estimate_formula && l.estimate_formula.length)
        ? ' data-user-formula="' + l.estimate_formula.replace(/"/g, '&quot;') + '"' : '';
      const estCellHtml = '<td class="num" onclick="fxCellFocus(document.getElementById(\'' + estId + '\'))">' +
        fxInput(estId, est, estFormula, 'estimate_override', estOverride, estUserFormulaAttr) + '</td>';

      // Forecast cell — always editable via formula bar
      const fcstUserFormulaAttr = (l.forecast_formula && l.forecast_formula.length)
        ? ' data-user-formula="' + l.forecast_formula.replace(/"/g, '&quot;') + '"' : '';
      const fcstCellHtml = '<td class="num" onclick="fxCellFocus(document.getElementById(\'' + fcstId + '\'))">' +
        fxInput(fcstId, fc, fcstFormula, 'forecast_override', fcstOverride, fcstUserFormulaAttr) + '</td>';

      // Proposed cell: non-linked rows editable via formula bar; linked rows are read-only linked
      let propCellHtml;
      if (isLinked) {
        // Linked row: read-only blue-styled cell with 🔗fx badge — no click handler (not editable)
        propCellHtml = '<td class="num" title="' + propFormulaDisplay + '">' +
          '<input class="cell cell-fx cell-fx-linked" type="text" readonly value="' + fD(prop) + '" data-raw="' + Math.round(prop) + '"' +
          ' style="cursor:not-allowed; pointer-events:none;">' +
          '</td>';
      } else {
        const pfAttr = propHasFormula ? ' data-proposed-formula="' + l.proposed_formula.replace(/"/g, '&quot;') + '"' : '';
        const propOverride = propHasFormula || propManualOverride;
        propCellHtml = '<td class="num" onclick="fxCellFocus(document.getElementById(\'' + propId + '\'))">' +
          fxInput(propId, prop, propFormulaDisplay, 'proposed_budget', propOverride, pfAttr) + '</td>';
      }

      // Editable $ cell (Prior, YTD, Curr Budget) — matches R&S
      const prDollarCell = (field, val) => {
        return '<td class="num"><input class="cell pr-gl-dollar" type="text" ' +
          'data-gl="' + l.gl_code + '" data-field="' + field + '" ' +
          'value="' + fD(val) + '" data-raw="' + Math.round(val || 0) + '" ' +
          'onfocus="this.value=this.dataset.raw" onblur="prDollarCellBlur(this)"></td>';
      };

      const zeroClass = prGlIsZero(l) ? ' prgl-zero-row' : '';
      html += '<tr class="prgl-row' + zeroClass + '" data-prgroup="' + g.key + '" data-gl="' + l.gl_code + '"' + hidden + '>' +
        '<td class="frozen frozen-gl"><span style="font-size:13px; font-variant-numeric:tabular-nums; font-weight:600;">' + linkIcon + l.gl_code + '</span></td>' +
        '<td class="frozen frozen-desc">' + (l.description || '') + '</td>' +
        prDollarCell('prior_year', l.prior_year) +
        prDollarCell('ytd_actual', l.ytd_actual) +
        estCellHtml +
        fcstCellHtml +
        prDollarCell('current_budget', curr) +
        '<td class="num"><input class="cell cell-pct pr-gl-pct" data-gl="' + l.gl_code + '" value="' + fP(l.increase_pct) + '" onchange="savePrGLIncrease(this)"' + pctDisabled + '></td>' +
        propCellHtml +
        '<td class="num"><span class="num-box" style="' + (varD >= 0 ? 'color:#2563eb;' : 'color:#16a34a;') + '">' + fD(varD) + '</span></td>' +
        '<td class="num"><span class="num-box">' + (varP * 100).toFixed(1) + '%</span></td>' +
        '<td><input class="cell cell-notes pr-gl-note" type="text" data-gl="' + l.gl_code + '" value="' + (l.notes || '').replace(/"/g, '&quot;') + '" onchange="savePrGLNote(this)" placeholder="Add note..."></td>' +
        '</tr>';
    });

    // Subtotal row (frozen GL + Description cells carry the label; numeric cells scroll)
    html += '<tr class="sub-row">' +
      '<td class="frozen frozen-gl"></td>' +
      '<td class="frozen frozen-desc">Total ' + g.label + '</td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.prior) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.ytd) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.estimate) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.forecast) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.currBudget) + '</span></td>' +
      '<td></td>' +
      '<td class="num"><span class="num-box" style="font-weight:800;">' + fD(subTotals.proposed) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.proposed - subTotals.currBudget) + '</span></td>' +
      '<td class="num"><span class="num-box">' + (subTotals.currBudget ? ((subTotals.proposed - subTotals.currBudget) / subTotals.currBudget * 100).toFixed(1) + '%' : '—') + '</span></td>' +
      '<td></td>' +
      '</tr>';

    // Accumulate grand totals
    Object.keys(grandTotals).forEach(k => { grandTotals[k] += subTotals[k]; });
  });

  // FA dir 2026-05-19: Grand total row cells now use fx-td so they're
  // clickable + editable via the formula bar (same as other sheet tabs'
  // Sheet Total). Each cell has data-col so fxSubtotalFocus can build the
  // SUM formula, and onclick wired to the same handler. Saved overrides
  // are applied on render via applySubtotalOverrides.
  const _grandVar = grandTotals.proposed - grandTotals.currBudget;
  const _grandPct = grandTotals.currBudget ? (_grandVar / grandTotals.currBudget * 100) : 0;
  function _payTotalTd(val, col) {
    return '<td class="num fx-td" data-col="' + col + '" data-raw="' + Math.round(val) + '" onclick="fxSubtotalFocus(this)" style="cursor:pointer;"><span class="sub-val num-box">' + fD(val) + '</span></td>';
  }
  html += '<tr class="total-row" id="faSheetTotal">' +
    '<td class="frozen frozen-gl"></td>' +
    '<td class="frozen frozen-desc">TOTAL PAYROLL</td>' +
    _payTotalTd(grandTotals.prior, 'prior') +
    _payTotalTd(grandTotals.ytd, 'ytd') +
    _payTotalTd(grandTotals.estimate, 'estimate') +
    _payTotalTd(grandTotals.forecast, 'forecast') +
    _payTotalTd(grandTotals.currBudget, 'budget') +
    '<td></td>' +
    _payTotalTd(grandTotals.proposed, 'proposed') +
    '<td class="num fx-td" data-col="variance" data-raw="' + Math.round(_grandVar) + '" onclick="fxSubtotalFocus(this)" style="cursor:pointer;"><span class="sub-val num-box">' + fD(_grandVar) + '</span></td>' +
    '<td class="num fx-td" data-col="pctchange" data-raw="' + _grandPct.toFixed(2) + '" onclick="fxSubtotalFocus(this)" style="cursor:pointer;"><span class="sub-val num-box">' + (grandTotals.currBudget ? _grandPct.toFixed(1) + '%' : '—') + '</span></td>' +
    '<td></td>' +
    '</tr>';

  html += '</tbody></table></div>';
  contentDiv.innerHTML = html;

  // Auto-expand wages group arrow
  const wArrow = document.getElementById('prgl_wages_arrow');
  if (wArrow) wArrow.style.transform = 'rotate(90deg)';

  // Auto-size all editable cells + refresh zero-row toggle
  if (typeof prAutoSizeAll === 'function') prAutoSizeAll();
  if (typeof prUpdateZeroToggleBtn === 'function') prUpdateZeroToggleBtn();

  // Store GL total for tie-out
  window._payrollGLTotal = grandTotals.proposed;
  renderPayrollTieOut(window._payrollCalcTotal || 0);
  // FA dir 2026-05-19: apply saved subtotal overrides to TOTAL PAYROLL row.
  // Same machinery as the other FA sheet tabs — overrides live in
  // budget.assumptions_json under sheet_subtotal_overrides[faSheetTotal][col].
  if (typeof applySubtotalOverrides === 'function') {
    applySubtotalOverrides(contentDiv);
  }
}

function float(v) { return parseFloat(v) || 0; }

function togglePrGLGroup(groupKey) {
  const rows = document.querySelectorAll('tr[data-prgroup="' + groupKey + '"]');
  const arrow = document.getElementById('prgl_' + groupKey + '_arrow');
  if (!rows.length) return;
  const isHidden = rows[0].style.display === 'none';
  rows.forEach(r => { r.style.display = isHidden ? '' : 'none'; });
  if (arrow) arrow.style.transform = isHidden ? 'rotate(90deg)' : '';
}

// ── Tie-Out Bar ───────────────────────────────────────────────────────────

// Push roster-derived component values to linked GL lines.
// Updates _payrollGLLines in memory, then persists to DB via /api/fa-lines.
let _prPushSaveTimer = null;
function pushRosterToGL() {
  const comps = window._payrollComponents;
  if (!comps || !Array.isArray(_payrollGLLines)) return;
  // No roster -> no push. An empty roster computes $0 for every component;
  // pushing those zeros overwrites stored GL proposals with 0 / -100% the
  // moment the tab renders (wiped 9 lines on 437, 2026-07-03 QA).
  if (!Array.isArray(_payrollPositions) || _payrollPositions.length === 0) return;

  const savePayload = [];
  let changed = false;

  _payrollGLLines.forEach(line => {
    const componentKey = PAYROLL_COMPONENT_MAP[line.gl_code];
    if (!componentKey || comps[componentKey] === undefined) {
      line._linked = false;
      return;
    }
    // Skip rows the user has manually overridden (proposed_formula set)
    if (line.proposed_formula) {
      line._linked = false;
      return;
    }
    const newProposed = Math.round(comps[componentKey]);
    const oldProposed = Math.round(line.proposed_budget || 0);
    line._linked = true;
    line.proposed_budget = newProposed;
    // Back-calc increase_pct from curr_budget so the column stays accurate
    const curr = float(line.current_budget || 0);
    line.increase_pct = curr ? (newProposed / curr - 1) : 0;
    if (newProposed !== oldProposed) {
      changed = true;
      savePayload.push({
        gl_code: line.gl_code,
        proposed_budget: newProposed,
        increase_pct: line.increase_pct
      });
    }
  });

  // Re-render GL section to reflect updated values + linked indicators
  renderPayrollGL();

  // Debounced persist — batches changes from rapid roster edits
  if (changed && savePayload.length > 0) {
    clearTimeout(_prPushSaveTimer);
    _prPushSaveTimer = setTimeout(async () => {
      try {
        await fetch('/api/fa-lines/' + entityCode, {
          method: 'PUT',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({lines: savePayload})
        });
        window._faDataDirty = true;  // QA fix 4
      } catch(e) { console.error('Failed to save roster-linked GL values:', e); }
    }, 800);
  }
}

// Called when a Payroll dollar cell (Prior, YTD, Accrual, Unpaid, Curr Budget)
// loses focus. Parses, saves via faAutoSave, updates _payrollGLLines, and re-renders.
function prDollarCellBlur(el) {
  const raw = parseDollar(el.value);
  const rounded = Math.round(raw);
  el.dataset.raw = rounded;
  const gl = el.dataset.gl, field = el.dataset.field;
  // Save via existing fa-lines endpoint (uses accumulator)
  faAutoSave(gl, field, rounded);
  // Update in-memory line + re-render Payroll GL
  const line = _payrollGLLines.find(l => l.gl_code === gl);
  if (line) {
    line[field] = rounded;
    renderPayrollGL();
    if (window._payrollCalcTotal !== undefined) {
      renderPayrollTieOut(window._payrollCalcTotal);
    }
  }
}

// Called from formulaBarAccept when a Payroll GL cell is edited.
// Syncs the in-memory _payrollGLLines array and triggers re-render.
function payrollCellEdited(el, glCode, field) {
  const line = _payrollGLLines.find(l => l.gl_code === glCode);
  if (!line) return;
  const raw = parseFloat(el.dataset.raw) || 0;
  const overrideSet = el.dataset.override === 'true';

  if (field === 'estimate_override') {
    line.estimate_override = overrideSet ? raw : null;
  } else if (field === 'forecast_override') {
    line.forecast_override = overrideSet ? raw : null;
  } else if (field === 'proposed_budget') {
    line.proposed_budget = raw;
    // Mark as user-overridden so pushRosterToGL won't re-link
    line.proposed_formula = el.dataset.proposedFormula || 'manual';
    line._linked = false;
    // Back-calc increase_pct to keep column accurate
    const curr = float(line.current_budget || 0);
    line.increase_pct = curr ? (raw / curr - 1) : 0;
  }

  // Re-render to refresh totals and any dependent displays
  renderPayrollGL();
  if (window._payrollCalcTotal !== undefined) {
    renderPayrollTieOut(window._payrollCalcTotal);
  }
}

function renderPayrollTieOut(calcTotal) {
  const div = document.getElementById('prTieOut');
  if (!div) return;
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };

  // Break down GL total into linked (roster-driven) vs manual (flat %)
  let linkedTotal = 0;
  let manualTotal = 0;
  let linkedCount = 0;
  if (Array.isArray(_payrollGLLines)) {
    _payrollGLLines.forEach(l => {
      const prop = Math.round(l.proposed_budget || 0);
      if (l._linked) { linkedTotal += prop; linkedCount++; }
      else { manualTotal += prop; }
    });
  }
  const glTotal = linkedTotal + manualTotal;
  window._payrollGLTotal = glTotal;

  // Match: linked total should equal roster calc total (by construction)
  const linkedMatch = Math.abs(linkedTotal - calcTotal) < 1;

  div.innerHTML = '<div style="padding:16px 20px; background:linear-gradient(135deg, #eff6ff 0%, #f0f9ff 100%); border-top:2px solid #93c5fd; border-radius:0 0 10px 10px;">' +
    '<div style="display:flex; gap:20px; align-items:center; flex-wrap:wrap;">' +
      '<div onclick="togglePrLinkedBreakdown()" style="flex:1; min-width:140px; cursor:pointer; padding:8px; margin:-8px; border-radius:6px; transition:background 0.15s;" onmouseover="this.style.background=\'rgba(255,255,255,0.5)\'" onmouseout="this.style.background=\'transparent\'" title="Click to see all linked GLs">' +
        '<div style="font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.5px;">🔗 Linked GLs (Auto) <span id="prLinkedArrow" style="display:inline-block; transition:transform 0.2s; font-size:9px; margin-left:3px;">▶</span></div>' +
        '<div style="font-size:20px; font-weight:800; color:#1e40af;">' + fD(linkedTotal) + '</div>' +
        '<div style="font-size:10px; color:#3b82f6; font-style:italic;">' + linkedCount + ' GLs driven by roster — click to view</div>' +
      '</div>' +
      '<div style="font-size:24px; color:#9ca3af;">+</div>' +
      '<div style="flex:1; min-width:140px;">' +
        '<div style="font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.5px;">Manual GLs</div>' +
        '<div style="font-size:20px; font-weight:800; color:#374151;">' + fD(manualTotal) + '</div>' +
        '<div style="font-size:10px; color:var(--gray-400); font-style:italic;">Flat % applied</div>' +
      '</div>' +
      '<div style="font-size:24px; color:#9ca3af;">=</div>' +
      '<div style="flex:1; min-width:140px;">' +
        '<div style="font-size:10px; font-weight:700; color:#1f2937; text-transform:uppercase; letter-spacing:0.5px;">Total Payroll</div>' +
        '<div style="font-size:22px; font-weight:800; color:#1f2937;">' + fD(glTotal) + '</div>' +
      '</div>' +
      '<div style="margin-left:auto; text-align:right;">' +
        '<div style="font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.5px;">Roster Calc Check</div>' +
        '<div style="font-size:14px; font-weight:700; color:' + (linkedMatch ? '#059669' : '#dc2626') + ';">' + (linkedMatch ? '✓ Matches ' : '⚠ Diff: ') + fD(calcTotal) + '</div>' +
      '</div>' +
    '</div>' +
    '<div id="prLinkedBreakdown" style="display:none; margin-top:16px; padding-top:16px; border-top:1px solid #bfdbfe;">' + buildLinkedBreakdownHTML() + '</div>' +
    '</div>';
}

// Build breakdown table showing each linked GL with override controls
function buildLinkedBreakdownHTML() {
  if (!Array.isArray(_payrollGLLines)) return '';
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const linkedLines = _payrollGLLines.filter(l => l._linked);
  if (linkedLines.length === 0) {
    return '<div style="font-size:12px; color:#6b7280; font-style:italic; text-align:center; padding:12px;">No linked GLs yet — update the roster or assumptions to drive GL values.</div>';
  }

  let html = '<div style="font-size:11px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Linked GL Breakdown</div>';
  html += '<table style="width:100%; border-collapse:collapse; font-size:12px;">';
  html += '<thead><tr style="background:rgba(255,255,255,0.6);">' +
    '<th style="text-align:left; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">GL Code</th>' +
    '<th style="text-align:left; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">Description</th>' +
    '<th style="text-align:left; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">Roster Component</th>' +
    '<th style="text-align:right; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">Current Value</th>' +
    '<th style="text-align:center; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">Override</th>' +
    '</tr></thead><tbody>';

  linkedLines.forEach(l => {
    const compKey = PAYROLL_COMPONENT_MAP[l.gl_code] || '—';
    const val = Math.round(l.proposed_budget || 0);
    html += '<tr style="border-top:1px solid rgba(147,197,253,0.3);">' +
      '<td style="padding:6px 10px; font-family:monospace; font-size:11px; font-weight:600; color:#1e40af;">🔗 ' + l.gl_code + '</td>' +
      '<td style="padding:6px 10px; font-size:12px; color:#1f2937;">' + (l.description || '') + '</td>' +
      '<td style="padding:6px 10px; font-size:11px; color:#3b82f6; font-family:monospace;">' + compKey + '</td>' +
      '<td style="padding:6px 10px; text-align:right; font-weight:700; color:#1e40af; font-variant-numeric:tabular-nums;">' + fD(val) + '</td>' +
      '<td style="padding:6px 10px; text-align:center;">' +
        '<input type="text" placeholder="Enter $" data-gl="' + l.gl_code + '" ' +
          'style="width:90px; padding:3px 6px; border:1px solid #93c5fd; border-radius:4px; font-size:11px; text-align:right; background:white;" ' +
          'onkeydown="if(event.key===\'Enter\'){prOverrideLinkedGL(this);}"> ' +
        '<button onclick="prOverrideLinkedGL(this.previousElementSibling)" ' +
          'style="padding:3px 10px; font-size:10px; font-weight:600; background:#2563eb; color:white; border:none; border-radius:4px; cursor:pointer; margin-left:4px;">Override</button>' +
      '</td>' +
      '</tr>';
  });

  html += '</tbody></table>';
  html += '<div style="margin-top:8px; font-size:10px; color:#6b7280; font-style:italic;">Entering an override value unlinks the row — it will keep that value until you click Clear on the Proposed cell.</div>';
  return html;
}

// Toggle the expand/collapse of the linked GL breakdown
function togglePrLinkedBreakdown() {
  const panel = document.getElementById('prLinkedBreakdown');
  const arrow = document.getElementById('prLinkedArrow');
  if (!panel) return;
  const isShown = panel.style.display !== 'none';
  panel.style.display = isShown ? 'none' : 'block';
  if (arrow) arrow.style.transform = isShown ? '' : 'rotate(90deg)';
}

// Apply a manual override on a linked GL from the breakdown panel
async function prOverrideLinkedGL(input) {
  const gl = input.dataset.gl;
  const raw = parseDollar(input.value);
  if (!raw || isNaN(raw)) { input.focus(); return; }
  const line = _payrollGLLines.find(l => l.gl_code === gl);
  if (!line) return;
  const rounded = Math.round(raw);
  line.proposed_budget = rounded;
  line.proposed_formula = 'manual';
  line._linked = false;
  const curr = float(line.current_budget || 0);
  line.increase_pct = curr ? (rounded / curr - 1) : 0;

  // Persist to DB
  try {
    await fetch('/api/fa-lines/' + entityCode, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lines: [{
        gl_code: gl,
        proposed_budget: rounded,
        proposed_formula: 'manual',
        increase_pct: line.increase_pct
      }]})
    });
  } catch(e) { console.error('Override save failed:', e); }

  // Re-render GL + tie-out
  renderPayrollGL();
  renderPayrollTieOut(window._payrollCalcTotal || 0);
  // Re-open breakdown since render just replaced it
  const panel = document.getElementById('prLinkedBreakdown');
  if (panel) panel.style.display = 'block';
  const arrow = document.getElementById('prLinkedArrow');
  if (arrow) arrow.style.transform = 'rotate(90deg)';
}

// ── GL Note & Increase Save Helpers ───────────────────────────────────────

async function savePrGLNote(el) {
  // FA directive 2026-05-10: skip when value didn't change.
  if (_isUnchangedInput(el)) return;
  const glCode = el.dataset.gl;
  const note = el.value;
  const ec = entityCode;
  try {
    await fetch('/api/fa-lines/' + ec, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lines: [{gl_code: glCode, notes: note}]})
    });
  } catch(e) { console.error('Failed to save GL note:', e); }
}

async function savePrGLIncrease(el) {
  // FA directive 2026-05-10: skip when value didn't change.
  if (_isUnchangedInput(el)) return;
  const glCode = el.dataset.gl;
  const pctStr = el.value.replace('%', '').trim();
  const pct = parseFloat(pctStr) / 100 || 0;
  const ec = entityCode;
  const line = _payrollGLLines.find(l => l.gl_code === glCode);
  if (!line) return;
  line.increase_pct = pct;
  const curr = float(line.current_budget);
  line.proposed_budget = curr * (1 + pct);
  try {
    await fetch('/api/fa-lines/' + ec, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lines: [{gl_code: glCode, increase_pct: pct, proposed_budget: line.proposed_budget}]})
    });
    renderPayrollGL();
  } catch(e) { console.error('Failed to save GL increase:', e); }
}

// ═══════════════════════════════════════════════════════════════════════════
// END PAYROLL TAB
// ═══════════════════════════════════════════════════════════════════════════

function renderEditableSheet(sheetName, sheetLines, contentDiv) {
  const NC = 15;
  const estLbl = estimateLabel();

  // Inject PM-style CSS if not already present
  if (!document.getElementById('faSheetStyle')) {
    const style = document.createElement('style');
    style.id = 'faSheetStyle';
    style.textContent = `
      .fa-grid { background:white; border-radius:12px; border:1px solid var(--gray-200); overflow:hidden; }
      .fa-grid-scroll { overflow-x:scroll; max-height:75vh; overflow-y:auto; }
      .fa-grid-scroll::-webkit-scrollbar { width:10px; height:12px; }
      .fa-grid-scroll::-webkit-scrollbar-track { background:var(--gray-100); border-radius:6px; }
      .fa-grid-scroll::-webkit-scrollbar-thumb { background:#8b7355; border-radius:6px; min-height:40px; }
      .fa-grid-scroll::-webkit-scrollbar-thumb:hover { background:#6b5740; }
      .fa-grid-scroll::-webkit-scrollbar-corner { background:var(--gray-100); }
      .fa-grid table { border-collapse:separate; border-spacing:0; font-size:13px; width:100%; }
      /* Income sheet: hide Accrual Adj (col 5) and Unpaid Bills (col 6) — not applicable for income GLs */
      .fa-grid.fa-grid-hide-adj thead th:nth-child(5),
      .fa-grid.fa-grid-hide-adj thead th:nth-child(6),
      .fa-grid.fa-grid-hide-adj tbody tr:not(.cat-hdr):not(.anc-drawer-row) td:nth-child(5),
      .fa-grid.fa-grid-hide-adj tbody tr:not(.cat-hdr):not(.anc-drawer-row) td:nth-child(6) { display:none !important; }
      .fa-grid thead { position:sticky; top:0; z-index:20; }
      .fa-grid th { padding:8px 6px; text-align:left; font-weight:600; border-bottom:2px solid var(--gray-300); white-space:nowrap; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); background:var(--gray-100); }
      .fa-grid th.num { text-align:right; }
      .fa-grid td, .fa-grid th { white-space:nowrap; width:1px; }
      .fa-grid td { padding:6px 6px; border-bottom:1px solid var(--gray-200); }
      .fa-grid td.num { text-align:right; font-variant-numeric:tabular-nums; }
      .fa-grid tbody tr:hover td { background:#eef2ff; }
      .fa-grid tbody tr:hover td.frozen { background:#ede5d8; }
      .fa-grid th.frozen, .fa-grid td.frozen { position:sticky; z-index:15; background:white; }
      .fa-grid thead th.frozen { z-index:25; background:var(--gray-100); }
      .fa-grid .frozen-gl { left:0; min-width:80px; }
      .fa-grid .frozen-desc { left:80px; min-width:180px; width:auto; border-right:2px solid var(--gray-300); box-shadow:2px 0 8px rgba(90,74,63,0.08); }
      .fa-grid thead th.frozen.frozen-desc { width:auto; min-width:180px; }
      .fa-grid .col-notes { color:var(--gray-500); font-size:12px; min-width:40px; text-align:center; }
      .fa-grid .cat-hdr td { background:var(--blue-light, #f5efe7); font-weight:700; color:var(--blue, #5a4a3f); font-size:14px; padding:10px 10px; border-bottom:2px solid var(--blue, #5a4a3f); }
      .fa-grid .cat-hdr td.frozen { background:var(--blue-light, #f5efe7); }
      .fa-grid .sub-row td { background:var(--gray-100); font-weight:700; border-top:2px solid var(--gray-300); }
      .fa-grid .sub-row td.frozen { background:var(--gray-100); }
      .fa-grid .total-row td { background:#1e3a5f; color:white; font-weight:700; font-size:14px; }
      .fa-grid .total-row td.frozen { background:#1e3a5f; color:white; }
      .fa-grid tr.drill-row td.frozen { border-right:none; box-shadow:none; }
      .fa-grid .cell { min-width:50px; width:auto; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; text-align:right; background:#fbfaf4; cursor:text; }
      /* Jacob 2026-06-10: the shrink-to-fit columns size to the header text,
         and "MAY-DEC EST" is short — six-figure estimates clipped ($99,31).
         Give estimate inputs room for $9,999,999. */
      .fa-grid input[data-field="estimate_override"] { min-width:92px; }
      .fa-grid .cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px var(--blue-light, #f5efe7); }
      .fa-grid .cell-fx { background:transparent; border-color:#e5e1d8; box-shadow:inset 3px 0 0 #16a34a; color:#15803d; }
      .fa-grid .cell-fx:focus { background:#ecfdf5; }
      .fa-fx { display:none !important; }
      .fa-grid .sub-row td.fx-td { background:#e8f5e9; }
      .fa-grid .sub-row td.fx-td .sub-val { color:#1b5e20; }
      .fa-grid .total-row td.fx-td { background:#1a3d2e; }
      .fa-grid .total-row td.fx-td .sub-val { color:#a5d6a7; }
      .fa-grid .cell-notes { text-align:left; min-width:100px; width:100%; }
      .fa-grid .cell-pct { min-width:45px; width:auto; }
      .fa-invoice-detail > td { padding:0 !important; }
      .fa-invoice-detail:hover { background:transparent !important; }
      .fa-invoice-detail .drill-sticky, .fa-grid .drill-sticky { position:sticky; left:220px; z-index:10; width:fit-content; min-width:850px; }
      .fa-controls { display:flex; justify-content:space-between; align-items:center; padding:12px 16px; background:white; border-radius:12px; border:1px solid var(--gray-200); margin-bottom:12px; }
      .fa-legend { display:flex; gap:14px; font-size:11px; color:var(--gray-500); align-items:center; flex-wrap:wrap; }
      .fa-legend-dot { display:inline-block; width:10px; height:10px; border-radius:2px; vertical-align:middle; margin-right:3px; border:1px solid var(--gray-300); }
      .fa-reclass-overlay { position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.4); display:flex; align-items:center; justify-content:center; z-index:9999; }
      .fa-reclass-modal { background:white; border-radius:12px; width:560px; max-height:80vh; display:flex; flex-direction:column; box-shadow:0 20px 60px rgba(0,0,0,0.3); }
      .fa-reclass-modal .rm-header { padding:16px 20px; border-bottom:1px solid var(--gray-200); display:flex; justify-content:space-between; align-items:center; }
      .fa-reclass-modal .rm-header h3 { font-size:15px; font-weight:700; color:var(--blue); }
      .fa-reclass-modal .rm-search { padding:12px 20px; border-bottom:1px solid var(--gray-200); }
      .fa-reclass-modal .rm-search input { width:100%; padding:8px 12px; border:1px solid var(--gray-300); border-radius:6px; font-size:13px; outline:none; }
      .fa-reclass-modal .rm-search input:focus { border-color:var(--blue); box-shadow:0 0 0 3px rgba(90,74,63,0.08); }
      .fa-reclass-modal .rm-list { flex:1; overflow-y:auto; max-height:400px; }
      .fa-reclass-modal .rm-cat-header { padding:6px 20px; font-size:11px; font-weight:700; text-transform:uppercase; color:var(--blue); background:var(--blue-light); position:sticky; top:0; }
      .fa-reclass-modal .rm-gl-row { padding:8px 20px; cursor:pointer; display:flex; justify-content:space-between; align-items:center; font-size:13px; border-bottom:1px solid var(--gray-100); }
      .fa-reclass-modal .rm-gl-row:hover { background:var(--blue-light); }
      .fa-reclass-modal .rm-gl-row .gl-code { font-family:monospace; font-weight:600; min-width:90px; }
      .fa-reclass-modal .rm-gl-row .gl-desc { flex:1; color:var(--gray-700); }
      .fa-reclass-modal .rm-footer { padding:12px 20px; border-top:1px solid var(--gray-200); display:flex; gap:8px; justify-content:flex-end; }

      /* ── Ancillary backup drawer ───────────────────────── */
      .fa-grid .anc-expand-icon { display:inline-block; width:16px; height:16px; line-height:15px; text-align:center; border-radius:3px; background:var(--blue, #2563eb); color:white; font-weight:700; font-size:12px; margin-right:6px; cursor:pointer; user-select:none; vertical-align:middle; }
      .fa-grid .anc-expand-icon:hover { background:#1d4ed8; }
      .fa-grid tr.anc-drawer-row td { padding:0 !important; background:#f9fafb; border-bottom:2px solid #cbd5e1; box-shadow:inset 4px 0 0 var(--blue, #2563eb); }
      .fa-grid tr.anc-drawer-row:hover td { background:#f9fafb !important; }
      .fa-grid .anc-drawer { padding:16px 20px 20px 36px; }
      .fa-grid .anc-drawer h3 { font-size:13px; margin:0 0 12px 0; color:var(--text, #0f172a); font-weight:700; display:flex; align-items:center; gap:8px; }
      .fa-grid .anc-drawer .anc-gl-small { color:var(--gray-500); font-weight:500; font-family:"SF Mono", Consolas, monospace; font-size:11px; }
      .fa-grid .anc-compare-strip { display:flex; gap:10px; margin-bottom:12px; flex-wrap:wrap; }
      .fa-grid .anc-compare-cell { background:white; border:1px solid var(--gray-200); border-radius:6px; padding:8px 12px; min-width:130px; flex:1; }
      .fa-grid .anc-compare-cell .anc-label { font-size:10px; text-transform:uppercase; letter-spacing:0.04em; color:var(--gray-500); font-weight:600; margin-bottom:2px; }
      .fa-grid .anc-compare-cell .anc-value { font-size:14px; font-weight:700; font-variant-numeric:tabular-nums; text-align:right; }
      .fa-grid .anc-compare-cell.highlight { background:#dbeafe; border-color:var(--blue, #2563eb); }
      .fa-grid .anc-compare-cell.highlight .anc-value { color:var(--blue, #2563eb); }
      .fa-grid .anc-compare-cell.ok { background:#dcfce7; border-color:#16a34a; }
      .fa-grid .anc-compare-cell.ok .anc-value { color:#15803d; }
      .fa-grid .anc-compare-cell.drift { background:#fef3c7; border-color:#f59e0b; }
      .fa-grid .anc-compare-cell.drift .anc-value { color:#92400e; }
      .fa-grid .anc-compare-cell .anc-hint { font-size:10px; color:var(--gray-500); margin-top:2px; text-align:right; }
      .fa-grid table.anc-lines { width:100%; border-collapse:collapse; background:white; border:1px solid var(--gray-200); border-radius:6px; overflow:hidden; margin-bottom:10px; }
      .fa-grid table.anc-lines th { font-size:10px; text-transform:uppercase; letter-spacing:0.03em; color:var(--gray-500); border-bottom:1px solid var(--gray-200); padding:8px 10px; text-align:left; font-weight:700; background:#fafbfc; }
      .fa-grid table.anc-lines th.num { text-align:right; }
      .fa-grid table.anc-lines td { padding:6px 10px; border-bottom:1px solid var(--gray-200); vertical-align:middle; white-space:normal; }
      .fa-grid table.anc-lines tr:last-child td { border-bottom:none; }
      .fa-grid table.anc-lines td.num { text-align:right; font-variant-numeric:tabular-nums; }
      .fa-grid table.anc-lines input[type="text"] { width:100%; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; background:#fffbeb; font-family:inherit; font-variant-numeric:tabular-nums; }
      .fa-grid table.anc-lines input.num-input { text-align:right; }
      .fa-grid table.anc-lines input:focus { outline:none; border-color:var(--blue, #2563eb); box-shadow:0 0 0 2px #dbeafe; }
      .fa-grid table.anc-lines select { width:100%; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:12px; background:#fffbeb; font-family:inherit; }
      .fa-grid table.anc-lines .anc-line-total { font-weight:600; color:var(--text, #0f172a); }
      .fa-grid table.anc-lines .anc-remove-btn { background:none; border:none; color:var(--gray-500); cursor:pointer; font-size:14px; padding:2px 6px; border-radius:3px; }
      .fa-grid table.anc-lines .anc-remove-btn:hover { color:#dc2626; background:#fee2e2; }
      .fa-grid table.anc-lines tr.anc-total-row td { background:#f1f5f9; border-top:2px solid var(--text, #0f172a); font-weight:700; font-size:14px; }
      .fa-grid .anc-actions { display:flex; justify-content:space-between; align-items:center; gap:10px; margin-bottom:6px; }
      .fa-grid .anc-add-btn { background:var(--blue, #2563eb); color:white; border:none; padding:6px 12px; border-radius:4px; font-size:12px; font-weight:600; cursor:pointer; }
      .fa-grid .anc-add-btn:hover { background:#1d4ed8; }
      .fa-grid .anc-sync-btn { background:#16a34a; color:white; border:none; padding:8px 16px; border-radius:4px; font-size:13px; font-weight:600; cursor:pointer; }
      .fa-grid .anc-sync-btn:hover { background:#15803d; }
      .fa-grid .anc-sync-btn.in-sync { background:#f1f5f9; color:var(--gray-500); cursor:default; }
      .fa-grid .anc-hint { font-size:11px; color:var(--gray-500); }
      .fa-grid .anc-hint code { background:#f1f5f9; padding:1px 4px; border-radius:3px; font-size:10px; }
      @media print { .fa-grid tr.anc-drawer-row, .fa-grid .anc-expand-icon { display:none !important; } }
    `;
    document.head.appendChild(style);
  }

  let html = '<div class="fa-controls"><div class="fa-legend">' +
    '<span><span class="fa-legend-dot" style="background:#fbfaf4;"></span>Editable</span>' +
    '<span><span class="fa-legend-dot" style="background:#f0fdf4; border-color:#bbf7d0;"></span>Calculated (click to see formula)</span>' +
    '</div><div style="display:flex; gap:8px;"><button id="faZeroToggle" onclick="faToggleZeroRows()" style="font-size:11px; padding:4px 12px; background:var(--blue-light, #dbeafe); color:var(--blue); border:1px solid var(--blue); border-radius:4px; cursor:pointer;"></button></div></div>';

  // Formula bar — Excel-style with live preview + Accept/Cancel
  // Sticky positioning so it stays visible as the user scrolls long sheets
  // (Income/R&S/Gen&Admin can be hundreds of rows). Mirrors the Payroll tab's
  // sticky formula bar at line 13201.
  html += '<div id="faFormulaBarWrap" style="position:sticky; top:0; z-index:50; display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200); border-radius:8px; margin-bottom:12px; box-shadow:0 2px 4px rgba(0,0,0,0.04);">' +
    '<span style="font-size:11px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>' +
    '<span id="faFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:var(--gray-600); white-space:nowrap; min-width:100px;"></span>' +
    '<input id="faFormulaBar" type="text" placeholder="Click a green formula cell to view its formula..." style="display:block; flex:1; padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:monospace; background:white;" oninput="formulaBarPreview()" onkeydown="formulaBarKeydown(event)">' +
    '<span id="faFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:var(--green); white-space:nowrap; min-width:80px; text-align:right;"></span>' +
    '<button id="faFormulaAccept" style="display:none; padding:4px 14px; font-size:12px; font-weight:600; background:var(--green); color:white; border:none; border-radius:4px; cursor:pointer;" onclick="formulaBarAccept()">Accept</button>' +
    '<button id="faFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:var(--gray-200); color:var(--gray-700); border:none; border-radius:4px; cursor:pointer;" onclick="formulaBarCancel()">Cancel</button>' +
    '<button id="faFormulaClear" style="display:none; padding:4px 10px; font-size:11px; background:#fef2f2; color:var(--red); border:1px solid #fecaca; border-radius:4px; cursor:pointer;" onclick="formulaBarClear()" title="Remove formula, revert to auto-calc">Clear</button>' +
    '<button id="faFormulaUndo" style="display:none; padding:4px 10px; font-size:11px; background:#fff7ed; color:#c2410c; border:1px solid #fed7aa; border-radius:4px; cursor:pointer;" onclick="formulaBarUndo()" title="Undo the last accepted formula change">↶ Undo</button>' +
    // FA dir 2026-05-19: per-tab Undo + History controls. Visible on every
    // sheet tab. Scoped to the active sheet via `sheet=` query param.
    '<span style="display:inline-block; width:1px; height:22px; background:var(--gray-300); margin:0 4px;"></span>' +
    '<button class="fa-tab-undo-btn" onclick="faTabUndoLast()" title="Restore the most recent change on this tab" style="padding:4px 10px; font-size:11px; background:white; color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer; font-weight:600; white-space:nowrap;">↩ Undo last</button>' +
    '<button class="fa-tab-hist-btn" onclick="faTabShowHistory()" title="See the last 50 changes on this tab" style="padding:4px 10px; font-size:11px; background:white; color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer; font-weight:600; white-space:nowrap;">⏱ History</button>' +
    '</div>';

  const _hideAdj = (sheetName === 'Income') ? ' fa-grid-hide-adj' : '';
  html += '<div class="fa-grid' + _hideAdj + '"><div class="fa-grid-scroll"><table><thead><tr>' +
    '<th class="frozen frozen-gl">GL Code</th><th class="frozen frozen-desc">Description</th>' +
    '<th class="num">Prior Year</th><th class="num">YTD Actual</th>' +
    '<th class="num">Accrual Adj</th><th class="num">Unpaid Bills</th>' +
    '<th class="num">' + estLbl + ' Est</th><th class="num">12 Mo Forecast</th>' +
    '<th class="num">Curr Budget</th><th class="num">Inc %</th>' +
    '<th class="num">Proposed</th><th class="num">$ Var</th><th class="num">% Chg</th>' +
    '<th class="col-notes">Notes</th>' +
    '</tr></thead><tbody>';

  const catConfig = SHEET_CATEGORIES[sheetName];

  // Single source of the default-proposed rule. buildLineRow (cells) and
  // sumLines (sheet/category totals) MUST use this same function or totals
  // silently include values no cell shows (724 #15: two zero-budget upfront
  // insurance premiums annualized x3 into the Sheet Total only).
  // FA #26/#25 (2026-06-15): R+M and Gen&Admin propose off the 2026 budget;
  // Payroll Processing (5172) = 2026 budget x 1.03; others forecast-based.
  function faDefaultProposed(l) {
    const budget = l.current_budget || 0;
    if (l.gl_code === '5172-0000') return budget * 1.03;
    if (l.sheet_name === 'Repairs & Supplies' || l.sheet_name === 'Gen & Admin') {
      return budget * (1 + (l.increase_pct || 0));
    }
    return faComputeForecast(l) * (1 + (l.increase_pct || 0));
  }

  function buildLineRow(l) {
    const gl = l.gl_code;
    const prior = l.prior_year || 0;
    const ytd = l.ytd_actual || 0;
    const accrual = l.accrual_adj || 0;
    const unpaid = l.unpaid_bills || 0;
    const budget = l.current_budget || 0;
    const isZero = !prior && !ytd && !accrual && !unpaid && !budget && !(l.increase_pct);
    const estimate = faComputeEstimate(l);
    const forecast = faComputeForecast(l);
    const userFormula = l.proposed_formula || '';
    // FA #26/#25 (2026-06-15): R+M and Gen&Admin propose off the 2026 budget,
    // not the 12-mo forecast; Payroll Processing (5172) = 2026 budget × 1.03.
    // Other expenses keep forecast×(1+incr). An explicit proposed_budget always
    // wins (handled by the `l.proposed_budget ||` below).
    const defaultProposed = faDefaultProposed(l);
    let proposed;
    if (userFormula) {
      const evalResult = safeEvalFormula(userFormula);
      proposed = evalResult !== null ? evalResult : (l.proposed_budget || defaultProposed);
    } else {
      proposed = l.proposed_budget || defaultProposed;
    }
    // FA rule (mirrors sumLines, _aggregate_by_prefix, and the Summary's capital
    // row): Capital lines have NO proposed budget. Force the line cell to 0 so it
    // matches the Sheet Total proposed (which already zeros capital) — otherwise
    // the total reads 0 while the line cells show forecast×inc and don't add up.
    if (l.sheet_name === 'Capital' || (l.category || '').toLowerCase() === 'capital') {
      proposed = 0;
    }
    // FA 2026-06-17 (B1/B4): never-budgeted income (prepaid / dividend /
    // messenger) shows $0 proposed, matching the Summary + Sheet Total.
    if (l.no_budget) {
      proposed = 0;
    }
    // FA dir 2026-06-05: $ Var / % Chg compare PROPOSED to Current Budget (the
    // change the FA is making to the budget), not the old Excel budget-vs-forecast
    // parity — that read as unrelated noise sitting next to the Proposed column.
    const variance = proposed - budget;
    const pctChange = budget ? ((proposed - budget) / budget) : 0;
    const incPct = ((l.increase_pct || 0) * 100).toFixed(1);
    const varColor = variance >= 0 ? 'var(--red)' : 'var(--green)';
    const reclassBadge = l.reclass_to_gl ? ' <span style="background:var(--orange-light); color:var(--orange); font-size:10px; padding:1px 5px; border-radius:8px;">R</span>' : '';
    const oneTimeBadge = faIsOneTimeFeeBilled(l) ? ' <span title="One-time annual fee — forecast = YTD only" style="background:#ffedd5; color:#ea580c; font-size:10px; font-weight:700; padding:2px 6px; border-radius:8px; border:1px solid #fdba74; letter-spacing:0.5px; cursor:help;">1×</span>' : '';

    const estFormula = faGetFormulaTooltip(l, 'estimate');
    const fcstFormula = faGetFormulaTooltip(l, 'forecast');
    const propFormula = faGetFormulaTooltip(l, 'proposed');

    // Dollar cell: shows $1,234 normally, raw number on focus for editing
    function $cell(id, field, val) {
      return '<input id="' + id + '" class="cell" type="text"' +
        ' value="' + fmt(val) + '"' +
        ' data-raw="' + Math.round(val) + '"' +
        ' data-gl="' + gl + '" data-field="' + field + '"' +
        ' onfocus="this.value=this.dataset.raw"' +
        ' onblur="cellBlur(this)">';
    }
    // Formula cell: shows $1,234, clicking opens formula in the formula bar at top
    function fxCell(id, field, val, formula, isOverride, proposedFormula, pinned, infoTitle) {
      const hasUserFormula = field === 'proposed_budget' && proposedFormula;
      const overrideAttr = (isOverride || hasUserFormula) ? 'true' : 'false';
      let badge;
      if (hasUserFormula) {
        badge = '<span class="fa-fx" style="background:#dbeafe; color:var(--blue); border-color:var(--blue);">fx</span>';
      } else if (isOverride) {
        badge = '<span class="fa-fx" style="background:#f97316; color:#fff; border-color:#ea580c;">✎</span>';
      } else {
        badge = '<span class="fa-fx">fx</span>';
      }
      const pfAttr = proposedFormula ? ' data-proposed-formula="' + proposedFormula.replace(/"/g, '&quot;') + '"' : '';
      // Task #99: tint + explain forecast cells pinned to budget (fully collectible income)
      const pinBg = pinned ? 'background:#ecfdf5;' : '';
      const pinTtl = pinned ? ' title="Forecast pinned to approved budget — fully collectible income (ties to the Summary tab)"'
                    : (infoTitle ? ' title="' + infoTitle + '"' : '');
      // QA fix 9 (2026-07-03): visible marker for cells whose edits feed the
      // export/board detail but NOT the Summary row (income truth flows
      // Summary -> tab, not tab -> Summary).
      const infoIcon = (!pinned && infoTitle) ? '<span style="position:absolute; top:1px; right:3px; font-size:9px; color:#b45309; cursor:help;">ⓘ</span>' : '';
      return '<td class="num"' + pinTtl + ' style="position:relative; cursor:pointer;' + pinBg + '" onclick="fxCellFocus(document.getElementById(\'' + id + '\'))">' + badge + infoIcon +
        '<input id="' + id + '" class="cell cell-fx" type="text" readonly' +
        ' value="' + fmt(val) + '"' +
        ' data-raw="' + Math.round(val) + '"' +
        ' data-formula="' + formula.replace(/"/g, '&quot;') + '"' +
        ' data-override="' + overrideAttr + '"' +
        pfAttr +
        ' data-gl="' + gl + '" data-field="' + field + '"' +
        ' onblur="fxCellBlur(this)"' +
        ' style="cursor:pointer; pointer-events:none;"></td>';
    }

    // Ancillary backup expand icon (only on Income sheet for eligible GL prefixes)
    const isAnc = (sheetName === 'Income') && _isAncillaryGl(gl);
    const ancExpanded = isAnc && _ancExpanded.has(gl);
    const ancIcon = isAnc
      ? '<span id="anc_icon_' + gl + '" class="anc-expand-icon" onclick="ancToggleDrawer(\'' + gl + '\', event)" title="Open backup worksheet">' + (ancExpanded ? '−' : '+') + '</span>'
      : '';

    const mainRow = '<tr data-gl="' + gl + '" class="' + (isZero ? 'zero-row' : '') + '"' + (isZero && !_faShowZeroRows ? ' style="display:none;"' : '') + '>' +
      '<td class="frozen frozen-gl">' + ancIcon + '<span style="font-size:13px; font-variant-numeric:tabular-nums;">' + gl + '</span>' + reclassBadge + '</td>' +
      '<td class="frozen frozen-desc"><a href="#" onclick="faToggleInvoices(\'' + gl + '\', this); return false;" style="color:inherit; text-decoration:none; cursor:pointer;" title="Click to view expenses">' + l.description + ' <span class="fa-drill-arrow" style="font-size:10px; color:var(--gray-400);">▶</span></a>' + oneTimeBadge + '</td>' +
      '<td class="num">' + $cell('pr_'+gl, 'prior_year', prior) + '</td>' +
      '<td class="num">' + $cell('ytd_'+gl, 'ytd_actual', ytd) + '</td>' +
      '<td class="num">' + $cell('acc_'+gl, 'accrual_adj', accrual) + '</td>' +
      '<td class="num">' + $cell('unp_'+gl, 'unpaid_bills', unpaid) + '</td>' +
      fxCell('est_'+gl, 'estimate_override', estimate, estFormula, l.estimate_override !== null && l.estimate_override !== undefined) +
      fxCell('fcst_'+gl, 'forecast_override', forecast, fcstFormula, l.forecast_override !== null && l.forecast_override !== undefined, undefined, faIsIncomePinned(l)) +
      '<td class="num">' + $cell('bud_'+gl, 'current_budget', budget) + '</td>' +
      '<td class="num"><input id="inc_'+gl+'" class="cell cell-pct" type="text" value="'+incPct+'%" data-raw="'+incPct+'" data-gl="'+gl+'" data-field="increase_pct" onfocus="this.value=this.dataset.raw" onblur="pctCellBlur(this)"></td>' +
      fxCell('prop_'+gl, 'proposed_budget', proposed, propFormula, false, userFormula, false,
             (sheetName === 'Income' ? 'Income note: the Summary tab sets the income rows independently. This line feeds the Excel export and the board document detail, not the Summary income row.' : '')) +
      '<td class="num" style="position:relative; cursor:pointer; color:'+varColor+';" onclick="fxCellFocus(document.getElementById(\'var_'+gl+'\'))">' +
        '<span class="fa-fx">fx</span>' +
        '<input id="var_'+gl+'" class="cell cell-fx" type="text" readonly' +
        ' value="' + fmt(variance) + '"' +
        ' data-raw="' + Math.round(variance) + '"' +
        ' data-formula="' + (sumExcelExpr([proposed, -budget]) || '=0') + '"' +
        ' data-gl="' + gl + '" data-field="variance"' +
        ' style="cursor:pointer; pointer-events:none; color:'+varColor+';"></td>' +
      '<td class="num" style="position:relative; cursor:pointer;" onclick="fxCellFocus(document.getElementById(\'pct_'+gl+'\'))">' +
        '<span class="fa-fx">fx</span>' +
        '<input id="pct_'+gl+'" class="cell cell-fx" type="text" readonly' +
        ' value="' + (pctChange*100).toFixed(1) + '%"' +
        ' data-raw="' + pctChange + '"' +
        ' data-formula="' + (Math.round(budget) ? ('=(' + (sumExcelExpr([proposed, -budget]) || '=0').slice(1) + ')/' + Math.round(budget)) : '=0') + '"' +
        ' data-gl="' + gl + '" data-field="pct_change"' +
        ' style="cursor:pointer; pointer-events:none;"></td>' +
      '<td class="col-notes"><input class="cell cell-notes" type="text" value="' + (l.notes||'').replace(/"/g,'&quot;') + '" data-gl="' + gl + '" data-field="notes" onchange="faAutoSave(\'' + gl + '\',\'notes\',this.value)"></td></tr>';

    // Ancillary drawer row (hidden by default; filled on expand)
    const ancDrawerRow = isAnc
      ? '<tr class="anc-drawer-row" data-anc-drawer="' + gl + '"' + (ancExpanded ? '' : ' style="display:none;"') + '><td colspan="' + NC + '">' + (ancExpanded ? ancRenderDrawer(gl) : '') + '</td></tr>'
      : '';

    return mainRow + ancDrawerRow;
  }

  function sumLines(lines) {
    const t = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
    lines.forEach(l => {
      t.prior += l.prior_year || 0;
      t.ytd += l.ytd_actual || 0;
      t.accrual += l.accrual_adj || 0;
      t.unpaid += l.unpaid_bills || 0;
      t.estimate += faComputeEstimate(l);
      t.forecast += faComputeForecast(l);
      t.budget += l.current_budget || 0;
      // FA directive 2026-05-05: Capital — no proposed budget. FA 2026-06-17
      // (B1/B4): never-budgeted income also contributes 0.
      const isCap = (l.sheet_name === 'Capital' || (l.category || '').toLowerCase() === 'capital');
      t.proposed += (isCap || l.no_budget) ? 0 : (l.proposed_budget || faDefaultProposed(l));
    });
    return t;
  }

  function subtotalRow(label, t, cls, rowId) {
    const v = t.proposed - t.budget;
    const p = t.budget ? ((t.proposed - t.budget)/t.budget) : 0;
    const idAttr = rowId ? ' id="' + rowId + '"' : '';
    const isTotal = cls === 'total-row';
    const bs = isTotal ? 'background:rgba(255,255,255,0.2); color:white; border-color:rgba(255,255,255,0.4);' : '';
    function fxTd(val, col) {
      return '<td class="num fx-td" style="position:relative; cursor:pointer;" data-col="' + col + '" data-raw="' + Math.round(val) + '" onclick="fxSubtotalFocus(this)">' +
        '<span class="sub-val">' + fmt(val) + '</span></td>';
    }
    const vc = v >= 0 ? 'var(--red)' : 'var(--green)';
    return '<tr class="' + (cls||'sub-row') + '"' + idAttr + '>' +
      '<td class="frozen frozen-gl"></td><td class="frozen frozen-desc">' + label + '</td>' +
      fxTd(t.prior, 'prior') +
      fxTd(t.ytd, 'ytd') +
      fxTd(t.accrual, 'accrual') +
      fxTd(t.unpaid, 'unpaid') +
      fxTd(t.estimate, 'estimate') +
      fxTd(t.forecast, 'forecast') +
      fxTd(t.budget, 'budget') +
      '<td class="num"></td>' +
      fxTd(t.proposed, 'proposed') +
      '<td class="num fx-td" style="position:relative; cursor:pointer; color:' + vc + ';" data-col="variance" data-raw="' + Math.round(v) + '" onclick="fxSubtotalFocus(this)"><span class="sub-val">' + fmt(v) + '</span></td>' +
      '<td class="num fx-td" style="position:relative; cursor:pointer;" data-col="pctchange" data-raw="' + p + '" onclick="fxSubtotalFocus(this)"><span class="sub-val">' + (p*100).toFixed(1) + '%</span></td>' +
      '<td class="col-notes"></td></tr>';
  }

  // Build category groups and populate _catGroupGLs for live recalculation
  window._catGroupGLs = {};
  if (catConfig) {
    catConfig.groups.forEach(grp => {
      const gl = sheetLines.filter(grp.match);
      if (gl.length === 0) return;
      window._catGroupGLs[grp.key] = gl.map(l => l.gl_code);
      html += '<tr class="cat-hdr"><td class="frozen frozen-gl"></td><td class="frozen frozen-desc">' + grp.label + '</td><td colspan="' + (NC - 2) + '"></td></tr>';
      gl.forEach(l => { html += buildLineRow(l); });
      html += subtotalRow('Total ' + grp.label, sumLines(gl), null, 'subtotal_' + grp.key);
    });
    const allGrouped = catConfig.groups.flatMap(g => sheetLines.filter(g.match));
    const ungrouped = sheetLines.filter(l => !allGrouped.includes(l));
    if (ungrouped.length > 0) {
      window._catGroupGLs['other'] = ungrouped.map(l => l.gl_code);
      html += '<tr class="cat-hdr"><td class="frozen frozen-gl"></td><td class="frozen frozen-desc" style="color:var(--gray-500); border-color:var(--gray-300);">Other</td><td colspan="' + (NC - 2) + '"></td></tr>';
      ungrouped.forEach(l => { html += buildLineRow(l); });
      html += subtotalRow('Total Other', sumLines(ungrouped), null, 'subtotal_other');
    }
  } else {
    sheetLines.forEach(l => { html += buildLineRow(l); });
  }

  html += subtotalRow('Sheet Total', sumLines(sheetLines), 'total-row', 'faSheetTotal');
  html += '</tbody></table></div></div>';
  contentDiv.innerHTML = html;
  // Auto-size numeric columns after render
  autoSizeColumns(contentDiv.querySelector('table'));
  // FA dir 2026-05-19: apply saved subtotal/sheet-total overrides on top of
  // the freshly-rendered computed values. Async fetch — overrides paint in
  // once they arrive (no blocking).
  applySubtotalOverrides(contentDiv);
}

// FA dir 2026-05-19: apply saved subtotal overrides to the rendered table.
// Override values are stored per (row_id, col); replacing the displayed sum
// with the FA's locked-in number. Visual indicator: blue + bold so the FA
// can tell it's overridden, with a hover-title showing the original sum.
async function applySubtotalOverrides(container) {
  try {
    const resp = await fetch('/api/sheet-subtotal-override/' + encodeURIComponent(entityCode));
    if (!resp.ok) return;
    const data = await resp.json();
    const overrides = data.overrides || {};
    for (const rowId in overrides) {
      const row = container.querySelector('tr#' + CSS.escape(rowId));
      if (!row) continue;
      const cells = row.querySelectorAll('td.fx-td');
      for (const td of cells) {
        const col = td.dataset.col;
        if (!col) continue;
        const val = overrides[rowId][col];
        const formula = overrides[rowId][col + '__formula'];
        if (val === undefined || val === null) continue;
        const span = td.querySelector('.sub-val');
        if (span) {
          const original = parseFloat(td.dataset.raw) || 0;
          span.textContent = fmt(val);
          span.style.color = 'var(--blue, #1d4ed8)';
          span.style.fontWeight = '700';
          span.title = 'FA override (computed sum was ' + fmt(original) + ')';
        }
        td.dataset.overrideValue = String(val);
        if (formula) td.dataset.overrideFormula = formula;
      }
    }
  } catch (_e) { /* silent — overrides are non-critical */ }
}

/* ── Grid Viewport Fit — keep horizontal scrollbar visible ────────── */
function faFitGridToViewport() {
  const gs = document.querySelector('.fa-grid-scroll');
  if (!gs) return;
  const rect = gs.getBoundingClientRect();
  const available = window.innerHeight - rect.top - 16;
  gs.style.maxHeight = Math.max(120, available) + 'px';
}
faFitGridToViewport();
window.addEventListener('resize', faFitGridToViewport);
window.addEventListener('scroll', faFitGridToViewport);
document.querySelector('.fa-grid-scroll')?.addEventListener('scroll', faFitGridToViewport);

/* ── Column Auto-Sizer ─────────────────────────────────────────────── */
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
      if (inp && !inp.classList.contains('cell-notes')) {
        inp.style.width = Math.max(colWidths[ci], 55) + 'px';
      }
    });
  });
}

function computeForecast(l) {
  const ytdActual = l.ytd_actual || 0;
  const accrualAdj = l.accrual_adj || 0;
  const unpaidBills = l.unpaid_bills || 0;
  const ytdTotal = ytdActual + accrualAdj + unpaidBills;
  // FA directive 2026-06-10 (supersedes 2026-05-05 minus sign): Capital
  // forecast = YTD + accrual + unpaid (no estimate).
  if (l.sheet_name === 'Capital' || (l.category || '').toLowerCase() === 'capital') {
    return ytdActual + accrualAdj + unpaidBills;
  }
  // 210 FA: RE-tax credit income (4105/4110/4115/4120/4125) — no extrapolation
  // (forecast = YTD + accrual + unpaid; estimate 0).
  if (['4105','4110','4115','4120','4125'].indexOf((l.gl_code||'').slice(0,4)) >= 0) {
    return ytdActual + accrualAdj + unpaidBills;
  }
  // FA #7 anomaly cap: negative YTD against non-negative prior year is a
  // one-time refund/credit; don't extrapolate.
  const prior = l.prior_year || 0;
  if (ytdTotal < 0 && prior >= 0) return ytdTotal;
  const ytdMonths = (typeof YTD_MONTHS !== 'undefined' && YTD_MONTHS > 0) ? YTD_MONTHS : 2;
  const remaining = (typeof REMAINING_MONTHS !== 'undefined') ? REMAINING_MONTHS : (12 - ytdMonths);
  return ytdTotal + (ytdTotal / ytdMonths) * remaining;
}

async function sendToPM(overrideOrphans) {
  if (!overrideOrphans && !confirm('Send to PM for expense review?')) return;
  const body = {status: 'pm_pending'};
  if (overrideOrphans) body.override_orphans = true;
  const resp = await fetch('/api/budgets/' + entityCode + '/status', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  // FA dir 2026-06-05 (QA on 733): the status POST can be REJECTED (e.g. 422
  // orphan_gls_unmapped — material GL data not mapped to the Summary). The old
  // code fired a success toast unconditionally, so the FA saw "Sent to PM" even
  // when the send was blocked and the budget stayed in draft. Check the response
  // and surface the server's reason instead of lying.
  if (!resp.ok) {
    let j = null;
    try { j = await resp.json(); } catch (e) {}
    // FA dir 2026-06-08: orphan gate is overridable. Offer an explicit
    // "send anyway" path that re-sends with override_orphans=true (logged
    // server-side). The override is a conscious choice, with the excluded
    // GLs + $ spelled out in the confirm.
    if (resp.status === 422 && j && j.error === 'orphan_gls_unmapped') {
      const gls = (j.orphan_gls || []).join(', ');
      const proceed = confirm(
        (j.message || 'Unmapped GL data would be dropped from the budget.') +
        '\n\nGLs not mapped: ' + gls +
        '\n\nOVERRIDE and send to PM anyway?\nThese dollars will NOT appear in the budget the PM sees. This override is recorded in the budget history.'
      );
      if (proceed) return sendToPM(true);
      return;
    }
    const msg = (j && (j.message || j.error)) || ('Send to PM failed (HTTP ' + resp.status + ').');
    showToast(msg, 'error');
    return;
  }
  showToast(overrideOrphans ? 'Sent to PM (orphan gate overridden — logged)' : 'Sent to PM for review', 'success');
  loadDetail();
}

async function approvePM() {
  if (!confirm('Approve PM review?')) return;
  const resp = await fetch('/api/budgets/' + entityCode + '/status', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({status: 'approved'})
  });
  if (!resp.ok) {
    let msg = 'Approve failed (HTTP ' + resp.status + ').';
    try { const j = await resp.json(); msg = j.message || j.error || msg; } catch (e) {}
    showToast(msg, 'error');
    return;
  }
  showToast('Budget approved!', 'success');
  loadDetail();
}

async function returnPM() {
  const notes = prompt('Notes for PM:');
  if (notes === null) return;
  const resp = await fetch('/api/budgets/' + entityCode + '/status', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({status: 'returned', notes: notes})
  });
  if (!resp.ok) {
    let msg = 'Return failed (HTTP ' + resp.status + ').';
    try { const j = await resp.json(); msg = j.message || j.error || msg; } catch (e) {}
    showToast(msg, 'error');
    return;
  }
  showToast('Budget returned to PM', 'info');
  loadDetail();
}

// ─── Health Drawer (Variant A — Quiet Pill) ───────────────────────────
// FA directive 2026-05-14 Phase 4: replaces inline summary-cards + inline
// Readiness Inspector. Trigger is the pill in the top nav. Drawer mirrors
// the same data the inline panels already fetch (no extra API calls).
function openHealthDrawer() {
  const d = document.getElementById('healthDrawer');
  const o = document.getElementById('healthOverlay');
  if (d) { d.classList.add('open'); d.setAttribute('aria-hidden','false'); }
  if (o) o.classList.add('open');
  // FA dir 2026-05-19: prefetch Data Quality so the badge updates immediately
  // even while the user is reading the Readiness tab.
  try { _loadDataQualityIfStale(); } catch (_e) {}
}

// ── Data Quality Tab (FA dir 2026-05-19) ─────────────────────────────
// Switches between the existing Readiness gates and the new Data Quality
// check list. Both share the same drawer + KPI strip up top.
function switchDrawerTab(tab) {
  document.querySelectorAll('.drawer-tab').forEach(b => {
    const active = b.dataset.drawerTab === tab;
    b.classList.toggle('active', active);
    b.style.borderBottomColor = active ? 'var(--blue, #1d4ed8)' : 'transparent';
    b.style.color = active ? 'var(--blue, #1d4ed8)' : 'var(--gray-500, #64748b)';
  });
  document.getElementById('drawerActions').style.display = tab === 'readiness' ? '' : 'none';
  document.getElementById('drawerDataQuality').style.display = tab === 'quality' ? '' : 'none';
  const rc = document.getElementById('drawerRecentChanges');
  if (rc) rc.style.display = tab === 'changes' ? '' : 'none';
  if (tab === 'quality') _loadDataQualityIfStale();
  if (tab === 'changes') _loadRecentChangesIfStale();
}

// ── Per-tab Undo + History (FA dir 2026-05-19 Phase 3) ────────────────
// Buttons in each sheet's formula bar. Scoped to the active sheet's GLs
// via the ?sheet= query param on the existing /api/recent-changes endpoint.
// Reuses the same undo endpoint as the Health drawer's Recent Changes tab.

async function faTabUndoLast() {
  const sheet = window._activeFaSheet || '';
  if (!sheet) { alert('No active sheet'); return; }
  // Fetch most recent change on this sheet
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) +
                              '?sheet=' + encodeURIComponent(sheet) + '&limit=20');
    if (!resp.ok) { alert('Could not load recent changes: ' + resp.status); return; }
    const data = await resp.json();
    const changes = data.changes || [];
    const target = changes.find(c => c.undoable);
    if (!target) {
      alert('No undoable changes on the ' + sheet + ' tab yet.\\n\\nThe Undo button reverts the most recent line-level edit (proposed budget, notes, formulas, etc.). If you made bulk changes or override edits, use the History button to browse.');
      return;
    }
    const fieldLabel = target.field || target.action || 'change';
    const batchNote = (target.batch_size && target.batch_size > 1)
      ? ('\\n\\nThis edit changed ' + target.batch_size + ' fields; they revert together.') : '';
    if (!confirm('Undo the most recent change on ' + sheet + '?\\n\\n' +
                  (target.gl_code ? target.gl_code + ' · ' : '') +
                  fieldLabel + ': ' +
                  (target.old_value || '(empty)') + ' ← ' + (target.new_value || '(empty)') + batchNote)) return;
    const undoResp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) + '/undo', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({revision_id: target.id}),
    });
    if (!undoResp.ok) {
      const err = await undoResp.text();
      alert('Undo failed: ' + err.slice(0, 200));
      return;
    }
    // Re-render the current sheet so the change shows (QA fix 4: mark the
    // bootstrap cache dirty first so the re-render pulls fresh data).
    window._faDataDirty = true;
    const tab = document.querySelector('.sheet-tab[data-sheet="' + sheet.replace(/"/g,'\\"') + '"]');
    if (tab) tab.click();
  } catch (e) {
    alert('Undo error: ' + e.message);
  }
}

async function faTabShowHistory() {
  const sheet = window._activeFaSheet || '';
  if (!sheet) { alert('No active sheet'); return; }
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) +
                              '?sheet=' + encodeURIComponent(sheet) + '&limit=50');
    if (!resp.ok) { alert('Could not load history: ' + resp.status); return; }
    const data = await resp.json();
    const changes = data.changes || [];
    _faTabRenderHistoryModal(sheet, changes);
  } catch (e) {
    alert('History error: ' + e.message);
  }
}

function _faTabRenderHistoryModal(sheet, changes) {
  // Strip any existing modal
  const existing = document.getElementById('faTabHistoryRoot');
  if (existing) existing.remove();
  let html = '';
  html += '<div id="faTabHistoryOverlay" onclick="_faTabCloseHistory()" style="position:fixed; inset:0; background:rgba(0,0,0,0.45); z-index:1000;"></div>';
  html += '<div id="faTabHistoryModal" style="position:fixed; top:60px; left:50%; transform:translateX(-50%); width:720px; max-width:94vw; max-height:82vh; background:white; border-radius:12px; box-shadow:0 24px 60px rgba(0,0,0,0.3); z-index:1001; overflow:hidden; display:flex; flex-direction:column;">';
  html += '<div style="padding:14px 22px; border-bottom:1px solid var(--gray-200); display:flex; justify-content:space-between; align-items:center;">';
  html += '<h3 style="margin:0; font-size:15px; font-weight:700; color:var(--gray-900);">⏱ History · ' + _escapeHtml(sheet) + ' tab</h3>';
  html += '<button onclick="_faTabCloseHistory()" style="border:none; background:transparent; font-size:20px; cursor:pointer; color:var(--gray-500); line-height:1;">×</button>';
  html += '</div>';
  html += '<div style="padding:8px 18px; font-size:11px; color:var(--gray-500); background:#fafbfc; border-bottom:1px solid var(--gray-200);">';
  html += changes.length + ' change' + (changes.length !== 1 ? 's' : '') + ' on this tab · newest first · Restore reverts a single field to its prior value';
  html += '</div>';
  html += '<div style="overflow-y:auto; flex:1;">';
  if (!changes.length) {
    html += '<div style="padding:40px; text-align:center; color:var(--gray-500); font-size:13px;">No changes logged on this tab yet.</div>';
  } else {
    for (const c of changes) {
      html += _faTabRenderHistoryEntry(c);
    }
  }
  html += '</div>';
  html += '<div style="padding:10px 22px; background:var(--gray-50, #fafbfc); border-top:1px solid var(--gray-200); font-size:10px; color:var(--gray-500); text-align:right;">Last 50 changes shown.</div>';
  html += '</div>';
  const wrap = document.createElement('div');
  wrap.id = 'faTabHistoryRoot';
  wrap.innerHTML = html;
  document.body.appendChild(wrap);
}

function _faTabRenderHistoryEntry(c) {
  const fieldLabels = {
    proposed_budget: 'Proposed', increase_pct: 'Increase %', increase_dollar: 'Increase $',
    estimate_override: 'Estimate', forecast_override: 'Forecast',
    estimate_formula: 'Est. formula', forecast_formula: 'Fcst. formula', proposed_formula: 'Prop. formula',
    accrual_adj: 'Accrual', unpaid_bills: 'Unpaid', current_budget: 'Curr. Budget',
    prior_year: 'Prior Year', ytd_actual: 'YTD',
    notes: 'Notes', category: 'Category', pm_review_state: 'PM review',
    fa_proposed_status: 'FA decision', fa_proposed_note: 'FA note', fa_override_value: 'FA override',
  };
  const fieldLabel = fieldLabels[c.field] || c.field || c.action;
  const oldDisp = _fmtChangeValue(c.old_value, c.field);
  const newDisp = _fmtChangeValue(c.new_value, c.field);
  const ts = c.ts ? new Date(c.ts) : null;
  const tsLocal = ts ? ts.toLocaleString([], {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'}) : '';
  let h = '<div style="padding:12px 22px; border-bottom:1px solid var(--gray-100); display:grid; grid-template-columns:1fr auto; gap:12px;">';
  h += '<div style="min-width:0;">';
  if (c.gl_code) {
    h += '<div style="font:600 13px -apple-system,sans-serif; color:var(--gray-900); margin-bottom:3px;">' + _escapeHtml(c.gl_code) + (c.description ? ' · ' + _escapeHtml(c.description) : '') + '</div>';
  }
  h += '<div style="font-size:12px; color:var(--gray-600); line-height:1.5;">';
  h += '<b style="color:var(--gray-900);">' + _escapeHtml(fieldLabel) + '</b>: ';
  h += '<span style="color:#94a3b8; text-decoration:line-through;">' + _escapeHtml(oldDisp) + '</span> → ';
  h += '<span style="color:var(--gray-900); font-weight:600;">' + _escapeHtml(newDisp) + '</span>';
  h += '</div>';
  h += '<div style="font-size:11px; color:var(--gray-400); margin-top:4px;">' + _escapeHtml(tsLocal);
  if (c.source) h += ' · ' + _escapeHtml(c.source);
  if (c.action === 'undo') h += ' · <span style="color:var(--blue);">UNDO</span>';
  h += '</div>';
  h += '</div>';
  if (c.undoable) {
    h += '<button onclick="_faTabRestoreFromHistory(' + c.id + ', this)" style="align-self:center; padding:6px 14px; font:600 12px -apple-system,sans-serif; background:var(--blue, #1d4ed8); color:white; border:none; border-radius:6px; cursor:pointer; white-space:nowrap;">↺ Restore</button>';
  } else {
    h += '<span style="align-self:center; color:var(--gray-400); font-size:11px;">not undoable</span>';
  }
  h += '</div>';
  return h;
}

function _faTabCloseHistory() {
  const r = document.getElementById('faTabHistoryRoot');
  if (r) r.remove();
}

async function _faTabRestoreFromHistory(revId, btn) {
  if (!confirm('Restore this version of the field?')) return;
  if (btn) { btn.disabled = true; btn.textContent = 'Restoring…'; }
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) + '/undo', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({revision_id: revId}),
    });
    if (!resp.ok) {
      alert('Restore failed: ' + await resp.text());
      if (btn) { btn.disabled = false; btn.textContent = '↺ Restore'; }
      return;
    }
    _faTabCloseHistory();
    // Re-render current sheet so the restored value appears
    const sheet = window._activeFaSheet || '';
    const tab = document.querySelector('.sheet-tab[data-sheet="' + sheet.replace(/"/g,'\\"') + '"]');
    if (tab) tab.click();
  } catch (e) {
    alert('Restore error: ' + e.message);
    if (btn) { btn.disabled = false; btn.textContent = '↺ Restore'; }
  }
}

// ── Recent Changes Tab (FA dir 2026-05-19 Phase 2) ────────────────────
// Reads BudgetRevision rows for the current building, renders newest-first,
// with one-click Undo for any line-field change. Re-renders after undo.
let _rcLoadedAt = 0;
async function _loadRecentChangesIfStale() {
  if (Date.now() - _rcLoadedAt < 15000) return;
  const container = document.getElementById('drawerRecentChanges');
  if (!container) return;
  if (!container.innerHTML.trim()) {
    container.innerHTML = '<div style="padding:24px; text-align:center; color:var(--gray-500); font-size:13px;">Loading recent changes…</div>';
  }
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) + '?limit=50');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    _rcLoadedAt = Date.now();
    _renderRecentChanges(data.changes || []);
  } catch (e) {
    container.innerHTML = '<div style="padding:24px; color:var(--red); font-size:13px;">Failed to load recent changes: ' + (e.message || 'unknown') + '</div>';
  }
}

function _renderRecentChanges(changes) {
  const container = document.getElementById('drawerRecentChanges');
  if (!container) return;
  if (!changes.length) {
    container.innerHTML = '<div style="padding:32px; text-align:center; color:var(--gray-500); font-size:13px;">No changes logged yet for this building. Edits to budget lines, notes, and FA actions will appear here.</div>';
    return;
  }
  let html = '<div style="padding:12px 16px; background:#fafbfc; border-bottom:1px solid var(--gray-200); display:flex; gap:14px; font-size:11px; color:var(--gray-500);">';
  html += '<span><b style="color:var(--gray-900);">' + changes.length + '</b> change' + (changes.length !== 1 ? 's' : '') + ' (newest first, last 50)</span>';
  html += '<span style="margin-left:auto;">Click ↺ to undo any change</span>';
  html += '</div>';
  for (const c of changes) {
    html += _renderRecentChangeRow(c);
  }
  container.innerHTML = html;
}

function _renderRecentChangeRow(c) {
  const ts = c.ts ? new Date(c.ts) : null;
  const tsLocal = ts ? ts.toLocaleString([], {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'}) : '';
  const fieldLabels = {
    proposed_budget: 'Proposed', increase_pct: 'Increase %', increase_dollar: 'Increase $',
    estimate_override: 'Estimate', forecast_override: 'Forecast',
    estimate_formula: 'Est. formula', forecast_formula: 'Fcst. formula', proposed_formula: 'Prop. formula',
    accrual_adj: 'Accrual', unpaid_bills: 'Unpaid', current_budget: 'Curr. Budget',
    prior_year: 'Prior Year', ytd_actual: 'YTD',
    notes: 'Notes', category: 'Category', pm_review_state: 'PM review',
    fa_proposed_status: 'FA decision', fa_proposed_note: 'FA note', fa_override_value: 'FA override',
  };
  const fieldLabel = fieldLabels[c.field] || c.field || c.action;
  const oldDisp = _fmtChangeValue(c.old_value, c.field);
  const newDisp = _fmtChangeValue(c.new_value, c.field);
  let html = '<div style="padding:10px 16px; border-bottom:1px solid var(--gray-200); display:grid; grid-template-columns:1fr auto; gap:8px;">';
  html += '<div style="min-width:0;">';
  if (c.gl_code) {
    html += '<div style="font:600 12px -apple-system,sans-serif; color:var(--gray-900); margin-bottom:2px;">' + _escapeHtml(c.gl_code) + (c.description ? ' · ' + _escapeHtml(c.description) : '') + '</div>';
  } else if (c.action) {
    html += '<div style="font:600 12px -apple-system,sans-serif; color:var(--gray-900); margin-bottom:2px;">' + _escapeHtml(c.action.replace(/_/g, ' ')) + '</div>';
  }
  html += '<div style="font-size:11px; color:var(--gray-600); line-height:1.4;">';
  html += '<b style="color:var(--gray-900);">' + _escapeHtml(fieldLabel) + '</b>: ';
  html += '<span style="color:#94a3b8; text-decoration:line-through;">' + _escapeHtml(oldDisp) + '</span> → ';
  html += '<span style="color:var(--gray-900); font-weight:600;">' + _escapeHtml(newDisp) + '</span>';
  html += '</div>';
  html += '<div style="font-size:10px; color:var(--gray-400); margin-top:3px;">' + _escapeHtml(tsLocal);
  if (c.source) html += ' · ' + _escapeHtml(c.source);
  if (c.action === 'undo') html += ' · <span style="color:var(--blue);">UNDO</span>';
  html += '</div>';
  html += '</div>';
  if (c.undoable) {
    html += '<button onclick="_undoRecentChange(' + c.id + ', this)" title="Restore the previous value" style="align-self:center; padding:5px 10px; font:600 11px -apple-system,sans-serif; background:white; color:var(--blue, #1d4ed8); border:1px solid var(--blue, #1d4ed8); border-radius:5px; cursor:pointer; white-space:nowrap;">↺ Undo</button>';
  } else {
    html += '<span style="align-self:center; color:var(--gray-400); font-size:10px;">—</span>';
  }
  html += '</div>';
  return html;
}

function _fmtChangeValue(raw, field) {
  if (raw === null || raw === undefined || raw === '') return '(empty)';
  const s = String(raw);
  // Treat numeric fields as currency for readability
  const numericFields = ['proposed_budget', 'increase_dollar', 'estimate_override', 'forecast_override',
                          'accrual_adj', 'unpaid_bills', 'current_budget', 'prior_year', 'ytd_actual', 'fa_override_value'];
  if (numericFields.indexOf(field) >= 0) {
    const n = parseFloat(s);
    if (!isNaN(n)) {
      return (n < 0 ? '-$' : '$') + Math.abs(Math.round(n)).toLocaleString();
    }
  }
  if (field === 'increase_pct') {
    const n = parseFloat(s);
    if (!isNaN(n)) return (n * 100).toFixed(1) + '%';
  }
  // Truncate long text
  if (s.length > 60) return s.slice(0, 57) + '…';
  return s;
}

async function _undoRecentChange(revisionId, btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Undoing…'; }
  try {
    const resp = await fetch('/api/recent-changes/' + encodeURIComponent(entityCode) + '/undo', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({revision_id: revisionId}),
    });
    if (!resp.ok) {
      const err = await resp.text();
      alert('Undo failed: ' + err.slice(0, 200));
      if (btn) { btn.disabled = false; btn.textContent = '↺ Undo'; }
      return;
    }
    // Refresh the feed
    _rcLoadedAt = 0;
    await _loadRecentChangesIfStale();
    // Also re-render the active sheet so the change is reflected in the grid
    if (typeof renderActiveSheet === 'function') {
      try { renderActiveSheet(); } catch (_e) {}
    } else if (typeof loadDetail === 'function') {
      try { loadDetail(); } catch (_e) {}
    }
  } catch (e) {
    alert('Undo error: ' + e.message);
    if (btn) { btn.disabled = false; btn.textContent = '↺ Undo'; }
  }
}

let _hcLoadedAt = 0;
async function _loadDataQualityIfStale() {
  // Re-fetch if older than 30 seconds OR never loaded
  if (Date.now() - _hcLoadedAt < 30000) return;
  const container = document.getElementById('drawerDataQuality');
  if (!container) return;
  if (!container.innerHTML.trim()) {
    container.innerHTML = '<div style="padding:24px; text-align:center; color:var(--gray-500); font-size:13px;">Running checks…</div>';
  }
  try {
    const resp = await fetch('/api/health-check/' + encodeURIComponent(entityCode));
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    _hcLoadedAt = Date.now();
    _renderDataQuality(data);
  } catch (e) {
    container.innerHTML = '<div style="padding:24px; color:var(--red); font-size:13px;">Health check failed: ' + (e.message || 'unknown') + '</div>';
  }
}

function _renderDataQuality(data) {
  const container = document.getElementById('drawerDataQuality');
  const badge = document.getElementById('drawerDqBadge');
  if (!container || !data) return;
  const s = data.summary || {pass:0, warn:0, fail:0};
  // Update tab badge (only shows when fail > 0)
  if (badge) {
    if (s.fail > 0) { badge.style.display = ''; badge.textContent = String(s.fail); }
    else if (s.warn > 0) { badge.style.display = ''; badge.textContent = String(s.warn); badge.style.background = '#d97706'; }
    else { badge.style.display = 'none'; }
  }
  let html = '';
  // Summary strip
  html += '<div style="padding:12px 16px; background:#fafbfc; border-bottom:1px solid var(--gray-200); display:flex; gap:14px; font-size:11px; font-weight:600;">';
  html += '<span style="color:#16a34a;">&#10003; ' + s.pass + ' passing</span>';
  html += '<span style="color:#d97706;">&#9888; ' + s.warn + ' warning' + (s.warn !== 1 ? 's' : '') + '</span>';
  html += '<span style="color:#dc2626;">&#10007; ' + s.fail + ' issue' + (s.fail !== 1 ? 's' : '') + '</span>';
  html += '<span style="margin-left:auto; color:var(--gray-500);">Last run: just now</span>';
  html += '</div>';
  // Check list — fails first, then warns, then passes
  const order = {fail: 0, warn: 1, pass: 2};
  const checks = (data.checks || []).slice().sort((a,b) => (order[a.status]||9) - (order[b.status]||9));
  for (const c of checks) {
    html += _renderHealthCheck(c);
  }
  container.innerHTML = html;
  // Attach expand handlers
  container.querySelectorAll('.hc-row').forEach(row => {
    row.addEventListener('click', () => {
      const e = row.nextElementSibling;
      if (e && e.classList.contains('hc-expand')) e.classList.toggle('open');
    });
  });
}

function _renderHealthCheck(c) {
  const icons = {pass: '&#10003;', warn: '&#9888;', fail: '&#10007;'};
  const colors = {pass: '#16a34a', warn: '#d97706', fail: '#dc2626'};
  const bgs = {pass: '#dcfce7', warn: '#fef9c3', fail: '#fee2e2'};
  const icon = icons[c.status] || '?';
  const color = colors[c.status] || '#64748b';
  const bg = bgs[c.status] || '#f1f5f9';
  let html = '';
  html += '<div class="hc-row" style="display:grid; grid-template-columns:20px 1fr auto; gap:10px; padding:12px 16px; cursor:pointer; border-bottom:1px solid var(--gray-200); align-items:center;">';
  html += '<span style="font-size:14px; color:' + color + ';">' + icon + '</span>';
  html += '<div>';
  html += '<div style="font:600 13px -apple-system,sans-serif; color:#0f172a; margin-bottom:2px;">' + _escapeHtml(c.name) + '</div>';
  html += '<div style="font-size:12px; color:var(--gray-500); line-height:1.4;">' + _escapeHtml(c.detail || '') + '</div>';
  html += '</div>';
  html += '<span style="font:600 10px -apple-system,sans-serif; padding:2px 8px; border-radius:999px; background:' + bg + '; color:' + color + '; text-transform:uppercase; letter-spacing:0.04em;">' + c.status + '</span>';
  html += '</div>';
  // Expand panel — only render if there's data or a fix
  if ((c.data && c.data.length) || c.fix) {
    html += '<div class="hc-expand" style="display:none; padding:12px 16px 14px 46px; background:#fafbfc; border-bottom:1px solid var(--gray-200); font-size:12px;">';
    if (c.data && c.data.length) {
      html += '<div style="font:600 10px -apple-system,sans-serif; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.04em; margin-bottom:6px;">Affected accounts</div>';
      for (const d of c.data) {
        html += '<div style="display:grid; grid-template-columns:100px 1fr 80px; gap:10px; padding:4px 0; border-bottom:1px dashed var(--gray-200);">';
        html += '<span style="font-family:ui-monospace,Menlo,monospace; color:var(--gray-500);">' + _escapeHtml(d.gl || '') + '</span>';
        html += '<span>' + _escapeHtml(d.desc || '') + '</span>';
        html += '<span style="font-family:ui-monospace,Menlo,monospace; text-align:right; font-weight:600;">$' + Math.round(d.ytd || 0).toLocaleString() + '</span>';
        html += '</div>';
      }
    }
    if (c.fix) {
      html += '<div style="margin-top:10px; padding:10px 12px; background:#eff6ff; border-left:3px solid #1d4ed8; border-radius:0 5px 5px 0;">';
      html += '<div style="font:600 10px -apple-system,sans-serif; color:#1d4ed8; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:4px;">Suggested fix</div>';
      html += '<div style="color:#0f172a;">' + _escapeHtml(c.fix.label || 'Take action') + '</div>';
      if (c.fix.url) {
        html += '<a href="' + _escapeAttr(c.fix.url) + '" style="display:inline-block; margin-top:6px; padding:5px 11px; background:#1d4ed8; color:white; border-radius:5px; text-decoration:none; font:600 11px -apple-system,sans-serif;">Open →</a>';
      } else if (c.fix.endpoint) {
        html += '<button onclick="_runHealthFix(this, \'' + _escapeAttr(c.fix.endpoint) + '\', ' + JSON.stringify(c.fix.body || null).replace(/"/g, '&quot;') + ')" style="margin-top:6px; padding:5px 11px; background:#1d4ed8; color:white; border:none; border-radius:5px; cursor:pointer; font:600 11px -apple-system,sans-serif;">Run fix →</button>';
      }
      html += '</div>';
    }
    html += '</div>';
  }
  return html;
}

async function _runHealthFix(btn, endpoint, body) {
  btn.disabled = true;
  btn.textContent = 'Running…';
  try {
    const opts = {method: 'POST', headers: {'Content-Type': 'application/json'}};
    if (body) opts.body = JSON.stringify(body);
    const resp = await fetch(endpoint, opts);
    if (resp.ok) {
      btn.textContent = '✓ Fixed — refreshing';
      btn.style.background = '#16a34a';
      // Re-run health check
      setTimeout(() => { _hcLoadedAt = 0; _loadDataQualityIfStale(); }, 800);
    } else {
      btn.textContent = '✗ Failed (' + resp.status + ')';
      btn.style.background = '#dc2626';
      btn.disabled = false;
    }
  } catch (e) {
    btn.textContent = '✗ Error';
    btn.style.background = '#dc2626';
    btn.disabled = false;
  }
}

function _escapeHtml(s) {
  if (s === null || s === undefined) return '';
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function _escapeAttr(s) { return _escapeHtml(s); }
function closeHealthDrawer() {
  const d = document.getElementById('healthDrawer');
  const o = document.getElementById('healthOverlay');
  if (d) { d.classList.remove('open'); d.setAttribute('aria-hidden','true'); }
  if (o) o.classList.remove('open');
}
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') closeHealthDrawer();
});

function _fmtDrawerMoney(n) {
  if (n === null || n === undefined || isNaN(n)) return '—';
  const v = Math.round(n);
  return (v < 0 ? '-$' : '$') + Math.abs(v).toLocaleString();
}

function populateHealthDrawerKpis(totalPrior, totalBudget, variance, pctChange, totalForecast) {
  const el = document.getElementById('drawerKpis');
  if (!el) return;
  const absPct = Math.abs(pctChange || 0);
  const varClass = absPct > 10 ? 'bad' : absPct > 5 ? 'warn' : 'pos';
  const arrow = pctChange > 0 ? ' ▲' : pctChange < 0 ? ' ▼' : '';
  const varSign = variance > 0 ? '+' : '';
  el.innerHTML =
    '<div class="drawer-kpi"><div class="num">' + _fmtDrawerMoney(totalPrior) + '</div><div class="lbl">Prior Year</div></div>' +
    '<div class="drawer-kpi"><div class="num">' + _fmtDrawerMoney(totalBudget) + '</div><div class="lbl">Current Budget</div></div>' +
    '<div class="drawer-kpi ' + varClass + '"><div class="num">' + (variance < 0 ? '' : varSign) + _fmtDrawerMoney(variance) + '</div><div class="lbl">Variance</div></div>' +
    '<div class="drawer-kpi ' + varClass + '"><div class="num">' + (totalForecast ? pctChange.toFixed(1) + '%' + arrow : '—') + '</div><div class="lbl">% Change</div></div>';
}

// Plain-English overrides for the readiness API's terse gate copy.
// Status-aware: shows action-style labels for fail/warn, past-tense for ok.
// FA directive 2026-05-14 Phase 4.2 (overrides) + Phase 4.5 (status-aware).
// Each override has:
//   { labelFor: (status) -> string, detail: (origDetail, status) -> string }
const DRAWER_GATE_COPY = {
  source_files: {
    labelFor: () => 'Last year\'s budget file located',
    detail: (orig) => orig || 'We found the 2026 approved budget Excel in SharePoint.'
  },
  audit_confirmed: {
    labelFor: (s) => s === 'fail' ? 'Upload FY2025 audit' : 'FY2025 audit confirmed',
    detail: (orig) => orig || 'Prior-year actuals are locked in for Column 1.'
  },
  period_set: {
    labelFor: (s) => s === 'fail' ? 'Tell me the last completed month' : 'Period set',
    detail: (orig, s) => s === 'fail'
      ? 'Pick the last month with actual numbers. Everything after that is forecast. Without this, the forecast defaults to "January through February" which is almost always wrong.'
      : (orig || 'The last completed month is set; YTD vs. forecast split is correct.')
  },
  building_type_set: {
    labelFor: () => 'Building type set',
    detail: (orig) => orig || 'Co-op / Condo / Rental — drives which line items show up.'
  },
  no_orphans: {
    labelFor: (s) => s === 'ok' ? 'No unmapped GLs' : 'Add summary rows for unmapped GLs',
    detail: (orig, s) => {
      if (s === 'ok') return orig || 'Every GL with data aggregates into a summary row.';
      const m = (orig || '').match(/(\d+)\s+GL/);
      const n = m ? m[1] : '?';
      return n + ' GL code(s) have data in the ledger but no matching summary row, so they aren\'t aggregating anywhere. Open the Summary tab and click "+ Add Row" next to each one.';
    }
  },
  no_duplicates: {
    labelFor: (s) => s === 'ok' ? 'No duplicate summary rows' : 'Resolve duplicate summary rows',
    detail: (orig, s) => {
      if (s === 'ok') return orig || 'No two summary rows share the same GL prefix.';
      const m = (orig || '').match(/(\d+)\s+duplicate/);
      const n = m ? m[1] : '?';
      return n + ' pair(s) of summary rows share the same GL prefix. The data will get split or double-counted. Open the Summary tab to see which rows and decide which to keep.';
    }
  },
  payroll_reviewed: {
    labelFor: (s) => s === 'ok' ? 'Payroll positions configured' : 'Add payroll positions',
    detail: (orig, s) => s === 'ok'
      ? (orig || 'Payroll has at least one position so the forecast can run.')
      : 'There are no payroll positions configured for this building, so the payroll forecast can\'t run. Open the Payroll tab and add at least one position.'
  },
  approved_file_labels: {
    labelFor: (s) => s === 'ok' ? 'Approved-file labels look good' : 'Match last year\'s row labels to summary rows',
    detail: (orig, s) => {
      // OK case: pass through the (now-improved) server detail text — it
      // already explains both the all-clean and all-already-handled cases.
      if (s === 'ok') {
        return orig || 'All row labels in last year\'s approved budget Excel have a matching row.';
      }
      const m = (orig || '').match(/(\d+)\s+label/);
      const n = m ? m[1] : '?';
      return n + ' row label(s) in last year\'s approved budget Excel don\'t have a matching summary row yet. Click "Show labels" to see which ones, then either rename them in the Excel or add a new row to match.';
    }
  },
  generated: {
    labelFor: (s) => s === 'ok' ? 'Budget generated' : 'Generate the proposed budget',
    detail: (orig, s) => s === 'ok'
      ? (orig || 'The proposed 2027 budget has been generated.')
      : 'Once everything above is green, click Generate to build the proposed 2027 budget from your data and assumptions.'
  }
};

// Cache for /api/wizard/<ec>/scan-findings + /api/summary/<ec> so opening
// the approved_file_labels expand a second time doesn't re-fetch.
let _scanFindingsCache = null;
let _summaryRowsCache = null;

// Build a Set of label strings (normalized lowercase) currently on this
// building's summary tab. Counts ANY row with a label — including subtotal
// and section_header rows. Reason: the import sometimes misclassifies rows
// (e.g., Building 148's "Prior Year Surplus" came in as row_type='subtotal'
// instead of 'data'). The FA still sees the row on the summary; we shouldn't
// tell them it's "Missing" just because of an internal row_type quirk.
// FA directive 2026-05-14 Phase 4.4.1.
function _buildCurrentLabelSet(summaryData) {
  const s = new Set();
  if (!summaryData || !Array.isArray(summaryData.rows)) return s;
  summaryData.rows.forEach(r => {
    if (r && r.label) {
      s.add(String(r.label).trim().toLowerCase());
    }
  });
  return s;
}

// Expand-toggle handler for drawer items that show inline detail.
// Used by approved_file_labels — clicking "Show labels" expands to
// reveal the actual list of unmapped labels, each marked with whether
// the FA has already added a custom row for it on this building's
// summary tab (✓) or whether it's still missing entirely (✕). The
// scan compares the Excel against a global master list; this column
// compares against THIS building's current rows. FA directive
// 2026-05-14 Phase 4.4 (clarifying the "already added but still
// flagged" confusion).
function toggleDrawerExpand(gateKey) {
  const wrap = document.getElementById('acExpand-' + gateKey);
  if (!wrap) return;
  const isOpen = wrap.style.display === 'block';
  if (isOpen) {
    wrap.style.display = 'none';
    return;
  }
  wrap.style.display = 'block';
  if (gateKey === 'approved_file_labels') {
    const render = (scanData, summaryData) => {
      const labels = (scanData && scanData.unmapped_labels) || [];
      const file = scanData && scanData.file_name;
      const currentSet = _buildCurrentLabelSet(summaryData);
      if (!labels.length) {
        wrap.innerHTML = '<div style="padding:10px 14px; color:var(--gray-500); font-size:11px;">No unmapped labels — looks clean.</div>';
        return;
      }
      const alreadyCount = labels.filter(u => currentSet.has(String(u.label || '').trim().toLowerCase())).length;
      const trulyMissing = labels.length - alreadyCount;

      let html = '<div style="padding:10px 14px 12px; background:#fafaf7; border-top:1px solid var(--gray-200);">';
      if (file) {
        html += '<div style="font-size:10px; color:var(--gray-500); margin-bottom:6px;">From file: <code style="font-size:10px; background:rgba(0,0,0,0.05); padding:1px 5px; border-radius:3px;">' + file.replace(/</g,'&lt;') + '</code></div>';
      }
      // Summary chip: how many are real problems vs already-handled.
      if (alreadyCount > 0) {
        html += '<div style="margin:4px 0 8px; padding:6px 10px; background:#f0fdf4; border:1px solid #bbf7d0; border-radius:6px; font-size:11px; color:#166534; line-height:1.5;">' +
          '<strong>' + alreadyCount + '</strong> of these are already a row on your summary tab (added manually). The scan still flags them because they\'re not in the system\'s master list. ' +
          (trulyMissing > 0 ? '<strong>' + trulyMissing + '</strong> ' + (trulyMissing === 1 ? 'still needs' : 'still need') + ' attention.' : 'Nothing else to do — these will import into the existing rows.') +
          '</div>';
      }
      html += '<table style="width:100%; border-collapse:collapse; font-size:11px;">';
      html += '<thead><tr>' +
        '<th style="text-align:left; padding:4px 6px; color:var(--gray-500); font-weight:600; text-transform:uppercase; letter-spacing:0.04em; font-size:9px; border-bottom:1px solid var(--gray-200);">Label in approved file</th>' +
        '<th style="text-align:left; padding:4px 6px; color:var(--gray-500); font-weight:600; text-transform:uppercase; letter-spacing:0.04em; font-size:9px; border-bottom:1px solid var(--gray-200);">On your summary?</th>' +
        '<th style="text-align:left; padding:4px 6px; color:var(--gray-500); font-weight:600; text-transform:uppercase; letter-spacing:0.04em; font-size:9px; border-bottom:1px solid var(--gray-200);">Suggested mapping</th>' +
        '</tr></thead><tbody>';
      labels.forEach(u => {
        const lblRaw = (u.label || '').trim();
        const lbl = lblRaw.replace(/</g,'&lt;');
        const isAlready = currentSet.has(lblRaw.toLowerCase());
        const statusCell = isAlready
          ? '<span style="display:inline-flex; align-items:center; gap:4px; color:#166534; font-weight:600; font-size:11px;">&#10003; Already added</span>'
          : '<span style="display:inline-flex; align-items:center; gap:4px; color:var(--red); font-weight:600; font-size:11px;">&#10007; Missing</span>';
        const sug = u.suggested ? u.suggested.replace(/</g,'&lt;') : '<span style="color:var(--gray-400);">&mdash; no match &mdash;</span>';
        html += '<tr>' +
          '<td style="padding:5px 6px; border-bottom:1px solid var(--gray-100); font-family:monospace; font-size:11px;">' + lbl + '</td>' +
          '<td style="padding:5px 6px; border-bottom:1px solid var(--gray-100);">' + statusCell + '</td>' +
          '<td style="padding:5px 6px; border-bottom:1px solid var(--gray-100); color:var(--gray-700); font-size:11px;">' + sug + '</td>' +
          '</tr>';
      });
      html += '</tbody></table>';
      html += '<div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">';
      html += '<a href="/admin/portfolio-health" target="_blank" class="ac-btn">Open standard label map &rarr;</a>';
      html += '<button onclick="rescanLabels(\'' + entityCode + '\')" class="ac-btn">Re-scan after fixing</button>';
      html += '</div>';
      html += '<div style="margin-top:8px; font-size:10px; color:var(--gray-500); line-height:1.5;">' +
        '<strong>Already added</strong> rows already exist on your summary tab (with a custom name), so the data will land there on import. The gate still flags them because the system\'s master row list doesn\'t recognize the name. To silence the alert permanently for these, the label needs to be added to the master list (admin change).<br>' +
        '<strong>Missing</strong> rows have no home on your summary yet. For each one, either (a) click + Add Row on the Summary tab and add a row with that name, or (b) rename the row in the Excel to match an existing standard label. Then re-scan to refresh this list.' +
        '</div>';
      html += '</div>';
      wrap.innerHTML = html;
    };

    if (_scanFindingsCache && _summaryRowsCache) {
      render(_scanFindingsCache, _summaryRowsCache);
      return;
    }
    wrap.innerHTML = '<div style="padding:10px 14px; color:var(--gray-500); font-size:11px;">Loading labels&hellip;</div>';
    Promise.all([
      fetch('/api/wizard/' + entityCode + '/scan-findings').then(r => r.json()),
      fetch('/api/summary/' + entityCode).then(r => r.json()).catch(() => ({rows: []}))
    ]).then(([scan, summary]) => {
      _scanFindingsCache = scan;
      _summaryRowsCache = summary;
      render(scan, summary);
    }).catch(err => {
      wrap.innerHTML = '<div style="padding:10px 14px; color:var(--red); font-size:11px;">Failed to load labels: ' + (err.message || err) + '</div>';
    });
  }
}

// Force-refresh the scan-findings (after the FA fixed labels in admin).
function rescanLabels(ec) {
  const wrap = document.getElementById('acExpand-approved_file_labels');
  if (wrap) wrap.innerHTML = '<div style="padding:10px 14px; color:var(--gray-500); font-size:11px;">Re-scanning…</div>';
  _scanFindingsCache = null;
  _summaryRowsCache = null;  // FA may have added rows since last view — re-fetch on rescan.
  fetch('/api/wizard/' + ec + '/scan-findings?refresh=1', {method:'POST'}).then(() => {
    // Re-render the entire readiness inspector (and the drawer with it).
    renderReadinessInspector();
    // Re-open the expand on the new content.
    setTimeout(() => toggleDrawerExpand('approved_file_labels'), 300);
  });
}

function populateHealthDrawerActions(gates, summary) {
  const s = summary || {};
  const fail = s.fail || 0;
  const warn = s.warn || 0;
  const ok = s.ok || 0;
  // Update pill badge — shows BLOCKER count only (not blockers + warnings).
  // Rationale: the badge means "you can't ship this." Warnings are
  // "review before submitting," not blockers, so they shouldn't trip
  // the red alarm. When blockers = 0, badge shows green ✓ even if
  // warnings still exist; the drawer header still surfaces the warning
  // count in text so it isn't invisible.
  const badge = document.getElementById('healthBadge');
  if (badge) {
    badge.textContent = fail > 0 ? String(fail) : '✓';
    badge.className = 'badge' + (fail === 0 ? ' zero' : '');
  }
  // Drawer header summary
  const sumEl = document.getElementById('healthSummary');
  if (sumEl) {
    const parts = [];
    if (fail > 0) parts.push(fail + ' blocker' + (fail !== 1 ? 's' : ''));
    if (warn > 0) parts.push(warn + ' warning' + (warn !== 1 ? 's' : ''));
    if (ok > 0) parts.push(ok + ' ready');
    sumEl.textContent = parts.join(' · ');
  }
  const actionsEl = document.getElementById('drawerActions');
  if (!actionsEl) return;
  const failGates = (gates || []).filter(function(g){ return g.status === 'fail'; });
  const warnGates = (gates || []).filter(function(g){ return g.status === 'warn'; });
  const okGates   = (gates || []).filter(function(g){ return g.status === 'ok' || g.status === 'skip'; });

  function renderGate(g, severity) {
    const iconClass = severity === 'fail' ? 'bad' : severity === 'warn' ? 'warn' : 'ok';
    const icon = severity === 'fail' ? '✕' : severity === 'warn' ? '!' : '✓';
    const btnClass = severity === 'fail' ? 'ac-btn primary' : 'ac-btn';

    // Plain-English copy override per gate key. Status-aware so the OK group
    // doesn't show imperative labels like "Tell me the last completed month"
    // on a gate that's already complete. Falls back to API copy when no
    // override exists. FA directive 2026-05-14 Phase 4.2 + 4.5.
    const ov = (g.key && DRAWER_GATE_COPY[g.key]) || null;
    const gStatus = (g.status || '').toLowerCase();
    const displayLabel = ov ? ov.labelFor(gStatus) : (g.label || '');
    const displayDetail = ov ? ov.detail(g.detail || '', gStatus) : (g.detail || '');

    let btnHtml = '';
    // Special case: approved_file_labels uses an inline expand pattern, not
    // a navigation. The FA wanted to see the actual list of unmapped labels
    // in the drawer, not be routed to another page.
    if (g.key === 'approved_file_labels' && severity !== 'ok') {
      btnHtml = '<a class="' + btnClass + '" onclick="toggleDrawerExpand(\'approved_file_labels\')">Show labels</a>';
    } else if (g.action_url && g.action_label) {
      const isHash = String(g.action_url).startsWith('#');
      if (isHash) {
        const safeUrl = String(g.action_url).replace(/'/g, "\\'");
        btnHtml = '<a class="' + btnClass + '" onclick="closeHealthDrawer(); readinessAction(\'' + safeUrl + '\')">' + g.action_label + '</a>';
      } else {
        btnHtml = '<a class="' + btnClass + '" href="' + g.action_url + '">' + g.action_label + '</a>';
      }
    } else if (g.action_label && g.key) {
      // Gate has an action label but no URL (e.g., period_set) — dispatch by
      // gate key to a client-side handler. Without this, the FA would see
      // a blocker with no way to fix it.
      const safeKey = String(g.key).replace(/'/g, "\\'");
      btnHtml = '<a class="' + btnClass + '" onclick="drawerGateAction(\'' + safeKey + '\')">' + g.action_label + '</a>';
    }

    // Inline-expand container — rendered empty, populated by toggleDrawerExpand.
    // Used today by approved_file_labels; available to any future gate that
    // wants in-drawer detail instead of a page navigation.
    const expandHtml = (g.key === 'approved_file_labels' && severity !== 'ok')
      ? '<div class="ac-expand" id="acExpand-' + g.key + '" style="display:none;"></div>'
      : '';

    return '<div class="ac-item">' +
      '<span class="ac-icon ' + iconClass + '">' + icon + '</span>' +
      '<div class="ac-text">' +
        '<div class="ac-label">' + displayLabel + '</div>' +
        (displayDetail ? '<div class="ac-detail">' + displayDetail + '</div>' : '') +
      '</div>' + btnHtml +
    '</div>' + expandHtml;
  }

  let html = '';
  if (failGates.length > 0) {
    html += '<div class="ac-group blockers"><div class="ac-group-title">🔴 Blockers — must resolve before generating</div>';
    failGates.forEach(function(g){ html += renderGate(g, 'fail'); });
    html += '</div>';
  }
  if (warnGates.length > 0) {
    html += '<div class="ac-group warnings"><div class="ac-group-title">🟡 Warnings — review before submitting</div>';
    warnGates.forEach(function(g){ html += renderGate(g, 'warn'); });
    html += '</div>';
  }
  if (okGates.length > 0) {
    html += '<div class="ac-group complete complete-collapsed" id="drawerOkGroup">' +
      '<div class="ac-group-title" onclick="document.getElementById(\'drawerOkGroup\').classList.toggle(\'complete-collapsed\');">▶ ' + okGates.length + ' things complete — click to expand</div>';
    okGates.forEach(function(g){ html += renderGate(g, 'ok'); });
    html += '</div>';
  }
  if (!html) {
    html = '<div style="padding:24px 22px; text-align:center; color:var(--gray-500);">✅ All checks clear — ready to generate.</div>';
  }
  actionsEl.innerHTML = html;
}

// Gate-action dispatcher for drawer buttons that have no action_url (the API
// returns action_label but action_url=null for gates whose fix is fully
// client-side, e.g. period_set). Without this, those blockers would render
// no button — dead UX.
function drawerGateAction(key) {
  if (key === 'period_set') {
    // The period picker lives inside #periodBanner (rendered by editPeriod()).
    // We hid that banner via CSS to clean up the workbook, so we have to
    // temporarily promote it to visible with inline !important — wins over
    // the CSS rule. After save, renderPeriodBanner re-paints it; we leave
    // visibility on so the FA sees their save took effect, then they can
    // re-open the drawer to confirm the gate flipped green.
    closeHealthDrawer();
    const ubs = document.getElementById('unifiedStatusBlock');
    const banner = document.getElementById('periodBanner');
    if (ubs) ubs.style.setProperty('display', 'block', 'important');
    if (banner) {
      banner.style.setProperty('display', 'block', 'important');
      banner.style.padding = '12px 24px';
      // Defer to next tick so the unhide paints first, then editPeriod()
      // can write its picker UI into the now-visible banner.
      setTimeout(function() {
        if (typeof editPeriod === 'function') editPeriod();
        banner.scrollIntoView({behavior: 'smooth', block: 'center'});
      }, 50);
    }
    return;
  }
  // Default fallback: log and let the FA figure it out (shouldn't happen if
  // every gate-key with action_label is wired above).
  console.warn('drawerGateAction: no handler for gate key', key);
}

loadDetail();
</script>

<!-- Health Drawer (Variant A — Quiet Pill). Lives at end of body so it can
     overlay the whole workbook. Populated by populateHealthDrawerKpis (from
     loadDetail's KPI block) + populateHealthDrawerActions (from
     renderReadinessInspector). FA directive 2026-05-14 Phase 4.
     FA dir 2026-05-19: added "Data Quality" tab alongside Readiness. -->
<div class="drawer-overlay" id="healthOverlay" onclick="closeHealthDrawer()"></div>
<aside class="drawer" id="healthDrawer" aria-hidden="true" aria-label="Health drawer">
  <div class="drawer-header">
    <h2>&#9889; Health</h2>
    <div id="healthSummary" style="font-size:11px; color:var(--gray-500);"></div>
    <button class="drawer-close" onclick="closeHealthDrawer()" aria-label="Close drawer">&#x2715;</button>
  </div>
  <div class="drawer-kpis" id="drawerKpis"></div>
  <!-- FA dir 2026-05-19: tab strip — Readiness (existing) + Data Quality + Recent Changes -->
  <div class="drawer-tabs" style="display:flex; gap:0; border-bottom:1px solid var(--gray-200); margin:8px 16px 0; padding:0;">
    <button class="drawer-tab active" data-drawer-tab="readiness"
            onclick="switchDrawerTab('readiness')"
            style="flex:1; padding:10px 8px; background:transparent; border:none; border-bottom:2px solid var(--blue, #1d4ed8); color:var(--blue, #1d4ed8); font:600 11px -apple-system,sans-serif; cursor:pointer; text-transform:uppercase; letter-spacing:0.04em;">Readiness</button>
    <button class="drawer-tab" data-drawer-tab="quality"
            onclick="switchDrawerTab('quality')"
            style="flex:1; padding:10px 8px; background:transparent; border:none; border-bottom:2px solid transparent; color:var(--gray-500, #64748b); font:600 11px -apple-system,sans-serif; cursor:pointer; text-transform:uppercase; letter-spacing:0.04em;">Data Quality <span id="drawerDqBadge" style="display:none; margin-left:4px; background:#dc2626; color:white; font-size:9px; padding:1px 5px; border-radius:999px; font-weight:700;">0</span></button>
    <button class="drawer-tab" data-drawer-tab="changes"
            onclick="switchDrawerTab('changes')"
            style="flex:1; padding:10px 8px; background:transparent; border:none; border-bottom:2px solid transparent; color:var(--gray-500, #64748b); font:600 11px -apple-system,sans-serif; cursor:pointer; text-transform:uppercase; letter-spacing:0.04em;">Recent Changes</button>
  </div>
  <div id="drawerActions"></div>
  <div id="drawerDataQuality" style="display:none;"></div>
  <div id="drawerRecentChanges" style="display:none;"></div>
</aside>

</body>
</html>
"""

