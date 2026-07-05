# Extracted from workflow.py 2026-07-05 (clean-architecture tranche 1).
# BYTE-IDENTICAL constant — template edits happen HERE now. Keep the
# string style unchanged (raw vs non-raw matters for JS escapes; see
# the wizard-template-js-escapes memory / check_template_js gate).

ACTION_CENTER_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<title>Action Center — {{ entity_code }}</title>
<style>
  :root {
    --bg: #f5f6f8;
    --card: white;
    --border: #e5e7eb;
    --text: #111827;
    --muted: #6b7280;
    --gray-50: #f9fafb;
    --gray-100: #f3f4f6;
    --gray-200: #e5e7eb;
    --gray-300: #d1d5db;
    --gray-400: #9ca3af;
    --gray-500: #6b7280;
    --gray-700: #374151;
    --red: #ef4444;
    --red-50: #fef2f2;
    --red-100: #fee2e2;
    --red-700: #b91c1c;
    --green: #16a34a;
    --green-50: #f0fdf4;
    --green-100: #dcfce7;
    --green-700: #15803d;
    --amber: #f59e0b;
    --amber-50: #fffbeb;
    --amber-100: #fef3c7;
    --amber-700: #b45309;
    --blue: #5a4a3f;
    --blue-light: #f5efe7;
    --brown: #5a4a3f;
  }
  * { box-sizing: border-box; }
  body {
    font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-size: 13px;
    line-height: 1.5;
  }
  /* Top nav */
  .top-nav {
    background: white;
    border-bottom: 1px solid var(--gray-200);
    padding: 10px 24px;
    display: flex;
    align-items: center;
    gap: 24px;
  }
  .top-nav .nav-brand { font-weight: 700; color: var(--text); text-decoration: none; }
  .top-nav .nav-links { display: flex; gap: 16px; }
  .top-nav .nav-link { color: var(--muted); text-decoration: none; font-size: 13px; }
  .top-nav .nav-link.active { color: var(--blue); font-weight: 600; }
  .top-nav .breadcrumb { margin-left: auto; font-size: 12px; color: var(--muted); }

  .container { max-width: 1100px; margin: 16px auto 60px; padding: 0 24px; }

  /* Building header */
  .building-head {
    background: var(--brown);
    color: white;
    padding: 18px 22px;
    border-radius: 10px 10px 0 0;
  }
  .building-head h1 { margin: 0 0 4px; font-size: 20px; font-weight: 700; }
  .building-head .meta { font-size: 12px; opacity: 0.85; }

  /* Pipeline */
  .pipeline {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px 22px;
    background: white;
    border-left: 1px solid var(--gray-200);
    border-right: 1px solid var(--gray-200);
    font-size: 12px;
    flex-wrap: wrap;
  }
  .pipe-step {
    padding: 4px 12px;
    border-radius: 999px;
    background: var(--gray-100);
    color: var(--muted);
    font-weight: 500;
  }
  .pipe-step.done { background: var(--green-100); color: var(--green-700); }
  .pipe-step.current { background: var(--red-100); color: var(--red-700); font-weight: 700; }
  .pipe-arrow { color: var(--gray-400); }
  .pipe-pm-chip {
    margin-left: auto;
    font-size: 11px;
    padding: 3px 10px;
    background: var(--blue-light);
    color: var(--brown);
    border-radius: 4px;
  }

  /* KPI cards */
  .kpi-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
    padding: 10px 22px 16px;
    background: white;
    border: 1px solid var(--gray-200);
    border-top: none;
    border-radius: 0 0 10px 10px;
  }
  .kpi {
    padding: 10px 14px;
    border-radius: 8px;
    background: var(--gray-100);
  }
  .kpi.pos { background: var(--green-50); }
  .kpi.neg { background: var(--red-50); }
  .kpi .num { font-size: 18px; font-weight: 700; color: var(--text); }
  .kpi.pos .num { color: var(--green-700); }
  .kpi.neg .num { color: var(--red-700); }
  .kpi .lbl { font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; margin-top: 2px; }

  /* Action Center */
  .ac-wrap {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 10px;
    margin-top: 16px;
    overflow: hidden;
  }
  .ac-header {
    padding: 14px 20px;
    background: linear-gradient(to right, #fef3c7, white 35%);
    border-bottom: 1px solid var(--gray-200);
    display: flex;
    align-items: center;
    gap: 14px;
  }
  .ac-header .ac-title {
    font-size: 17px;
    font-weight: 700;
    color: #1f2937;
  }
  .ac-header .ac-tagline {
    font-size: 11px;
    color: var(--muted);
  }
  .ac-counts {
    margin-left: auto;
    display: flex;
    gap: 6px;
    font-size: 11px;
    font-weight: 700;
  }
  .ac-counts span {
    padding: 4px 10px;
    border-radius: 999px;
  }
  .ac-counts .blocker { background: var(--red-100); color: var(--red-700); }
  .ac-counts .warn { background: var(--amber-100); color: var(--amber-700); }
  .ac-counts .ok { background: var(--green-100); color: var(--green-700); }

  .ac-group {
    padding: 12px 20px;
    border-bottom: 1px solid var(--gray-100);
  }
  .ac-group:last-child { border-bottom: none; }
  .ac-group-title {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 8px;
  }
  .ac-group.blockers .ac-group-title { color: var(--red-700); }
  .ac-group.warnings .ac-group-title { color: var(--amber-700); }
  .ac-group.complete .ac-group-title { color: var(--green-700); cursor: pointer; user-select: none; }
  .ac-group.complete.collapsed .ac-items { display: none; }
  .ac-items {}

  .ac-item {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 0;
    border-top: 1px solid var(--gray-100);
    font-size: 13px;
  }
  .ac-item:first-of-type { border-top: none; }
  .ac-icon {
    width: 24px;
    height: 24px;
    border-radius: 50%;
    flex-shrink: 0;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 12px;
    font-weight: 700;
  }
  .ac-icon.bad { background: var(--red-100); color: var(--red-700); }
  .ac-icon.warn { background: var(--amber-100); color: var(--amber-700); }
  .ac-icon.ok { background: var(--green-100); color: var(--green-700); }
  .ac-text { flex: 1; }
  .ac-label { font-weight: 600; color: var(--text); }
  .ac-detail { font-size: 11px; color: var(--muted); }
  .ac-button {
    padding: 5px 12px;
    border-radius: 5px;
    font-size: 11px;
    font-weight: 600;
    background: var(--blue-light);
    color: var(--brown);
    text-decoration: none;
    cursor: pointer;
    border: 1px solid transparent;
    white-space: nowrap;
  }
  .ac-button:hover { border-color: var(--brown); }
  .ac-button.primary {
    background: var(--red);
    color: white;
  }
  .ac-button.primary:hover { background: var(--red-700); border-color: transparent; }
  .ac-item .ac-text + a.ac-button { margin-left: auto; }

  .ac-expand {
    margin: 6px 0 4px 36px;
    padding: 12px 14px;
    background: var(--amber-50);
    border: 1px solid var(--amber-100);
    border-radius: 8px;
    font-size: 11px;
    display: none;
  }
  .ac-expand.open { display: block; }
  .ac-expand ul { margin: 6px 0; padding-left: 18px; }
  .ac-expand li { padding: 2px 0; }
  .ac-expand code {
    background: white;
    padding: 1px 5px;
    border-radius: 3px;
    border: 1px solid var(--gray-200);
    font-size: 10px;
  }
  .ac-expand .ac-button { margin-right: 6px; }

  /* Pending edits chip */
  .pending-chip {
    margin: 14px 0;
    padding: 12px 18px;
    background: var(--amber);
    color: white;
    border-radius: 8px;
    display: none;
    align-items: center;
    gap: 10px;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    text-decoration: none;
  }
  .pending-chip.visible { display: flex; }
  .pending-chip:hover { background: var(--amber-700); }
  .pending-chip .arrow { margin-left: auto; opacity: 0.7; }

  /* CTAs */
  .cta-row {
    display: grid;
    grid-template-columns: 2fr 1fr 1fr;
    gap: 10px;
    margin-top: 16px;
  }
  .cta {
    padding: 18px 18px;
    border-radius: 10px;
    background: white;
    border: 1px solid var(--gray-200);
    cursor: pointer;
    display: flex;
    flex-direction: column;
    gap: 6px;
    text-decoration: none;
    color: var(--text);
  }
  .cta:hover { border-color: var(--brown); background: var(--blue-light); }
  .cta.primary {
    background: var(--brown);
    color: white;
    border-color: var(--brown);
  }
  .cta.primary:hover { background: #4a3a30; color: white; }
  .cta .cta-title { font-size: 15px; font-weight: 700; }
  .cta .cta-sub { font-size: 11px; opacity: 0.75; }

  .loading {
    text-align: center;
    padding: 40px;
    color: var(--muted);
    font-size: 13px;
  }
  .all-clear {
    padding: 24px;
    text-align: center;
    color: var(--green-700);
    font-size: 14px;
  }
</style>
</head>
<body>

<nav class="top-nav">
  <a href="/" class="nav-brand">Century Management</a>
  <div class="nav-links">
    <a href="/" class="nav-link">Home</a>
    <a href="/dashboard" class="nav-link">FA Dashboard</a>
    <a href="/pm" class="nav-link">PM Portal</a>
    <a href="/audited-financials" class="nav-link">Audited Financials</a>
  </div>
  <div class="breadcrumb"><span id="bcEntity"></span> · Action Center</div>
</nav>

<div class="container">
  <!-- Building header strip -->
  <div class="building-head">
    <h1 id="buildingName">Loading...</h1>
    <div class="meta" id="buildingMeta"></div>
  </div>
  <!-- Status pipeline -->
  <div class="pipeline" id="statusPipeline"></div>
  <!-- KPI cards -->
  <div class="kpi-row" id="kpiCards"></div>

  <!-- Action Center panel -->
  <div class="ac-wrap" id="acWrap" style="display:none;">
    <div class="ac-header">
      <div>
        <div class="ac-title">⚡ Action Center</div>
        <div class="ac-tagline" id="acTagline"></div>
      </div>
      <div class="ac-counts" id="acCounts"></div>
    </div>
    <div id="acGroupBlockers" class="ac-group blockers" style="display:none;">
      <div class="ac-group-title">🔴 Blockers — must resolve before generating</div>
      <div class="ac-items" id="acBlockersItems"></div>
    </div>
    <div id="acGroupWarnings" class="ac-group warnings" style="display:none;">
      <div class="ac-group-title">🟡 Warnings — review before submitting</div>
      <div class="ac-items" id="acWarningsItems"></div>
    </div>
    <div id="acGroupComplete" class="ac-group complete collapsed">
      <div class="ac-group-title" onclick="document.getElementById('acGroupComplete').classList.toggle('collapsed');">
        <span id="acCompleteToggle">▶</span> <span id="acCompleteLabel">Complete — click to expand</span>
      </div>
      <div class="ac-items" id="acCompleteItems"></div>
    </div>
  </div>

  <!-- Pending edits chip (hidden when 0) -->
  <a href="/dashboard/{{ entity_code }}#pmReviewPanel" class="pending-chip" id="pendingChip">
    <span>⚠ <span id="pendingCount">0</span> items pending PM/FA review</span>
    <span class="arrow">→</span>
  </a>

  <!-- CTAs -->
  <div class="cta-row">
    <a href="/dashboard/{{ entity_code }}" class="cta primary">
      <div class="cta-title">📊 Open Workbook →</div>
      <div class="cta-sub">Edit budget numbers across all tabs</div>
    </a>
    <a href="#" id="boardPresLink" class="cta">
      <div class="cta-title">📋 Board Presentation</div>
      <div class="cta-sub">Generate slide deck</div>
    </a>
    <a href="" id="downloadExcelLink" class="cta">
      <div class="cta-title">📥 Download Excel</div>
      <div class="cta-sub">Export current state</div>
    </a>
  </div>

  <!-- Initial loading state -->
  <div id="acLoading" class="loading">Loading building health…</div>
</div>

<script>
const entityCode = '{{ entity_code }}';
const BY = {{ budget_year }};
document.getElementById('bcEntity').textContent = entityCode;
// /api/budget/<e>/export was never a registered route (dead 404). Point at the
// real, working full-budget export (live SUMIF/forecast/proposed formulas).
document.getElementById('downloadExcelLink').href = '/api/export-excel/' + entityCode;

function fmt(n) {
  if (n == null || isNaN(n)) return '$0';
  const v = Math.round(n);
  return (v < 0 ? '-$' : '$') + Math.abs(v).toLocaleString();
}

// Group complete-toggle indicator
const cg = document.getElementById('acGroupComplete');
const obs = new MutationObserver(() => {
  const t = document.getElementById('acCompleteToggle');
  if (t) t.textContent = cg.classList.contains('collapsed') ? '▶' : '▼';
});
obs.observe(cg, { attributes: true, attributeFilter: ['class'] });

// Fetch everything we need in parallel
Promise.all([
  fetch('/api/dashboard/' + entityCode).then(r => r.json()),
  fetch('/api/readiness/' + entityCode).then(r => r.json()),
  fetch('/api/summary/' + entityCode).then(r => r.json()).catch(() => ({warnings: []})),
  fetch('/api/wizard/' + entityCode + '/scan-findings').then(r => r.json()).catch(() => null),
]).then(([dash, readiness, summary, scan]) => {
  // Header
  const b = dash.budget || {};
  const a = dash.assignments || {};
  document.getElementById('buildingName').textContent = b.building_name || entityCode;
  document.getElementById('buildingMeta').textContent =
    'Entity ' + entityCode + ' | ' + BY + ' Budget' +
    (a.fa ? ' | FA: ' + a.fa : '') +
    (a.pm ? ' | PM: ' + a.pm : '');

  // Pipeline
  const status = (b.status || 'draft').toLowerCase();
  const stages = [
    {key: 'draft', label: 'Draft'},
    {key: 'pm_pending', label: 'PM Review', match: ['pm_pending', 'pm_in_progress']},
    {key: 'fa_review', label: 'FA Review'},
    {key: 'approved', label: 'Approved'},
  ];
  const order = ['draft', 'pm_pending', 'pm_in_progress', 'fa_review', 'approved'];
  const curIdx = Math.max(0, order.indexOf(status));
  let pipeHtml = '';
  stages.forEach((s, i) => {
    if (i > 0) pipeHtml += '<span class="pipe-arrow">›</span>';
    let cls = 'pipe-step';
    const stageOrders = s.match ? s.match.map(k => order.indexOf(k)) : [order.indexOf(s.key)];
    const stageMin = Math.min.apply(null, stageOrders);
    const stageMax = Math.max.apply(null, stageOrders);
    if (stageMax < curIdx) cls += ' done';
    else if (stageMin <= curIdx && curIdx <= stageMax) cls += ' current';
    pipeHtml += '<span class="' + cls + '">' + s.label + '</span>';
  });
  // Inline PM chip with sent-at / awaiting info
  const pmStatusStr = (function() {
    if (status === 'pm_pending') return 'Sent ' + (b.pm_sent_at ? new Date(b.pm_sent_at).toLocaleDateString() : '') + ' · awaiting PM';
    if (status === 'pm_in_progress') return 'PM editing · sent ' + (b.pm_sent_at ? new Date(b.pm_sent_at).toLocaleDateString() : '');
    if (status === 'fa_review') return 'PM returned · ready for FA';
    if (status === 'approved') return 'Approved';
    if (a.pm) return 'PM: ' + a.pm + ' — not yet sent';
    return '';
  })();
  if (pmStatusStr) {
    pipeHtml += '<span class="pipe-pm-chip">' + pmStatusStr + '</span>';
  }
  document.getElementById('statusPipeline').innerHTML = pipeHtml;

  // KPI cards — totals are computed by summing the lines array (same as
  // the workbook dashboard does — Budget object has no precomputed totals)
  const lines = dash.lines || [];
  let prior = 0, current = 0;
  lines.forEach(l => {
    prior += l.prior_year || 0;
    current += l.current_budget || 0;
  });
  const variance = current - prior;
  const pctChange = prior ? (variance / prior) * 100 : 0;
  const varPos = variance >= 0;
  document.getElementById('kpiCards').innerHTML =
    '<div class="kpi"><div class="num">' + fmt(prior) + '</div><div class="lbl">Prior Year</div></div>' +
    '<div class="kpi"><div class="num">' + fmt(current) + '</div><div class="lbl">Current Budget</div></div>' +
    '<div class="kpi ' + (varPos ? 'pos' : 'neg') + '"><div class="num">' + (varPos ? '+' : '') + fmt(variance) + '</div><div class="lbl">Variance</div></div>' +
    '<div class="kpi ' + (varPos ? 'pos' : 'neg') + '"><div class="num">' + (prior ? (varPos ? '+' : '') + pctChange.toFixed(1) + '% ' + (varPos ? '▲' : '▼') : '—') + '</div><div class="lbl">% Change</div></div>';

  // ── Consolidate items into Blockers / Warnings / Complete ──
  const blockers = [];
  const warnings = [];
  const complete = [];

  // From readiness gates
  const gates = readiness.gates || [];
  gates.forEach(g => {
    // Resolve the right action url per gate — fix the broken JSON jump for approved_file_labels
    let actionUrl = g.action_url || '';
    if (g.key === 'approved_file_labels') {
      actionUrl = '#expand-labels';   // inline-expand instead of JSON page
    }
    // Pipe gate into the right bucket
    const item = {
      key: g.key,
      icon: g.status === 'ok' ? 'ok' : (g.status === 'warn' ? 'warn' : 'bad'),
      label: g.label,
      detail: g.detail || '',
      actionUrl: actionUrl,
      actionLabel: g.action_label || '',
      source: 'readiness',
    };
    if (g.status === 'fail') blockers.push(item);
    else if (g.status === 'warn') warnings.push(item);
    else if (g.status === 'ok') complete.push(item);
    // 'skip' status = ignore
  });

  // Add scan findings inline-expand data (if present)
  const scanFindings = (scan && scan.unmapped_labels) || [];
  const filesAvailable = scan && scan.has_file;

  // De-dupe: if readiness already has approved_file_labels we won't add a second one
  // from summary warnings about labels (it's the same data).

  // From /api/summary warnings — orphan GLs + duplicate rows are already in readiness
  // gates; we only add NEW info if any
  // (Skip — readiness covers the same items. This dedupes by design.)

  // Set the tagline + counts
  const total = blockers.length + warnings.length + complete.length;
  document.getElementById('acTagline').textContent =
    blockers.length === 0 && warnings.length === 0
      ? '✓ All clear — ready to generate'
      : blockers.length + ' blocker' + (blockers.length === 1 ? '' : 's') +
        ' · ' + warnings.length + ' warning' + (warnings.length === 1 ? '' : 's') +
        ' to resolve';

  document.getElementById('acCounts').innerHTML =
    '<span class="blocker">' + blockers.length + ' blockers</span>' +
    '<span class="warn">' + warnings.length + ' warnings</span>' +
    '<span class="ok">' + complete.length + ' ready</span>';

  document.getElementById('acCompleteLabel').textContent =
    complete.length + ' thing' + (complete.length === 1 ? '' : 's') + ' complete';

  // Render groups
  function renderItem(it, expandable) {
    let extra = '';
    if (it.key === 'approved_file_labels' && scanFindings.length > 0) {
      let lis = scanFindings.slice(0, 10).map(u => {
        const lbl = (u.label || '').replace(/</g, '&lt;');
        const sug = u.suggested ? '<span style="color:#6b7280;"> → ' + u.suggested.replace(/</g, '&lt;') + '</span>' : '';
        return '<li><code>' + lbl + '</code>' + sug + '</li>';
      }).join('');
      const more = scanFindings.length > 10 ? '<li style="color:#6b7280;">+' + (scanFindings.length - 10) + ' more</li>' : '';
      extra = '<div class="ac-expand" id="acExpand-' + it.key + '">' +
        '<strong>' + scanFindings.length + ' unmapped label' + (scanFindings.length === 1 ? '' : 's') +
        (scan.file_name ? ' in <code>' + scan.file_name.replace(/</g, '&lt;') + '</code>' : '') + ':</strong>' +
        '<ul>' + lis + more + '</ul>' +
        '<a class="ac-button" href="/admin/portfolio-health">Open standard label map</a> ' +
        '<a class="ac-button" href="/api/wizard/' + entityCode + '/scan-findings?refresh=1" onclick="event.preventDefault(); fetch(this.href, {method: \'POST\'}).then(()=>location.reload());">Re-scan</a>' +
      '</div>';
    }
    const btnCls = it.icon === 'bad' ? 'ac-button primary' : 'ac-button';
    let actionHtml = '';
    if (it.actionLabel && it.actionUrl) {
      if (it.actionUrl.startsWith('#expand-')) {
        actionHtml = '<a class="' + btnCls + '" href="#" onclick="event.preventDefault(); document.getElementById(\'acExpand-' + it.key + '\').classList.toggle(\'open\');">' + it.actionLabel + '</a>';
      } else if (it.actionUrl.startsWith('#tab=')) {
        // Switch to workbook + jump to tab anchor
        actionHtml = '<a class="' + btnCls + '" href="/dashboard/' + entityCode + it.actionUrl + '">' + it.actionLabel + '</a>';
      } else if (it.actionUrl.startsWith('#')) {
        actionHtml = '<a class="' + btnCls + '" href="/dashboard/' + entityCode + it.actionUrl + '">' + it.actionLabel + '</a>';
      } else {
        actionHtml = '<a class="' + btnCls + '" href="' + it.actionUrl + '">' + it.actionLabel + '</a>';
      }
    }
    return '<div class="ac-item">' +
      '<span class="ac-icon ' + it.icon + '">' + (it.icon === 'ok' ? '✓' : (it.icon === 'warn' ? '!' : '✕')) + '</span>' +
      '<div class="ac-text">' +
        '<div class="ac-label">' + it.label.replace(/</g, '&lt;') + '</div>' +
        '<div class="ac-detail">' + (it.detail || '').replace(/</g, '&lt;') + '</div>' +
      '</div>' +
      actionHtml +
    '</div>' + extra;
  }

  document.getElementById('acBlockersItems').innerHTML = blockers.map(renderItem).join('');
  document.getElementById('acWarningsItems').innerHTML = warnings.map(renderItem).join('');
  document.getElementById('acCompleteItems').innerHTML = complete.map(renderItem).join('');

  document.getElementById('acGroupBlockers').style.display = blockers.length ? '' : 'none';
  document.getElementById('acGroupWarnings').style.display = warnings.length ? '' : 'none';
  document.getElementById('acWrap').style.display = '';
  document.getElementById('acLoading').style.display = 'none';

  // Pending edits chip
  let pendingTotal = 0;
  try {
    const notesCount = (dash.pending_notes_count || 0);
    const reclassCount = (dash.pending_reclass_count || 0);
    const proposalsCount = (dash.pending_proposals_count || 0);
    pendingTotal = notesCount + reclassCount + proposalsCount;
  } catch (e) {}
  if (pendingTotal > 0) {
    document.getElementById('pendingCount').textContent = pendingTotal;
    document.getElementById('pendingChip').classList.add('visible');
  }

  // Board presentation link
  document.getElementById('boardPresLink').href = '/board-presentation/' + entityCode;
}).catch(err => {
  document.getElementById('acLoading').innerHTML =
    '<div style="color:#dc2626;">Failed to load: ' + err.message + '</div>' +
    '<a href="/dashboard/' + entityCode + '" style="color:#2563eb;">Open Workbook directly →</a>';
});
</script>

</body>
</html>
"""

