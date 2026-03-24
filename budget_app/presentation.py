"""
Client Presentation Blueprint for Century Management Budget System.

Provides shareable, live-editable budget presentations for client meetings.
- Token-based access (no login required — anyone with link can view/edit)
- Inline editing with real-time revision tracking
- Chart.js visualizations (expense breakdown, year-over-year comparison)
- Revision sidebar with polling
"""

import json
import uuid
import logging
from datetime import datetime, timedelta
from flask import Blueprint, render_template_string, request, jsonify

logger = logging.getLogger(__name__)


def create_presentation_blueprint(db, models, helpers):
    """
    Create presentation blueprint.

    Args:
        db: SQLAlchemy database instance
        models: dict of model classes from workflow blueprint
        helpers: dict of helper functions

    Returns:
        blueprint
    """
    Budget = models["Budget"]
    BudgetLine = models["BudgetLine"]
    PresentationSession = models["PresentationSession"]
    PresentationEdit = models["PresentationEdit"]
    BudgetRevision = models["BudgetRevision"]
    User = models["User"]

    record_revision = helpers["record_revision"]

    bp = Blueprint("presentation", __name__)

    # ─── Internal Routes (for FA/exec to manage presentations) ──────────

    @bp.route("/api/presentation/create/<entity_code>", methods=["POST"])
    def create_presentation(entity_code):
        """Generate a shareable presentation link for a budget."""
        data = request.get_json() or {}

        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        if budget.status not in ("presentation", "exec_review", "approved"):
            return jsonify({"error": f"Budget must be in presentation/exec_review/approved status, currently '{budget.status}'"}), 400

        # Generate token
        token = uuid.uuid4().hex + uuid.uuid4().hex[:32]

        # Set expiry (default 7 days)
        expires_hours = data.get("expires_hours", 168)
        expires_at = datetime.utcnow() + timedelta(hours=expires_hours)

        session = PresentationSession(
            budget_id=budget.id,
            token=token,
            created_by=data.get("user_id", 0),
            client_name=data.get("client_name", ""),
            notes=data.get("notes", ""),
            expires_at=expires_at,
        )
        db.session.add(session)

        # Store token on budget too
        budget.presentation_token = token
        budget.presentation_created_at = datetime.utcnow()

        record_revision(
            budget_id=budget.id,
            action="create",
            field_name="presentation_token",
            new_value=token[:12] + "...",
            notes=f"Presentation link created for {data.get('client_name', 'client')}",
            source="web",
        )

        db.session.commit()

        return jsonify({
            "token": token,
            "url": f"/present/{token}",
            "expires_at": expires_at.isoformat(),
            "session_id": session.id,
        })

    @bp.route("/api/presentation/deactivate/<token>", methods=["POST"])
    def deactivate_presentation(token):
        """Deactivate a presentation link."""
        session = PresentationSession.query.filter_by(token=token).first()
        if not session:
            return jsonify({"error": "Session not found"}), 404

        session.is_active = False
        db.session.commit()

        return jsonify({"status": "deactivated"})

    @bp.route("/presentation/manage", methods=["GET"])
    def manage_presentations():
        """Manage active presentation links."""
        sessions = (PresentationSession.query
                    .order_by(PresentationSession.created_at.desc())
                    .all())

        sessions_data = []
        for s in sessions:
            budget = Budget.query.get(s.budget_id)
            sessions_data.append({
                **s.to_dict(),
                "entity_code": budget.entity_code if budget else "?",
                "building_name": budget.building_name if budget else "?",
                "edit_count": len(s.edits),
            })

        return render_template_string(
            MANAGE_TEMPLATE,
            sessions_json=json.dumps(sessions_data),
        )

    # ─── Public Routes (token-based, no auth) ───────────────────────────

    @bp.route("/present/<token>", methods=["GET"])
    def presentation_view(token):
        """Client-facing budget presentation page."""
        session = PresentationSession.query.filter_by(token=token).first()
        if not session:
            return "Presentation not found", 404

        if not session.is_active:
            return "This presentation link has been deactivated", 403

        if session.expires_at and session.expires_at < datetime.utcnow():
            return "This presentation link has expired", 403

        budget = Budget.query.get(session.budget_id)
        if not budget:
            return "Budget not found", 404

        lines = (BudgetLine.query
                 .filter_by(budget_id=budget.id)
                 .order_by(BudgetLine.sheet_name, BudgetLine.row_num)
                 .all())

        lines_data = [l.to_dict() for l in lines]

        return render_template_string(
            PRESENTATION_TEMPLATE,
            token=token,
            building_name=budget.building_name,
            entity_code=budget.entity_code,
            year=budget.year,
            status=budget.status,
            client_name=session.client_name or "",
            lines_json=json.dumps(lines_data),
            budget_json=json.dumps(budget.to_dict()),
        )

    @bp.route("/api/present/<token>/data", methods=["GET"])
    def presentation_data(token):
        """Get current budget data as JSON."""
        session = PresentationSession.query.filter_by(token=token, is_active=True).first()
        if not session:
            return jsonify({"error": "Invalid or inactive session"}), 404

        budget = Budget.query.get(session.budget_id)
        lines = BudgetLine.query.filter_by(budget_id=budget.id).order_by(BudgetLine.sheet_name, BudgetLine.row_num).all()

        return jsonify({
            "budget": budget.to_dict(),
            "lines": [l.to_dict() for l in lines],
        })

    @bp.route("/api/present/<token>/edit", methods=["PUT"])
    def presentation_edit(token):
        """Save an edit made during a presentation."""
        session = PresentationSession.query.filter_by(token=token, is_active=True).first()
        if not session:
            return jsonify({"error": "Invalid or inactive session"}), 404

        data = request.get_json()
        line_id = data.get("line_id")
        field = data.get("field", "proposed_budget")
        new_value = data.get("value")

        if not line_id:
            return jsonify({"error": "line_id required"}), 400

        line = BudgetLine.query.get(line_id)
        if not line or line.budget_id != session.budget_id:
            return jsonify({"error": "Line not found"}), 404

        old_value = str(getattr(line, field, ""))

        # Update the line
        if field == "proposed_budget":
            line.proposed_budget = float(new_value or 0)
        elif field == "notes":
            line.notes = str(new_value or "")

        # Record in presentation_edits
        edit = PresentationEdit(
            session_id=session.id,
            budget_line_id=line.id,
            field_name=field,
            old_value=old_value,
            new_value=str(new_value),
        )
        db.session.add(edit)

        # Record in audit trail
        record_revision(
            budget_id=session.budget_id,
            budget_line_id=line.id,
            action="presentation_edit",
            field_name=field,
            old_value=old_value,
            new_value=str(new_value),
            notes=f"Edited during presentation ({session.client_name})",
            source="presentation",
        )

        db.session.commit()

        return jsonify({"status": "saved", "edit_id": edit.id})

    @bp.route("/api/present/<token>/history", methods=["GET"])
    def presentation_history(token):
        """Get revision history for this presentation session."""
        session = PresentationSession.query.filter_by(token=token).first()
        if not session:
            return jsonify({"error": "Session not found"}), 404

        edits = (PresentationEdit.query
                 .filter_by(session_id=session.id)
                 .order_by(PresentationEdit.edited_at.desc())
                 .all())

        result = []
        for e in edits:
            line = BudgetLine.query.get(e.budget_line_id)
            result.append({
                **e.to_dict(),
                "gl_code": line.gl_code if line else "?",
                "description": line.description if line else "?",
            })

        return jsonify(result)

    @bp.route("/api/present/<token>/approve", methods=["POST"])
    def presentation_approve(token):
        """Mark budget as approved from the presentation."""
        session = PresentationSession.query.filter_by(token=token, is_active=True).first()
        if not session:
            return jsonify({"error": "Invalid or inactive session"}), 404

        data = request.get_json() or {}
        budget = Budget.query.get(session.budget_id)

        if budget.status not in ("presentation", "exec_review"):
            return jsonify({"error": f"Cannot approve from '{budget.status}' status"}), 400

        old_status = budget.status
        budget.status = "approved"
        budget.approved_at = datetime.utcnow()
        if data.get("increase_pct") is not None:
            budget.increase_pct = float(data["increase_pct"])
        if data.get("effective_date"):
            budget.effective_date = data["effective_date"]

        record_revision(
            budget_id=budget.id,
            action="status_change",
            field_name="status",
            old_value=old_status,
            new_value="approved",
            notes=f"Approved during presentation ({session.client_name})",
            source="presentation",
        )

        session.is_active = False
        db.session.commit()

        return jsonify({"status": "approved", "budget": budget.to_dict()})

    return bp


# ─── HTML Templates ──────────────────────────────────────────────────────────

MANAGE_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Manage Presentations - Century Management</title>
<style>
  :root { --blue: #1a56db; --blue-light: #e1effe; --green: #057a55; --green-light: #def7ec; --red: #e02424; --gray-50: #f9fafb; --gray-100: #f3f4f6; --gray-200: #e5e7eb; --gray-500: #6b7280; --gray-900: #111827; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--gray-50); color: var(--gray-900); line-height: 1.5; }
  header { background: linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%); color: white; padding: 24px 20px; }
  header h1 { font-size: 24px; }
  .nav { padding: 12px 20px; background: white; border-bottom: 1px solid var(--gray-200); }
  .nav a { color: var(--blue); text-decoration: none; font-size: 14px; margin-right: 16px; }
  .container { max-width: 1200px; margin: 0 auto; padding: 32px 20px; }
  table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; border: 1px solid var(--gray-200); }
  th { padding: 12px; text-align: left; font-size: 12px; font-weight: 600; color: var(--gray-500); text-transform: uppercase; background: var(--gray-50); border-bottom: 1px solid var(--gray-200); }
  td { padding: 12px; border-bottom: 1px solid var(--gray-100); font-size: 14px; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .badge-active { background: var(--green-light); color: var(--green); }
  .badge-inactive { background: var(--gray-100); color: var(--gray-500); }
  .btn-sm { padding: 4px 10px; border: none; border-radius: 4px; font-size: 12px; font-weight: 600; cursor: pointer; }
  .btn-red { background: #fde8e8; color: var(--red); }
  .btn-red:hover { background: #fca5a5; }
  a.link { color: var(--blue); text-decoration: none; font-size: 13px; }
</style>
</head>
<body>
<header><h1>Manage Presentations</h1></header>
<div class="nav">
  <a href="/">Home</a>
  <a href="/dashboard">Dashboard</a>
  <a href="/presentation/manage">Presentations</a>
</div>
<div class="container">
  <table>
    <thead><tr><th>Building</th><th>Client</th><th>Status</th><th>Edits</th><th>Created</th><th>Link</th><th>Actions</th></tr></thead>
    <tbody id="sessionsBody"></tbody>
  </table>
</div>
<script>
const sessions = {{ sessions_json | safe }};
const tbody = document.getElementById('sessionsBody');
sessions.forEach(s => {
  const tr = document.createElement('tr');
  const active = s.is_active;
  tr.innerHTML = `
    <td><strong>${s.entity_code}</strong> - ${s.building_name}</td>
    <td>${s.client_name || '-'}</td>
    <td><span class="badge ${active ? 'badge-active' : 'badge-inactive'}">${active ? 'Active' : 'Inactive'}</span></td>
    <td>${s.edit_count}</td>
    <td>${new Date(s.created_at).toLocaleDateString()}</td>
    <td>${active ? `<a class="link" href="/present/${s.token}" target="_blank">Open</a>` : '-'}</td>
    <td>${active ? `<button class="btn-sm btn-red" onclick="deactivate('${s.token}')">Deactivate</button>` : ''}</td>
  `;
  tbody.appendChild(tr);
});

async function deactivate(token) {
  if (!confirm('Deactivate this presentation link?')) return;
  await fetch('/api/presentation/deactivate/' + token, {method: 'POST'});
  location.reload();
}
</script>
</body>
</html>
"""


PRESENTATION_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ building_name }} - 2027 Budget Presentation</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --blue: #1a56db; --blue-light: #e1effe;
    --green: #057a55; --green-light: #def7ec;
    --red: #e02424; --yellow: #f59e0b;
    --gray-50: #f9fafb; --gray-100: #f3f4f6; --gray-200: #e5e7eb;
    --gray-300: #d1d5db; --gray-500: #6b7280; --gray-700: #374151; --gray-900: #111827;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: white; line-height: 1.5; }

  /* Header */
  .header {
    background: linear-gradient(135deg, #1e3a5f 0%, #0f172a 100%);
    padding: 32px 40px;
    border-bottom: 1px solid #334155;
  }
  .header h1 { font-size: 28px; font-weight: 700; }
  .header .subtitle { color: #94a3b8; font-size: 14px; margin-top: 4px; }
  .header .client { color: #60a5fa; font-size: 16px; margin-top: 8px; }

  /* Layout */
  .main { display: grid; grid-template-columns: 1fr 320px; min-height: calc(100vh - 120px); }
  .content { padding: 24px 32px; overflow-y: auto; }
  .sidebar { background: #1e293b; border-left: 1px solid #334155; padding: 20px; overflow-y: auto; }

  /* Summary cards */
  .summary-cards { display: grid; grid-template-columns: repeat(5, 1fr); gap: 14px; margin-bottom: 28px; }
  .s-card { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px; text-align: center; }
  .s-card .value { font-size: 24px; font-weight: 700; color: #60a5fa; }
  .s-card .label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }

  /* Charts */
  .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 28px; }
  .chart-box { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px; }
  .chart-box h3 { font-size: 14px; color: #94a3b8; margin-bottom: 12px; }
  canvas { max-height: 250px; }

  /* Detail table */
  .detail-section { margin-bottom: 16px; }
  .section-toggle {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 8px;
    padding: 12px 16px;
    cursor: pointer;
    display: flex;
    justify-content: space-between;
    align-items: center;
    color: white;
    font-weight: 600;
    font-size: 14px;
    width: 100%;
  }
  .section-toggle:hover { background: #334155; }
  .section-body { display: none; margin-top: 2px; }
  .section-body.open { display: block; }

  table { width: 100%; border-collapse: collapse; }
  th { padding: 8px 12px; text-align: left; font-size: 11px; color: #94a3b8; text-transform: uppercase; border-bottom: 1px solid #334155; background: #0f172a; }
  th.number { text-align: right; }
  td { padding: 8px 12px; border-bottom: 1px solid #1e293b; font-size: 13px; }
  td.number { text-align: right; font-variant-numeric: tabular-nums; }
  tr:hover { background: rgba(96, 165, 250, 0.05); }

  /* Editable cells */
  .editable {
    cursor: pointer;
    padding: 4px 8px;
    border-radius: 4px;
    transition: background 0.15s;
  }
  .editable:hover { background: rgba(96, 165, 250, 0.15); }
  .editable.edited { background: rgba(250, 204, 21, 0.15); border: 1px solid rgba(250, 204, 21, 0.3); }
  .editable input {
    background: #0f172a;
    border: 1px solid #60a5fa;
    color: white;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 13px;
    text-align: right;
    width: 120px;
  }

  .subtotal td { background: #1e293b; font-weight: 700; color: #e2e8f0; }
  .grand-total td { background: #1e3a5f; font-weight: 700; color: white; font-size: 14px; }

  /* Sidebar */
  .sidebar h3 { font-size: 14px; color: #94a3b8; margin-bottom: 14px; text-transform: uppercase; letter-spacing: 0.5px; }
  .edit-item {
    background: #0f172a;
    border: 1px solid #334155;
    border-radius: 6px;
    padding: 10px;
    margin-bottom: 8px;
    font-size: 12px;
  }
  .edit-item .time { color: #64748b; font-size: 11px; }
  .edit-item .gl { color: #60a5fa; font-weight: 600; }
  .edit-item .change { color: #fbbf24; }
  .empty-sidebar { color: #64748b; text-align: center; padding: 40px 0; font-size: 13px; }

  /* Footer */
  .footer {
    background: #1e293b;
    border-top: 1px solid #334155;
    padding: 16px 32px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .btn {
    padding: 10px 24px;
    border: none;
    border-radius: 6px;
    font-weight: 600;
    cursor: pointer;
    font-size: 14px;
    transition: all 0.15s;
  }
  .btn-approve { background: #059669; color: white; }
  .btn-approve:hover { background: #047857; }
  .btn-outline { background: transparent; color: #94a3b8; border: 1px solid #475569; }
  .btn-outline:hover { color: white; border-color: #94a3b8; }

  @media print {
    .sidebar, .footer { display: none; }
    .main { grid-template-columns: 1fr; }
    body { background: white; color: black; }
    .header { background: white; color: black; border-bottom: 2px solid #000; }
    .s-card { background: #f8f8f8; border: 1px solid #ddd; }
    .s-card .value { color: #1a56db; }
  }
</style>
</head>
<body>

<div class="header">
  <h1>{{ building_name }}</h1>
  <div class="subtitle">Entity {{ entity_code }} &mdash; {{ year }} Budget</div>
  {% if client_name %}<div class="client">Prepared for: {{ client_name }}</div>{% endif %}
</div>

<div class="main">
  <div class="content">
    <div class="summary-cards" id="summaryCards"></div>
    <div class="charts">
      <div class="chart-box">
        <h3>Expense Breakdown</h3>
        <canvas id="pieChart"></canvas>
      </div>
      <div class="chart-box">
        <h3>Year-over-Year Comparison</h3>
        <canvas id="barChart"></canvas>
      </div>
    </div>
    <div id="detailSections"></div>
  </div>

  <div class="sidebar">
    <h3>Changes</h3>
    <div id="editsList"><div class="empty-sidebar">No changes yet. Click any Proposed Budget value to edit.</div></div>
  </div>
</div>

<div class="footer">
  <div>
    <button class="btn btn-outline" onclick="window.print()">Print</button>
  </div>
  <div style="display:flex;gap:10px;">
    <button class="btn btn-approve" onclick="approveFromPresentation()">Approve Budget</button>
  </div>
</div>

<script>
const TOKEN = "{{ token }}";
const LINES = {{ lines_json | safe }};
const BUDGET = {{ budget_json | safe }};
let editHistory = [];

function fmt(n) {
    if (n == null || isNaN(n)) return '$0';
    return '$' + Math.round(n).toLocaleString();
}

// Group lines by sheet
function groupBySheet() {
    const groups = {};
    LINES.forEach(l => {
        const sheet = l.sheet_name || 'Other';
        if (!groups[sheet]) groups[sheet] = [];
        groups[sheet].push(l);
    });
    return groups;
}

function computeTotals() {
    const sheets = groupBySheet();
    const totals = {};
    let grandPrior = 0, grandCurrent = 0, grandProposed = 0, grandYtd = 0;

    for (const [sheet, lines] of Object.entries(sheets)) {
        let prior = 0, current = 0, proposed = 0, ytd = 0;
        lines.forEach(l => {
            prior += (l.prior_year || 0);
            current += (l.current_budget || 0);
            proposed += (l.proposed_budget || l.current_budget || 0);
            ytd += (l.ytd_actual || 0);
        });
        totals[sheet] = {prior, current, proposed, ytd, count: lines.length};
        grandPrior += prior;
        grandCurrent += current;
        grandProposed += proposed;
        grandYtd += ytd;
    }
    return {sheets: totals, grandPrior, grandCurrent, grandProposed, grandYtd};
}

function renderSummary() {
    const t = computeTotals();
    const changePct = t.grandCurrent > 0 ? ((t.grandProposed - t.grandCurrent) / t.grandCurrent * 100).toFixed(1) : '0.0';
    const noi = t.grandProposed; // simplified

    document.getElementById('summaryCards').innerHTML = `
        <div class="s-card"><div class="value">${fmt(t.grandPrior)}</div><div class="label">Prior Year</div></div>
        <div class="s-card"><div class="value">${fmt(t.grandYtd)}</div><div class="label">YTD Actual</div></div>
        <div class="s-card"><div class="value">${fmt(t.grandCurrent)}</div><div class="label">Current Budget</div></div>
        <div class="s-card"><div class="value" style="color:#fbbf24">${fmt(t.grandProposed)}</div><div class="label">Proposed Budget</div></div>
        <div class="s-card"><div class="value" style="color:${parseFloat(changePct) > 0 ? '#f87171' : '#34d399'}">${changePct}%</div><div class="label">Change</div></div>
    `;
}

function renderCharts() {
    const t = computeTotals();
    const sheetNames = Object.keys(t.sheets);
    const colors = ['#3b82f6','#ef4444','#f59e0b','#10b981','#8b5cf6','#ec4899','#6366f1'];

    // Pie chart
    new Chart(document.getElementById('pieChart'), {
        type: 'doughnut',
        data: {
            labels: sheetNames,
            datasets: [{
                data: sheetNames.map(s => Math.round(t.sheets[s].proposed)),
                backgroundColor: colors.slice(0, sheetNames.length),
                borderWidth: 0,
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'right', labels: { color: '#94a3b8', font: {size: 11} } }
            }
        }
    });

    // Bar chart
    new Chart(document.getElementById('barChart'), {
        type: 'bar',
        data: {
            labels: sheetNames,
            datasets: [
                { label: 'Prior Year', data: sheetNames.map(s => Math.round(t.sheets[s].prior)), backgroundColor: '#475569' },
                { label: 'Current', data: sheetNames.map(s => Math.round(t.sheets[s].current)), backgroundColor: '#3b82f6' },
                { label: 'Proposed', data: sheetNames.map(s => Math.round(t.sheets[s].proposed)), backgroundColor: '#fbbf24' },
            ]
        },
        options: {
            responsive: true,
            scales: {
                x: { ticks: { color: '#94a3b8', font: {size: 10} }, grid: { color: '#1e293b' } },
                y: { ticks: { color: '#94a3b8', callback: v => '$' + (v/1000).toFixed(0) + 'k' }, grid: { color: '#1e293b' } }
            },
            plugins: { legend: { labels: { color: '#94a3b8', font: {size: 11} } } }
        }
    });
}

function renderDetails() {
    const container = document.getElementById('detailSections');
    container.innerHTML = '';
    const sheets = groupBySheet();
    const t = computeTotals();

    for (const [sheet, lines] of Object.entries(sheets)) {
        const section = document.createElement('div');
        section.className = 'detail-section';

        const totals = t.sheets[sheet];

        section.innerHTML = `
            <button class="section-toggle" onclick="this.nextElementSibling.classList.toggle('open')">
                <span>${sheet} (${lines.length} lines) &mdash; Proposed: ${fmt(totals.proposed)}</span>
                <span>&#9660;</span>
            </button>
            <div class="section-body">
                <table>
                    <thead><tr>
                        <th>GL Code</th><th>Description</th>
                        <th class="number">Prior Year</th><th class="number">YTD Actual</th>
                        <th class="number">Current Budget</th><th class="number">Proposed Budget</th>
                    </tr></thead>
                    <tbody>
                        ${lines.map(l => `
                            <tr>
                                <td style="color:#64748b">${l.gl_code}</td>
                                <td>${l.description}</td>
                                <td class="number">${fmt(l.prior_year)}</td>
                                <td class="number">${fmt(l.ytd_actual)}</td>
                                <td class="number">${fmt(l.current_budget)}</td>
                                <td class="number">
                                    <span class="editable ${l._edited ? 'edited' : ''}"
                                          onclick="startEdit(this, ${l.id}, '${l.gl_code}')"
                                          id="prop_${l.id}">
                                        ${fmt(l.proposed_budget || l.current_budget)}
                                    </span>
                                </td>
                            </tr>
                        `).join('')}
                        <tr class="subtotal">
                            <td colspan="2"><strong>Total ${sheet}</strong></td>
                            <td class="number">${fmt(totals.prior)}</td>
                            <td class="number">${fmt(totals.ytd)}</td>
                            <td class="number">${fmt(totals.current)}</td>
                            <td class="number">${fmt(totals.proposed)}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        `;
        container.appendChild(section);
    }

    // Grand total
    const grand = document.createElement('div');
    grand.innerHTML = `
        <table><tr class="grand-total">
            <td colspan="2"><strong>GRAND TOTAL</strong></td>
            <td class="number">${fmt(t.grandPrior)}</td>
            <td class="number">${fmt(t.grandYtd)}</td>
            <td class="number">${fmt(t.grandCurrent)}</td>
            <td class="number">${fmt(t.grandProposed)}</td>
        </tr></table>
    `;
    container.appendChild(grand);
}

function startEdit(el, lineId, glCode) {
    const line = LINES.find(l => l.id === lineId);
    if (!line) return;

    const currentVal = Math.round(line.proposed_budget || line.current_budget || 0);
    el.innerHTML = `<input type="number" value="${currentVal}" onblur="finishEdit(this, ${lineId}, '${glCode}')" onkeydown="if(event.key==='Enter')this.blur()">`;
    el.querySelector('input').focus();
    el.querySelector('input').select();
}

async function finishEdit(input, lineId, glCode) {
    const newVal = parseFloat(input.value) || 0;
    const line = LINES.find(l => l.id === lineId);
    if (!line) return;

    const oldVal = line.proposed_budget || line.current_budget || 0;
    line.proposed_budget = newVal;
    line._edited = true;

    // Update display
    const el = document.getElementById('prop_' + lineId);
    if (el) {
        el.innerHTML = fmt(newVal);
        el.classList.add('edited');
    }

    // Save to server
    try {
        await fetch(`/api/present/${TOKEN}/edit`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({line_id: lineId, field: 'proposed_budget', value: newVal})
        });

        editHistory.unshift({
            gl_code: glCode,
            description: line.description,
            old_value: fmt(oldVal),
            new_value: fmt(newVal),
            time: new Date().toLocaleTimeString(),
        });
        renderEdits();
        renderSummary();
    } catch(e) {
        console.error('Save failed:', e);
    }
}

function renderEdits() {
    const list = document.getElementById('editsList');
    if (editHistory.length === 0) {
        list.innerHTML = '<div class="empty-sidebar">No changes yet.</div>';
        return;
    }
    list.innerHTML = editHistory.map(e => `
        <div class="edit-item">
            <div class="time">${e.time}</div>
            <div><span class="gl">${e.gl_code}</span> ${e.description}</div>
            <div class="change">${e.old_value} &rarr; ${e.new_value}</div>
        </div>
    `).join('');
}

async function approveFromPresentation() {
    const pct = prompt('Approved increase percentage (e.g. 3.5):');
    if (pct === null) return;
    const date = prompt('Effective date (YYYY-MM-DD):');
    if (!date) return;

    const resp = await fetch(`/api/present/${TOKEN}/approve`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({increase_pct: parseFloat(pct), effective_date: date})
    });
    const data = await resp.json();
    if (data.status === 'approved') {
        alert('Budget approved!');
    } else {
        alert('Error: ' + (data.error || 'Unknown'));
    }
}

// Poll for external edits every 5s
setInterval(async () => {
    try {
        const resp = await fetch(`/api/present/${TOKEN}/history`);
        const edits = await resp.json();
        if (edits.length > editHistory.length) {
            // New edits from elsewhere
            editHistory = edits.map(e => ({
                gl_code: e.gl_code,
                description: e.description,
                old_value: e.old_value,
                new_value: e.new_value,
                time: new Date(e.edited_at).toLocaleTimeString(),
            }));
            renderEdits();
        }
    } catch(e) {}
}, 5000);

// Initialize
renderSummary();
renderCharts();
renderDetails();
renderEdits();
</script>
</body>
</html>
"""
