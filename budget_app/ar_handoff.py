"""
AR Handoff Blueprint for Century Management Budget System.

Handles the final phase of the budget pipeline:
- Auto-creates AR handoff records when budgets are approved
- Provides AR dashboard showing all pending handoffs
- AR form for acknowledging receipt and confirming Yardi entry
"""

import json
import logging
from datetime import datetime
from flask import Blueprint, render_template_string, request, jsonify

logger = logging.getLogger(__name__)


def create_ar_handoff_blueprint(db, models, helpers):
    """
    Create AR handoff blueprint.

    Args:
        db: SQLAlchemy database instance
        models: dict of model classes from workflow blueprint
        helpers: dict of helper functions

    Returns:
        blueprint
    """
    Budget = models["Budget"]
    BudgetLine = models["BudgetLine"]
    ARHandoff = models["ARHandoff"]
    User = models["User"]

    record_revision = helpers["record_revision"]

    bp = Blueprint("ar_handoff", __name__)

    # ─── Helper: Create Handoff Record ──────────────────────────────────

    def create_handoff_for_budget(budget):
        """Auto-create an AR handoff record from an approved budget."""
        existing = ARHandoff.query.filter_by(budget_id=budget.id).first()
        if existing:
            return existing

        # Compute totals from budget lines
        lines = BudgetLine.query.filter_by(budget_id=budget.id).all()
        total_current = sum(l.current_budget or 0 for l in lines)
        total_proposed = sum(l.proposed_budget or l.current_budget or 0 for l in lines)

        # Get approver name
        approver_name = ""
        if budget.approved_by:
            user = User.query.get(budget.approved_by)
            approver_name = user.name if user else ""

        handoff = ARHandoff(
            budget_id=budget.id,
            entity_code=budget.entity_code,
            building_name=budget.building_name,
            approved_increase_pct=budget.increase_pct or 0.0,
            effective_date=budget.effective_date or "",
            approved_by_name=approver_name,
            approved_at=budget.approved_at,
            total_current_budget=total_current,
            total_proposed_budget=total_proposed,
            supporting_notes=budget.ar_notes or budget.fa_notes or "",
        )
        db.session.add(handoff)
        return handoff

    # ─── API Routes ─────────────────────────────────────────────────────

    @bp.route("/api/ar/<entity_code>/create", methods=["POST"])
    def create_ar_handoff(entity_code):
        """Create AR handoff from an approved budget and advance status."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        if budget.status != "approved":
            return jsonify({"error": f"Budget must be approved, currently '{budget.status}'"}), 400

        handoff = create_handoff_for_budget(budget)
        budget.status = "ar_pending"

        record_revision(
            budget_id=budget.id,
            action="status_change",
            field_name="status",
            old_value="approved",
            new_value="ar_pending",
            notes="AR handoff created",
            source="system",
        )

        db.session.commit()

        return jsonify(handoff.to_dict())

    @bp.route("/api/ar/<entity_code>/acknowledge", methods=["PUT"])
    def acknowledge_handoff(entity_code):
        """AR acknowledges receipt of the handoff."""
        data = request.get_json() or {}

        handoff = ARHandoff.query.filter_by(entity_code=entity_code).first()
        if not handoff:
            return jsonify({"error": "Handoff not found"}), 404

        handoff.ar_status = "acknowledged"
        handoff.ar_acknowledged_by = data.get("acknowledged_by", "")
        handoff.ar_acknowledged_at = datetime.utcnow()

        record_revision(
            budget_id=handoff.budget_id,
            action="update",
            field_name="ar_status",
            old_value="pending",
            new_value="acknowledged",
            notes=f"Acknowledged by {handoff.ar_acknowledged_by}",
            source="web",
        )

        db.session.commit()
        return jsonify(handoff.to_dict())

    @bp.route("/api/ar/<entity_code>/enter", methods=["PUT"])
    def enter_in_yardi(entity_code):
        """AR marks the increase as entered in Yardi."""
        data = request.get_json() or {}

        handoff = ARHandoff.query.filter_by(entity_code=entity_code).first()
        if not handoff:
            return jsonify({"error": "Handoff not found"}), 404

        handoff.ar_status = "entered"
        handoff.ar_entered_at = datetime.utcnow()
        handoff.yardi_confirmation = data.get("yardi_confirmation", "")

        # Advance budget status to ar_complete
        budget = Budget.query.get(handoff.budget_id)
        if budget and budget.status == "ar_pending":
            budget.status = "ar_complete"
            record_revision(
                budget_id=budget.id,
                action="status_change",
                field_name="status",
                old_value="ar_pending",
                new_value="ar_complete",
                notes=f"Entered in Yardi. Confirmation: {handoff.yardi_confirmation}",
                source="web",
            )

        db.session.commit()
        return jsonify(handoff.to_dict())

    @bp.route("/api/ar/pending", methods=["GET"])
    def list_pending_handoffs():
        """List all AR handoffs."""
        handoffs = ARHandoff.query.order_by(ARHandoff.created_at.desc()).all()
        return jsonify([h.to_dict() for h in handoffs])

    # ─── Pages ──────────────────────────────────────────────────────────

    @bp.route("/ar", methods=["GET"])
    def ar_dashboard():
        """AR dashboard showing all buildings with pending handoffs."""
        handoffs = ARHandoff.query.order_by(ARHandoff.created_at.desc()).all()
        return render_template_string(
            AR_DASHBOARD_TEMPLATE,
            handoffs_json=json.dumps([h.to_dict() for h in handoffs]),
        )

    @bp.route("/ar/<entity_code>", methods=["GET"])
    def ar_form(entity_code):
        """AR form for a specific building."""
        handoff = ARHandoff.query.filter_by(entity_code=entity_code).first()
        if not handoff:
            return "Handoff not found for this building", 404

        return render_template_string(
            AR_FORM_TEMPLATE,
            handoff_json=json.dumps(handoff.to_dict()),
            entity_code=entity_code,
        )

    return bp, {"create_handoff_for_budget": create_handoff_for_budget}


# ─── HTML Templates ──────────────────────────────────────────────────────────

AR_DASHBOARD_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AR Dashboard - Century Management</title>
<style>
  :root { --blue: #1a56db; --blue-light: #e1effe; --green: #057a55; --green-light: #def7ec; --yellow: #f59e0b; --yellow-light: #fef3c7; --red: #e02424; --gray-50: #f9fafb; --gray-100: #f3f4f6; --gray-200: #e5e7eb; --gray-300: #d1d5db; --gray-500: #6b7280; --gray-700: #374151; --gray-900: #111827; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--gray-50); color: var(--gray-900); line-height: 1.5; }
  header { background: linear-gradient(135deg, #b45309 0%, #92400e 100%); color: white; padding: 24px 20px; }
  header h1 { font-size: 24px; font-weight: 700; }
  header p { opacity: 0.85; margin-top: 4px; }
  .nav { padding: 12px 20px; background: white; border-bottom: 1px solid var(--gray-200); }
  .nav a { color: var(--blue); text-decoration: none; font-size: 14px; margin-right: 16px; }
  .container { max-width: 1200px; margin: 0 auto; padding: 32px 20px; }

  .status-cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 24px; }
  .s-card { background: white; border: 1px solid var(--gray-200); border-radius: 8px; padding: 16px; text-align: center; }
  .s-card .count { font-size: 28px; font-weight: 700; color: var(--blue); }
  .s-card .label { font-size: 11px; color: var(--gray-500); text-transform: uppercase; }

  .table-wrap { background: white; border: 1px solid var(--gray-200); border-radius: 8px; overflow: hidden; }
  table { width: 100%; border-collapse: collapse; }
  th { padding: 12px 14px; text-align: left; font-size: 12px; font-weight: 600; color: var(--gray-500); text-transform: uppercase; background: var(--gray-50); border-bottom: 1px solid var(--gray-200); }
  td { padding: 12px 14px; border-bottom: 1px solid var(--gray-100); font-size: 14px; }
  tr:hover { background: var(--gray-50); }

  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .badge-pending { background: var(--yellow-light); color: #92400e; }
  .badge-acknowledged { background: var(--blue-light); color: var(--blue); }
  .badge-entered { background: var(--green-light); color: var(--green); }
  .badge-verified { background: var(--green-light); color: var(--green); }

  .btn-sm { padding: 5px 12px; border: none; border-radius: 4px; font-size: 12px; font-weight: 600; cursor: pointer; text-decoration: none; display: inline-block; }
  .btn-blue { background: var(--blue); color: white; }
  .btn-blue:hover { background: #1e429f; }
</style>
</head>
<body>
<header>
  <h1>AR Department Dashboard</h1>
  <p>Budget increase handoffs &mdash; acknowledge and enter into Yardi</p>
</header>
<div class="nav">
  <a href="/">Home</a>
  <a href="/dashboard">FA Dashboard</a>
  <a href="/pipeline">Pipeline</a>
  <a href="/ar">AR Dashboard</a>
</div>

<div class="container">
  <div class="status-cards" id="statusCards"></div>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Entity</th>
          <th>Building</th>
          <th>Increase %</th>
          <th>Effective Date</th>
          <th>Current Budget</th>
          <th>Proposed Budget</th>
          <th>Status</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody id="handoffsBody"></tbody>
    </table>
  </div>
</div>

<script>
const handoffs = {{ handoffs_json | safe }};

function fmt(n) {
    if (n == null || isNaN(n)) return '$0';
    return '$' + Math.round(n).toLocaleString();
}

function renderCards() {
    const counts = {pending: 0, acknowledged: 0, entered: 0};
    handoffs.forEach(h => { counts[h.ar_status] = (counts[h.ar_status] || 0) + 1; });

    document.getElementById('statusCards').innerHTML = `
        <div class="s-card"><div class="count">${handoffs.length}</div><div class="label">Total</div></div>
        <div class="s-card"><div class="count">${counts.pending || 0}</div><div class="label">Pending</div></div>
        <div class="s-card"><div class="count">${counts.acknowledged || 0}</div><div class="label">Acknowledged</div></div>
        <div class="s-card"><div class="count">${counts.entered || 0}</div><div class="label">Entered</div></div>
    `;
}

function renderTable() {
    const tbody = document.getElementById('handoffsBody');
    tbody.innerHTML = handoffs.map(h => `
        <tr>
            <td><strong>${h.entity_code}</strong></td>
            <td>${h.building_name}</td>
            <td style="font-weight:700;color:#b45309">${h.approved_increase_pct.toFixed(1)}%</td>
            <td>${h.effective_date || '-'}</td>
            <td>${fmt(h.total_current_budget)}</td>
            <td>${fmt(h.total_proposed_budget)}</td>
            <td><span class="badge badge-${h.ar_status}">${h.ar_status}</span></td>
            <td><a href="/ar/${h.entity_code}" class="btn-sm btn-blue">Open</a></td>
        </tr>
    `).join('');
}

renderCards();
renderTable();
</script>
</body>
</html>
"""


AR_FORM_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AR Handoff - {{ entity_code }}</title>
<style>
  :root { --blue: #1a56db; --blue-light: #e1effe; --green: #057a55; --green-light: #def7ec; --yellow: #f59e0b; --yellow-light: #fef3c7; --gray-50: #f9fafb; --gray-100: #f3f4f6; --gray-200: #e5e7eb; --gray-300: #d1d5db; --gray-500: #6b7280; --gray-700: #374151; --gray-900: #111827; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--gray-50); color: var(--gray-900); line-height: 1.5; }
  header { background: linear-gradient(135deg, #b45309 0%, #92400e 100%); color: white; padding: 24px 20px; }
  header h1 { font-size: 24px; }
  header p { opacity: 0.85; margin-top: 4px; }
  .nav { padding: 12px 20px; background: white; border-bottom: 1px solid var(--gray-200); }
  .nav a { color: var(--blue); text-decoration: none; font-size: 14px; margin-right: 16px; }
  .container { max-width: 800px; margin: 0 auto; padding: 32px 20px; }

  .card { background: white; border: 1px solid var(--gray-200); border-radius: 10px; padding: 24px; margin-bottom: 20px; }
  .card h2 { font-size: 16px; color: var(--blue); margin-bottom: 16px; padding-bottom: 8px; border-bottom: 1px solid var(--gray-200); }

  .field { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--gray-100); }
  .field:last-child { border-bottom: none; }
  .field .label { color: var(--gray-500); font-size: 14px; }
  .field .value { font-weight: 600; font-size: 14px; }
  .field .value.highlight { color: #b45309; font-size: 18px; }
  .field .value.green { color: var(--green); }
  .field .value.change { color: #dc2626; }

  .action-card { background: #fffbeb; border: 2px solid #f59e0b; }
  .action-card h2 { color: #92400e; }

  label { display: block; font-weight: 600; font-size: 14px; margin-bottom: 4px; margin-top: 12px; }
  input[type="text"], textarea {
    width: 100%; padding: 10px; border: 1px solid var(--gray-300); border-radius: 6px; font-size: 14px;
  }
  textarea { height: 80px; resize: vertical; }

  .checkbox-row { display: flex; align-items: center; gap: 8px; margin: 12px 0; }
  .checkbox-row input[type="checkbox"] { width: 18px; height: 18px; }
  .checkbox-row label { margin: 0; }

  .btn { padding: 12px 24px; border: none; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 15px; }
  .btn-green { background: var(--green); color: white; }
  .btn-green:hover { background: #046c4e; }
  .btn-blue { background: var(--blue); color: white; }
  .btn-blue:hover { background: #1e429f; }
  .btn:disabled { background: var(--gray-300); cursor: not-allowed; }

  .actions { display: flex; gap: 12px; margin-top: 20px; }

  .success-banner { background: var(--green-light); border: 1px solid var(--green); color: #065f46; padding: 14px; border-radius: 8px; margin-bottom: 20px; font-weight: 600; display: none; }
</style>
</head>
<body>
<header>
  <h1>AR Budget Handoff</h1>
  <p>Enter approved increase into Yardi</p>
</header>
<div class="nav">
  <a href="/">Home</a>
  <a href="/ar">AR Dashboard</a>
</div>

<div class="container">
  <div class="success-banner" id="successBanner">Yardi entry confirmed. This handoff is complete.</div>

  <div class="card">
    <h2>Building Information</h2>
    <div class="field"><span class="label">Entity Code</span><span class="value" id="fEntity"></span></div>
    <div class="field"><span class="label">Building Name</span><span class="value" id="fBuilding"></span></div>
  </div>

  <div class="card">
    <h2>Budget Summary</h2>
    <div class="field"><span class="label">Current Total Budget</span><span class="value" id="fCurrent"></span></div>
    <div class="field"><span class="label">Proposed Total Budget</span><span class="value" id="fProposed"></span></div>
    <div class="field"><span class="label">Net Change</span><span class="value change" id="fChange"></span></div>
  </div>

  <div class="card">
    <h2>Approval Details</h2>
    <div class="field"><span class="label">Approved By</span><span class="value" id="fApprover"></span></div>
    <div class="field"><span class="label">Approved Date</span><span class="value" id="fApproveDate"></span></div>
    <div class="field"><span class="label">Approved Increase</span><span class="value highlight" id="fIncrease"></span></div>
    <div class="field"><span class="label">Effective Date</span><span class="value green" id="fEffective"></span></div>
  </div>

  <div class="card action-card" id="actionCard">
    <h2>AR Action</h2>

    <div class="checkbox-row">
      <input type="checkbox" id="ackCheck" onchange="handleAcknowledge()">
      <label for="ackCheck">I acknowledge receipt of this handoff</label>
    </div>

    <div id="entrySection" style="display:none">
      <div class="checkbox-row">
        <input type="checkbox" id="enteredCheck">
        <label for="enteredCheck">I have entered this increase into Yardi</label>
      </div>

      <label>Yardi Confirmation #</label>
      <input type="text" id="yardiConfirm" placeholder="Enter Yardi confirmation number or reference">

      <label>AR Notes</label>
      <textarea id="arNotes" placeholder="Optional notes..."></textarea>

      <div class="actions">
        <button class="btn btn-green" onclick="submitEntry()">Confirm Yardi Entry</button>
      </div>
    </div>
  </div>
</div>

<script>
const ENTITY = "{{ entity_code }}";
const H = {{ handoff_json | safe }};

function fmt(n) {
    if (n == null || isNaN(n)) return '$0';
    return '$' + Math.round(n).toLocaleString();
}

// Populate fields
document.getElementById('fEntity').textContent = H.entity_code;
document.getElementById('fBuilding').textContent = H.building_name;
document.getElementById('fCurrent').textContent = fmt(H.total_current_budget);
document.getElementById('fProposed').textContent = fmt(H.total_proposed_budget);

const netChange = H.total_proposed_budget - H.total_current_budget;
const pctChange = H.total_current_budget > 0 ? (netChange / H.total_current_budget * 100).toFixed(1) : '0.0';
document.getElementById('fChange').textContent = `${fmt(netChange)} (${pctChange}%)`;

document.getElementById('fApprover').textContent = H.approved_by_name || '-';
document.getElementById('fApproveDate').textContent = H.approved_at ? new Date(H.approved_at).toLocaleDateString() : '-';
document.getElementById('fIncrease').textContent = H.approved_increase_pct.toFixed(1) + '%';
document.getElementById('fEffective').textContent = H.effective_date || '-';

// Handle state
if (H.ar_status === 'acknowledged' || H.ar_status === 'entered') {
    document.getElementById('ackCheck').checked = true;
    document.getElementById('ackCheck').disabled = true;
    document.getElementById('entrySection').style.display = 'block';
}
if (H.ar_status === 'entered') {
    document.getElementById('enteredCheck').checked = true;
    document.getElementById('enteredCheck').disabled = true;
    document.getElementById('yardiConfirm').value = H.yardi_confirmation || '';
    document.getElementById('yardiConfirm').disabled = true;
    document.getElementById('arNotes').disabled = true;
    document.getElementById('successBanner').style.display = 'block';
    document.getElementById('actionCard').style.display = 'none';
}

async function handleAcknowledge() {
    if (!document.getElementById('ackCheck').checked) return;
    const name = prompt('Your name for acknowledgement:');
    if (!name) { document.getElementById('ackCheck').checked = false; return; }

    await fetch(`/api/ar/${ENTITY}/acknowledge`, {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({acknowledged_by: name})
    });
    document.getElementById('ackCheck').disabled = true;
    document.getElementById('entrySection').style.display = 'block';
}

async function submitEntry() {
    if (!document.getElementById('enteredCheck').checked) {
        alert('Please confirm you have entered the increase into Yardi.');
        return;
    }
    const confirmation = document.getElementById('yardiConfirm').value.trim();
    if (!confirmation) {
        alert('Please enter a Yardi confirmation number.');
        return;
    }

    const resp = await fetch(`/api/ar/${ENTITY}/enter`, {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            yardi_confirmation: confirmation,
            notes: document.getElementById('arNotes').value
        })
    });
    const data = await resp.json();
    if (data.ar_status === 'entered') {
        document.getElementById('successBanner').style.display = 'block';
        document.getElementById('actionCard').style.display = 'none';
    }
}
</script>
</body>
</html>
"""
