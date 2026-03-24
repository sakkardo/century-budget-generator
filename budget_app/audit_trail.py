"""
Audit Trail Blueprint for Century Management Budget System.

Provides endpoints for viewing revision history across all budget phases.
The actual record_revision() helper lives in workflow.py (close to the models).
This blueprint provides the UI and API routes for viewing that history.
"""

from flask import Blueprint, render_template_string, request, jsonify
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def create_audit_trail_blueprint(db, models):
    """
    Create audit trail blueprint.

    Args:
        db: SQLAlchemy database instance
        models: dict of model classes from workflow blueprint

    Returns:
        blueprint
    """
    Budget = models["Budget"]
    BudgetRevision = models["BudgetRevision"]
    User = models["User"]

    bp = Blueprint("audit_trail", __name__)

    @bp.route("/history/<entity_code>", methods=["GET"])
    def revision_history(entity_code):
        """Human-readable revision history page for a building."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return "Budget not found", 404

        revisions = (BudgetRevision.query
                     .filter_by(budget_id=budget.id)
                     .order_by(BudgetRevision.created_at.desc())
                     .all())

        # Enrich with user names
        user_cache = {}
        rev_data = []
        for rev in revisions:
            if rev.user_id and rev.user_id not in user_cache:
                user = User.query.get(rev.user_id)
                user_cache[rev.user_id] = user.name if user else "Unknown"

            rev_data.append({
                **rev.to_dict(),
                "user_name": user_cache.get(rev.user_id, "System"),
            })

        import json
        return render_template_string(
            HISTORY_TEMPLATE,
            entity_code=entity_code,
            building_name=budget.building_name,
            revisions_json=json.dumps(rev_data),
        )

    @bp.route("/api/revisions/<entity_code>", methods=["GET"])
    def get_revisions(entity_code):
        """Get full revision history for a building as JSON."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        revisions = (BudgetRevision.query
                     .filter_by(budget_id=budget.id)
                     .order_by(BudgetRevision.created_at.desc())
                     .all())

        return jsonify([r.to_dict() for r in revisions])

    @bp.route("/api/revisions/<entity_code>/summary", methods=["GET"])
    def get_revision_summary(entity_code):
        """Aggregated change summary — changes per phase, per user."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        revisions = BudgetRevision.query.filter_by(budget_id=budget.id).all()

        by_action = {}
        by_user = {}
        by_source = {}

        for rev in revisions:
            by_action[rev.action] = by_action.get(rev.action, 0) + 1
            by_source[rev.source] = by_source.get(rev.source, 0) + 1

            if rev.user_id:
                user = User.query.get(rev.user_id)
                name = user.name if user else f"User #{rev.user_id}"
                by_user[name] = by_user.get(name, 0) + 1

        return jsonify({
            "total_revisions": len(revisions),
            "by_action": by_action,
            "by_user": by_user,
            "by_source": by_source,
        })

    return bp


# ─── HTML Template ──────────────────────────────────────────────────────────

HISTORY_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Revision History - {{ entity_code }}</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-light: #e1effe;
    --green: #057a55;
    --green-light: #def7ec;
    --red: #e02424;
    --yellow: #f59e0b;
    --gray-50: #f9fafb;
    --gray-100: #f3f4f6;
    --gray-200: #e5e7eb;
    --gray-500: #6b7280;
    --gray-700: #374151;
    --gray-900: #111827;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--gray-50);
    color: var(--gray-900);
    line-height: 1.5;
  }
  header {
    background: linear-gradient(135deg, var(--blue) 0%, #1e429f 100%);
    color: white;
    padding: 24px 20px;
  }
  header h1 { font-size: 24px; font-weight: 700; }
  header p { opacity: 0.85; margin-top: 4px; }
  .nav { padding: 12px 20px; background: white; border-bottom: 1px solid var(--gray-200); }
  .nav a { color: var(--blue); text-decoration: none; font-size: 14px; }
  .nav a:hover { text-decoration: underline; }
  .container { max-width: 1000px; margin: 0 auto; padding: 32px 20px; }
  .timeline { position: relative; }
  .timeline::before {
    content: '';
    position: absolute;
    left: 20px;
    top: 0;
    bottom: 0;
    width: 2px;
    background: var(--gray-200);
  }
  .rev-item {
    position: relative;
    padding-left: 50px;
    margin-bottom: 20px;
  }
  .rev-dot {
    position: absolute;
    left: 14px;
    top: 6px;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    border: 2px solid white;
  }
  .rev-dot.status_change { background: var(--blue); }
  .rev-dot.update { background: var(--green); }
  .rev-dot.create { background: var(--yellow); }
  .rev-dot.reclass { background: #8b5cf6; }
  .rev-dot.presentation_edit { background: #ec4899; }
  .rev-card {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 8px;
    padding: 14px 18px;
  }
  .rev-meta {
    font-size: 12px;
    color: var(--gray-500);
    margin-bottom: 6px;
  }
  .rev-action {
    font-weight: 600;
    font-size: 14px;
    margin-bottom: 4px;
  }
  .rev-detail {
    font-size: 13px;
    color: var(--gray-700);
  }
  .rev-detail .old { color: var(--red); text-decoration: line-through; }
  .rev-detail .new { color: var(--green); font-weight: 600; }
  .badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
  }
  .badge-web { background: var(--blue-light); color: var(--blue); }
  .badge-presentation { background: #fce7f3; color: #ec4899; }
  .badge-system { background: var(--gray-100); color: var(--gray-500); }
  .empty { text-align: center; color: var(--gray-500); padding: 60px; }
</style>
</head>
<body>
<header>
  <h1>Revision History</h1>
  <p>{{ building_name }} ({{ entity_code }})</p>
</header>
<div class="nav">
  <a href="/">Home</a> &rarr; <a href="/dashboard">Dashboard</a> &rarr; History
</div>
<div class="container">
  <div class="timeline" id="timeline"></div>
</div>

<script>
const revisions = {{ revisions_json | safe }};

const timeline = document.getElementById('timeline');

if (revisions.length === 0) {
  timeline.innerHTML = '<div class="empty">No revisions recorded yet.</div>';
} else {
  revisions.forEach(rev => {
    const item = document.createElement('div');
    item.className = 'rev-item';

    const actionLabel = {
      'status_change': 'Status Changed',
      'update': 'Line Updated',
      'create': 'Created',
      'reclass': 'Reclassification',
      'presentation_edit': 'Presentation Edit',
    }[rev.action] || rev.action;

    const sourceBadge = {
      'web': 'badge-web',
      'presentation': 'badge-presentation',
      'system': 'badge-system',
    }[rev.source] || 'badge-system';

    let detail = '';
    if (rev.field_name) {
      detail = `<strong>${rev.field_name}</strong>: `;
      if (rev.old_value) detail += `<span class="old">${rev.old_value}</span> → `;
      detail += `<span class="new">${rev.new_value}</span>`;
    }
    if (rev.notes) {
      detail += (detail ? '<br>' : '') + `Note: ${rev.notes}`;
    }

    const date = new Date(rev.created_at);
    const dateStr = date.toLocaleDateString('en-US', {
      month: 'short', day: 'numeric', year: 'numeric',
      hour: '2-digit', minute: '2-digit'
    });

    item.innerHTML = `
      <div class="rev-dot ${rev.action}"></div>
      <div class="rev-card">
        <div class="rev-meta">
          ${dateStr} · ${rev.user_name} · <span class="badge ${sourceBadge}">${rev.source}</span>
        </div>
        <div class="rev-action">${actionLabel}</div>
        ${detail ? `<div class="rev-detail">${detail}</div>` : ''}
      </div>
    `;
    timeline.appendChild(item);
  });
}
</script>
</body>
</html>
"""
