"""
PM Budget Review Workflow Blueprint for Century Management.

Implements workflow for FA and PM budget review process with:
- User and building assignment management
- Budget and line item tracking
- PM data entry for R&M line items
- Status progression and approval workflow
"""

from flask import Blueprint, render_template_string, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

# R&M GL Code Mapping: gl_code|description|template_row|category
RM_GL_MAP = {
    "5406-0000": ("Supplies & Hardware", 8, "supplies"),
    "5445-0000": ("Uniforms Purchased", 13, "supplies"),
    "5606-0000": ("Insurance Repairs", 15, "repairs"),
    "5609-0000": ("Appliance Repairs", 16, "repairs"),
    "5621-0000": ("Equipment Repairs", 17, "repairs"),
    "5622-0000": ("HVAC Repairs", 18, "repairs"),
    "5624-0000": ("Heating Repairs", 19, "repairs"),
    "5627-0000": ("Boiler/Burner Repairs", 20, "repairs"),
    "5630-0000": ("Plumbing Repairs", 21, "repairs"),
    "5630-0001": ("Pump/Motor Repair", 22, "repairs"),
    "5633-0000": ("Electrical Repairs", 23, "repairs"),
    "5636-0000": ("Air Conditioning Repairs", 24, "repairs"),
    "5639-0000": ("Elevator Repairs", 25, "repairs"),
    "5642-0000": ("Compactor / Incinerator Repairs", 26, "repairs"),
    "5645-0000": ("Window Repairs", 27, "repairs"),
    "5648-0000": ("Floor / Carpet Repair", 28, "repairs"),
    "5657-0000": ("Lobby Repairs", 29, "repairs"),
    "5660-0000": ("Paint & Plaster Repairs", 30, "repairs"),
    "5666-0000": ("Door Repairs", 31, "repairs"),
    "5670-0000": ("Intercom Repairs", 32, "repairs"),
    "5672-0000": ("Time Recorder Repairs", 33, "repairs"),
    "5674-0000": ("Facade/Waterproofing Repairs", 34, "repairs"),
    "5678-0000": ("Roof Tank / Water Tank Repairs", 35, "repairs"),
    "5680-0000": ("Roof Exhaust Repairs", 36, "repairs"),
    "5682-0000": ("Sprinkler System Repairs", 37, "repairs"),
    "5692-0000": ("Fitness Equipment Repair", 38, "repairs"),
    "5695-0000": ("Other Building Repairs", 39, "repairs"),
    "5603-0000": ("Apartment Repairs", 40, "repairs"),
    "5668-0000": ("Canopy Repairs", 41, "repairs"),
    "5684-0000": ("Sidewalk Concrete Repairs", 42, "repairs"),
    "5803-0000": ("Cleaning & Maintenance", 46, "maintenance"),
    "5806-0000": ("Air Conditioning Maintenance", 47, "maintenance"),
    "5810-0000": ("HVAC Maintenance", 48, "maintenance"),
    "5812-0000": ("Elevator Maintenance", 49, "maintenance"),
    "5815-0000": ("Floor/Carpet Cleaning", 50, "maintenance"),
    "5818-0000": ("Window Cleaning", 51, "maintenance"),
    "5821-0000": ("Fire Extinguisher Maintenance", 52, "maintenance"),
    "5825-0000": ("Uniform Cleaning & Maintenance", 53, "maintenance"),
    "5828-0000": ("Rubbish Removal", 54, "maintenance"),
    "5831-0000": ("Exterminating", 55, "maintenance"),
    "5834-0000": ("Cooling Tower Maint", 56, "maintenance"),
    "5837-0000": ("Alarm / Security System Maintenance", 57, "maintenance"),
    "5840-0000": ("Metal/Marble/Wood Maintenance", 58, "maintenance"),
    "5846-0000": ("Garage Maintenance", 59, "maintenance"),
    "5852-0000": ("Water Treatment Maintenance", 60, "maintenance"),
    "5856-0000": ("Equipment Maint - Software", 61, "maintenance"),
    "5865-0000": ("Landscaping", 62, "maintenance"),
    "5870-0000": ("Rooftop Garden & Landscaping", 63, "maintenance"),
    "5809-0010": ("Boiler Maintenance", 64, "maintenance"),
    "5874-0000": ("Other Maintenance", 65, "maintenance"),
    "5809-0016": ("Sprinkler Maintenance", 66, "maintenance"),
}

BUDGET_STATUSES = ["draft", "pm_pending", "pm_in_progress", "fa_review", "approved", "returned"]
USER_ROLES = ["fa", "pm", "admin"]


def create_workflow_blueprint(db):
    """
    Create and configure the workflow blueprint.

    Args:
        db: SQLAlchemy database instance from app.py

    Returns:
        tuple: (blueprint, models_dict, helpers_dict)
    """

    # ─── SQLAlchemy Models ────────────────────────────────────────────────────

    class User(db.Model):
        """User account for FA, PM, or admin roles."""
        __tablename__ = "users"

        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(255), nullable=False)
        email = db.Column(db.String(255), unique=True, nullable=False)
        role = db.Column(db.String(20), nullable=False)  # 'fa', 'pm', 'admin'
        created_at = db.Column(db.DateTime, default=datetime.utcnow)

        # Relationships
        assignments = db.relationship("BuildingAssignment", back_populates="user", cascade="all, delete-orphan")

        def to_dict(self):
            return {
                "id": self.id,
                "name": self.name,
                "email": self.email,
                "role": self.role,
                "created_at": self.created_at.isoformat() if self.created_at else None
            }


    class BuildingAssignment(db.Model):
        """Assignment of FA/PM to buildings."""
        __tablename__ = "building_assignments"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False)
        user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
        role = db.Column(db.String(20), nullable=False)  # 'fa' or 'pm'

        # Relationships
        user = db.relationship("User", back_populates="assignments")

        __table_args__ = (db.UniqueConstraint("entity_code", "user_id", "role", name="uq_entity_user_role"),)

        def to_dict(self):
            return {
                "id": self.id,
                "entity_code": self.entity_code,
                "user_id": self.user_id,
                "user_name": self.user.name if self.user else None,
                "role": self.role
            }


    class Budget(db.Model):
        """Master budget record for a building/year."""
        __tablename__ = "budgets"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False, index=True)
        building_name = db.Column(db.String(255), nullable=False)
        year = db.Column(db.Integer, default=2027)
        status = db.Column(db.String(20), default="draft")  # draft, pm_pending, pm_in_progress, fa_review, approved, returned
        fa_notes = db.Column(db.Text, default="")
        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        # Relationships
        lines = db.relationship("BudgetLine", back_populates="budget", cascade="all, delete-orphan")

        __table_args__ = (db.UniqueConstraint("entity_code", "year", name="uq_entity_year"),)

        def to_dict(self):
            return {
                "id": self.id,
                "entity_code": self.entity_code,
                "building_name": self.building_name,
                "year": self.year,
                "status": self.status,
                "fa_notes": self.fa_notes,
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None
            }


    class BudgetLine(db.Model):
        """Individual R&M line item within a budget."""
        __tablename__ = "budget_lines"

        id = db.Column(db.Integer, primary_key=True)
        budget_id = db.Column(db.Integer, db.ForeignKey("budgets.id"), nullable=False)
        gl_code = db.Column(db.String(50), nullable=False)
        description = db.Column(db.String(255), nullable=False)
        category = db.Column(db.String(50), nullable=False)  # supplies, repairs, maintenance
        row_num = db.Column(db.Integer, nullable=False)

        # YSL-sourced columns (from prior runs, don't overwrite PM inputs)
        prior_year = db.Column(db.Float, default=0.0)
        ytd_actual = db.Column(db.Float, default=0.0)
        ytd_budget = db.Column(db.Float, default=0.0)
        current_budget = db.Column(db.Float, default=0.0)

        # PM-entered columns
        accrual_adj = db.Column(db.Float, default=0.0)
        unpaid_bills = db.Column(db.Float, default=0.0)
        increase_pct = db.Column(db.Float, default=0.0)
        notes = db.Column(db.Text, default="")

        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        # Relationships
        budget = db.relationship("Budget", back_populates="lines")

        def to_dict(self):
            return {
                "id": self.id,
                "budget_id": self.budget_id,
                "gl_code": self.gl_code,
                "description": self.description,
                "category": self.category,
                "row_num": self.row_num,
                "prior_year": float(self.prior_year or 0),
                "ytd_actual": float(self.ytd_actual or 0),
                "ytd_budget": float(self.ytd_budget or 0),
                "current_budget": float(self.current_budget or 0),
                "accrual_adj": float(self.accrual_adj or 0),
                "unpaid_bills": float(self.unpaid_bills or 0),
                "increase_pct": float(self.increase_pct or 0),
                "notes": self.notes,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None
            }


    # ─── Helper Functions ────────────────────────────────────────────────────

    def store_rm_lines(entity_code, building_name, gl_data):
        """
        Store R&M lines from YSL data into the database.

        gl_data: dict of {gl_code: {period_2, period_3, period_4, period_5, ...}}

        - period_2 → prior_year
        - period_3 → ytd_actual
        - period_4 → ytd_budget
        - period_5 → current_budget

        Only stores lines where gl_code is in RM_GL_MAP.
        If budget exists with status='draft', updates lines.
        If status is anything else, only updates YSL columns (doesn't overwrite PM inputs).
        """
        try:
            # Get or create budget
            budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
            if not budget:
                budget = Budget(
                    entity_code=entity_code,
                    building_name=building_name,
                    year=2027,
                    status="draft"
                )
                db.session.add(budget)
                db.session.flush()

            is_draft = budget.status == "draft"

            # Process each GL code from YSL
            for gl_code, gl_values in gl_data.items():
                if gl_code not in RM_GL_MAP:
                    continue

                desc, row_num, category = RM_GL_MAP[gl_code]

                prior_year = float(gl_values.get("period_2", 0) or 0)
                ytd_actual = float(gl_values.get("period_3", 0) or 0)
                ytd_budget = float(gl_values.get("period_4", 0) or 0)
                current_budget = float(gl_values.get("period_5", 0) or 0)

                # Find existing line or create new
                line = BudgetLine.query.filter_by(budget_id=budget.id, gl_code=gl_code).first()

                if line:
                    # Always update YSL-sourced columns
                    line.prior_year = prior_year
                    line.ytd_actual = ytd_actual
                    line.ytd_budget = ytd_budget
                    line.current_budget = current_budget
                    # Only reset PM inputs if this is a draft
                    if is_draft:
                        line.accrual_adj = 0.0
                        line.unpaid_bills = 0.0
                        line.increase_pct = 0.0
                        line.notes = ""
                else:
                    # Create new line
                    line = BudgetLine(
                        budget_id=budget.id,
                        gl_code=gl_code,
                        description=desc,
                        category=category,
                        row_num=row_num,
                        prior_year=prior_year,
                        ytd_actual=ytd_actual,
                        ytd_budget=ytd_budget,
                        current_budget=current_budget
                    )
                    db.session.add(line)

            db.session.commit()
            logger.info(f"Stored R&M lines for {entity_code}")
            return True
        except Exception as e:
            logger.error(f"Error storing R&M lines: {e}")
            db.session.rollback()
            return False


    def get_pm_projections(entity_code):
        """
        Get PM-entered projections for a building.

        Returns: {gl_code: {accrual_adj, unpaid_bills, increase_pct, notes}}
        """
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return {}

        result = {}
        for line in budget.lines:
            result[line.gl_code] = {
                "accrual_adj": float(line.accrual_adj or 0),
                "unpaid_bills": float(line.unpaid_bills or 0),
                "increase_pct": float(line.increase_pct or 0),
                "notes": line.notes
            }

        return result


    def compute_forecast(ytd_actual, accrual_adj, unpaid_bills, prior_year):
        """
        Compute 12-month forecast.

        Formula: ytd_actual + accrual_adj + unpaid_bills + estimate
        where estimate = IF(ytd_actual+accrual+unpaid >= prior_year,
                            (ytd_actual+accrual+unpaid)/2*10,
                            prior_year - (ytd_actual+accrual+unpaid))
        (2 YTD months, 10 remaining months)
        """
        ytd_total = ytd_actual + accrual_adj + unpaid_bills

        if ytd_total >= prior_year:
            estimate = (ytd_total / 2) * 10
        else:
            estimate = prior_year - ytd_total

        return ytd_total + estimate


    def compute_proposed_budget(forecast, increase_pct):
        """Compute proposed budget = forecast * (1 + increase_pct)"""
        return forecast * (1 + increase_pct)


    # ─── Blueprint Creation ──────────────────────────────────────────────────

    bp = Blueprint("workflow", __name__)


    # ─── Admin Routes ────────────────────────────────────────────────────────

    @bp.route("/admin", methods=["GET"])
    def admin():
        """Admin dashboard for user and building management."""
        import json as json_mod
        users = User.query.all()
        assignments = BuildingAssignment.query.all()

        return render_template_string(
            ADMIN_TEMPLATE,
            users_json=json_mod.dumps([u.to_dict() for u in users]),
            assignments_json=json_mod.dumps([a.to_dict() for a in assignments]),
        )


    @bp.route("/dashboard", methods=["GET"])
    def dashboard():
        """FA Dashboard - view all buildings and budget status."""
        import json as json_mod
        budgets = Budget.query.all()

        status_counts = {}
        for status in BUDGET_STATUSES:
            status_counts[status] = len([b for b in budgets if b.status == status])

        return render_template_string(
            DASHBOARD_TEMPLATE,
            budgets_json=json_mod.dumps([b.to_dict() for b in budgets]),
            status_counts_json=json_mod.dumps(status_counts),
        )


    @bp.route("/pm", methods=["GET"])
    def pm_portal():
        """PM Portal - select building and edit R&M lines."""
        import json as json_mod
        pm_users = User.query.filter_by(role="pm").all()

        return render_template_string(
            PM_PORTAL_TEMPLATE,
            pm_users_json=json_mod.dumps([u.to_dict() for u in pm_users]),
        )


    @bp.route("/pm/<entity_code>", methods=["GET"])
    def pm_edit(entity_code):
        """PM Edit Page - spreadsheet-style R&M grid."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()

        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        # Check if PM can edit this budget
        can_edit = budget.status in ["pm_pending", "pm_in_progress", "returned"]

        lines = BudgetLine.query.filter_by(budget_id=budget.id).order_by(BudgetLine.row_num).all()
        import json as json_mod

        lines_data = [l.to_dict() for l in lines]

        return render_template_string(
            PM_EDIT_TEMPLATE,
            entity_code=entity_code,
            building_name=budget.building_name,
            status=budget.status,
            can_edit="true" if can_edit else "false",
            fa_notes=budget.fa_notes or "",
            lines_json=json_mod.dumps(lines_data),
        )


    # ─── API Routes: Users ───────────────────────────────────────────────────

    @bp.route("/api/users", methods=["GET"])
    def list_users():
        """List all users."""
        users = User.query.all()
        return jsonify([u.to_dict() for u in users])


    @bp.route("/api/users", methods=["POST"])
    def create_user():
        """Create a new user."""
        data = request.get_json()

        if not data.get("name") or not data.get("email") or not data.get("role"):
            return jsonify({"error": "Missing required fields"}), 400

        if data["role"] not in USER_ROLES:
            return jsonify({"error": f"Invalid role. Must be one of {USER_ROLES}"}), 400

        # Check if email already exists
        if User.query.filter_by(email=data["email"]).first():
            return jsonify({"error": "Email already exists"}), 400

        user = User(
            name=data["name"],
            email=data["email"],
            role=data["role"]
        )

        db.session.add(user)
        db.session.commit()

        return jsonify(user.to_dict()), 201


    @bp.route("/api/users/<int:user_id>", methods=["PUT"])
    def update_user(user_id):
        """Update a user."""
        user = User.query.get(user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404

        data = request.get_json()

        if "name" in data:
            user.name = data["name"]
        if "email" in data:
            if data["email"] != user.email and User.query.filter_by(email=data["email"]).first():
                return jsonify({"error": "Email already exists"}), 400
            user.email = data["email"]
        if "role" in data:
            if data["role"] not in USER_ROLES:
                return jsonify({"error": f"Invalid role. Must be one of {USER_ROLES}"}), 400
            user.role = data["role"]

        db.session.commit()
        return jsonify(user.to_dict())


    @bp.route("/api/users/<int:user_id>", methods=["DELETE"])
    def delete_user(user_id):
        """Delete a user."""
        user = User.query.get(user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404

        db.session.delete(user)
        db.session.commit()

        return jsonify({"status": "deleted"}), 204


    # ─── API Routes: Assignments ─────────────────────────────────────────────

    @bp.route("/api/assignments", methods=["GET"])
    def list_assignments():
        """List all building assignments."""
        assignments = BuildingAssignment.query.all()
        return jsonify([a.to_dict() for a in assignments])


    @bp.route("/api/assignments", methods=["POST"])
    def create_assignment():
        """Create a new building assignment."""
        data = request.get_json()

        if not data.get("entity_code") or not data.get("user_id") or not data.get("role"):
            return jsonify({"error": "Missing required fields"}), 400

        if data["role"] not in ["fa", "pm"]:
            return jsonify({"error": "Role must be 'fa' or 'pm'"}), 400

        # Check if user exists
        user = User.query.get(data["user_id"])
        if not user:
            return jsonify({"error": "User not found"}), 404

        # Check for duplicates
        existing = BuildingAssignment.query.filter_by(
            entity_code=data["entity_code"],
            user_id=data["user_id"],
            role=data["role"]
        ).first()

        if existing:
            return jsonify({"error": "Assignment already exists"}), 400

        assignment = BuildingAssignment(
            entity_code=data["entity_code"],
            user_id=data["user_id"],
            role=data["role"]
        )

        db.session.add(assignment)
        db.session.commit()

        return jsonify(assignment.to_dict()), 201


    @bp.route("/api/assignments/<int:assignment_id>", methods=["DELETE"])
    def delete_assignment(assignment_id):
        """Delete an assignment."""
        assignment = BuildingAssignment.query.get(assignment_id)
        if not assignment:
            return jsonify({"error": "Assignment not found"}), 404

        db.session.delete(assignment)
        db.session.commit()

        return jsonify({"status": "deleted"}), 204


    # ─── API Routes: Budgets ─────────────────────────────────────────────────

    @bp.route("/api/budgets", methods=["GET"])
    def list_budgets():
        """List all budgets with status."""
        budgets = Budget.query.all()
        return jsonify([b.to_dict() for b in budgets])


    @bp.route("/api/budgets/<entity_code>/status", methods=["POST"])
    def change_budget_status(entity_code):
        """Change budget status with validation."""
        data = request.get_json()
        new_status = data.get("status")

        if new_status not in BUDGET_STATUSES:
            return jsonify({"error": f"Invalid status. Must be one of {BUDGET_STATUSES}"}), 400

        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        # Validation rules
        if new_status == "pm_pending" and budget.status != "draft":
            return jsonify({"error": "Can only move to pm_pending from draft"}), 400

        if new_status == "approved" and budget.status != "fa_review":
            return jsonify({"error": "Can only approve from fa_review"}), 400

        if new_status == "returned" and budget.status != "fa_review":
            return jsonify({"error": "Can only return from fa_review"}), 400

        if "notes" in data:
            budget.fa_notes = data["notes"]

        budget.status = new_status
        db.session.commit()

        return jsonify(budget.to_dict())


    # ─── API Routes: Lines ───────────────────────────────────────────────────

    @bp.route("/api/lines/<entity_code>", methods=["GET"])
    def get_lines(entity_code):
        """Get all R&M lines for a building."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        lines = BudgetLine.query.filter_by(budget_id=budget.id).order_by(BudgetLine.row_num).all()
        return jsonify([l.to_dict() for l in lines])


    @bp.route("/api/lines/<entity_code>", methods=["PUT"])
    def update_lines(entity_code):
        """Update R&M lines for a building (PM data entry)."""
        data = request.get_json()

        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        # Check if PM can edit
        if budget.status not in ["pm_pending", "pm_in_progress", "returned"]:
            return jsonify({"error": "Budget is not in editable status"}), 400

        # Mark as in progress
        if budget.status == "pm_pending":
            budget.status = "pm_in_progress"

        # Update each line — match by gl_code (or id if provided)
        for line_data in data.get("lines", []):
            line = None
            if line_data.get("id"):
                line = BudgetLine.query.get(line_data["id"])
                if line and line.budget_id != budget.id:
                    line = None
            elif line_data.get("gl_code"):
                line = BudgetLine.query.filter_by(
                    budget_id=budget.id, gl_code=line_data["gl_code"]
                ).first()
            if not line:
                continue

            line.accrual_adj = float(line_data.get("accrual_adj", 0) or 0)
            line.unpaid_bills = float(line_data.get("unpaid_bills", 0) or 0)
            line.increase_pct = float(line_data.get("increase_pct", 0) or 0)
            line.notes = line_data.get("notes", "")

        db.session.commit()

        return jsonify(budget.to_dict())


    # ─── HTML Templates ─────────────────────────────────────────────────────

    return (bp, {"User": User, "BuildingAssignment": BuildingAssignment, "Budget": Budget, "BudgetLine": BudgetLine},
            {"store_rm_lines": store_rm_lines, "get_pm_projections": get_pm_projections,
             "compute_forecast": compute_forecast, "compute_proposed_budget": compute_proposed_budget})


# ─── HTML Template Strings ───────────────────────────────────────────────────

ADMIN_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Admin - Century Management Workflow</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-light: #e1effe;
    --green: #057a55;
    --green-light: #def7ec;
    --red: #e02424;
    --red-light: #fde8e8;
    --yellow: #f59e0b;
    --yellow-light: #fef3c7;
    --gray-50: #f9fafb;
    --gray-100: #f3f4f6;
    --gray-200: #e5e7eb;
    --gray-300: #d1d5db;
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
    padding: 30px 20px;
  }
  header h1 {
    font-size: 28px;
    font-weight: 700;
  }
  .container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 40px 20px;
  }
  .section {
    background: white;
    border-radius: 12px;
    padding: 32px;
    margin-bottom: 32px;
    border: 1px solid var(--gray-200);
  }
  .section h2 {
    font-size: 20px;
    font-weight: 600;
    margin-bottom: 24px;
    color: var(--blue);
  }
  .form-group {
    margin-bottom: 16px;
  }
  label {
    display: block;
    font-size: 14px;
    font-weight: 500;
    margin-bottom: 6px;
    color: var(--gray-700);
  }
  input, select, textarea {
    width: 100%;
    padding: 10px 12px;
    border: 1px solid var(--gray-300);
    border-radius: 6px;
    font-size: 14px;
  }
  input:focus, select:focus, textarea:focus {
    outline: none;
    border-color: var(--blue);
    box-shadow: 0 0 0 3px var(--blue-light);
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
  button:hover {
    background: #1542b8;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 16px;
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
  .btn-delete {
    background: var(--red);
    padding: 6px 12px;
    font-size: 12px;
  }
  .btn-delete:hover {
    background: #d01f1f;
  }
</style>
</head>
<body>
<header>
  <a href="/" class="back-link" style="color:white; text-decoration:none; font-size:14px;">← Home</a>
  <h1>Admin Dashboard</h1>
  <p>Manage users and building assignments</p>
</header>
<div class="container">
  <div class="section" style="border-left: 4px solid #6366f1;">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
      <div>
        <h2 style="margin-bottom:4px;">Monday.com Sync</h2>
        <p style="font-size:13px; color:var(--gray-500);">Pull PM and FA assignments from the Building Master List</p>
      </div>
      <div style="display:flex; gap:10px; align-items:center;">
        <span id="syncStatus" style="font-size:13px; color:var(--gray-500);"></span>
        <button onclick="syncMonday()" id="syncBtn" style="background:#6366f1;">
          Sync from Monday.com
        </button>
      </div>
    </div>
    <div id="syncResults" style="display:none; background:var(--gray-50); border-radius:8px; padding:16px; font-size:13px;">
    </div>
  </div>

  <div class="section">
    <h2>Add User</h2>
    <form id="user-form">
      <div class="form-group">
        <label>Name</label>
        <input type="text" name="name" required>
      </div>
      <div class="form-group">
        <label>Email</label>
        <input type="email" name="email" required>
      </div>
      <div class="form-group">
        <label>Role</label>
        <select name="role" required>
          <option value="">Select a role</option>
          <option value="fa">Financial Analyst (FA)</option>
          <option value="pm">Property Manager (PM)</option>
          <option value="admin">Admin</option>
        </select>
      </div>
      <button type="submit">Create User</button>
    </form>
  </div>

  <div class="section">
    <h2>Users</h2>
    <table id="users-table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Email</th>
          <th>Role</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="section">
    <h2>Add Building Assignment</h2>
    <form id="assignment-form">
      <div class="form-group">
        <label>Entity Code</label>
        <input type="text" name="entity_code" required placeholder="e.g., 01234">
      </div>
      <div class="form-group">
        <label>User</label>
        <select name="user_id" required id="user-select"></select>
      </div>
      <div class="form-group">
        <label>Role</label>
        <select name="role" required>
          <option value="">Select a role</option>
          <option value="fa">FA</option>
          <option value="pm">PM</option>
        </select>
      </div>
      <button type="submit">Create Assignment</button>
    </form>
  </div>

  <div class="section">
    <h2>Building Assignments</h2>
    <input type="text" id="assignmentSearch" placeholder="Search by address, entity, or person..." style="width:100%; padding:10px 14px; border:1px solid #d1d5db; border-radius:8px; font-size:14px; margin-bottom:12px; box-sizing:border-box;" oninput="filterAssignments()">
    <table id="assignments-table">
      <thead>
        <tr>
          <th>Entity Code</th>
          <th>User Name</th>
          <th>Role</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<script>
// ── Monday.com Sync ─────────────────────────────────────────────────────
async function syncMonday() {
  const btn = document.getElementById('syncBtn');
  const status = document.getElementById('syncStatus');
  const results = document.getElementById('syncResults');

  btn.disabled = true;
  btn.textContent = 'Syncing...';
  status.textContent = 'Fetching from Monday.com...';

  try {
    // Fetch building data from the app's Monday.com proxy endpoint
    const resp = await fetch('/api/sync-monday-fetch');
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'Fetch failed');

    status.textContent = `Syncing ${data.buildings.length} buildings...`;

    // Send to sync endpoint
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
      Buildings synced: <strong>${s.buildings_synced}</strong> &nbsp;|&nbsp;
      Users created: <strong>${s.users_created}</strong> &nbsp;|&nbsp;
      Assignments created: <strong>${s.assignments_created}</strong> &nbsp;|&nbsp;
      Assignments removed: <strong>${s.assignments_removed}</strong>
    `;
    status.textContent = '';

    // Reload tables
    loadUsers();
    loadAssignments();
  } catch(e) {
    status.textContent = '';
    results.style.display = 'block';
    results.innerHTML = '<strong style="color:#e02424;">Error:</strong> ' + e.message;
  }

  btn.disabled = false;
  btn.textContent = 'Sync from Monday.com';
}

async function loadUsers() {
  try {
    const res = await fetch('/api/users');
    const users = await res.json();
    renderUsers(users);
    populateUserSelect(users);
    return users;
  } catch (err) {
    console.error('Failed to load users:', err);
    return [];
  }
}

async function loadAssignments() {
  try {
    const res = await fetch('/api/assignments');
    const assignments = await res.json();
    renderAssignments(assignments);
    return assignments;
  } catch (err) {
    console.error('Failed to load assignments:', err);
    return [];
  }
}

async function loadBuildings() {
  try {
    const res = await fetch('/api/buildings');
    const buildings = await res.json();
    return buildings;
  } catch (err) {
    console.error('Failed to load buildings:', err);
    return [];
  }
}

function populateUserSelect(users) {
  const select = document.getElementById('user-select');
  select.innerHTML = '<option value="">Select a user</option>';
  users.forEach(user => {
    const opt = document.createElement('option');
    opt.value = user.id;
    opt.textContent = user.name;
    select.appendChild(opt);
  });
}

function renderUsers(users) {
  const tbody = document.querySelector('#users-table tbody');
  tbody.innerHTML = '';
  users.forEach(user => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${user.name}</td>
      <td>${user.email}</td>
      <td>${user.role}</td>
      <td><button class="btn-delete" onclick="deleteUser(${user.id})">Delete</button></td>
    `;
    tbody.appendChild(tr);
  });
}

function renderAssignments(assignments) {
  const tbody = document.querySelector('#assignments-table tbody');
  tbody.innerHTML = '';
  assignments.forEach(a => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${a.entity_code}</td>
      <td>${a.user_name || 'N/A'}</td>
      <td>${a.role}</td>
      <td><button class="btn-delete" onclick="deleteAssignment(${a.id})">Delete</button></td>
    `;
    tbody.appendChild(tr);
  });
}

async function deleteUser(userId) {
  if (!confirm('Delete this user?')) return;
  try {
    await fetch(`/api/users/${userId}`, { method: 'DELETE' });
    await loadUsers();
  } catch (err) {
    alert('Failed to delete user');
    console.error(err);
  }
}

async function deleteAssignment(assignmentId) {
  if (!confirm('Delete this assignment?')) return;
  try {
    await fetch(`/api/assignments/${assignmentId}`, { method: 'DELETE' });
    await loadAssignments();
  } catch (err) {
    alert('Failed to delete assignment');
    console.error(err);
  }
}

document.getElementById('user-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const formData = new FormData(e.target);
  try {
    await fetch('/api/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: formData.get('name'),
        email: formData.get('email'),
        role: formData.get('role')
      })
    });
    e.target.reset();
    await loadUsers();
  } catch (err) {
    alert('Failed to create user');
    console.error(err);
  }
});

document.getElementById('assignment-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const formData = new FormData(e.target);
  try {
    await fetch('/api/assignments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        entity_code: formData.get('entity_code'),
        user_id: parseInt(formData.get('user_id')),
        role: formData.get('role')
      })
    });
    e.target.reset();
    await loadAssignments();
  } catch (err) {
    alert('Failed to create assignment');
    console.error(err);
  }
});

function filterAssignments() {
  const query = document.getElementById('assignmentSearch').value.toLowerCase();
  const rows = document.querySelectorAll('#assignments-table tbody tr');
  rows.forEach(row => {
    const text = row.textContent.toLowerCase();
    row.style.display = text.includes(query) ? '' : 'none';
  });
}

// Initialize on page load
(async () => {
  await loadUsers();
  await loadAssignments();
})();
</script>
</body>
</html>
"""

DASHBOARD_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FA Dashboard - Century Management</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-light: #e1effe;
    --green: #057a55;
    --green-light: #def7ec;
    --red: #e02424;
    --red-light: #fde8e8;
    --yellow: #f59e0b;
    --yellow-light: #fef3c7;
    --orange: #f97316;
    --orange-light: #fed7aa;
    --gray-50: #f9fafb;
    --gray-100: #f3f4f6;
    --gray-200: #e5e7eb;
    --gray-300: #d1d5db;
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
    padding: 30px 20px;
  }
  header h1 {
    font-size: 28px;
    font-weight: 700;
    margin-bottom: 8px;
  }
  .container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 40px 20px;
  }
  .status-summary {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 16px;
    margin-bottom: 32px;
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
</style>
</head>
<body>
<header>
  <a href="/" class="back-link" style="color:white; text-decoration:none; font-size:14px;">← Home</a>
  <h1>FA Dashboard</h1>
  <p>Review and manage building budgets</p>
</header>
<div class="container">
  <div class="status-summary" id="status-summary"></div>

  <div class="section">
    <h2>All Buildings</h2>
    <table id="budgets-table">
      <thead>
        <tr>
          <th>Building Name</th>
          <th>Entity Code</th>
          <th>Year</th>
          <th>Status</th>
          <th>Updated</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<script>
const statusLabels = {
  'draft': 'Draft',
  'pm_pending': 'Pending PM',
  'pm_in_progress': 'PM In Progress',
  'fa_review': 'FA Review',
  'approved': 'Approved',
  'returned': 'Returned'
};

async function loadBudgets() {
  try {
    const res = await fetch('/api/budgets');
    const budgets = await res.json();
    renderBudgets(budgets);
    renderStatusSummary(budgets);
    return budgets;
  } catch (err) {
    console.error('Failed to load budgets:', err);
    return [];
  }
}

function renderStatusSummary(budgets) {
  const summary = document.getElementById('status-summary');
  summary.innerHTML = '';

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

  Object.entries(counts).forEach(([status, count]) => {
    const card = document.createElement('div');
    card.className = 'status-card';
    card.innerHTML = `
      <div class="count">${count}</div>
      <div class="label">${statusLabels[status]}</div>
    `;
    summary.appendChild(card);
  });
}

function renderBudgets(budgets) {
  const tbody = document.querySelector('#budgets-table tbody');
  tbody.innerHTML = '';

  budgets.forEach(b => {
    const tr = document.createElement('tr');
    const statusLabel = statusLabels[b.status] || b.status;
    const statusClass = `pill-${b.status}`;
    const updated = new Date(b.updated_at).toLocaleDateString();

    let actionHtml = '';
    if (b.status === 'draft') {
      actionHtml = `<button onclick="changeStatus('${b.entity_code}', 'pm_pending')">Send to PM</button>`;
    } else if (b.status === 'fa_review') {
      actionHtml = `
        <button onclick="approveStatus('${b.entity_code}')">Approve</button>
        <button onclick="returnTopm('${b.entity_code}')" style="margin-left: 4px; background: #f59e0b;">Return</button>
      `;
    }

    tr.innerHTML = `
      <td><a href="/pm/${b.entity_code}" style="color: var(--blue); text-decoration: none;">${b.building_name}</a></td>
      <td>${b.entity_code}</td>
      <td>${b.year}</td>
      <td><span class="pill ${statusClass}">${statusLabel}</span></td>
      <td>${updated}</td>
      <td>${actionHtml}</td>
    `;
    tbody.appendChild(tr);
  });
}

async function changeStatus(entity, newStatus) {
  if (!confirm(`Change status to ${statusLabels[newStatus]}?`)) return;
  try {
    await fetch(`/api/budgets/${entity}/status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: newStatus })
    });
    await loadBudgets();
  } catch (err) {
    alert('Failed to update status');
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
    await loadBudgets();
  } catch (err) {
    alert('Failed to return budget');
    console.error(err);
  }
}

// Initialize on page load
(async () => {
  await loadBudgets();
})();
</script>
</body>
</html>
"""

PM_PORTAL_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PM Portal - Century Management</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-light: #e1effe;
    --gray-50: #f9fafb;
    --gray-100: #f3f4f6;
    --gray-200: #e5e7eb;
    --gray-300: #d1d5db;
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
    padding: 30px 20px;
  }
  header h1 {
    font-size: 28px;
    font-weight: 700;
  }
  .container {
    max-width: 1000px;
    margin: 0 auto;
    padding: 40px 20px;
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
  .buildings-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
    gap: 16px;
    margin-top: 24px;
  }
  .building-card {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 12px;
    padding: 24px;
    text-decoration: none;
    color: var(--gray-900);
    transition: all 0.15s;
    cursor: pointer;
  }
  .building-card:hover {
    border-color: var(--blue);
    box-shadow: 0 10px 25px rgba(26, 86, 219, 0.15);
    transform: translateY(-4px);
  }
  .building-card h3 {
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 8px;
    color: var(--blue);
  }
  .building-card p {
    font-size: 13px;
    color: var(--gray-500);
    margin-bottom: 4px;
  }
</style>
</head>
<body>
<header>
  <a href="/" class="back-link" style="color:white; text-decoration:none; font-size:14px;">← Home</a>
  <h1>PM Portal</h1>
  <p>Select your name and review assigned buildings</p>
</header>
<div class="container">
  <div class="form-group">
    <label>Select Your Name</label>
    <select id="pm-select">
      <option value="">-- Choose your name --</option>
    </select>
  </div>

  <div class="buildings-grid" id="buildings-grid" style="display: none;"></div>
</div>

<script>
const editableStatuses = ['pm_pending', 'pm_in_progress', 'returned'];
const statusLabels = {
  'draft': 'Draft',
  'pm_pending': 'Pending PM',
  'pm_in_progress': 'PM In Progress',
  'fa_review': 'FA Review',
  'approved': 'Approved',
  'returned': 'Returned'
};

let allUsers = [];
let allAssignments = [];
let allBuildings = [];

async function loadInitialData() {
  try {
    const [usersRes, assignmentsRes, buildingsRes] = await Promise.all([
      fetch('/api/users'),
      fetch('/api/assignments'),
      fetch('/api/buildings')
    ]);

    allUsers = await usersRes.json();
    allAssignments = await assignmentsRes.json();
    allBuildings = await buildingsRes.json();

    populatePMSelect();
  } catch (err) {
    console.error('Failed to load data:', err);
  }
}

function populatePMSelect() {
  const select = document.getElementById('pm-select');
  select.innerHTML = '<option value="">-- Choose your name --</option>';

  const pmUsers = allUsers.filter(u => u.role === 'pm');
  pmUsers.forEach(user => {
    const opt = document.createElement('option');
    opt.value = user.id;
    opt.textContent = user.name;
    select.appendChild(opt);
  });
}

function getBuildingName(entityCode) {
  const building = allBuildings.find(b => b.entity_code === entityCode);
  return building ? building.name : entityCode;
}

function renderBuildings(userId) {
  const grid = document.getElementById('buildings-grid');
  const userAssignments = allAssignments.filter(a => a.user_id === userId && a.role === 'pm');

  if (userAssignments.length === 0) {
    grid.style.display = 'none';
    const msg = document.createElement('p');
    msg.style.marginTop = '24px';
    msg.style.color = 'var(--gray-500)';
    msg.textContent = 'No buildings assigned to you.';
    grid.parentElement.appendChild(msg);
    return;
  }

  grid.innerHTML = '';
  grid.style.display = 'grid';

  userAssignments.forEach(a => {
    const buildingName = getBuildingName(a.entity_code);
    const isEditable = editableStatuses.includes(a.status);

    const card = document.createElement('div');
    card.className = 'building-card';
    if (isEditable) {
      card.style.cursor = 'pointer';
      card.onclick = () => window.location.href = `/pm/${a.entity_code}`;
    } else {
      card.style.opacity = '0.6';
      card.style.cursor = 'default';
    }

    const statusLabel = statusLabels[a.status] || a.status;
    card.innerHTML = `
      <h3>${buildingName}</h3>
      <p><strong>Entity Code:</strong> ${a.entity_code}</p>
      <p><strong>Status:</strong> <span style="color: var(--gray-700);">${statusLabel}</span></p>
      ${!isEditable ? '<p style="color: var(--gray-500); font-size: 12px;">Read-only</p>' : ''}
    `;
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

PM_EDIT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PM Edit — {{ building_name }} — Century Management</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-light: #e1effe;
    --green: #057a55;
    --green-light: #def7ec;
    --orange: #d97706;
    --orange-light: #fef3c7;
    --red: #e02424;
    --gray-50: #f9fafb;
    --gray-100: #f3f4f6;
    --gray-200: #e5e7eb;
    --gray-300: #d1d5db;
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
  header h1 { font-size: 24px; margin-bottom: 4px; }
  header p { opacity: 0.9; font-size: 14px; }
  .back-link { color: rgba(255,255,255,0.8); text-decoration: none; font-size: 14px; }
  .back-link:hover { color: white; }
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
    padding: 4px 8px;
  }
  .save-indicator.saving { color: var(--orange); }
  .save-indicator.saved { color: var(--green); }

  .grid-wrapper {
    background: white;
    border-radius: 12px;
    border: 1px solid var(--gray-200);
    overflow: hidden;
  }
  .grid-container { overflow-x: auto; max-height: 75vh; overflow-y: auto; }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  thead { background: var(--gray-100); position: sticky; top: 0; z-index: 10; }
  th {
    padding: 10px 12px;
    text-align: left;
    font-weight: 600;
    border-bottom: 2px solid var(--gray-300);
    white-space: nowrap;
  }
  th.number { text-align: right; }
  td { padding: 8px 12px; border-bottom: 1px solid var(--gray-200); }
  td.number { text-align: right; font-variant-numeric: tabular-nums; }
  tbody tr:hover { background: var(--blue-light); }

  .category-header td {
    background: var(--blue-light);
    font-weight: 700;
    color: var(--blue);
    font-size: 14px;
    padding: 10px 12px;
    border-bottom: 2px solid var(--blue);
  }
  .subtotal-row td {
    background: var(--gray-100);
    font-weight: 700;
    border-top: 2px solid var(--gray-300);
  }
  .grand-total td {
    background: #1e3a5f;
    color: white;
    font-weight: 700;
    font-size: 14px;
  }

  input[type="number"], input[type="text"] {
    width: 100%;
    padding: 5px 8px;
    border: 1px solid var(--gray-300);
    border-radius: 4px;
    font-size: 13px;
    background: #fffff0;
  }
  input[type="number"] { text-align: right; width: 90px; }
  input[type="text"] { min-width: 140px; }
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
</style>
</head>
<body>
<header>
  <a href="/pm" class="back-link">← Back to buildings</a>
  <h1>{{ building_name }}</h1>
  <p>Entity {{ entity_code }} — Repairs, Maintenance & Supplies Budget</p>
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
    <div>
      <button class="btn btn-green" id="submitBtn" onclick="submitForReview()">Submit for FA Review</button>
    </div>
  </div>

  <div class="grid-wrapper">
    <div class="grid-container">
      <table id="linesTable">
        <thead>
          <tr>
            <th>GL Code</th>
            <th>Description</th>
            <th class="number">Prior Year</th>
            <th class="number">YTD Actual</th>
            <th class="number">12-Mo Forecast</th>
            <th class="number">Increase %</th>
            <th class="number">Accrual Adj</th>
            <th class="number">Unpaid Bills</th>
            <th class="number">Proposed Budget</th>
            <th>Notes</th>
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
const LINES = {{ lines_json | safe }};
const YTD_MONTHS = 2;
const REMAINING_MONTHS = 10;

let saveTimer = null;
const indicator = document.getElementById('saveIndicator');

function fmt(n) {
    if (n == null || isNaN(n)) return '$0';
    return '$' + Math.round(n).toLocaleString();
}

function pctFmt(n) {
    if (n == null || isNaN(n)) return '0.0%';
    return (n * 100).toFixed(1) + '%';
}

function computeForecast(line) {
    const e = line.ytd_actual || 0;
    const f = line.accrual_adj || 0;
    const g = line.unpaid_bills || 0;
    const d = line.prior_year || 0;
    const base = e + f + g;
    let estimate;
    if (base >= d && d > 0) {
        estimate = (base / YTD_MONTHS) * REMAINING_MONTHS;
    } else {
        estimate = Math.max(d - base, 0);
    }
    return base + estimate;
}

function computeProposed(line) {
    const forecast = computeForecast(line);
    return forecast * (1 + (line.increase_pct || 0));
}

function renderTable() {
    const tbody = document.getElementById('linesBody');
    tbody.innerHTML = '';

    // Group by category
    const categories = {supplies: [], repairs: [], maintenance: []};
    const catLabels = {supplies: 'Supplies', repairs: 'Repairs', maintenance: 'Maintenance'};
    LINES.forEach(l => {
        if (categories[l.category]) categories[l.category].push(l);
    });

    let grandPrior = 0, grandYtd = 0, grandForecast = 0, grandProposed = 0;

    for (const [cat, catLines] of Object.entries(categories)) {
        if (catLines.length === 0) continue;

        // Category header
        const headerRow = document.createElement('tr');
        headerRow.className = 'category-header';
        headerRow.innerHTML = '<td colspan="10">' + catLabels[cat] + '</td>';
        tbody.appendChild(headerRow);

        let catPrior = 0, catYtd = 0, catForecast = 0, catProposed = 0;

        catLines.forEach(line => {
            const forecast = computeForecast(line);
            const proposed = computeProposed(line);
            catPrior += (line.prior_year || 0);
            catYtd += (line.ytd_actual || 0);
            catForecast += forecast;
            catProposed += proposed;

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${line.gl_code}</td>
                <td>${line.description}</td>
                <td class="number">${fmt(line.prior_year)}</td>
                <td class="number">${fmt(line.ytd_actual)}</td>
                <td class="number" id="fc_${line.gl_code}">${fmt(forecast)}</td>
                <td class="number">
                    <input type="number" step="0.01" value="${((line.increase_pct || 0) * 100).toFixed(1)}"
                           data-gl="${line.gl_code}" data-field="increase_pct"
                           onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'}>
                </td>
                <td class="number">
                    <input type="number" step="1" value="${Math.round(line.accrual_adj || 0)}"
                           data-gl="${line.gl_code}" data-field="accrual_adj"
                           onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'}>
                </td>
                <td class="number">
                    <input type="number" step="1" value="${Math.round(line.unpaid_bills || 0)}"
                           data-gl="${line.gl_code}" data-field="unpaid_bills"
                           onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'}>
                </td>
                <td class="number" id="pb_${line.gl_code}">${fmt(proposed)}</td>
                <td>
                    <input type="text" value="${line.notes || ''}"
                           data-gl="${line.gl_code}" data-field="notes"
                           onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'}>
                </td>
            `;
            tbody.appendChild(tr);
        });

        // Subtotal row
        const subRow = document.createElement('tr');
        subRow.className = 'subtotal-row';
        subRow.innerHTML = `
            <td></td><td>Total ${catLabels[cat]}</td>
            <td class="number">${fmt(catPrior)}</td>
            <td class="number">${fmt(catYtd)}</td>
            <td class="number" id="subfc_${cat}">${fmt(catForecast)}</td>
            <td></td><td></td><td></td>
            <td class="number" id="subpb_${cat}">${fmt(catProposed)}</td>
            <td></td>
        `;
        tbody.appendChild(subRow);

        grandPrior += catPrior;
        grandYtd += catYtd;
        grandForecast += catForecast;
        grandProposed += catProposed;
    }

    // Grand total
    const grandRow = document.createElement('tr');
    grandRow.className = 'grand-total';
    grandRow.innerHTML = `
        <td></td><td>GRAND TOTAL R&M</td>
        <td class="number">${fmt(grandPrior)}</td>
        <td class="number">${fmt(grandYtd)}</td>
        <td class="number">${fmt(grandForecast)}</td>
        <td></td><td></td><td></td>
        <td class="number">${fmt(grandProposed)}</td>
        <td></td>
    `;
    tbody.appendChild(grandRow);
}

function onInput(el) {
    const gl = el.dataset.gl;
    const field = el.dataset.field;
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    if (field === 'increase_pct') {
        line.increase_pct = parseFloat(el.value) / 100 || 0;
    } else if (field === 'accrual_adj') {
        line.accrual_adj = parseFloat(el.value) || 0;
    } else if (field === 'unpaid_bills') {
        line.unpaid_bills = parseFloat(el.value) || 0;
    } else if (field === 'notes') {
        line.notes = el.value;
    }

    // Update computed columns
    const fc = computeForecast(line);
    const pb = computeProposed(line);
    const fcEl = document.getElementById('fc_' + gl);
    const pbEl = document.getElementById('pb_' + gl);
    if (fcEl) fcEl.textContent = fmt(fc);
    if (pbEl) pbEl.textContent = fmt(pb);

    // Debounced auto-save
    if (saveTimer) clearTimeout(saveTimer);
    indicator.textContent = 'Unsaved changes...';
    indicator.className = 'save-indicator saving';
    saveTimer = setTimeout(saveAll, 800);
}

async function saveAll() {
    indicator.textContent = 'Saving...';
    indicator.className = 'save-indicator saving';
    try {
        const payload = LINES.map(l => ({
            gl_code: l.gl_code,
            increase_pct: l.increase_pct || 0,
            accrual_adj: l.accrual_adj || 0,
            unpaid_bills: l.unpaid_bills || 0,
            notes: l.notes || ''
        }));
        const resp = await fetch('/api/lines/' + ENTITY, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({lines: payload})
        });
        if (resp.ok) {
            indicator.textContent = 'Saved';
            indicator.className = 'save-indicator saved';
            setTimeout(() => { indicator.textContent = ''; }, 2000);
        } else {
            indicator.textContent = 'Save failed!';
        }
    } catch(e) {
        indicator.textContent = 'Save error!';
    }
}

async function submitForReview() {
    // Save first
    await saveAll();
    if (!confirm('Submit this budget for FA review?')) return;

    const resp = await fetch('/api/budgets/' + ENTITY + '/status', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({status: 'fa_review'})
    });
    if (resp.ok) {
        alert('Submitted for FA review!');
        window.location.href = '/pm';
    } else {
        const err = await resp.json();
        alert('Error: ' + (err.error || 'Unknown'));
    }
}

// Disable submit button if not editable
if (!CAN_EDIT) {
    const btn = document.getElementById('submitBtn');
    if (btn) btn.disabled = true;
}

renderTable();
</script>
</body>
</html>
"""
