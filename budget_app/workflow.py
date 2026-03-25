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


    @bp.route("/dashboard/<entity_code>", methods=["GET"])
    def building_detail(entity_code):
        """FA Building Detail - combined view of budget, expenses, audit."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return "No budget found for this building", 404
        return render_template_string(BUILDING_DETAIL_TEMPLATE, entity_code=entity_code)


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
        """List all budgets with status and completeness data."""
        budgets = Budget.query.all()
        result = []
        for b in budgets:
            d = b.to_dict()
            # Check expense report exists
            try:
                has_expenses = db.session.execute(
                    db.text("SELECT 1 FROM expense_report WHERE entity_code = :ec LIMIT 1"),
                    {"ec": b.entity_code}
                ).fetchone() is not None
            except Exception:
                has_expenses = False
            # Check confirmed audit exists
            try:
                has_audit = db.session.execute(
                    db.text("SELECT 1 FROM audit_upload WHERE entity_code = :ec AND status = 'confirmed' LIMIT 1"),
                    {"ec": b.entity_code}
                ).fetchone() is not None
            except Exception:
                has_audit = False
            d["has_expenses"] = has_expenses
            d["has_audit"] = has_audit
            result.append(d)
        return jsonify(result)


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


    @bp.route("/api/dashboard/<entity_code>", methods=["GET"])
    def api_building_detail(entity_code):
        """Get combined budget data for building detail view."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        lines = BudgetLine.query.filter_by(budget_id=budget.id).order_by(BudgetLine.row_num).all()

        # Get assignments
        assignments = BuildingAssignment.query.filter_by(entity_code=entity_code).all()
        fa_name = next((a.user.name for a in assignments if a.role == "fa"), None)
        pm_name = next((a.user.name for a in assignments if a.role == "pm"), None)

        # Check expense report
        expense_data = {"exists": False}
        try:
            row = db.session.execute(
                db.text("SELECT id, period_from, period_to, total_amount FROM expense_report WHERE entity_code = :ec ORDER BY uploaded_at DESC LIMIT 1"),
                {"ec": entity_code}
            ).fetchone()
            if row:
                invoice_count = db.session.execute(
                    db.text("SELECT COUNT(*) FROM expense_invoice WHERE report_id = :rid"),
                    {"rid": row[0]}
                ).fetchone()[0]
                expense_data = {
                    "exists": True,
                    "period_from": row[1],
                    "period_to": row[2],
                    "total_amount": float(row[3]) if row[3] else 0,
                    "invoice_count": invoice_count
                }
        except Exception:
            pass

        # Check audit data
        audit_data = {"exists": False}
        try:
            audit_row = db.session.execute(
                db.text("SELECT mapped_data, fiscal_year_end FROM audit_upload WHERE entity_code = :ec AND status = 'confirmed' ORDER BY confirmed_at DESC LIMIT 1"),
                {"ec": entity_code}
            ).fetchone()
            if audit_row and audit_row[0]:
                import json
                mapped = json.loads(audit_row[0])
                audit_data = {
                    "exists": True,
                    "fiscal_year": audit_row[1],
                    "categories": mapped
                }
        except Exception:
            pass

        return jsonify({
            "budget": budget.to_dict(),
            "lines": [l.to_dict() for l in lines],
            "assignments": {"fa": fa_name, "pm": pm_name},
            "expenses": expense_data,
            "audit": audit_data
        })


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
<title>User Management - Century Management</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-light: #e1effe;
    --green: #057a55;
    --green-light: #def7ec;
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
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
      <h2 style="margin-bottom:0;">All Buildings</h2>
      <input type="text" id="budgetSearch" placeholder="Search buildings..." oninput="filterBudgetTable()"
        style="padding:8px 14px; border:1px solid var(--gray-200); border-radius:8px; font-size:14px; width:260px; outline:none;">
    </div>
    <table id="budgets-table">
      <thead>
        <tr>
          <th>Building</th>
          <th>Entity</th>
          <th>Data</th>
          <th>PM Review</th>
          <th>Status</th>
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

    // Data completeness indicators
    const budgetIcon = '<span style="color:var(--green);" title="Budget">&#10003; Budget</span>';
    const expenseIcon = b.has_expenses
      ? '<span style="color:var(--green);" title="Expenses uploaded">&#10003; Expenses</span>'
      : '<span style="color:var(--gray-300);" title="No expenses">&#10007; Expenses</span>';
    const auditIcon = b.has_audit
      ? '<span style="color:var(--green);" title="Audit confirmed">&#10003; Audit</span>'
      : '<span style="color:var(--gray-300);" title="No audit">&#10007; Audit</span>';

    // PM review status pill
    const pmStatusMap = {
      'draft': 'Not Sent',
      'pm_pending': 'Sent to PM',
      'pm_in_progress': 'PM Working',
      'fa_review': 'Submitted',
      'approved': 'Approved',
      'returned': 'Returned'
    };
    const pmLabel = pmStatusMap[b.status] || b.status;

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
      <td><a href="/dashboard/${b.entity_code}" style="color: var(--blue); text-decoration: none; font-weight:500;">${b.building_name}</a></td>
      <td style="font-family:monospace; font-size:13px;">${b.entity_code}</td>
      <td style="font-size:12px; line-height:1.8;">${budgetIcon}<br>${expenseIcon}<br>${auditIcon}</td>
      <td><span class="pill ${statusClass}">${pmLabel}</span></td>
      <td><span class="pill ${statusClass}">${statusLabel}</span></td>
      <td>${actionHtml}</td>
    `;
    tbody.appendChild(tr);
  });
}

function filterBudgetTable() {
  const query = document.getElementById('budgetSearch').value.toLowerCase();
  const rows = document.querySelectorAll('#budgets-table tbody tr');
  rows.forEach(row => {
    const text = row.textContent.toLowerCase();
    row.style.display = text.includes(query) ? '' : 'none';
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

BUILDING_DETAIL_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Building Detail - Century Management</title>
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
    padding: 24px 20px;
  }
  header h1 { font-size: 24px; font-weight: 700; }
  header p { font-size: 14px; opacity: 0.85; margin-top: 4px; }
  .container { max-width: 1200px; margin: 0 auto; padding: 32px 20px; }
  .summary-cards {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 28px;
  }
  .summary-card {
    background: white;
    border-radius: 12px;
    padding: 24px;
    border: 1px solid var(--gray-200);
    text-align: center;
  }
  .card-value { font-size: 26px; font-weight: 700; color: var(--blue); }
  .card-label { font-size: 12px; color: var(--gray-500); text-transform: uppercase; font-weight: 600; margin-top: 4px; }
  .tracks {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 28px;
  }
  .section {
    background: white;
    border-radius: 12px;
    padding: 28px;
    border: 1px solid var(--gray-200);
    margin-bottom: 28px;
  }
  .section h2 { font-size: 18px; font-weight: 600; margin-bottom: 20px; color: var(--blue); }
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
  @media (max-width: 768px) {
    .summary-cards { grid-template-columns: repeat(2, 1fr); }
    .tracks { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
<header>
  <div style="display:flex; justify-content:space-between; align-items:flex-start;">
    <div>
      <a href="/dashboard" style="color:rgba(255,255,255,0.7); text-decoration:none; font-size:13px;">&#8592; Back to Dashboard</a>
      <h1 id="buildingName">Loading...</h1>
      <p id="buildingMeta"></p>
    </div>
    <a href="/" style="color:rgba(255,255,255,0.7); text-decoration:none; font-size:13px;">&#8592; Home</a>
  </div>
</header>
<div class="container">
  <!-- Summary Cards -->
  <div class="summary-cards" id="summaryCards"></div>

  <!-- Two Track Cards -->
  <div class="tracks">
    <!-- Track 1: PM Expense Review -->
    <div class="section" style="margin-bottom:0;">
      <h2>PM Expense Review</h2>
      <div id="pmTrackContent"></div>
    </div>

    <!-- Track 2: Budget Assembly -->
    <div class="section" style="margin-bottom:0;">
      <h2>Budget Assembly</h2>
      <div id="assemblyContent"></div>
    </div>
  </div>

  <!-- Budget Detail Table -->
  <div class="section">
    <h2>Budget Detail</h2>
    <table id="detailTable">
      <thead>
        <tr>
          <th>Category</th>
          <th>GL Code</th>
          <th>Description</th>
          <th style="text-align:right">Prior Year</th>
          <th style="text-align:right">Current Budget</th>
          <th style="text-align:right">PM Adjusted</th>
          <th style="text-align:right">Variance $</th>
          <th style="text-align:right">Variance %</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<script>
const entityCode = '{{ entity_code }}';

async function loadDetail() {
  const res = await fetch('/api/dashboard/' + entityCode);
  if (!res.ok) { document.getElementById('buildingName').textContent = 'Error loading data'; return; }
  const data = await res.json();
  renderDetail(data);
}

function fmt(n) {
  if (n === null || n === undefined) return '\u2014';
  return '$' + Math.round(n).toLocaleString();
}

function renderDetail(data) {
  const b = data.budget;

  // Header
  document.getElementById('buildingName').textContent = b.building_name;
  document.title = b.building_name + ' - Century Management';
  let meta = 'Entity ' + b.entity_code + ' | ' + b.year + ' Budget';
  if (data.assignments.fa) meta += ' | FA: ' + data.assignments.fa;
  if (data.assignments.pm) meta += ' | PM: ' + data.assignments.pm;
  document.getElementById('buildingMeta').textContent = meta;

  // Summary cards
  const lines = data.lines;
  let totalPrior = 0, totalBudget = 0, totalPM = 0;
  lines.forEach(l => {
    totalPrior += l.prior_year || 0;
    totalBudget += l.current_budget || 0;
    const forecast = computeForecast(l);
    const proposed = forecast * (1 + (l.increase_pct || 0));
    totalPM += proposed;
  });

  document.getElementById('summaryCards').innerHTML = `
    <div class="summary-card">
      <div class="card-value">${fmt(totalPrior)}</div>
      <div class="card-label">Prior Year</div>
    </div>
    <div class="summary-card">
      <div class="card-value">${fmt(totalBudget)}</div>
      <div class="card-label">Current Budget</div>
    </div>
    <div class="summary-card">
      <div class="card-value">${fmt(totalBudget - totalPrior)}</div>
      <div class="card-label">Variance</div>
    </div>
    <div class="summary-card">
      <div class="card-value">${totalPrior ? ((totalBudget - totalPrior) / totalPrior * 100).toFixed(1) + '%' : '\u2014'}</div>
      <div class="card-label">% Change</div>
    </div>
  `;

  // PM Track
  const statusLabels = { draft: 'Not Sent', pm_pending: 'Sent to PM', pm_in_progress: 'PM Working', fa_review: 'Submitted for Review', approved: 'Approved', returned: 'Returned' };
  const pmStatus = statusLabels[b.status] || b.status;
  let pmActions = '';
  if (b.status === 'draft') {
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

  // Assembly Track
  const checks = [
    { label: 'Budget Generated', done: true },
    { label: 'Expense Distribution Uploaded', done: data.expenses.exists },
    { label: 'Audit Data Confirmed', done: data.audit.exists }
  ];
  let assemblyHtml = '<div style="display:flex; flex-direction:column; gap:8px;">';
  checks.forEach(c => {
    const icon = c.done ? '\u2713' : '\u2717';
    const color = c.done ? 'var(--green)' : 'var(--gray-300)';
    assemblyHtml += '<div style="display:flex; align-items:center; gap:8px;"><span style="color:' + color + '; font-weight:bold; font-size:18px;">' + icon + '</span> ' + c.label + '</div>';
  });
  assemblyHtml += '</div>';

  if (data.expenses.exists) {
    assemblyHtml += '<div style="margin-top:12px; font-size:13px; color:var(--gray-500);">' +
      data.expenses.invoice_count + ' invoices | ' + data.expenses.period_from + ' to ' + data.expenses.period_to +
      ' | Total: ' + fmt(data.expenses.total_amount) + '</div>';
  }

  document.getElementById('assemblyContent').innerHTML = assemblyHtml;

  // Budget Detail Table
  const tbody = document.querySelector('#detailTable tbody');
  tbody.innerHTML = '';

  let currentCat = '';
  lines.forEach(l => {
    if (l.category !== currentCat) {
      currentCat = l.category;
      const catRow = document.createElement('tr');
      catRow.innerHTML = '<td colspan="8" style="font-weight:700; background:var(--blue-light); color:var(--blue); padding:10px 12px;">' + (currentCat || 'Uncategorized') + '</td>';
      tbody.appendChild(catRow);
    }

    const priorYear = l.prior_year || 0;
    const currentBudget = l.current_budget || 0;
    const forecast = computeForecast(l);
    const proposed = forecast * (1 + (l.increase_pct || 0));
    const pmAdjusted = (l.increase_pct || l.accrual_adj || l.unpaid_bills) ? proposed : null;

    const variance = currentBudget - priorYear;
    const variancePct = priorYear ? (variance / priorYear * 100) : 0;

    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td></td>' +
      '<td style="font-family:monospace; font-size:12px;">' + l.gl_code + '</td>' +
      '<td>' + l.description + '</td>' +
      '<td style="text-align:right">' + fmt(priorYear) + '</td>' +
      '<td style="text-align:right">' + fmt(currentBudget) + '</td>' +
      '<td style="text-align:right">' + (pmAdjusted !== null ? fmt(pmAdjusted) : '\u2014') + '</td>' +
      '<td style="text-align:right; color:' + (variance >= 0 ? 'var(--red)' : 'var(--green)') + '">' + fmt(variance) + '</td>' +
      '<td style="text-align:right; color:' + (variance >= 0 ? 'var(--red)' : 'var(--green)') + '">' + variancePct.toFixed(1) + '%</td>';
    tbody.appendChild(tr);
  });

  // Total row
  const totalRow = document.createElement('tr');
  totalRow.style.fontWeight = '700';
  totalRow.style.background = 'var(--gray-100)';
  const totalVar = totalBudget - totalPrior;
  totalRow.innerHTML =
    '<td colspan="3">Total</td>' +
    '<td style="text-align:right">' + fmt(totalPrior) + '</td>' +
    '<td style="text-align:right">' + fmt(totalBudget) + '</td>' +
    '<td style="text-align:right">' + fmt(totalPM) + '</td>' +
    '<td style="text-align:right">' + fmt(totalVar) + '</td>' +
    '<td style="text-align:right">' + (totalPrior ? ((totalVar) / totalPrior * 100).toFixed(1) + '%' : '\u2014') + '</td>';
  tbody.appendChild(totalRow);
}

function computeForecast(l) {
  const ytdActual = l.ytd_actual || 0;
  const accrualAdj = l.accrual_adj || 0;
  const unpaidBills = l.unpaid_bills || 0;
  const priorYear = l.prior_year || 0;
  const ytdTotal = ytdActual + accrualAdj + unpaidBills;

  if (ytdTotal >= priorYear) {
    return ytdTotal + (ytdTotal / 2) * 10;
  } else {
    return ytdTotal + (priorYear - ytdTotal);
  }
}

async function sendToPM() {
  if (!confirm('Send to PM for expense review?')) return;
  await fetch('/api/budgets/' + entityCode + '/status', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({status: 'pm_pending'})
  });
  loadDetail();
}

async function approvePM() {
  if (!confirm('Approve PM review?')) return;
  await fetch('/api/budgets/' + entityCode + '/status', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({status: 'approved'})
  });
  loadDetail();
}

async function returnPM() {
  const notes = prompt('Notes for PM:');
  if (notes === null) return;
  await fetch('/api/budgets/' + entityCode + '/status', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({status: 'returned', notes: notes})
  });
  loadDetail();
}

loadDetail();
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
