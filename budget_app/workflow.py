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
import os

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

# Maps budget_line category -> Century audit category
BUDGET_CAT_TO_CENTURY = {
    "supplies": "Supplies",
    "repairs": "Repairs & Maintenance",
    "maintenance": "Repairs & Maintenance",
}

BUDGET_STATUSES = [
    "not_started", "data_collection", "data_ready", "draft",
    "pm_pending", "pm_in_progress", "fa_review",
    "exec_review", "presentation", "approved",
    "ar_pending", "ar_complete", "returned"
]
USER_ROLES = ["fa", "pm", "admin", "cfo", "director", "ar"]

VALID_TRANSITIONS = {
    "not_started": ["data_collection"],
    "data_collection": ["data_ready"],
    "data_ready": ["draft"],
    "draft": ["pm_pending"],
    "pm_pending": ["pm_in_progress", "draft"],
    "pm_in_progress": ["fa_review"],
    "fa_review": ["approved", "returned", "exec_review"],
    "exec_review": ["presentation", "approved", "returned"],
    "presentation": ["approved", "returned"],
    "approved": ["ar_pending"],
    "ar_pending": ["ar_complete"],
    "returned": ["draft"],
}


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
        building_type = db.Column(db.String(50), default="")
        year = db.Column(db.Integer, nullable=False)
        version = db.Column(db.Integer, default=1)
        status = db.Column(db.String(20), default="not_started")
        fa_notes = db.Column(db.Text, default="")
        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        # Pipeline tracking
        initiated_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
        initiated_at = db.Column(db.DateTime, nullable=True)
        return_to_status = db.Column(db.String(20), nullable=True)

        # Presentation
        presentation_token = db.Column(db.String(64), unique=True, nullable=True)

        # Approval
        approved_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
        approved_at = db.Column(db.DateTime, nullable=True)
        increase_pct = db.Column(db.Float, nullable=True)
        effective_date = db.Column(db.String(20), nullable=True)
        ar_notes = db.Column(db.Text, default="")

        # Assumptions snapshot (JSON — merged portfolio + building overrides)
        assumptions_json = db.Column(db.Text, default="{}")

        # Relationships (use backref on child side to avoid forward-reference issues)
        lines = db.relationship("BudgetLine", back_populates="budget", cascade="all, delete-orphan")

        __table_args__ = (db.UniqueConstraint("entity_code", "year", "version", name="uq_entity_year_ver"),)

        def to_dict(self):
            return {
                "id": self.id,
                "entity_code": self.entity_code,
                "building_name": self.building_name,
                "building_type": self.building_type or "",
                "year": self.year,
                "version": self.version or 1,
                "status": self.status,
                "fa_notes": self.fa_notes,
                "initiated_by": self.initiated_by,
                "initiated_at": (self.initiated_at.isoformat() + "Z") if self.initiated_at else None,
                "presentation_token": self.presentation_token,
                "approved_by": self.approved_by,
                "approved_at": (self.approved_at.isoformat() + "Z") if self.approved_at else None,
                "increase_pct": self.increase_pct,
                "effective_date": self.effective_date,
                "created_at": (self.created_at.isoformat() + "Z") if self.created_at else None,
                "updated_at": (self.updated_at.isoformat() + "Z") if self.updated_at else None,
                "ar_notes": self.ar_notes or ""
            }


    class BudgetLine(db.Model):
        """Individual budget line item (all GL codes, not just R&M)."""
        __tablename__ = "budget_lines"

        id = db.Column(db.Integer, primary_key=True)
        budget_id = db.Column(db.Integer, db.ForeignKey("budgets.id"), nullable=False)
        gl_code = db.Column(db.String(50), nullable=False)
        description = db.Column(db.String(255), nullable=False)
        category = db.Column(db.String(50), nullable=False)  # supplies, repairs, maintenance, income, payroll, etc.
        row_num = db.Column(db.Integer, nullable=False)
        sheet_name = db.Column(db.String(50), default="")  # Income, Payroll, Energy, Water & Sewer, Repairs & Supplies, Gen & Admin

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
        pm_editable = db.Column(db.Boolean, default=False)

        # Reclassification (PM can propose moving expenses to different GL)
        reclass_to_gl = db.Column(db.String(50), nullable=True)
        reclass_amount = db.Column(db.Float, default=0.0)
        reclass_notes = db.Column(db.Text, default="")

        # FA override fields (when FA manually overrides a formula cell)
        estimate_override = db.Column(db.Float, nullable=True)
        forecast_override = db.Column(db.Float, nullable=True)

        # Proposed budget (computed or manually entered)
        proposed_budget = db.Column(db.Float, default=0.0)
        proposed_formula = db.Column(db.Text, nullable=True)  # e.g. "=3462.12*1.04*12"

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
                "sheet_name": self.sheet_name or "",
                "prior_year": float(self.prior_year or 0),
                "ytd_actual": float(self.ytd_actual or 0),
                "ytd_budget": float(self.ytd_budget or 0),
                "current_budget": float(self.current_budget or 0),
                "accrual_adj": float(self.accrual_adj or 0),
                "unpaid_bills": float(self.unpaid_bills or 0),
                "increase_pct": float(self.increase_pct or 0),
                "notes": self.notes,
                "pm_editable": self.pm_editable,
                "reclass_to_gl": self.reclass_to_gl,
                "reclass_amount": float(self.reclass_amount or 0),
                "reclass_notes": self.reclass_notes or "",
                "proposed_budget": float(self.proposed_budget or 0),
                "proposed_formula": self.proposed_formula,
                "estimate_override": self.estimate_override,
                "forecast_override": self.forecast_override,
                "updated_at": (self.updated_at.isoformat() + "Z") if self.updated_at else None
            }


    # ─── New Pipeline Tables ─────────────────────────────────────────────────

    class DataSource(db.Model):
        """Tracks collection status of each data source per building budget."""
        __tablename__ = "data_sources"

        id = db.Column(db.Integer, primary_key=True)
        budget_id = db.Column(db.Integer, db.ForeignKey("budgets.id"), nullable=False)
        source_type = db.Column(db.String(50), nullable=False)  # ysl, audit, sharepoint_payroll, etc.
        status = db.Column(db.String(20), default="pending")  # pending, collecting, collected, failed, not_required
        file_path = db.Column(db.Text, nullable=True)
        collected_at = db.Column(db.DateTime, nullable=True)
        error_message = db.Column(db.Text, default="")
        metadata_json = db.Column(db.Text, default="{}")

        budget = db.relationship("Budget", backref=db.backref("data_sources", cascade="all, delete-orphan"))

        def to_dict(self):
            return {
                "id": self.id, "budget_id": self.budget_id,
                "source_type": self.source_type, "status": self.status,
                "file_path": self.file_path,
                "collected_at": self.collected_at.isoformat() if self.collected_at else None,
                "error_message": self.error_message
            }


    class BudgetRevision(db.Model):
        """Audit trail for every change to a budget or its lines."""
        __tablename__ = "budget_revisions"

        id = db.Column(db.Integer, primary_key=True)
        budget_id = db.Column(db.Integer, db.ForeignKey("budgets.id"), nullable=False)
        budget_line_id = db.Column(db.Integer, db.ForeignKey("budget_lines.id"), nullable=True)
        user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
        action = db.Column(db.String(50), nullable=False)  # create, update, status_change, reclass, presentation_edit
        field_name = db.Column(db.String(100), default="")
        old_value = db.Column(db.Text, default="")
        new_value = db.Column(db.Text, default="")
        notes = db.Column(db.Text, default="")
        source = db.Column(db.String(30), default="web")  # web, presentation, api, system
        created_at = db.Column(db.DateTime, default=datetime.utcnow)

        budget = db.relationship("Budget", backref=db.backref("revisions", cascade="all, delete-orphan"))

        def to_dict(self):
            return {
                "id": self.id, "budget_id": self.budget_id,
                "budget_line_id": self.budget_line_id, "user_id": self.user_id,
                "action": self.action, "field_name": self.field_name,
                "old_value": self.old_value, "new_value": self.new_value,
                "notes": self.notes, "source": self.source,
                "created_at": self.created_at.isoformat() if self.created_at else None
            }


    class PresentationSession(db.Model):
        """Tracks live presentation sessions for client budget review."""
        __tablename__ = "presentation_sessions"

        id = db.Column(db.Integer, primary_key=True)
        budget_id = db.Column(db.Integer, db.ForeignKey("budgets.id"), nullable=False)
        token = db.Column(db.String(64), unique=True, nullable=False)
        created_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
        is_active = db.Column(db.Boolean, default=True)
        expires_at = db.Column(db.DateTime, nullable=True)
        client_name = db.Column(db.String(255), default="")
        notes = db.Column(db.Text, default="")
        created_at = db.Column(db.DateTime, default=datetime.utcnow)

        budget = db.relationship("Budget")

        def to_dict(self):
            return {
                "id": self.id, "budget_id": self.budget_id,
                "token": self.token, "is_active": self.is_active,
                "client_name": self.client_name,
                "created_at": self.created_at.isoformat() if self.created_at else None
            }


    class PresentationEdit(db.Model):
        """Individual edits made during a client presentation."""
        __tablename__ = "presentation_edits"

        id = db.Column(db.Integer, primary_key=True)
        session_id = db.Column(db.Integer, db.ForeignKey("presentation_sessions.id"), nullable=False)
        budget_line_id = db.Column(db.Integer, db.ForeignKey("budget_lines.id"), nullable=False)
        field_name = db.Column(db.String(100), nullable=False)
        old_value = db.Column(db.Text, default="")
        new_value = db.Column(db.Text, default="")
        edited_at = db.Column(db.DateTime, default=datetime.utcnow)

        session = db.relationship("PresentationSession", backref=db.backref("edits", cascade="all, delete-orphan"))

        def to_dict(self):
            return {
                "id": self.id, "session_id": self.session_id,
                "budget_line_id": self.budget_line_id,
                "field_name": self.field_name,
                "old_value": self.old_value, "new_value": self.new_value,
                "edited_at": self.edited_at.isoformat() if self.edited_at else None
            }


    class ARHandoff(db.Model):
        """AR department handoff record for approved budgets."""
        __tablename__ = "ar_handoffs"

        id = db.Column(db.Integer, primary_key=True)
        budget_id = db.Column(db.Integer, db.ForeignKey("budgets.id"), nullable=False, unique=True)
        entity_code = db.Column(db.String(50), nullable=False)
        building_name = db.Column(db.String(255), nullable=False)
        approved_increase_pct = db.Column(db.Float, nullable=False)
        effective_date = db.Column(db.String(20), nullable=False)
        approved_by_name = db.Column(db.String(255), default="")
        approved_at = db.Column(db.DateTime, nullable=True)
        total_current_budget = db.Column(db.Float, default=0.0)
        total_proposed_budget = db.Column(db.Float, default=0.0)
        supporting_notes = db.Column(db.Text, default="")
        ar_status = db.Column(db.String(20), default="pending")  # pending, acknowledged, entered, verified
        ar_acknowledged_by = db.Column(db.String(255), default="")
        ar_acknowledged_at = db.Column(db.DateTime, nullable=True)
        ar_entered_at = db.Column(db.DateTime, nullable=True)
        yardi_confirmation = db.Column(db.Text, default="")
        created_at = db.Column(db.DateTime, default=datetime.utcnow)

        budget = db.relationship("Budget", backref=db.backref("ar_handoff", uselist=False))

        def to_dict(self):
            return {
                "id": self.id, "budget_id": self.budget_id,
                "entity_code": self.entity_code, "building_name": self.building_name,
                "approved_increase_pct": self.approved_increase_pct,
                "effective_date": self.effective_date,
                "approved_by_name": self.approved_by_name,
                "total_current_budget": float(self.total_current_budget or 0),
                "total_proposed_budget": float(self.total_proposed_budget or 0),
                "supporting_notes": self.supporting_notes,
                "ar_status": self.ar_status,
                "ar_acknowledged_by": self.ar_acknowledged_by,
                "ar_acknowledged_at": self.ar_acknowledged_at.isoformat() if self.ar_acknowledged_at else None,
                "ar_entered_at": self.ar_entered_at.isoformat() if self.ar_entered_at else None,
                "yardi_confirmation": self.yardi_confirmation,
                "created_at": self.created_at.isoformat() if self.created_at else None
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
            # Get or create budget (latest version for budget year)
            budget = get_budget_for_year(entity_code, BUDGET_YEAR)
            if not budget:
                budget = Budget(
                    entity_code=entity_code,
                    building_name=building_name,
                    year=BUDGET_YEAR,
                    version=1,
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
                        line.reclass_to_gl = None
                        line.reclass_amount = 0.0
                        line.reclass_notes = ""
                        line.estimate_override = None
                        line.forecast_override = None
                        line.proposed_budget = 0.0
                        line.proposed_formula = None
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


    SHEET_TO_CATEGORY = {
        "Income": "income",
        "Payroll": "payroll",
        "Energy": "energy",
        "Water & Sewer": "water_sewer",
        "Repairs & Supplies": "rm",
        "Gen & Admin": "gen_admin",
    }

    # ── Building type lookup (from CSV, synced from Monday.com) ──────────────
    _building_type_cache = {}

    def _load_building_types():
        """Load entity→building_type mapping from buildings.csv (once, cached)."""
        if _building_type_cache:
            return _building_type_cache
        import csv as _csv
        csv_path = os.path.join(os.path.dirname(__file__), "budget_system", "buildings.csv")
        if not os.path.exists(csv_path):
            csv_path = os.path.join(os.path.dirname(__file__), "..", "budget_system", "buildings.csv")
        try:
            with open(csv_path, newline="", encoding="utf-8") as f:
                for row in _csv.DictReader(f):
                    ec = str(row.get("entity_code", "")).strip()
                    btype = (row.get("type", "") or "").strip()
                    if ec:
                        _building_type_cache[ec] = btype
        except Exception as e:
            logger.warning(f"Could not load building types from CSV: {e}")
        return _building_type_cache

    def _lookup_building_type(entity_code):
        """Return building type (Coop/Condo/etc.) for an entity code."""
        types = _load_building_types()
        return types.get(str(entity_code).strip(), "")

    # ── Budget year + version helpers ────────────────────────────────────
    BUDGET_YEAR = datetime.utcnow().year + 1  # YSL is current year, budget is next

    def get_active_budget(entity_code):
        """Return the latest-version budget for an entity (any year, latest version).
        Uses NULLS LAST so old budgets with NULL version don't sort first."""
        return (Budget.query
                .filter_by(entity_code=str(entity_code).strip())
                .order_by(Budget.year.desc(), db.func.coalesce(Budget.version, 1).desc())
                .first())

    def get_budget_for_year(entity_code, year):
        """Return the latest-version budget for an entity + specific year.
        Uses NULLS LAST so old budgets with NULL version don't sort first."""
        return (Budget.query
                .filter_by(entity_code=str(entity_code).strip(), year=year)
                .order_by(db.func.coalesce(Budget.version, 1).desc())
                .first())

    def _safe_sql(sql, params):
        """Execute SQL safely using a savepoint so failures don't kill the transaction."""
        try:
            nested = db.session.begin_nested()
            result = db.session.execute(db.text(sql), params).rowcount
            nested.commit()
            return result
        except Exception:
            nested.rollback()
            return 0

    def _clear_entity_customizations(entity_code):
        """Clear PM customizations on entity-level data (reclasses, etc.)
        without deleting the underlying Yardi source data.
        Called by fresh_start to reset user-entered changes."""
        ec = str(entity_code).strip()
        cleared = _safe_sql("""
            UPDATE expense_invoices SET reclass_to_gl = NULL,
                   reclass_notes = NULL, reclassed_by = NULL, reclassed_at = NULL
            WHERE report_id IN (SELECT id FROM expense_reports WHERE entity_code = :ec)
              AND reclass_to_gl IS NOT NULL
        """, {"ec": ec})
        if cleared:
            logger.info(f"Cleared {cleared} invoice reclasses for {ec}")

    def _delete_entity_data(entity_code):
        """Delete ALL entity-level supplementary data (expenses, open AP, etc.).
        Called by budget deletion to fully remove an entity's data."""
        ec = str(entity_code).strip()
        # Each table pair wrapped separately so a missing table doesn't poison the transaction
        _safe_sql("DELETE FROM expense_invoices WHERE report_id IN (SELECT id FROM expense_reports WHERE entity_code = :ec)", {"ec": ec})
        _safe_sql("DELETE FROM expense_reports WHERE entity_code = :ec", {"ec": ec})
        _safe_sql("DELETE FROM open_ap_invoices WHERE report_id IN (SELECT id FROM open_ap_reports WHERE entity_code = :ec)", {"ec": ec})
        _safe_sql("DELETE FROM open_ap_reports WHERE entity_code = :ec", {"ec": ec})
        logger.info(f"Deleted entity-level data for {ec}")

    def store_all_lines(entity_code, building_name, gl_data, template_path, assumptions=None, fresh_start=False):
        """
        Store ALL GL codes from YSL data into budget_lines (not just R&M).
        Uses GLMapper to get sheet/row/description for every GL code.
        Optionally stores merged assumptions snapshot on the Budget record.
        """
        try:
            from gl_mapper import build_gl_mapping_with_descriptions
        except ImportError:
            from budget_system.gl_mapper import build_gl_mapping_with_descriptions

        try:
            gl_mapping = build_gl_mapping_with_descriptions(template_path)
        except Exception as e:
            logger.error(f"Failed to build GL mapping: {e}")
            gl_mapping = {}

        try:
            if fresh_start:
                # Create a brand new version — old budget stays untouched
                existing = get_budget_for_year(entity_code, BUDGET_YEAR)
                next_ver = ((existing.version or 1) + 1) if existing else 1
                budget = Budget(
                    entity_code=entity_code,
                    building_name=building_name,
                    building_type=_lookup_building_type(entity_code),
                    year=BUDGET_YEAR,
                    version=next_ver,
                    status="draft"
                )
                db.session.add(budget)
                db.session.flush()

                # Clear PM customizations (reclasses etc.) but keep Yardi source data
                _clear_entity_customizations(str(entity_code))
                logger.info(f"Fresh start: cleared customizations for {entity_code}")
            else:
                # Update existing or create first version
                budget = get_budget_for_year(entity_code, BUDGET_YEAR)
                if not budget:
                    budget = Budget(
                        entity_code=entity_code,
                        building_name=building_name,
                        building_type=_lookup_building_type(entity_code),
                        year=BUDGET_YEAR,
                        version=1,
                        status="draft"
                    )
                    db.session.add(budget)
                    db.session.flush()
                elif not budget.building_type:
                    budget.building_type = _lookup_building_type(entity_code)

            # Store assumptions snapshot if provided
            if assumptions:
                import json as _json_mod
                budget.assumptions_json = _json_mod.dumps(assumptions)

            is_draft = budget.status == "draft"
            stored = 0

            for gl_code, gl_values in gl_data.items():
                prior_year = float(gl_values.get("period_2", 0) or 0)
                ytd_actual = float(gl_values.get("period_3", 0) or 0)
                ytd_budget = float(gl_values.get("period_4", 0) or 0)
                current_budget = float(gl_values.get("period_5", 0) or 0)

                # Determine sheet, row, description, category
                if gl_code in RM_GL_MAP:
                    desc, row_num, category = RM_GL_MAP[gl_code]
                    sheet_name = "Repairs & Supplies"
                    pm_editable = True
                elif gl_code in gl_mapping:
                    sheet_name, row_num, desc = gl_mapping[gl_code]
                    category = SHEET_TO_CATEGORY.get(sheet_name, "other")
                    pm_editable = False
                else:
                    desc = gl_code
                    sheet_name = "Unmapped"
                    row_num = 0
                    category = "other"
                    pm_editable = False

                line = BudgetLine.query.filter_by(budget_id=budget.id, gl_code=gl_code).first()
                if line:
                    line.prior_year = prior_year
                    line.ytd_actual = ytd_actual
                    line.ytd_budget = ytd_budget
                    line.current_budget = current_budget
                    line.sheet_name = sheet_name
                    line.description = desc
                    line.category = category
                    line.pm_editable = pm_editable
                    if is_draft:
                        # Clear ALL customizations on regeneration
                        line.accrual_adj = 0.0
                        line.unpaid_bills = 0.0
                        line.increase_pct = 0.0
                        line.notes = ""
                        line.reclass_to_gl = None
                        line.reclass_amount = 0.0
                        line.reclass_notes = ""
                        line.estimate_override = None
                        line.forecast_override = None
                        line.proposed_budget = 0.0
                        line.proposed_formula = None
                else:
                    line = BudgetLine(
                        budget_id=budget.id,
                        gl_code=gl_code,
                        description=desc,
                        category=category,
                        row_num=row_num,
                        sheet_name=sheet_name,
                        pm_editable=pm_editable,
                        prior_year=prior_year,
                        ytd_actual=ytd_actual,
                        ytd_budget=ytd_budget,
                        current_budget=current_budget
                    )
                    db.session.add(line)
                stored += 1

            db.session.commit()
            logger.info(f"Stored {stored} lines for {entity_code} (all GL codes)")
            return True
        except Exception as e:
            logger.error(f"Error storing lines: {e}")
            db.session.rollback()
            return False


    def get_pm_projections(entity_code):
        """
        Get PM-entered projections for a building.

        Returns: {gl_code: {accrual_adj, unpaid_bills, increase_pct, notes}}
        """
        budget = get_active_budget(entity_code)
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


    def compute_forecast(ytd_actual, accrual_adj, unpaid_bills, prior_year=0, ytd_months=2):
        """
        Compute 12-month forecast.

        Formula: ytd_actual + accrual_adj + unpaid_bills + estimate
        where estimate = (ytd_actual / ytd_months) * remaining_months
        Estimate annualizes only YTD Actual; accrual and unpaid are added separately.
        """
        remaining = 12 - ytd_months

        if ytd_months > 0 and ytd_actual > 0:
            estimate = (ytd_actual / ytd_months) * remaining
        else:
            estimate = 0

        return ytd_actual + accrual_adj + unpaid_bills + estimate


    def forecast_method(ytd_actual, accrual_adj, unpaid_bills, prior_year=0):
        """Return the forecast method label for display purposes."""
        return 'Annualized'


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
        budget = get_active_budget(entity_code)
        if not budget:
            return "No budget found for this building", 404
        return render_template_string(BUILDING_DETAIL_TEMPLATE, entity_code=entity_code)


    @bp.route("/pm", methods=["GET"])
    def pm_portal():
        """PM Portal - select building and edit R&M lines."""
        import json as json_mod

        # Ensure at least one PM user exists for demo
        pm_users = User.query.filter_by(role="pm").all()
        if not pm_users:
            demo_pm = User(name="Test PM", email="testpm@centuryny.com", role="pm")
            db.session.add(demo_pm)
            db.session.commit()
            pm_users = [demo_pm]

        return render_template_string(
            PM_PORTAL_TEMPLATE,
            pm_users_json=json_mod.dumps([u.to_dict() for u in pm_users]),
        )


    @bp.route("/pm/<entity_code>", methods=["GET"])
    def pm_edit(entity_code):
        """PM Edit Page - spreadsheet-style R&M grid."""
        budget = get_active_budget(entity_code)

        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        # Check if PM can edit this budget
        can_edit = budget.status in ["pm_pending", "pm_in_progress", "returned"]

        # PM only sees Repairs & Supplies lines (pm_editable=True)
        lines = BudgetLine.query.filter_by(budget_id=budget.id, pm_editable=True).order_by(BudgetLine.row_num).all()
        import json as json_mod

        lines_data = [l.to_dict() for l in lines]

        # Derive dynamic YTD months from assumptions
        _ytd_months = 2
        try:
            assumptions = json_mod.loads(budget.assumptions_json) if budget.assumptions_json else {}
            bp = assumptions.get("budget_period", "")
            if "/" in str(bp):
                _ytd_months = int(str(bp).split("/")[0])
        except Exception:
            pass
        _remaining_months = 12 - _ytd_months

        return render_template_string(
            PM_EDIT_TEMPLATE,
            entity_code=entity_code,
            building_name=budget.building_name,
            status=budget.status,
            can_edit="true" if can_edit else "false",
            fa_notes=budget.fa_notes or "",
            lines_json=json_mod.dumps(lines_data),
            ytd_months=_ytd_months,
            remaining_months=_remaining_months,
            estimate_label=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][_ytd_months] + '-Dec' if _ytd_months < 12 else 'Estimate',
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


    @bp.route("/api/users/<int:user_id>/delete", methods=["POST"])
    def delete_user(user_id):
        """Delete a user."""
        user = User.query.get(user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404

        db.session.delete(user)
        db.session.commit()

        return jsonify({"status": "deleted"})


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


    @bp.route("/api/assignments/<int:assignment_id>/delete", methods=["POST"])
    def delete_assignment(assignment_id):
        """Delete an assignment."""
        assignment = BuildingAssignment.query.get(assignment_id)
        if not assignment:
            return jsonify({"error": "Assignment not found"}), 404

        db.session.delete(assignment)
        db.session.commit()

        return jsonify({"status": "deleted"})


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
                    db.text("SELECT 1 FROM expense_reports WHERE entity_code = :ec LIMIT 1"),
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
            # Check maintenance proof exists
            try:
                has_maint_proof = db.session.execute(
                    db.text("SELECT 1 FROM maint_proof_reports WHERE entity_code = :ec LIMIT 1"),
                    {"ec": b.entity_code}
                ).fetchone() is not None
            except Exception:
                has_maint_proof = False
            d["has_expenses"] = has_expenses
            d["has_audit"] = has_audit
            d["has_maint_proof"] = has_maint_proof
            result.append(d)
        return jsonify(result)


    @bp.route("/api/budgets/<entity_code>/status", methods=["POST"])
    def change_budget_status(entity_code):
        """Change budget status with validation using VALID_TRANSITIONS."""
        data = request.get_json()
        new_status = data.get("status")

        if new_status not in BUDGET_STATUSES:
            return jsonify({"error": f"Invalid status. Must be one of {BUDGET_STATUSES}"}), 400

        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        # Validate transition using VALID_TRANSITIONS
        allowed = VALID_TRANSITIONS.get(budget.status, [])
        if new_status not in allowed:
            return jsonify({"error": f"Cannot move from '{budget.status}' to '{new_status}'. Allowed: {allowed}"}), 400

        if "notes" in data:
            budget.fa_notes = data["notes"]

        if new_status == "approved":
            budget.approved_by = data.get("approved_by", "system")
            budget.approved_at = datetime.utcnow()

        old_status = budget.status
        budget.status = new_status

        # Log status change
        db.session.add(BudgetRevision(
            budget_id=budget.id, action="status_change",
            field_name="status", old_value=old_status, new_value=new_status,
            notes=data.get("notes", ""), source="web"
        ))
        db.session.commit()

        return jsonify(budget.to_dict())


    @bp.route("/api/budgets/<int:budget_id>", methods=["DELETE"])
    def delete_budget(budget_id):
        """Delete a non-approved budget and all its related records."""
        budget = Budget.query.get(budget_id)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        if budget.status == "approved":
            return jsonify({"error": "Cannot delete an approved budget."}), 400

        entity = budget.entity_code
        ver = budget.version or 1

        try:
            line_ids = [l.id for l in budget.lines]
            # Delete records that reference budget_lines (must go first)
            if line_ids:
                PresentationEdit.query.filter(PresentationEdit.budget_line_id.in_(line_ids)).delete(synchronize_session=False)
                BudgetRevision.query.filter(BudgetRevision.budget_line_id.in_(line_ids)).delete(synchronize_session=False)
            # Delete records that reference the budget directly
            BudgetRevision.query.filter_by(budget_id=budget_id).delete(synchronize_session=False)
            PresentationSession.query.filter_by(budget_id=budget_id).delete(synchronize_session=False)
            ARHandoff.query.filter_by(budget_id=budget_id).delete(synchronize_session=False)
            DataSource.query.filter_by(budget_id=budget_id).delete(synchronize_session=False)
            BudgetLine.query.filter_by(budget_id=budget_id).delete(synchronize_session=False)
            # Wipe entity-level data (expense reports, open AP, etc.)
            _delete_entity_data(entity)
            db.session.delete(budget)
            db.session.commit()
            logger.info(f"Deleted budget {budget_id} (entity {entity}, v{ver})")
            return jsonify({"message": f"Budget v{ver} for {entity} deleted", "id": budget_id})
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to delete budget {budget_id}: {e}")
            return jsonify({"error": f"Failed to delete: {str(e)}"}), 500


    @bp.route("/api/dashboard/<entity_code>", methods=["GET"])
    def api_building_detail(entity_code):
        """Get combined budget data for building detail view."""
        budget = get_active_budget(entity_code)
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
                db.text("SELECT id, period_from, period_to, total_amount FROM expense_reports WHERE entity_code = :ec ORDER BY uploaded_at DESC LIMIT 1"),
                {"ec": entity_code}
            ).fetchone()
            if row:
                invoice_count = db.session.execute(
                    db.text("SELECT COUNT(*) FROM expense_invoices WHERE report_id = :rid"),
                    {"rid": row[0]}
                ).fetchone()[0]
                expense_data = {
                    "exists": True,
                    "report_id": row[0],
                    "period_from": row[1],
                    "period_to": row[2],
                    "total_amount": float(row[3]) if row[3] else 0,
                    "invoice_count": invoice_count
                }
        except Exception:
            db.session.rollback()

        # Check audit data — fetch ALL confirmed uploads for multi-year comparison
        audit_data = {"exists": False, "years": {}}
        try:
            import json as _json
            audit_rows = db.session.execute(
                db.text("SELECT mapped_data, fiscal_year_end FROM audit_upload WHERE entity_code = :ec AND status = 'confirmed' ORDER BY fiscal_year_end DESC"),
                {"ec": entity_code}
            ).fetchall()
            if audit_rows:
                years_data = {}
                for row in audit_rows:
                    if not row[0]:
                        continue
                    mapped = _json.loads(row[0])
                    fiscal_year = row[1] or "Unknown"
                    # Extract year_totals[0] for each category (the primary year)
                    year_cats = {}
                    for cat, data in mapped.items():
                        if isinstance(data, dict):
                            totals = data.get("year_totals", data.get("years", []))
                            if totals and len(totals) > 0:
                                year_cats[cat] = totals[0]
                            elif data.get("total"):
                                year_cats[cat] = data["total"]
                    if year_cats:
                        years_data[fiscal_year] = year_cats
                if years_data:
                    audit_data = {
                        "exists": True,
                        "years": years_data,
                        "category_mapping": BUDGET_CAT_TO_CENTURY
                    }
        except Exception:
            db.session.rollback()

        # Check maintenance proof data
        maint_proof_data = {"exists": False}
        try:
            mp_row = db.session.execute(
                db.text("SELECT id, building_type, charge_label, total_units, total_shares, total_monthly, total_annual, file_name FROM maint_proof_reports WHERE entity_code = :ec ORDER BY uploaded_at DESC LIMIT 1"),
                {"ec": entity_code}
            ).fetchone()
            if mp_row:
                maint_proof_data = {
                    "exists": True,
                    "building_type": mp_row[1] or "",
                    "charge_label": mp_row[2] or "Maintenance",
                    "total_units": mp_row[3] or 0,
                    "total_shares": float(mp_row[4] or 0),
                    "total_monthly": float(mp_row[5] or 0),
                    "total_annual": float(mp_row[6] or 0),
                    "file_name": mp_row[7] or "",
                }
        except Exception:
            db.session.rollback()

        # Check building type for this entity (from CSV cache)
        btype = _lookup_building_type(entity_code)
        if btype:
            charge_label = "Common Charges" if btype.lower() == "condo" else "Maintenance"
            building_type_info = {
                "building_type": btype,
                "charge_label": charge_label,
                "needs_maint_proof": btype.lower() in ["coop", "condo", "cond-op"],
            }
        else:
            building_type_info = {"building_type": "", "charge_label": "Maintenance", "needs_maint_proof": False}

        # Compute expense invoice reclass adjustments per GL
        expense_reclass_adj = {}  # {gl_code: net_ytd_adjustment}
        reclass_summary_items = []  # for FA summary panel
        try:
            exp_report_id = expense_data.get("report_id")
            if exp_report_id:
                reclassed_invoices = db.session.execute(
                    db.text("SELECT gl_code, reclass_to_gl, amount, payee_name, invoice_num, reclassed_by, reclassed_at, reclass_notes FROM expense_invoices WHERE report_id = :rid AND reclass_to_gl IS NOT NULL AND reclass_to_gl != ''"),
                    {"rid": exp_report_id}
                ).fetchall()
                for inv in reclassed_invoices:
                    src_gl = inv[0]
                    tgt_gl = inv[1]
                    amt = float(inv[2] or 0)
                    expense_reclass_adj[src_gl] = expense_reclass_adj.get(src_gl, 0) - amt
                    expense_reclass_adj[tgt_gl] = expense_reclass_adj.get(tgt_gl, 0) + amt
                    reclass_summary_items.append({
                        "from_gl": src_gl,
                        "to_gl": tgt_gl,
                        "amount": round(amt, 2),
                        "vendor": inv[3] or "",
                        "invoice_num": inv[4] or "",
                        "reclassed_by": inv[5] or "PM",
                        "reclassed_at": str(inv[6]) if inv[6] else "",
                        "notes": inv[7] or ""
                    })
                if reclassed_invoices:
                    logger.info(f"Expense reclass for {entity_code}: {len(reclassed_invoices)} reclassed invoices, adj={expense_reclass_adj}")
        except Exception as e:
            logger.warning(f"Could not compute expense reclass adjustments: {e}")

        # Group lines by sheet for tabbed view
        sheets = {}
        for l in lines:
            sn = l.sheet_name or "Unmapped"
            if sn not in sheets:
                sheets[sn] = []
            ld = l.to_dict()
            # Attach reclass adjustment info
            adj = expense_reclass_adj.get(l.gl_code, 0)
            if adj:
                ld["_reclass_ytd_adj"] = round(adj, 2)
                ld["_orig_ytd"] = ld["ytd_actual"]
                ld["ytd_actual"] = round(ld["ytd_actual"] + adj, 2)
            sheets[sn].append(ld)

        # Ordered sheet tab names
        sheet_order = ["Income", "Payroll", "Energy", "Water & Sewer", "Repairs & Supplies", "Gen & Admin", "RE Taxes", "Unmapped"]

        # Parse stored assumptions
        import json as _json
        try:
            assumptions = _json.loads(budget.assumptions_json) if budget.assumptions_json else {}
        except Exception:
            assumptions = {}

        # Derive YTD months from assumptions or default to 2
        ytd_months = 2
        try:
            bp = assumptions.get("budget_period", "")
            if bp and "/" in bp:
                ytd_months = int(bp.split("/")[0])
        except Exception:
            pass
        remaining_months = 12 - ytd_months

        # Fetch RE Taxes data for co-ops
        re_taxes_data = None
        try:
            from dof_taxes import is_coop, compute_re_taxes
            if is_coop(entity_code):
                re_taxes_data = compute_re_taxes(entity_code, assumptions.get("re_taxes_overrides"))
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"RE Taxes fetch failed for {entity_code}: {e}")

        # Build lines list with reclass adjustments applied
        lines_with_adj = []
        for l in lines:
            ld = l.to_dict()
            adj = expense_reclass_adj.get(l.gl_code, 0)
            if adj:
                ld["_reclass_ytd_adj"] = round(adj, 2)
                ld["_orig_ytd"] = ld["ytd_actual"]
                ld["ytd_actual"] = round(ld["ytd_actual"] + adj, 2)
            lines_with_adj.append(ld)

        return jsonify({
            "budget": budget.to_dict(),
            "lines": lines_with_adj,
            "sheets": sheets,
            "sheet_order": [s for s in sheet_order if s in sheets or (s == "RE Taxes" and re_taxes_data)],
            "assignments": {"fa": fa_name, "pm": pm_name},
            "expenses": expense_data,
            "audit": audit_data,
            "maint_proof": maint_proof_data,
            "building_type_info": building_type_info,
            "assumptions": assumptions,
            "ytd_months": ytd_months,
            "remaining_months": remaining_months,
            "reclass_summary": reclass_summary_items,
            "re_taxes": re_taxes_data
        })


    # ─── API Routes: Budget Assumptions ──────────────────────────────────────

    @bp.route("/api/budget-assumptions/<entity_code>", methods=["GET"])
    def get_budget_assumptions(entity_code):
        """Get assumptions for a budget."""
        import json as _json
        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404
        try:
            assumptions = _json.loads(budget.assumptions_json) if budget.assumptions_json else {}
        except Exception:
            assumptions = {}
        return jsonify(assumptions)

    @bp.route("/api/budget-assumptions/<entity_code>", methods=["PUT"])
    def update_budget_assumptions(entity_code):
        """Update assumptions for a budget and recalculate affected lines."""
        import json as _json
        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Merge updates into existing assumptions
        try:
            current = _json.loads(budget.assumptions_json) if budget.assumptions_json else {}
        except Exception:
            current = {}

        # Deep merge: update each section
        for key, value in data.items():
            if isinstance(value, dict) and isinstance(current.get(key), dict):
                current[key].update(value)
            else:
                current[key] = value

        budget.assumptions_json = _json.dumps(current)

        # Log assumption changes
        for key, value in data.items():
            db.session.add(BudgetRevision(
                budget_id=budget.id, action="update",
                field_name="assumptions." + key,
                old_value="", new_value=_json.dumps(value) if isinstance(value, dict) else str(value),
                source="web"
            ))

        # Recalculate affected BudgetLine increase_pct based on assumption changes
        changed_sections = list(data.keys())
        lines = BudgetLine.query.filter_by(budget_id=budget.id).all()
        recalc_count = 0

        # Map assumption sections to sheet names
        section_to_sheet = {
            "energy": "Energy",
            "water_sewer": "Water & Sewer",
            "insurance_renewal": "Insurance",  # Insurance lines are typically in Gen & Admin
        }

        for section in changed_sections:
            if section == "energy":
                # Apply energy rate increases to Energy sheet lines
                gas_inc = float(current.get("energy", {}).get("gas_rate_increase", 0) or 0)
                elec_inc = float(current.get("energy", {}).get("electric_rate_increase", 0) or 0)
                oil_inc = float(current.get("energy", {}).get("oil_rate_increase", 0) or 0)
                # Use the average of non-zero rates as default, or gas rate
                default_rate = gas_inc or elec_inc or oil_inc
                for line in lines:
                    if line.sheet_name == "Energy":
                        gl = line.gl_code or ""
                        # Gas GLs typically start with 5105, Electric with 5110, Oil with 5115
                        if "5105" in gl or "gas" in (line.description or "").lower():
                            line.increase_pct = gas_inc
                        elif "5110" in gl or "electric" in (line.description or "").lower():
                            line.increase_pct = elec_inc
                        elif "5115" in gl or "oil" in (line.description or "").lower() or "fuel" in (line.description or "").lower():
                            line.increase_pct = oil_inc
                        else:
                            line.increase_pct = default_rate
                        recalc_count += 1

            elif section == "water_sewer":
                # Apply water rate increase to Water & Sewer sheet lines
                water_inc = float(current.get("water_sewer", {}).get("rate_increase", 0) or 0)
                for line in lines:
                    if line.sheet_name == "Water & Sewer":
                        line.increase_pct = water_inc
                        recalc_count += 1

            elif section == "insurance_renewal":
                # Apply insurance renewal increase to insurance GL codes (6105-6195)
                ins_inc = float(current.get("insurance_renewal", {}).get("increase_percent", 0) or 0)
                for line in lines:
                    gl = line.gl_code or ""
                    if gl.startswith("61") and gl < "6200-0000":
                        line.increase_pct = ins_inc
                        recalc_count += 1

            elif section == "wage_increase":
                # Apply wage increase to all payroll lines
                wage_inc = float(current.get("wage_increase", {}).get("percent", 0) or 0)
                for line in lines:
                    if line.sheet_name == "Payroll":
                        line.increase_pct = wage_inc
                        recalc_count += 1

        # Derive YTD months from budget period assumption
        _ytd_months = 2
        try:
            bp = current.get("budget_period", "")
            if bp and "/" in bp:
                _ytd_months = int(bp.split("/")[0])
        except Exception:
            pass
        _remaining = 12 - _ytd_months

        # Recompute proposed_budget for all affected lines
        for line in lines:
            if line.increase_pct:
                ytd = float(line.ytd_actual or 0)
                accrual = float(line.accrual_adj or 0)
                unpaid = float(line.unpaid_bills or 0)
                if ytd > 0 and _ytd_months > 0:
                    estimate = (ytd / _ytd_months) * _remaining
                else:
                    estimate = 0
                forecast = ytd + accrual + unpaid + estimate
                line.proposed_budget = forecast * (1 + float(line.increase_pct or 0))

        db.session.commit()
        logger.info(f"Assumptions updated for {entity_code}, recalculated {recalc_count} lines")

        return jsonify({"status": "saved", "assumptions": current, "recalculated": recalc_count})

    @bp.route("/api/re-taxes/<entity_code>", methods=["GET"])
    def get_re_taxes(entity_code):
        """Get RE Taxes calculation for a co-op property, pulling from NYC DOF."""
        try:
            from dof_taxes import is_coop, compute_re_taxes
            if not is_coop(entity_code):
                return jsonify({"error": "Not a co-op", "is_coop": False}), 200
            import json as _json
            budget = get_active_budget(entity_code)
            overrides = None
            if budget and budget.assumptions_json:
                try:
                    assumptions = _json.loads(budget.assumptions_json)
                    overrides = assumptions.get("re_taxes_overrides")
                except Exception:
                    pass
            result = compute_re_taxes(entity_code, overrides)
            return jsonify({"is_coop": True, "re_taxes": result})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @bp.route("/api/re-taxes/<entity_code>", methods=["PUT"])
    def update_re_taxes(entity_code):
        """Save RE Taxes overrides (exemptions, transitional increase, etc.)."""
        import json as _json
        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        try:
            current = _json.loads(budget.assumptions_json) if budget.assumptions_json else {}
        except Exception:
            current = {}
        current["re_taxes_overrides"] = data
        budget.assumptions_json = _json.dumps(current)
        try:
            from dof_taxes import compute_re_taxes
            result = compute_re_taxes(entity_code, data)
            _update_gl_line(budget.id, "6315-0000", result["gross_tax"])
            _update_gl_line(budget.id, "6315-0010", -result["exemptions"]["coop_abatement"]["budget_year"])
            _update_gl_line(budget.id, "6315-0020", -result["exemptions"]["star"]["budget_year"])
            _update_gl_line(budget.id, "6315-0025", -result["exemptions"]["veteran"]["budget_year"])
            _update_gl_line(budget.id, "6315-0035", -result["exemptions"]["sche"]["budget_year"])
        except Exception as e:
            logger.warning(f"Failed to update Gen & Admin tax lines: {e}")
        db.session.commit()
        return jsonify({"status": "saved", "re_taxes": result})

    def _update_gl_line(budget_id, gl_code, value):
        """Update the proposed_budget for a specific GL line."""
        line = BudgetLine.query.filter_by(budget_id=budget_id, gl_code=gl_code).first()
        if line:
            line.proposed_budget = value

    # ─── API Routes: Lines ───────────────────────────────────────────────────

    @bp.route("/api/lines/<entity_code>", methods=["GET"])
    def get_lines(entity_code):
        """Get all R&M lines for a building."""
        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        lines = BudgetLine.query.filter_by(budget_id=budget.id).order_by(BudgetLine.row_num).all()
        return jsonify([l.to_dict() for l in lines])


    @bp.route("/api/lines/<entity_code>", methods=["PUT"])
    def update_lines(entity_code):
        """Update R&M lines for a building (PM data entry)."""
        data = request.get_json()

        budget = get_active_budget(entity_code)
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
            if "category" in line_data and line_data["category"]:
                line.category = line_data["category"]

        db.session.commit()

        return jsonify(budget.to_dict())


    # ─── FA Line Edit & Reclass Endpoints ────────────────────────────────────

    @bp.route("/api/fa-lines/<entity_code>", methods=["PUT"])
    def update_fa_lines(entity_code):
        """FA edits to any budget line (all sheets, not just R&M)."""
        data = request.get_json()
        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        for line_data in data.get("lines", []):
            line = BudgetLine.query.filter_by(
                budget_id=budget.id, gl_code=line_data.get("gl_code")
            ).first()
            if not line:
                continue
            # Track changes for audit trail — all editable fields
            changes = []
            editable_float_fields = [
                "increase_pct", "proposed_budget", "accrual_adj", "unpaid_bills",
                "prior_year", "ytd_actual", "ytd_budget", "current_budget",
                "estimate_override", "forecast_override"
            ]
            nullable_fields = {"estimate_override", "forecast_override"}
            for fname in editable_float_fields:
                if fname in line_data:
                    raw = line_data[fname]
                    if fname in nullable_fields and raw is None:
                        new_val = None
                    else:
                        new_val = float(raw or 0)
                    old_val = getattr(line, fname, None)
                    old_cmp = old_val if old_val is None else float(old_val or 0)
                    if old_cmp != new_val:
                        changes.append((fname, str(old_cmp), str(new_val)))
                    setattr(line, fname, new_val)
            if "notes" in line_data:
                new_val = line_data["notes"]
                if (line.notes or "") != new_val:
                    changes.append(("notes", line.notes or "", new_val))
                line.notes = new_val
            if "proposed_formula" in line_data:
                new_val = line_data["proposed_formula"]  # string or None
                old_val = line.proposed_formula or ""
                if old_val != (new_val or ""):
                    changes.append(("proposed_formula", old_val, new_val or ""))
                line.proposed_formula = new_val or None

            for field, old_v, new_v in changes:
                db.session.add(BudgetRevision(
                    budget_id=budget.id, budget_line_id=line.id,
                    action="update", field_name=field,
                    old_value=old_v, new_value=new_v, source="web"
                ))

        db.session.commit()
        return jsonify({"status": "ok"})


    @bp.route("/api/lines/<entity_code>/reclass", methods=["PUT"])
    def update_reclass(entity_code):
        """PM suggests reclassifying a GL line (FA acts on it)."""
        data = request.get_json()
        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        gl_code = data.get("gl_code")
        line = BudgetLine.query.filter_by(budget_id=budget.id, gl_code=gl_code).first()
        if not line:
            return jsonify({"error": "Line not found"}), 404

        line.reclass_to_gl = data.get("reclass_to_gl") or None
        line.reclass_amount = float(data.get("reclass_amount", 0) or 0)
        line.reclass_notes = data.get("reclass_notes", "")
        db.session.commit()
        return jsonify(line.to_dict())


    @bp.route("/api/budget-history/<entity_code>", methods=["GET"])
    def get_budget_history(entity_code):
        """Get change history (revisions) for a budget."""
        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        revisions = BudgetRevision.query.filter_by(budget_id=budget.id)\
            .order_by(BudgetRevision.created_at.desc()).limit(200).all()

        # Enrich with GL code info where applicable
        result = []
        for r in revisions:
            entry = r.to_dict()
            if r.budget_line_id:
                line = BudgetLine.query.get(r.budget_line_id)
                if line:
                    entry["gl_code"] = line.gl_code
                    entry["description"] = line.description
            result.append(entry)

        return jsonify({"revisions": result})


    @bp.route("/api/download-budget/<entity_code>", methods=["GET"])
    def download_budget(entity_code):
        """Regenerate and download budget Excel from DB data."""
        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        lines = BudgetLine.query.filter_by(budget_id=budget.id).all()
        if not lines:
            return jsonify({"error": "No budget lines found"}), 404

        # Rebuild gl_data dict from budget_lines
        gl_data = {}
        for l in lines:
            gl_data[l.gl_code] = {
                "period_2": l.prior_year or 0,
                "period_3": l.ytd_actual or 0,
                "period_4": l.ytd_budget or 0,
                "period_5": l.current_budget or 0,
            }

        try:
            from template_populator import populate_template
        except ImportError:
            from budget_system.template_populator import populate_template

        import tempfile
        from pathlib import Path as _Path
        from flask import send_file as _send_file

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = _Path(tmpdir) / f"{entity_code}_{budget.building_name}_{budget.year}_Budget.xlsx"
            template_path = _Path(__file__).parent.parent / "budget_system" / "Budget_Final_Template_v2.xlsx"

            property_info = {
                "property_code": entity_code,
                "property_name": budget.building_name,
            }

            # Dynamic YTD months for Excel export
            import json as _json_mod
            _exp_ytd = 2
            try:
                _exp_assumptions = _json_mod.loads(budget.assumptions_json) if budget.assumptions_json else {}
                _exp_bp = _exp_assumptions.get("budget_period", "")
                if "/" in str(_exp_bp):
                    _exp_ytd = int(str(_exp_bp).split("/")[0])
            except Exception:
                pass

            success = populate_template(
                template_path=template_path,
                gl_data=gl_data,
                property_info=property_info,
                output_path=output_path,
                ytd_months=_exp_ytd,
                remaining_months=12 - _exp_ytd,
            )

            if not success or not output_path.exists():
                return jsonify({"error": "Failed to generate Excel"}), 500

            return _send_file(
                output_path,
                as_attachment=True,
                download_name=output_path.name,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


    # ─── Presentation Routes ───────────────────────────────────────────────

    @bp.route("/api/presentation/generate/<entity_code>", methods=["POST"])
    def generate_presentation_link(entity_code):
        """Generate a shareable presentation token for a budget."""
        import secrets
        budget = get_active_budget(entity_code)
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        # Generate or reuse token
        if not budget.presentation_token:
            budget.presentation_token = secrets.token_urlsafe(32)
            db.session.commit()

        url = request.host_url.rstrip("/") + "/presentation/" + budget.presentation_token
        return jsonify({"token": budget.presentation_token, "url": url})


    @bp.route("/presentation/<token>", methods=["GET"])
    def presentation_view(token):
        """Client-facing read-only budget presentation."""
        import json as _json
        budget = Budget.query.filter_by(presentation_token=token).first()
        if not budget:
            return "<h1>Presentation not found</h1><p>This link may have expired or is invalid.</p>", 404

        lines = BudgetLine.query.filter_by(budget_id=budget.id).order_by(BudgetLine.sheet_name, BudgetLine.row_num).all()

        # Group by sheet
        sheets = {}
        for l in lines:
            sn = l.sheet_name or "Other"
            if sn not in sheets:
                sheets[sn] = []
            sheets[sn].append(l.to_dict())

        sheet_order = ["Income", "Payroll", "Energy", "Water & Sewer", "Repairs & Supplies", "Gen & Admin"]
        ordered = [s for s in sheet_order if s in sheets]

        # Parse assumptions for YTD months
        ytd_months = 2
        try:
            assumptions = _json.loads(budget.assumptions_json) if budget.assumptions_json else {}
            bp_val = assumptions.get("budget_period", "")
            if "/" in str(bp_val):
                ytd_months = int(str(bp_val).split("/")[0])
        except Exception:
            pass

        return render_template_string(
            PRESENTATION_TEMPLATE,
            building_name=budget.building_name,
            entity_code=budget.entity_code,
            year=budget.year,
            sheets_json=_json.dumps(sheets),
            sheet_order_json=_json.dumps(ordered),
            ytd_months=ytd_months,
            remaining_months=12 - ytd_months,
        )


    # ─── HTML Templates ─────────────────────────────────────────────────────

    return (bp, {"User": User, "BuildingAssignment": BuildingAssignment, "Budget": Budget, "BudgetLine": BudgetLine, "BudgetRevision": BudgetRevision},
            {"store_rm_lines": store_rm_lines, "store_all_lines": store_all_lines,
             "get_pm_projections": get_pm_projections,
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
    --blue-dark: #1e429f;
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
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 16px;
    margin-bottom: 32px;
  }
  .status-card {
    background: white;
    border-radius: 12px;
    padding: 24px;
    border: 2px solid var(--gray-200);
    text-align: center;
    cursor: pointer;
    transition: all 0.15s;
  }
  .status-card:hover { border-color: var(--blue); background: var(--blue-light); }
  .status-card.active { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(26,86,219,0.15); }
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

  /* Overflow menu */
  .action-menu { position: relative; display: inline-block; }
  .action-menu-btn { background: transparent; border: 1px solid var(--gray-300); border-radius: 6px; padding: 4px 10px; cursor: pointer; font-size: 16px; line-height: 1; color: var(--gray-500); }
  .action-menu-btn:hover { background: var(--gray-100); }
  .action-menu-items { display: none; position: absolute; right: 0; top: 100%; margin-top: 4px; background: white; border: 1px solid var(--gray-200); border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); min-width: 140px; z-index: 10; padding: 4px 0; }
  .action-menu-items.open { display: block; }
  .action-menu-items button { display: block; width: 100%; text-align: left; padding: 8px 14px; border: none; background: none; cursor: pointer; font-size: 13px; }
  .action-menu-items button:hover { background: var(--gray-50); }
  .action-menu-items .del-item { color: var(--red); }
  .action-menu-items .del-item:hover { background: var(--red-light); }

  /* Data dots */
  .data-dots { display: flex; gap: 6px; align-items: center; }
  .data-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
  .data-dot.on { background: var(--green); }
  .data-dot.off { background: transparent; border: 2px solid var(--gray-300); }

  /* Filter indicator */
  .filter-bar { font-size: 13px; color: var(--gray-500); margin-bottom: 12px; display: none; align-items: center; gap: 8px; }
  .filter-bar.visible { display: flex; }
  .filter-bar a { color: var(--blue); cursor: pointer; text-decoration: none; font-weight: 600; }
</style>
</head>
<body>

<!-- Global Nav -->
<nav class="top-nav">
  <a href="/" class="nav-brand">Century Management</a>
  <div class="nav-links">
    <a href="/" class="nav-link">Home</a>
    <a href="/dashboard" class="nav-link active">FA Dashboard</a>
    <a href="/pm" class="nav-link">PM Portal</a>
    <a href="/generate" class="nav-link">Generator</a>
    <a href="/audited-financials" class="nav-link">Audited Financials</a>
    <a href="/files" class="nav-link">Files</a>
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
        <h2 style="margin-bottom:0;">All Buildings</h2>
        <input type="text" id="budgetSearch" placeholder="Search buildings..." oninput="filterBudgetTable()"
          style="padding:8px 14px; border:1px solid var(--gray-200); border-radius:8px; font-size:14px; width:260px; outline:none;">
      </div>
      <div class="filter-bar" id="filterBar">Showing: <span id="filterLabel"></span> &mdash; <a onclick="clearStatusFilter()">Show all</a></div>
      <table id="budgets-table">
        <thead>
          <tr>
            <th>Building</th>
            <th>Entity</th>
            <th>Last Updated</th>
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

function showToast(msg, type='info') {
  const c = document.getElementById('toastContainer');
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity 0.3s'; setTimeout(() => t.remove(), 300); }, 3000);
}

async function loadBudgets() {
  try {
    const res = await fetch('/api/budgets');
    const budgets = await res.json();
    _allBudgets = budgets;
    renderBudgets(budgets);
    renderStatusSummary(budgets);
    document.getElementById('loadingState').style.display = 'none';
    document.getElementById('dashboardContent').style.display = '';
    return budgets;
  } catch (err) {
    console.error('Failed to load budgets:', err);
    document.getElementById('loadingState').innerHTML = '<p style="color:var(--red);">Failed to load budgets. Please refresh.</p>';
    return [];
  }
}

let _activeFilter = null;
let _allBudgets = [];

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
    card.className = 'status-card' + (_activeFilter === status ? ' active' : '');
    card.onclick = () => toggleStatusFilter(status);
    card.innerHTML = `
      <div class="count">${count}</div>
      <div class="label">${statusLabels[status]}</div>
    `;
    summary.appendChild(card);
  });
}

function toggleStatusFilter(status) {
  _activeFilter = (_activeFilter === status) ? null : status;
  renderStatusSummary(_allBudgets);
  renderBudgets(_allBudgets);
  const bar = document.getElementById('filterBar');
  if (_activeFilter) {
    bar.classList.add('visible');
    document.getElementById('filterLabel').textContent = statusLabels[_activeFilter];
  } else {
    bar.classList.remove('visible');
  }
}

function clearStatusFilter() {
  _activeFilter = null;
  renderStatusSummary(_allBudgets);
  renderBudgets(_allBudgets);
  document.getElementById('filterBar').classList.remove('visible');
}

function renderBudgets(budgets) {
  const tbody = document.querySelector('#budgets-table tbody');
  tbody.innerHTML = '';

  const filtered = _activeFilter ? budgets.filter(b => b.status === _activeFilter) : budgets;

  filtered.forEach(b => {
    const tr = document.createElement('tr');
    const statusLabel = statusLabels[b.status] || b.status;
    const statusClass = `pill-${b.status}`;

    // Data completeness: colored dots with tooltips
    const dot = (on, label) => `<span class="data-dot ${on ? 'on' : 'off'}" title="${label}"></span>`;
    const dataHtml = dot(true, 'Budget') + dot(b.has_expenses, 'Expenses') + dot(b.has_audit, 'Audit') + dot(b.has_maint_proof, 'Maint Proof');

    // PM review status pill
    const pmStatusMap = {
      'draft': 'Not Sent', 'pm_pending': 'Sent to PM', 'pm_in_progress': 'PM Working',
      'fa_review': 'Submitted', 'approved': 'Approved', 'returned': 'Returned'
    };
    const pmLabel = pmStatusMap[b.status] || b.status;
    const pmPillStyle = {
      'draft': 'background:transparent; color:var(--gray-500); border:1.5px solid var(--gray-300);',
      'pm_pending': 'background:transparent; color:#a16207; border:1.5px solid #f59e0b;',
      'pm_in_progress': 'background:transparent; color:var(--blue); border:1.5px solid var(--blue);',
      'fa_review': 'background:transparent; color:var(--orange); border:1.5px solid var(--orange);',
      'approved': 'background:transparent; color:var(--green); border:1.5px solid var(--green);',
      'returned': 'background:transparent; color:var(--red); border:1.5px solid var(--red);'
    }[b.status] || '';

    // Action buttons + overflow menu for delete
    let actionHtml = '';
    if (b.status === 'draft') {
      actionHtml = `<button class="btn-action btn-blue" onclick="changeStatus('${b.entity_code}', 'pm_pending')">Send to PM</button>`;
    } else if (b.status === 'fa_review') {
      actionHtml = `
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

    // Format timestamp
    let updatedStr = '\u2014';
    const ts = b.updated_at || b.created_at;
    if (ts) {
      const d = new Date(ts);
      updatedStr = d.toLocaleDateString('en-US', {month:'short', day:'numeric'}) + ' ' + d.toLocaleTimeString('en-US', {hour:'numeric', minute:'2-digit'});
    }

    const verBadge = (b.version && b.version > 1) ? `<span style="background:#dbeafe; color:var(--blue); font-size:10px; padding:1px 5px; border-radius:8px; margin-left:4px;">v${b.version}</span>` : '';
    tr.innerHTML = `
      <td><a href="/dashboard/${b.entity_code}" style="color: var(--blue); text-decoration: none; font-weight:500;">${b.building_name}</a>${verBadge}</td>
      <td style="font-family:monospace; font-size:13px;">${b.entity_code}</td>
      <td style="font-size:12px; color:var(--gray-500); white-space:nowrap;">${updatedStr}</td>
      <td><div class="data-dots">${dataHtml}</div></td>
      <td><span class="pill" style="${pmPillStyle}">${pmLabel}</span></td>
      <td><span class="pill ${statusClass}">${statusLabel}</span></td>
      <td style="white-space:nowrap;">${actionHtml}</td>
    `;
    tbody.appendChild(tr);
  });
}

function toggleMenu(btn) {
  const menu = btn.nextElementSibling;
  // Close any other open menus
  document.querySelectorAll('.action-menu-items.open').forEach(m => { if (m !== menu) m.classList.remove('open'); });
  menu.classList.toggle('open');
}
document.addEventListener('click', e => {
  if (!e.target.closest('.action-menu')) {
    document.querySelectorAll('.action-menu-items.open').forEach(m => m.classList.remove('open'));
  }
});

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
    showToast('Status updated to ' + statusLabels[newStatus], 'success');
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
    --blue-dark: #1e429f;
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
  .status-pipeline { display: flex; align-items: center; gap: 0; background: white; border-radius: 10px; padding: 12px 20px; margin-bottom: 24px; border: 1px solid var(--gray-200); overflow-x: auto; }
  .pipeline-step { display: flex; align-items: center; gap: 8px; padding: 6px 16px; font-size: 13px; font-weight: 600; white-space: nowrap; color: var(--gray-400); }
  .pipeline-step.completed { color: var(--green); }
  .pipeline-step.current { color: var(--blue); background: var(--blue-light); border-radius: 6px; }
  .pipeline-arrow { color: var(--gray-300); font-size: 16px; margin: 0 4px; }

  header {
    background: linear-gradient(135deg, var(--blue) 0%, var(--blue-dark) 100%);
    color: white;
    padding: 24px 20px;
  }
  header h1 { font-size: 24px; font-weight: 700; }
  header p { font-size: 14px; opacity: 0.85; margin-top: 4px; }
  .container { max-width: 1400px; margin: 0 auto; padding: 24px 20px; }
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
  .badge-green { background: var(--green-light); color: var(--green); }
  .badge-amber { background: var(--yellow-light); color: #d97706; }
  .badge-gray { background: var(--gray-100); color: var(--gray-500); }
  .panel-header .chevron { color: var(--gray-400); font-size: 16px; transition: transform 0.2s; }
  .panel-header .chevron.open { transform: rotate(180deg); }
  .panel-body { padding: 0 20px 16px; display: none; }
  .panel-body.open { display: block; }
  .panel-summary { font-size: 12px; color: var(--gray-500); margin-left: 8px; }
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
  .workbook-section {
    background: white;
    border: 2px solid var(--blue);
    border-radius: 12px;
    overflow: hidden;
    margin-bottom: 28px;
  }
  .workbook-section .workbook-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 16px 24px;
    border-bottom: 1px solid var(--gray-200);
  }
  .workbook-section .workbook-header h2 { font-size: 16px; font-weight: 700; color: var(--blue); margin: 0; }
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
</style>
</head>
<body>

<!-- Global Nav -->
<nav class="top-nav">
  <a href="/" class="nav-brand">Century Management</a>
  <div class="nav-links">
    <a href="/" class="nav-link">Home</a>
    <a href="/dashboard" class="nav-link active">FA Dashboard</a>
    <a href="/pm" class="nav-link">PM Portal</a>
    <a href="/generate" class="nav-link">Generator</a>
    <a href="/audited-financials" class="nav-link">Audited Financials</a>
    <a href="/files" class="nav-link">Files</a>
  </div>
  <div class="breadcrumb">
    <a href="/dashboard">Dashboard</a> &rsaquo; <span id="breadcrumbName">Loading...</span>
  </div>
</nav>

<!-- Toast container -->
<div class="toast-container" id="toastContainer"></div>

<header>
  <h1 id="buildingName">Loading...</h1>
  <p id="buildingMeta"></p>
</header>
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
  <!-- summary cards removed -->

  <div class="context-strip">
    <div class="panel" id="pmPanel">
      <div class="panel-header" onclick="togglePanel(this)">
        <div style="display:flex; align-items:center; gap:8px;">
          <h3>PM Expense Review</h3>
          <span class="badge badge-gray" id="pmBadge"></span>
          <span class="panel-summary" id="pmSummary"></span>
        </div>
        <span class="chevron">▾</span>
      </div>
      <div class="panel-body" id="pmTrackContent"></div>
    </div>
    <div class="panel" id="faPanel">
      <div class="panel-header" onclick="togglePanel(this)">
        <div style="display:flex; align-items:center; gap:8px;">
          <h3>FA Completion Checklist</h3>
          <span class="badge badge-blue" id="faBadge"></span>
          <span class="panel-summary" id="faSummary"></span>
        </div>
        <span class="chevron">▾</span>
      </div>
      <div class="panel-body" id="assemblyContent"></div>
    </div>
  </div>

  <div class="panel" id="auditActualsSection" style="display:none; margin-bottom:16px;">
    <div class="panel-header" onclick="togglePanel(this)">
      <div style="display:flex; align-items:center; gap:8px;">
        <h3>Historical Actuals</h3>
        <span class="badge badge-gray" id="auditBadge"></span>
      </div>
      <span class="chevron">▾</span>
    </div>
    <div class="panel-body">
      <table id="auditActualsTable">
        <thead id="auditActualsHead"></thead>
        <tbody id="auditActualsBody"></tbody>
      </table>
    </div>
  </div>

  <!-- Maintenance Proof: upload moved to /files repository -->

  <div class="panel" id="reclassSuggestions" style="display:none; margin-bottom:16px;">
    <div class="panel-header" onclick="togglePanel(this)">
      <div style="display:flex; align-items:center; gap:8px;">
        <h3>Invoice Reclasses</h3>
        <span class="badge badge-green" id="reclassBadge"></span>
      </div>
      <span class="chevron">▾</span>
    </div>
    <div class="panel-body">
      <table id="reclassTable" style="width:100%; border-collapse:collapse; font-size:13px;">
        <thead><tr><th>From GL</th><th>To GL</th><th>Amount</th><th>PM Notes</th><th>Action</th></tr></thead>
        <tbody id="reclassBody"></tbody>
      </table>
    </div>
  </div>

  <!-- Invoice Reclass Summary Panel -->
  <div class="section" id="reclassSummarySection" style="display:none;">
    <div style="display:flex; align-items:center; gap:12px; margin-bottom:12px; cursor:pointer;" onclick="document.getElementById('reclassSummaryBody').style.display = document.getElementById('reclassSummaryBody').style.display === 'none' ? '' : 'none'; this.querySelector('.toggle-arrow').textContent = document.getElementById('reclassSummaryBody').style.display === 'none' ? '▶' : '▼';">
      <h2 style="margin:0;">Invoice Reclasses</h2>
      <span class="toggle-arrow" style="font-size:12px; color:var(--gray-400);">▼</span>
      <span id="reclassSummaryBadge" style="font-size:12px; font-weight:600; padding:2px 10px; border-radius:12px; background:#fef3c7; color:#d97706;"></span>
    </div>
    <div id="reclassSummaryBody">
      <div id="reclassSummaryStats" style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:12px; margin-bottom:12px;"></div>
      <div style="overflow-x:auto;">
        <table style="width:100%; font-size:12px; border-collapse:collapse;" id="reclassSummaryTable">
          <thead><tr style="background:var(--gray-50); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px; color:var(--gray-500);">
            <th style="padding:6px 10px; text-align:left;">Invoice</th>
            <th style="padding:6px 10px; text-align:left;">Vendor</th>
            <th style="padding:6px 10px; text-align:right;">Amount</th>
            <th style="padding:6px 10px; text-align:left;">From GL</th>
            <th style="padding:6px 10px; text-align:left;">To GL</th>
            <th style="padding:6px 10px; text-align:left;">By</th>
            <th style="padding:6px 10px; text-align:left;">Notes</th>
          </tr></thead>
          <tbody id="reclassSummaryTableBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="workbook-section">
    <div class="workbook-header">
      <h2>Budget Workbook</h2>
      <div style="display:flex; gap:8px;">
        <button onclick="generatePresentationLink()" id="presLinkBtn" class="btn" style="background:var(--primary); color:white; border:none; font-size:13px; padding:8px 16px; border-radius:6px; cursor:pointer;">Board Presentation</button>
        <a href="" id="downloadExcelBtn" class="btn" style="background:var(--green); color:white; text-decoration:none; font-size:13px; padding:8px 16px; border-radius:6px;">Download Excel</a>
      </div>
    </div>
    <div id="sheetTabs" style="display:flex; gap:4px; border-bottom:2px solid var(--gray-200); margin-bottom:0; flex-wrap:wrap; padding:0 24px; background:var(--gray-50);"></div>
    <div id="sheetContent" style="overflow-x:auto; padding:0 24px;"></div>
    <div id="faSaveIndicator" style="font-size:12px; color:var(--green); margin-top:8px; padding:0 24px 12px;"></div>
  </div>

  </div><!-- end detailContent -->
</div>

<script>
const entityCode = '{{ entity_code }}';

function togglePanel(header) {
  const body = header.nextElementSibling;
  const chevron = header.querySelector('.chevron');
  body.classList.toggle('open');
  chevron.classList.toggle('open');
}
let allSheets = {};  // populated in loadDetail, used by Budget Summary
let allAssumptions = {};  // populated in loadDetail, used by Budget Summary
let YTD_MONTHS = 2;  // updated from API response
let REMAINING_MONTHS = 10;  // updated from API response

const MONTH_ABBR = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function estimateLabel() {
  // e.g. YTD_MONTHS=2 → "Mar-Dec Estimate", YTD_MONTHS=6 → "Jul-Dec Estimate"
  return MONTH_ABBR[YTD_MONTHS] + '-Dec';
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

  // Header + breadcrumb
  document.getElementById('buildingName').textContent = b.building_name;
  document.getElementById('breadcrumbName').textContent = b.building_name;
  document.title = b.building_name + ' - Century Management';
  let meta = 'Entity ' + b.entity_code + ' | ' + b.year + ' Budget' + (b.version > 1 ? ' (v' + b.version + ')' : '');
  if (b.created_at) meta += ' | Generated ' + new Date(b.created_at).toLocaleDateString('en-US', {month:'short', day:'numeric'}) + ' ' + new Date(b.created_at).toLocaleTimeString('en-US', {hour:'numeric', minute:'2-digit'});
  if (b.updated_at && b.updated_at !== b.created_at) meta += ' | Updated ' + new Date(b.updated_at).toLocaleDateString('en-US', {month:'short', day:'numeric'}) + ' ' + new Date(b.updated_at).toLocaleTimeString('en-US', {hour:'numeric', minute:'2-digit'});
  if (data.assignments.fa) meta += ' | FA: ' + data.assignments.fa;
  if (data.assignments.pm) meta += ' | PM: ' + data.assignments.pm;
  document.getElementById('buildingMeta').textContent = meta;

  // Status Pipeline
  renderStatusPipeline(b.status);

  const lines = data.lines;

  // PM Track
  const statusLabels = { draft: 'Not Sent', pm_pending: 'Sent to PM', pm_in_progress: 'PM Working', fa_review: 'Submitted for Review', approved: 'Approved', returned: 'Returned' };
  const pmStatus = statusLabels[b.status] || b.status;
  const pmBadgeClass = ['fa_review','approved'].includes(b.status) ? 'badge-green' : ['pm_pending','pm_in_progress'].includes(b.status) ? 'badge-amber' : 'badge-gray';
  document.getElementById('pmBadge').className = 'badge ' + pmBadgeClass;
  document.getElementById('pmBadge').textContent = pmStatus;
  document.getElementById('pmSummary').textContent = data.assignments.pm ? data.assignments.pm : '';

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

  // Gather PM notes and reclasses from budget lines
  const pmNoteLines = lines.filter(l => l.notes && l.notes.trim());
  const reclassedLines = lines.filter(l => l.reclass_to_gl);
  let pmExtra = '';
  if (pmNoteLines.length > 0) {
    pmExtra += '<div style="margin-top:16px;"><h4 style="font-size:13px; font-weight:600; color:var(--gray-700); margin-bottom:8px;">PM Notes (' + pmNoteLines.length + ')</h4>';
    pmExtra += '<div style="max-height:200px; overflow-y:auto; border:1px solid var(--gray-200); border-radius:8px; background:white;">';
    pmExtra += '<table style="width:100%; font-size:12px; border-collapse:collapse;">';
    pmNoteLines.forEach(l => {
      pmExtra += '<tr style="border-bottom:1px solid var(--gray-100);"><td style="padding:6px 10px; font-family:monospace; white-space:nowrap; color:var(--blue); width:90px;">' + l.gl_code + '</td><td style="padding:6px 10px; color:var(--gray-500);">' + (l.description || '') + '</td><td style="padding:6px 10px;">' + l.notes + '</td></tr>';
    });
    pmExtra += '</table></div></div>';
  }
  if (reclassedLines.length > 0) {
    pmExtra += '<div style="margin-top:16px;"><h4 style="font-size:13px; font-weight:600; color:var(--orange); margin-bottom:8px;">Reclass Suggestions (' + reclassedLines.length + ')</h4>';
    pmExtra += '<div style="max-height:200px; overflow-y:auto; border:1px solid var(--gray-200); border-radius:8px; background:white;">';
    pmExtra += '<table style="width:100%; font-size:12px; border-collapse:collapse;">';
    reclassedLines.forEach(l => {
      pmExtra += '<tr style="border-bottom:1px solid var(--gray-100);"><td style="padding:6px 10px; font-family:monospace; white-space:nowrap; width:90px;">' + l.gl_code + '</td><td style="padding:6px 10px;">→</td><td style="padding:6px 10px; font-family:monospace;">' + l.reclass_to_gl + '</td><td style="padding:6px 10px; text-align:right;">' + fmt(l.reclass_amount) + '</td><td style="padding:6px 10px; color:var(--gray-500);">' + (l.reclass_notes || '') + '</td></tr>';
    });
    pmExtra += '</table></div></div>';
  }

  document.getElementById('pmTrackContent').innerHTML =
    '<div style="display:flex; align-items:center; gap:12px; margin-bottom:16px;">' +
      '<span class="pill pill-' + b.status + '">' + pmStatus + '</span>' +
      (data.assignments.pm ? '<span style="font-size:13px; color:var(--gray-500);">Assigned to: ' + data.assignments.pm + '</span>' : '') +
    '</div>' + pmActions + pmExtra;

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

  const checks = [
    { label: 'YSL Data Imported', done: true, detail: lines.length + ' GL lines loaded' },
    { label: 'Assumptions Configured', done: anyAssumptions, detail: hasBudgetPeriod ? 'Period: ' + assumptions.budget_period : 'Not set — click Assumptions tab', action: !anyAssumptions ? 'openAssumptions' : null },
    { label: 'Expense Distribution', done: data.expenses.exists, detail: data.expenses.exists ? data.expenses.invoice_count + ' invoices (' + fmt(data.expenses.total_amount) + ')' : 'Upload via Data Collection' },
    { label: 'Audited Financials', done: data.audit.exists, detail: data.audit.exists ? Object.keys(data.audit.years || {}).length + ' years of history' : 'Upload via Data Collection' },
    { label: data.building_type_info.charge_label + ' Proof', done: data.maint_proof.exists, detail: data.maint_proof.exists ? data.maint_proof.total_units + ' units, ' + fmt(data.maint_proof.total_monthly) + '/mo (' + data.maint_proof.charge_label + ')' : (data.building_type_info.needs_maint_proof ? 'Upload via Data Collection' : 'N/A — ' + (data.building_type_info.building_type || 'Rental') + ' building') },
    { label: 'PM Review', done: pmDone, detail: pmDone ? 'PM review complete' : (pmSent ? 'Awaiting PM response' : 'Not yet sent'), action: !pmSent ? 'sendToPM' : null },
    { label: 'Review All Sheets', done: linesWithProposed >= lines.length * 0.5, detail: linesWithProposed + ' of ' + lines.length + ' lines have proposed values' },
    { label: 'Final Approval', done: b.status === 'approved', detail: b.status === 'approved' ? 'Budget approved' : 'Complete all steps above first' }
  ];

  const doneCount = checks.filter(c => c.done).length;
  const pct = Math.round(doneCount / checks.length * 100);
  const barColor = pct === 100 ? 'var(--green)' : pct >= 60 ? 'var(--blue)' : 'var(--yellow)';

  const faBadgeClass = pct === 100 ? 'badge-green' : pct >= 50 ? 'badge-blue' : 'badge-amber';
  document.getElementById('faBadge').className = 'badge ' + faBadgeClass;
  document.getElementById('faBadge').textContent = doneCount + ' / ' + checks.length;
  document.getElementById('faSummary').textContent = pct + '% complete';

  let assemblyHtml = '<div style="margin-bottom:12px;">' +
    '<div style="display:flex; justify-content:space-between; font-size:12px; color:var(--gray-500); margin-bottom:4px;"><span>' + doneCount + ' of ' + checks.length + ' complete</span><span>' + pct + '%</span></div>' +
    '<div style="height:6px; background:var(--gray-100); border-radius:3px; overflow:hidden;"><div style="height:100%; width:' + pct + '%; background:' + barColor + '; border-radius:3px; transition:width 0.3s;"></div></div></div>';

  checks.forEach((c, i) => {
    const iconClass = c.done ? 'check-done' : 'check-pending';
    const iconChar = c.done ? '\u2713' : '';
    const actionBtn = c.action ? ' <button onclick="' + c.action + '()" style="font-size:11px; padding:2px 8px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer; margin-left:8px;">Go</button>' : '';
    assemblyHtml += '<div class="checklist-item">' +
      '<div class="check-icon ' + iconClass + '">' + iconChar + '</div>' +
      '<div><div class="checklist-label">' + c.label + actionBtn + '</div>' +
      '<div class="checklist-detail">' + c.detail + '</div></div></div>';
  });

  document.getElementById('assemblyContent').innerHTML = assemblyHtml;

  // Invoice Reclass Summary Panel
  const reclassSummary = data.reclass_summary || [];
  if (reclassSummary.length > 0) {
    document.getElementById('reclassSummarySection').style.display = '';
    const totalDollars = reclassSummary.reduce((s, r) => s + r.amount, 0);
    const glsAffected = new Set();
    reclassSummary.forEach(r => { glsAffected.add(r.from_gl); glsAffected.add(r.to_gl); });

    document.getElementById('reclassSummaryBadge').textContent = reclassSummary.length + ' invoice' + (reclassSummary.length !== 1 ? 's' : '') + ' · ' + fmt(totalDollars) + ' moved';

    document.getElementById('reclassSummaryStats').innerHTML =
      '<div style="background:white; border:1px solid var(--gray-200); border-radius:8px; padding:12px; text-align:center;">' +
        '<div style="font-size:22px; font-weight:700; color:var(--blue);">' + reclassSummary.length + '</div>' +
        '<div style="font-size:11px; color:var(--gray-500);">Invoices Reclassed</div></div>' +
      '<div style="background:white; border:1px solid var(--gray-200); border-radius:8px; padding:12px; text-align:center;">' +
        '<div style="font-size:22px; font-weight:700; color:var(--blue);">' + fmt(totalDollars) + '</div>' +
        '<div style="font-size:11px; color:var(--gray-500);">Total Dollars Moved</div></div>' +
      '<div style="background:white; border:1px solid var(--gray-200); border-radius:8px; padding:12px; text-align:center;">' +
        '<div style="font-size:22px; font-weight:700; color:var(--blue);">' + glsAffected.size + '</div>' +
        '<div style="font-size:11px; color:var(--gray-500);">GL Lines Affected</div></div>';

    const tbody = document.getElementById('reclassSummaryTableBody');
    tbody.innerHTML = '';
    // Find GL descriptions from budget lines
    const glDescMap = {};
    lines.forEach(l => { if (l.gl_code) glDescMap[l.gl_code] = l.description || ''; });
    reclassSummary.forEach(r => {
      const tr = document.createElement('tr');
      tr.style.borderBottom = '1px solid var(--gray-100)';
      tr.innerHTML =
        '<td style="padding:6px 10px; font-family:monospace; font-size:11px;">#' + (r.invoice_num || '—') + '</td>' +
        '<td style="padding:6px 10px;">' + (r.vendor || '—') + '</td>' +
        '<td style="padding:6px 10px; text-align:right; font-variant-numeric:tabular-nums;">' + fmt(r.amount) + '</td>' +
        '<td style="padding:6px 10px; color:var(--red);">' + r.from_gl + ' <span style="font-size:10px; color:var(--gray-400);">' + (glDescMap[r.from_gl] || '') + '</span></td>' +
        '<td style="padding:6px 10px; color:var(--green);">' + r.to_gl + ' <span style="font-size:10px; color:var(--gray-400);">' + (glDescMap[r.to_gl] || '') + '</span></td>' +
        '<td style="padding:6px 10px; font-size:11px;">' + (r.reclassed_by || 'PM') + '</td>' +
        '<td style="padding:6px 10px; font-size:11px; color:var(--gray-500);">' + (r.notes || '') + '</td>';
      tbody.appendChild(tr);
    });
  }

  // Historical Actuals Panel (from audited financials)
  const auditYears = data.audit.exists ? data.audit.years : {};
  const auditYearKeys = Object.keys(auditYears).sort().reverse();
  const catMapping = data.audit.category_mapping || {};

  if (auditYearKeys.length > 0) {
    document.getElementById('auditActualsSection').style.display = '';
    document.getElementById('auditBadge').textContent = auditYearKeys.length + ' years';
    const auditHead = document.getElementById('auditActualsHead');
    const auditBody = document.getElementById('auditActualsBody');
    auditHead.innerHTML = '<tr><th>Century Category</th>' + auditYearKeys.map(y => '<th style="text-align:right">' + y + ' Actual</th>').join('') + '</tr>';
    auditBody.innerHTML = '';

    // Collect all categories across all years
    const allCats = new Set();
    auditYearKeys.forEach(y => Object.keys(auditYears[y]).forEach(c => allCats.add(c)));
    const sortedCats = Array.from(allCats).sort();

    let auditGrandTotals = auditYearKeys.map(() => 0);
    sortedCats.forEach(cat => {
      const tr = document.createElement('tr');
      let cells = '<td>' + cat + '</td>';
      auditYearKeys.forEach((y, i) => {
        const val = auditYears[y][cat] || 0;
        auditGrandTotals[i] += val;
        cells += '<td style="text-align:right">' + fmt(val) + '</td>';
      });
      tr.innerHTML = cells;
      auditBody.appendChild(tr);
    });

    // Total row
    const totalTr = document.createElement('tr');
    totalTr.style.fontWeight = '700';
    totalTr.style.background = 'var(--gray-100)';
    totalTr.innerHTML = '<td>Total</td>' + auditGrandTotals.map(t => '<td style="text-align:right">' + fmt(t) + '</td>').join('');
    auditBody.appendChild(totalTr);
  }

  // Reclass Suggestions
  const reclassLines = lines.filter(l => l.reclass_to_gl);
  if (reclassLines.length > 0) {
    document.getElementById('reclassSuggestions').style.display = '';
    const totalReclass = reclassLines.reduce((s, l) => s + Math.abs(l.reclass_amount || 0), 0);
    document.getElementById('reclassBadge').textContent = reclassLines.length + ' items \u00b7 ' + fmt(totalReclass);
    const reclassBody = document.getElementById('reclassBody');
    reclassBody.innerHTML = '';
    reclassLines.forEach(l => {
      const tr = document.createElement('tr');
      tr.innerHTML =
        '<td style="font-family:monospace;">' + l.gl_code + ' (' + l.description + ')</td>' +
        '<td style="font-family:monospace;">' + l.reclass_to_gl + '</td>' +
        '<td style="text-align:right">' + fmt(l.reclass_amount) + '</td>' +
        '<td>' + (l.reclass_notes || '') + '</td>' +
        '<td><button onclick="dismissReclass(\'' + l.gl_code + '\')" style="font-size:12px; padding:4px 8px; background:var(--gray-200); border:none; border-radius:4px; cursor:pointer;">Dismiss</button></td>';
      reclassBody.appendChild(tr);
    });
  }

  // Download Excel button
  document.getElementById('downloadExcelBtn').href = '/api/download-budget/' + entityCode;

  // Budget Workbook Tabs
  allSheets = data.sheets || {};  // global for Budget Summary access
  window._reTaxesData = data.re_taxes || null;  // RE Taxes tab data for co-ops
  allAssumptions = data.assumptions || {};  // global for Budget Summary access
  const sheets = allSheets;
  const sheetOrder = data.sheet_order || Object.keys(sheets);
  const tabsDiv = document.getElementById('sheetTabs');
  const contentDiv = document.getElementById('sheetContent');
  tabsDiv.innerHTML = '';

  if (sheetOrder.length === 0) {
    contentDiv.innerHTML = '<p style="padding:24px; color:var(--gray-500);">No budget data yet. Generate a budget first.</p>';
  } else {
    sheetOrder.forEach((sheetName, i) => {
      const tab = document.createElement('button');
      tab.textContent = sheetName;
      tab.className = 'sheet-tab' + (i === 0 ? ' active' : '');
      tab.dataset.sheet = sheetName;
      tab.onclick = () => renderSheet(sheetName, sheets[sheetName], tab);
      tabsDiv.appendChild(tab);
    });

    // Add Summary tab before Assumptions
    const summaryTab = document.createElement('button');
    summaryTab.textContent = '\ud83d\udcca Summary';
    summaryTab.className = 'sheet-tab';
    summaryTab.style.background = '#e0f2fe';
    summaryTab.style.color = 'var(--primary)';
    summaryTab.style.fontWeight = '600';
    summaryTab.onclick = () => renderSheet('Summary', null, summaryTab);
    tabsDiv.appendChild(summaryTab);

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

    renderSheet(sheetOrder[0], sheets[sheetOrder[0]], tabsDiv.firstChild);
  }

  // Maintenance proof upload moved to /files repository
}

// ── Checklist Action Helpers ──
async function generatePresentationLink() {
  const btn = document.getElementById('presLinkBtn');
  btn.textContent = 'Generating...';
  btn.disabled = true;
  try {
    const resp = await fetch('/api/presentation/generate/' + entityCode, {method:'POST'});
    const data = await resp.json();
    if (data.url) {
      // Show a modal with the link
      const modal = document.createElement('div');
      modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);display:flex;align-items:center;justify-content:center;z-index:1000;';
      modal.innerHTML = '<div style="background:white;border-radius:12px;padding:32px;max-width:500px;width:90%;">' +
        '<h3 style="margin-bottom:12px;">Board Presentation Link</h3>' +
        '<p style="font-size:13px;color:#64748b;margin-bottom:16px;">Share this link with board members. It provides a read-only view of the budget.</p>' +
        '<input type="text" value="' + data.url + '" readonly style="width:100%;padding:10px;border:1px solid #e2e8f0;border-radius:6px;font-size:13px;margin-bottom:12px;" onclick="this.select()">' +
        '<div style="display:flex;gap:8px;justify-content:flex-end;">' +
        '<button onclick="navigator.clipboard.writeText(\'' + data.url + '\');this.textContent=\'Copied!\'" style="padding:8px 16px;background:var(--primary);color:white;border:none;border-radius:6px;cursor:pointer;">Copy Link</button>' +
        '<button onclick="window.open(\'' + data.url + '\',\'_blank\')" style="padding:8px 16px;background:var(--green);color:white;border:none;border-radius:6px;cursor:pointer;">Open</button>' +
        '<button onclick="this.closest(\'div\').parentElement.remove()" style="padding:8px 16px;background:var(--gray-200);border:none;border-radius:6px;cursor:pointer;">Close</button>' +
        '</div></div>';
      document.body.appendChild(modal);
      modal.onclick = (e) => { if (e.target === modal) modal.remove(); };
    }
  } catch(err) { showToast('Error generating link: ' + err.message, 'error'); }
  btn.textContent = 'Board Presentation';
  btn.disabled = false;
}

function openAssumptions() {
  const tabs = document.querySelectorAll('.sheet-tab');
  const assumTab = Array.from(tabs).find(t => t.textContent.includes('Assumptions'));
  if (assumTab) assumTab.click();
  assumTab?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

// ── Assumptions Tab ──
let _assumSaveTimer = null;
function assumAutoSave(section, field, value) {
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

function recalcPayrollEstimate() {
  const hc = parseFloat(document.getElementById('ps_headcount')?.value) || 0;
  const rate = parseFloat(document.getElementById('ps_avg_rate')?.value) || 0;
  const hrs = parseFloat(document.getElementById('ps_hrs_wk')?.value) || 40;
  const wks = parseFloat(document.getElementById('ps_wks_yr')?.value) || 52;
  const benPct = (parseFloat(document.getElementById('ps_ben_pct')?.value) || 0) / 100;
  const baseWages = hc * rate * hrs * wks;
  const benefits = baseWages * benPct;
  const total = baseWages + benefits;
  const el = id => document.getElementById(id);
  if (el('ps_base_wages')) el('ps_base_wages').textContent = fmt(baseWages);
  if (el('ps_benefits')) el('ps_benefits').textContent = fmt(benefits);
  if (el('ps_total')) el('ps_total').textContent = fmt(total);
}

function renderAssumptionsTab(assumptions, contentDiv) {
  const a = assumptions || {};
  const inputStyle = 'padding:6px 10px; border:1px solid var(--gray-200); border-radius:6px; font-size:13px; width:120px;';
  const pctStyle = inputStyle + ' text-align:right; width:80px;';
  const dollarStyle = inputStyle + ' text-align:right; width:100px;';

  function pctVal(v) { return v ? (v * 100).toFixed(2) : '0'; }
  function numVal(v) { return v || 0; }

  function field(section, key, val, style, suffix) {
    const s = suffix || '';
    return '<input type="number" step="any" value="' + val + '" style="' + style + '" onchange="assumAutoSave(\'' + section + '\',\'' + key + '\', ' + (suffix === '%' ? 'this.value/100' : 'parseFloat(this.value)||0') + ')">' + s;
  }

  function section(title, content) {
    return '<div style="background:white; border:1px solid var(--gray-200); border-radius:10px; padding:20px 24px; margin-bottom:16px;">' +
      '<h3 style="font-size:16px; color:var(--blue); margin-bottom:16px; font-weight:600;">' + title + '</h3>' +
      '<div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:12px;">' + content + '</div></div>';
  }

  function item(label, input) {
    return '<div><label style="font-size:12px; color:var(--gray-500); display:block; margin-bottom:4px;">' + label + '</label>' + input + '</div>';
  }

  const pt = a.payroll_tax || {};
  const ub = a.union_benefits || {};
  const wc = a.workers_comp || {};
  const wi = a.wage_increase || {};
  const ir = a.insurance_renewal || {};
  const en = a.energy || {};
  const ws = a.water_sewer || {};

  let html = '<div style="padding:16px 0;">';

  // Payroll Tax Rates
  html += section('Payroll Tax Rates',
    item('FICA', field('payroll_tax','FICA', pctVal(pt.FICA), pctStyle, '%')) +
    item('SUI', field('payroll_tax','SUI', pctVal(pt.SUI), pctStyle, '%')) +
    item('FUI', field('payroll_tax','FUI', pctVal(pt.FUI), pctStyle, '%')) +
    item('MTA', field('payroll_tax','MTA', pctVal(pt.MTA), pctStyle, '%')) +
    item('NYS Disability', field('payroll_tax','NYS_Disability', pctVal(pt.NYS_Disability), pctStyle, '%')) +
    item('PFL', field('payroll_tax','PFL', pctVal(pt.PFL), pctStyle, '%'))
  );

  // Union Benefits
  html += section('Union Benefits (32BJ)',
    item('Welfare ($/mo/man)', field('union_benefits','welfare_monthly', numVal(ub.welfare_monthly), dollarStyle, '')) +
    item('Pension ($/wk/man)', field('union_benefits','pension_weekly', numVal(ub.pension_weekly), dollarStyle, '')) +
    item('Supp Retirement ($/wk)', field('union_benefits','supp_retirement_weekly', numVal(ub.supp_retirement_weekly), dollarStyle, '')) +
    item('Legal ($/mo)', field('union_benefits','legal_monthly', numVal(ub.legal_monthly), dollarStyle, '')) +
    item('Training ($/mo)', field('union_benefits','training_monthly', numVal(ub.training_monthly), dollarStyle, '')) +
    item('Profit Sharing ($/qtr)', field('union_benefits','profit_sharing_quarterly', numVal(ub.profit_sharing_quarterly), dollarStyle, ''))
  );

  // Workers Comp + Wage Increase
  html += section('Workers Comp & Wage Increase',
    item('Workers Comp %', field('workers_comp','percent', pctVal(wc.percent), pctStyle, '%')) +
    item('Wage Increase %', field('wage_increase','percent', pctVal(wi.percent), pctStyle, '%')) +
    item('Effective Week', '<input type="text" value="' + (wi.effective_week || 'Wk 16') + '" style="' + inputStyle + '" onchange="assumAutoSave(\'wage_increase\',\'effective_week\', this.value)">') +
    item('Pre-Increase Weeks', field('wage_increase','pre_increase_weeks', numVal(wi.pre_increase_weeks), inputStyle, '')) +
    item('Post-Increase Weeks', field('wage_increase','post_increase_weeks', numVal(wi.post_increase_weeks), inputStyle, ''))
  );

  // Insurance
  html += section('Insurance Renewal',
    item('Renewal Increase %', field('insurance_renewal','increase_percent', pctVal(ir.increase_percent), pctStyle, '%')) +
    item('Effective Date', '<input type="text" value="' + (ir.effective_date || 'Mar 2027') + '" style="' + inputStyle + '" onchange="assumAutoSave(\'insurance_renewal\',\'effective_date\', this.value)">') +
    item('Pre-Renewal Months', field('insurance_renewal','pre_renewal_months', numVal(ir.pre_renewal_months), inputStyle, '')) +
    item('Post-Renewal Months', field('insurance_renewal','post_renewal_months', numVal(ir.post_renewal_months), inputStyle, ''))
  );

  // Energy
  html += section('Energy Rates',
    item('Gas ESCO Rate ($/Therm)', field('energy','gas_esco_rate', numVal(en.gas_esco_rate), dollarStyle, '')) +
    item('Electric ESCO Rate ($/KWH)', field('energy','electric_esco_rate', numVal(en.electric_esco_rate), dollarStyle, '')) +
    item('Gas Rate Increase %', field('energy','gas_rate_increase', pctVal(en.gas_rate_increase), pctStyle, '%')) +
    item('Electric Rate Increase %', field('energy','electric_rate_increase', pctVal(en.electric_rate_increase), pctStyle, '%')) +
    item('Oil Price/Gallon', field('energy','oil_price_per_gallon', numVal(en.oil_price_per_gallon), dollarStyle, '')) +
    item('Oil Rate Increase %', field('energy','oil_rate_increase', pctVal(en.oil_rate_increase), pctStyle, '%'))
  );

  // Water & Sewer
  html += section('Water & Sewer',
    item('Rate Increase %', field('water_sewer','rate_increase', pctVal(ws.rate_increase), pctStyle, '%'))
  );

  // Payroll Staffing (assumptions-driven estimator)
  const ps = a.payroll_staffing || {};
  const psHeadcount = numVal(ps.headcount) || 0;
  const psAvgRate = numVal(ps.avg_hourly_rate) || 0;
  const psHrsWk = numVal(ps.hours_per_week) || 40;
  const psWeeksYr = numVal(ps.weeks_per_year) || 52;
  const psBenefitsPct = ps.benefits_pct !== undefined ? ps.benefits_pct : 0;
  const psBaseWages = psHeadcount * psAvgRate * psHrsWk * psWeeksYr;
  const psBenefits = psBaseWages * psBenefitsPct;
  const psTotalPayroll = psBaseWages + psBenefits;

  html += '<div style="background:white; border:1px solid var(--gray-200); border-radius:10px; padding:20px 24px; margin-bottom:16px;">' +
    '<h3 style="font-size:16px; color:var(--blue); margin-bottom:4px; font-weight:600;">Payroll Staffing Estimator</h3>' +
    '<p style="font-size:11px; color:var(--gray-400); margin-bottom:16px;">Estimates total payroll from staffing inputs. Use as a reference when budgeting payroll GL lines.</p>' +
    '<div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:12px;">' +
    item('Union Contract', '<input type="text" value="' + (ps.union_contract || '32BJ') + '" style="' + inputStyle + '" onchange="assumAutoSave(\'payroll_staffing\',\'union_contract\', this.value); recalcPayrollEstimate();">') +
    item('Total Headcount', '<input type="number" step="1" id="ps_headcount" value="' + psHeadcount + '" style="' + inputStyle + '" onchange="assumAutoSave(\'payroll_staffing\',\'headcount\', parseFloat(this.value)||0); recalcPayrollEstimate();">') +
    item('Avg Hourly Rate ($)', '<input type="number" step="0.01" id="ps_avg_rate" value="' + psAvgRate + '" style="' + dollarStyle + '" onchange="assumAutoSave(\'payroll_staffing\',\'avg_hourly_rate\', parseFloat(this.value)||0); recalcPayrollEstimate();">') +
    item('Hours / Week', '<input type="number" step="1" id="ps_hrs_wk" value="' + psHrsWk + '" style="' + inputStyle + '" onchange="assumAutoSave(\'payroll_staffing\',\'hours_per_week\', parseFloat(this.value)||0); recalcPayrollEstimate();">') +
    item('Weeks / Year', '<input type="number" step="1" id="ps_wks_yr" value="' + psWeeksYr + '" style="' + inputStyle + '" onchange="assumAutoSave(\'payroll_staffing\',\'weeks_per_year\', parseFloat(this.value)||0); recalcPayrollEstimate();">') +
    item('Benefits %', '<input type="number" step="0.1" id="ps_ben_pct" value="' + (psBenefitsPct * 100).toFixed(1) + '" style="' + pctStyle + '" onchange="assumAutoSave(\'payroll_staffing\',\'benefits_pct\', this.value/100); recalcPayrollEstimate();">%') +
    '</div>' +
    '<div style="margin-top:16px; padding-top:16px; border-top:1px solid var(--gray-200); display:grid; grid-template-columns:1fr 1fr 1fr; gap:12px; text-align:center;">' +
    '<div><div style="font-size:11px; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.5px;">Est. Base Wages</div><div id="ps_base_wages" style="font-size:18px; font-weight:700; color:var(--gray-700); margin-top:4px;">' + fmt(psBaseWages) + '</div></div>' +
    '<div><div style="font-size:11px; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.5px;">Est. Benefits</div><div id="ps_benefits" style="font-size:18px; font-weight:700; color:var(--gray-700); margin-top:4px;">' + fmt(psBenefits) + '</div></div>' +
    '<div><div style="font-size:11px; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.5px;">Est. Total Payroll</div><div id="ps_total" style="font-size:20px; font-weight:700; color:var(--primary); margin-top:4px;">' + fmt(psTotalPayroll) + '</div></div>' +
    '</div></div>';

  // Non-Operating Items
  const no = a.non_operating || {};
  const noI = no.income || {};
  const noE = no.expense || {};

  function noField(subSection, key, val) {
    return '<input type="number" step="1" value="' + (val || 0) + '" style="' + dollarStyle + '" ' +
      'onchange="assumAutoSave(\'non_operating\', \'' + subSection + '\', Object.assign({}, (allAssumptions.non_operating||{}).' + subSection + ' || {}, {' + key + ': parseFloat(this.value)||0})); allAssumptions.non_operating = allAssumptions.non_operating || {}; allAssumptions.non_operating.' + subSection + ' = allAssumptions.non_operating.' + subSection + ' || {}; allAssumptions.non_operating.' + subSection + '.' + key + ' = parseFloat(this.value)||0;">';
  }

  html += section('Non-Operating Income',
    item('Capital Assessment', noField('income','capital_assessment', noI.capital_assessment)) +
    item('Special Assessment', noField('income','special_assessment', noI.special_assessment)) +
    item('Interest Income', noField('income','interest_income', noI.interest_income)) +
    item('Insurance Proceeds', noField('income','insurance_proceeds', noI.insurance_proceeds)) +
    item('Other Non-Op Income', noField('income','other_non_op_income', noI.other_non_op_income))
  );

  html += section('Non-Operating Expenses',
    item('Capital Expenses', noField('expense','capital_expenses', noE.capital_expenses)) +
    item('Cert Fee (Tax Reduction)', noField('expense','cert_fee_tax_reduction', noE.cert_fee_tax_reduction)) +
    item('Other Non-Op Expense', noField('expense','other_non_op_expense', noE.other_non_op_expense))
  );

  // Capital Reserve
  const cr = a.capital_reserve || {};
  html += section('Capital Reserve',
    item('Reserve Balance ($)', '<input type="number" step="1" value="' + numVal(cr.reserve_balance) + '" style="' + dollarStyle + '" onchange="assumAutoSave(\'capital_reserve\',\'reserve_balance\', parseFloat(this.value)||0)">') +
    item('Reserve Note', '<input type="text" value="' + (cr.note || '') + '" style="' + inputStyle + ' width:200px;" onchange="assumAutoSave(\'capital_reserve\',\'note\', this.value)">')
  );

  html += '</div>';
  contentDiv.innerHTML = html;
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

// Parse a displayed dollar value like "$1,234" or "-$500" back to a number
function parseDollar(s) {
  if (typeof s !== 'string') return parseFloat(s) || 0;
  return parseFloat(s.replace(/[$,\s]/g, '')) || 0;
}

// cellBlur: user finished editing a regular dollar cell — reformat and save
function cellBlur(el) {
  const raw = parseDollar(el.value);
  el.dataset.raw = Math.round(raw);
  el.value = fmt(raw);
  const gl = el.dataset.gl, field = el.dataset.field;
  faLineChanged(gl, field, raw);
}

// Track the currently selected formula cell
let _activeFxCell = null;
let _formulaBarOriginal = '';  // track original value to detect changes

// ── Safe math evaluator (no eval) ──────────────────────────────────────
function safeEvalFormula(expr) {
  // Strip leading = sign
  let s = expr.trim();
  if (s.startsWith('=')) s = s.substring(1);
  // Handle percentage: 4% → 0.04
  s = s.replace(/([\d.]+)\s*%/g, '($1/100)');
  // Only allow: digits, operators, parens, decimal, whitespace
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
}

// ── fxCellFocus: populate the formula bar when clicking a formula cell ─
function fxCellFocus(el) {
  _activeFxCell = el;
  const bar = document.getElementById('faFormulaBar');
  const label = document.getElementById('faFormulaLabel');
  if (!bar || !label) return;

  const field = el.dataset.field;
  const fieldLabel = field === 'proposed_budget' ? 'Proposed Budget' :
                     field === 'estimate_override' ? 'Estimate' :
                     field === 'forecast_override' ? 'Forecast' : field;
  label.textContent = el.dataset.gl + ' / ' + fieldLabel;
  label.style.display = 'inline';
  bar.style.display = 'block';

  // All formula cells show =formula with live preview and buttons
  if (field === 'proposed_budget' && el.dataset.proposedFormula) {
    bar.value = el.dataset.proposedFormula;
  } else if (el.dataset.override === 'true') {
    bar.value = el.dataset.raw || '';
  } else {
    bar.value = el.dataset.formula || '';
  }
  _formulaBarOriginal = bar.value;
  const hasStoredFormula = !!(el.dataset.proposedFormula);
  _showFormulaButtons(true, hasStoredFormula);
  formulaBarPreview();

  // Highlight the active cell
  el.style.border = '2px solid var(--blue)';
  el.style.borderRadius = '4px';
  el.style.background = '#ecfdf5';

  // Focus the formula bar for editing — do NOT select all (user needs to edit within formula)
  bar.focus({ preventScroll: true });
  bar.setSelectionRange(bar.value.length, bar.value.length);
}

// fxCellBlur: just restore visual styling (editing now happens via Accept)
function fxCellBlur(el) {
  // Only restore styling if this cell is no longer the active formula cell
  // (clicking formula bar would blur the cell, but we don't want to deselect)
  setTimeout(() => {
    const bar = document.getElementById('faFormulaBar');
    if (document.activeElement === bar) return;  // user clicked into formula bar
    if (_activeFxCell === el) {
      el.style.border = '';
      el.style.borderRadius = '';
      el.style.background = '';
    }
  }, 100);
}

// ── Formula bar live preview ───────────────────────────────────────────
function formulaBarPreview() {
  const bar = document.getElementById('faFormulaBar');
  const preview = document.getElementById('faFormulaPreview');
  if (!bar || !preview || !_activeFxCell) return;

  const typed = bar.value.trim();
  if (!typed) {
    preview.style.display = 'none';
    // Keep buttons visible if cell had a formula (so user can Clear)
    const hadFormula = !!_activeFxCell.dataset.proposedFormula;
    _showFormulaButtons(hadFormula, hadFormula);
    return;
  }

  // Always evaluate and show live preview for any content in the bar
  const result = safeEvalFormula(typed);
  const isChanged = typed !== _formulaBarOriginal;
  if (result !== null) {
    preview.textContent = '= ' + fmt(result);
    preview.style.color = isChanged ? '#059669' : 'var(--green)';  // darker green when changed
  } else if (/^[\d$,.\-\s]+$/.test(typed)) {
    const num = parseDollar(typed);
    preview.textContent = '= ' + fmt(num);
    preview.style.color = isChanged ? '#2563eb' : 'var(--blue)';
  } else {
    preview.textContent = 'Invalid formula';
    preview.style.color = 'var(--red)';
  }
  preview.style.display = 'inline-block';

  // Always show Accept/Cancel when there's content; show Clear if cell has stored formula
  const hasStoredFormula = !!_activeFxCell.dataset.proposedFormula;
  _showFormulaButtons(true, hasStoredFormula || isChanged);
}

// ── Accept: commit formula/value to cell and save ──────────────────────
function formulaBarAccept() {
  const bar = document.getElementById('faFormulaBar');
  if (!bar || !_activeFxCell) return;

  const el = _activeFxCell;
  const typed = bar.value.trim();
  const gl = el.dataset.gl, field = el.dataset.field;

  if (field === 'proposed_budget') {
    // Proposed Budget: support formulas
    const formulaResult = safeEvalFormula(typed);
    if (formulaResult !== null && (typed.startsWith('=') || /[+\-*\/()]/.test(typed))) {
      // It's a formula — store both the result and the formula
      const rounded = Math.round(formulaResult);
      el.dataset.raw = rounded;
      el.dataset.proposedFormula = typed.startsWith('=') ? typed : '=' + typed;
      el.dataset.override = 'true';
      el.value = fmt(formulaResult);
      // Update badge to show formula indicator
      const badge = el.parentElement.querySelector('.fa-fx');
      if (badge) { badge.textContent = 'fx'; badge.style.background = '#dbeafe'; badge.style.color = 'var(--blue)'; badge.style.borderColor = 'var(--blue)'; }
      faAutoSave(gl, 'proposed_budget', rounded);
      faAutoSave(gl, 'proposed_formula', el.dataset.proposedFormula);
      faUpdateSheetTotals();
    } else {
      // Plain number
      const num = parseDollar(typed);
      if (!isNaN(num)) {
        el.dataset.raw = Math.round(num);
        el.dataset.override = 'true';
        el.dataset.proposedFormula = '';
        el.value = fmt(num);
        const badge = el.parentElement.querySelector('.fa-fx');
        if (badge) { badge.textContent = '✎'; badge.style.background = '#fef3c7'; badge.style.color = '#d97706'; badge.style.borderColor = '#d97706'; }
        faAutoSave(gl, 'proposed_budget', Math.round(num));
        faAutoSave(gl, 'proposed_formula', null);
        faUpdateSheetTotals();
      }
    }
  } else {
    // Estimate/Forecast override: formula, plain number, or revert
    const formulaResult = safeEvalFormula(typed);
    const numericVal = parseDollar(typed);
    if (formulaResult !== null && (typed.startsWith('=') || /[+\-*\/()]/.test(typed))) {
      // User typed a formula — evaluate it and save the result as override
      const rounded = Math.round(formulaResult);
      el.dataset.raw = rounded;
      el.dataset.override = 'true';
      el.value = fmt(formulaResult);
      // Update bar to show the formula still (so they can re-edit)
      el.dataset.formula = typed.startsWith('=') ? typed : '=' + typed;
      const badge = el.parentElement.querySelector('.fa-fx');
      if (badge) { badge.textContent = 'fx✎'; badge.style.background = '#dbeafe'; badge.style.color = 'var(--blue)'; badge.style.borderColor = 'var(--blue)'; }
      faLineChanged(gl, field, formulaResult);
      faAutoSave(gl, field, rounded);
    } else if (typed !== '' && !isNaN(numericVal) && /^[\d$,.\-\s]+$/.test(typed)) {
      el.dataset.raw = Math.round(numericVal);
      el.dataset.override = 'true';
      el.value = fmt(numericVal);
      const badge = el.parentElement.querySelector('.fa-fx');
      if (badge) { badge.textContent = '✎'; badge.style.background = '#fef3c7'; badge.style.color = '#d97706'; badge.style.borderColor = '#d97706'; }
      faLineChanged(gl, field, numericVal);
      faAutoSave(gl, field, Math.round(numericVal));
    } else if (typed === '' || typed.toLowerCase() === 'auto' || typed.toLowerCase() === 'formula') {
      el.dataset.override = 'false';
      const badge = el.parentElement.querySelector('.fa-fx');
      if (badge) { badge.textContent = 'fx'; badge.style.background = ''; badge.style.color = ''; badge.style.borderColor = ''; }
      faLineChanged(gl, field === 'estimate_override' ? '__recalc_estimate' :
                         field === 'forecast_override' ? '__recalc_forecast' : field, null);
      faAutoSave(gl, field, null);
    }
  }

  // Success feedback: flash the cell green, update bar, hide buttons
  el.style.border = '2px solid var(--green)';
  el.style.background = '#ecfdf5';
  const preview = document.getElementById('faFormulaPreview');
  if (preview) {
    preview.textContent = '✓ Accepted';
    preview.style.color = 'var(--green)';
    preview.style.display = 'inline-block';
  }
  _showFormulaButtons(false, false);
  // Update _formulaBarOriginal so further edits start from new value
  _formulaBarOriginal = bar.value.trim();
  setTimeout(() => {
    el.style.border = '';
    el.style.borderRadius = '';
    el.style.background = '';
    if (preview) preview.style.display = 'none';
  }, 1200);
}

// ── Cancel: revert formula bar to original ─────────────────────────────
function formulaBarCancel() {
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
    // Revert to auto-calc: forecast × (1 + increase%)
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

// fxCellKeydown: Enter/Tab applies, Escape reverts
function fxCellKeydown(el, e) {
  if (e.key === 'Enter' || e.key === 'Tab') {
    e.preventDefault();
    el.blur(); // triggers fxCellBlur
  } else if (e.key === 'Escape') {
    e.preventDefault();
    el.value = fmt(parseFloat(el.dataset.raw) || 0);
    el.readOnly = true;
    el.style.pointerEvents = 'none';
    el.style.background = '';
    el.style.border = '';
    el.style.borderRadius = '';
  }
}

// pctCellBlur: user finished editing a percentage cell — reformat and save
function pctCellBlur(el) {
  const raw = parseFloat(el.value) || 0;
  el.dataset.raw = raw.toFixed(1);
  el.value = raw.toFixed(1) + '%';
  const gl = el.dataset.gl;
  faLineChanged(gl, 'increase_pct', raw);
}

// When an input field changes, recalculate computed cells in that row and save
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
  const budget = getRaw('bud_' + gl);
  const incRaw = parseFloat(document.getElementById('inc_' + gl)?.dataset.raw) || 0;
  const incPct = incRaw / 100;

  let estimate, forecast;
  if (field === 'estimate_override' && value !== null) {
    estimate = parseFloat(value) || 0;
    forecast = ytd + accrual + unpaid + estimate;
  } else if (field === 'forecast_override' && value !== null) {
    forecast = parseFloat(value) || 0;
    estimate = forecast - (ytd + accrual + unpaid);
  } else {
    if (ytd > 0 && YTD_MONTHS > 0) {
      estimate = (ytd / YTD_MONTHS) * REMAINING_MONTHS;
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
    // Re-evaluate the stored formula (it doesn't change with forecast)
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
  // Build updated formula strings (real =formulas matching faGetFormulaTooltip)
  const estFormula = (ytd > 0 && YTD_MONTHS > 0) ? '=' + ytd + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '=0';
  const estExpr = (ytd > 0 && YTD_MONTHS > 0) ? ytd + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0';
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

  const variance = proposed - forecast;
  const varEl = document.getElementById('var_' + gl);
  if (varEl) { varEl.textContent = fmt(variance); varEl.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)'; }
  const pctEl = document.getElementById('pct_' + gl);
  if (pctEl) pctEl.textContent = (forecast ? ((proposed / forecast - 1) * 100).toFixed(1) : '0.0') + '%';

  // Recalculate sheet totals from live cell values
  faUpdateSheetTotals();
}

function faUpdateSheetTotals() {
  const raw = (id) => { const el = document.getElementById(id); return el ? parseFloat(el.dataset.raw) || 0 : 0; };

  function sumGLs(glCodes) {
    const t = {ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
    glCodes.forEach(gl => {
      const row = document.querySelector('tr[data-gl="' + gl + '"]');
      if (row && row.style.display === 'none') return;
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
    const v = t.proposed - t.forecast;
    const p = t.forecast ? (t.proposed / t.forecast - 1) : 0;
    const cells = rowEl.querySelectorAll('td');
    if (cells.length >= 11) {
      cells[1].textContent = fmt(t.ytd);
      cells[2].textContent = fmt(t.accrual);
      cells[3].textContent = fmt(t.unpaid);
      cells[4].textContent = fmt(t.estimate);
      cells[5].textContent = fmt(t.forecast);
      cells[6].textContent = fmt(t.budget);
      cells[7].textContent = '';
      cells[8].textContent = fmt(t.proposed);
      cells[9].textContent = fmt(v);
      cells[9].style.color = v >= 0 ? 'var(--red)' : 'var(--green)';
      cells[10].textContent = (p * 100).toFixed(1) + '%';
    }
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

let _faSaveTimer = null;
const _faSavePending = {};  // accumulate fields before debounced save
function faAutoSave(gl, field, value) {
  if (!_faSavePending[gl]) _faSavePending[gl] = {};
  _faSavePending[gl][field] = value;
  clearTimeout(_faSaveTimer);
  _faSaveTimer = setTimeout(async () => {
    const indicator = document.getElementById('faSaveIndicator');
    indicator.textContent = 'Saving...';
    const lines = [];
    for (const [glCode, fields] of Object.entries(_faSavePending)) {
      lines.push(Object.assign({gl_code: glCode}, fields));
    }
    // Clear pending
    for (const k in _faSavePending) delete _faSavePending[k];
    await fetch('/api/fa-lines/' + entityCode, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lines: lines})
    });
    indicator.textContent = 'Saved';
    setTimeout(() => { indicator.textContent = ''; }, 2000);
  }, 800);
}

async function dismissReclass(glCode) {
  await fetch('/api/lines/' + entityCode + '/reclass', {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({gl_code: glCode, reclass_to_gl: '', reclass_amount: 0, reclass_notes: ''})
  });
  loadDetail();
}

// Category grouping definitions per sheet
const SHEET_CATEGORIES = {
  'Income': {
    groups: [
      {key: 'recurring', label: 'Recurring Income', match: l => l.gl_code >= '4000' && l.gl_code < '4700'},
      {key: 'non_recurring', label: 'Non-Recurring Income', match: l => l.gl_code >= '4700' && l.gl_code < '5000'}
    ]
  },
  'Repairs & Supplies': {
    groups: [
      {key: 'supplies', label: 'Supplies', match: l => l.category === 'supplies'},
      {key: 'repairs', label: 'Repairs', match: l => l.category === 'repairs'},
      {key: 'maintenance', label: 'Maintenance Contracts', match: l => l.category === 'maintenance'}
    ]
  },
  'Gen & Admin': {
    groups: [
      {key: 'prof_fees', label: 'Professional Fees', match: l => (l.row_num >= 8 && l.row_num <= 16)},
      {key: 'admin', label: 'Administrative & Other', match: l => (l.row_num >= 20 && l.row_num <= 49)},
      {key: 'insurance', label: 'Insurance', match: l => (l.row_num >= 53 && l.row_num <= 64)},
      {key: 'taxes', label: 'Taxes', match: l => (l.row_num >= 68 && l.row_num <= 78)},
      {key: 'financial', label: 'Financial Expenses', match: l => (l.row_num >= 82 && l.row_num <= 90)}
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

function faComputeEstimate(l) {
  // Use override if FA set one
  if (l.estimate_override !== null && l.estimate_override !== undefined) return l.estimate_override;
  const ytd = l.ytd_actual || 0;
  if (ytd > 0 && YTD_MONTHS > 0) return (ytd / YTD_MONTHS) * REMAINING_MONTHS;
  return 0;
}

function faComputeForecast(l) {
  // Use override if FA set one
  if (l.forecast_override !== null && l.forecast_override !== undefined) return l.forecast_override;
  return (l.ytd_actual || 0) + (l.accrual_adj || 0) + (l.unpaid_bills || 0) + faComputeEstimate(l);
}

function faGetFormulaTooltip(l, field) {
  const ytd = l.ytd_actual || 0;
  const accrual = l.accrual_adj || 0;
  const unpaid = l.unpaid_bills || 0;
  const base = ytd + accrual + unpaid;
  const estimate = faComputeEstimate(l);
  const forecast = faComputeForecast(l);
  const incPct = l.increase_pct || 0;

  if (field === 'estimate') {
    if (ytd > 0 && YTD_MONTHS > 0) return '=' + ytd + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
    return '=0';
  }
  if (field === 'forecast') {
    // Use exact estimate expression to avoid rounding mismatch
    const estExpr = (ytd > 0 && YTD_MONTHS > 0) ? ytd + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0';
    return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + estExpr + ')';
  }
  if (field === 'proposed') {
    if (l.proposed_formula) return l.proposed_formula;
    // Use exact forecast expression to avoid rounding
    const fcstExpr = ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + ((ytd > 0 && YTD_MONTHS > 0) ? ytd + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0') + ')';
    return '=(' + fcstExpr + ')*(1+' + incPct.toFixed(4) + ')';
  }
  return '';
}

function renderSheet(sheetName, sheetLines, tabEl) {
  document.querySelectorAll('.sheet-tab').forEach(t => t.classList.remove('active'));
  tabEl.classList.add('active');

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

  if (!sheetLines || sheetLines.length === 0) {
    contentDiv.innerHTML = '<p style="padding:24px; color:var(--gray-500);">No data for this sheet.</p>';
    return;
  }

  // All sheets are editable for the FA — this is the budget workbench
  renderEditableSheet(sheetName, sheetLines, contentDiv);
  setTimeout(faUpdateZeroToggle, 50);
}

function renderBudgetSummary(contentDiv) {
  const thStyle = 'text-align:right; padding:10px 12px; white-space:nowrap;';
  let html = '<div style="margin-bottom:8px; display:flex; align-items:center; gap:12px;">' +
    '<span style="font-size:14px; color:var(--gray-500);">Executive budget overview — all figures roll up from detail sheets</span></div>';
  html += '<table style="width:100%; border-collapse:collapse; font-size:14px;">' +
    '<thead><tr style="background:var(--gray-100); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
    '<th style="text-align:left; padding:10px 12px; width:35%;">Category</th>' +
    '<th style="' + thStyle + '">Current<br>Budget</th>' +
    '<th style="' + thStyle + '">Proposed<br>Budget</th>' +
    '<th style="' + thStyle + '">$ V.<br>Forecast</th>' +
    '<th style="' + thStyle + '">%<br>Change</th>' +
    '</tr></thead><tbody>';

  let totalIncome = {budget:0, proposed:0, forecast:0};
  let totalExpense = {budget:0, proposed:0, forecast:0};

  SUMMARY_ROWS.forEach((sr, idx) => {
    const sheetLines = allSheets[sr.sheet] || [];
    let lines = sheetLines;
    if (sr.rowRange) {
      lines = sheetLines.filter(l => l.row_num >= sr.rowRange[0] && l.row_num <= sr.rowRange[1]);
    }
    let budget = 0, proposed = 0, fcTotal = 0;
    lines.forEach(l => {
      budget += l.current_budget || 0;
      const forecast = faComputeForecast(l);
      fcTotal += forecast;
      proposed += l.proposed_budget || (forecast * (1 + (l.increase_pct || 0)));
    });

    const variance = proposed - fcTotal;
    const pctChange = fcTotal ? (proposed / fcTotal - 1) : 0;
    const varColor = sr.type === 'income'
      ? (variance >= 0 ? 'var(--green)' : 'var(--red)')
      : (variance >= 0 ? 'var(--red)' : 'var(--green)');

    if (sr.type === 'income') { totalIncome.budget += budget; totalIncome.proposed += proposed; totalIncome.forecast += fcTotal; }
    else { totalExpense.budget += budget; totalExpense.proposed += proposed; totalExpense.forecast += fcTotal; }

    // Bold for income row, normal for expense detail
    const isIncomeRow = idx === 0;
    const rowStyle = isIncomeRow ? 'font-weight:600; background:var(--blue-50, #eff6ff);' : '';
    html += '<tr style="border-bottom:1px solid var(--gray-100); ' + rowStyle + '">' +
      '<td style="padding:10px 12px;">' + sr.label + '</td>' +
      '<td style="text-align:right; padding:10px 12px;">' + fmt(budget) + '</td>' +
      '<td style="text-align:right; padding:10px 12px;">' + fmt(proposed) + '</td>' +
      '<td style="text-align:right; padding:10px 12px; color:' + varColor + ';">' + fmt(variance) + '</td>' +
      '<td style="text-align:right; padding:10px 12px;">' + (pctChange * 100).toFixed(1) + '%</td></tr>';

    // After last expense row, add totals
    if (idx === SUMMARY_ROWS.length - 1) {
      const teBudget = totalExpense.budget, teProposed = totalExpense.proposed, teForecast = totalExpense.forecast;
      const teVar = teProposed - teForecast;
      const tePct = teForecast ? (teProposed / teForecast - 1) : 0;
      html += '<tr style="font-weight:700; background:var(--gray-100); border-top:2px solid var(--gray-300);"><td style="padding:10px 12px;">Total Operating Expenses</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(teBudget) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(teProposed) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(teVar) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + (tePct * 100).toFixed(1) + '%</td></tr>';

      // NOI
      const noiForecast = totalIncome.forecast - teForecast;
      const noiProposed = totalIncome.proposed - teProposed;
      const noiVar = noiProposed - noiForecast;
      const noiPct = noiForecast ? (noiProposed / noiForecast - 1) : 0;
      const noiColor = noiVar >= 0 ? 'var(--green)' : 'var(--red)';
      html += '<tr style="font-weight:700; background:var(--blue-50, #eff6ff); border-top:2px solid var(--primary);"><td style="padding:10px 12px;">Net Operating Income</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(totalIncome.budget - teBudget) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(noiProposed) + '</td>' +
        '<td style="text-align:right; padding:10px 12px; color:' + noiColor + ';">' + fmt(noiVar) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + (noiPct * 100).toFixed(1) + '%</td></tr>';
    }
  });

  html += '</tbody></table>';

  // ── Deficit Calculator + Maintenance Increase ─────────────────────
  const deficit = totalIncome.proposed - totalExpense.proposed;
  const isDeficit = deficit < 0;
  const deficitColor = isDeficit ? 'var(--red)' : 'var(--green)';
  const deficitLabel = isDeficit ? '<Deficit>' : 'Surplus';
  const maintIncrease = (isDeficit && totalIncome.proposed > 0)
    ? (Math.abs(deficit) / totalIncome.proposed * 100).toFixed(2)
    : '0.00';

  html += '<div style="margin-top:24px; padding:20px; background:var(--gray-50, #f9fafb); border:1px solid var(--gray-200); border-radius:8px;">' +
    '<div style="font-size:14px; font-weight:600; margin-bottom:12px; color:var(--gray-700);">Operating Surplus / ' + deficitLabel + '</div>' +
    '<div style="display:grid; grid-template-columns:1fr 1fr; gap:8px 24px; font-size:14px;">' +
    '<div style="color:var(--gray-500);">Total Income (Proposed)</div><div style="text-align:right; font-weight:500;">' + fmt(totalIncome.proposed) + '</div>' +
    '<div style="color:var(--gray-500);">Total Expenses (Proposed)</div><div style="text-align:right; font-weight:500;">' + fmt(totalExpense.proposed) + '</div>' +
    '<div style="border-top:1px solid var(--gray-300); padding-top:8px; font-weight:600;">Net Operating</div>' +
    '<div style="border-top:1px solid var(--gray-300); padding-top:8px; text-align:right; font-weight:700; font-size:16px; color:' + deficitColor + ';">' + fmt(deficit) + '</div>' +
    '</div>';

  if (isDeficit) {
    html += '<div style="margin-top:16px; padding-top:16px; border-top:1px dashed var(--gray-300);">' +
      '<div style="font-size:13px; font-weight:600; margin-bottom:8px; color:var(--gray-600);">Maintenance Increase to Cover Deficit</div>' +
      '<div style="display:grid; grid-template-columns:1fr 1fr; gap:4px 24px; font-size:13px;">' +
      '<div style="color:var(--gray-500);">Current Proposed Income</div><div style="text-align:right;">' + fmt(totalIncome.proposed) + '</div>' +
      '<div style="color:var(--gray-500);">Deficit Amount</div><div style="text-align:right; color:var(--red);">' + fmt(Math.abs(deficit)) + '</div>' +
      '<div style="font-weight:600; color:var(--orange, #d97706);">Required Increase</div><div style="text-align:right; font-weight:700; font-size:15px; color:var(--orange, #d97706);">' + maintIncrease + '%</div>' +
      '</div></div>';
  }

  html += '</div>';

  // ── Non-Operating Income / Expense ────────────────────────────────
  const nonOp = allAssumptions.non_operating || {};
  const noIncome = nonOp.income || {};
  const noExpense = nonOp.expense || {};
  const noIncomeItems = [
    {key: 'capital_assessment', label: 'Capital Assessment'},
    {key: 'special_assessment', label: 'Special Assessment'},
    {key: 'interest_income', label: 'Interest Income'},
    {key: 'insurance_proceeds', label: 'Insurance Proceeds'},
    {key: 'other_non_op_income', label: 'Other Non-Op Income'}
  ];
  const noExpenseItems = [
    {key: 'capital_expenses', label: 'Capital Expenses'},
    {key: 'cert_fee_tax_reduction', label: 'Cert Fee for Tax Reduction'},
    {key: 'other_non_op_expense', label: 'Other Non-Op Expense'}
  ];

  let totalNOI = 0, totalNOE = 0;
  noIncomeItems.forEach(i => { totalNOI += parseFloat(noIncome[i.key] || 0); });
  noExpenseItems.forEach(i => { totalNOE += parseFloat(noExpense[i.key] || 0); });

  if (totalNOI > 0 || totalNOE > 0) {
    html += '<div style="margin-top:24px; padding:20px; background:white; border:1px solid var(--gray-200); border-radius:8px;">' +
      '<div style="font-size:14px; font-weight:600; margin-bottom:12px; color:var(--gray-700);">Non-Operating Items</div>';

    if (totalNOI > 0) {
      html += '<div style="font-size:12px; font-weight:600; color:var(--green); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Non-Operating Income</div>' +
        '<div style="display:grid; grid-template-columns:1fr 1fr; gap:4px 24px; font-size:13px; margin-bottom:12px;">';
      noIncomeItems.forEach(i => {
        const v = parseFloat(noIncome[i.key] || 0);
        if (v > 0) html += '<div style="color:var(--gray-500);">' + i.label + '</div><div style="text-align:right;">' + fmt(v) + '</div>';
      });
      html += '<div style="font-weight:600; border-top:1px solid var(--gray-200); padding-top:4px;">Total Non-Op Income</div>' +
        '<div style="text-align:right; font-weight:600; border-top:1px solid var(--gray-200); padding-top:4px;">' + fmt(totalNOI) + '</div></div>';
    }

    if (totalNOE > 0) {
      html += '<div style="font-size:12px; font-weight:600; color:var(--red); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Non-Operating Expense</div>' +
        '<div style="display:grid; grid-template-columns:1fr 1fr; gap:4px 24px; font-size:13px; margin-bottom:12px;">';
      noExpenseItems.forEach(i => {
        const v = parseFloat(noExpense[i.key] || 0);
        if (v > 0) html += '<div style="color:var(--gray-500);">' + i.label + '</div><div style="text-align:right;">' + fmt(v) + '</div>';
      });
      html += '<div style="font-weight:600; border-top:1px solid var(--gray-200); padding-top:4px;">Total Non-Op Expense</div>' +
        '<div style="text-align:right; font-weight:600; border-top:1px solid var(--gray-200); padding-top:4px;">' + fmt(totalNOE) + '</div></div>';
    }

    // Net after non-operating
    const netAfterNonOp = deficit + totalNOI - totalNOE;
    const netColor = netAfterNonOp >= 0 ? 'var(--green)' : 'var(--red)';
    html += '<div style="border-top:2px solid var(--gray-300); padding-top:12px; display:grid; grid-template-columns:1fr 1fr; gap:4px 24px; font-size:14px;">' +
      '<div style="font-weight:700;">Net After Non-Operating</div>' +
      '<div style="text-align:right; font-weight:700; font-size:16px; color:' + netColor + ';">' + fmt(netAfterNonOp) + '</div></div>';

    html += '</div>';
  }

  // ── Capital Budget + Reserve Tracker ──────────────────────────────
  const reserveBal = parseFloat((allAssumptions.capital_reserve || {}).reserve_balance || 0);
  // Find capital GL lines (7xxx series) across all sheets
  const capLines = [];
  Object.keys(allSheets).forEach(sn => {
    (allSheets[sn] || []).forEach(l => {
      if (l.gl_code && l.gl_code.startsWith('7')) capLines.push(l);
    });
  });
  const capExpFromNonOp = parseFloat((nonOp.expense || {}).capital_expenses || 0);
  const totalCapFromLines = capLines.reduce((s, l) => s + (l.proposed_budget || faComputeForecast(l) * (1 + (l.increase_pct || 0))), 0);
  const totalCapSpend = totalCapFromLines > 0 ? totalCapFromLines : capExpFromNonOp;

  if (reserveBal > 0 || totalCapSpend > 0 || capLines.length > 0) {
    const remaining = reserveBal - totalCapSpend;
    const utilizationPct = reserveBal > 0 ? Math.min(100, (totalCapSpend / reserveBal * 100)) : 0;
    const barColor = utilizationPct > 80 ? 'var(--red)' : utilizationPct > 50 ? 'var(--orange, #d97706)' : 'var(--green)';

    html += '<div style="margin-top:24px; padding:20px; background:white; border:1px solid var(--gray-200); border-radius:8px;">' +
      '<div style="font-size:14px; font-weight:600; margin-bottom:16px; color:var(--gray-700);">Capital Budget</div>';

    // Reserve summary
    if (reserveBal > 0) {
      html += '<div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:12px; text-align:center; margin-bottom:16px;">' +
        '<div style="background:var(--gray-50); padding:12px; border-radius:6px;"><div style="font-size:11px; color:var(--gray-500); text-transform:uppercase;">Reserve Balance</div><div style="font-size:18px; font-weight:700; color:var(--gray-700); margin-top:4px;">' + fmt(reserveBal) + '</div></div>' +
        '<div style="background:var(--gray-50); padding:12px; border-radius:6px;"><div style="font-size:11px; color:var(--gray-500); text-transform:uppercase;">Planned Spend</div><div style="font-size:18px; font-weight:700; color:var(--red); margin-top:4px;">' + fmt(totalCapSpend) + '</div></div>' +
        '<div style="background:var(--gray-50); padding:12px; border-radius:6px;"><div style="font-size:11px; color:var(--gray-500); text-transform:uppercase;">Remaining</div><div style="font-size:18px; font-weight:700; color:' + (remaining >= 0 ? 'var(--green)' : 'var(--red)') + '; margin-top:4px;">' + fmt(remaining) + '</div></div></div>';

      // Utilization bar
      html += '<div style="margin-bottom:16px;"><div style="display:flex; justify-content:space-between; font-size:11px; color:var(--gray-500); margin-bottom:4px;"><span>Reserve Utilization</span><span>' + utilizationPct.toFixed(0) + '%</span></div>' +
        '<div style="background:var(--gray-200); border-radius:4px; height:8px; overflow:hidden;"><div style="background:' + barColor + '; height:100%; width:' + utilizationPct.toFixed(0) + '%; border-radius:4px; transition:width 0.3s;"></div></div></div>';
    }

    // Capital line items table
    if (capLines.length > 0) {
      html += '<table style="width:100%; border-collapse:collapse; font-size:13px;">' +
        '<thead><tr style="background:var(--gray-100); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
        '<th style="text-align:left; padding:8px;">GL Code</th><th style="text-align:left; padding:8px;">Description</th>' +
        '<th style="text-align:right; padding:8px;">YTD Actual</th><th style="text-align:right; padding:8px;">Proposed</th></tr></thead><tbody>';
      let capTotal = 0;
      capLines.forEach(l => {
        const proposed = l.proposed_budget || (faComputeForecast(l) * (1 + (l.increase_pct || 0)));
        capTotal += proposed;
        html += '<tr style="border-bottom:1px solid var(--gray-100);"><td style="font-family:monospace; font-size:12px; padding:6px 8px;">' + l.gl_code + '</td>' +
          '<td style="padding:6px 8px;">' + l.description + '</td>' +
          '<td style="text-align:right; padding:6px 8px;">' + fmt(l.ytd_actual || 0) + '</td>' +
          '<td style="text-align:right; padding:6px 8px;">' + fmt(proposed) + '</td></tr>';
      });
      html += '<tr style="font-weight:700; background:var(--gray-100);"><td colspan="2" style="padding:8px;">Total Capital</td>' +
        '<td></td><td style="text-align:right; padding:8px;">' + fmt(capTotal) + '</td></tr>';
      html += '</tbody></table>';
    }

    html += '</div>';
  }

  contentDiv.innerHTML = html;
}

// ── RE Taxes Tab — Custom Calculation Layout ──────────────────────────────────────────
function renderRETaxesTab(contentDiv) {
  const reTaxes = window._reTaxesData;
  if (!reTaxes) {
    contentDiv.innerHTML = '<p style="padding:24px; color:var(--gray-500);">RE Taxes data not available. This building may not be a co-op, or DOF data has not been fetched yet.</p>';
    return;
  }
  const fmt = v => '$' + Math.round(v).toLocaleString();
  const pctFmt = v => (v * 100).toFixed(2) + '%';
  const rateFmt = v => (v * 100).toFixed(4) + '%';
  const inputStyle = 'background:var(--blue-light,#eff6ff); color:var(--blue,#1a56db); font-weight:600; text-align:right; padding:7px 10px; border:1.5px solid var(--gray-200,#e5e7eb); border-radius:6px; width:140px; font-size:13px; outline:none; transition:border-color 0.15s;';
  const outputStyle = 'font-weight:600; text-align:right; padding:8px 12px; color:var(--gray-700,#374151); font-size:14px;';
  const labelStyle = 'padding:10px 16px; font-size:13px; color:var(--gray-700,#374151);';
  const noteStyle = 'padding:8px 12px; font-size:12px; color:var(--gray-400,#9ca3af);';
  const sectionStyle = 'padding:10px 16px; font-size:12px; font-weight:700; color:var(--blue,#1a56db); text-transform:uppercase; letter-spacing:0.5px; background:var(--blue-light,#eff6ff); border-top:1px solid var(--gray-200,#e5e7eb);';
  const subHeaderStyle = 'background:var(--gray-50,#f9fafb); padding:8px 16px; font-weight:600; font-size:12px; color:var(--gray-500,#6b7280);';

  const d = reTaxes;
  const ex = d.exemptions || {};

  let html = `
  <div style="max-width:900px; margin:0 auto;">
    <!-- Header -->
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
      <div>
        <div style="font-size:15px; font-weight:700; color:var(--gray-700,#374151);">Real Estate Tax Calculation</div>
        <div style="font-size:12px; color:var(--gray-400,#9ca3af); margin-top:4px;">BBL: ${d.bbl || 'N/A'} &nbsp;&middot;&nbsp; Tax Class: ${d.tax_class || '2'} &nbsp;&middot;&nbsp; Source: ${d.source || 'N/A'}</div>
      </div>
      <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
        <span id="dofRefreshTimestamp" style="font-size:11px; color:var(--gray-400,#9ca3af);"></span>
        <button onclick="refreshDOFData()" style="padding:7px 14px; background:white; color:var(--blue,#1a56db); border:1.5px solid var(--blue,#1a56db); border-radius:6px; cursor:pointer; font-size:12px; font-weight:600; transition:background 0.15s;">
          ↻ Refresh from NYC DOF
        </button>
        <a id="dofVerifyLink" href="#" target="_blank" rel="noopener" style="display:inline-flex; align-items:center; gap:5px; padding:7px 14px; background:var(--gray-50,#f9fafb); color:var(--gray-600,#4b5563); border:1.5px solid var(--gray-300,#d1d5db); border-radius:6px; text-decoration:none; font-size:12px; font-weight:600; transition:background 0.15s, border-color 0.15s;" onmouseover="this.style.background='#fff'; this.style.borderColor='var(--blue,#1a56db)'; this.style.color='var(--blue,#1a56db)';" onmouseout="this.style.background='var(--gray-50,#f9fafb)'; this.style.borderColor='var(--gray-300,#d1d5db)'; this.style.color='var(--gray-600,#4b5563)';">
          <span style="font-size:14px;">↗</span> Verify on DOF Site
        </a>
      </div>
    </div>

    <!-- Tax Calculation Card -->
    <div style="background:white; border:1px solid var(--gray-200,#e5e7eb); border-radius:10px; overflow:hidden; margin-bottom:16px;">
      <div style="${sectionStyle}; border-top:none;">Tax Calculation</div>

      <table style="width:100%; border-collapse:collapse; font-size:14px;">
        <colgroup>
          <col style="width:35%">
          <col style="width:20%">
          <col style="width:25%">
          <col style="width:20%">
        </colgroup>

        <!-- 1st Half -->
        <tr><td colspan="4" style="${subHeaderStyle}">1st Half — Current City Fiscal Year (Jul–Dec)</td></tr>
        <tr style="border-bottom:1px solid var(--gray-100,#f3f4f6);">
          <td style="${labelStyle}">Transitional Assessed Value</td>
          <td style="padding:6px 16px;"><input type="text" id="re_av" value="${d.assessed_value}" onchange="reCalcTaxes()" style="${inputStyle}"></td>
          <td colspan="2" style="${noteStyle}">Trans AV from DOF Final Assessment Roll (not Market AV)</td>
        </tr>
        <tr style="border-bottom:1px solid var(--gray-100,#f3f4f6);">
          <td style="${labelStyle}">Tax Rate (Actual) <span style="display:block; font-size:10px; color:var(--orange,#d97706); margin-top:2px;">⚠ Manual — not from DOF API</span></td>
          <td style="padding:6px 16px;"><input type="text" id="re_rate" value="${d.tax_rate}" onchange="reCalcTaxes()" style="${inputStyle}"></td>
          <td style="${outputStyle}" id="re_h1_tax">${fmt(d.first_half_tax)}</td>
          <td style="${noteStyle}">1st Half Tax</td>
        </tr>

        <!-- 2nd Half -->
        <tr><td colspan="4" style="${subHeaderStyle}">2nd Half — Next City Fiscal Year (Jan–Jun)</td></tr>
        <tr style="border-bottom:1px solid var(--gray-100,#f3f4f6);">
          <td style="${labelStyle}">Transitional AV Increase %</td>
          <td style="padding:6px 16px;"><input type="text" id="re_trans" value="${d.transitional_av_increase}" onchange="reCalcTaxes()" style="${inputStyle}"></td>
          <td colspan="2" style="${noteStyle}">Estimated increase in assessed valuation</td>
        </tr>
        <tr style="border-bottom:1px solid var(--gray-100,#f3f4f6);">
          <td style="${labelStyle}">Estimated Assessed Valuation</td>
          <td style="${outputStyle}; padding-left:16px;" id="re_est_av">${fmt(d.est_assessed_value)}</td>
          <td colspan="2" style="${noteStyle}">AV × (1 + increase %)</td>
        </tr>
        <tr style="border-bottom:1px solid var(--gray-100,#f3f4f6);">
          <td style="${labelStyle}">Estimated Tax Rate <span style="display:block; font-size:10px; color:var(--orange,#d97706); margin-top:2px;">⚠ Manual — verify on DOF site</span></td>
          <td style="padding:6px 16px;"><input type="text" id="re_est_rate" value="${d.est_tax_rate}" onchange="reCalcTaxes()" style="${inputStyle}"></td>
          <td style="${outputStyle}" id="re_h2_tax">${fmt(d.second_half_tax)}</td>
          <td style="${noteStyle}">2nd Half Tax</td>
        </tr>
      </table>

      <!-- Gross Total -->
      <div style="display:flex; justify-content:space-between; align-items:center; padding:14px 16px; background:var(--gray-50,#f9fafb); border-top:2px solid var(--gray-200,#e5e7eb);">
        <span style="font-weight:700; font-size:14px; color:var(--gray-700,#374151);">Gross Tax Liability (Full Year)</span>
        <div style="display:flex; align-items:center; gap:12px;">
          <span style="font-weight:700; font-size:16px; color:var(--blue,#1a56db);" id="re_gross">${fmt(d.gross_tax)}</span>
          <span style="font-size:11px; color:var(--gray-400,#9ca3af);">1st Half + 2nd Half</span>
        </div>
      </div>
    </div>

    <!-- Exemptions Card -->
    <div style="background:white; border:1px solid var(--gray-200,#e5e7eb); border-radius:10px; overflow:hidden; margin-bottom:16px;">
      <div style="${sectionStyle}; border-top:none;">Tax Exemptions & Abatements</div>

      <table style="width:100%; border-collapse:collapse; font-size:13px;">
        <colgroup>
          <col style="width:35%">
          <col style="width:18%">
          <col style="width:22%">
          <col style="width:25%">
        </colgroup>
        <tr style="border-bottom:1px solid var(--gray-200,#e5e7eb);">
          <td style="padding:8px 16px; font-weight:600; font-size:11px; color:var(--gray-500,#6b7280); text-transform:uppercase; letter-spacing:0.3px;">Exemption Type</td>
          <td style="padding:8px 12px; font-weight:600; font-size:11px; color:var(--gray-500,#6b7280); text-transform:uppercase; letter-spacing:0.3px; text-align:center;">Growth %</td>
          <td style="padding:8px 12px; font-weight:600; font-size:11px; color:var(--gray-500,#6b7280); text-transform:uppercase; letter-spacing:0.3px; text-align:right;">Current Year</td>
          <td style="padding:8px 12px; font-weight:600; font-size:11px; color:var(--gray-500,#6b7280); text-transform:uppercase; letter-spacing:0.3px; text-align:right;">Budget Year</td>
        </tr>`;

  // Exemption rows
  const exRows = [
    {key:'veteran', label:'Veteran Exemption', gl:'6315-0025'},
    {key:'sche', label:'Senior Citizen (SCHE)', gl:'6315-0035'},
    {key:'star', label:'STAR Exemption', gl:'6315-0020'},
    {key:'coop_abatement', label:'Co-op Abatement', gl:'6315-0010'},
  ];
  exRows.forEach(r => {
    const e = ex[r.key] || {growth_pct:0, current_year:0, budget_year:0};
    html += `<tr style="border-bottom:1px solid var(--gray-100,#f3f4f6);">
      <td style="${labelStyle}">${r.label} <span style="font-size:11px; color:var(--gray-400,#9ca3af);">GL ${r.gl}</span></td>
      <td style="padding:6px 12px; text-align:center;"><input type="text" id="re_ex_${r.key}_growth" value="${e.growth_pct}" onchange="reCalcTaxes()" style="${inputStyle} width:80px;"></td>
      <td style="padding:6px 12px;"><input type="text" id="re_ex_${r.key}_current" value="${e.current_year}" onchange="reCalcTaxes()" style="${inputStyle}"></td>
      <td style="${outputStyle}; padding-right:16px;" id="re_ex_${r.key}_budget">${fmt(e.budget_year)}</td>
    </tr>`;
  });

  html += `
        <tr style="border-top:1px solid var(--gray-200,#e5e7eb);">
          <td style="padding:10px 16px; font-weight:700; font-size:13px; color:var(--gray-700,#374151);">Total Exemptions</td>
          <td></td>
          <td style="${outputStyle}" id="re_ex_total_current">${fmt(d.total_exemptions_current)}</td>
          <td style="${outputStyle}; padding-right:16px;" id="re_ex_total_budget">${fmt(d.total_exemptions_budget)}</td>
        </tr>
      </table>
    </div>

    <!-- Net Total Card -->
    <div style="background:var(--blue-light,#eff6ff); border:2px solid var(--blue,#1a56db); border-radius:10px; padding:16px 20px; display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
      <div>
        <div style="font-weight:700; font-size:15px; color:var(--blue,#1a56db);">Net Tax Liability (Proposed Budget)</div>
        <div style="font-size:12px; color:var(--gray-400,#9ca3af); margin-top:2px;">Gross Tax − Total Exemptions</div>
      </div>
      <span style="font-weight:700; font-size:22px; color:var(--blue,#1a56db);" id="re_net">${fmt(d.net_tax)}</span>
    </div>

    <!-- Actions -->
    <div style="display:flex; align-items:center; gap:12px;">
      <button onclick="saveRETaxes()" style="padding:8px 20px; background:var(--blue,#1a56db); color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:13px; font-weight:600; transition:background 0.15s;">
        ✓ Save RE Taxes
      </button>
      <span id="reTaxSaveStatus" style="font-size:13px; color:var(--gray-400); padding:8px 0;"></span>
    </div>
  </div>`;

  contentDiv.innerHTML = html;

  // Build DOF verify link — use Property Information Portal (direct BBL lookup)
  try {
    const bblClean = (d.bbl || '').replace(/-/g, '');
    if (bblClean.length >= 10) {
      const dofLink = document.getElementById('dofVerifyLink');
      if (dofLink) {
        dofLink.href = 'https://propertyinformationportal.nyc.gov/parcels/parcel/' + bblClean;
      }
    }
  } catch(e) { console.warn('Could not build DOF verify link:', e); }

  // Format input displays on load
  _reFmtDollar('re_av');
  _reFmtRate('re_rate');
  _reFmtPct('re_trans');
  _reFmtRate('re_est_rate');
  ['veteran','sche','star','coop_abatement'].forEach(k => {
    _reFmtPct('re_ex_' + k + '_growth');
    _reFmtDollar('re_ex_' + k + '_current');
  });

  // Attach focus/blur handlers to toggle between raw and formatted
  // On focus: show the formatted percentage/dollar (user-friendly) not the raw decimal
  // On blur: parse what the user typed and convert back to raw decimal
  document.querySelectorAll('#sheetContent input[type=text]').forEach(inp => {
    inp.addEventListener('focus', function() {
      // Show editable value in user-friendly format (percentage for rates/pcts)
      const raw = parseFloat(this.dataset.raw) || 0;
      if (this.dataset.type === 'rate') this.value = (raw * 100).toFixed(4);
      else if (this.dataset.type === 'pct') this.value = (raw * 100).toFixed(2);
      else if (this.dataset.type === 'dollar') this.value = Math.round(raw);
      else this.value = this.dataset.raw || _reParseNum(this.value);
      this.select();
    });
    inp.addEventListener('blur', function() {
      let v = parseFloat(_reParseNum(this.value)) || 0;
      // User types percentage values (e.g. 9.6324 for 9.6324%) — convert to decimal
      if (this.dataset.type === 'rate' || this.dataset.type === 'pct') {
        v = v / 100;
      }
      this.dataset.raw = v;
      _reApplyFmt(this);
      reCalcTaxes();
    });
  });

  // Show last refresh timestamp if available
  const tsEl = document.getElementById('dofRefreshTimestamp');
  if (tsEl && window._dofLastRefresh) {
    const t = window._dofLastRefresh;
    tsEl.textContent = 'Last refreshed ' + t.toLocaleDateString('en-US', {month:'short', day:'numeric'}) + ' at ' + t.toLocaleTimeString('en-US', {hour:'numeric', minute:'2-digit'});
  }
}

// ── RE Taxes formatting helpers ──
function _reParseNum(v) { return String(v).replace(/[$,%\s]/g, '').replace(/,/g, '') || '0'; }
function _reFmtDollar(id) { const el = document.getElementById(id); if (!el) return; const n = parseFloat(_reParseNum(el.value)) || 0; el.dataset.raw = n; el.value = '$' + Math.round(n).toLocaleString(); el.dataset.type = 'dollar'; }
function _reFmtRate(id) { const el = document.getElementById(id); if (!el) return; const n = parseFloat(_reParseNum(el.value)) || 0; el.dataset.raw = n; el.value = (n * 100).toFixed(4) + '%'; el.dataset.type = 'rate'; }
function _reFmtPct(id) { const el = document.getElementById(id); if (!el) return; const n = parseFloat(_reParseNum(el.value)) || 0; el.dataset.raw = n; el.value = (n * 100).toFixed(2) + '%'; el.dataset.type = 'pct'; }
function _reApplyFmt(el) {
  const raw = parseFloat(el.dataset.raw) || 0;
  if (el.dataset.type === 'dollar') el.value = '$' + Math.round(raw).toLocaleString();
  else if (el.dataset.type === 'rate') el.value = (raw * 100).toFixed(4) + '%';
  else if (el.dataset.type === 'pct') el.value = (raw * 100).toFixed(2) + '%';
}

// Live recalculation of RE Taxes when inputs change
function _reVal(id) { const el = document.getElementById(id); return parseFloat(el.dataset.raw !== undefined ? el.dataset.raw : _reParseNum(el.value)) || 0; }
function reCalcTaxes() {
  const av = _reVal('re_av');
  const rate = _reVal('re_rate');
  const trans = _reVal('re_trans');
  const estRate = _reVal('re_est_rate');

  const h1 = av * rate / 2;
  const estAv = av * (1 + trans);
  const h2 = estAv * estRate / 2;
  const gross = h1 + h2;

  document.getElementById('re_h1_tax').textContent = '$' + Math.round(h1).toLocaleString();
  document.getElementById('re_est_av').textContent = '$' + Math.round(estAv).toLocaleString();
  document.getElementById('re_h2_tax').textContent = '$' + Math.round(h2).toLocaleString();
  document.getElementById('re_gross').textContent = '$' + Math.round(gross).toLocaleString();

  let totalCurrent = 0, totalBudget = 0;
  ['veteran','sche','star','coop_abatement'].forEach(key => {
    const growth = _reVal('re_ex_' + key + '_growth');
    const current = _reVal('re_ex_' + key + '_current');
    const budget = current * (1 + growth);
    document.getElementById('re_ex_' + key + '_budget').textContent = '$' + Math.round(budget).toLocaleString();
    totalCurrent += current;
    totalBudget += budget;
  });

  document.getElementById('re_ex_total_current').textContent = '$' + Math.round(totalCurrent).toLocaleString();
  document.getElementById('re_ex_total_budget').textContent = '$' + Math.round(totalBudget).toLocaleString();
  document.getElementById('re_net').textContent = '$' + Math.round(gross - totalBudget).toLocaleString();

  // Re-format inputs that weren't just edited
  if (document.activeElement && document.activeElement.tagName !== 'INPUT') {
    _reFmtDollar('re_av'); _reFmtRate('re_rate'); _reFmtPct('re_trans'); _reFmtRate('re_est_rate');
    ['veteran','sche','star','coop_abatement'].forEach(k => { _reFmtPct('re_ex_'+k+'_growth'); _reFmtDollar('re_ex_'+k+'_current'); });
  }
}

// Save RE Taxes overrides to server
async function saveRETaxes() {
  // Don't save assessed_value or tax_rate as overrides — those come from
  // config/DOF (Trans AV). Only save user-adjustable fields.
  const overrides = {
    transitional_av_increase: _reVal('re_trans'),
    est_tax_rate: _reVal('re_est_rate'),
    veteran_growth: _reVal('re_ex_veteran_growth'),
    veteran_current: _reVal('re_ex_veteran_current'),
    sche_growth: _reVal('re_ex_sche_growth'),
    sche_current: _reVal('re_ex_sche_current'),
    star_growth: _reVal('re_ex_star_growth'),
    star_current: _reVal('re_ex_star_current'),
    abatement_growth: _reVal('re_ex_coop_abatement_growth'),
    abatement_current: _reVal('re_ex_coop_abatement_current'),
  };
  const statusEl = document.getElementById('reTaxSaveStatus');
  statusEl.textContent = 'Saving...';
  statusEl.style.color = 'var(--gray-500)';
  try {
    const resp = await fetch('/api/re-taxes/' + entityCode, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(overrides)
    });
    const result = await resp.json();
    if (result.status === 'saved') {
      statusEl.textContent = '✓ Saved — Gen & Admin tax lines updated';
      statusEl.style.color = 'var(--green, #22c55e)';
      window._reTaxesData = result.re_taxes;
    } else {
      statusEl.textContent = 'Error: ' + (result.error || 'Unknown');
      statusEl.style.color = 'var(--red, #ef4444)';
    }
  } catch (e) {
    statusEl.textContent = 'Error: ' + e.message;
    statusEl.style.color = 'var(--red, #ef4444)';
  }
}

// Refresh DOF data from NYC
async function refreshDOFData() {
  const statusEl = document.getElementById('reTaxSaveStatus');
  if (statusEl) { statusEl.textContent = 'Fetching from NYC DOF...'; statusEl.style.color = 'var(--gray-500)'; }
  try {
    const resp = await fetch('/api/re-taxes/' + entityCode);
    const result = await resp.json();
    if (result.re_taxes) {
      window._reTaxesData = result.re_taxes;
      window._dofLastRefresh = new Date();
      renderRETaxesTab(document.getElementById('sheetContent'));
      if (statusEl) { statusEl.textContent = '✓ DOF data refreshed'; statusEl.style.color = 'var(--green, #22c55e)'; }
    }
  } catch (e) {
    if (statusEl) { statusEl.textContent = 'Error: ' + e.message; statusEl.style.color = 'var(--red, #ef4444)'; }
  }
}

function renderReadOnlySheet(sheetName, sheetLines, contentDiv) {
  const thStyle = 'text-align:right; padding:8px; white-space:nowrap;';
  let html = '<table style="width:100%; border-collapse:collapse; font-size:13px;">' +
    '<thead><tr style="background:var(--gray-100); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
    '<th style="text-align:left; padding:8px;">GL Code</th>' +
    '<th style="text-align:left; padding:8px;">Description</th>' +
    '<th style="' + thStyle + '">YTD<br>Actual</th>' +
    '<th style="' + thStyle + '">Approved<br>Budget</th>' +
    '<th style="' + thStyle + '">Variance</th>' +
    '</tr></thead><tbody>';

  let totals = {ytd:0, budget:0};
  sheetLines.forEach(l => {
    const ytd = l.ytd_actual || 0;
    const budget = l.current_budget || 0;
    const variance = budget - ytd;
    totals.ytd += ytd; totals.budget += budget;
    const varColor = variance >= 0 ? 'var(--red)' : 'var(--green)';

    html += '<tr style="border-bottom:1px solid var(--gray-100);">' +
      '<td style="font-family:monospace; font-size:12px; padding:6px 8px;">' + l.gl_code + '</td>' +
      '<td style="padding:6px 8px;">' + l.description + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(ytd) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(budget) + '</td>' +
      '<td style="text-align:right; padding:6px 8px; color:' + varColor + ';">' + fmt(variance) + '</td></tr>';
  });

  const totalVar = totals.budget - totals.ytd;
  html += '<tr style="font-weight:700; background:var(--gray-100);"><td style="padding:8px;" colspan="2">Sheet Total</td>' +
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

// ── FA Open AP (Unpaid Bills) drill-down ─────────────────────────────
let _faOpenAPCache = null;

async function faFetchOpenAPData() {
  if (_faOpenAPCache !== null) return _faOpenAPCache;
  try {
    const res = await fetch('/api/open-ap/' + entityCode);
    if (!res.ok) { _faOpenAPCache = false; return null; }
    _faOpenAPCache = await res.json();
    return _faOpenAPCache;
  } catch(e) { _faOpenAPCache = false; return null; }
}

async function faToggleUnpaidDrill(glCode, el) {
  const row = el.closest('tr');
  const next = row.nextElementSibling;
  if (next && next.classList.contains('fa-unpaid-detail')) {
    next.remove();
    return;
  }
  // Remove any existing open detail rows for this GL
  const existing = row.parentNode.querySelector('.fa-unpaid-detail[data-gl="' + glCode + '"]');
  if (existing) existing.remove();

  const data = await faFetchOpenAPData();
  if (!data || !data.gl_groups) {
    const noData = document.createElement('tr');
    noData.className = 'fa-unpaid-detail';
    noData.dataset.gl = glCode;
    noData.innerHTML = '<td colspan="15" style="padding:12px 24px; background:#fef3c7; font-size:13px;">No Open AP data uploaded yet. Include the AP Aging report in your budget generator upload.</td>';
    row.after(noData);
    return;
  }

  const glGroup = data.gl_groups.find(g => g.gl_code === glCode);
  if (!glGroup || !glGroup.invoices || glGroup.invoices.length === 0) {
    const noInv = document.createElement('tr');
    noInv.className = 'fa-unpaid-detail';
    noInv.dataset.gl = glCode;
    noInv.innerHTML = '<td colspan="15" style="padding:12px 24px; background:var(--gray-50); font-size:13px; color:var(--gray-500);">No unpaid invoices for ' + glCode + '</td>';
    row.after(noInv);
    return;
  }

  const detailRow = document.createElement('tr');
  detailRow.className = 'fa-unpaid-detail';
  detailRow.dataset.gl = glCode;
  let html = '<td colspan="15" style="padding:0;"><div style="padding:10px 16px 10px 40px; background:linear-gradient(to right, #fef3c7, #fffbeb); border-left:3px solid #f59e0b; border-bottom:1px solid var(--gray-200);">';
  html += '<table style="width:100%; font-size:12px; border-collapse:collapse; background:white; border-radius:6px; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,0.05);">';
  html += '<thead><tr style="background:#fef3c7; color:#92400e; font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
  html += '<td style="padding:6px 10px;">Vendor</td><td style="padding:6px 10px;">Invoice #</td><td style="padding:6px 10px;">Date</td><td style="padding:6px 10px;">Description</td><td style="padding:6px 10px; text-align:right;">Amount</td></tr></thead>';

  glGroup.invoices.forEach(inv => {
    html += '<tr style="border-top:1px solid var(--gray-100);">';
    html += '<td style="padding:5px 10px;">' + (inv.payee_name || inv.payee_code || '—') + '</td>';
    html += '<td style="padding:5px 10px; font-family:monospace; font-size:11px;">' + (inv.invoice_num || '—') + '</td>';
    html += '<td style="padding:5px 10px; white-space:nowrap;">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>';
    html += '<td style="padding:5px 10px; max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="' + (inv.invoice_notes || inv.notes || '').replace(/"/g,'&quot;') + '">' + (inv.invoice_notes || inv.notes || '—') + '</td>';
    html += '<td style="padding:5px 10px; text-align:right; font-variant-numeric:tabular-nums;">' + fmt(inv.current_owed) + '</td>';
    html += '</tr>';
  });

  // Total row — makes the sum obvious
  html += '<tr style="border-top:2px solid #f59e0b; background:#fef3c7;">';
  html += '<td colspan="4" style="padding:6px 10px; font-weight:700; color:#92400e; font-size:12px;">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' → ' + glCode + ' ' + (glGroup.gl_name || '') + '</td>';
  html += '<td style="padding:6px 10px; text-align:right; font-weight:700; font-variant-numeric:tabular-nums; color:#92400e; font-size:13px;">' + fmt(glGroup.total || 0) + '</td>';
  html += '</tr>';

  html += '</table></div></td>';
  detailRow.innerHTML = html;
  row.after(detailRow);
}

async function faToggleAccrualDrill(glCode, el) {
  const row = el.closest('tr');
  // Check if drill-down already open
  const existingDrill = row.nextElementSibling;
  if (existingDrill && existingDrill.classList.contains('fa-accrual-detail')) {
    existingDrill.remove();
    el.textContent = '▼';
    return;
  }
  el.textContent = '▲';

  // Fetch prior-year invoices
  try {
    const resp = await fetch('/api/accrual-invoices/' + entityCode + '/' + glCode);
    const data = await resp.json();

    if (!data.invoices || data.invoices.length === 0) {
      showToast('No prior-year invoices found for ' + glCode, 'info');
      el.textContent = '▼';
      return;
    }

    const drillRow = document.createElement('tr');
    drillRow.className = 'fa-accrual-detail';
    const nc = row.querySelectorAll('td').length;
    let html = '<td colspan="' + nc + '" style="padding:0; background:#fef3c7;">' +
      '<div style="padding:12px 16px;">' +
      '<div style="font-size:12px; font-weight:600; color:#92400e; margin-bottom:8px;">Prior-Year Invoices (before ' + (data.cutoff || '?') + ') — ' + data.invoices.length + ' invoice(s), Total: ' + fmt(data.total) + '</div>' +
      '<table style="width:100%; font-size:12px; border-collapse:collapse;">' +
      '<tr style="background:#fde68a; font-size:10px; text-transform:uppercase;"><th style="text-align:left; padding:4px 8px;">Payee</th><th style="padding:4px 8px;">Invoice #</th><th style="padding:4px 8px;">Invoice Date</th><th style="text-align:right; padding:4px 8px;">Amount</th></tr>';

    data.invoices.forEach(inv => {
      html += '<tr style="border-bottom:1px solid #fde68a;">' +
        '<td style="padding:4px 8px;">' + (inv.payee_name || inv.payee_code || '—') + '</td>' +
        '<td style="padding:4px 8px;">' + (inv.invoice_num || '—') + '</td>' +
        '<td style="padding:4px 8px;">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>' +
        '<td style="text-align:right; padding:4px 8px;">' + fmt(inv.amount) + '</td></tr>';
    });

    html += '<tr style="font-weight:700; background:#fde68a;"><td colspan="3" style="padding:4px 8px;">Accrual Adjustment (negative)</td>' +
      '<td style="text-align:right; padding:4px 8px; color:var(--red);">' + fmt(data.total) + '</td></tr>';
    html += '</table></div></td>';
    drillRow.innerHTML = html;
    row.after(drillRow);
  } catch(e) {
    showToast('Error loading accrual invoices: ' + e.message, 'error');
    el.textContent = '▼';
  }
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
    noData.innerHTML = '<td colspan="15" style="padding:12px 24px; background:#fef3c7; font-size:13px;">No expense data uploaded yet.</td>';
    row.after(noData);
    return;
  }

  const glGroup = data.gl_groups.find(g => g.gl_code === glCode);
  if (!glGroup || !glGroup.invoices || glGroup.invoices.length === 0) {
    const noInv = document.createElement('tr');
    noInv.className = 'fa-invoice-detail';
    noInv.innerHTML = '<td colspan="15" style="padding:12px 24px; background:var(--gray-50); font-size:13px; color:var(--gray-500);">No invoices for ' + glCode + '</td>';
    row.after(noInv);
    return;
  }

  // Build all GL codes for reclass dropdown — sorted alphabetically, across all sheets, with descriptions
  const allGLMap = {};
  Object.values(allSheets).forEach(sheetArr => sheetArr.forEach(l => { if (l.gl_code !== glCode) allGLMap[l.gl_code] = l.description || ''; }));
  const allGLs = Object.keys(allGLMap).sort();

  const detailRow = document.createElement('tr');
  detailRow.className = 'fa-invoice-detail';
  let html = '<td colspan="15" style="padding:0;"><div style="padding:12px 16px 12px 40px; background:linear-gradient(to right, #f0f4ff, #f8faff); border-left:3px solid var(--blue); border-bottom:1px solid var(--gray-200);">';
  html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">';
  html += '<span style="font-weight:600; font-size:13px; color:var(--blue);">' + glCode + ' — ' + (glGroup.gl_name || '') + '</span>';
  html += '<span style="font-size:12px; color:var(--gray-500);">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' · $' + Math.round(glGroup.total || 0).toLocaleString() + '</span>';
  html += '</div>';
  html += '<table style="width:100%; font-size:12px; border-collapse:collapse; background:white; border-radius:6px; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,0.05);">';
  html += '<thead><tr style="background:var(--gray-100); color:var(--gray-600); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
  html += '<td style="padding:6px 10px;">Payee</td><td style="padding:6px 10px;">Invoice #</td><td style="padding:6px 10px;">Date</td><td style="padding:6px 10px;">Check #</td><td style="padding:6px 10px; text-align:right;">Amount</td><td style="padding:6px 10px; text-align:right; width:180px;">Action</td></tr></thead>';

  glGroup.invoices.forEach(inv => {
    const isReclassed = !!inv.reclass_to_gl;
    html += '<tr style="border-top:1px solid var(--gray-200);' + (isReclassed ? ' opacity:0.5; text-decoration:line-through;' : '') + '">';
    html += '<td style="padding:6px 10px;">' + (inv.payee_name || inv.payee_code || '—') + '</td>';
    html += '<td style="padding:6px 10px; font-family:monospace; font-size:11px;">' + (inv.invoice_num || '—') + '</td>';
    html += '<td style="padding:6px 10px;">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>';
    html += '<td style="padding:6px 10px;">' + (inv.check_num || '—') + '</td>';
    html += '<td style="padding:6px 10px; text-align:right; font-variant-numeric:tabular-nums;">$' + Math.round(inv.amount).toLocaleString() + '</td>';
    html += '<td style="padding:6px 10px; text-align:right;">';
    if (isReclassed) {
      html += '<span style="font-size:11px; color:var(--orange);">→ ' + inv.reclass_to_gl + '</span> ';
      html += '<button onclick="faUndoReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:#fef3c7; color:#92400e; border:1px solid #fcd34d; border-radius:4px; cursor:pointer;">Undo</button>';
    } else {
      html += '<input id="fa_reclass_gl_' + inv.id + '" list="fa_reclass_list_' + inv.id + '" placeholder="Search GL..." style="font-size:11px; padding:2px 6px; border:1px solid var(--gray-300); border-radius:4px; width:200px;">';
      html += '<datalist id="fa_reclass_list_' + inv.id + '">';
      allGLs.forEach(g => { html += '<option value="' + g + '">' + g + ' - ' + (allGLMap[g] || '') + '</option>'; });
      html += '</datalist> ';
      html += '<button onclick="faInlineReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer;">Go</button>';
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

// ── FA Maint Proof toggle for Income lines ───────────────────────────
async function faToggleMaintProof(glCode, el) {
  const row = el.closest('tr');
  if (!row) return;
  const detailRow = row.nextElementSibling;

  // Collapse if already open
  if (detailRow && detailRow.classList && detailRow.classList.contains('fa-maint-detail')) {
    detailRow.remove();
    el.classList.remove('fa-drill-open');
    return;
  }

  // Map GL codes to charge codes
  const glToChargeCode = {
    '4010-0000': 'maint',
    '4130-0010': 'storage',
    '4130-0015': 'bike'
  };

  const chargeCode = glToChargeCode[glCode];

  // Create detail row
  const newRow = document.createElement('tr');
  newRow.classList.add('fa-maint-detail');
  newRow.innerHTML = '<td colspan="15" style="padding:0;">';

  if (!chargeCode) {
    newRow.innerHTML += '<div style="padding:12px 16px; background:#f3f4f6; color:var(--gray-500); font-size:12px;">No proof data for this income line</div>';
  } else {
    newRow.innerHTML += '<div style="padding:16px; background:#f3f4f6; min-height:100px;"><p style="font-size:12px; color:var(--gray-600); margin-bottom:8px;">Loading maint proof data...</p></div>';
  }

  newRow.innerHTML += '</td>';
  row.after(newRow);
  el.classList.add('fa-drill-open');

  if (!chargeCode) return;

  // Fetch maint proof data
  try {
    const proofResp = await fetch('/api/maint-proof/' + entityCode);
    if (!proofResp.ok) {
      newRow.querySelector('div').innerHTML = '<p style="font-size:12px; color:var(--red); padding:12px 16px;">Failed to load maint proof data</p>';
      return;
    }

    const proofData = await proofResp.json();
    const charge = proofData.charge_summary ? proofData.charge_summary[chargeCode] : null;

    if (!charge) {
      newRow.querySelector('div').innerHTML = '<p style="font-size:12px; color:var(--gray-500); padding:12px 16px;">No data for ' + chargeCode + '</p>';
      return;
    }

    // Fetch unit detail
    try {
      const unitsResp = await fetch('/api/maint-proof/' + entityCode + '/units?charge_code=' + chargeCode);
      const unitsData = unitsResp.ok ? await unitsResp.json() : {};
      const units = unitsData.units || [];

      // Build unit table
      let html = '<div style="padding:16px; background:#f3f4f6;">';
      html += '<div style="margin-bottom:12px; font-weight:600; font-size:13px; color:var(--gray-700);">' + chargeCode.toUpperCase() + ' Unit Proof</div>';
      html += '<table style="width:100%; border-collapse:collapse; font-size:12px; background:white; border-radius:6px; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,0.05);">';
      html += '<thead><tr style="background:var(--gray-100); color:var(--gray-600); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
      html += '<td style="padding:6px 10px;">Unit Code</td><td style="padding:6px 10px;">Shares</td><td style="padding:6px 10px;">Status</td><td style="padding:6px 10px; text-align:right;">Monthly</td><td style="padding:6px 10px; text-align:right;">Annual</td></tr></thead>';
      html += '<tbody>';

      units.forEach(u => {
        html += '<tr style="border-top:1px solid var(--gray-200);">';
        html += '<td style="padding:6px 10px;">' + (u.unit_code || '—') + '</td>';
        html += '<td style="padding:6px 10px;">' + (u.shares || '—') + '</td>';
        html += '<td style="padding:6px 10px;">' + (u.status || '—') + '</td>';
        html += '<td style="padding:6px 10px; text-align:right;">$' + (u.monthly ? Math.round(u.monthly).toLocaleString() : '—') + '</td>';
        html += '<td style="padding:6px 10px; text-align:right;">$' + (u.annual ? Math.round(u.annual).toLocaleString() : '—') + '</td>';
        html += '</tr>';
      });

      html += '</tbody></table>';
      html += '<div style="margin-top:10px; font-size:11px; color:var(--gray-500);">Total: ' + units.length + ' units · ' + (charge.count || '—') + ' records</div>';
      html += '</div>';

      newRow.querySelector('div').innerHTML = html;
    } catch(e) {
      newRow.querySelector('div').innerHTML = '<p style="font-size:12px; color:var(--red); padding:12px 16px;">Failed to load unit detail: ' + e.message + '</p>';
    }
  } catch(e) {
    newRow.querySelector('div').innerHTML = '<p style="font-size:12px; color:var(--red); padding:12px 16px;">Error: ' + e.message + '</p>';
  }
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
  faUpdateSheetTotals();
}

function renderEditableSheet(sheetName, sheetLines, contentDiv) {
  const NC = 15;
  const estLbl = estimateLabel();

  // ── Invoice reclass adjustments are now computed server-side ──
  // Lines with _reclass_ytd_adj already have adjusted ytd_actual from the API
  // Ensure _orig_ytd and _reclass_ytd_adj are available for tooltip display
  sheetLines.forEach(l => {
    if (!l._orig_ytd && l._reclass_ytd_adj) {
      l._orig_ytd = (l.ytd_actual || 0) - l._reclass_ytd_adj;
    }
    if (!l._reclass_ytd_adj) l._reclass_ytd_adj = 0;
    if (!l._orig_ytd) l._orig_ytd = l.ytd_actual || 0;
  });

  // Inject PM-style CSS if not already present
  if (!document.getElementById('faSheetStyle')) {
    const style = document.createElement('style');
    style.id = 'faSheetStyle';
    style.textContent = `
      .fa-grid { background:white; border-radius:12px; border:1px solid var(--gray-200); overflow:hidden; position:relative; }
      .fa-grid-scroll { overflow:auto; max-height:calc(100vh - 280px); }
      .fa-grid table { width:100%; border-collapse:collapse; font-size:13px; }
      .fa-grid thead { background:var(--gray-100); position:sticky; top:0; z-index:10; }
      .fa-grid th { padding:10px 12px; text-align:left; font-weight:600; border-bottom:2px solid var(--gray-300); white-space:nowrap; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); }
      .fa-grid th.num { text-align:right; }
      .fa-grid td { padding:8px 12px; border-bottom:1px solid var(--gray-200); }
      .fa-grid td.num { text-align:right; font-variant-numeric:tabular-nums; }
      .fa-grid tbody tr:hover { background:#eef2ff; }
      .fa-grid .cat-hdr td { background:var(--blue-light, #e1effe); font-weight:700; color:var(--blue, #1a56db); font-size:14px; padding:10px 12px; border-bottom:2px solid var(--blue, #1a56db); }
      .fa-grid .sub-row td { background:var(--gray-100); font-weight:700; border-top:2px solid var(--gray-300); }
      .fa-grid .total-row td { background:#1e3a5f; color:white; font-weight:700; font-size:14px; }
      .fa-grid .cell { width:90px; padding:5px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; text-align:right; background:#fffff0; cursor:text; }
      .fa-grid .cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px var(--blue-light, #e1effe); }
      .fa-grid .cell-fx { background:#f0fdf4; border-color:#bbf7d0; }
      .fa-grid .cell-fx:focus { background:#ecfdf5; }
      .fa-grid .cell-notes { text-align:left; min-width:100px; width:100%; }
      .fa-grid .cell-pct { width:60px; }
      .fa-fx { position:absolute; top:2px; right:2px; font-size:9px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:3px; padding:0 3px; cursor:pointer; user-select:none; z-index:5; }
      .fa-invoice-detail td { padding:0 !important; }
      .fa-invoice-detail:hover { background:transparent !important; }
      .fa-controls { display:flex; justify-content:space-between; align-items:center; padding:12px 16px; background:white; border-radius:12px; border:1px solid var(--gray-200); margin-bottom:12px; }
      .fa-legend { display:flex; gap:14px; font-size:11px; color:var(--gray-500); align-items:center; flex-wrap:wrap; }
      .fa-legend-dot { display:inline-block; width:10px; height:10px; border-radius:2px; vertical-align:middle; margin-right:3px; border:1px solid var(--gray-300); }
    `;
    document.head.appendChild(style);
  }

  let html = '<div class="fa-controls"><div class="fa-legend">' +
    '<span><span class="fa-legend-dot" style="background:#fffff0;"></span>Editable</span>' +
    '<span><span class="fa-legend-dot" style="background:#f0fdf4; border-color:#bbf7d0;"></span>Calculated (click to see formula)</span>' +
    '</div><div style="display:flex; gap:8px;"><button id="faZeroToggle" onclick="faToggleZeroRows()" style="font-size:11px; padding:4px 12px; background:var(--blue-light, #dbeafe); color:var(--blue); border:1px solid var(--blue); border-radius:4px; cursor:pointer;"></button></div></div>';

  // Formula bar — Excel-style with live preview + Accept/Cancel
  html += '<div id="faFormulaBarWrap" style="display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200); border-radius:8px; margin-bottom:12px;">' +
    '<span style="font-size:11px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>' +
    '<span id="faFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:var(--gray-600); white-space:nowrap; min-width:100px;"></span>' +
    '<input id="faFormulaBar" type="text" placeholder="Click a green formula cell to view its formula..." style="display:block; flex:1; padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:monospace; background:white;" oninput="formulaBarPreview()" onkeydown="formulaBarKeydown(event)">' +
    '<span id="faFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:var(--green); white-space:nowrap; min-width:80px; text-align:right;"></span>' +
    '<button id="faFormulaAccept" style="display:none; padding:4px 14px; font-size:12px; font-weight:600; background:var(--green); color:white; border:none; border-radius:4px; cursor:pointer;" onclick="formulaBarAccept()">Accept</button>' +
    '<button id="faFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:var(--gray-200); color:var(--gray-700); border:none; border-radius:4px; cursor:pointer;" onclick="formulaBarCancel()">Cancel</button>' +
    '<button id="faFormulaClear" style="display:none; padding:4px 10px; font-size:11px; background:#fef2f2; color:var(--red); border:1px solid #fecaca; border-radius:4px; cursor:pointer;" onclick="formulaBarClear()" title="Remove formula, revert to auto-calc">Clear</button>' +
    '</div>';

  // Maint proof tie-out panel for Income sheet
  if (sheetName === 'Income') {
    html += '<div id="faMaintTieoutPanel" style="padding:12px 16px; background:#f0f9ff; border:1px solid var(--blue-light, #bfdbfe); border-radius:8px; margin-bottom:12px;">' +
      '<div style="font-weight:600; font-size:13px; color:var(--blue); margin-bottom:8px;">Maint Proof Tie-Out</div>' +
      '<p style="font-size:12px; color:var(--gray-500); margin-bottom:8px;">Loading proof data...</p>' +
      '</div>';

    // Fetch and render maint proof tie-out
    (async () => {
      try {
        const proofResp = await fetch('/api/maint-proof/' + entityCode);
        const panel = document.getElementById('faMaintTieoutPanel');
        if (!panel) return; // DOM was replaced (tab switched)

        if (!proofResp.ok) {
          panel.innerHTML = '<p style="font-size:12px; color:var(--red);">Failed to load maint proof data</p>';
          return;
        }

        const proofData = await proofResp.json();
        const cs = proofData.charge_summary || {};

        if (!proofData.exists) {
          panel.innerHTML =
            '<div style="display:flex; align-items:center; gap:10px;">' +
            '<div style="font-weight:600; font-size:13px; color:var(--blue);">Maint Proof Tie-Out</div>' +
            '<span style="font-size:12px; color:var(--gray-500);">No maintenance proof uploaded yet. Upload via Data Collection to validate income lines.</span>' +
            '</div>';
          return;
        }

        const chargeMap = {
          'maint': {gl: '4010-0000', label: 'Maintenance'},
          'storage': {gl: '4130-0010', label: 'Storage Room'},
          'bike': {gl: '4130-0015', label: 'Bicycle Charge'}
        };

        let tieoutHtml = '<div style="font-weight:600; font-size:13px; color:var(--blue); margin-bottom:8px;">Maint Proof Tie-Out <span style="font-size:11px; font-weight:400; color:var(--gray-500);">(' + (proofData.report.total_units || 0) + ' units, ' + (proofData.report.total_shares || 0).toLocaleString() + ' shares)</span></div>';
        tieoutHtml += '<table style="width:100%; border-collapse:collapse; font-size:12px; background:white; border-radius:4px; overflow:hidden;">';
        tieoutHtml += '<thead><tr style="background:var(--blue-light, #bfdbfe); border-bottom:1px solid var(--blue, #1a56db);">';
        tieoutHtml += '<th style="text-align:left; padding:6px 10px; color:var(--blue); font-weight:600;">Charge Code</th>';
        tieoutHtml += '<th style="text-align:right; padding:6px 10px; color:var(--blue); font-weight:600;">Proof Monthly</th>';
        tieoutHtml += '<th style="text-align:right; padding:6px 10px; color:var(--blue); font-weight:600;">Proof Annual</th>';
        tieoutHtml += '<th style="text-align:right; padding:6px 10px; color:var(--blue); font-weight:600;">Budget Amount</th>';
        tieoutHtml += '<th style="text-align:right; padding:6px 10px; color:var(--blue); font-weight:600;">Variance</th>';
        tieoutHtml += '<th style="text-align:center; padding:6px 10px; color:var(--blue); font-weight:600;">Status</th>';
        tieoutHtml += '</tr></thead><tbody>';

        Object.keys(chargeMap).forEach(chargeCode => {
          const cfg = chargeMap[chargeCode];
          const charge = cs[chargeCode];
          const budgetLine = sheetLines.find(l => l.gl_code === cfg.gl);
          const proofAnnual = charge ? (charge.annual || 0) : 0;
          const budgetAmount = budgetLine ? (budgetLine.current_budget || 0) : 0;
          const variance = budgetAmount - proofAnnual;
          const varColor = Math.abs(variance) <= 1 ? 'var(--green)' : 'var(--red)';
          const status = Math.abs(variance) <= 1 ? '✓' : '⚠';
          const statusColor = Math.abs(variance) <= 1 ? 'var(--green)' : 'var(--orange)';

          tieoutHtml += '<tr style="border-top:1px solid var(--gray-200);">';
          tieoutHtml += '<td style="padding:6px 10px; color:var(--gray-700);">' + cfg.label + ' (' + cfg.gl + ')</td>';
          tieoutHtml += '<td style="padding:6px 10px; text-align:right; font-variant-numeric:tabular-nums;">$' + (charge ? Math.round(charge.monthly || 0).toLocaleString() : '0') + '</td>';
          tieoutHtml += '<td style="padding:6px 10px; text-align:right; font-variant-numeric:tabular-nums;">$' + Math.round(proofAnnual).toLocaleString() + '</td>';
          tieoutHtml += '<td style="padding:6px 10px; text-align:right; font-variant-numeric:tabular-nums;">$' + Math.round(budgetAmount).toLocaleString() + '</td>';
          tieoutHtml += '<td style="padding:6px 10px; text-align:right; font-variant-numeric:tabular-nums; color:' + varColor + ';">' + (variance >= 0 ? '+' : '') + '$' + Math.round(variance).toLocaleString() + '</td>';
          tieoutHtml += '<td style="padding:6px 10px; text-align:center; color:' + statusColor + '; font-weight:700;">' + status + '</td>';
          tieoutHtml += '</tr>';
        });

        tieoutHtml += '</tbody></table>';
        if (panel) panel.innerHTML = tieoutHtml;
      } catch(e) {
        const p = document.getElementById('faMaintTieoutPanel');
        if (p) p.innerHTML = '<p style="font-size:12px; color:var(--red);">Error loading tie-out: ' + e.message + '</p>';
      }
    })();
  }

  html += '<div class="fa-grid"><div class="fa-grid-scroll"><table><thead><tr>' +
    '<th>GL Code</th><th>Description</th><th>Notes</th>' +
    '<th class="num">YTD Actual</th>' +
    '<th class="num">Accrual Adj</th><th class="num">Unpaid Bills</th>' +
    '<th class="num">' + estLbl + ' Est</th><th class="num">12 Mo Forecast</th>' +
    '<th class="num">Curr Budget</th><th class="num">Inc %</th>' +
    '<th class="num">Proposed</th><th class="num">$ V. Fcst</th><th class="num">% Chg</th>' +
    '</tr></thead><tbody>';

  const catConfig = SHEET_CATEGORIES[sheetName];

  function buildLineRow(l) {
    const gl = l.gl_code;
    const ytd = l.ytd_actual || 0;
    const accrual = l.accrual_adj || 0;
    const unpaid = l.unpaid_bills || 0;
    const budget = l.current_budget || 0;
    const isZero = !ytd && !accrual && !unpaid && !budget && !(l.increase_pct);
    const estimate = faComputeEstimate(l);
    const forecast = faComputeForecast(l);
    // If there's a stored formula, evaluate it; otherwise use proposed_budget or auto-calc
    let proposed;
    const userFormula = l.proposed_formula || '';
    if (userFormula) {
      const evalResult = safeEvalFormula(userFormula);
      proposed = evalResult !== null ? evalResult : (l.proposed_budget || (forecast * (1 + (l.increase_pct || 0))));
    } else {
      proposed = l.proposed_budget || (forecast * (1 + (l.increase_pct || 0)));
    }
    const variance = proposed - forecast;
    const pctChange = forecast ? (proposed / forecast - 1) : 0;
    const incPct = ((l.increase_pct || 0) * 100).toFixed(1);
    const varColor = variance >= 0 ? 'var(--red)' : 'var(--green)';
    const reclassBadge = l.reclass_to_gl ? ' <span style="background:var(--orange-light); color:var(--orange); font-size:10px; padding:1px 5px; border-radius:8px;">R</span>' : '';

    // Build notes display: prepend PM reclass info if present
    let notesDisplay = l.notes || '';
    if (l.reclass_to_gl) {
      const reclassInfo = '[PM Reclass → ' + l.reclass_to_gl + (l.reclass_amount ? ' $' + Math.round(l.reclass_amount).toLocaleString() : '') + ']' + (l.reclass_notes ? ' ' + l.reclass_notes : '');
      notesDisplay = notesDisplay ? reclassInfo + ' | ' + notesDisplay : reclassInfo;
    }

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
    function fxCell(id, field, val, formula, isOverride, proposedFormula) {
      const hasUserFormula = field === 'proposed_budget' && proposedFormula;
      const overrideAttr = (isOverride || hasUserFormula) ? 'true' : 'false';
      let badge;
      if (hasUserFormula) {
        badge = '<span class="fa-fx" style="background:#dbeafe; color:var(--blue); border-color:var(--blue);">fx</span>';
      } else if (isOverride) {
        badge = '<span class="fa-fx" style="background:#fef3c7; color:#d97706; border-color:#d97706;">✎</span>';
      } else {
        badge = '<span class="fa-fx">fx</span>';
      }
      const pfAttr = proposedFormula ? ' data-proposed-formula="' + proposedFormula.replace(/"/g, '&quot;') + '"' : '';
      return '<td class="num" style="position:relative; cursor:pointer;" onclick="fxCellFocus(document.getElementById(\'' + id + '\'))">' + badge +
        '<input id="' + id + '" class="cell cell-fx" type="text" readonly' +
        ' value="' + fmt(val) + '"' +
        ' data-raw="' + Math.round(val) + '"' +
        ' data-formula="' + formula.replace(/"/g, '&quot;') + '"' +
        ' data-override="' + overrideAttr + '"' +
        pfAttr +
        ' data-gl="' + gl + '" data-field="' + field + '"' +
        ' onblur="fxCellBlur(this)"' +
        ' onkeydown="fxCellKeydown(this, event)"' +
        ' style="cursor:pointer; pointer-events:none;"></td>';
    }

    // Determine which toggle function to use based on sheet type
    const toggleFn = sheetName === 'Income' ? 'faToggleMaintProof' : 'faToggleInvoices';
    const toggleTitle = sheetName === 'Income' ? 'Click to view unit proof' : 'Click to view expenses';

    return '<tr data-gl="' + gl + '" class="' + (isZero ? 'zero-row' : '') + '"' + (isZero && !_faShowZeroRows ? ' style="display:none;"' : '') + '>' +
      '<td><span style="font-family:monospace; font-size:12px;">' + gl + '</span>' + reclassBadge + '</td>' +
      '<td style="font-size:12px;"><a href="#" onclick="' + toggleFn + '(\'' + gl + '\', this); return false;" style="color:inherit; text-decoration:none; cursor:pointer;" title="' + toggleTitle + '">' + l.description + ' <span class="fa-drill-arrow" style="font-size:10px; color:var(--gray-400);">▶</span></a></td>' +
      '<td><input class="cell cell-notes" type="text" value="' + notesDisplay.replace(/"/g,'&quot;') + '" data-gl="' + gl + '" data-field="notes" onchange="faAutoSave(\'' + gl + '\',\'notes\',this.value)"' + (l.reclass_to_gl ? ' style="background:#fef9e7; border-left:3px solid var(--orange);"' : '') + '></td>' +
      '<td class="num" style="position:relative;">' + $cell('ytd_'+gl, 'ytd_actual', ytd) +
        (l._reclass_ytd_adj ? '<span style="position:absolute; top:1px; right:2px; font-size:9px; color:var(--orange); background:var(--orange-light); padding:0 3px; border-radius:3px; border:1px solid var(--orange);" title="Original: ' + fmt(l._orig_ytd) + ' | Reclass adj: ' + (l._reclass_ytd_adj > 0 ? '+' : '') + fmt(l._reclass_ytd_adj) + '">R</span>' : '') + '</td>' +
      '<td class="num" style="position:relative;' + (accrual !== 0 ? 'cursor:pointer;' : '') + '"' + (accrual !== 0 ? ' onclick="faToggleAccrualDrill(\'' + gl + '\', this)" onmouseenter="this.style.background=\'#e1effe\'" onmouseleave="this.style.background=\'\'" title="Click to view prior-year invoices"' : '') + '>' + $cell('acc_'+gl, 'accrual_adj', accrual) +
        (accrual !== 0 ? '<span style="position:absolute; top:2px; right:2px; font-size:9px; color:var(--blue); background:var(--blue-light, #e1effe); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">▼</span>' : '') + '</td>' +
      '<td class="num" style="position:relative;' + (unpaid !== 0 ? 'cursor:pointer;' : '') + '"' + (unpaid !== 0 ? ' onclick="faToggleUnpaidDrill(\'' + gl + '\', this)" onmouseenter="this.style.background=\'#fef3c7\'" onmouseleave="this.style.background=\'\'" title="Click to view unpaid invoices"' : '') + '>' + $cell('unp_'+gl, 'unpaid_bills', unpaid) +
        (unpaid !== 0 ? '<span style="position:absolute; top:2px; right:2px; font-size:9px; color:#92400e; background:#fef3c7; padding:0 3px; border-radius:3px; border:1px solid #f59e0b;">▼</span>' : '') + '</td>' +
      fxCell('est_'+gl, 'estimate_override', estimate, estFormula, l.estimate_override !== null && l.estimate_override !== undefined) +
      fxCell('fcst_'+gl, 'forecast_override', forecast, fcstFormula, l.forecast_override !== null && l.forecast_override !== undefined) +
      '<td class="num">' + $cell('bud_'+gl, 'current_budget', budget) + '</td>' +
      '<td class="num"><input id="inc_'+gl+'" class="cell cell-pct" type="text" value="'+incPct+'%" data-raw="'+incPct+'" data-gl="'+gl+'" data-field="increase_pct" onfocus="this.value=this.dataset.raw" onblur="pctCellBlur(this)"></td>' +
      fxCell('prop_'+gl, 'proposed_budget', proposed, propFormula, false, userFormula) +
      '<td class="num" id="var_'+gl+'" style="color:'+varColor+';">' + fmt(variance) + '</td>' +
      '<td class="num" id="pct_'+gl+'">' + (pctChange*100).toFixed(1) + '%</td></tr>';
  }

  function sumLines(lines) {
    const t = {ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
    lines.forEach(l => {
      t.ytd += l.ytd_actual || 0;
      t.accrual += l.accrual_adj || 0;
      t.unpaid += l.unpaid_bills || 0;
      t.estimate += faComputeEstimate(l);
      t.forecast += faComputeForecast(l);
      t.budget += l.current_budget || 0;
      t.proposed += l.proposed_budget || (faComputeForecast(l) * (1 + (l.increase_pct || 0)));
    });
    return t;
  }

  function subtotalRow(label, t, cls, rowId) {
    const v = t.proposed - t.forecast;
    const p = t.forecast ? (t.proposed/t.forecast-1) : 0;
    const rid = rowId ? ' id="' + rowId + '"' : '';
    return '<tr class="' + (cls||'sub-row') + '"' + rid + '>' +
      '<td colspan="3">' + label + '</td>' +
      '<td class="num">' + fmt(t.ytd) + '</td>' +
      '<td class="num">' + fmt(t.accrual || 0) + '</td>' +
      '<td class="num">' + fmt(t.unpaid || 0) + '</td>' +
      '<td class="num">' + fmt(t.estimate) + '</td>' +
      '<td class="num">' + fmt(t.forecast) + '</td>' +
      '<td class="num">' + fmt(t.budget) + '</td>' +
      '<td class="num"></td>' +
      '<td class="num">' + fmt(t.proposed) + '</td>' +
      '<td class="num" style="color:' + (v>=0?'var(--red)':'var(--green)') + ';">' + fmt(v) + '</td>' +
      '<td class="num">' + (p*100).toFixed(1) + '%</td></tr>';
  }

  // Track category group -> GL codes for dynamic subtotal updates
  const _catGroupGLs = {};

  if (catConfig) {
    catConfig.groups.forEach(grp => {
      const gl = sheetLines.filter(grp.match);
      if (gl.length === 0) return;
      _catGroupGLs[grp.key] = gl.map(l => l.gl_code);
      html += '<tr class="cat-hdr"><td colspan="' + NC + '">' + grp.label + '</td></tr>';
      gl.forEach(l => { html += buildLineRow(l); });
      html += subtotalRow('Total ' + grp.label, sumLines(gl), 'sub-row', 'subtotal_' + grp.key);
    });
    const allGrouped = catConfig.groups.flatMap(g => sheetLines.filter(g.match));
    const ungrouped = sheetLines.filter(l => !allGrouped.includes(l));
    if (ungrouped.length > 0) {
      _catGroupGLs['_other'] = ungrouped.map(l => l.gl_code);
      html += '<tr class="cat-hdr"><td colspan="' + NC + '" style="color:var(--gray-500); border-color:var(--gray-300);">Other</td></tr>';
      ungrouped.forEach(l => { html += buildLineRow(l); });
      html += subtotalRow('Total Other', sumLines(ungrouped), 'sub-row', 'subtotal__other');
    }
  } else {
    sheetLines.forEach(l => { html += buildLineRow(l); });
  }

  // Store group mapping globally so faUpdateSheetTotals can use it
  window._catGroupGLs = _catGroupGLs;

  html += subtotalRow('Sheet Total', sumLines(sheetLines), 'total-row', 'faSheetTotal');
  html += '</tbody></table></div>';
  html += '</div>';
  contentDiv.innerHTML = html;

  // Recalculate totals from rounded cell values so they match exactly
  setTimeout(() => {
    faUpdateSheetTotals();
  }, 100);
}

function computeForecast(l) {
  const ytdActual = l.ytd_actual || 0;
  const accrualAdj = l.accrual_adj || 0;
  const unpaidBills = l.unpaid_bills || 0;
  const estimate = (ytdActual > 0 && YTD_MONTHS > 0) ? (ytdActual / YTD_MONTHS) * REMAINING_MONTHS : 0;
  return ytdActual + accrualAdj + unpaidBills + estimate;
}

async function sendToPM() {
  if (!confirm('Send to PM for expense review?')) return;
  await fetch('/api/budgets/' + entityCode + '/status', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({status: 'pm_pending'})
  });
  showToast('Sent to PM for review', 'success');
  loadDetail();
}

async function approvePM() {
  if (!confirm('Approve PM review?')) return;
  await fetch('/api/budgets/' + entityCode + '/status', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({status: 'approved'})
  });
  showToast('Budget approved!', 'success');
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
  showToast('Budget returned to PM', 'info');
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
    --blue-dark: #1e429f;
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
    <a href="/generate" class="nav-link">Generator</a>
    <a href="/audited-financials" class="nav-link">Audited Financials</a>
    <a href="/files" class="nav-link">Files</a>
  </div>
</nav>

<header>
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
  return building ? (building.building_name || building.name || entityCode) : entityCode;
}

function renderBuildings(userId) {
  const grid = document.getElementById('buildings-grid');
  const userAssignments = allAssignments.filter(a => a.user_id === userId && a.role === 'pm');

  // If PM has assignments, show those buildings; otherwise show all budgets (demo mode)
  let buildingList = [];
  if (userAssignments.length > 0) {
    buildingList = userAssignments.map(a => {
      const budget = allBudgets.find(b => b.entity_code === a.entity_code);
      return { entity_code: a.entity_code, budget };
    });
  } else {
    // Demo mode: show all budgets
    buildingList = allBudgets.map(b => ({ entity_code: b.entity_code, budget: b }));
  }

  if (buildingList.length === 0) {
    grid.style.display = 'none';
    return;
  }

  grid.innerHTML = '';
  grid.style.display = 'grid';

  buildingList.forEach(item => {
    const buildingName = item.budget ? (item.budget.building_name || getBuildingName(item.entity_code)) : getBuildingName(item.entity_code);
    const budgetStatus = item.budget ? item.budget.status : null;
    const isEditable = editableStatuses.includes(budgetStatus);

    const card = document.createElement('div');
    card.className = 'building-card';
    if (isEditable) {
      card.style.cursor = 'pointer';
      card.onclick = () => window.location.href = `/pm/${item.entity_code}`;
    } else {
      card.style.opacity = '0.6';
      card.style.cursor = 'default';
    }

    const statusLabel = budgetStatus ? (statusLabels[budgetStatus] || budgetStatus) : 'No Budget';
    const pillClass = budgetStatus ? 'pill-' + budgetStatus : 'pill-draft';
    card.innerHTML = `
      <h3>${buildingName}</h3>
      <p style="margin-bottom:8px;"><span style="font-family:monospace; font-size:12px; color:var(--gray-500);">Entity ${item.entity_code}</span></p>
      <span class="status-pill ${pillClass}">${statusLabel}</span>
      ${isEditable ? '<p style="color: var(--blue); font-size: 12px; margin-top:8px; font-weight:600;">Click to edit &rarr;</p>' : ''}
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

PM_EDIT_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PM Edit — {{ building_name }} — Century Management</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-dark: #1e429f;
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
  /* ── Global Nav ── */
  .top-nav { background: white; border-bottom: 1px solid var(--gray-200); padding: 0 20px; display: flex; align-items: center; height: 48px; position: sticky; top: 0; z-index: 100; }
  .top-nav .nav-brand { font-weight: 700; font-size: 15px; color: var(--blue); text-decoration: none; margin-right: 32px; }
  .top-nav .nav-links { display: flex; gap: 4px; }
  .top-nav .nav-link { padding: 6px 14px; font-size: 13px; font-weight: 500; color: var(--gray-500); text-decoration: none; border-radius: 6px; transition: all 0.15s; }
  .top-nav .nav-link:hover { background: var(--gray-100); color: var(--gray-900); }
  .top-nav .nav-link.active { background: var(--blue-light); color: var(--blue); }
  /* ── Toast ── */
  .toast-container { position: fixed; top: 60px; right: 20px; z-index: 9999; display: flex; flex-direction: column; gap: 8px; }
  .toast { padding: 12px 20px; border-radius: 8px; font-size: 14px; font-weight: 500; box-shadow: 0 4px 12px rgba(0,0,0,0.15); animation: slideIn 0.3s ease; max-width: 360px; }
  .toast-success { background: var(--green); color: white; }
  .toast-error { background: var(--red); color: white; }
  .toast-info { background: var(--blue); color: white; }
  @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }

  header {
    background: linear-gradient(135deg, var(--blue) 0%, var(--blue-dark) 100%);
    color: white;
    padding: 24px 20px;
  }
  header h1 { font-size: 24px; margin-bottom: 4px; }
  header p { opacity: 0.9; font-size: 14px; }
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

  .invoice-detail-row td { padding: 0 !important; }
  .invoice-detail-row:hover { background: transparent !important; }
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
    <a href="/generate" class="nav-link">Generator</a>
    <a href="/audited-financials" class="nav-link">Audited Financials</a>
    <a href="/files" class="nav-link">Files</a>
  </div>
</nav>

<!-- Toast container -->
<div class="toast-container" id="toastContainer"></div>

<header>
  <h1>{{ building_name }}</h1>
  <p>Entity {{ entity_code }} — Repairs & Supplies Budget Review</p>
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
    <div style="display:flex; gap:12px; align-items:center;">
      <button id="zeroToggle" onclick="toggleZeroRows()" class="btn" style="background:var(--gray-200); color:var(--gray-600); font-size:12px; padding:6px 14px; border:1px solid var(--gray-300); border-radius:6px; cursor:pointer;"></button>
      <a href="/pm/{{ entity_code }}/expenses" class="btn" style="background:var(--gray-500); text-decoration:none;">View Expense Report</a>
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
            <th>Notes</th>
            <th class="number">YTD<br>Actual</th>
            <th class="number">Accrual<br>Adj</th>
            <th class="number">Unpaid<br>Bills</th>
            <th class="number">{{ estimate_label }}<br>Estimate <span style="font-size:9px; color:var(--blue); background:var(--blue-light, #e1effe); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">fx</span></th>
            <th class="number">12 Month<br>Forecast <span style="font-size:9px; color:var(--blue); background:var(--blue-light, #e1effe); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">fx</span></th>
            <th class="number">Current<br>Budget</th>
            <th class="number">Increase<br>%</th>
            <th class="number">Proposed<br>Budget <span style="font-size:9px; color:var(--blue); background:var(--blue-light, #e1effe); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">fx</span></th>
            <th class="number">$ V.<br>Forecast</th>
            <th class="number">%<br>Change</th>
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
const YTD_MONTHS = {{ ytd_months }};
const REMAINING_MONTHS = {{ remaining_months }};

let saveTimer = null;
const indicator = document.getElementById('saveIndicator');

function showToast(msg, type='info') {
  const c = document.getElementById('toastContainer');
  if (!c) return;
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity 0.3s'; setTimeout(() => t.remove(), 300); }, 3000);
}

function fmt(n) {
    if (n == null || isNaN(n)) return '$0';
    return '$' + Math.round(n).toLocaleString();
}

function pctFmt(n) {
    if (n == null || isNaN(n)) return '0.0%';
    return (n * 100).toFixed(1) + '%';
}

function computeEstimate(line) {
    const ytd = line.ytd_actual || 0;
    if (ytd > 0 && YTD_MONTHS > 0) {
        return (ytd / YTD_MONTHS) * REMAINING_MONTHS;
    }
    return 0;
}

function computeForecast(line) {
    const ytd = line.ytd_actual || 0;
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    return ytd + accrual + unpaid + computeEstimate(line);
}

function computeProposed(line) {
    const forecast = computeForecast(line);
    return forecast * (1 + (line.increase_pct || 0));
}

function renderTable() {
    const tbody = document.getElementById('linesBody');
    tbody.innerHTML = '';

    // Group by category: Supplies, Repairs, Maintenance
    const categories = {supplies: [], repairs: [], maintenance: []};
    const catLabels = {supplies: 'Supplies', repairs: 'Repairs', maintenance: 'Maintenance Contracts'};
    LINES.forEach(l => {
        if (categories[l.category]) categories[l.category].push(l);
    });

    let grandTotals = {ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
    const NC = 13;

    for (const [cat, catLines] of Object.entries(categories)) {
        if (catLines.length === 0) continue;

        const headerRow = document.createElement('tr');
        headerRow.className = 'category-header';
        headerRow.innerHTML = '<td colspan="' + NC + '">' + catLabels[cat] + '</td>';
        tbody.appendChild(headerRow);

        let catTotals = {ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};

        catLines.forEach(line => {
            const estimate = computeEstimate(line);
            const forecast = computeForecast(line);
            const proposed = computeProposed(line);
            const variance = proposed - forecast;
            const pctChange = forecast ? (proposed / forecast - 1) : 0;

            catTotals.ytd += (line.ytd_actual || 0);
            catTotals.accrual += (line.accrual_adj || 0);
            catTotals.unpaid += (line.unpaid_bills || 0);
            catTotals.estimate += estimate;
            catTotals.forecast += forecast;
            catTotals.budget += (line.current_budget || 0);
            catTotals.proposed += proposed;

            const reclassBadge = line.reclass_to_gl ? ' <span style="background:var(--orange-light); color:var(--orange); font-size:10px; padding:1px 5px; border-radius:8px;">Reclass</span>' : '';

            const isZero = !(line.ytd_actual || line.accrual_adj || line.unpaid_bills || line.current_budget || (line.increase_pct && line.increase_pct !== 0));
            const tr = document.createElement('tr');
            if (isZero) { tr.classList.add('zero-row'); if (!_showZeroRows) tr.style.display = 'none'; }
            tr.dataset.gl = line.gl_code;
            tr.innerHTML = `
                <td><a href="#" onclick="toggleInvoices('${line.gl_code}', this); return false;" style="color:var(--blue); text-decoration:none; font-family:monospace;" title="Click to view invoices">${line.gl_code}</a>${reclassBadge}</td>
                <td><a href="#" onclick="toggleInvoices('${line.gl_code}', this); return false;" style="color:inherit; text-decoration:none; cursor:pointer;" title="Click to view expenses">${line.description} <span class="drill-arrow" style="font-size:10px; color:var(--gray-400); transition:transform 0.2s;">▶</span></a></td>
                <td><input type="text" value="${(line.notes || '').replace(/"/g, '&quot;')}" data-gl="${line.gl_code}" data-field="notes" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'} style="min-width:100px;"></td>
                <td class="number" style="position:relative;">${fmt(line.ytd_actual)}${line._reclass_ytd_adj ? '<span style="position:absolute; top:1px; right:2px; font-size:9px; color:var(--orange,#d97706); background:var(--orange-light,#fef3c7); padding:0 3px; border-radius:3px; border:1px solid var(--orange,#d97706);" title="Original: ' + fmt(line._orig_ytd) + ' | Reclass adj: ' + (line._reclass_ytd_adj > 0 ? '+' : '') + fmt(line._reclass_ytd_adj) + '">R</span>' : ''}</td>
                <td class="number" style="position:relative;"><input type="number" step="1" value="${Math.round(line.accrual_adj || 0)}" data-gl="${line.gl_code}" data-field="accrual_adj" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'}>${(line.accrual_adj || 0) !== 0 ? '<span onclick="pmToggleAccrualDrill(\'' + line.gl_code + '\', this)" style="position:absolute; top:2px; right:2px; font-size:9px; color:#92400e; cursor:pointer; background:#fef3c7; padding:0 3px; border-radius:3px; border:1px solid #fde68a;" title="View prior-year invoices">▼</span>' : ''}</td>
                <td class="number" style="position:relative;"><input type="number" step="1" value="${Math.round(line.unpaid_bills || 0)}" data-gl="${line.gl_code}" data-field="unpaid_bills" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'}>${(line.unpaid_bills || 0) !== 0 ? '<span onclick="pmToggleUnpaidDrill(\'' + line.gl_code + '\', this)" style="position:absolute; top:2px; right:2px; font-size:9px; color:#92400e; cursor:pointer; background:#fef3c7; padding:0 3px; border-radius:3px; border:1px solid #f59e0b;" title="View unpaid invoices">▼</span>' : ''}</td>
                <td class="number" id="est_${line.gl_code}" style="cursor:pointer; position:relative;" onclick="showPmFormula(this, 'est', '${line.gl_code}')" title="Click to see formula">${fmt(estimate)}</td>
                <td class="number" id="fc_${line.gl_code}" style="cursor:pointer; position:relative;" onclick="showPmFormula(this, 'fc', '${line.gl_code}')" title="Click to see formula">${fmt(forecast)}</td>
                <td class="number">${fmt(line.current_budget)}</td>
                <td class="number"><input type="number" step="0.1" value="${((line.increase_pct || 0) * 100).toFixed(1)}" data-gl="${line.gl_code}" data-field="increase_pct" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'} style="width:70px;"></td>
                <td class="number" id="pb_${line.gl_code}" style="cursor:pointer; position:relative;" onclick="showPmFormula(this, 'pb', '${line.gl_code}')" title="Click to see formula">${fmt(proposed)}</td>
                <td class="number" id="var_${line.gl_code}" style="color:${variance >= 0 ? 'var(--red)' : 'var(--green)'};">${fmt(variance)}</td>
                <td class="number" id="pct_${line.gl_code}">${(pctChange * 100).toFixed(1)}%</td>
            `;
            tbody.appendChild(tr);
        });

        // Subtotal
        const catVar = catTotals.proposed - catTotals.forecast;
        const subRow = document.createElement('tr');
        subRow.className = 'subtotal-row';
        subRow.innerHTML = `
            <td></td><td>Total ${catLabels[cat]}</td><td></td>
            <td class="number">${fmt(catTotals.ytd)}</td>
            <td class="number"><input type="number" step="1" value="${Math.round(catTotals.accrual || 0)}" disabled></td>
            <td class="number"><input type="number" step="1" value="${Math.round(catTotals.unpaid || 0)}" disabled></td>
            <td class="number">${fmt(catTotals.estimate)}</td>
            <td class="number">${fmt(catTotals.forecast)}</td>
            <td class="number">${fmt(catTotals.budget)}</td>
            <td></td>
            <td class="number">${fmt(catTotals.proposed)}</td>
            <td class="number">${fmt(catVar)}</td>
            <td></td>
        `;
        tbody.appendChild(subRow);

        Object.keys(grandTotals).forEach(k => { if (k in catTotals) grandTotals[k] += catTotals[k]; });
    }

    // Grand total
    const grandVar = grandTotals.proposed - grandTotals.forecast;
    const grandPct = grandTotals.forecast ? (grandTotals.proposed / grandTotals.forecast - 1) : 0;
    const grandRow = document.createElement('tr');
    grandRow.className = 'grand-total';
    grandRow.innerHTML = `
        <td></td><td>GRAND TOTAL R&M</td><td></td>
        <td class="number">${fmt(grandTotals.ytd)}</td>
        <td class="number"><input type="number" step="1" value="${Math.round(grandTotals.accrual || 0)}" disabled></td>
        <td class="number"><input type="number" step="1" value="${Math.round(grandTotals.unpaid || 0)}" disabled></td>
        <td class="number">${fmt(grandTotals.estimate)}</td>
        <td class="number">${fmt(grandTotals.forecast)}</td>
        <td class="number">${fmt(grandTotals.budget)}</td>
        <td></td>
        <td class="number">${fmt(grandTotals.proposed)}</td>
        <td class="number">${fmt(grandVar)}</td>
        <td class="number">${(grandPct * 100).toFixed(1)}%</td>
    `;
    tbody.appendChild(grandRow);
}

// ── Zero-row toggle ──────────────────────────────────────────────────
let _showZeroRows = false;

function countZeroRows() {
    return document.querySelectorAll('#linesBody .zero-row').length;
}

function updateZeroToggle() {
    const btn = document.getElementById('zeroToggle');
    if (!btn) return;
    const count = countZeroRows();
    if (count === 0) { btn.style.display = 'none'; return; }
    btn.style.display = '';
    btn.textContent = _showZeroRows ? 'Hide ' + count + ' Zero Rows' : 'Show ' + count + ' Hidden Zero Rows';
    btn.style.background = _showZeroRows ? 'var(--gray-200)' : 'var(--blue-light, #e1effe)';
    btn.style.color = _showZeroRows ? 'var(--gray-600)' : 'var(--blue)';
    btn.style.borderColor = _showZeroRows ? 'var(--gray-300)' : 'var(--blue)';
}

function toggleZeroRows() {
    _showZeroRows = !_showZeroRows;
    document.querySelectorAll('#linesBody .zero-row').forEach(row => {
        row.style.display = _showZeroRows ? '' : 'none';
    });
    updateZeroToggle();
}

// ── PM Formula popover ───────────────────────────────────────────────
let _activePmPopover = null;

function showPmFormula(cell, type, gl) {
    // Close any existing popover
    if (_activePmPopover) { _activePmPopover.remove(); _activePmPopover = null; }

    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    const ytd = line.ytd_actual || 0;
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    const base = ytd + accrual + unpaid;
    const est = computeEstimate(line);
    const fc = computeForecast(line);
    const incPct = ((line.increase_pct || 0) * 100).toFixed(1);

    let formula = '';
    if (type === 'est') {
        formula = `YTD ${fmt(ytd)} / ${YTD_MONTHS} mo × ${REMAINING_MONTHS} mo = ${fmt(est)}`;
    } else if (type === 'fc') {
        formula = `YTD ${fmt(ytd)} + Adj ${fmt(accrual)} + Unpaid ${fmt(unpaid)} + Est ${fmt(est)} = ${fmt(fc)}`;
    } else if (type === 'pb') {
        formula = `Forecast ${fmt(fc)} × (1 + ${incPct}%) = ${fmt(computeProposed(line))}`;
    }

    const pop = document.createElement('div');
    pop.style.cssText = 'position:absolute; bottom:100%; left:0; right:0; background:white; border:1px solid var(--blue); border-radius:6px; padding:8px 12px; font-size:11px; color:var(--gray-700); box-shadow:0 4px 12px rgba(0,0,0,0.15); z-index:50; white-space:nowrap; min-width:220px;';
    pop.innerHTML = '<div style="font-weight:600; color:var(--blue); margin-bottom:2px; font-size:10px; text-transform:uppercase;">Formula</div>' + formula;
    cell.appendChild(pop);
    _activePmPopover = pop;

    // Close on click outside
    setTimeout(() => {
        document.addEventListener('click', function closePop(e) {
            if (!pop.contains(e.target) && e.target !== cell) {
                pop.remove();
                _activePmPopover = null;
                document.removeEventListener('click', closePop);
            }
        });
    }, 10);
}

// Expense distribution drill-down
let _expenseCache = null;

async function fetchExpenseData() {
    if (_expenseCache !== null) return _expenseCache;
    try {
        const res = await fetch('/api/expense-dist/' + ENTITY);
        if (!res.ok) { _expenseCache = false; return null; }
        _expenseCache = await res.json();
        return _expenseCache;
    } catch(e) { _expenseCache = false; return null; }
}

// ── PM Open AP (Unpaid Bills) drill-down ────────────────────────────
let _pmOpenAPCache = null;

async function pmFetchOpenAPData() {
    if (_pmOpenAPCache !== null) return _pmOpenAPCache;
    try {
        const res = await fetch('/api/open-ap/' + ENTITY);
        if (!res.ok) { _pmOpenAPCache = false; return null; }
        _pmOpenAPCache = await res.json();
        return _pmOpenAPCache;
    } catch(e) { _pmOpenAPCache = false; return null; }
}

async function pmToggleUnpaidDrill(glCode, linkEl) {
    const row = linkEl.closest('tr');
    const next = row.nextElementSibling;
    if (next && next.classList.contains('pm-unpaid-detail')) {
        next.remove();
        return;
    }

    const data = await pmFetchOpenAPData();
    if (!data || !data.gl_groups) {
        const noData = document.createElement('tr');
        noData.className = 'pm-unpaid-detail';
        noData.innerHTML = '<td colspan="15" style="padding:12px 24px; background:#fef3c7; font-size:13px;">No Open AP data available. Upload the AP Aging report with the budget generator.</td>';
        row.after(noData);
        return;
    }

    const glGroup = data.gl_groups.find(g => g.gl_code === glCode);
    if (!glGroup || !glGroup.invoices || glGroup.invoices.length === 0) {
        const noInv = document.createElement('tr');
        noInv.className = 'pm-unpaid-detail';
        noInv.innerHTML = '<td colspan="15" style="padding:12px 24px; background:var(--gray-50); font-size:13px; color:var(--gray-500);">No unpaid invoices for ' + glCode + '</td>';
        row.after(noInv);
        return;
    }

    const detailRow = document.createElement('tr');
    detailRow.className = 'pm-unpaid-detail';
    let html = '<td colspan="15" style="padding:0;"><div style="padding:10px 16px 10px 40px; background:linear-gradient(to right, #fef3c7, #fffbeb); border-left:3px solid #f59e0b; border-bottom:1px solid var(--gray-200);">';
    html += '<table style="width:100%; font-size:12px; border-collapse:collapse; background:white; border-radius:6px; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,0.05);">';
    html += '<thead><tr style="background:#fef3c7; color:#92400e; font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
    html += '<td style="padding:6px 10px;">Vendor</td><td style="padding:6px 10px;">Invoice #</td><td style="padding:6px 10px;">Date</td><td style="padding:6px 10px;">Description</td><td style="padding:6px 10px; text-align:right;">Amount</td></tr></thead>';

    glGroup.invoices.forEach(inv => {
        html += '<tr style="border-top:1px solid var(--gray-100);">';
        html += '<td style="padding:5px 10px;">' + (inv.payee_name || inv.payee_code || '—') + '</td>';
        html += '<td style="padding:5px 10px; font-family:monospace; font-size:11px;">' + (inv.invoice_num || '—') + '</td>';
        html += '<td style="padding:5px 10px; white-space:nowrap;">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>';
        html += '<td style="padding:5px 10px; max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="' + ((inv.invoice_notes || inv.notes || '').replace(/"/g,'&quot;')) + '">' + (inv.invoice_notes || inv.notes || '—') + '</td>';
        html += '<td style="padding:5px 10px; text-align:right; font-variant-numeric:tabular-nums;">' + fmt(inv.current_owed) + '</td>';
        html += '</tr>';
    });

    // Total row
    html += '<tr style="border-top:2px solid #f59e0b; background:#fef3c7;">';
    html += '<td colspan="4" style="padding:6px 10px; font-weight:700; color:#92400e; font-size:12px;">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' → ' + glCode + ' ' + (glGroup.gl_name || '') + '</td>';
    html += '<td style="padding:6px 10px; text-align:right; font-weight:700; font-variant-numeric:tabular-nums; color:#92400e; font-size:13px;">' + fmt(glGroup.total || 0) + '</td>';
    html += '</tr>';

    html += '</table></div></td>';
    detailRow.innerHTML = html;
    row.after(detailRow);
}

// ── Apply invoice-level reclass adjustments to LINES YTD ──
// Scans reclassed invoices in expense data and adjusts ytd_actual on LINES
async function applyExpenseReclassAdjustments() {
    const data = await fetchExpenseData();
    if (!data || !data.gl_groups) return;

    // Restore original YTD values first (avoid double-counting on re-render)
    LINES.forEach(l => {
        if (l._orig_ytd !== undefined) l.ytd_actual = l._orig_ytd;
        l._orig_ytd = l.ytd_actual || 0;
    });

    // Compute net adjustment per GL from reclassed invoices
    const adj = {};  // {gl_code: net_amount_change}
    data.gl_groups.forEach(g => {
        if (!g.invoices) return;
        g.invoices.forEach(inv => {
            if (inv.reclass_to_gl) {
                // Source GL loses this amount
                adj[g.gl_code] = (adj[g.gl_code] || 0) - inv.amount;
                // Target GL gains this amount
                adj[inv.reclass_to_gl] = (adj[inv.reclass_to_gl] || 0) + inv.amount;
            }
        });
    });

    // Apply adjustments to LINES
    LINES.forEach(l => {
        const a = adj[l.gl_code] || 0;
        if (a) {
            l.ytd_actual = (l.ytd_actual || 0) + a;
            l._reclass_ytd_adj = a;
        } else {
            l._reclass_ytd_adj = 0;
        }
    });
}

async function pmToggleAccrualDrill(glCode, el) {
  const row = el.closest('tr');
  const existingDrill = row.nextElementSibling;
  if (existingDrill && existingDrill.classList.contains('pm-accrual-detail')) {
    existingDrill.remove();
    el.textContent = '▼';
    return;
  }
  el.textContent = '▲';
  try {
    const resp = await fetch('/api/accrual-invoices/' + ENTITY + '/' + glCode);
    const data = await resp.json();
    if (!data.invoices || data.invoices.length === 0) {
      el.textContent = '▼';
      return;
    }
    const drillRow = document.createElement('tr');
    drillRow.className = 'pm-accrual-detail';
    const nc = row.querySelectorAll('td').length;
    let html = '<td colspan="' + nc + '" style="padding:0; background:#fef3c7;"><div style="padding:10px 14px;">' +
      '<div style="font-size:11px; font-weight:600; color:#92400e; margin-bottom:6px;">Prior-Year Invoices (before ' + (data.cutoff || '?') + ') — Total: ' + fmt(data.total) + '</div>' +
      '<table style="width:100%; font-size:11px; border-collapse:collapse;">' +
      '<tr style="background:#fde68a; font-size:10px;"><th style="text-align:left; padding:3px 6px;">Payee</th><th style="padding:3px 6px;">Invoice #</th><th style="padding:3px 6px;">Date</th><th style="text-align:right; padding:3px 6px;">Amount</th></tr>';
    data.invoices.forEach(inv => {
      html += '<tr style="border-bottom:1px solid #fde68a;"><td style="padding:3px 6px;">' + (inv.payee_name || '—') + '</td>' +
        '<td style="padding:3px 6px;">' + (inv.invoice_num || '—') + '</td>' +
        '<td style="padding:3px 6px;">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>' +
        '<td style="text-align:right; padding:3px 6px;">' + fmt(inv.amount) + '</td></tr>';
    });
    html += '</table></div></td>';
    drillRow.innerHTML = html;
    row.after(drillRow);
  } catch(e) { el.textContent = '▼'; }
}

async function toggleInvoices(glCode, linkEl) {
    const row = linkEl.closest('tr');
    const existingDetail = row.nextElementSibling;
    if (existingDetail && existingDetail.classList.contains('invoice-detail-row')) {
        existingDetail.remove();
        // Reset arrow indicators
        row.querySelectorAll('.drill-arrow').forEach(a => a.textContent = '▶');
        return;
    }

    // Set arrow to expanded
    row.querySelectorAll('.drill-arrow').forEach(a => a.textContent = '▼');

    const data = await fetchExpenseData();
    if (!data || !data.gl_groups) {
        const noData = document.createElement('tr');
        noData.className = 'invoice-detail-row';
        noData.innerHTML = '<td colspan="15" style="padding:12px 24px; background:#fef3c7; font-size:13px;">No expense distribution data uploaded yet. <a href="/pm/' + ENTITY + '/expenses" style="color:var(--blue);">Upload here</a></td>';
        row.after(noData);
        return;
    }

    const glGroup = data.gl_groups.find(g => g.gl_code === glCode);
    if (!glGroup || !glGroup.invoices || glGroup.invoices.length === 0) {
        const noInv = document.createElement('tr');
        noInv.className = 'invoice-detail-row';
        noInv.innerHTML = '<td colspan="15" style="padding:12px 24px; background:var(--gray-50); font-size:13px; color:var(--gray-500);">No invoices found for ' + glCode + '</td>';
        row.after(noInv);
        return;
    }

    // Build all GL codes for reclass dropdown — sorted alphabetically, with descriptions
    const allGLMap = {};
    LINES.forEach(l => { if (l.gl_code !== glCode) allGLMap[l.gl_code] = l.description || ''; });
    const allGLs = Object.keys(allGLMap).sort();

    const detailRow = document.createElement('tr');
    detailRow.className = 'invoice-detail-row';
    let html = '<td colspan="15" style="padding:0;"><div style="padding:12px 16px 12px 40px; background:linear-gradient(to right, #f0f4ff, #f8faff); border-left:3px solid var(--blue); border-bottom:1px solid var(--gray-200);">';
    html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">';
    html += '<span style="font-weight:600; font-size:13px; color:var(--blue);">' + glCode + ' — ' + (glGroup.gl_name || '') + '</span>';
    html += '<span style="font-size:12px; color:var(--gray-500);">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' · ' + fmt(glGroup.total || 0) + '</span>';
    html += '</div>';

    // Build datalist options once (shared)
    let dlOptions = '';
    allGLs.forEach(g => { dlOptions += '<option value="' + g + '">' + g + ' - ' + (allGLMap[g] || '') + '</option>'; });

    html += '<table style="width:100%; font-size:12px; border-collapse:collapse; background:white; border-radius:6px; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,0.05);">';
    html += '<thead><tr style="background:var(--gray-100); color:var(--gray-600); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
    html += '<td style="padding:6px 6px; width:28px;"><input type="checkbox" id="batch_all_' + glCode.replace(/[^a-zA-Z0-9]/g,'_') + '" onchange="toggleBatchAll(this,\'' + glCode + '\')" title="Select all"></td>';
    html += '<td style="padding:6px 10px;">Payee</td><td style="padding:6px 10px;">Invoice #</td><td style="padding:6px 10px;">Date</td><td style="padding:6px 10px;">Check #</td><td style="padding:6px 10px; text-align:right;">Amount</td><td style="padding:6px 10px; text-align:right; width:180px;">Action</td></tr></thead>';

    glGroup.invoices.forEach(inv => {
        const isReclassed = !!inv.reclass_to_gl;
        html += '<tr style="border-top:1px solid var(--gray-200);' + (isReclassed ? ' opacity:0.5; text-decoration:line-through;' : '') + '">';
        html += '<td style="padding:6px 6px;">';
        if (!isReclassed) {
            html += '<input type="checkbox" class="batch-cb" data-inv-id="' + inv.id + '" data-gl="' + glCode + '" data-amt="' + inv.amount + '" onchange="updateBatchBar(\'' + glCode + '\')">';
        }
        html += '</td>';
        html += '<td style="padding:6px 10px;">' + (inv.payee_name || inv.payee_code || '—') + '</td>';
        html += '<td style="padding:6px 10px; font-family:monospace; font-size:11px;">' + (inv.invoice_num || '—') + '</td>';
        html += '<td style="padding:6px 10px;">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>';
        html += '<td style="padding:6px 10px;">' + (inv.check_num || '—') + '</td>';
        html += '<td style="padding:6px 10px; text-align:right; font-variant-numeric:tabular-nums;">' + fmt(inv.amount) + '</td>';
        html += '<td style="padding:6px 10px; text-align:right;">';
        if (isReclassed) {
            html += '<span style="font-size:11px; color:var(--orange);">→ ' + inv.reclass_to_gl + '</span> ';
            html += '<button onclick="inlineUndoReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:#fef3c7; color:#92400e; border:1px solid #fcd34d; border-radius:4px; cursor:pointer;">Undo</button>';
        } else {
            html += '<input id="reclass_gl_' + inv.id + '" list="reclass_list_' + glCode.replace(/[^a-zA-Z0-9]/g,'_') + '" placeholder="Search GL..." style="font-size:11px; padding:2px 6px; border:1px solid var(--gray-300); border-radius:4px; width:200px;">';
            html += ' <button onclick="inlineReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer;">Go</button>';
        }
        html += '</td></tr>';
    });
    // Shared datalist (one per GL)
    html += '</table>';
    html += '<datalist id="reclass_list_' + glCode.replace(/[^a-zA-Z0-9]/g,'_') + '">' + dlOptions + '</datalist>';

    // Batch reclass bar (hidden by default)
    html += '<div id="batch_bar_' + glCode.replace(/[^a-zA-Z0-9]/g,'_') + '" style="display:none; margin-top:8px; padding:8px 12px; background:#dbeafe; border:1px solid #2563eb; border-radius:6px; font-size:12px;">';
    html += '<div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">';
    html += '<span id="batch_count_' + glCode.replace(/[^a-zA-Z0-9]/g,'_') + '" style="font-weight:600; color:#2563eb;">0 selected ($0)</span>';
    html += '<span>→ Reclass to:</span>';
    html += '<input id="batch_target_' + glCode.replace(/[^a-zA-Z0-9]/g,'_') + '" list="reclass_list_' + glCode.replace(/[^a-zA-Z0-9]/g,'_') + '" placeholder="Search GL..." style="font-size:11px; padding:3px 8px; border:1px solid #2563eb; border-radius:4px; width:220px;">';
    html += '<button onclick="batchReclass(\'' + glCode + '\')" style="font-size:11px; padding:4px 12px; background:#2563eb; color:white; border:none; border-radius:4px; cursor:pointer; font-weight:600;">Reclass Selected</button>';
    html += '</div></div>';

    html += '</div></td>';
    detailRow.innerHTML = html;
    row.after(detailRow);
}

async function inlineReclass(invoiceId, fromGL) {
    const select = document.getElementById('reclass_gl_' + invoiceId);
    if (!select || !select.value) { alert('Select a target GL code'); return; }
    try {
        const resp = await fetch('/api/expense-dist/reclass/' + invoiceId, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ reclass_to_gl: select.value, reclass_notes: 'Reclassed from PM budget review' })
        });
        if (resp.ok) {
            _expenseCache = null; // Clear cache to refresh
            // Re-apply reclass adjustments to YTD and re-render the whole table
            await applyExpenseReclassAdjustments();
            renderTable();
            updateZeroToggle();
            showToast('Invoice reclassified to ' + select.value + ' — YTD updated', 'success');
        } else { showToast('Reclass failed', 'error'); }
    } catch(e) { showToast('Reclass error: ' + e.message, 'error'); }
}

async function inlineUndoReclass(invoiceId, fromGL) {
    try {
        const resp = await fetch('/api/expense-dist/reclass/' + invoiceId, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ reclass_to_gl: '' })
        });
        if (resp.ok) {
            _expenseCache = null;
            // Re-apply reclass adjustments to YTD and re-render
            await applyExpenseReclassAdjustments();
            renderTable();
            updateZeroToggle();
            showToast('Reclass undone — YTD restored', 'success');
        } else { showToast('Undo failed', 'error'); }
    } catch(e) { showToast('Undo error: ' + e.message, 'error'); }
}

function glKey(glCode) { return glCode.replace(/[^a-zA-Z0-9]/g,'_'); }

function toggleBatchAll(masterCb, glCode) {
    const cbs = document.querySelectorAll('.batch-cb[data-gl="' + glCode + '"]');
    cbs.forEach(cb => { cb.checked = masterCb.checked; });
    updateBatchBar(glCode);
}

function updateBatchBar(glCode) {
    const cbs = document.querySelectorAll('.batch-cb[data-gl="' + glCode + '"]:checked');
    const bar = document.getElementById('batch_bar_' + glKey(glCode));
    const countEl = document.getElementById('batch_count_' + glKey(glCode));
    if (!bar) return;
    let total = 0;
    cbs.forEach(cb => { total += parseFloat(cb.dataset.amt || 0); });
    if (cbs.length > 0) {
        bar.style.display = 'block';
        countEl.textContent = cbs.length + ' selected (' + fmt(total) + ')';
    } else {
        bar.style.display = 'none';
    }
}

async function batchReclass(glCode) {
    const cbs = document.querySelectorAll('.batch-cb[data-gl="' + glCode + '"]:checked');
    const target = document.getElementById('batch_target_' + glKey(glCode));
    if (!cbs.length) { alert('No invoices selected'); return; }
    if (!target || !target.value) { alert('Select a target GL code'); return; }
    const ids = Array.from(cbs).map(cb => parseInt(cb.dataset.invId));
    try {
        const resp = await fetch('/api/expense-dist/reclass-batch', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ invoice_ids: ids, reclass_to_gl: target.value, reclass_notes: 'Batch reclassed from PM budget review' })
        });
        const data = await resp.json();
        if (resp.ok) {
            _expenseCache = null;
            await applyExpenseReclassAdjustments();
            renderTable();
            updateZeroToggle();
            showToast(data.reclassed + ' invoice' + (data.reclassed !== 1 ? 's' : '') + ' reclassified to ' + target.value + ' — YTD updated', 'success');
        } else { showToast('Batch reclass failed: ' + (data.error || 'Unknown error'), 'error'); }
    } catch(e) { showToast('Batch reclass error: ' + e.message, 'error'); }
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
    } else if (field === 'category') {
        line.category = el.value;
    }

    // Update computed columns
    const est = computeEstimate(line);
    const fc = computeForecast(line);
    const pb = computeProposed(line);
    const variance = pb - (line.current_budget || 0);
    const pctChange = (line.current_budget || 0) ? (pb / line.current_budget - 1) : 0;

    const estEl = document.getElementById('est_' + gl);
    const fcEl = document.getElementById('fc_' + gl);
    const pbEl = document.getElementById('pb_' + gl);
    const varEl = document.getElementById('var_' + gl);
    const pctEl = document.getElementById('pct_' + gl);
    if (estEl) estEl.textContent = fmt(est);
    if (fcEl) fcEl.textContent = fmt(fc);
    if (pbEl) pbEl.textContent = fmt(pb);
    if (varEl) { varEl.textContent = fmt(variance); varEl.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)'; }
    if (pctEl) pctEl.textContent = (pctChange * 100).toFixed(1) + '%';

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
            notes: l.notes || '',
            category: l.category || ''
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
        showToast('Submitted for FA review!', 'success');
        setTimeout(() => { window.location.href = '/pm'; }, 1000);
    } else {
        const err = await resp.json();
        showToast('Error: ' + (err.error || 'Unknown'), 'error');
    }
}

// Reclass suggestion modal
function showReclass(glCode) {
    const line = LINES.find(l => l.gl_code === glCode);
    if (!line) return;

    // Build GL options from RM_GL_MAP keys (all budget template GL codes)
    const glOptions = LINES.filter(l => l.gl_code !== glCode)
        .sort((a, b) => a.gl_code.localeCompare(b.gl_code))
        .map(l => `<option value="${l.gl_code}">${l.gl_code} - ${l.description}</option>`)
        .join('');

    const row = document.querySelector(`[data-gl="${glCode}"]`).closest('tr');
    // Remove existing reclass form if any
    const existing = row.nextElementSibling;
    if (existing && existing.classList.contains('reclass-form-row')) {
        existing.remove();
        return;
    }

    const formRow = document.createElement('tr');
    formRow.className = 'reclass-form-row';
    formRow.innerHTML = `
        <td colspan="13" style="padding:12px 24px; background:var(--blue-light); border-left:3px solid var(--blue);">
            <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
                <label style="font-size:12px; font-weight:600;">Suggest reclass to:</label>
                <input id="reclass_target_${glCode}" list="reclass_target_list_${glCode}" placeholder="Search GL code..." style="font-size:12px; padding:4px 8px; border:1px solid var(--gray-300); border-radius:4px; width:180px;">
                <datalist id="reclass_target_list_${glCode}">
                    ${glOptions}
                </datalist>
                <input type="number" id="reclass_amount_${glCode}" placeholder="Amount" step="1" value="${Math.round(line.ytd_actual || 0)}"
                       style="width:100px; font-size:12px; padding:4px 8px; border:1px solid var(--gray-300); border-radius:4px;">
                <input type="text" id="reclass_notes_${glCode}" placeholder="Notes for FA" value="${line.reclass_notes || ''}"
                       style="width:200px; font-size:12px; padding:4px 8px; border:1px solid var(--gray-300); border-radius:4px;">
                <button onclick="saveReclass('${glCode}')" style="font-size:12px; padding:4px 12px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer;">Save</button>
                <button onclick="this.closest('tr').remove()" style="font-size:12px; padding:4px 12px; background:var(--gray-200); border:none; border-radius:4px; cursor:pointer;">Cancel</button>
            </div>
        </td>
    `;
    row.after(formRow);
}

async function saveReclass(glCode) {
    const target = document.getElementById('reclass_target_' + glCode).value;
    const amount = document.getElementById('reclass_amount_' + glCode).value;
    const notes = document.getElementById('reclass_notes_' + glCode).value;

    if (!target) { alert('Select a target GL code'); return; }

    const resp = await fetch('/api/lines/' + ENTITY + '/reclass', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({gl_code: glCode, reclass_to_gl: target, reclass_amount: parseFloat(amount) || 0, reclass_notes: notes})
    });

    if (resp.ok) {
        // Update local data and re-render
        const line = LINES.find(l => l.gl_code === glCode);
        if (line) {
            line.reclass_to_gl = target;
            line.reclass_amount = parseFloat(amount) || 0;
            line.reclass_notes = notes;
        }
        renderTable();
        updateZeroToggle();
    } else {
        alert('Error saving reclass suggestion');
    }
}

// Disable submit button if not editable
if (!CAN_EDIT) {
    const btn = document.getElementById('submitBtn');
    if (btn) btn.disabled = true;
}

// Apply invoice reclass adjustments before first render, then render
(async () => {
    await applyExpenseReclassAdjustments();
    renderTable();
    updateZeroToggle();
})();
</script>
</body>
</html>
"""


PRESENTATION_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{ building_name }} - {{ year }} Budget Presentation</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:'Inter',sans-serif; background:#0f172a; color:#e2e8f0; }
  .header { background:linear-gradient(135deg, #1e293b 0%, #0f172a 100%); padding:40px 60px; border-bottom:1px solid #334155; }
  .header h1 { font-size:32px; font-weight:300; color:#f8fafc; letter-spacing:-0.5px; }
  .header .subtitle { font-size:14px; color:#94a3b8; margin-top:8px; letter-spacing:1px; text-transform:uppercase; }
  .header .logo { font-size:13px; color:#64748b; margin-top:4px; }
  .nav { display:flex; gap:4px; padding:0 60px; background:#1e293b; border-bottom:1px solid #334155; overflow-x:auto; }
  .nav button { padding:12px 20px; background:transparent; border:none; color:#94a3b8; font-size:13px; font-weight:500; cursor:pointer; border-bottom:2px solid transparent; white-space:nowrap; }
  .nav button:hover { color:#e2e8f0; }
  .nav button.active { color:#38bdf8; border-bottom-color:#38bdf8; }
  .content { padding:30px 60px; max-width:1400px; }
  .summary-cards { display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:16px; margin-bottom:30px; }
  .card { background:#1e293b; border:1px solid #334155; border-radius:12px; padding:20px; }
  .card .label { font-size:11px; text-transform:uppercase; letter-spacing:1px; color:#64748b; margin-bottom:8px; }
  .card .value { font-size:24px; font-weight:600; color:#f8fafc; }
  .card .delta { font-size:13px; margin-top:4px; }
  .delta-up { color:#f87171; }
  .delta-down { color:#4ade80; }
  table { width:100%; border-collapse:collapse; }
  thead th { text-align:left; padding:10px 12px; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:#64748b; border-bottom:1px solid #334155; }
  thead th.num { text-align:right; }
  tbody td { padding:8px 12px; border-bottom:1px solid #1e293b; font-size:13px; }
  tbody td.num { text-align:right; font-variant-numeric:tabular-nums; }
  tbody tr:hover { background:#1e293b; }
  .subtotal td { font-weight:600; background:#1e293b; border-top:1px solid #334155; border-bottom:1px solid #334155; color:#f8fafc; }
  .sheet-total td { font-weight:700; background:#0f172a; border-top:2px solid #38bdf8; color:#38bdf8; font-size:14px; }
  .cat-header td { padding:14px 12px 6px; font-weight:600; color:#38bdf8; font-size:14px; border-bottom:2px solid #1e3a5f; }
  .variance-neg { color:#4ade80; }
  .variance-pos { color:#f87171; }
  .footer { padding:30px 60px; font-size:11px; color:#475569; border-top:1px solid #1e293b; margin-top:40px; }
  @media print {
    body { background:white; color:#1e293b; }
    .header { background:white; border-bottom:2px solid #1e293b; }
    .header h1 { color:#0f172a; }
    .nav { display:none; }
    .card { border:1px solid #e2e8f0; }
    .card .value { color:#0f172a; }
    thead th { color:#64748b; border-bottom:2px solid #e2e8f0; }
    tbody td { border-bottom:1px solid #f1f5f9; }
    .subtotal td { background:#f8fafc; }
    .sheet-total td { border-top:2px solid #0f172a; color:#0f172a; }
    .cat-header td { color:#0f172a; border-bottom:2px solid #e2e8f0; }
    .variance-neg { color:#16a34a; }
    .variance-pos { color:#dc2626; }
    @page { margin:0.5in; }
  }
</style>
</head>
<body>

<div class="header">
  <h1>{{ building_name }}</h1>
  <div class="subtitle">{{ year }} Operating Budget</div>
  <div class="logo">Century Management</div>
</div>

<div class="nav" id="tabNav"></div>
<div class="content" id="mainContent"></div>

<div class="footer">
  Prepared by Century Management &middot; Confidential &middot; <span id="printDate"></span>
</div>

<script>
const SHEETS = {{ sheets_json | safe }};
const SHEET_ORDER = {{ sheet_order_json | safe }};
const YTD_MONTHS = {{ ytd_months }};
const REMAINING_MONTHS = {{ remaining_months }};
const MONTH_ABBR = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

document.getElementById('printDate').textContent = new Date().toLocaleDateString('en-US', {year:'numeric', month:'long', day:'numeric'});

function fmt(n) { return '$' + Math.round(n).toLocaleString(); }

function computeEstimate(l) {
  const ytd = l.ytd_actual || 0;
  if (ytd > 0 && YTD_MONTHS > 0) return (ytd / YTD_MONTHS) * REMAINING_MONTHS;
  return 0;
}

function computeForecast(l) {
  return (l.ytd_actual || 0) + (l.accrual_adj || 0) + (l.unpaid_bills || 0) + computeEstimate(l);
}

const CATEGORIES = {
  'Repairs & Supplies': [
    {label:'Supplies', match: l => l.category === 'supplies'},
    {label:'Repairs', match: l => l.category === 'repairs'},
    {label:'Maintenance Contracts', match: l => l.category === 'maintenance'}
  ],
  'Gen & Admin': [
    {label:'Professional Fees', match: l => l.row_num >= 8 && l.row_num <= 16},
    {label:'Administrative & Other', match: l => l.row_num >= 20 && l.row_num <= 49},
    {label:'Insurance', match: l => l.row_num >= 53 && l.row_num <= 64},
    {label:'Taxes', match: l => l.row_num >= 68 && l.row_num <= 78},
    {label:'Financial Expenses', match: l => l.row_num >= 82 && l.row_num <= 90}
  ]
};

function sumLines(lines) {
  const t = {forecast:0, proposed:0, budget:0};
  lines.forEach(l => {
    t.forecast += computeForecast(l);
    t.budget += l.current_budget || 0;
    t.proposed += l.proposed_budget || (computeForecast(l) * (1 + (l.increase_pct || 0)));
  });
  return t;
}

function renderSummary() {
  const content = document.getElementById('mainContent');
  let allLines = [];
  SHEET_ORDER.forEach(s => { allLines = allLines.concat(SHEETS[s] || []); });
  const incomeLines = SHEETS['Income'] || [];
  const expenseLines = allLines.filter(l => l.sheet_name !== 'Income');

  const inc = sumLines(incomeLines);
  const exp = sumLines(expenseLines);
  const noiProposed = inc.proposed - exp.proposed;
  const noiBudget = inc.budget - exp.budget;

  let html = '<div class="summary-cards">' +
    '<div class="card"><div class="label">Total Income</div><div class="value">' + fmt(inc.proposed) + '</div>' +
    '<div class="delta ' + (inc.proposed >= inc.budget ? 'delta-down' : 'delta-up') + '">' + (inc.budget ? ((inc.proposed/inc.budget-1)*100).toFixed(1) + '% vs budget' : '') + '</div></div>' +
    '<div class="card"><div class="label">Total Expenses</div><div class="value">' + fmt(exp.proposed) + '</div>' +
    '<div class="delta ' + (exp.proposed <= exp.budget ? 'delta-down' : 'delta-up') + '">' + (exp.budget ? ((exp.proposed/exp.budget-1)*100).toFixed(1) + '% vs budget' : '') + '</div></div>' +
    '<div class="card"><div class="label">Net Operating Income</div><div class="value">' + fmt(noiProposed) + '</div>' +
    '<div class="delta ' + (noiProposed >= noiBudget ? 'delta-down' : 'delta-up') + '">' + fmt(noiProposed - noiBudget) + ' vs budget</div></div>' +
    '<div class="card"><div class="label">Operating Ratio</div><div class="value">' + (inc.proposed ? (exp.proposed/inc.proposed*100).toFixed(1) + '%' : '—') + '</div>' +
    '<div class="delta" style="color:#94a3b8;">Expenses / Income</div></div></div>';

  // Summary table by sheet
  html += '<table><thead><tr><th>Category</th><th class="num">Proposed Budget</th></tr></thead><tbody>';
  SHEET_ORDER.forEach(s => {
    const cats = CATEGORIES[s];
    if (cats) {
      const sheetLines = SHEETS[s] || [];
      cats.forEach(cat => {
        const gl = sheetLines.filter(cat.match);
        if (gl.length === 0) return;
        const t = sumLines(gl);
        html += '<tr><td style="padding-left:24px;">' + cat.label + '</td><td class="num">' + fmt(t.proposed) + '</td></tr>';
      });
      const st = sumLines(SHEETS[s] || []);
      html += '<tr class="subtotal"><td>' + s + '</td><td class="num">' + fmt(st.proposed) + '</td></tr>';
    } else {
      const t = sumLines(SHEETS[s] || []);
      html += '<tr class="subtotal"><td>' + s + '</td><td class="num">' + fmt(t.proposed) + '</td></tr>';
    }
  });
  // Total row
  html += '<tr class="sheet-total"><td>Total Operating Expenses</td><td class="num">' + fmt(exp.proposed) + '</td></tr>';
  // NOI
  html += '<tr class="sheet-total"><td>Net Operating Income</td><td class="num">' + fmt(noiProposed) + '</td></tr>';
  html += '</tbody></table>';
  content.innerHTML = html;
}

function renderSheet(sheetName) {
  const content = document.getElementById('mainContent');
  const lines = SHEETS[sheetName] || [];
  const estLabel = MONTH_ABBR[YTD_MONTHS] + '-Dec';

  let html = '<table><thead><tr>' +
    '<th>GL Code</th><th>Description</th>' +
    '<th class="num">YTD Actual</th>' +
    '<th class="num">' + estLabel + ' Est</th><th class="num">12 Mo Forecast</th>' +
    '<th class="num">Current Budget</th><th class="num">Proposed Budget</th><th class="num">$ V. Forecast</th><th class="num">% Change</th>' +
    '</tr></thead><tbody>';

  const cats = CATEGORIES[sheetName];

  function buildRow(l) {
    const budget = l.current_budget || 0;
    const forecast = computeForecast(l);
    const proposed = l.proposed_budget || (forecast * (1 + (l.increase_pct || 0)));
    const v = proposed - forecast;
    const p = forecast ? (proposed/forecast-1)*100 : 0;
    return '<tr><td style="font-family:monospace; font-size:12px;">' + l.gl_code + '</td><td>' + l.description + '</td>' +
      '<td class="num">' + fmt(l.ytd_actual || 0) + '</td>' +
      '<td class="num">' + fmt(computeEstimate(l)) + '</td><td class="num">' + fmt(forecast) + '</td>' +
      '<td class="num">' + fmt(budget) + '</td>' +
      '<td class="num" style="font-weight:600;">' + fmt(proposed) + '</td>' +
      '<td class="num ' + (v >= 0 ? 'variance-pos' : 'variance-neg') + '">' + fmt(v) + '</td>' +
      '<td class="num">' + p.toFixed(1) + '%</td></tr>';
  }

  function buildSubtotal(label, ls) {
    const t = sumLines(ls);
    const v = t.proposed - t.forecast;
    return '<tr class="subtotal"><td colspan="2">' + label + '</td>' +
      '<td class="num"></td><td class="num"></td><td class="num">' + fmt(t.forecast) + '</td>' +
      '<td class="num">' + fmt(t.budget) + '</td><td class="num">' + fmt(t.proposed) + '</td><td class="num ' + (v >= 0 ? 'variance-pos' : 'variance-neg') + '">' + fmt(v) + '</td>' +
      '<td class="num">' + (t.forecast ? ((t.proposed/t.forecast-1)*100).toFixed(1) : '0.0') + '%</td></tr>';
  }

  if (cats) {
    cats.forEach(cat => {
      const gl = lines.filter(cat.match);
      if (gl.length === 0) return;
      html += '<tr class="cat-header"><td colspan="9">' + cat.label + '</td></tr>';
      gl.forEach(l => { html += buildRow(l); });
      html += buildSubtotal('Total ' + cat.label, gl);
    });
  } else {
    lines.forEach(l => { html += buildRow(l); });
  }

  // Sheet total
  const t = sumLines(lines);
  const tv = t.proposed - t.forecast;
  html += '<tr class="sheet-total"><td colspan="2">Total ' + sheetName + '</td>' +
    '<td class="num"></td><td class="num"></td><td class="num">' + fmt(t.forecast) + '</td>' +
    '<td class="num">' + fmt(t.budget) + '</td><td class="num">' + fmt(t.proposed) + '</td><td class="num ' + (tv >= 0 ? 'variance-pos' : 'variance-neg') + '">' + fmt(tv) + '</td>' +
    '<td class="num">' + (t.forecast ? ((t.proposed/t.forecast-1)*100).toFixed(1) : '0.0') + '%</td></tr>';
  html += '</tbody></table>';
  content.innerHTML = html;
}

function switchTab(name, el) {
  document.querySelectorAll('.nav button').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  if (name === 'Summary') renderSummary();
  else renderSheet(name);
}

// Build tabs
const nav = document.getElementById('tabNav');
const summaryBtn = document.createElement('button');
summaryBtn.textContent = 'Summary';
summaryBtn.className = 'active';
summaryBtn.onclick = function() { switchTab('Summary', this); };
nav.appendChild(summaryBtn);

SHEET_ORDER.forEach(s => {
  const btn = document.createElement('button');
  btn.textContent = s;
  btn.onclick = function() { switchTab(s, this); };
  nav.appendChild(btn);
});

renderSummary();
</script>
</body>
</html>
"""
