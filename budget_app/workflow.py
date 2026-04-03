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

# Comprehensive mapping: budget_line category → Century audit category
BUDGET_CAT_TO_CENTURY = {
    "supplies": "Supplies",
    "repairs": "Repairs & Maintenance",
    "maintenance": "Repairs & Maintenance",
    "payroll": "Payroll",
    "electric": "Electric",
    "gas": "Gas Cooking / Heating",
    "fuel": "Fuel",
    "oil": "Fuel",
    "water": "Water & Sewer",
    "sewer": "Water & Sewer",
    "insurance": "Insurance",
    "re_taxes": "Real Estate Taxes",
    "professional": "Professional Fees",
    "admin": "Administrative & Other",
    "financial": "Financial Expenses",
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
        year = db.Column(db.Integer, default=2027)
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

        # Building type (coop, condo, rental) for charge mapping
        building_type = db.Column(db.String(50), default="")

        # Versioning
        version = db.Column(db.Integer, default=1)

        # Relationships (use backref on child side to avoid forward-reference issues)
        lines = db.relationship("BudgetLine", back_populates="budget", cascade="all, delete-orphan")

        __table_args__ = (db.UniqueConstraint("entity_code", "year", "version", name="uq_entity_year_ver"),)

        def to_dict(self):
            return {
                "id": self.id,
                "entity_code": self.entity_code,
                "building_name": self.building_name,
                "year": self.year,
                "status": self.status,
                "fa_notes": self.fa_notes,
                "initiated_by": self.initiated_by,
                "initiated_at": self.initiated_at.isoformat() if self.initiated_at else None,
                "presentation_token": self.presentation_token,
                "approved_by": self.approved_by,
                "approved_at": self.approved_at.isoformat() if self.approved_at else None,
                "increase_pct": self.increase_pct,
                "effective_date": self.effective_date,
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
                "ar_notes": self.ar_notes or "",
                "version": self.version or 1,
                "building_type": self.building_type or ""
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
                "proposed_formula": self.proposed_formula or "",
                "estimate_override": self.estimate_override,
                "forecast_override": self.forecast_override,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None
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
                        line.reclass_to_gl = None
                        line.reclass_amount = 0.0
                        line.reclass_notes = ""
                        line.estimate_override = None
                        line.forecast_override = None
                        line.proposed_budget = 0.0
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

    def _delete_entity_data(entity_code):
        """Delete ALL entity-level supplementary data (expenses, open AP, etc.).
        Called by budget deletion to fully remove an entity's data.
        Each table is deleted in its own try/except with rollback to prevent
        one failure from poisoning the entire transaction."""
        ec = str(entity_code).strip()
        for sql in [
            "DELETE FROM expense_invoices WHERE report_id IN (SELECT id FROM expense_reports WHERE entity_code = :ec)",
            "DELETE FROM expense_reports WHERE entity_code = :ec",
            "DELETE FROM open_ap_invoices WHERE report_id IN (SELECT id FROM open_ap_reports WHERE entity_code = :ec)",
            "DELETE FROM open_ap_reports WHERE entity_code = :ec",
        ]:
            try:
                db.session.execute(db.text(sql), {"ec": ec})
            except Exception as e:
                db.session.rollback()
                logger.warning(f"_delete_entity_data skip: {e}")
        logger.info(f"Deleted entity-level data for {ec}")

    def store_all_lines(entity_code, building_name, gl_data, template_path, assumptions=None, fresh_start=False):
        """
        Store ALL GL codes from YSL data into budget_lines (not just R&M).
        Uses GLMapper to get sheet/row/description for every GL code.
        Optionally stores merged assumptions snapshot on the Budget record.
        If fresh_start=True, deletes all existing lines and resets to draft.
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

            # Fresh start: wipe all existing lines and reset to draft
            if fresh_start and budget.id:
                deleted = BudgetLine.query.filter_by(budget_id=budget.id).delete()
                budget.status = "draft"
                db.session.flush()
                logger.info(f"Fresh start: deleted {deleted} lines for {entity_code}")

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


    def compute_forecast(ytd_actual, accrual_adj, unpaid_bills, prior_year, ytd_months=2):
        """
        Compute 12-month forecast.

        Formula: ytd_actual + accrual_adj + unpaid_bills + estimate
        where estimate = IF(ytd_total >= prior_year,
                            annualize: (ytd_total / ytd_months) * remaining_months,
                            prior_year_adj: prior_year - ytd_total)
        """
        ytd_total = ytd_actual + accrual_adj + unpaid_bills
        remaining = 12 - ytd_months

        if ytd_total >= prior_year and prior_year > 0 and ytd_months > 0:
            estimate = (ytd_total / ytd_months) * remaining
        else:
            estimate = max(prior_year - ytd_total, 0)

        return ytd_total + estimate


    def forecast_method(ytd_actual, accrual_adj, unpaid_bills, prior_year):
        """Return the forecast method label for display purposes."""
        ytd_total = ytd_actual + accrual_adj + unpaid_bills
        if ytd_total >= prior_year and prior_year > 0:
            return 'Annualized'
        return 'Prior Year Adjusted'


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
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()

        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        # Check if PM can edit this budget
        can_edit = budget.status in ["pm_pending", "pm_in_progress", "returned"]

        # PM only sees Repairs & Supplies lines (pm_editable=True)
        lines = BudgetLine.query.filter_by(budget_id=budget.id, pm_editable=True).order_by(BudgetLine.row_num).all()
        import json as json_mod

        lines_data = [l.to_dict() for l in lines]

        # Get ALL GL codes for reclass modal (not just pm_editable)
        all_gls = db.session.query(BudgetLine.gl_code, BudgetLine.description, BudgetLine.category).filter_by(budget_id=budget.id).order_by(BudgetLine.gl_code).all()
        all_gl_list = [{"gl_code": g.gl_code, "description": g.description, "category": g.category} for g in all_gls]

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
            all_gl_json=json_mod.dumps(all_gl_list),
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
            d["has_expenses"] = has_expenses
            d["has_audit"] = has_audit
            result.append(d)
        return jsonify(result)


    @bp.route("/api/budgets/<entity_code>/status", methods=["POST"])
    def change_budget_status(entity_code):
        """Change budget status with validation using VALID_TRANSITIONS."""
        data = request.get_json()
        new_status = data.get("status")

        if new_status not in BUDGET_STATUSES:
            return jsonify({"error": f"Invalid status. Must be one of {BUDGET_STATUSES}"}), 400

        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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
        """Delete a non-approved budget and all its related records.
        Uses raw SQL to avoid ORM session poisoning issues."""
        # Always start with a clean session
        try:
            db.session.rollback()
        except Exception:
            pass

        # Look up budget via raw SQL — immune to session poisoning
        row = db.session.execute(
            db.text("SELECT id, entity_code, status, version FROM budgets WHERE id = :id"),
            {"id": budget_id}
        ).fetchone()
        if not row:
            return jsonify({"error": "Budget not found"}), 404

        bid, entity, status, ver = row[0], row[1], row[2], row[3] or 1
        if status == "approved":
            return jsonify({"error": "Cannot delete an approved budget."}), 400

        try:
            # Get line IDs for this budget
            line_rows = db.session.execute(
                db.text("SELECT id FROM budget_lines WHERE budget_id = :bid"), {"bid": bid}
            ).fetchall()
            line_ids = [r[0] for r in line_rows]

            # Delete in dependency order using raw SQL
            if line_ids:
                ids_str = ",".join(str(i) for i in line_ids)
                db.session.execute(db.text(f"DELETE FROM presentation_edits WHERE budget_line_id IN ({ids_str})"))
                db.session.execute(db.text(f"DELETE FROM budget_revisions WHERE budget_line_id IN ({ids_str})"))
            db.session.execute(db.text("DELETE FROM budget_revisions WHERE budget_id = :bid"), {"bid": bid})
            db.session.execute(db.text("DELETE FROM presentation_sessions WHERE budget_id = :bid"), {"bid": bid})
            db.session.execute(db.text("DELETE FROM ar_handoffs WHERE budget_id = :bid"), {"bid": bid})
            db.session.execute(db.text("DELETE FROM data_sources WHERE budget_id = :bid"), {"bid": bid})
            db.session.execute(db.text("DELETE FROM budget_lines WHERE budget_id = :bid"), {"bid": bid})
            # Wipe entity-level data
            _delete_entity_data(entity)
            # Delete the budget itself
            db.session.execute(db.text("DELETE FROM budgets WHERE id = :bid"), {"bid": bid})
            db.session.commit()
            logger.info(f"Deleted budget {bid} (entity {entity}, v{ver})")
            return jsonify({"message": f"Budget v{ver} for {entity} deleted", "id": bid})
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to delete budget {bid}: {e}")
            return jsonify({"error": f"Failed to delete: {str(e)}"}), 500


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
                    "period_from": row[1],
                    "period_to": row[2],
                    "total_amount": float(row[3]) if row[3] else 0,
                    "invoice_count": invoice_count
                }
        except Exception:
            pass

        # Check audit data — fetch ALL confirmed uploads for multi-year comparison
        audit_data = {"exists": False, "years": {}, "summary_years": {}}
        try:
            import json as _json
            from budget_app.audited_financials import CENTURY_TO_SUMMARY

            audit_rows = db.session.execute(
                db.text("SELECT mapped_data, fiscal_year_end FROM audit_upload WHERE entity_code = :ec AND status = 'confirmed' ORDER BY fiscal_year_end DESC"),
                {"ec": entity_code}
            ).fetchall()
            if audit_rows:
                years_data = {}
                summary_years_data = {}
                for row in audit_rows:
                    if not row[0]:
                        continue
                    mapped = _json.loads(row[0])
                    fiscal_year = row[1] or "Unknown"
                    # Extract year_totals[0] for each category (the primary year)
                    year_cats = {}
                    summary_totals = {}
                    for cat, data in mapped.items():
                        if isinstance(data, dict):
                            totals = data.get("year_totals", data.get("years", []))
                            if totals and len(totals) > 0:
                                year_cats[cat] = totals[0]
                                # Also aggregate to summary row
                                summary_label = CENTURY_TO_SUMMARY.get(cat, cat)
                                summary_totals[summary_label] = summary_totals.get(summary_label, 0) + totals[0]
                            elif data.get("total"):
                                year_cats[cat] = data["total"]
                                summary_label = CENTURY_TO_SUMMARY.get(cat, cat)
                                summary_totals[summary_label] = summary_totals.get(summary_label, 0) + data["total"]
                    if year_cats:
                        years_data[fiscal_year] = year_cats
                    if summary_totals:
                        summary_years_data[fiscal_year] = summary_totals

                # Limit to 2 most recent fiscal years
                years_data_limited = dict(sorted(years_data.items(), reverse=True)[:2])
                summary_years_data_limited = dict(sorted(summary_years_data.items(), reverse=True)[:2])

                if years_data_limited:
                    audit_data = {
                        "exists": True,
                        "years": years_data_limited,
                        "summary_years": summary_years_data_limited,
                        "category_mapping": BUDGET_CAT_TO_CENTURY
                    }
        except Exception:
            pass

        # Group lines by sheet for tabbed view
        sheets = {}
        for l in lines:
            sn = l.sheet_name or "Unmapped"
            if sn not in sheets:
                sheets[sn] = []
            sheets[sn].append(l.to_dict())

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

        return jsonify({
            "budget": budget.to_dict(),
            "lines": [l.to_dict() for l in lines],
            "sheets": sheets,
            "sheet_order": [s for s in sheet_order if s in sheets or (s == "RE Taxes" and re_taxes_data)],
            "assignments": {"fa": fa_name, "pm": pm_name},
            "expenses": expense_data,
            "audit": audit_data,
            "assumptions": assumptions,
            "ytd_months": ytd_months,
            "remaining_months": remaining_months,
            "re_taxes": re_taxes_data
        })


    # ─── API Routes: Budget Assumptions ──────────────────────────────────────

    @bp.route("/api/budget-assumptions/<entity_code>", methods=["GET"])
    def get_budget_assumptions(entity_code):
        """Get assumptions for a budget."""
        import json as _json
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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
                prior = float(line.prior_year or 0)
                ytd = float(line.ytd_actual or 0)
                accrual = float(line.accrual_adj or 0)
                unpaid = float(line.unpaid_bills or 0)
                base = ytd + accrual + unpaid
                if base >= prior and prior > 0:
                    estimate = (base / _ytd_months) * _remaining if _ytd_months > 0 else 0
                else:
                    estimate = max(prior - base, 0)
                forecast = base + estimate
                line.proposed_budget = forecast * (1 + float(line.increase_pct or 0))

        db.session.commit()
        logger.info(f"Assumptions updated for {entity_code}, recalculated {recalc_count} lines")

        return jsonify({"status": "saved", "assumptions": current, "recalculated": recalc_count})


    # ─── API Routes: RE Taxes (NYC DOF) ─────────────────────────────────────

    @bp.route("/api/re-taxes/<entity_code>", methods=["GET"])
    def get_re_taxes(entity_code):
        """Get RE Taxes calculation for a co-op property, pulling from NYC DOF."""
        try:
            from dof_taxes import is_coop, compute_re_taxes
            if not is_coop(entity_code):
                return jsonify({"error": "Not a co-op — condos do not have building-level RE taxes", "is_coop": False}), 200
            import json as _json
            budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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
        # Also update the Gen & Admin GL lines that reference RE Taxes
        try:
            from dof_taxes import compute_re_taxes
            result = compute_re_taxes(entity_code, data)
            # Update GL 6315-0000 (Gross Tax) proposed budget
            _update_gl_line(budget.id, "6315-0000", result["gross_tax"])
            # Update exemption lines (negated — they reduce tax)
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
            line.proposed_budget = round(value, 2)

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
            if "category" in line_data and line_data["category"]:
                line.category = line_data["category"]

            # Override fields (PM Tier 1-5 edits)
            if "estimate_override" in line_data:
                val = line_data["estimate_override"]
                line.estimate_override = float(val) if val is not None else None
            if "forecast_override" in line_data:
                val = line_data["forecast_override"]
                line.forecast_override = float(val) if val is not None else None
            if "proposed_budget" in line_data:
                line.proposed_budget = float(line_data["proposed_budget"] or 0)
            if "proposed_formula" in line_data:
                line.proposed_formula = line_data["proposed_formula"] or None
            for fname in ("prior_year", "ytd_actual", "ytd_budget", "current_budget"):
                if fname in line_data:
                    setattr(line, fname, float(line_data[fname] or 0))

        db.session.commit()

        return jsonify(budget.to_dict())


    # ─── FA Line Edit & Reclass Endpoints ────────────────────────────────────

    @bp.route("/api/fa-lines/<entity_code>", methods=["PUT"])
    def update_fa_lines(entity_code):
        """FA edits to any budget line (all sheets, not just R&M)."""
        data = request.get_json()
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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
                "prior_year", "ytd_actual", "ytd_budget", "current_budget"
            ]
            for fname in editable_float_fields:
                if fname in line_data:
                    new_val = float(line_data[fname] or 0)
                    old_val = getattr(line, fname, None) or 0
                    if old_val != new_val:
                        changes.append((fname, str(old_val), str(new_val)))
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

            # Nullable override fields (null = use formula, number = manual override)
            for ofield in ("estimate_override", "forecast_override"):
                if ofield in line_data:
                    raw = line_data[ofield]
                    new_val = float(raw) if raw is not None else None
                    old_val = getattr(line, ofield, None)
                    if old_val != new_val:
                        changes.append((ofield, str(old_val), str(new_val)))
                    setattr(line, ofield, new_val)

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
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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
            output_path = _Path(tmpdir) / f"{entity_code}_{budget.building_name}_2027_Budget.xlsx"
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
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

DASHBOARD_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FA Dashboard - Century Management</title>
<style>
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
  .action-menu { position: relative; display: inline-block; }
  .action-menu-btn { background: transparent; border: 1px solid var(--gray-300); border-radius: 6px; padding: 4px 10px; cursor: pointer; font-size: 16px; line-height: 1; color: var(--gray-500); }
  .action-menu-btn:hover { background: var(--gray-100); }
  .action-menu-items { display: none; position: absolute; right: 0; top: 100%; margin-top: 4px; background: white; border: 1px solid var(--gray-200); border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); min-width: 140px; z-index: 10; padding: 4px 0; }
  .action-menu-items button { display: block; width: 100%; text-align: left; padding: 8px 14px; border: none; background: none; cursor: pointer; font-size: 13px; }
  .action-menu-items button:hover { background: var(--gray-50); }
  .action-menu-items .del-item { color: var(--red); }
  .action-menu-items .del-item:hover { background: var(--red-light); }
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

function toggleMenu(btn) {
  const menu = btn.nextElementSibling;
  document.querySelectorAll('.action-menu-items').forEach(m => { if (m !== menu) m.style.display = 'none'; });
  menu.style.display = menu.style.display === 'block' ? 'none' : 'block';
}
document.addEventListener('click', e => {
  if (!e.target.closest('.action-menu')) document.querySelectorAll('.action-menu-items').forEach(m => m.style.display = 'none');
});

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
  /* ── Promoted Budget Workbook ── */
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
  <div class="summary-cards" id="summaryCards"></div>

  <!-- Context Strip: PM Review + FA Checklist as collapsible panels -->
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

  <!-- Historical Actuals (from Audited Financials) — collapsible -->
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

  <!-- Reclass Suggestions — compact collapsible panel -->
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

  <!-- Budget Workbook (PROMOTED — blue border, primary visual element) -->
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
      <div class="card-value">${fmt(totalBudget - totalForecast)}</div>
      <div class="card-label">Variance</div>
    </div>
    <div class="summary-card">
      <div class="card-value">${totalForecast ? ((totalBudget - totalForecast) / totalForecast * 100).toFixed(1) + '%' : '\u2014'}</div>
      <div class="card-label">% Change</div>
    </div>
  `;

  // PM Track — collapsible panel with badge
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

  const checks = [
    { label: 'YSL Data Imported', done: true, detail: lines.length + ' GL lines loaded' },
    { label: 'Assumptions Configured', done: anyAssumptions, detail: hasBudgetPeriod ? 'Period: ' + assumptions.budget_period : 'Not set — click Assumptions tab', action: !anyAssumptions ? 'openAssumptions' : null },
    { label: 'Expense Distribution', done: data.expenses.exists, detail: data.expenses.exists ? data.expenses.invoice_count + ' invoices (' + fmt(data.expenses.total_amount) + ')' : 'Upload via Data Collection' },
    { label: 'Audited Financials', done: data.audit.exists, detail: data.audit.exists ? Object.keys(data.audit.years || {}).length + ' years of history' : 'Upload via Data Collection' },
    { label: 'PM Review', done: pmDone, detail: pmDone ? 'PM review complete' : (pmSent ? 'Awaiting PM response' : 'Not yet sent'), action: !pmSent ? 'sendToPM' : null },
    { label: 'Review All Sheets', done: linesWithProposed >= lines.length * 0.5, detail: linesWithProposed + ' of ' + lines.length + ' lines have proposed values' },
    { label: 'Final Approval', done: b.status === 'approved', detail: b.status === 'approved' ? 'Budget approved' : 'Complete all steps above first' }
  ];

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

  checks.forEach((c, i) => {
    const iconClass = c.done ? 'check-done' : 'check-pending';
    const iconChar = c.done ? '✓' : '';
    const actionBtn = c.action ? ' <button onclick="' + c.action + '()" style="font-size:11px; padding:2px 8px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer; margin-left:8px;">Go</button>' : '';
    assemblyHtml += '<div class="checklist-item">' +
      '<div class="check-icon ' + iconClass + '">' + iconChar + '</div>' +
      '<div><div class="checklist-label">' + c.label + actionBtn + '</div>' +
      '<div class="checklist-detail">' + c.detail + '</div></div></div>';
  });

  document.getElementById('assemblyContent').innerHTML = assemblyHtml;

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
    document.getElementById('reclassBadge').textContent = reclassLines.length + ' items · ' + fmt(totalReclass);
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
  window._data = data;  // Store data for renderBudgetSummary access to audit.summary_years
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

  // Real Estate Taxes — rates flow into the RE Taxes tab
  const rt = a.re_taxes_overrides || {};
  html += section('Real Estate Taxes',
    item('Tax Rate %', field('re_taxes_overrides','tax_rate', rt.tax_rate ? (rt.tax_rate * 100).toFixed(4) : '0', pctStyle, '%')) +
    item('Est. Tax Rate %', field('re_taxes_overrides','est_tax_rate', rt.est_tax_rate ? (rt.est_tax_rate * 100).toFixed(4) : '0', pctStyle, '%'))
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
  const isNeg = /^\s*\(.*\)\s*$/.test(s);
  const val = parseFloat(s.replace(/[$,\s()]/g, '')) || 0;
  return isNeg ? -val : val;
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
}

// ── fxCellFocus: populate the formula bar when clicking a formula cell ─
function fxCellFocus(el) {
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
  } else if (el.dataset.override === 'true') {
    bar.value = el.dataset.raw || '';
  } else {
    bar.value = el.dataset.formula || '';
  }
  _formulaBarOriginal = bar.value;
  const isReadOnly = field === 'variance' || field === 'pct_change';
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
  const row = td.closest('tr');
  const rowId = row ? row.id : '';
  const col = td.dataset.col;
  // Row label from first cell text
  let rowLabel = 'Total';
  if (row) { const fc = row.querySelector('td'); if (fc) rowLabel = fc.textContent.trim(); }
  const colLabels = {prior:'Prior Year', ytd:'YTD Actual', accrual:'Accrual Adj', unpaid:'Unpaid Bills', ytdBudget:'YTD Budget', estimate:'Estimate', forecast:'12 Mo Forecast', budget:'Curr Budget', proposed:'Proposed', variance:'$ Variance', pctchange:'% Change'};
  label.textContent = rowLabel + ' / ' + (colLabels[col] || col);
  label.style.display = 'inline';
  bar.style.display = 'block';
  // Gather GL codes for this row
  const colPrefix = {prior:'pr_', ytd:'ytd_', accrual:'acc_', unpaid:'unp_', ytdBudget:'ytdb_', estimate:'est_', forecast:'fcst_', budget:'bud_', proposed:'prop_'};
  let glCodes = [];
  if (rowId === 'faSheetTotal') {
    document.querySelectorAll('tr[data-gl]').forEach(r => { if (r.style.display !== 'none') glCodes.push(r.dataset.gl); });
  } else if (rowId.startsWith('subtotal_')) {
    const key = rowId.replace('subtotal_', '');
    glCodes = (window._catGroupGLs || {})[key] || [];
  }
  if (col === 'variance') {
    bar.value = '= Curr Budget - 12 Mo Forecast';
  } else if (col === 'pctchange') {
    bar.value = '= (Curr Budget - 12 Mo Forecast) / 12 Mo Forecast';
  } else {
    const pfx = colPrefix[col];
    if (pfx && glCodes.length) {
      const parts = glCodes.map(gl => { const el = document.getElementById(pfx + gl); return el ? fmt(parseFloat(el.dataset.raw) || 0) : '$0'; });
      bar.value = '= ' + parts.join(' + ');
    } else {
      bar.value = '= ' + fmt(parseFloat(td.dataset.raw) || 0);
    }
  }
  _formulaBarOriginal = bar.value;
  _showFormulaButtons(false, false);
  bar.readOnly = true;
  // Highlight clicked cell
  td.style.outline = '2px solid var(--blue)';
  td.style.outlineOffset = '-2px';
  td.style.borderRadius = '4px';
  const cleanup = (e) => {
    if (!td.contains(e.target) && e.target !== bar) {
      td.style.outline = '';
      td.style.outlineOffset = '';
      td.style.borderRadius = '';
      bar.readOnly = false;
      document.removeEventListener('click', cleanup);
    }
  };
  setTimeout(() => document.addEventListener('click', cleanup), 0);
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
      faUpdateSheetTotals();
    } else {
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
    const formulaResult = safeEvalFormula(typed);
    const numericVal = parseDollar(typed);
    if (formulaResult !== null && (typed.startsWith('=') || /[+\-*\/()]/.test(typed))) {
      const rounded = Math.round(formulaResult);
      el.dataset.raw = rounded;
      el.dataset.override = 'true';
      el.value = fmt(formulaResult);
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
    // Excel: =IFERROR(IF((YTD+Accrual+Unpaid)>=Prior,(YTD+Accrual+Unpaid)/YTD_MONTHS*REMAINING,Prior-(YTD+Accrual+Unpaid)),0)
    if (base >= prior && prior > 0 && YTD_MONTHS > 0) {
      estimate = (base / YTD_MONTHS) * REMAINING_MONTHS;
    } else if (prior > 0) {
      estimate = Math.max(prior - base, 0);
    } else if (base > 0 && YTD_MONTHS > 0) {
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
  // Build updated formula strings matching Excel: =IF((base)>=prior, base/YTD*REM, prior-base)
  let estFormula, estExpr;
  if (base >= prior && prior > 0 && YTD_MONTHS > 0) {
    estFormula = '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
    estExpr = '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
  } else if (prior > 0) {
    estFormula = '=' + prior + '-(' + ytd + '+' + accrual + '+' + unpaid + ')';
    estExpr = prior + '-(' + ytd + '+' + accrual + '+' + unpaid + ')';
  } else if (base > 0 && YTD_MONTHS > 0) {
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

  // Excel: Variance = Curr Budget - 12 Mo Forecast; % Change = (Budget - Forecast) / Forecast
  const variance = budget - forecast;
  const pctChange = forecast ? ((budget - forecast) / forecast) : 0;
  const varEl = document.getElementById('var_' + gl);
  if (varEl) {
    varEl.value = fmt(variance);
    varEl.dataset.raw = Math.round(variance);
    varEl.dataset.formula = '= ' + fmt(budget) + ' - ' + fmt(forecast);
    varEl.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
    const varTd = varEl.closest('td');
    if (varTd) varTd.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
  }
  const pctEl = document.getElementById('pct_' + gl);
  if (pctEl) {
    pctEl.value = (pctChange * 100).toFixed(1) + '%';
    pctEl.dataset.raw = pctChange;
    pctEl.dataset.formula = '= (' + fmt(budget) + ' - ' + fmt(forecast) + ') / ' + fmt(forecast);
  }

  // Recalculate sheet totals from live cell values
  faUpdateSheetTotals();
}

function faUpdateSheetTotals() {
  const raw = (id) => { const el = document.getElementById(id); return el ? parseFloat(el.dataset.raw) || 0 : 0; };

  function sumGLs(glCodes) {
    const t = {prior:0, ytd:0, accrual:0, unpaid:0, ytdBudget:0, estimate:0, forecast:0, budget:0, proposed:0};
    glCodes.forEach(gl => {
      const row = document.querySelector('tr[data-gl="' + gl + '"]');
      if (row && row.style.display === 'none') return;
      t.prior += raw('pr_' + gl);
      t.ytd += raw('ytd_' + gl);
      t.accrual += raw('acc_' + gl);
      t.unpaid += raw('unp_' + gl);
      t.ytdBudget += raw('ytdb_' + gl);
      t.estimate += raw('est_' + gl);
      t.forecast += raw('fcst_' + gl);
      t.budget += raw('bud_' + gl);
      t.proposed += raw('prop_' + gl);
    });
    return t;
  }

  function updateTotalRow(rowEl, t) {
    if (!rowEl) return;
    const v = t.budget - t.forecast;
    const p = t.forecast ? ((t.budget - t.forecast) / t.forecast) : 0;
    const cells = rowEl.querySelectorAll('td');
    // With colspan="3" first cell: cells[0]=label, cells[1]=prior, cells[2]=ytd,
    // cells[3]=accrual, cells[4]=unpaid, cells[5]=ytdBudget,
    // cells[6]=estimate, cells[7]=forecast, cells[8]=budget, cells[9]=inc%(empty),
    // cells[10]=proposed, cells[11]=variance, cells[12]=pctChange
    function setC(cell, val) {
      const sp = cell.querySelector('.sub-val');
      if (sp) { sp.textContent = fmt(val); cell.dataset.raw = Math.round(val).toString(); }
      else { cell.textContent = fmt(val); }
    }
    if (cells.length >= 13) {
      setC(cells[1], t.prior);
      setC(cells[2], t.ytd);
      setC(cells[3], t.accrual);
      setC(cells[4], t.unpaid);
      setC(cells[5], t.ytdBudget);
      setC(cells[6], t.estimate);
      setC(cells[7], t.forecast);
      setC(cells[8], t.budget);
      setC(cells[10], t.proposed);
      const vs = cells[11].querySelector('.sub-val');
      if (vs) { vs.textContent = fmt(v); cells[11].dataset.raw = Math.round(v).toString(); }
      else { cells[11].textContent = fmt(v); }
      cells[11].style.color = v >= 0 ? 'var(--red)' : 'var(--green)';
      const ps = cells[12].querySelector('.sub-val');
      if (ps) { ps.textContent = (p * 100).toFixed(1) + '%'; cells[12].dataset.raw = p.toString(); }
      else { cells[12].textContent = (p * 100).toFixed(1) + '%'; }
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
function faAutoSave(gl, field, value) {
  clearTimeout(_faSaveTimer);
  _faSaveTimer = setTimeout(async () => {
    const indicator = document.getElementById('faSaveIndicator');
    indicator.textContent = 'Saving...';
    const lineData = {gl_code: gl};
    lineData[field] = value;
    await fetch('/api/fa-lines/' + entityCode, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lines: [lineData]})
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
  const accrual = l.accrual_adj || 0;
  const unpaid = l.unpaid_bills || 0;
  const prior = l.prior_year || 0;
  const base = ytd + accrual + unpaid;
  // Excel: =IFERROR(IF((YTD+Accrual+Unpaid)>=Prior,(YTD+Accrual+Unpaid)/YTD_MONTHS*REMAINING,Prior-(YTD+Accrual+Unpaid)),0)
  if (base >= prior && prior > 0 && YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
  if (prior > 0) return Math.max(prior - base, 0);
  if (base > 0 && YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
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
  const estimate = faComputeEstimate(l);
  const forecast = faComputeForecast(l);
  const incPct = l.increase_pct || 0;

  if (field === 'estimate') {
    const prior = l.prior_year || 0;
    const base = ytd + accrual + unpaid;
    if (base >= prior && prior > 0 && YTD_MONTHS > 0) return '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
    if (prior > 0) return '=' + prior + '-(' + ytd + '+' + accrual + '+' + unpaid + ')';
    if (base > 0 && YTD_MONTHS > 0) return '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
    return '=0';
  }
  if (field === 'forecast') {
    const estExpr = (ytd > 0 && YTD_MONTHS > 0) ? ytd + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0';
    return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + estExpr + ')';
  }
  if (field === 'proposed') {
    if (l.proposed_formula) return l.proposed_formula;
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

// ── RE Taxes Tab — Custom Calculation Layout ──────────────────────────────
function renderRETaxesTab(contentDiv) {
  const reTaxes = window._reTaxesData;
  if (!reTaxes) {
    contentDiv.innerHTML = '<p style="padding:24px; color:var(--gray-500);">RE Taxes data not available. This building may not be a co-op, or DOF data has not been fetched yet.</p>';
    return;
  }
  // Formatters matching the budget workbook
  const fmtD = v => '$' + Math.round(v).toLocaleString();
  const fmtPct = v => (v * 100).toFixed(2) + '%';
  const fmtRate = v => (v * 100).toFixed(4) + '%';
  const fmtDollarInput = v => '$' + Math.round(v).toLocaleString();
  // Styles matching FA dashboard grid
  const thStyle = 'padding:8px 10px; font-size:12px; font-weight:600; text-transform:uppercase; letter-spacing:0.3px; color:var(--gray-600); white-space:nowrap;';
  const cellEdit = 'text-align:right; padding:4px; border-bottom:1px solid var(--gray-200);';
  const cellCalc = 'text-align:right; padding:8px 10px; border-bottom:1px solid var(--gray-200); background:#f0faf0; font-weight:600;';
  const cellLabel = 'padding:8px 10px; font-size:13px; border-bottom:1px solid var(--gray-200);';
  const cellNote = 'padding:8px 10px; font-size:11px; color:var(--gray-400); border-bottom:1px solid var(--gray-200);';
  const inputDollar = 'width:100%; text-align:right; padding:6px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-weight:500; background:var(--gray-50);';
  const inputRate = 'width:90px; text-align:right; padding:6px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-weight:500; background:var(--gray-50);';
  const fxBadge = '<span class="re-fx-badge" style="display:inline-block; background:#4ade80; color:#fff; font-size:9px; font-weight:700; padding:1px 4px; border-radius:3px; margin-left:4px; vertical-align:middle;">fx</span>';
  // Wrap calculated values in a span so reCalcTaxes can update value without destroying the fx badge
  // Each fx cell is clickable: onclick populates the formula bar
  const fxCell = (id, val, formula, label) => {
    return '<td style="' + cellCalc + ' cursor:pointer;" id="' + id + '" data-formula="' + formula + '" data-label="' + label + '" onclick="reTaxFxClick(this)" tabindex="0">' +
      '<span class="re-fx-val">' + val + '</span>' + fxBadge + '</td>';
  };

  const d = reTaxes;
  const ex = d.exemptions || {};

  let html = `
  <div style="max-width:960px; margin:0 auto;">
    <!-- Formula bar — matches FA grid style exactly, sticky like Excel -->
    <div id="reTaxFormulaBarWrap" style="display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200); border-radius:8px; margin-bottom:12px; position:sticky; top:0; z-index:10;">
      <span style="font-size:11px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>
      <span id="reTaxFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:var(--gray-600); white-space:nowrap; min-width:120px;"></span>
      <input id="reTaxFormulaBar" type="text" placeholder="Click a green formula cell to view its formula..." style="flex:1; padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:monospace; background:white;" oninput="reTaxFormulaPreview()" onkeydown="reTaxFormulaKeydown(event)">
      <span id="reTaxFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:var(--green); white-space:nowrap; min-width:80px; text-align:right;"></span>
      <button id="reTaxFormulaAccept" style="display:none; padding:4px 14px; font-size:12px; font-weight:600; background:var(--green); color:white; border:none; border-radius:4px; cursor:pointer;" onclick="reTaxFormulaAccept()">Accept</button>
      <button id="reTaxFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:var(--gray-200); color:var(--gray-700); border:none; border-radius:4px; cursor:pointer;" onclick="reTaxFormulaCancel()">Cancel</button>
      <button id="reTaxFormulaClear" style="display:none; padding:4px 10px; font-size:11px; background:#fef2f2; color:var(--red); border:1px solid #fecaca; border-radius:4px; cursor:pointer;" onclick="reTaxFormulaClear()" title="Remove override, revert to auto-calc">Clear</button>
    </div>

    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
      <div>
        <div style="font-size:11px; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.5px;">Real Estate Tax Calculation</div>
        <div style="font-size:12px; color:var(--gray-400); margin-top:2px;">BBL: ${d.bbl || 'N/A'} | Tax Class: ${d.tax_class || '2'} | Source: ${d.source || 'N/A'}${d.year ? ' | Year: ' + d.year : ''}</div>
      </div>
      <div style="display:flex; gap:8px; align-items:center;">
        <button onclick="refreshDOFData()" style="padding:6px 14px; background:var(--primary); color:#fff; border:none; border-radius:4px; cursor:pointer; font-size:12px;">
          Refresh DOF
        </button>
        <a href="https://propertyinformationportal.nyc.gov/parcels/parcel/${(d.bbl || '').replace(/-/g, '')}" target="_blank" rel="noopener"
           style="padding:6px 14px; background:#f59e0b; color:#fff; border:none; border-radius:4px; cursor:pointer; font-size:12px; text-decoration:none; font-weight:500;">
          Verify on DOF
        </a>
      </div>
    </div>

    <table style="width:100%; border-collapse:collapse; font-size:13px;">
      <colgroup>
        <col style="width:32%">
        <col style="width:18%">
        <col style="width:18%">
        <col style="width:32%">
      </colgroup>

      <!-- 1st Half Header -->
      <tr style="background:var(--gray-100); border-bottom:2px solid var(--gray-300);">
        <td colspan="4" style="padding:8px 10px; font-weight:700; font-size:13px;">1ST HALF — Current Fiscal Year (Jul–Dec)</td>
      </tr>

      <tr>
        <td style="${cellLabel}">Transitional AV (Prior Year)</td>
        <td style="${cellEdit}"><input type="text" id="re_av" data-raw="${d.assessed_value}" value="${fmtDollarInput(d.assessed_value)}" onfocus="reTaxFocus(this)" onblur="reTaxBlurDollar(this)" style="${inputDollar}"></td>
        ${fxCell('re_h1_tax', fmtD(d.first_half_tax), '= (re_av × re_rate) / 2', '1st Half Tax')}
        <td style="${cellNote}">= (re_av × re_rate) / 2</td>
      </tr>
      <tr>
        <td style="${cellLabel}">Tax Rate</td>
        <td style="${cellEdit}"><input type="text" id="re_rate" data-raw="${d.tax_rate}" value="${fmtRate(d.tax_rate)}" onfocus="reTaxFocus(this)" onblur="reTaxBlurRate(this)" style="${inputRate}"></td>
        <td style="${cellNote}" colspan="2"></td>
      </tr>

      <!-- 2nd Half Header -->
      <tr style="background:var(--gray-100); border-bottom:2px solid var(--gray-300); border-top:2px solid var(--gray-300);">
        <td colspan="4" style="padding:8px 10px; font-weight:700; font-size:13px;">2ND HALF — Next Fiscal Year (Jan–Jun)</td>
      </tr>

      <tr>
        <td style="${cellLabel}">Transitional AV (Current)</td>
        <td style="${cellEdit}"><input type="text" id="re_av2" data-raw="${d.est_assessed_value}" value="${fmtDollarInput(d.est_assessed_value)}" onfocus="reTaxFocus(this)" onblur="reTaxBlurDollar(this)" style="${inputDollar}"></td>
        ${fxCell('re_h2_tax', fmtD(d.second_half_tax), '= (re_av2 × re_est_rate) / 2', '2nd Half Tax')}
        <td style="${cellNote}">= (re_av2 × re_est_rate) / 2</td>
      </tr>
      <tr>
        <td style="${cellLabel}">Trans AV Change</td>
        ${fxCell('re_trans_pct', d.prior_trans_av > 0 ? fmtPct((d.est_assessed_value / d.assessed_value) - 1) : '\u2014', '= re_av2 / re_av - 1', 'Trans AV Change')}
        <td style="${cellNote}" colspan="2">= re_av2 / re_av - 1</td>
      </tr>
      <tr>
        <td style="${cellLabel}">Estimated Tax Rate</td>
        <td style="${cellEdit}"><input type="text" id="re_est_rate" data-raw="${d.est_tax_rate}" value="${fmtRate(d.est_tax_rate)}" onfocus="reTaxFocus(this)" onblur="reTaxBlurRate(this)" style="${inputRate}"></td>
        <td style="${cellNote}" colspan="2"></td>
      </tr>

      <!-- Gross -->
      <tr style="background:var(--gray-100); border-top:2px solid var(--gray-300);">
        <td style="padding:10px; font-weight:700; font-size:14px;">GROSS TAX LIABILITY</td>
        <td style="text-align:right; padding:10px; font-weight:700; font-size:14px; background:#f0faf0; cursor:pointer;" id="re_gross" data-formula="= re_h1_tax + re_h2_tax" data-label="Gross Tax" onclick="reTaxFxClick(this)" tabindex="0"><span class="re-fx-val">${fmtD(d.gross_tax)}</span>${fxBadge}</td>
        <td style="${cellNote}" colspan="2">= re_h1_tax + re_h2_tax</td>
      </tr>

      <!-- Exemptions Header -->
      <tr style="background:var(--gray-100); border-top:2px solid var(--gray-300); border-bottom:2px solid var(--gray-300);">
        <td style="padding:8px 10px; font-weight:700; font-size:13px;">EXEMPTIONS & ABATEMENTS</td>
        <td style="${thStyle} text-align:center;">Growth %</td>
        <td style="${thStyle} text-align:right;">Current Year</td>
        <td style="${thStyle} text-align:right;">Budget Year</td>
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
    html += `<tr>
      <td style="${cellLabel}">${r.label} <span style="font-size:10px; color:var(--gray-400);">${r.gl}</span></td>
      <td style="${cellEdit} text-align:center;"><input type="text" id="re_ex_${r.key}_growth" data-raw="${e.growth_pct}" value="${e.growth_pct ? fmtPct(e.growth_pct) : '0.00%'}" onfocus="reTaxFocus(this)" onblur="reTaxBlurPct(this)" style="${inputRate}"></td>
      <td style="${cellEdit}"><input type="text" id="re_ex_${r.key}_current" data-raw="${e.current_year}" value="${fmtDollarInput(e.current_year)}" onfocus="reTaxFocus(this)" onblur="reTaxBlurDollar(this)" style="${inputDollar}"></td>
      <td style="${cellCalc} cursor:pointer;" id="re_ex_${r.key}_budget" data-formula="= current × (1 + growth)" data-label="${r.label} Budget" onclick="reTaxFxClick(this)" tabindex="0"><span class="re-fx-val">${fmtD(e.budget_year)}</span>${fxBadge}</td>
    </tr>`;
  });

  html += `
      <tr style="border-top:2px solid var(--gray-300);">
        <td style="padding:10px; font-weight:700;">TOTAL EXEMPTIONS</td>
        <td></td>
        <td style="text-align:right; padding:10px; font-weight:600; cursor:pointer; background:#f0faf0;" id="re_ex_total_current" data-formula="= SUM(current exemptions)" data-label="Total Exemptions (Current)" onclick="reTaxFxClick(this)" tabindex="0"><span class="re-fx-val">${fmtD(d.total_exemptions_current)}</span>${fxBadge}</td>
        <td style="text-align:right; padding:10px; font-weight:600; cursor:pointer; background:#f0faf0;" id="re_ex_total_budget" data-formula="= SUM(budget exemptions)" data-label="Total Exemptions (Budget)" onclick="reTaxFxClick(this)" tabindex="0"><span class="re-fx-val">${fmtD(d.total_exemptions_budget)}</span>${fxBadge}</td>
      </tr>

      <!-- Net Tax -->
      <tr style="border-top:3px solid var(--gray-400); background:#f0faf0;">
        <td style="padding:12px 10px; font-weight:700; font-size:15px;">NET TAX LIABILITY</td>
        <td style="text-align:right; padding:12px 10px; font-weight:700; font-size:15px; background:#f0faf0; cursor:pointer;" id="re_net" data-formula="= re_gross - re_ex_total_budget" data-label="Net Tax Liability" onclick="reTaxFxClick(this)" tabindex="0"><span class="re-fx-val">${fmtD(d.net_tax)}</span>${fxBadge}</td>
        <td style="${cellNote}" colspan="2">= re_gross - re_ex_total_budget</td>
      </tr>
    </table>

    <div style="margin-top:12px; display:flex; gap:12px; align-items:center;">
      <button onclick="saveRETaxes()" style="padding:8px 20px; background:var(--green, #22c55e); color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:13px; font-weight:600;">
        Save RE Taxes
      </button>
      <span id="reTaxSaveStatus" style="font-size:12px; color:var(--gray-400);"></span>
    </div>
  </div>`;

  contentDiv.innerHTML = html;
}

// RE Taxes input focus/blur helpers — show raw on focus, formatted on blur
function reTaxFocus(el) {
  el.value = el.dataset.raw || '';
  el.select();
}
function reTaxBlurDollar(el) {
  const v = parseFloat(el.value) || 0;
  el.dataset.raw = v;
  el.value = '$' + Math.round(v).toLocaleString();
  reCalcTaxes();
}
function reTaxBlurRate(el) {
  let v = parseFloat(el.value) || 0;
  if (v > 1) v = v / 100;  // user typed 9.6 meaning 9.6%
  el.dataset.raw = v;
  el.value = (v * 100).toFixed(4) + '%';
  reCalcTaxes();
}
function reTaxBlurPct(el) {
  let v = parseFloat(el.value) || 0;
  if (v > 1) v = v / 100;  // user typed 3 meaning 3%
  el.dataset.raw = v;
  el.value = (v * 100).toFixed(2) + '%';
  reCalcTaxes();
}

// ── RE Taxes formula bar interaction (matches FA grid behavior) ────────
let _activeReTaxFxCell = null;
let _reTaxFormulaOriginal = '';

function _showReTaxButtons(show, hasOverride) {
  ['reTaxFormulaPreview','reTaxFormulaAccept','reTaxFormulaCancel'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = show ? 'inline-block' : 'none';
  });
  const clearBtn = document.getElementById('reTaxFormulaClear');
  if (clearBtn) clearBtn.style.display = (show && hasOverride) ? 'inline-block' : 'none';
}

// ── Build live formula with actual cell values (real math, no words) ──
function _buildReTaxFormula(id) {
  const av1 = reRaw('re_av');
  const rate = reRaw('re_rate');
  const av2 = reRaw('re_av2');
  const estRate = reRaw('re_est_rate');
  const h1 = av1 * rate / 2;
  const h2 = av2 * estRate / 2;
  const gross = h1 + h2;

  if (id === 're_h1_tax') return '= ' + av1 + ' * ' + rate + ' / 2';
  if (id === 're_h2_tax') return '= ' + av2 + ' * ' + estRate + ' / 2';
  if (id === 're_trans_pct') return av1 > 0 ? '= ' + av2 + ' / ' + av1 + ' - 1' : '= 0';
  if (id === 're_gross') return '= ' + Math.round(h1) + ' + ' + Math.round(h2);

  // Exemption budget formulas: = current * (1 + growth)
  const exKeys = ['veteran','sche','star','coop_abatement'];
  for (const k of exKeys) {
    if (id === 're_ex_' + k + '_budget') {
      const cur = reRaw('re_ex_' + k + '_current');
      const g = reRaw('re_ex_' + k + '_growth');
      return '= ' + cur + ' * (1 + ' + g + ')';
    }
  }

  // Total exemptions
  if (id === 're_ex_total_current') {
    const vals = exKeys.map(k => reRaw('re_ex_' + k + '_current'));
    return '= ' + vals.join(' + ');
  }
  if (id === 're_ex_total_budget') {
    const vals = exKeys.map(k => {
      const el = document.getElementById('re_ex_' + k + '_budget');
      if (!el) return 0;
      const span = el.querySelector('.re-fx-val');
      return span ? parseFloat(span.textContent.replace(/[$,]/g, '')) || 0 : 0;
    });
    return '= ' + vals.map(v => Math.round(v)).join(' + ');
  }

  // Net tax
  if (id === 're_net') {
    const grossVal = Math.round(gross);
    const totalEx = exKeys.reduce((sum, k) => {
      const el = document.getElementById('re_ex_' + k + '_budget');
      if (!el) return sum;
      const span = el.querySelector('.re-fx-val');
      return sum + (span ? parseFloat(span.textContent.replace(/[$,]/g, '')) || 0 : 0);
    }, 0);
    return '= ' + grossVal + ' - ' + Math.round(totalEx);
  }

  return '';
}

// ── reTaxFxClick: populate formula bar with live math ─────────────────
function reTaxFxClick(el) {
  // Deselect previous cell
  if (_activeReTaxFxCell && _activeReTaxFxCell !== el) {
    _activeReTaxFxCell.style.border = '';
    _activeReTaxFxCell.style.borderRadius = '';
    _activeReTaxFxCell.style.background = '';
  }
  _activeReTaxFxCell = el;
  const bar = document.getElementById('reTaxFormulaBar');
  const label = document.getElementById('reTaxFormulaLabel');
  if (!bar || !label) return;

  // Label shows cell name
  label.textContent = el.dataset.label || el.id;
  label.style.display = 'inline';
  bar.style.display = 'block';

  // Bar shows actual math formula with real numbers, or override value
  if (el.dataset.override === 'true') {
    bar.value = el.dataset.overrideVal || '';
  } else {
    bar.value = _buildReTaxFormula(el.id);
  }
  _reTaxFormulaOriginal = bar.value;
  const hasOverride = (el.dataset.override === 'true');
  _showReTaxButtons(true, hasOverride);
  reTaxFormulaPreview();

  // Highlight active cell
  el.style.border = '2px solid var(--blue)';
  el.style.borderRadius = '4px';
  el.style.background = '#ecfdf5';

  // Focus bar
  bar.focus({ preventScroll: true });
  bar.setSelectionRange(bar.value.length, bar.value.length);
}

// ── Live preview (mirrors FA formulaBarPreview) ──────────────────────
function reTaxFormulaPreview() {
  const bar = document.getElementById('reTaxFormulaBar');
  const preview = document.getElementById('reTaxFormulaPreview');
  if (!bar || !preview || !_activeReTaxFxCell) return;

  const typed = bar.value.trim();
  if (!typed) {
    preview.style.display = 'none';
    const hasOverride = (_activeReTaxFxCell.dataset.override === 'true');
    _showReTaxButtons(hasOverride, hasOverride);
    return;
  }

  const result = safeEvalFormula(typed);
  const isChanged = typed !== _reTaxFormulaOriginal;
  if (result !== null) {
    preview.textContent = '= $' + Math.round(result).toLocaleString();
    preview.style.color = isChanged ? '#059669' : 'var(--green)';
  } else if (/^[\d$,.\-\s]+$/.test(typed)) {
    const num = parseFloat(typed.replace(/[$,]/g, '')) || 0;
    preview.textContent = '= $' + Math.round(num).toLocaleString();
    preview.style.color = isChanged ? '#2563eb' : 'var(--blue)';
  } else {
    // Non-evaluable formula — show current cell value
    const valSpan = _activeReTaxFxCell.querySelector('.re-fx-val');
    preview.textContent = valSpan ? valSpan.textContent : '';
    preview.style.color = 'var(--green)';
  }
  preview.style.display = 'inline-block';
  const hasOverride = (_activeReTaxFxCell.dataset.override === 'true');
  _showReTaxButtons(true, hasOverride || isChanged);
}

// ── Accept (mirrors FA formulaBarAccept) ─────────────────────────────
function reTaxFormulaAccept() {
  const bar = document.getElementById('reTaxFormulaBar');
  if (!bar || !_activeReTaxFxCell) return;
  const el = _activeReTaxFxCell;
  const typed = bar.value.trim();
  if (typed === _reTaxFormulaOriginal) { reTaxFormulaCancel(); return; }

  const formulaResult = safeEvalFormula(typed);
  const numericVal = parseFloat(typed.replace(/[$,]/g, '')) || null;

  if (formulaResult !== null && (typed.startsWith('=') || /[+\-*\/()]/.test(typed))) {
    // User typed a formula — evaluate it, store as override
    const rounded = Math.round(formulaResult);
    const valSpan = el.querySelector('.re-fx-val');
    if (valSpan) valSpan.textContent = '$' + rounded.toLocaleString();
    el.dataset.override = 'true';
    el.dataset.overrideVal = typed;
    // Badge → blue fx (formula override)
    const badge = el.querySelector('.re-fx-badge');
    if (badge) { badge.textContent = 'fx'; badge.style.background = '#dbeafe'; badge.style.color = 'var(--blue)'; badge.style.borderColor = 'var(--blue)'; }
  } else if (typed !== '' && numericVal !== null && /^[\d$,.\-\s]+$/.test(typed)) {
    // User typed a plain number — store as override
    const rounded = Math.round(numericVal);
    const valSpan = el.querySelector('.re-fx-val');
    if (valSpan) valSpan.textContent = '$' + rounded.toLocaleString();
    el.dataset.override = 'true';
    el.dataset.overrideVal = typed;
    // Badge → pencil (manual override)
    const badge = el.querySelector('.re-fx-badge');
    if (badge) { badge.textContent = '\u270e'; badge.style.background = '#fef3c7'; badge.style.color = '#d97706'; badge.style.borderColor = '#d97706'; }
  } else if (typed === '' || typed.toLowerCase() === 'auto' || typed.toLowerCase() === 'formula') {
    // Revert to auto formula
    el.dataset.override = 'false';
    el.dataset.overrideVal = '';
    const badge = el.querySelector('.re-fx-badge');
    if (badge) { badge.textContent = 'fx'; badge.style.background = '#4ade80'; badge.style.color = '#fff'; badge.style.borderColor = ''; }
    reCalcTaxes();
  }

  // Green flash feedback (same as FA)
  el.style.border = '2px solid var(--green)';
  el.style.background = '#ecfdf5';
  const preview = document.getElementById('reTaxFormulaPreview');
  if (preview) {
    preview.textContent = '\u2713 Accepted';
    preview.style.color = 'var(--green)';
    preview.style.display = 'inline-block';
  }
  _showReTaxButtons(false, false);
  _reTaxFormulaOriginal = bar.value.trim();
  setTimeout(() => {
    el.style.border = '';
    el.style.borderRadius = '';
    el.style.background = '';
    if (preview) preview.style.display = 'none';
  }, 1200);

  // Auto-save after override
  saveRETaxes();
}

// ── Cancel (mirrors FA formulaBarCancel) ─────────────────────────────
function reTaxFormulaCancel() {
  const bar = document.getElementById('reTaxFormulaBar');
  if (bar) bar.value = _reTaxFormulaOriginal;
  _showReTaxButtons(false, false);
  const preview = document.getElementById('reTaxFormulaPreview');
  if (preview) preview.style.display = 'none';
  if (_activeReTaxFxCell) {
    _activeReTaxFxCell.style.border = '';
    _activeReTaxFxCell.style.borderRadius = '';
    _activeReTaxFxCell.style.background = '';
  }
}

// ── Clear: remove override, revert to auto-calc (mirrors FA formulaBarClear) ─
function reTaxFormulaClear() {
  if (!_activeReTaxFxCell) return;
  const el = _activeReTaxFxCell;
  el.dataset.override = 'false';
  el.dataset.overrideVal = '';
  const badge = el.querySelector('.re-fx-badge');
  if (badge) { badge.textContent = 'fx'; badge.style.background = '#4ade80'; badge.style.color = '#fff'; badge.style.borderColor = ''; }
  reCalcTaxes();
  const bar = document.getElementById('reTaxFormulaBar');
  if (bar) bar.value = el.dataset.formula || '';
  _reTaxFormulaOriginal = bar ? bar.value : '';
  _showReTaxButtons(false, false);
  el.style.border = '';
  el.style.borderRadius = '';
  el.style.background = '';
  saveRETaxes();
}

// ── Keyboard: Enter = Accept, Escape = Cancel (same as FA) ──────────
function reTaxFormulaKeydown(e) {
  if (e.key === 'Enter') {
    e.preventDefault();
    reTaxFormulaAccept();
  } else if (e.key === 'Escape') {
    e.preventDefault();
    reTaxFormulaCancel();
  }
}

// ── Deselect when clicking outside (same as FA fxCellBlur pattern) ───
document.addEventListener('click', function(e) {
  if (!_activeReTaxFxCell) return;
  const wrap = document.getElementById('reTaxFormulaBarWrap');
  if (_activeReTaxFxCell.contains(e.target)) return;
  if (wrap && wrap.contains(e.target)) return;
  // Clicked outside — deselect
  _activeReTaxFxCell.style.border = '';
  _activeReTaxFxCell.style.borderRadius = '';
  _activeReTaxFxCell.style.background = '';
  _activeReTaxFxCell = null;
  const bar = document.getElementById('reTaxFormulaBar');
  const label = document.getElementById('reTaxFormulaLabel');
  const preview = document.getElementById('reTaxFormulaPreview');
  if (bar) { bar.value = ''; bar.placeholder = 'Click a green formula cell to view its formula...'; }
  if (label) label.style.display = 'none';
  if (preview) preview.style.display = 'none';
  _showReTaxButtons(false, false);
});

// Helper: read raw numeric value from data-raw attribute (inputs show formatted text)
function reRaw(id) {
  const el = document.getElementById(id);
  return el ? (parseFloat(el.dataset.raw) || 0) : 0;
}
// Helper: set value in a calculated cell — targets the .re-fx-val span to preserve the fx badge
function reSetCalc(id, text) {
  const cell = document.getElementById(id);
  if (!cell) return;
  const span = cell.querySelector('.re-fx-val');
  if (span) { span.textContent = text; } else { cell.textContent = text; }
  // Update formula bar result if this cell is currently selected
  if (_activeReTaxFxCell === cell) {
    const result = document.getElementById('reTaxFormulaResult');
    if (result) result.textContent = text;
  }
}

// Live recalculation of RE Taxes when inputs change
function reCalcTaxes() {
  const av1 = reRaw('re_av');
  const rate = reRaw('re_rate');
  const av2 = reRaw('re_av2');
  const estRate = reRaw('re_est_rate');

  const h1 = av1 * rate / 2;
  const h2 = av2 * estRate / 2;
  const gross = h1 + h2;

  // Show trans AV change %
  if (av1 > 0) {
    reSetCalc('re_trans_pct', ((av2 / av1 - 1) * 100).toFixed(2) + '%');
  }

  reSetCalc('re_h1_tax', '$' + Math.round(h1).toLocaleString());
  reSetCalc('re_h2_tax', '$' + Math.round(h2).toLocaleString());
  reSetCalc('re_gross', '$' + Math.round(gross).toLocaleString());

  let totalCurrent = 0, totalBudget = 0;
  ['veteran','sche','star','coop_abatement'].forEach(key => {
    const growth = reRaw('re_ex_' + key + '_growth');
    const current = reRaw('re_ex_' + key + '_current');
    const budget = current * (1 + growth);
    reSetCalc('re_ex_' + key + '_budget', '$' + Math.round(budget).toLocaleString());
    totalCurrent += current;
    totalBudget += budget;
  });

  reSetCalc('re_ex_total_current', '$' + Math.round(totalCurrent).toLocaleString());
  reSetCalc('re_ex_total_budget', '$' + Math.round(totalBudget).toLocaleString());
  reSetCalc('re_net', '$' + Math.round(gross - totalBudget).toLocaleString());
}

// Save RE Taxes overrides to server
async function saveRETaxes() {
  const overrides = {
    first_half_av: reRaw('re_av'),
    tax_rate: reRaw('re_rate'),
    second_half_av: reRaw('re_av2'),
    est_tax_rate: reRaw('re_est_rate'),
    veteran_growth: reRaw('re_ex_veteran_growth'),
    veteran_current: reRaw('re_ex_veteran_current'),
    sche_growth: reRaw('re_ex_sche_growth'),
    sche_current: reRaw('re_ex_sche_current'),
    star_growth: reRaw('re_ex_star_growth'),
    star_current: reRaw('re_ex_star_current'),
    abatement_growth: reRaw('re_ex_coop_abatement_growth'),
    abatement_current: reRaw('re_ex_coop_abatement_current'),
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
      renderRETaxesTab(document.getElementById('sheetContent'));
      if (statusEl) { statusEl.textContent = '✓ DOF data refreshed'; statusEl.style.color = 'var(--green, #22c55e)'; }
    }
  } catch (e) {
    if (statusEl) { statusEl.textContent = 'Error: ' + e.message; statusEl.style.color = 'var(--red, #ef4444)'; }
  }
}

// ── Summary tab formula bar ───────────────────────────────────────────
let _activeSumFxCell = null;
let _sumFormulaOriginal = '';
let _sumCatData = {};

function _showSumButtons(show) {
  ['sumFormulaPreview','sumFormulaAccept','sumFormulaCancel'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = show ? 'inline-block' : 'none';
  });
}

function _buildSumFormula(cellId) {
  const data = _sumCatData[cellId];
  if (!data) return '';
  const field = data.field;
  const lines = data.lines;

  if (field === 'var') {
    const budget = Math.round(lines.reduce((s, l) => s + (l.current_budget || 0), 0));
    const forecast = Math.round(lines.reduce((s, l) => s + faComputeForecast(l), 0));
    return '= ' + budget + ' - ' + forecast;
  }
  if (field === 'pct') {
    const budget = Math.round(lines.reduce((s, l) => s + (l.current_budget || 0), 0));
    const forecast = Math.round(lines.reduce((s, l) => s + faComputeForecast(l), 0));
    return forecast ? '= (' + budget + ' - ' + forecast + ') / ' + forecast : '= 0';
  }

  // For budget/proposed/prior, show SUM of GL line values
  const getVal = (l) => {
    if (field === 'prior') return Math.round(l.prior_year || 0);
    if (field === 'budget') return Math.round(l.current_budget || 0);
    if (field === 'proposed') {
      const f = faComputeForecast(l);
      return Math.round(l.proposed_budget || (f * (1 + (l.increase_pct || 0))));
    }
    return 0;
  };

  const vals = lines.map(getVal).filter(v => v !== 0);
  if (vals.length === 0) return '= 0';
  if (vals.length <= 10) return '= ' + vals.join(' + ');
  return '= SUM(' + vals.length + ' GL lines) = ' + vals.reduce((a, b) => a + b, 0);
}

function sumFxClick(el) {
  if (_activeSumFxCell && _activeSumFxCell !== el) {
    _activeSumFxCell.style.border = '';
    _activeSumFxCell.style.borderRadius = '';
    _activeSumFxCell.style.background = '';
  }
  _activeSumFxCell = el;
  const bar = document.getElementById('sumFormulaBar');
  const label = document.getElementById('sumFormulaLabel');
  if (!bar || !label) return;

  label.textContent = el.dataset.label || el.id;
  label.style.display = 'inline';

  bar.value = _buildSumFormula(el.id);
  _sumFormulaOriginal = bar.value;
  _showSumButtons(true);
  _sumFormulaPreview();

  el.style.border = '2px solid var(--blue)';
  el.style.borderRadius = '4px';
  el.style.background = '#ecfdf5';

  // Populate the detail breakdown
  const detail = document.getElementById('sumFormulaDetail');
  if (detail) {
    detail.innerHTML = _buildSumDetail(el.id);
    detail.style.display = detail.innerHTML ? 'block' : 'none';
  }

  bar.focus({ preventScroll: true });
  bar.setSelectionRange(bar.value.length, bar.value.length);
}

// Build GL-level breakdown for detail area
function _buildSumDetail(cellId) {
  const data = _sumCatData[cellId];
  if (!data || !data.lines || data.lines.length === 0) return '';
  const field = data.field;
  const lines = data.lines;

  // For var/pct, describe what's being compared
  if (field === 'var') return '<b>$ Variance</b> = Curr Budget \u2212 12 Mo Forecast';
  if (field === 'pct') return '<b>% Change</b> = (Curr Budget \u2212 12 Mo Forecast) / 12 Mo Forecast';
  if (field && field.startsWith('sub_')) {
    const subField = field.replace('sub_', '');
    if (subField === 'var') return '<b>$ Variance</b> = Curr Budget \u2212 12 Mo Forecast';
    if (subField === 'pct') return '<b>% Change</b> = (Curr Budget \u2212 12 Mo Forecast) / 12 Mo Forecast';
  }

  // For value columns, show each GL line
  const getVal = (l) => {
    if (field === 'prior' || field === 'sub_prior') return Math.round(l.prior_year || 0);
    if (field === 'budget' || field === 'sub_budget') return Math.round(l.current_budget || 0);
    if (field === 'proposed' || field === 'sub_proposed') {
      const f = faComputeForecast(l);
      return Math.round(l.proposed_budget || (f * (1 + (l.increase_pct || 0))));
    }
    return 0;
  };

  const items = lines.map(l => ({
    gl: l.gl_code,
    desc: l.description || '',
    val: getVal(l)
  })).filter(x => x.val !== 0);

  if (items.length === 0) return '<span style="color:var(--gray-400);">All GL lines are $0</span>';

  const sheet = lines[0] && lines[0].sheet_name ? lines[0].sheet_name : '';
  let html = '<b>' + (sheet ? sheet + ' \u2014 ' : '') + items.length + ' GL line' + (items.length !== 1 ? 's' : '') + ':</b> ';
  html += items.map(x => '<span style="white-space:nowrap;">' + x.gl + ' ' + x.desc + ' <b>$' + x.val.toLocaleString() + '</b></span>').join(' + ');
  return html;
}

function _sumFormulaPreview() {
  const bar = document.getElementById('sumFormulaBar');
  const preview = document.getElementById('sumFormulaPreview');
  if (!bar || !preview || !_activeSumFxCell) return;
  const typed = bar.value.trim();
  if (!typed) { preview.style.display = 'none'; return; }
  const result = safeEvalFormula(typed);
  if (result !== null) {
    const field = (_sumCatData[_activeSumFxCell.id] || {}).field || '';
    if (field === 'pct') {
      preview.textContent = '= ' + result.toFixed(1) + '%';
    } else {
      preview.textContent = '= $' + Math.round(result).toLocaleString();
    }
    preview.style.color = 'var(--green)';
    preview.style.display = 'inline-block';
  } else {
    preview.style.display = 'none';
  }
}

function sumFormulaCancel() {
  const bar = document.getElementById('sumFormulaBar');
  if (bar) bar.value = _sumFormulaOriginal;
  _showSumButtons(false);
  const preview = document.getElementById('sumFormulaPreview');
  if (preview) preview.style.display = 'none';
  const detail = document.getElementById('sumFormulaDetail');
  if (detail) detail.style.display = 'none';
  if (_activeSumFxCell) {
    _activeSumFxCell.style.border = '';
    _activeSumFxCell.style.borderRadius = '';
    _activeSumFxCell.style.background = '';
  }
}

document.addEventListener('click', function(e) {
  if (!_activeSumFxCell) return;
  const wrap = document.getElementById('sumFormulaBarWrap');
  if (_activeSumFxCell.contains(e.target)) return;
  if (wrap && wrap.contains(e.target)) return;
  _activeSumFxCell.style.border = '';
  _activeSumFxCell.style.borderRadius = '';
  _activeSumFxCell.style.background = '';
  _activeSumFxCell = null;
  const bar = document.getElementById('sumFormulaBar');
  const label = document.getElementById('sumFormulaLabel');
  const preview = document.getElementById('sumFormulaPreview');
  const detail = document.getElementById('sumFormulaDetail');
  if (bar) { bar.value = ''; bar.placeholder = 'Click any fx cell to view its formula...'; }
  if (label) label.style.display = 'none';
  if (preview) preview.style.display = 'none';
  if (detail) detail.style.display = 'none';
  _showSumButtons(false);
});

function renderBudgetSummary(contentDiv) {
  const thStyle = 'text-align:right; padding:10px 12px; white-space:nowrap;';
  _sumCatData = {};

  // Get audit summary years (up to 2 most recent)
  const auditSummary = (window._data && window._data.audit && window._data.audit.summary_years) ? window._data.audit.summary_years : {};
  const auditYearKeys = Object.keys(auditSummary).sort().reverse().slice(0, 2).reverse(); // chronological, max 2
  const hasAudit = auditYearKeys.length > 0;

  // Track custom rows added by FA
  if (!window._customSummaryRows) window._customSummaryRows = [];

  let showZeroRows = window._showZeroSummaryRows || false;

  const fxBadge = '<span style="display:inline-block; background:#4ade80; color:#fff; font-size:8px; font-weight:700; padding:1px 3px; border-radius:3px; margin-left:3px; vertical-align:middle;">fx</span>';
  let _cellIdx = 0;
  function sfx(val, label, field, lines, extraStyle) {
    const id = 'sumfx_' + (++_cellIdx);
    _sumCatData[id] = {field: field, lines: lines};
    return '<td style="text-align:right; padding:10px 12px; cursor:pointer;' + (extraStyle || '') + '" id="' + id + '" data-label="' + label.replace(/"/g, '&quot;') + '" onclick="sumFxClick(this)">' + val + fxBadge + '</td>';
  }

  let html = '<div style="margin-bottom:12px; display:flex; align-items:center; gap:12px; flex-wrap:wrap;">' +
    '<span style="font-size:14px; color:var(--gray-500);">Executive budget overview — all figures roll up from detail sheets</span>' +
    '<button onclick="toggleZeroRows()" id="zeroToggleBtn" style="margin-left:auto; padding:4px 12px; font-size:11px; border:1px solid var(--gray-300); border-radius:4px; background:white; cursor:pointer;">' +
    (showZeroRows ? 'Hide Empty Rows' : 'Show All Rows') + '</button></div>';

  // Formula bar
  html += '<div id="sumFormulaBarWrap" style="display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200); border-radius:8px; margin-bottom:12px;">' +
    '<span style="font-size:11px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>' +
    '<span id="sumFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:var(--gray-600); white-space:nowrap; min-width:120px;"></span>' +
    '<input id="sumFormulaBar" type="text" readonly placeholder="Click any fx cell to view its formula..." style="flex:1; padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:monospace; background:white;" onkeydown="if(event.key===\'Escape\')sumFormulaCancel()">' +
    '<span id="sumFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:var(--green); white-space:nowrap; min-width:80px; text-align:right;"></span>' +
    '<button id="sumFormulaAccept" style="display:none;">OK</button>' +
    '<button id="sumFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:var(--gray-200); color:var(--gray-700); border:none; border-radius:4px; cursor:pointer;" onclick="sumFormulaCancel()">Close</button>' +
    '</div>' +
    '<div id="sumFormulaDetail" style="display:none; padding:6px 16px 8px; font-size:11px; color:var(--gray-600); line-height:1.6; background:#f8fafc; border:1px solid var(--gray-200); border-top:none; border-radius:0 0 8px 8px; margin-top:-13px; margin-bottom:12px; max-height:80px; overflow-y:auto;"></div>';

  // Table header
  html += '<table id="summaryTable" style="width:100%; border-collapse:collapse; font-size:13px;">' +
    '<thead><tr style="background:var(--gray-100); font-size:10px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
    '<th style="text-align:left; padding:10px 12px; width:28%;">Category</th>';

  // Audit year columns
  auditYearKeys.forEach(y => {
    html += '<th style="' + thStyle + '">FY' + y + '<br>Audit</th>';
  });

  html += '<th style="' + thStyle + '">Prior Year<br>Actual</th>' +
    '<th style="' + thStyle + '">Current<br>Budget</th>' +
    '<th style="' + thStyle + '">Proposed<br>Budget</th>' +
    '<th style="' + thStyle + '">$<br>Variance</th>' +
    '<th style="' + thStyle + '">%<br>Change</th>' +
    '</tr></thead><tbody>';

  let totalIncome = {prior:0, budget:0, proposed:0, forecast:0};
  let totalExpense = {prior:0, budget:0, proposed:0, forecast:0};
  let auditTotalIncome = auditYearKeys.map(() => 0);
  let auditTotalExpense = auditYearKeys.map(() => 0);

  SUMMARY_ROWS.forEach((sr, idx) => {
    const sheetLines = allSheets[sr.sheet] || [];
    let lines = sheetLines;
    if (sr.rowRange) {
      lines = sheetLines.filter(l => l.row_num >= sr.rowRange[0] && l.row_num <= sr.rowRange[1]);
    }
    let prior = 0, budget = 0, proposed = 0, forecastTotal = 0;
    lines.forEach(l => {
      prior += l.prior_year || 0;
      budget += l.current_budget || 0;
      const forecast = faComputeForecast(l);
      forecastTotal += forecast;
      proposed += l.proposed_budget || (forecast * (1 + (l.increase_pct || 0)));
    });

    // Add custom rows for this category
    window._customSummaryRows.forEach(cr => {
      if (cr.summaryLabel === sr.label) {
        proposed += cr.amount;
      }
    });

    // Get audit amounts for this summary row
    const auditAmounts = auditYearKeys.map(y => {
      return (auditSummary[y] && auditSummary[y][sr.label]) ? auditSummary[y][sr.label] : 0;
    });

    const allZero = prior === 0 && budget === 0 && proposed === 0 && auditAmounts.every(a => a === 0);
    if (allZero && !showZeroRows) return; // skip zero rows

    const variance = budget - forecastTotal;
    const pctChange = forecastTotal ? ((budget - forecastTotal) / forecastTotal) : 0;
    const varColor = sr.type === 'income'
      ? (variance >= 0 ? 'var(--green)' : 'var(--red)')
      : (variance >= 0 ? 'var(--red)' : 'var(--green)');

    if (sr.type === 'income') {
      totalIncome.prior += prior; totalIncome.budget += budget; totalIncome.proposed += proposed; totalIncome.forecast += forecastTotal;
      auditAmounts.forEach((a, i) => auditTotalIncome[i] += a);
    } else {
      totalExpense.prior += prior; totalExpense.budget += budget; totalExpense.proposed += proposed; totalExpense.forecast += forecastTotal;
      auditAmounts.forEach((a, i) => auditTotalExpense[i] += a);
    }

    const isIncomeRow = idx === 0;
    const rowStyle = isIncomeRow ? 'font-weight:600; background:var(--blue-light, #f5efe7);' : '';
    html += '<tr style="border-bottom:1px solid var(--gray-100); ' + rowStyle + '">' +
      '<td style="padding:10px 12px;">' + sr.label + '</td>';

    auditAmounts.forEach(a => {
      html += '<td style="text-align:right; padding:10px 12px; color:var(--gray-500);">' + fmt(a) + '</td>';
    });

    html += sfx(fmt(prior), sr.label + ' / Prior Year', 'prior', lines) +
      sfx(fmt(budget), sr.label + ' / Curr Budget', 'budget', lines) +
      sfx(fmt(proposed), sr.label + ' / Proposed', 'proposed', lines) +
      sfx(fmt(variance), sr.label + ' / $ Var', 'var', lines, ' color:' + varColor + ';') +
      sfx((pctChange * 100).toFixed(1) + '%', sr.label + ' / % Chg', 'pct', lines) +
      '</tr>';

    // After last expense row, add totals
    if (idx === SUMMARY_ROWS.length - 1) {
      const tePrior = totalExpense.prior, teBudget = totalExpense.budget, teProposed = totalExpense.proposed, teForecast = totalExpense.forecast;
      const teVar = teBudget - teForecast;
      const tePct = teForecast ? ((teBudget - teForecast) / teForecast) : 0;
      // Collect all expense lines for formula
      const allExpLines = SUMMARY_ROWS.filter(r => r.type === 'expense').reduce((arr, r) => {
        let ls = allSheets[r.sheet] || [];
        if (r.rowRange) ls = ls.filter(l => l.row_num >= r.rowRange[0] && l.row_num <= r.rowRange[1]);
        return arr.concat(ls);
      }, []);

      html += '<tr style="font-weight:700; background:var(--gray-100); border-top:2px solid var(--gray-300);"><td style="padding:10px 12px;">Total Operating Expenses</td>';
      auditTotalExpense.forEach(a => { html += '<td style="text-align:right; padding:10px 12px; color:var(--gray-500);">' + fmt(a) + '</td>'; });
      html += sfx(fmt(tePrior), 'Total Expenses / Prior Year', 'prior', allExpLines) +
        sfx(fmt(teBudget), 'Total Expenses / Curr Budget', 'budget', allExpLines) +
        sfx(fmt(teProposed), 'Total Expenses / Proposed', 'proposed', allExpLines) +
        sfx(fmt(teVar), 'Total Expenses / $ Var', 'var', allExpLines, teVar >= 0 ? ' color:var(--red);' : ' color:var(--green);') +
        sfx((tePct * 100).toFixed(1) + '%', 'Total Expenses / % Chg', 'pct', allExpLines) +
        '</tr>';

      // NOI
      const noiPrior = totalIncome.prior - tePrior;
      const noiBudget = totalIncome.budget - teBudget;
      const noiProposed = totalIncome.proposed - teProposed;
      const noiForecast = totalIncome.forecast - teForecast;
      const noiVar = noiBudget - noiForecast;
      const noiPct = noiForecast ? ((noiBudget - noiForecast) / noiForecast) : 0;
      const noiColor = noiVar >= 0 ? 'var(--green)' : 'var(--red)';

      // NOI audit amounts
      const noiAudit = auditYearKeys.map((_, i) => auditTotalIncome[i] - auditTotalExpense[i]);
      // All lines for NOI formula
      const allIncLines = SUMMARY_ROWS.filter(r => r.type === 'income').reduce((arr, r) => {
        let ls = allSheets[r.sheet] || [];
        if (r.rowRange) ls = ls.filter(l => l.row_num >= r.rowRange[0] && l.row_num <= r.rowRange[1]);
        return arr.concat(ls);
      }, []);

      html += '<tr style="font-weight:700; background:var(--blue-light, #f5efe7); border-top:2px solid var(--blue);"><td style="padding:10px 12px;">Net Operating Income</td>';
      noiAudit.forEach(a => { html += '<td style="text-align:right; padding:10px 12px; color:var(--gray-500);">' + fmt(a) + '</td>'; });
      html += sfx(fmt(noiPrior), 'NOI / Prior Year', 'prior', allIncLines.concat(allExpLines)) +
        sfx(fmt(noiBudget), 'NOI / Curr Budget', 'budget', allIncLines.concat(allExpLines)) +
        sfx(fmt(noiProposed), 'NOI / Proposed', 'proposed', allIncLines.concat(allExpLines)) +
        sfx(fmt(noiVar), 'NOI / $ Var', 'var', allIncLines.concat(allExpLines), ' color:' + noiColor + ';') +
        sfx((noiPct * 100).toFixed(1) + '%', 'NOI / % Chg', 'pct', allIncLines.concat(allExpLines)) +
        '</tr>';
    }
  });

  html += '</tbody></table>';

  // Manual row add section
  const colCount = 5 + auditYearKeys.length; // category + audit cols + prior + budget + proposed + var + %
  html += '<div style="margin-top:12px; padding:12px; background:var(--gray-100); border-radius:8px;" id="addRowSection">' +
    '<button onclick="document.getElementById(\'addRowForm\').style.display=\'flex\'" style="padding:6px 14px; font-size:12px; border:1px dashed var(--gray-300); border-radius:6px; background:white; cursor:pointer; color:var(--gray-500);">+ Add Custom Row</button>' +
    '<div id="addRowForm" style="display:none; flex-wrap:wrap; gap:8px; margin-top:8px; align-items:center;">' +
    '<select id="addRowCategory" style="padding:6px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:12px;">' +
    '<option value="">Select category...</option>';
  SUMMARY_ROWS.forEach(sr => {
    html += '<option value="' + sr.label + '">' + sr.label + '</option>';
  });
  html += '</select>' +
    '<input id="addRowLabel" placeholder="Line description" style="padding:6px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:12px; width:180px;" />' +
    '<input id="addRowAmount" type="number" placeholder="Amount" style="padding:6px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:12px; width:100px;" />' +
    '<button onclick="addCustomSummaryRow()" style="padding:6px 14px; background:var(--blue); color:white; border:none; border-radius:4px; font-size:12px; cursor:pointer;">Add</button>' +
    '<button onclick="document.getElementById(\'addRowForm\').style.display=\'none\'" style="padding:6px 14px; border:1px solid var(--gray-300); border-radius:4px; font-size:12px; cursor:pointer; background:white;">Cancel</button>' +
    '</div></div>';

  contentDiv.innerHTML = html;
}

// Toggle zero rows
function toggleZeroRows() {
  window._showZeroSummaryRows = !window._showZeroSummaryRows;
  const contentDiv = document.querySelector('[data-sheet="Summary"]') || document.getElementById('sheetContent');
  if (contentDiv) renderBudgetSummary(contentDiv);
}

// Add custom summary row
function addCustomSummaryRow() {
  const cat = document.getElementById('addRowCategory').value;
  const label = document.getElementById('addRowLabel').value;
  const amount = parseFloat(document.getElementById('addRowAmount').value) || 0;
  if (!cat || !label || !amount) { alert('Fill in all fields'); return; }
  if (!window._customSummaryRows) window._customSummaryRows = [];
  window._customSummaryRows.push({summaryLabel: cat, label: label, amount: amount});
  const contentDiv = document.querySelector('[data-sheet="Summary"]') || document.getElementById('sheetContent');
  if (contentDiv) renderBudgetSummary(contentDiv);
}

function renderReadOnlySheet(sheetName, sheetLines, contentDiv) {
  const thStyle = 'text-align:right; padding:8px; white-space:nowrap;';
  let html = '<table style="width:100%; border-collapse:collapse; font-size:13px;">' +
    '<thead><tr style="background:var(--gray-100); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
    '<th style="text-align:left; padding:8px;">GL Code</th>' +
    '<th style="text-align:left; padding:8px;">Description</th>' +
    '<th style="' + thStyle + '">Prior Year<br>Actual</th>' +
    '<th style="' + thStyle + '">YTD<br>Actual</th>' +
    '<th style="' + thStyle + '">YTD<br>Budget</th>' +
    '<th style="' + thStyle + '">Approved<br>Budget</th>' +
    '<th style="' + thStyle + '">Variance</th>' +
    '</tr></thead><tbody>';

  let totals = {prior:0, ytd:0, ytdBudget:0, budget:0};
  sheetLines.forEach(l => {
    const prior = l.prior_year || 0;
    const ytd = l.ytd_actual || 0;
    const ytdBudget = l.ytd_budget || 0;
    const budget = l.current_budget || 0;
    const variance = budget - prior;
    totals.prior += prior; totals.ytd += ytd; totals.ytdBudget += ytdBudget; totals.budget += budget;
    const varColor = variance >= 0 ? 'var(--red)' : 'var(--green)';

    html += '<tr style="border-bottom:1px solid var(--gray-100);">' +
      '<td style="font-family:monospace; font-size:12px; padding:6px 8px;">' + l.gl_code + '</td>' +
      '<td style="padding:6px 8px;">' + l.description + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(prior) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(ytd) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(ytdBudget) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(budget) + '</td>' +
      '<td style="text-align:right; padding:6px 8px; color:' + varColor + ';">' + fmt(variance) + '</td></tr>';
  });

  const totalVar = totals.budget - totals.prior;
  html += '<tr style="font-weight:700; background:var(--gray-100);"><td style="padding:8px;" colspan="2">Sheet Total</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.prior) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.ytd) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.ytdBudget) + '</td>' +
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

  const detailRow = document.createElement('tr');
  detailRow.className = 'fa-invoice-detail';
  let html = '<td colspan="15" style="padding:0;"><div style="padding:12px 16px 12px 40px; background:linear-gradient(to right, #f0f4ff, #f8faff); border-left:3px solid var(--blue); border-bottom:1px solid var(--gray-200);">';
  html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">';
  html += '<span style="font-weight:600; font-size:13px; color:var(--blue);">' + glCode + ' — ' + (glGroup.gl_name || '') + '</span>';
  html += '<span style="font-size:12px; color:var(--gray-500);">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' · $' + Math.round(glGroup.total || 0).toLocaleString() + '</span>';
  html += '</div>';
  html += '<table style="width:100%; font-size:12px; border-collapse:collapse; background:white; border-radius:6px; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,0.05); table-layout:fixed;">';
  html += '<colgroup><col style="width:18%"><col style="width:22%"><col style="width:10%"><col style="width:9%"><col style="width:11%"><col style="width:8%"><col style="width:22%"></colgroup>';
  html += '<thead><tr style="background:var(--gray-100); color:var(--gray-600); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
  html += '<td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Payee</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Description</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Invoice #</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Date</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap; text-align:right;">Amount</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Check #</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap; text-align:right;">Action</div></td></tr></thead>';

  glGroup.invoices.forEach(inv => {
    const isReclassed = !!inv.reclass_to_gl;
    html += '<tr style="border-top:1px solid var(--gray-200);' + (isReclassed ? ' opacity:0.5; text-decoration:line-through;' : '') + '">';
    html += '<td><div style="padding:6px 10px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="' + ((inv.payee_name || inv.payee_code || '').replace(/"/g, '&quot;')) + '">' + (inv.payee_name || inv.payee_code || '—') + '</div></td>';
    html += '<td><div style="padding:6px 10px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-size:11px; color:var(--gray-600);" title="' + ((inv.notes || '').replace(/"/g, '&quot;')) + '">' + (inv.notes || '—') + '</div></td>';
    html += '<td><div style="padding:6px 10px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-family:monospace; font-size:11px;">' + (inv.invoice_num || '—') + '</div></td>';
    html += '<td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</div></td>';
    html += '<td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap; text-align:right; font-variant-numeric:tabular-nums;">$' + Math.round(inv.amount).toLocaleString() + '</div></td>';
    html += '<td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">' + (inv.check_num || '—') + '</div></td>';
    html += '<td style="text-align:right;">';
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

function renderEditableSheet(sheetName, sheetLines, contentDiv) {
  const NC = 15;
  const estLbl = estimateLabel();

  // Inject PM-style CSS if not already present
  if (!document.getElementById('faSheetStyle')) {
    const style = document.createElement('style');
    style.id = 'faSheetStyle';
    style.textContent = `
      .fa-grid { background:white; border-radius:12px; border:1px solid var(--gray-200); overflow:hidden; }
      .fa-grid-scroll { overflow-x:auto; max-height:75vh; overflow-y:auto; }
      .fa-grid table { width:100%; border-collapse:collapse; font-size:13px; }
      .fa-grid thead { background:var(--gray-100); position:sticky; top:0; z-index:10; }
      .fa-grid th { padding:10px 12px; text-align:left; font-weight:600; border-bottom:2px solid var(--gray-300); white-space:nowrap; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); }
      .fa-grid th.num { text-align:right; }
      .fa-grid td { padding:8px 12px; border-bottom:1px solid var(--gray-200); }
      .fa-grid td.num { text-align:right; font-variant-numeric:tabular-nums; }
      .fa-grid tbody tr:hover { background:#eef2ff; }
      .fa-grid .cat-hdr td { background:var(--blue-light, #f5efe7); font-weight:700; color:var(--blue, #5a4a3f); font-size:14px; padding:10px 12px; border-bottom:2px solid var(--blue, #5a4a3f); }
      .fa-grid .sub-row td { background:var(--gray-100); font-weight:700; border-top:2px solid var(--gray-300); }
      .fa-grid .total-row td { background:#1e3a5f; color:white; font-weight:700; font-size:14px; }
      .fa-grid .cell { width:90px; padding:5px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; text-align:right; background:#fffff0; cursor:text; }
      .fa-grid .cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px var(--blue-light, #f5efe7); }
      .fa-grid .cell-fx { background:#f0fdf4; border-color:#bbf7d0; }
      .fa-grid .cell-fx:focus { background:#ecfdf5; }
      .fa-fx { position:absolute; top:2px; right:2px; font-size:9px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:3px; padding:0 3px; cursor:pointer; user-select:none; z-index:5; }
      .fa-grid .cell-notes { text-align:left; min-width:100px; width:100%; }
      .fa-grid .cell-pct { width:60px; }
      .fa-invoice-detail td { padding:0 !important; }
      .fa-invoice-detail:hover { background:transparent !important; }
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

  html += '<div class="fa-grid"><div class="fa-grid-scroll"><table><thead><tr>' +
    '<th>GL Code</th><th>Description</th><th>Notes</th>' +
    '<th class="num">Prior Year</th><th class="num">YTD Actual</th>' +
    '<th class="num">Accrual Adj</th><th class="num">Unpaid Bills</th>' +
    '<th class="num">YTD Budget</th>' +
    '<th class="num">' + estLbl + ' Est</th><th class="num">12 Mo Forecast</th>' +
    '<th class="num">Curr Budget</th><th class="num">Inc %</th>' +
    '<th class="num">Proposed</th><th class="num">$ Var</th><th class="num">% Chg</th>' +
    '</tr></thead><tbody>';

  const catConfig = SHEET_CATEGORIES[sheetName];

  function buildLineRow(l) {
    const gl = l.gl_code;
    const prior = l.prior_year || 0;
    const ytd = l.ytd_actual || 0;
    const accrual = l.accrual_adj || 0;
    const unpaid = l.unpaid_bills || 0;
    const ytdBudget = l.ytd_budget || 0;
    const budget = l.current_budget || 0;
    const isZero = !prior && !ytd && !accrual && !unpaid && !ytdBudget && !budget && !(l.increase_pct);
    const estimate = faComputeEstimate(l);
    const forecast = faComputeForecast(l);
    const userFormula = l.proposed_formula || '';
    let proposed;
    if (userFormula) {
      const evalResult = safeEvalFormula(userFormula);
      proposed = evalResult !== null ? evalResult : (l.proposed_budget || (forecast * (1 + (l.increase_pct || 0))));
    } else {
      proposed = l.proposed_budget || (forecast * (1 + (l.increase_pct || 0)));
    }
    const variance = budget - forecast;
    const pctChange = forecast ? ((budget - forecast) / forecast) : 0;
    const incPct = ((l.increase_pct || 0) * 100).toFixed(1);
    const varColor = variance >= 0 ? 'var(--red)' : 'var(--green)';
    const reclassBadge = l.reclass_to_gl ? ' <span style="background:var(--orange-light); color:var(--orange); font-size:10px; padding:1px 5px; border-radius:8px;">R</span>' : '';

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
        ' style="cursor:pointer; pointer-events:none;"></td>';
    }

    return '<tr data-gl="' + gl + '" class="' + (isZero ? 'zero-row' : '') + '"' + (isZero && !_faShowZeroRows ? ' style="display:none;"' : '') + '>' +
      '<td><span style="font-family:monospace; font-size:12px;">' + gl + '</span>' + reclassBadge + '</td>' +
      '<td style="font-size:12px;"><a href="#" onclick="faToggleInvoices(\'' + gl + '\', this); return false;" style="color:inherit; text-decoration:none; cursor:pointer;" title="Click to view expenses">' + l.description + ' <span class="fa-drill-arrow" style="font-size:10px; color:var(--gray-400);">▶</span></a></td>' +
      '<td><input class="cell cell-notes" type="text" value="' + (l.notes||'').replace(/"/g,'&quot;') + '" data-gl="' + gl + '" data-field="notes" onchange="faAutoSave(\'' + gl + '\',\'notes\',this.value)"></td>' +
      '<td class="num">' + $cell('pr_'+gl, 'prior_year', prior) + '</td>' +
      '<td class="num">' + $cell('ytd_'+gl, 'ytd_actual', ytd) + '</td>' +
      '<td class="num">' + $cell('acc_'+gl, 'accrual_adj', accrual) + '</td>' +
      '<td class="num">' + $cell('unp_'+gl, 'unpaid_bills', unpaid) + '</td>' +
      '<td class="num">' + $cell('ytdb_'+gl, 'ytd_budget', ytdBudget) + '</td>' +
      fxCell('est_'+gl, 'estimate_override', estimate, estFormula, l.estimate_override !== null && l.estimate_override !== undefined) +
      fxCell('fcst_'+gl, 'forecast_override', forecast, fcstFormula, l.forecast_override !== null && l.forecast_override !== undefined) +
      '<td class="num">' + $cell('bud_'+gl, 'current_budget', budget) + '</td>' +
      '<td class="num"><input id="inc_'+gl+'" class="cell cell-pct" type="text" value="'+incPct+'%" data-raw="'+incPct+'" data-gl="'+gl+'" data-field="increase_pct" onfocus="this.value=this.dataset.raw" onblur="pctCellBlur(this)"></td>' +
      fxCell('prop_'+gl, 'proposed_budget', proposed, propFormula, false, userFormula) +
      '<td class="num" style="position:relative; cursor:pointer; color:'+varColor+';" onclick="fxCellFocus(document.getElementById(\'var_'+gl+'\'))">' +
        '<span class="fa-fx">fx</span>' +
        '<input id="var_'+gl+'" class="cell cell-fx" type="text" readonly' +
        ' value="' + fmt(variance) + '"' +
        ' data-raw="' + Math.round(variance) + '"' +
        ' data-formula="= ' + fmt(budget) + ' - ' + fmt(forecast) + '"' +
        ' data-gl="' + gl + '" data-field="variance"' +
        ' style="cursor:pointer; pointer-events:none; color:'+varColor+';"></td>' +
      '<td class="num" style="position:relative; cursor:pointer;" onclick="fxCellFocus(document.getElementById(\'pct_'+gl+'\'))">' +
        '<span class="fa-fx">fx</span>' +
        '<input id="pct_'+gl+'" class="cell cell-fx" type="text" readonly' +
        ' value="' + (pctChange*100).toFixed(1) + '%"' +
        ' data-raw="' + pctChange + '"' +
        ' data-formula="= (' + fmt(budget) + ' - ' + fmt(forecast) + ') / ' + fmt(forecast) + '"' +
        ' data-gl="' + gl + '" data-field="pct_change"' +
        ' style="cursor:pointer; pointer-events:none;"></td></tr>';
  }

  function sumLines(lines) {
    const t = {prior:0, ytd:0, accrual:0, unpaid:0, ytdBudget:0, estimate:0, forecast:0, budget:0, proposed:0};
    lines.forEach(l => {
      t.prior += l.prior_year || 0;
      t.ytd += l.ytd_actual || 0;
      t.accrual += l.accrual_adj || 0;
      t.unpaid += l.unpaid_bills || 0;
      t.ytdBudget += l.ytd_budget || 0;
      t.estimate += faComputeEstimate(l);
      t.forecast += faComputeForecast(l);
      t.budget += l.current_budget || 0;
      t.proposed += l.proposed_budget || (faComputeForecast(l) * (1 + (l.increase_pct || 0)));
    });
    return t;
  }

  function subtotalRow(label, t, cls, rowId) {
    const v = t.budget - t.forecast;
    const p = t.forecast ? ((t.budget - t.forecast)/t.forecast) : 0;
    const idAttr = rowId ? ' id="' + rowId + '"' : '';
    const isTotal = cls === 'total-row';
    const bs = isTotal ? 'background:rgba(255,255,255,0.2); color:white; border-color:rgba(255,255,255,0.4);' : '';
    function fxTd(val, col) {
      return '<td class="num" style="position:relative; cursor:pointer;" data-col="' + col + '" data-raw="' + Math.round(val) + '" onclick="fxSubtotalFocus(this)">' +
        '<span class="fa-fx" style="' + bs + '">fx</span>' +
        '<span class="sub-val">' + fmt(val) + '</span></td>';
    }
    const vc = v >= 0 ? 'var(--red)' : 'var(--green)';
    return '<tr class="' + (cls||'sub-row') + '"' + idAttr + '>' +
      '<td colspan="3">' + label + '</td>' +
      fxTd(t.prior, 'prior') +
      fxTd(t.ytd, 'ytd') +
      fxTd(t.accrual, 'accrual') +
      fxTd(t.unpaid, 'unpaid') +
      fxTd(t.ytdBudget, 'ytdBudget') +
      fxTd(t.estimate, 'estimate') +
      fxTd(t.forecast, 'forecast') +
      fxTd(t.budget, 'budget') +
      '<td class="num"></td>' +
      fxTd(t.proposed, 'proposed') +
      '<td class="num" style="position:relative; cursor:pointer; color:' + vc + ';" data-col="variance" data-raw="' + Math.round(v) + '" onclick="fxSubtotalFocus(this)"><span class="fa-fx" style="' + bs + '">fx</span><span class="sub-val">' + fmt(v) + '</span></td>' +
      '<td class="num" style="position:relative; cursor:pointer;" data-col="pctchange" data-raw="' + p + '" onclick="fxSubtotalFocus(this)"><span class="fa-fx" style="' + bs + '">fx</span><span class="sub-val">' + (p*100).toFixed(1) + '%</span></td></tr>';
  }

  // Build category groups and populate _catGroupGLs for live recalculation
  window._catGroupGLs = {};
  if (catConfig) {
    catConfig.groups.forEach(grp => {
      const gl = sheetLines.filter(grp.match);
      if (gl.length === 0) return;
      window._catGroupGLs[grp.key] = gl.map(l => l.gl_code);
      html += '<tr class="cat-hdr"><td colspan="' + NC + '">' + grp.label + '</td></tr>';
      gl.forEach(l => { html += buildLineRow(l); });
      html += subtotalRow('Total ' + grp.label, sumLines(gl), null, 'subtotal_' + grp.key);
    });
    const allGrouped = catConfig.groups.flatMap(g => sheetLines.filter(g.match));
    const ungrouped = sheetLines.filter(l => !allGrouped.includes(l));
    if (ungrouped.length > 0) {
      window._catGroupGLs['other'] = ungrouped.map(l => l.gl_code);
      html += '<tr class="cat-hdr"><td colspan="' + NC + '" style="color:var(--gray-500); border-color:var(--gray-300);">Other</td></tr>';
      ungrouped.forEach(l => { html += buildLineRow(l); });
      html += subtotalRow('Total Other', sumLines(ungrouped), null, 'subtotal_other');
    }
  } else {
    sheetLines.forEach(l => { html += buildLineRow(l); });
  }

  html += subtotalRow('Sheet Total', sumLines(sheetLines), 'total-row', 'faSheetTotal');
  html += '</tbody></table></div></div>';
  contentDiv.innerHTML = html;
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
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
  :root {
    --blue: #5a4a3f;
    --blue-dark: #3d322a;
    --blue-light: #f5efe7;
    --green: #057a55;
    --green-light: #def7ec;
    --orange: #d97706;
    --orange-light: #fef3c7;
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

  /* ── Formula Bar ── */
  .pm-formula-bar {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 12px;
    padding: 10px 20px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 12px;
    min-height: 44px;
    transition: all 0.2s;
  }
  .pm-formula-bar.active { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(90,74,63,0.08); }
  .pm-formula-bar .fb-label { font-size: 11px; font-weight: 700; color: var(--blue); text-transform: uppercase; white-space: nowrap; min-width: 60px; }
  .pm-formula-bar .fb-cell-ref { font-family: monospace; font-size: 13px; font-weight: 600; color: var(--gray-700); background: var(--gray-100); padding: 2px 8px; border-radius: 4px; min-width: 90px; text-align: center; }
  .pm-formula-bar .fb-formula { font-family: 'Courier New', monospace; font-size: 13px; color: var(--gray-700); flex: 1; padding: 4px 8px; background: var(--gray-50); border: 1px solid var(--gray-200); border-radius: 4px; }
  .pm-formula-bar .fb-badge { font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: 600; }
  .pm-formula-bar .fb-badge.auto { background: var(--green-light); color: var(--green); border: 1px solid var(--green); }

  /* ── Reclass Modal ── */
  .reclass-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.4); z-index: 1000; display: flex; align-items: center; justify-content: center; }
  .reclass-modal { background: white; border-radius: 12px; width: 560px; max-height: 80vh; display: flex; flex-direction: column; box-shadow: 0 20px 60px rgba(0,0,0,0.3); }
  .reclass-modal .rm-header { padding: 16px 20px; border-bottom: 1px solid var(--gray-200); display: flex; justify-content: space-between; align-items: center; }
  .reclass-modal .rm-header h3 { font-size: 15px; font-weight: 700; color: var(--blue); }
  .reclass-modal .rm-search { padding: 12px 20px; border-bottom: 1px solid var(--gray-200); }
  .reclass-modal .rm-search input { width: 100%; padding: 8px 12px; border: 1px solid var(--gray-300); border-radius: 6px; font-size: 13px; outline: none; }
  .reclass-modal .rm-search input:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(90,74,63,0.08); }
  .reclass-modal .rm-list { flex: 1; overflow-y: auto; max-height: 400px; }
  .reclass-modal .rm-cat-header { padding: 6px 20px; font-size: 11px; font-weight: 700; text-transform: uppercase; color: var(--blue); background: var(--blue-light); position: sticky; top: 0; }
  .reclass-modal .rm-gl-row { padding: 8px 20px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; font-size: 13px; border-bottom: 1px solid var(--gray-100); }
  .reclass-modal .rm-gl-row:hover { background: var(--blue-light); }
  .reclass-modal .rm-gl-row .gl-code { font-family: monospace; font-weight: 600; min-width: 90px; }
  .reclass-modal .rm-gl-row .gl-desc { flex: 1; color: var(--gray-700); }
  .reclass-modal .rm-footer { padding: 12px 20px; border-top: 1px solid var(--gray-200); display: flex; gap: 8px; justify-content: flex-end; }

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

  /* PM Cell Styles */
  .pm-cell { width:90px; padding:5px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; text-align:right; background:#fffff0; cursor:text; font-variant-numeric:tabular-nums; }
  .pm-cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px var(--blue-light, #f5efe7); }
  input.pm-cell-fx { background:#f0fdf4; border:1px solid #bbf7d0; }
  input.pm-cell-fx:focus { background:#ecfdf5; }
  .pm-fx { position:absolute; top:2px; right:2px; font-size:9px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:3px; padding:0 3px; cursor:pointer; user-select:none; z-index:5; }
  .pm-cell-pct { width:60px; }
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

  <div id="pmFormulaBarWrap" style="display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200); border-radius:8px; margin-bottom:12px;">
    <span style="font-size:11px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>
    <span id="pmFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:var(--gray-600); white-space:nowrap; min-width:100px;"></span>
    <input id="pmFormulaBar" type="text" placeholder="Click a green formula cell to view its formula..." style="display:block; flex:1; padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:monospace; background:white;" oninput="pmFormulaBarPreview()" onkeydown="pmFormulaBarKeydown(event)">
    <span id="pmFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:var(--green); white-space:nowrap; min-width:80px; text-align:right;"></span>
    <button id="pmFormulaAccept" style="display:none; padding:4px 14px; font-size:12px; font-weight:600; background:var(--green); color:white; border:none; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarAccept()">Accept</button>
    <button id="pmFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:var(--gray-200); color:var(--gray-700); border:none; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarCancel()">Cancel</button>
    <button id="pmFormulaClear" style="display:none; padding:4px 10px; font-size:11px; background:#fef2f2; color:var(--red); border:1px solid #fecaca; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarClear()" title="Remove formula, revert to auto-calc">Clear</button>
  </div>

  <div class="grid-wrapper">
    <div class="grid-container">
      <table id="linesTable">
        <thead>
          <tr>
            <th>GL Code</th>
            <th>Description</th>
            <th>Notes</th>
            <th class="number">Prior Year<br>Actual</th>
            <th class="number">YTD<br>Actual</th>
            <th class="number">Accrual<br>Adj</th>
            <th class="number">Unpaid<br>Bills</th>
            <th class="number">YTD<br>Budget</th>
            <th class="number">{{ estimate_label }}<br>Estimate <span style="font-size:9px; color:var(--blue); background:var(--blue-light, #f5efe7); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">fx</span></th>
            <th class="number">12 Month<br>Forecast <span style="font-size:9px; color:var(--blue); background:var(--blue-light, #f5efe7); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">fx</span></th>
            <th class="number">Current<br>Budget</th>
            <th class="number">Increase<br>%</th>
            <th class="number">Proposed<br>Budget <span style="font-size:9px; color:var(--blue); background:var(--blue-light, #f5efe7); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">fx</span></th>
            <th class="number">$<br>Variance</th>
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
const ALL_GL_CODES = {{ all_gl_json | safe }};
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

function fmtAmt(n) {
    if (n == null || isNaN(n)) return '$0.00';
    const abs = Math.abs(n);
    const str = abs.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    return n < 0 ? '($' + str + ')' : '$' + str;
}

function pctFmt(n) {
    if (n == null || isNaN(n)) return '0.0%';
    return (n * 100).toFixed(1) + '%';
}

function parseDollar(s) {
    if (typeof s !== 'string') return parseFloat(s) || 0;
    const isNeg = /^\s*\(.*\)\s*$/.test(s);
    const val = parseFloat(s.replace(/[$,\s()]/g, '')) || 0;
    return isNeg ? -val : val;
}

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

function computeEstimate(line) {
    if (line.estimate_override !== null && line.estimate_override !== undefined) return line.estimate_override;
    const ytd = line.ytd_actual || 0;
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    const prior = line.prior_year || 0;
    const base = ytd + accrual + unpaid;
    // Excel: =IFERROR(IF((YTD+Accrual+Unpaid)>=Prior,(base)/YTD_MONTHS*REM,Prior-base),0)
    if (base >= prior && prior > 0 && YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
    if (prior > 0) return Math.max(prior - base, 0);
    if (base > 0 && YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
    return 0;
}

function computeForecast(line) {
    if (line.forecast_override !== null && line.forecast_override !== undefined) return line.forecast_override;
    const ytd = line.ytd_actual || 0;
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    return ytd + accrual + unpaid + computeEstimate(line);
}

function computeProposed(line) {
    const forecast = computeForecast(line);
    return forecast * (1 + (line.increase_pct || 0));
}

// ── PM Cell Helper Functions ──────────────────────────────────────
let _pmCurrentCell = null;
let _pmEditMode = false;
let _pmOriginalFormula = '';

// Editable $cell: formats value on blur, triggers cascade on change
function pmCellBlur(el) {
    const gl = el.dataset.gl;
    const field = el.dataset.field;
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    if (field === 'increase_pct') {
        const pctVal = parseFloat(el.value) || 0;
        el.dataset.raw = pctVal.toFixed(1);
        el.value = pctVal.toFixed(1) + '%';
        if (line.increase_pct === pctVal / 100) return;
        line.increase_pct = pctVal / 100;
    } else {
        const val = parseDollar(el.value);
        el.dataset.raw = Math.round(val);
        el.value = fmt(val);
        if (line[field] === val) return;
        line[field] = val;
    }
    pmLineChanged(gl, field, null);
}

// Formula cell focus: opens formula bar for editing (if editable) or read-only display
function pmFxCellFocus(el) {
    const gl = el.dataset.gl;
    const field = el.dataset.field;
    const formula = el.dataset.formula || '';
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    // Highlight cell
    if (_pmCurrentCell) _pmCurrentCell.style.outline = '';
    el.style.outline = '2px solid var(--blue)';
    el.style.outlineOffset = '-1px';
    _pmCurrentCell = el;

    const bar = document.getElementById('pmFormulaBar');
    const label = document.getElementById('pmFormulaLabel');
    const preview = document.getElementById('pmFormulaPreview');
    const accept = document.getElementById('pmFormulaAccept');
    const cancel = document.getElementById('pmFormulaCancel');
    const clear = document.getElementById('pmFormulaClear');

    label.textContent = gl + ' · ' + field.replace('_', ' ');
    label.style.display = '';

    // Check if field is editable (estimate, forecast, proposed) vs read-only (variance, pct_change)
    const isEditable = field === 'estimate' || field === 'forecast' || field === 'proposed' || field === 'prior_year' || field === 'ytd_actual' || field === 'accrual_adj' || field === 'unpaid_bills' || field === 'ytd_budget' || field === 'current_budget' || field === 'increase_pct';
    const isFormulaCell = field === 'estimate' || field === 'forecast' || field === 'proposed' || field === 'variance' || field === 'pct_change';

    if ((isFormulaCell && !isEditable) || !CAN_EDIT) {
        // Read-only: non-editable formula cells OR user lacks edit permission
        bar.value = formula;
        bar.disabled = true;
        bar.style.opacity = '0.6';
        bar.style.cursor = 'not-allowed';
        accept.style.display = 'none';
        cancel.style.display = 'none';
        clear.style.display = 'none';
        preview.style.display = 'none';
        _pmEditMode = false;
    } else {
        // Editable cells
        bar.value = el.dataset.proposedFormula || formula;
        bar.disabled = false;
        bar.style.opacity = '1';
        bar.style.cursor = 'text';
        accept.style.display = '';
        cancel.style.display = '';
        clear.style.display = el.dataset.proposedFormula ? '' : 'none';
        _pmEditMode = true;
        _pmOriginalFormula = bar.value;
    }
}

// Formula cell blur: remove outline
function pmFxCellBlur(el) {
    if (_pmCurrentCell === el) _pmCurrentCell.style.outline = '';
}

// Subtotal focus: show SUM formula (read-only)
function pmSubtotalFocus(td) {
    const col = td.dataset.col;
    const raw = parseFloat(td.dataset.raw) || 0;
    const bar = document.getElementById('pmFormulaBar');
    const label = document.getElementById('pmFormulaLabel');
    const preview = document.getElementById('pmFormulaPreview');
    const accept = document.getElementById('pmFormulaAccept');
    const cancel = document.getElementById('pmFormulaCancel');
    const clear = document.getElementById('pmFormulaClear');

    bar.value = '=SUM(...)';
    bar.disabled = true;
    bar.style.opacity = '0.6';
    label.textContent = 'Subtotal · ' + col;
    label.style.display = '';
    accept.style.display = 'none';
    cancel.style.display = 'none';
    clear.style.display = 'none';
    preview.style.display = 'none';
}

// Live preview while typing in formula bar
function pmFormulaBarPreview() {
    const bar = document.getElementById('pmFormulaBar');
    const preview = document.getElementById('pmFormulaPreview');
    if (!_pmEditMode) { preview.style.display = 'none'; return; }
    const typed = bar.value.trim();
    if (!typed || typed === _pmOriginalFormula) { preview.style.display = 'none'; return; }
    const result = safeEvalFormula(typed);
    if (result !== null) {
        preview.textContent = '= ' + fmt(result);
        preview.style.color = '#059669';
        preview.style.display = '';
    } else if (/^[\d$,.\-\s]+$/.test(typed)) {
        const num = parseDollar(typed);
        preview.textContent = '= ' + fmt(num);
        preview.style.color = '#2563eb';
        preview.style.display = '';
    } else {
        preview.textContent = 'Invalid formula';
        preview.style.color = 'var(--red)';
        preview.style.display = '';
    }
}

// Accept formula/value from formula bar
function pmFormulaBarAccept() {
    if (!_pmCurrentCell) return;
    const bar = document.getElementById('pmFormulaBar');
    const gl = _pmCurrentCell.dataset.gl;
    const field = _pmCurrentCell.dataset.field;
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    const typed = bar.value.trim();
    const isFormula = typed.startsWith('=') || /[+\-*\/()]/.test(typed);
    const formulaResult = safeEvalFormula(typed);
    const numericVal = parseDollar(typed);

    let finalVal;
    if (isFormula && formulaResult !== null) {
        finalVal = formulaResult;
    } else if (!isNaN(numericVal)) {
        finalVal = numericVal;
    } else {
        showToast('Invalid formula or value', 'error');
        return;
    }

    // Set override on LINES object based on field
    if (field === 'estimate') {
        line.estimate_override = Math.round(finalVal);
    } else if (field === 'forecast') {
        line.forecast_override = Math.round(finalVal);
    } else if (field === 'proposed') {
        line.proposed_budget = Math.round(finalVal);
        if (isFormula && formulaResult !== null) {
            line.proposed_formula = typed.startsWith('=') ? typed : '=' + typed;
        } else {
            line.proposed_formula = null;
        }
    }

    _pmCurrentCell.dataset.formula = typed;
    _pmCurrentCell.value = fmt(finalVal);
    _pmCurrentCell.dataset.raw = Math.round(finalVal);

    // Update badge: fx for formula, ✎ for manual override
    const badge = _pmCurrentCell.parentElement.querySelector('.pm-fx');
    if (badge) {
        if (isFormula && formulaResult !== null) {
            badge.textContent = 'fx';
            badge.style.background = '#dbeafe';
            badge.style.color = 'var(--blue)';
            badge.style.borderColor = 'var(--blue)';
        } else {
            badge.textContent = '✎';
            badge.style.background = '#fef3c7';
            badge.style.color = '#d97706';
            badge.style.borderColor = '#d97706';
        }
    }

    // Flash green confirmation
    _pmCurrentCell.style.outline = '2px solid var(--green)';
    setTimeout(() => { if (_pmCurrentCell) _pmCurrentCell.style.outline = ''; }, 1200);

    pmLineChanged(gl, field, null);
    pmFormulaBarCancel();
}

// Cancel formula bar edits
function pmFormulaBarCancel() {
    const bar = document.getElementById('pmFormulaBar');
    const label = document.getElementById('pmFormulaLabel');
    const preview = document.getElementById('pmFormulaPreview');
    const accept = document.getElementById('pmFormulaAccept');
    const cancel = document.getElementById('pmFormulaCancel');
    const clear = document.getElementById('pmFormulaClear');

    bar.value = '';
    label.style.display = 'none';
    preview.style.display = 'none';
    accept.style.display = 'none';
    cancel.style.display = 'none';
    clear.style.display = 'none';
    _pmEditMode = false;
}

// Clear formula/override, revert to auto-calc
function pmFormulaBarClear() {
    if (!_pmCurrentCell) return;
    const gl = _pmCurrentCell.dataset.gl;
    const field = _pmCurrentCell.dataset.field;
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    if (field === 'estimate' && line.estimate_override !== null && line.estimate_override !== undefined) {
        line.estimate_override = null;
    } else if (field === 'forecast' && line.forecast_override !== null && line.forecast_override !== undefined) {
        line.forecast_override = null;
    } else if (field === 'proposed' && line.proposed_formula) {
        line.proposed_formula = null;
    }

    // Recalculate and update cell
    let newVal;
    if (field === 'estimate') {
        newVal = computeEstimate(line);
    } else if (field === 'forecast') {
        newVal = computeForecast(line);
    } else if (field === 'proposed') {
        newVal = computeProposed(line);
    }

    _pmCurrentCell.value = fmt(newVal);
    _pmCurrentCell.dataset.raw = Math.round(newVal);

    const badge = _pmCurrentCell.parentElement.querySelector('.pm-fx');
    if (badge) {
        badge.textContent = 'fx';
        badge.style.background = 'var(--blue-light, #e1effe)';
        badge.style.color = 'var(--blue)';
        badge.style.borderColor = 'var(--blue)';
    }

    pmLineChanged(gl, field, newVal);
    pmFormulaBarCancel();
}

// Keyboard navigation in formula bar
function pmFormulaBarKeydown(e) {
    if (e.key === 'Enter') {
        pmFormulaBarAccept();
    } else if (e.key === 'Escape') {
        pmFormulaBarCancel();
    }
}

// Get formula tooltip string for cell
function pmGetFormulaTooltip(line, type) {
    const ytd = line.ytd_actual || 0;
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    const base = ytd + accrual + unpaid;
    const prior = line.prior_year || 0;
    const estimate = computeEstimate(line);
    const forecast = computeForecast(line);
    const incPct = line.increase_pct || 0;

    if (type === 'estimate') {
        if (base >= prior && prior > 0 && YTD_MONTHS > 0) return '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
        if (prior > 0) return '=' + prior + '-(' + ytd + '+' + accrual + '+' + unpaid + ')';
        if (base > 0 && YTD_MONTHS > 0) return '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
        return '=0';
    }
    if (type === 'forecast') {
        const estExpr = (ytd > 0 && YTD_MONTHS > 0) ? ytd + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0';
        return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + estExpr + ')';
    }
    if (type === 'proposed') {
        if (line.proposed_formula) return line.proposed_formula;
        const fcstExpr = ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + ((ytd > 0 && YTD_MONTHS > 0) ? ytd + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0') + ')';
        return '=(' + fcstExpr + ')*(1+' + incPct.toFixed(4) + ')';
    }
    return '';
}

// Cascade recalculation when any cell changes
function pmLineChanged(gl, field, value) {
    const line = LINES.find(l => l.gl_code === gl);
    if (!line) return;

    // Recalculate dependent fields
    const estimate = computeEstimate(line);
    const forecast = computeForecast(line);
    const proposed = computeProposed(line);
    const variance = (line.current_budget || 0) - forecast;
    const pctChange = (forecast && isFinite(forecast)) ? (((line.current_budget || 0) - forecast) / forecast) : 0;

    // Update cells in DOM
    const estEl = document.getElementById('pm_est_' + gl);
    const fcEl = document.getElementById('pm_fc_' + gl);
    const propEl = document.getElementById('pm_prop_' + gl);
    const varEl = document.getElementById('pm_var_' + gl);
    const pctEl = document.getElementById('pm_pct_' + gl);

    if (estEl && estEl.dataset.field === 'estimate') {
        estEl.value = fmt(estimate); estEl.dataset.raw = Math.round(estimate);
        if (!(line.estimate_override !== null && line.estimate_override !== undefined)) estEl.dataset.formula = pmGetFormulaTooltip(line, 'estimate');
    }
    if (fcEl && fcEl.dataset.field === 'forecast') {
        fcEl.value = fmt(forecast); fcEl.dataset.raw = Math.round(forecast);
        if (!(line.forecast_override !== null && line.forecast_override !== undefined)) fcEl.dataset.formula = pmGetFormulaTooltip(line, 'forecast');
    }
    if (propEl && propEl.dataset.field === 'proposed') {
        propEl.value = fmt(proposed); propEl.dataset.raw = Math.round(proposed);
        if (!line.proposed_formula) propEl.dataset.formula = pmGetFormulaTooltip(line, 'proposed');
    }
    if (varEl) {
        varEl.value = fmt(variance); varEl.dataset.raw = Math.round(variance);
        varEl.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
        varEl.dataset.formula = '= ' + fmt(line.current_budget || 0) + ' - ' + fmt(forecast);
        const varTd = varEl.parentElement; if (varTd) varTd.style.color = variance >= 0 ? 'var(--red)' : 'var(--green)';
    }
    if (pctEl) {
        const pctDisp = isFinite(pctChange) ? (pctChange * 100).toFixed(1) : '0.0';
        pctEl.value = pctDisp + '%'; pctEl.dataset.raw = isFinite(pctChange) ? pctChange : 0;
        pctEl.dataset.formula = '= (' + fmt(line.current_budget || 0) + ' - ' + fmt(forecast) + ') / ' + fmt(forecast);
    }

    // Update subtotals and grand totals
    pmUpdateTotals();

    // Debounced save
    if (saveTimer) clearTimeout(saveTimer);
    indicator.textContent = 'Unsaved changes...';
    indicator.className = 'save-indicator saving';
    saveTimer = setTimeout(saveAll, 800);
}

// Update all subtotal and grand total rows
function pmUpdateTotals() {
    const categories = {supplies: [], repairs: [], maintenance: []};
    const catLabels = {supplies: 'Supplies', repairs: 'Repairs', maintenance: 'Maintenance Contracts'};
    LINES.forEach(l => {
        if (categories[l.category]) categories[l.category].push(l);
    });

    let grandTotals = {prior:0, ytd:0, accrual:0, unpaid:0, ytdBudget:0, estimate:0, forecast:0, budget:0, proposed:0};

    for (const [cat, catLines] of Object.entries(categories)) {
        if (catLines.length === 0) continue;

        let catTotals = {prior:0, ytd:0, accrual:0, unpaid:0, ytdBudget:0, estimate:0, forecast:0, budget:0, proposed:0};
        catLines.forEach(l => {
            catTotals.prior += (l.prior_year || 0);
            catTotals.ytd += (l.ytd_actual || 0);
            catTotals.accrual += (l.accrual_adj || 0);
            catTotals.unpaid += (l.unpaid_bills || 0);
            catTotals.ytdBudget += (l.ytd_budget || 0);
            catTotals.estimate += computeEstimate(l);
            catTotals.forecast += computeForecast(l);
            catTotals.budget += (l.current_budget || 0);
            catTotals.proposed += computeProposed(l);
        });

        // Update subtotal cells
        const subPrior = document.getElementById('pm_subtotal_prior_' + cat);
        const subYtd = document.getElementById('pm_subtotal_ytd_' + cat);
        const subYtdBudget = document.getElementById('pm_subtotal_ytdbudget_' + cat);
        const subEstimate = document.getElementById('pm_subtotal_estimate_' + cat);
        const subForecast = document.getElementById('pm_subtotal_forecast_' + cat);
        const subBudget = document.getElementById('pm_subtotal_budget_' + cat);
        const subProposed = document.getElementById('pm_subtotal_proposed_' + cat);
        const subVar = document.getElementById('pm_subtotal_variance_' + cat);

        if (subPrior) subPrior.textContent = fmt(catTotals.prior);
        if (subYtd) subYtd.textContent = fmt(catTotals.ytd);
        if (subYtdBudget) subYtdBudget.textContent = fmt(catTotals.ytdBudget);
        if (subEstimate) subEstimate.textContent = fmt(catTotals.estimate);
        if (subForecast) subForecast.textContent = fmt(catTotals.forecast);
        if (subBudget) subBudget.textContent = fmt(catTotals.budget);
        if (subProposed) subProposed.textContent = fmt(catTotals.proposed);
        const catVar = catTotals.budget - catTotals.forecast;
        if (subVar) { subVar.textContent = fmt(catVar); subVar.style.color = catVar >= 0 ? 'var(--red)' : 'var(--green)'; }

        Object.keys(grandTotals).forEach(k => grandTotals[k] += catTotals[k]);
    }

    // Update grand total cells
    const grandPrior = document.getElementById('pm_grandtotal_prior');
    const grandYtd = document.getElementById('pm_grandtotal_ytd');
    const grandYtdBudget = document.getElementById('pm_grandtotal_ytdbudget');
    const grandEstimate = document.getElementById('pm_grandtotal_estimate');
    const grandForecast = document.getElementById('pm_grandtotal_forecast');
    const grandBudget = document.getElementById('pm_grandtotal_budget');
    const grandProposed = document.getElementById('pm_grandtotal_proposed');
    const grandVar = document.getElementById('pm_grandtotal_variance');
    const grandPct = document.getElementById('pm_grandtotal_pct');

    if (grandPrior) grandPrior.textContent = fmt(grandTotals.prior);
    if (grandYtd) grandYtd.textContent = fmt(grandTotals.ytd);
    if (grandYtdBudget) grandYtdBudget.textContent = fmt(grandTotals.ytdBudget);
    if (grandEstimate) grandEstimate.textContent = fmt(grandTotals.estimate);
    if (grandForecast) grandForecast.textContent = fmt(grandTotals.forecast);
    if (grandBudget) grandBudget.textContent = fmt(grandTotals.budget);
    if (grandProposed) grandProposed.textContent = fmt(grandTotals.proposed);
    const gVar = grandTotals.budget - grandTotals.forecast;
    const gPct = grandTotals.forecast ? ((grandTotals.budget - grandTotals.forecast) / grandTotals.forecast) : 0;
    if (grandVar) { grandVar.textContent = fmt(gVar); grandVar.style.color = gVar >= 0 ? 'var(--red)' : 'var(--green)'; }
    if (grandPct) grandPct.textContent = (gPct * 100).toFixed(1) + '%';
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

    let grandTotals = {prior:0, ytd:0, accrual:0, unpaid:0, ytdBudget:0, estimate:0, forecast:0, budget:0, proposed:0};
    const NC = 15;

    for (const [cat, catLines] of Object.entries(categories)) {
        if (catLines.length === 0) continue;

        const headerRow = document.createElement('tr');
        headerRow.className = 'category-header';
        headerRow.innerHTML = '<td colspan="' + NC + '">' + catLabels[cat] + '</td>';
        tbody.appendChild(headerRow);

        let catTotals = {prior:0, ytd:0, accrual:0, unpaid:0, ytdBudget:0, estimate:0, forecast:0, budget:0, proposed:0};

        catLines.forEach(line => {
            const estimate = computeEstimate(line);
            const forecast = computeForecast(line);
            const proposed = computeProposed(line);
            const variance = (line.current_budget || 0) - forecast;
            const pctChange = forecast ? (((line.current_budget || 0) - forecast) / forecast) : 0;

            catTotals.prior += (line.prior_year || 0);
            catTotals.ytd += (line.ytd_actual || 0);
            catTotals.ytdBudget += (line.ytd_budget || 0);
            catTotals.estimate += estimate;
            catTotals.forecast += forecast;
            catTotals.budget += (line.current_budget || 0);
            catTotals.proposed += proposed;

            const reclassBadge = line.reclass_to_gl ? ' <span style="background:var(--orange-light); color:var(--orange); font-size:10px; padding:1px 5px; border-radius:8px;">Reclass</span>' : '';

            const isZero = !(line.prior_year || line.ytd_actual || line.accrual_adj || line.unpaid_bills || line.ytd_budget || line.current_budget || (line.increase_pct && line.increase_pct !== 0));
            const tr = document.createElement('tr');
            if (isZero) { tr.classList.add('zero-row'); if (!_showZeroRows) tr.style.display = 'none'; }

            const gl = line.gl_code;
            const estFormula = pmGetFormulaTooltip(line, 'estimate');
            const fcstFormula = pmGetFormulaTooltip(line, 'forecast');
            const propFormula = pmGetFormulaTooltip(line, 'proposed');

            tr.innerHTML = `
                <td><a href="#" onclick="toggleInvoices('${gl}', this); return false;" style="color:var(--blue); text-decoration:none; font-family:monospace;" title="Click to view invoices">${gl}</a>${reclassBadge}</td>
                <td><a href="#" onclick="toggleInvoices('${gl}', this); return false;" style="color:inherit; text-decoration:none; cursor:pointer;" title="Click to view expenses">${line.description} <span class="drill-arrow" style="font-size:10px; color:var(--gray-400); transition:transform 0.2s;">▶</span></a></td>
                <td><input type="text" value="${(line.notes || '').replace(/"/g, '&quot;')}" data-gl="${gl}" data-field="notes" onchange="pmLineChanged('${gl}', 'notes', this.value)" ${CAN_EDIT ? '' : 'disabled'} style="min-width:100px;"></td>
                <td class="number"><input id="pm_pr_${gl}" class="pm-cell" type="text" value="${fmt(line.prior_year)}" data-raw="${Math.round(line.prior_year || 0)}" data-gl="${gl}" data-field="prior_year" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number"><input id="pm_ytd_${gl}" class="pm-cell" type="text" value="${fmt(line.ytd_actual)}" data-raw="${Math.round(line.ytd_actual || 0)}" data-gl="${gl}" data-field="ytd_actual" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number"><input id="pm_acc_${gl}" class="pm-cell" type="text" value="${fmt(line.accrual_adj)}" data-raw="${Math.round(line.accrual_adj || 0)}" data-gl="${gl}" data-field="accrual_adj" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number"><input id="pm_unp_${gl}" class="pm-cell" type="text" value="${fmt(line.unpaid_bills)}" data-raw="${Math.round(line.unpaid_bills || 0)}" data-gl="${gl}" data-field="unpaid_bills" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number"><input id="pm_ytdb_${gl}" class="pm-cell" type="text" value="${fmt(line.ytd_budget)}" data-raw="${Math.round(line.ytd_budget || 0)}" data-gl="${gl}" data-field="ytd_budget" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number" style="position:relative; cursor:pointer;" onclick="pmFxCellFocus(document.getElementById('pm_est_${gl}'))">
                    <span class="pm-fx">fx</span>
                    <input id="pm_est_${gl}" class="pm-cell pm-cell-fx" type="text" readonly value="${fmt(estimate)}" data-raw="${Math.round(estimate)}" data-formula="${estFormula}" data-gl="${gl}" data-field="estimate" style="cursor:pointer; pointer-events:none;">
                </td>
                <td class="number" style="position:relative; cursor:pointer;" onclick="pmFxCellFocus(document.getElementById('pm_fc_${gl}'))">
                    <span class="pm-fx">fx</span>
                    <input id="pm_fc_${gl}" class="pm-cell pm-cell-fx" type="text" readonly value="${fmt(forecast)}" data-raw="${Math.round(forecast)}" data-formula="${fcstFormula}" data-gl="${gl}" data-field="forecast" style="cursor:pointer; pointer-events:none;">
                </td>
                <td class="number"><input id="pm_bud_${gl}" class="pm-cell" type="text" value="${fmt(line.current_budget)}" data-raw="${Math.round(line.current_budget || 0)}" data-gl="${gl}" data-field="current_budget" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number"><input id="pm_inc_${gl}" class="pm-cell pm-cell-pct" type="text" value="${((line.increase_pct || 0) * 100).toFixed(1)}%" data-raw="${((line.increase_pct || 0) * 100).toFixed(1)}" data-gl="${gl}" data-field="increase_pct" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number" style="position:relative; cursor:pointer;" onclick="pmFxCellFocus(document.getElementById('pm_prop_${gl}'))">
                    <span class="pm-fx">fx</span>
                    <input id="pm_prop_${gl}" class="pm-cell pm-cell-fx" type="text" readonly value="${fmt(proposed)}" data-raw="${Math.round(proposed)}" data-formula="${propFormula}" data-gl="${gl}" data-field="proposed" style="cursor:pointer; pointer-events:none;">
                </td>
                <td class="number" style="position:relative; cursor:pointer; color:${variance >= 0 ? 'var(--red)' : 'var(--green)'};" onclick="pmFxCellFocus(document.getElementById('pm_var_${gl}'))">
                    <span class="pm-fx">fx</span>
                    <input id="pm_var_${gl}" class="pm-cell pm-cell-fx" type="text" readonly value="${fmt(variance)}" data-raw="${Math.round(variance)}" data-formula="= ${fmt(line.current_budget || 0)} - ${fmt(forecast)}" data-gl="${gl}" data-field="variance" style="cursor:pointer; pointer-events:none; color:${variance >= 0 ? 'var(--red)' : 'var(--green)'};">
                </td>
                <td class="number" style="position:relative; cursor:pointer;" onclick="pmFxCellFocus(document.getElementById('pm_pct_${gl}'))">
                    <span class="pm-fx">fx</span>
                    <input id="pm_pct_${gl}" class="pm-cell pm-cell-fx" type="text" readonly value="${(pctChange*100).toFixed(1)}%" data-raw="${pctChange}" data-formula="= (${fmt(line.current_budget || 0)} - ${fmt(forecast)}) / ${fmt(forecast)}" data-gl="${gl}" data-field="pct_change" style="cursor:pointer; pointer-events:none;">
                </td>
            `;
            tbody.appendChild(tr);
        });

        // Subtotal
        const catVar = catTotals.budget - catTotals.forecast;
        const subRow = document.createElement('tr');
        subRow.className = 'subtotal-row';
        subRow.innerHTML = `
            <td></td><td>Total ${catLabels[cat]}</td><td></td>
            <td class="number" id="pm_subtotal_prior_${cat}" style="position:relative; cursor:pointer;" data-col="prior" data-raw="${Math.round(catTotals.prior)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(catTotals.prior)}</span></td>
            <td class="number" id="pm_subtotal_ytd_${cat}" style="position:relative; cursor:pointer;" data-col="ytd" data-raw="${Math.round(catTotals.ytd)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(catTotals.ytd)}</span></td>
            <td></td><td></td>
            <td class="number" id="pm_subtotal_ytdbudget_${cat}" style="position:relative; cursor:pointer;" data-col="ytdbudget" data-raw="${Math.round(catTotals.ytdBudget)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(catTotals.ytdBudget)}</span></td>
            <td class="number" id="pm_subtotal_estimate_${cat}" style="position:relative; cursor:pointer;" data-col="estimate" data-raw="${Math.round(catTotals.estimate)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(catTotals.estimate)}</span></td>
            <td class="number" id="pm_subtotal_forecast_${cat}" style="position:relative; cursor:pointer;" data-col="forecast" data-raw="${Math.round(catTotals.forecast)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(catTotals.forecast)}</span></td>
            <td class="number" id="pm_subtotal_budget_${cat}" style="position:relative; cursor:pointer;" data-col="budget" data-raw="${Math.round(catTotals.budget)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(catTotals.budget)}</span></td>
            <td></td>
            <td class="number" id="pm_subtotal_proposed_${cat}" style="position:relative; cursor:pointer;" data-col="proposed" data-raw="${Math.round(catTotals.proposed)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(catTotals.proposed)}</span></td>
            <td class="number" id="pm_subtotal_variance_${cat}" style="position:relative; cursor:pointer; color:${catVar >= 0 ? 'var(--red)' : 'var(--green)'};" data-col="variance" data-raw="${Math.round(catVar)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(catVar)}</span></td>
            <td></td>
        `;
        tbody.appendChild(subRow);

        Object.keys(grandTotals).forEach(k => grandTotals[k] += catTotals[k]);
    }

    // Grand total
    const grandVar = grandTotals.budget - grandTotals.forecast;
    const grandPct = grandTotals.forecast ? ((grandTotals.budget - grandTotals.forecast) / grandTotals.forecast) : 0;
    const grandRow = document.createElement('tr');
    grandRow.className = 'grand-total';
    grandRow.innerHTML = `
        <td></td><td>GRAND TOTAL R&M</td><td></td>
        <td class="number" id="pm_grandtotal_prior" style="position:relative; cursor:pointer;" data-col="prior" data-raw="${Math.round(grandTotals.prior)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(grandTotals.prior)}</span></td>
        <td class="number" id="pm_grandtotal_ytd" style="position:relative; cursor:pointer;" data-col="ytd" data-raw="${Math.round(grandTotals.ytd)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(grandTotals.ytd)}</span></td>
        <td></td><td></td>
        <td class="number" id="pm_grandtotal_ytdbudget" style="position:relative; cursor:pointer;" data-col="ytdbudget" data-raw="${Math.round(grandTotals.ytdBudget)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(grandTotals.ytdBudget)}</span></td>
        <td class="number" id="pm_grandtotal_estimate" style="position:relative; cursor:pointer;" data-col="estimate" data-raw="${Math.round(grandTotals.estimate)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(grandTotals.estimate)}</span></td>
        <td class="number" id="pm_grandtotal_forecast" style="position:relative; cursor:pointer;" data-col="forecast" data-raw="${Math.round(grandTotals.forecast)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(grandTotals.forecast)}</span></td>
        <td class="number" id="pm_grandtotal_budget" style="position:relative; cursor:pointer;" data-col="budget" data-raw="${Math.round(grandTotals.budget)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(grandTotals.budget)}</span></td>
        <td></td>
        <td class="number" id="pm_grandtotal_proposed" style="position:relative; cursor:pointer;" data-col="proposed" data-raw="${Math.round(grandTotals.proposed)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(grandTotals.proposed)}</span></td>
        <td class="number" id="pm_grandtotal_variance" style="position:relative; cursor:pointer; color:${grandVar >= 0 ? 'var(--red)' : 'var(--green)'};" data-col="variance" data-raw="${Math.round(grandVar)}" onclick="pmSubtotalFocus(this)"><span class="pm-fx">fx</span><span class="sub-val">${fmt(grandVar)}</span></td>
        <td class="number" id="pm_grandtotal_pct">${(grandPct * 100).toFixed(1)}%</td>
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
    btn.style.background = _showZeroRows ? 'var(--gray-200)' : 'var(--blue-light, #f5efe7)';
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

    // Build all GL codes for reclass dropdown
    const allGLs = LINES.map(l => l.gl_code).filter(g => g !== glCode);

    const detailRow = document.createElement('tr');
    detailRow.className = 'invoice-detail-row';
    let html = '<td colspan="15" style="padding:0;"><div style="padding:12px 16px 12px 40px; background:linear-gradient(to right, #f0f4ff, #f8faff); border-left:3px solid var(--blue); border-bottom:1px solid var(--gray-200);">';
    html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">';
    html += '<span style="font-weight:600; font-size:13px; color:var(--blue);">' + glCode + ' — ' + (glGroup.gl_name || '') + '</span>';
    html += '<span style="font-size:12px; color:var(--gray-500);">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' · ' + fmtAmt(glGroup.total || 0) + '</span>';
    html += '</div>';

    html += '<table style="width:100%; font-size:12px; border-collapse:collapse; background:white; border-radius:6px; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,0.05); table-layout:fixed;">';
    html += '<colgroup><col style="width:18%"><col style="width:22%"><col style="width:10%"><col style="width:9%"><col style="width:11%"><col style="width:8%"><col style="width:22%"></colgroup>';
    html += '<thead><tr style="background:var(--gray-100); color:var(--gray-600); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
    html += '<td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Payee</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Description</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Invoice #</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Date</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap; text-align:right;">Amount</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">Check #</div></td><td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap; text-align:right;">Action</div></td></tr></thead>';

    glGroup.invoices.forEach(inv => {
        const isReclassed = !!inv.reclass_to_gl;
        html += '<tr style="border-top:1px solid var(--gray-200);' + (isReclassed ? ' opacity:0.5; text-decoration:line-through;' : '') + '">';
        html += '<td><div style="padding:6px 10px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="' + ((inv.payee_name || inv.payee_code || '').replace(/"/g, '&quot;')) + '">' + (inv.payee_name || inv.payee_code || '—') + '</div></td>';
        html += '<td><div style="padding:6px 10px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-size:11px; color:var(--gray-600);" title="' + ((inv.notes || '').replace(/"/g, '&quot;')) + '">' + (inv.notes || '—') + '</div></td>';
        html += '<td><div style="padding:6px 10px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-family:monospace; font-size:11px;">' + (inv.invoice_num || '—') + '</div></td>';
        html += '<td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</div></td>';
        html += '<td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap; text-align:right; font-variant-numeric:tabular-nums;">' + fmtAmt(inv.amount) + '</div></td>';
        html += '<td><div style="padding:6px 10px; overflow:hidden; white-space:nowrap;">' + (inv.check_num || '—') + '</div></td>';
        html += '<td style="text-align:right;">';
        if (isReclassed) {
            html += '<span style="font-size:11px; color:var(--orange);">→ ' + inv.reclass_to_gl + '</span> ';
            html += '<button onclick="inlineUndoReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:#fef3c7; color:#92400e; border:1px solid #fcd34d; border-radius:4px; cursor:pointer;">Undo</button>';
        } else {
            html += '<span id="reclass_label_' + inv.id + '" style="font-size:11px; color:var(--gray-500); margin-right:4px;"></span>';
            html += '<input type="hidden" id="reclass_gl_' + inv.id + '" value="">';
            html += '<button onclick="openReclassModal(' + inv.id + ',\'' + glCode + '\',\'inline\')" style="font-size:11px; padding:2px 8px; background:var(--gray-100); color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer;">Reclass to…</button> ';
            html += '<button id="reclass_go_' + inv.id + '" onclick="inlineReclass(' + inv.id + ',\'' + glCode + '\')" style="font-size:11px; padding:2px 8px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer; display:none;">Go</button>';
        }
        html += '</td></tr>';
    });

    html += '</table></div></td>';
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
            // Re-toggle to refresh the detail view
            const glLink = document.querySelector('a[onclick*="' + fromGL + '"]');
            if (glLink) { toggleInvoices(fromGL, glLink); setTimeout(() => toggleInvoices(fromGL, glLink), 100); }
            showToast('Invoice reclassified to ' + select.value, 'success');
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
            const glLink = document.querySelector('a[onclick*="' + fromGL + '"]');
            if (glLink) { toggleInvoices(fromGL, glLink); setTimeout(() => toggleInvoices(fromGL, glLink), 100); }
            showToast('Reclass undone', 'success');
        } else { showToast('Undo failed', 'error'); }
    } catch(e) { showToast('Undo error: ' + e.message, 'error'); }
}

// Legacy stub — now uses pmLineChanged for cascade system
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

    pmLineChanged(gl, field, el.value);
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
            category: l.category || '',
            estimate_override: l.estimate_override !== null && l.estimate_override !== undefined ? l.estimate_override : null,
            forecast_override: l.forecast_override !== null && l.forecast_override !== undefined ? l.forecast_override : null,
            proposed_budget: l.proposed_budget || 0,
            proposed_formula: l.proposed_formula || null,
            prior_year: l.prior_year || 0,
            ytd_actual: l.ytd_actual || 0,
            ytd_budget: l.ytd_budget || 0,
            current_budget: l.current_budget || 0
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

// ── Searchable Reclass Modal ─────────────────────────────────────────
let _reclassCallback = null;

function openReclassModal(invoiceIdOrGl, fromGL, mode) {
    // mode: 'inline' (invoice-level) or 'line' (GL-level)
    _reclassCallback = { id: invoiceIdOrGl, fromGL: fromGL, mode: mode };

    // Build modal HTML
    let overlay = document.getElementById('reclassOverlay');
    if (overlay) overlay.remove();

    // Group ALL_GL_CODES by category, sorted by gl_code
    const cats = {};
    const catOrder = [];
    ALL_GL_CODES.filter(g => g.gl_code !== fromGL).forEach(g => {
        const cat = g.category || 'other';
        if (!cats[cat]) { cats[cat] = []; catOrder.push(cat); }
        cats[cat].push(g);
    });
    // Sort each category's GLs
    catOrder.forEach(c => cats[c].sort((a,b) => a.gl_code.localeCompare(b.gl_code)));
    catOrder.sort();

    // Build category label map
    const catLabels = {supplies:'Supplies',repairs:'Repairs',maintenance:'Maintenance',payroll:'Payroll',electric:'Electric',gas:'Gas',fuel:'Fuel',water:'Water & Sewer',sewer:'Water & Sewer',insurance:'Insurance',re_taxes:'Real Estate Taxes',professional:'Professional Fees',admin:'Administrative',financial:'Financial',income:'Income',other:'Other'};

    let listHtml = '';
    catOrder.forEach(cat => {
        listHtml += '<div class="rm-cat-header">' + (catLabels[cat] || cat) + '</div>';
        cats[cat].forEach(g => {
            listHtml += '<div class="rm-gl-row" data-gl="' + g.gl_code + '" data-desc="' + (g.description || '').toLowerCase() + '" data-cat="' + cat + '" onclick="selectReclassGL(\'' + g.gl_code + '\',\'' + g.description.replace(/'/g, "\\'") + '\')">';
            listHtml += '<span class="gl-code">' + g.gl_code + '</span>';
            listHtml += '<span class="gl-desc">' + (g.description || '') + '</span>';
            listHtml += '</div>';
        });
    });

    overlay = document.createElement('div');
    overlay.id = 'reclassOverlay';
    overlay.className = 'reclass-overlay';
    overlay.innerHTML = `
        <div class="reclass-modal">
            <div class="rm-header">
                <h3>Select Target GL Code</h3>
                <button onclick="document.getElementById('reclassOverlay').remove()" style="background:none; border:none; font-size:18px; cursor:pointer; color:var(--gray-500);">✕</button>
            </div>
            <div class="rm-search">
                <input type="text" id="reclassSearch" placeholder="Search by GL code, name, or category…" oninput="filterReclassModal(this.value)" autofocus>
            </div>
            <div class="rm-list" id="reclassListContainer">${listHtml}</div>
            <div class="rm-footer">
                <span style="font-size:12px; color:var(--gray-500);">${ALL_GL_CODES.length} GL codes available</span>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);

    // Close on overlay click
    overlay.addEventListener('click', function(e) { if (e.target === overlay) overlay.remove(); });

    // Focus search
    setTimeout(() => document.getElementById('reclassSearch').focus(), 50);
}

function filterReclassModal(q) {
    q = q.toLowerCase();
    const container = document.getElementById('reclassListContainer');
    const rows = container.querySelectorAll('.rm-gl-row');
    const catHeaders = container.querySelectorAll('.rm-cat-header');
    const catVisible = {};

    rows.forEach(r => {
        const gl = r.dataset.gl.toLowerCase();
        const desc = r.dataset.desc;
        const cat = r.dataset.cat;
        const match = !q || gl.includes(q) || desc.includes(q) || (cat && cat.includes(q));
        r.style.display = match ? '' : 'none';
        if (match) catVisible[cat] = true;
    });

    catHeaders.forEach(h => {
        const catName = h.textContent.toLowerCase();
        // Show cat header if any child matches
        const nextRows = [];
        let sib = h.nextElementSibling;
        while (sib && !sib.classList.contains('rm-cat-header')) { nextRows.push(sib); sib = sib.nextElementSibling; }
        const anyVisible = nextRows.some(r => r.style.display !== 'none');
        h.style.display = anyVisible ? '' : 'none';
    });
}

function selectReclassGL(glCode, glDesc) {
    if (!_reclassCallback) return;
    const cb = _reclassCallback;

    if (cb.mode === 'inline') {
        // Set hidden input and show label
        const hidden = document.getElementById('reclass_gl_' + cb.id);
        const label = document.getElementById('reclass_label_' + cb.id);
        const goBtn = document.getElementById('reclass_go_' + cb.id);
        if (hidden) hidden.value = glCode;
        if (label) { label.textContent = '→ ' + glCode; label.style.color = 'var(--blue)'; label.style.fontWeight = '600'; }
        if (goBtn) goBtn.style.display = '';
    } else if (cb.mode === 'line') {
        // Set the hidden input for line-level reclass
        const hidden = document.getElementById('reclass_target_' + cb.fromGL);
        const label = document.getElementById('reclass_target_label_' + cb.fromGL);
        if (hidden) hidden.value = glCode;
        if (label) { label.textContent = glCode + ' — ' + glDesc; label.style.color = 'var(--blue)'; label.style.fontWeight = '600'; }
    }

    document.getElementById('reclassOverlay').remove();
}

// Line-level reclass suggestion
function showReclass(glCode) {
    const line = LINES.find(l => l.gl_code === glCode);
    if (!line) return;

    const row = document.querySelector(`[data-gl="${glCode}"]`).closest('tr');
    const existing = row.nextElementSibling;
    if (existing && existing.classList.contains('reclass-form-row')) {
        existing.remove();
        return;
    }

    const formRow = document.createElement('tr');
    formRow.className = 'reclass-form-row';
    formRow.innerHTML = `
        <td colspan="15" style="padding:12px 24px; background:var(--blue-light); border-left:3px solid var(--blue);">
            <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
                <label style="font-size:12px; font-weight:600;">Suggest reclass to:</label>
                <input type="hidden" id="reclass_target_${glCode}" value="">
                <span id="reclass_target_label_${glCode}" style="font-size:12px; color:var(--gray-500);">No GL selected</span>
                <button onclick="openReclassModal('${glCode}','${glCode}','line')" style="font-size:12px; padding:4px 12px; background:var(--gray-100); color:var(--gray-700); border:1px solid var(--gray-300); border-radius:4px; cursor:pointer;">Choose GL…</button>
                <input type="number" id="reclass_amount_${glCode}" placeholder="Amount" step="1" value="${Math.round(line.current_budget || 0)}"
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

renderTable();
updateZeroToggle();
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
  if (l.estimate_override !== null && l.estimate_override !== undefined) return l.estimate_override;
  const ytd = l.ytd_actual || 0, accrual = l.accrual_adj || 0, unpaid = l.unpaid_bills || 0, prior = l.prior_year || 0;
  const base = ytd + accrual + unpaid;
  if (base >= prior && prior > 0 && YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
  if (prior > 0) return Math.max(prior - base, 0);
  if (base > 0 && YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
  return 0;
}

function computeForecast(l) {
  if (l.forecast_override !== null && l.forecast_override !== undefined) return l.forecast_override;
  return (l.ytd_actual || 0) + (l.accrual_adj || 0) + (l.unpaid_bills || 0) + computeEstimate(l);
}

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
  const t = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
  lines.forEach(l => {
    t.prior += l.prior_year || 0;
    t.ytd += l.ytd_actual || 0;
    t.accrual += l.accrual_adj || 0;
    t.unpaid += l.unpaid_bills || 0;
    t.estimate += computeEstimate(l);
    t.forecast += computeForecast(l);
    t.budget += l.current_budget || 0;
    t.proposed += l.proposed_budget || (computeForecast(l) * (1 + (l.increase_pct || 0)));
  });
  return t;
}

// ── Summary formula bar state ─────────────────────────────────────────
let _activeSumFxCell = null;
let _sumFormulaOriginal = '';
// Store category data so formula builder can access it
let _sumCatData = {};

function _showSumButtons(show) {
  ['sumFormulaPreview','sumFormulaAccept','sumFormulaCancel'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = show ? 'inline-block' : 'none';
  });
}

// Build real math formula for a summary cell
function _buildSumFormula(cellId) {
  const data = _sumCatData[cellId];
  if (!data) return '';
  const field = data.field;
  const lines = data.lines;

  if (field === 'var') {
    // $ Change = Curr Budget - 12 Mo Forecast
    const budget = Math.round(lines.reduce((s, l) => s + (l.current_budget || 0), 0));
    const forecast = Math.round(lines.reduce((s, l) => s + computeForecast(l), 0));
    return '= ' + budget + ' - ' + forecast;
  }
  if (field === 'pct') {
    // % Change = (Curr Budget - 12 Mo Forecast) / 12 Mo Forecast
    const budget = Math.round(lines.reduce((s, l) => s + (l.current_budget || 0), 0));
    const forecast = Math.round(lines.reduce((s, l) => s + computeForecast(l), 0));
    return forecast ? '= (' + budget + ' - ' + forecast + ') / ' + forecast : '= 0';
  }
  if (field === 'forecast') {
    // Show SUM of GL forecasts
    const vals = lines.map(l => Math.round(computeForecast(l)));
    if (vals.length <= 8) return '= ' + vals.join(' + ');
    return '= SUM of ' + vals.length + ' GL lines = ' + Math.round(vals.reduce((a, b) => a + b, 0));
  }
  if (field === 'proposed') {
    const vals = lines.map(l => Math.round(l.proposed_budget || (computeForecast(l) * (1 + (l.increase_pct || 0)))));
    if (vals.length <= 8) return '= ' + vals.join(' + ');
    return '= SUM of ' + vals.length + ' GL lines = ' + Math.round(vals.reduce((a, b) => a + b, 0));
  }
  if (field === 'ytd') {
    const vals = lines.map(l => Math.round(l.ytd_actual || 0)).filter(v => v !== 0);
    if (vals.length <= 8) return '= ' + (vals.length ? vals.join(' + ') : '0');
    return '= SUM of ' + vals.length + ' GL lines = ' + Math.round(vals.reduce((a, b) => a + b, 0));
  }
  if (field === 'estimate') {
    const vals = lines.map(l => Math.round(computeEstimate(l))).filter(v => v !== 0);
    if (vals.length <= 8) return '= ' + (vals.length ? vals.join(' + ') : '0');
    return '= SUM of ' + vals.length + ' GL lines = ' + Math.round(vals.reduce((a, b) => a + b, 0));
  }
  if (field === 'budget') {
    const vals = lines.map(l => Math.round(l.current_budget || 0)).filter(v => v !== 0);
    if (vals.length <= 8) return '= ' + (vals.length ? vals.join(' + ') : '0');
    return '= SUM of ' + vals.length + ' GL lines = ' + Math.round(vals.reduce((a, b) => a + b, 0));
  }
  // Subtotal rows (field starts with 'sub_')
  if (field.startsWith('sub_')) {
    const subField = field.replace('sub_', '');
    const catIds = data.catIds || [];
    if (catIds.length && subField !== 'var' && subField !== 'pct') {
      const vals = catIds.map(cid => {
        const cd = _sumCatData[cid];
        if (!cd) return 0;
        const t = sumLines(cd.lines);
        return Math.round(t[subField] || 0);
      });
      return '= ' + vals.join(' + ');
    }
    if (subField === 'var') {
      const t = sumLines(lines);
      return '= ' + Math.round(t.budget) + ' - ' + Math.round(t.forecast);
    }
    if (subField === 'pct') {
      const t = sumLines(lines);
      return t.forecast ? '= (' + Math.round(t.budget) + ' - ' + Math.round(t.forecast) + ') / ' + Math.round(t.forecast) : '= 0';
    }
    // Fallback: sum all lines for the field
    const t = sumLines(lines);
    return '= SUM of ' + lines.length + ' GL lines = ' + Math.round(t[subField] || 0);
  }
  return '';
}

function sumFxClick(el) {
  if (_activeSumFxCell && _activeSumFxCell !== el) {
    _activeSumFxCell.style.border = '';
    _activeSumFxCell.style.borderRadius = '';
    _activeSumFxCell.style.background = '';
  }
  _activeSumFxCell = el;
  const bar = document.getElementById('sumFormulaBar');
  const label = document.getElementById('sumFormulaLabel');
  if (!bar || !label) return;

  label.textContent = el.dataset.label || el.id;
  label.style.display = 'inline';
  bar.style.display = 'block';

  bar.value = _buildSumFormula(el.id);
  _sumFormulaOriginal = bar.value;
  _showSumButtons(true);
  sumFormulaPreview();

  el.style.border = '2px solid var(--blue)';
  el.style.borderRadius = '4px';
  el.style.background = '#ecfdf5';

  bar.focus({ preventScroll: true });
  bar.setSelectionRange(bar.value.length, bar.value.length);
}

function sumFormulaPreview() {
  const bar = document.getElementById('sumFormulaBar');
  const preview = document.getElementById('sumFormulaPreview');
  if (!bar || !preview || !_activeSumFxCell) return;
  const typed = bar.value.trim();
  if (!typed) { preview.style.display = 'none'; return; }
  const result = safeEvalFormula(typed);
  if (result !== null) {
    const field = (_sumCatData[_activeSumFxCell.id] || {}).field || '';
    if (field === 'pct' || field === 'sub_pct') {
      preview.textContent = '= ' + result.toFixed(1) + '%';
    } else {
      preview.textContent = '= $' + Math.round(result).toLocaleString();
    }
    preview.style.color = 'var(--green)';
  } else {
    preview.textContent = '';
  }
  preview.style.display = result !== null ? 'inline-block' : 'none';
}

function sumFormulaAccept() {
  // Summary is read-only — just dismiss
  sumFormulaCancel();
}

function sumFormulaCancel() {
  const bar = document.getElementById('sumFormulaBar');
  if (bar) bar.value = _sumFormulaOriginal;
  _showSumButtons(false);
  const preview = document.getElementById('sumFormulaPreview');
  if (preview) preview.style.display = 'none';
  if (_activeSumFxCell) {
    _activeSumFxCell.style.border = '';
    _activeSumFxCell.style.borderRadius = '';
    _activeSumFxCell.style.background = '';
  }
}

function sumFormulaKeydown(e) {
  if (e.key === 'Escape') { e.preventDefault(); sumFormulaCancel(); }
}

document.addEventListener('click', function(e) {
  if (!_activeSumFxCell) return;
  const wrap = document.getElementById('sumFormulaBarWrap');
  if (_activeSumFxCell.contains(e.target)) return;
  if (wrap && wrap.contains(e.target)) return;
  _activeSumFxCell.style.border = '';
  _activeSumFxCell.style.borderRadius = '';
  _activeSumFxCell.style.background = '';
  _activeSumFxCell = null;
  const bar = document.getElementById('sumFormulaBar');
  const label = document.getElementById('sumFormulaLabel');
  const preview = document.getElementById('sumFormulaPreview');
  if (bar) { bar.value = ''; bar.placeholder = 'Click any fx cell to view its formula...'; }
  if (label) label.style.display = 'none';
  if (preview) preview.style.display = 'none';
  _showSumButtons(false);
});

function renderSummary() {
  const content = document.getElementById('mainContent');
  _sumCatData = {};
  let allLines = [];
  SHEET_ORDER.forEach(s => { allLines = allLines.concat(SHEETS[s] || []); });
  const incomeLines = SHEETS['Income'] || [];
  const expenseLines = allLines.filter(l => l.sheet_name !== 'Income');

  const inc = sumLines(incomeLines);
  const exp = sumLines(expenseLines);
  const noiProposed = inc.proposed - exp.proposed;
  const noiPrior = inc.prior - exp.prior;

  const fxBadge = '<span style="display:inline-block; background:#4ade80; color:#fff; font-size:8px; font-weight:700; padding:1px 3px; border-radius:3px; margin-left:3px; vertical-align:middle;">fx</span>';

  // Helper: make an fx cell for the summary table
  let _cellIdx = 0;
  function sfx(val, label, field, lines, cls, catIds) {
    const id = 'sum_' + (++_cellIdx);
    _sumCatData[id] = {field: field, lines: lines, catIds: catIds || []};
    const extraCls = cls || '';
    return '<td class="num ' + extraCls + '" id="' + id + '" data-label="' + label.replace(/"/g, '&quot;') + '" style="cursor:pointer;" onclick="sumFxClick(this)">' + val + fxBadge + '</td>';
  }
  // Plain cell (Prior Year — no formula bar)
  function pln(val, cls) { return '<td class="num ' + (cls || '') + '">' + val + '</td>'; }

  let html = '<div class="summary-cards">' +
    '<div class="card"><div class="label">Total Income</div><div class="value">' + fmt(inc.proposed) + '</div>' +
    '<div class="delta ' + (inc.proposed >= inc.prior ? 'delta-down' : 'delta-up') + '">' + (inc.prior ? ((inc.proposed/inc.prior-1)*100).toFixed(1) + '% vs prior' : '') + '</div></div>' +
    '<div class="card"><div class="label">Total Expenses</div><div class="value">' + fmt(exp.proposed) + '</div>' +
    '<div class="delta ' + (exp.proposed <= exp.prior ? 'delta-down' : 'delta-up') + '">' + (exp.prior ? ((exp.proposed/exp.prior-1)*100).toFixed(1) + '% vs prior' : '') + '</div></div>' +
    '<div class="card"><div class="label">Net Operating Income</div><div class="value">' + fmt(noiProposed) + '</div>' +
    '<div class="delta ' + (noiProposed >= noiPrior ? 'delta-down' : 'delta-up') + '">' + fmt(noiProposed - noiPrior) + ' vs prior</div></div>' +
    '<div class="card"><div class="label">Operating Ratio</div><div class="value">' + (inc.proposed ? (exp.proposed/inc.proposed*100).toFixed(1) + '%' : '\u2014') + '</div>' +
    '<div class="delta" style="color:#94a3b8;">Expenses / Income</div></div></div>';

  // Formula bar
  html += '<div id="sumFormulaBarWrap" style="display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200,#e2e8f0); border-radius:8px; margin-bottom:12px;">' +
    '<span style="font-size:11px; font-weight:700; color:#3b82f6; background:#dbeafe; border:1px solid #3b82f6; border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>' +
    '<span id="sumFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:#64748b; white-space:nowrap; min-width:120px;"></span>' +
    '<input id="sumFormulaBar" type="text" readonly placeholder="Click any fx cell to view its formula..." style="flex:1; padding:6px 10px; border:1px solid #cbd5e1; border-radius:4px; font-size:13px; font-family:monospace; background:white;" oninput="sumFormulaPreview()" onkeydown="sumFormulaKeydown(event)">' +
    '<span id="sumFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:#22c55e; white-space:nowrap; min-width:80px; text-align:right;"></span>' +
    '<button id="sumFormulaAccept" style="display:none; padding:4px 14px; font-size:12px; font-weight:600; background:#22c55e; color:white; border:none; border-radius:4px; cursor:pointer;" onclick="sumFormulaAccept()">OK</button>' +
    '<button id="sumFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:#e2e8f0; color:#334155; border:none; border-radius:4px; cursor:pointer;" onclick="sumFormulaCancel()">Close</button>' +
    '</div>';

  // Summary table with expanded columns
  html += '<table><thead><tr><th>Category</th>' +
    '<th class="num">Prior Year</th>' +
    '<th class="num">YTD Actual</th>' +
    '<th class="num">Estimate</th>' +
    '<th class="num">Forecast</th>' +
    '<th class="num">Curr Budget</th>' +
    '<th class="num">Proposed</th>' +
    '<th class="num">$ Var</th>' +
    '<th class="num">% Chg</th>' +
    '</tr></thead><tbody>';

  SHEET_ORDER.forEach(s => {
    const cats = CATEGORIES[s];
    const sheetLines = SHEETS[s] || [];
    if (cats) {
      const catCellIds = [];
      cats.forEach(cat => {
        const gl = sheetLines.filter(cat.match);
        if (gl.length === 0) return;
        const t = sumLines(gl);
        const v = t.budget - t.forecast;
        const p = t.forecast ? ((t.budget - t.forecast)/t.forecast)*100 : 0;
        const lbl = cat.label;
        // Track this category's cell ID base for subtotal formulas
        const baseIdx = _cellIdx + 1;
        html += '<tr><td style="padding-left:24px;">' + lbl + '</td>' +
          pln(fmt(t.prior)) +
          sfx(fmt(t.ytd), lbl + ' / YTD', 'ytd', gl) +
          sfx(fmt(t.estimate), lbl + ' / Estimate', 'estimate', gl) +
          sfx(fmt(t.forecast), lbl + ' / Forecast', 'forecast', gl) +
          sfx(fmt(t.budget), lbl + ' / Curr Budget', 'budget', gl) +
          sfx(fmt(t.proposed), lbl + ' / Proposed', 'proposed', gl) +
          sfx(fmt(v), lbl + ' / $ Var', 'var', gl, v >= 0 ? 'variance-pos' : 'variance-neg') +
          sfx(p.toFixed(1) + '%', lbl + ' / % Chg', 'pct', gl) +
          '</tr>';
        // Save category cell IDs for subtotal reference
        catCellIds.push({ytd:'sum_'+(baseIdx), est:'sum_'+(baseIdx+1), fcst:'sum_'+(baseIdx+2), bud:'sum_'+(baseIdx+3), prop:'sum_'+(baseIdx+4)});
      });
      // Sheet subtotal
      const st = sumLines(sheetLines);
      const sv = st.budget - st.forecast;
      const sp = st.forecast ? ((st.budget - st.forecast)/st.forecast*100) : 0;
      html += '<tr class="subtotal"><td>' + s + '</td>' +
        pln(fmt(st.prior)) +
        sfx(fmt(st.ytd), s + ' Total / YTD', 'sub_ytd', sheetLines) +
        sfx(fmt(st.estimate), s + ' Total / Estimate', 'sub_estimate', sheetLines) +
        sfx(fmt(st.forecast), s + ' Total / Forecast', 'sub_forecast', sheetLines) +
        sfx(fmt(st.budget), s + ' Total / Curr Budget', 'sub_budget', sheetLines) +
        sfx(fmt(st.proposed), s + ' Total / Proposed', 'sub_proposed', sheetLines) +
        sfx(fmt(sv), s + ' Total / $ Var', 'sub_var', sheetLines, sv >= 0 ? 'variance-pos' : 'variance-neg') +
        sfx(sp.toFixed(1) + '%', s + ' Total / % Chg', 'sub_pct', sheetLines) +
        '</tr>';
    } else {
      const t = sumLines(sheetLines);
      const v = t.budget - t.forecast;
      const p = t.forecast ? ((t.budget - t.forecast)/t.forecast*100) : 0;
      html += '<tr class="subtotal"><td>' + s + '</td>' +
        pln(fmt(t.prior)) +
        sfx(fmt(t.ytd), s + ' / YTD', 'ytd', sheetLines) +
        sfx(fmt(t.estimate), s + ' / Estimate', 'estimate', sheetLines) +
        sfx(fmt(t.forecast), s + ' / Forecast', 'forecast', sheetLines) +
        sfx(fmt(t.budget), s + ' / Curr Budget', 'budget', sheetLines) +
        sfx(fmt(t.proposed), s + ' / Proposed', 'proposed', sheetLines) +
        sfx(fmt(v), s + ' / $ Var', 'var', sheetLines, v >= 0 ? 'variance-pos' : 'variance-neg') +
        sfx(p.toFixed(1) + '%', s + ' / % Chg', 'pct', sheetLines) +
        '</tr>';
    }
  });

  // Total Operating Expenses
  const totalV = exp.budget - exp.forecast;
  const totalPct = exp.forecast ? ((exp.budget - exp.forecast)/exp.forecast*100).toFixed(1) : '0.0';
  html += '<tr class="sheet-total"><td>Total Operating Expenses</td>' +
    pln(fmt(exp.prior)) +
    sfx(fmt(exp.ytd), 'Total Expenses / YTD', 'ytd', expenseLines) +
    sfx(fmt(exp.estimate), 'Total Expenses / Estimate', 'estimate', expenseLines) +
    sfx(fmt(exp.forecast), 'Total Expenses / Forecast', 'forecast', expenseLines) +
    sfx(fmt(exp.budget), 'Total Expenses / Curr Budget', 'budget', expenseLines) +
    sfx(fmt(exp.proposed), 'Total Expenses / Proposed', 'proposed', expenseLines) +
    sfx(fmt(totalV), 'Total Expenses / $ Var', 'var', expenseLines, totalV >= 0 ? 'variance-pos' : 'variance-neg') +
    sfx(totalPct + '%', 'Total Expenses / % Chg', 'pct', expenseLines) +
    '</tr>';

  // NOI
  const noiBudget = inc.budget - exp.budget;
  const noiForecast = inc.forecast - exp.forecast;
  const noiV = noiBudget - noiForecast;
  const noiPct = noiForecast ? ((noiBudget - noiForecast)/noiForecast*100).toFixed(1) : '0.0';
  html += '<tr class="sheet-total"><td>Net Operating Income</td>' +
    pln(fmt(noiPrior)) +
    sfx(fmt(inc.ytd - exp.ytd), 'NOI / YTD', 'sub_ytd', allLines) +
    sfx(fmt(inc.estimate - exp.estimate), 'NOI / Estimate', 'sub_estimate', allLines) +
    sfx(fmt(inc.forecast - exp.forecast), 'NOI / Forecast', 'sub_forecast', allLines) +
    sfx(fmt(inc.budget - exp.budget), 'NOI / Curr Budget', 'sub_budget', allLines) +
    sfx(fmt(noiProposed), 'NOI / Proposed', 'sub_proposed', allLines) +
    sfx(fmt(noiV), 'NOI / $ Var', 'sub_var', allLines, noiV >= 0 ? 'variance-neg' : 'variance-pos') +
    sfx(noiPct + '%', 'NOI / % Chg', 'sub_pct', allLines) +
    '</tr>';

  html += '</tbody></table>';
  content.innerHTML = html;
}

function renderSheet(sheetName) {
  const content = document.getElementById('mainContent');
  const lines = SHEETS[sheetName] || [];
  const estLabel = MONTH_ABBR[YTD_MONTHS] + '-Dec';

  let html = '<table><thead><tr>' +
    '<th>GL Code</th><th>Description</th>' +
    '<th class="num">Prior Year</th><th class="num">YTD Actual</th>' +
    '<th class="num">' + estLabel + ' Est</th><th class="num">12 Mo Forecast</th>' +
    '<th class="num">Proposed Budget</th><th class="num">$ Variance</th><th class="num">% Change</th>' +
    '</tr></thead><tbody>';

  const cats = CATEGORIES[sheetName];

  function buildRow(l) {
    const prior = l.prior_year || 0;
    const forecast = computeForecast(l);
    const proposed = l.proposed_budget || (forecast * (1 + (l.increase_pct || 0)));
    const budget = l.current_budget || 0;
    const v = budget - forecast;
    const p = forecast ? ((budget - forecast)/forecast*100) : 0;
    return '<tr><td style="font-family:monospace; font-size:12px;">' + l.gl_code + '</td><td>' + l.description + '</td>' +
      '<td class="num">' + fmt(prior) + '</td><td class="num">' + fmt(l.ytd_actual || 0) + '</td>' +
      '<td class="num">' + fmt(computeEstimate(l)) + '</td><td class="num">' + fmt(forecast) + '</td>' +
      '<td class="num" style="font-weight:600;">' + fmt(proposed) + '</td>' +
      '<td class="num ' + (v >= 0 ? 'variance-pos' : 'variance-neg') + '">' + fmt(v) + '</td>' +
      '<td class="num">' + p.toFixed(1) + '%</td></tr>';
  }

  function buildSubtotal(label, ls) {
    const t = sumLines(ls);
    const v = t.budget - t.forecast;
    return '<tr class="subtotal"><td colspan="2">' + label + '</td>' +
      '<td class="num">' + fmt(t.prior) + '</td><td class="num"></td><td class="num"></td><td class="num">' + fmt(t.forecast) + '</td>' +
      '<td class="num">' + fmt(t.proposed) + '</td><td class="num ' + (v >= 0 ? 'variance-pos' : 'variance-neg') + '">' + fmt(v) + '</td>' +
      '<td class="num">' + (t.forecast ? ((t.budget - t.forecast)/t.forecast*100).toFixed(1) : '0.0') + '%</td></tr>';
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
  const tv = t.budget - t.forecast;
  html += '<tr class="sheet-total"><td colspan="2">Total ' + sheetName + '</td>' +
    '<td class="num">' + fmt(t.prior) + '</td><td class="num"></td><td class="num"></td><td class="num">' + fmt(t.forecast) + '</td>' +
    '<td class="num">' + fmt(t.proposed) + '</td><td class="num ' + (tv >= 0 ? 'variance-pos' : 'variance-neg') + '">' + fmt(tv) + '</td>' +
    '<td class="num">' + (t.forecast ? ((t.budget - t.forecast)/t.forecast*100).toFixed(1) : '0.0') + '%</td></tr>';
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
