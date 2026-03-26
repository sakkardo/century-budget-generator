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

        # Relationships (use backref on child side to avoid forward-reference issues)
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
                "initiated_by": self.initiated_by,
                "initiated_at": self.initiated_at.isoformat() if self.initiated_at else None,
                "presentation_token": self.presentation_token,
                "approved_by": self.approved_by,
                "approved_at": self.approved_at.isoformat() if self.approved_at else None,
                "increase_pct": self.increase_pct,
                "effective_date": self.effective_date,
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
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

        # Proposed budget (computed or manually entered)
        proposed_budget = db.Column(db.Float, default=0.0)

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

    def store_all_lines(entity_code, building_name, gl_data, template_path, assumptions=None):
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
            pass

        # Group lines by sheet for tabbed view
        sheets = {}
        for l in lines:
            sn = l.sheet_name or "Unmapped"
            if sn not in sheets:
                sheets[sn] = []
            sheets[sn].append(l.to_dict())

        # Ordered sheet tab names
        sheet_order = ["Income", "Payroll", "Energy", "Water & Sewer", "Repairs & Supplies", "Gen & Admin", "Unmapped"]

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

        return jsonify({
            "budget": budget.to_dict(),
            "lines": [l.to_dict() for l in lines],
            "sheets": sheets,
            "sheet_order": [s for s in sheet_order if s in sheets],
            "assignments": {"fa": fa_name, "pm": pm_name},
            "expenses": expense_data,
            "audit": audit_data,
            "assumptions": assumptions,
            "ytd_months": ytd_months,
            "remaining_months": remaining_months
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
            if "increase_pct" in line_data:
                line.increase_pct = float(line_data["increase_pct"] or 0)
            if "notes" in line_data:
                line.notes = line_data["notes"]
            if "proposed_budget" in line_data:
                line.proposed_budget = float(line_data["proposed_budget"] or 0)

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


    # ─── HTML Templates ─────────────────────────────────────────────────────

    return (bp, {"User": User, "BuildingAssignment": BuildingAssignment, "Budget": Budget, "BudgetLine": BudgetLine},
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
    .tracks { grid-template-columns: 1fr; }
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

  <!-- Historical Actuals (from Audited Financials) -->
  <div class="section" id="auditActualsSection" style="display:none;">
    <h2>Historical Actuals <span style="font-size:13px; font-weight:400; color:var(--gray-500);">(from Audited Financials)</span></h2>
    <table id="auditActualsTable">
      <thead id="auditActualsHead"></thead>
      <tbody id="auditActualsBody"></tbody>
    </table>
  </div>

  <!-- Reclass Suggestions (shown if any exist) -->
  <div class="section" id="reclassSuggestions" style="display:none;">
    <h2>Pending Reclass Suggestions <span style="font-size:13px; font-weight:400; color:var(--gray-500);">(from PM)</span></h2>
    <table id="reclassTable">
      <thead><tr><th>From GL</th><th>To GL</th><th>Amount</th><th>PM Notes</th><th>Action</th></tr></thead>
      <tbody id="reclassBody"></tbody>
    </table>
  </div>

  <!-- Budget Workbook (Tabbed) -->
  <div class="section">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
      <h2>Budget Workbook</h2>
      <a href="" id="downloadExcelBtn" class="btn" style="background:var(--green); color:white; text-decoration:none; font-size:13px; padding:8px 16px; border-radius:6px;">Download Excel</a>
    </div>
    <div id="sheetTabs" style="display:flex; gap:4px; border-bottom:2px solid var(--gray-200); margin-bottom:0; flex-wrap:wrap;"></div>
    <div id="sheetContent" style="overflow-x:auto;"></div>
    <div id="faSaveIndicator" style="font-size:12px; color:var(--green); margin-top:8px;"></div>
  </div>

  </div><!-- end detailContent -->
</div>

<script>
const entityCode = '{{ entity_code }}';
let allSheets = {};  // populated in loadDetail, used by Budget Summary
let YTD_MONTHS = 2;  // updated from API response
let REMAINING_MONTHS = 10;  // updated from API response

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

  // Historical Actuals Panel (from audited financials)
  const auditYears = data.audit.exists ? data.audit.years : {};
  const auditYearKeys = Object.keys(auditYears).sort().reverse();
  const catMapping = data.audit.category_mapping || {};

  if (auditYearKeys.length > 0) {
    document.getElementById('auditActualsSection').style.display = '';
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

    renderSheet(sheetOrder[0], sheets[sheetOrder[0]], tabsDiv.firstChild);
  }
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

  html += '</div>';
  contentDiv.innerHTML = html;
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
  const ytd = l.ytd_actual || 0;
  const accrual = l.accrual_adj || 0;
  const unpaid = l.unpaid_bills || 0;
  const prior = l.prior_year || 0;
  const base = ytd + accrual + unpaid;
  if (base >= prior && prior > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
  return Math.max(prior - base, 0);
}

function faComputeForecast(l) {
  return (l.ytd_actual || 0) + (l.accrual_adj || 0) + (l.unpaid_bills || 0) + faComputeEstimate(l);
}

function faGetFormulaTooltip(l, field) {
  const ytd = l.ytd_actual || 0;
  const accrual = l.accrual_adj || 0;
  const unpaid = l.unpaid_bills || 0;
  const prior = l.prior_year || 0;
  const base = ytd + accrual + unpaid;
  const estimate = faComputeEstimate(l);
  const forecast = faComputeForecast(l);
  const incPct = l.increase_pct || 0;

  if (field === 'estimate') {
    if (base >= prior && prior > 0) {
      return 'Annualized: (' + fmt(ytd) + ' + ' + fmt(accrual) + ' + ' + fmt(unpaid) + ') / ' + YTD_MONTHS + ' × ' + REMAINING_MONTHS + ' = ' + fmt(estimate);
    } else {
      return 'Prior Year Adj: ' + fmt(prior) + ' - (' + fmt(ytd) + ' + ' + fmt(accrual) + ' + ' + fmt(unpaid) + ') = ' + fmt(estimate);
    }
  }
  if (field === 'forecast') {
    return fmt(ytd) + ' + ' + fmt(accrual) + ' + ' + fmt(unpaid) + ' + ' + fmt(estimate) + ' = ' + fmt(forecast);
  }
  if (field === 'proposed') {
    const proposed = l.proposed_budget || (forecast * (1 + incPct));
    return fmt(forecast) + ' × (1 + ' + (incPct * 100).toFixed(1) + '%) = ' + fmt(proposed);
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

  if (!sheetLines || sheetLines.length === 0) {
    contentDiv.innerHTML = '<p style="padding:24px; color:var(--gray-500);">No data for this sheet.</p>';
    return;
  }

  // All sheets are editable for the FA — this is the budget workbench
  renderEditableSheet(sheetName, sheetLines, contentDiv);
}

function renderBudgetSummary(contentDiv) {
  const thStyle = 'text-align:right; padding:10px 12px; white-space:nowrap;';
  let html = '<div style="margin-bottom:8px; display:flex; align-items:center; gap:12px;">' +
    '<span style="font-size:14px; color:var(--gray-500);">Executive budget overview — all figures roll up from detail sheets</span></div>';
  html += '<table style="width:100%; border-collapse:collapse; font-size:14px;">' +
    '<thead><tr style="background:var(--gray-100); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
    '<th style="text-align:left; padding:10px 12px; width:35%;">Category</th>' +
    '<th style="' + thStyle + '">Prior Year<br>Actual</th>' +
    '<th style="' + thStyle + '">Current<br>Budget</th>' +
    '<th style="' + thStyle + '">Proposed<br>Budget</th>' +
    '<th style="' + thStyle + '">$<br>Variance</th>' +
    '<th style="' + thStyle + '">%<br>Change</th>' +
    '</tr></thead><tbody>';

  let totalIncome = {prior:0, budget:0, proposed:0};
  let totalExpense = {prior:0, budget:0, proposed:0};

  SUMMARY_ROWS.forEach((sr, idx) => {
    const sheetLines = allSheets[sr.sheet] || [];
    let lines = sheetLines;
    if (sr.rowRange) {
      lines = sheetLines.filter(l => l.row_num >= sr.rowRange[0] && l.row_num <= sr.rowRange[1]);
    }
    let prior = 0, budget = 0, proposed = 0;
    lines.forEach(l => {
      prior += l.prior_year || 0;
      budget += l.current_budget || 0;
      const forecast = faComputeForecast(l);
      proposed += l.proposed_budget || (forecast * (1 + (l.increase_pct || 0)));
    });

    const variance = proposed - prior;
    const pctChange = prior ? (proposed / prior - 1) : 0;
    const varColor = sr.type === 'income'
      ? (variance >= 0 ? 'var(--green)' : 'var(--red)')
      : (variance >= 0 ? 'var(--red)' : 'var(--green)');

    if (sr.type === 'income') { totalIncome.prior += prior; totalIncome.budget += budget; totalIncome.proposed += proposed; }
    else { totalExpense.prior += prior; totalExpense.budget += budget; totalExpense.proposed += proposed; }

    // Bold for income row, normal for expense detail
    const isIncomeRow = idx === 0;
    const rowStyle = isIncomeRow ? 'font-weight:600; background:var(--blue-50, #eff6ff);' : '';
    html += '<tr style="border-bottom:1px solid var(--gray-100); ' + rowStyle + '">' +
      '<td style="padding:10px 12px;">' + sr.label + '</td>' +
      '<td style="text-align:right; padding:10px 12px;">' + fmt(prior) + '</td>' +
      '<td style="text-align:right; padding:10px 12px;">' + fmt(budget) + '</td>' +
      '<td style="text-align:right; padding:10px 12px;">' + fmt(proposed) + '</td>' +
      '<td style="text-align:right; padding:10px 12px; color:' + varColor + ';">' + fmt(variance) + '</td>' +
      '<td style="text-align:right; padding:10px 12px;">' + (pctChange * 100).toFixed(1) + '%</td></tr>';

    // After last expense row, add totals
    if (idx === SUMMARY_ROWS.length - 1) {
      const tePrior = totalExpense.prior, teBudget = totalExpense.budget, teProposed = totalExpense.proposed;
      const teVar = teProposed - tePrior;
      const tePct = tePrior ? (teProposed / tePrior - 1) : 0;
      html += '<tr style="font-weight:700; background:var(--gray-100); border-top:2px solid var(--gray-300);"><td style="padding:10px 12px;">Total Operating Expenses</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(tePrior) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(teBudget) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(teProposed) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(teVar) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + (tePct * 100).toFixed(1) + '%</td></tr>';

      // NOI
      const noiPrior = totalIncome.prior - tePrior;
      const noiBudget = totalIncome.budget - teBudget;
      const noiProposed = totalIncome.proposed - teProposed;
      const noiVar = noiProposed - noiPrior;
      const noiPct = noiPrior ? (noiProposed / noiPrior - 1) : 0;
      const noiColor = noiVar >= 0 ? 'var(--green)' : 'var(--red)';
      html += '<tr style="font-weight:700; background:var(--blue-50, #eff6ff); border-top:2px solid var(--primary);"><td style="padding:10px 12px;">Net Operating Income</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(noiPrior) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(noiBudget) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + fmt(noiProposed) + '</td>' +
        '<td style="text-align:right; padding:10px 12px; color:' + noiColor + ';">' + fmt(noiVar) + '</td>' +
        '<td style="text-align:right; padding:10px 12px;">' + (noiPct * 100).toFixed(1) + '%</td></tr>';
    }
  });

  html += '</tbody></table>';
  contentDiv.innerHTML = html;
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

function renderEditableSheet(sheetName, sheetLines, contentDiv) {
  const thStyle = 'text-align:right; padding:8px; white-space:nowrap;';
  const inputStyle = 'text-align:right; border:1px solid var(--gray-200); border-radius:4px; padding:4px; font-size:12px;';
  // Cell background colors: system-imported=light blue, formula-computed=light green, FA-editable=white
  const sysBg = '#eff6ff';  // light blue — data from Yardi
  const formulaBg = '#f0fdf4';  // light green — computed
  const faBg = '#ffffff';  // white — FA editable

  // Legend
  let html = '<div style="display:flex; gap:16px; margin-bottom:8px; font-size:11px; color:var(--gray-500);">' +
    '<span><span style="display:inline-block;width:12px;height:12px;background:' + sysBg + ';border:1px solid #bfdbfe;border-radius:2px;vertical-align:middle;margin-right:4px;"></span>Yardi Data</span>' +
    '<span><span style="display:inline-block;width:12px;height:12px;background:' + formulaBg + ';border:1px solid #bbf7d0;border-radius:2px;vertical-align:middle;margin-right:4px;"></span>Calculated</span>' +
    '<span><span style="display:inline-block;width:12px;height:12px;background:' + faBg + ';border:1px solid #e5e7eb;border-radius:2px;vertical-align:middle;margin-right:4px;"></span>FA Editable</span>' +
    '</div>';

  html += '<table style="width:100%; border-collapse:collapse; font-size:13px;">' +
    '<thead><tr style="background:var(--gray-100); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
    '<th style="text-align:left; padding:8px;">GL Code</th>' +
    '<th style="text-align:left; padding:8px;">Description</th>' +
    '<th style="text-align:left; padding:8px;">Notes</th>' +
    '<th style="' + thStyle + '">Prior Year<br>Actual</th>' +
    '<th style="' + thStyle + '">YTD<br>Actual</th>' +
    '<th style="' + thStyle + '">Accrual<br>Adj</th>' +
    '<th style="' + thStyle + '">Unpaid<br>Bills</th>' +
    '<th style="' + thStyle + '">YTD<br>Budget</th>' +
    '<th style="' + thStyle + '">Sep-Dec<br>Estimate</th>' +
    '<th style="' + thStyle + '">12 Month<br>Forecast</th>' +
    '<th style="' + thStyle + '">Current<br>Budget</th>' +
    '<th style="' + thStyle + '">Increase<br>%</th>' +
    '<th style="' + thStyle + '">Proposed<br>Budget</th>' +
    '<th style="' + thStyle + '">$<br>Variance</th>' +
    '<th style="' + thStyle + '">%<br>Change</th>' +
    '</tr></thead><tbody>';

  const catConfig = SHEET_CATEGORIES[sheetName];

  function buildLineRow(l) {
    const prior = l.prior_year || 0;
    const ytd = l.ytd_actual || 0;
    const ytdBudget = l.ytd_budget || 0;
    const budget = l.current_budget || 0;
    const estimate = faComputeEstimate(l);
    const forecast = faComputeForecast(l);
    const proposed = l.proposed_budget || (forecast * (1 + (l.increase_pct || 0)));
    const variance = proposed - prior;
    const pctChange = prior ? (proposed / prior - 1) : 0;
    const incPct = ((l.increase_pct || 0) * 100).toFixed(1);
    const varColor = variance >= 0 ? 'var(--red)' : 'var(--green)';
    const reclassBadge = l.reclass_to_gl ? ' <span style="background:var(--orange-light); color:var(--orange); font-size:10px; padding:2px 6px; border-radius:10px;">Reclass</span>' : '';

    // Tooltip for formula cells
    const estTip = faGetFormulaTooltip(l, 'estimate');
    const fcstTip = faGetFormulaTooltip(l, 'forecast');
    const propTip = faGetFormulaTooltip(l, 'proposed');

    return '<tr style="border-bottom:1px solid var(--gray-100);">' +
      '<td style="font-family:monospace; font-size:12px; padding:6px 8px; white-space:nowrap;">' + l.gl_code + reclassBadge + '</td>' +
      '<td style="padding:6px 8px;">' + l.description + '</td>' +
      '<td style="padding:6px 8px;"><input type="text" value="' + (l.notes || '').replace(/"/g, '&quot;') + '" style="width:100px; ' + inputStyle + '" onchange="faAutoSave(\'' + l.gl_code + '\', \'notes\', this.value)"></td>' +
      '<td style="text-align:right; padding:6px 8px; background:' + sysBg + ';">' + fmt(prior) + '</td>' +
      '<td style="text-align:right; padding:6px 8px; background:' + sysBg + ';">' + fmt(ytd) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(l.accrual_adj || 0) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(l.unpaid_bills || 0) + '</td>' +
      '<td style="text-align:right; padding:6px 8px; background:' + sysBg + ';">' + fmt(ytdBudget) + '</td>' +
      '<td style="text-align:right; padding:6px 8px; background:' + formulaBg + '; cursor:help;" title="' + estTip + '">' + fmt(estimate) + '</td>' +
      '<td style="text-align:right; padding:6px 8px; background:' + formulaBg + '; cursor:help;" title="' + fcstTip + '">' + fmt(forecast) + '</td>' +
      '<td style="text-align:right; padding:6px 8px; background:' + sysBg + ';">' + fmt(budget) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;"><input type="number" step="0.1" value="' + incPct + '" style="width:60px; ' + inputStyle + '" onchange="faAutoSave(\'' + l.gl_code + '\', \'increase_pct\', this.value / 100)"></td>' +
      '<td style="text-align:right; padding:6px 8px;"><input type="number" step="1" value="' + Math.round(proposed) + '" style="width:90px; ' + inputStyle + '" onchange="faAutoSave(\'' + l.gl_code + '\', \'proposed_budget\', this.value)" title="' + propTip + '"></td>' +
      '<td style="text-align:right; padding:6px 8px; color:' + varColor + ';">' + fmt(variance) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + (pctChange * 100).toFixed(1) + '%</td></tr>';
  }

  function sumLines(lines) {
    const t = {prior:0, ytd:0, ytdBudget:0, estimate:0, forecast:0, budget:0, proposed:0};
    lines.forEach(l => {
      t.prior += l.prior_year || 0;
      t.ytd += l.ytd_actual || 0;
      t.ytdBudget += l.ytd_budget || 0;
      t.estimate += faComputeEstimate(l);
      t.forecast += faComputeForecast(l);
      t.budget += l.current_budget || 0;
      const forecast = faComputeForecast(l);
      t.proposed += l.proposed_budget || (forecast * (1 + (l.increase_pct || 0)));
    });
    return t;
  }

  function buildSubtotalRow(label, t) {
    const v = t.proposed - t.prior;
    const p = t.prior ? (t.proposed / t.prior - 1) : 0;
    return '<tr style="font-weight:600; background:#f8fafc; border-top:1px solid var(--gray-200); border-bottom:1px solid var(--gray-200);">' +
      '<td style="padding:8px;" colspan="2">' + label + '</td><td></td>' +
      '<td style="text-align:right; padding:8px;">' + fmt(t.prior) + '</td>' +
      '<td style="text-align:right; padding:8px;">' + fmt(t.ytd) + '</td>' +
      '<td></td><td></td>' +
      '<td style="text-align:right; padding:8px;">' + fmt(t.ytdBudget) + '</td>' +
      '<td style="text-align:right; padding:8px;">' + fmt(t.estimate) + '</td>' +
      '<td style="text-align:right; padding:8px;">' + fmt(t.forecast) + '</td>' +
      '<td style="text-align:right; padding:8px;">' + fmt(t.budget) + '</td>' +
      '<td></td>' +
      '<td style="text-align:right; padding:8px;">' + fmt(t.proposed) + '</td>' +
      '<td style="text-align:right; padding:8px;">' + fmt(v) + '</td>' +
      '<td style="text-align:right; padding:8px;">' + (p * 100).toFixed(1) + '%</td></tr>';
  }

  if (catConfig) {
    // Render with category groupings and subtotals
    catConfig.groups.forEach(grp => {
      const groupLines = sheetLines.filter(grp.match);
      if (groupLines.length === 0) return;

      // Category header
      html += '<tr><td colspan="15" style="padding:10px 8px 4px; font-weight:700; color:var(--primary); font-size:14px; border-bottom:2px solid var(--primary);">' + grp.label + '</td></tr>';

      groupLines.forEach(l => { html += buildLineRow(l); });

      // Category subtotal
      const t = sumLines(groupLines);
      html += buildSubtotalRow('Total ' + grp.label, t);
    });

    // Render any lines that didn't match a category
    const allGrouped = catConfig.groups.flatMap(g => sheetLines.filter(g.match));
    const ungrouped = sheetLines.filter(l => !allGrouped.includes(l));
    if (ungrouped.length > 0) {
      html += '<tr><td colspan="15" style="padding:10px 8px 4px; font-weight:700; color:var(--gray-500); font-size:14px; border-bottom:2px solid var(--gray-300);">Other</td></tr>';
      ungrouped.forEach(l => { html += buildLineRow(l); });
      html += buildSubtotalRow('Total Other', sumLines(ungrouped));
    }
  } else {
    // Simple sheet — no category groupings, just rows
    sheetLines.forEach(l => { html += buildLineRow(l); });
  }

  // Sheet total
  const totals = sumLines(sheetLines);
  const totalVar = totals.proposed - totals.prior;
  const totalPct = totals.prior ? (totals.proposed / totals.prior - 1) : 0;
  html += '<tr style="font-weight:700; background:var(--gray-100); border-top:2px solid var(--gray-300);"><td style="padding:8px;" colspan="2">Sheet Total</td><td></td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.prior) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.ytd) + '</td>' +
    '<td></td><td></td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.ytdBudget) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.estimate) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.forecast) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.budget) + '</td>' +
    '<td></td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.proposed) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totalVar) + '</td>' +
    '<td style="text-align:right; padding:8px;">' + (totalPct * 100).toFixed(1) + '%</td></tr>';
  html += '</tbody></table>';
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
            <th class="number">Prior Year<br>Actual</th>
            <th class="number">YTD<br>Actual</th>
            <th class="number">Accrual<br>Adj</th>
            <th class="number">Unpaid<br>Bills</th>
            <th class="number">YTD<br>Budget</th>
            <th class="number">Sep-Dec<br>Estimate</th>
            <th class="number">12 Month<br>Forecast</th>
            <th class="number">Current<br>Budget</th>
            <th class="number">Increase<br>%</th>
            <th class="number">Proposed<br>Budget</th>
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
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    const prior = line.prior_year || 0;
    const base = ytd + accrual + unpaid;
    if (base >= prior && prior > 0) {
        return (base / YTD_MONTHS) * REMAINING_MONTHS;
    } else {
        return Math.max(prior - base, 0);
    }
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
            const variance = proposed - (line.prior_year || 0);
            const pctChange = (line.prior_year || 0) ? (proposed / (line.prior_year) - 1) : 0;

            catTotals.prior += (line.prior_year || 0);
            catTotals.ytd += (line.ytd_actual || 0);
            catTotals.ytdBudget += (line.ytd_budget || 0);
            catTotals.estimate += estimate;
            catTotals.forecast += forecast;
            catTotals.budget += (line.current_budget || 0);
            catTotals.proposed += proposed;

            const reclassBadge = line.reclass_to_gl ? ' <span style="background:var(--orange-light); color:var(--orange); font-size:10px; padding:1px 5px; border-radius:8px;">Reclass</span>' : '';

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><a href="#" onclick="toggleInvoices('${line.gl_code}', this); return false;" style="color:var(--blue); text-decoration:none; font-family:monospace;" title="Click to view invoices">${line.gl_code}</a>${reclassBadge}</td>
                <td>${line.description}</td>
                <td><input type="text" value="${(line.notes || '').replace(/"/g, '&quot;')}" data-gl="${line.gl_code}" data-field="notes" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'} style="min-width:100px;"></td>
                <td class="number">${fmt(line.prior_year)}</td>
                <td class="number">${fmt(line.ytd_actual)}</td>
                <td class="number"><input type="number" step="1" value="${Math.round(line.accrual_adj || 0)}" data-gl="${line.gl_code}" data-field="accrual_adj" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number"><input type="number" step="1" value="${Math.round(line.unpaid_bills || 0)}" data-gl="${line.gl_code}" data-field="unpaid_bills" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number">${fmt(line.ytd_budget)}</td>
                <td class="number" id="est_${line.gl_code}">${fmt(estimate)}</td>
                <td class="number" id="fc_${line.gl_code}">${fmt(forecast)}</td>
                <td class="number">${fmt(line.current_budget)}</td>
                <td class="number"><input type="number" step="0.1" value="${((line.increase_pct || 0) * 100).toFixed(1)}" data-gl="${line.gl_code}" data-field="increase_pct" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'} style="width:70px;"></td>
                <td class="number" id="pb_${line.gl_code}">${fmt(proposed)}</td>
                <td class="number" id="var_${line.gl_code}" style="color:${variance >= 0 ? 'var(--red)' : 'var(--green)'};">${fmt(variance)}</td>
                <td class="number" id="pct_${line.gl_code}">${(pctChange * 100).toFixed(1)}%</td>
            `;
            tbody.appendChild(tr);
        });

        // Subtotal
        const catVar = catTotals.proposed - catTotals.prior;
        const subRow = document.createElement('tr');
        subRow.className = 'subtotal-row';
        subRow.innerHTML = `
            <td></td><td>Total ${catLabels[cat]}</td><td></td>
            <td class="number">${fmt(catTotals.prior)}</td>
            <td class="number">${fmt(catTotals.ytd)}</td>
            <td></td><td></td>
            <td class="number">${fmt(catTotals.ytdBudget)}</td>
            <td class="number">${fmt(catTotals.estimate)}</td>
            <td class="number">${fmt(catTotals.forecast)}</td>
            <td class="number">${fmt(catTotals.budget)}</td>
            <td></td>
            <td class="number">${fmt(catTotals.proposed)}</td>
            <td class="number">${fmt(catVar)}</td>
            <td></td>
        `;
        tbody.appendChild(subRow);

        Object.keys(grandTotals).forEach(k => grandTotals[k] += catTotals[k]);
    }

    // Grand total
    const grandVar = grandTotals.proposed - grandTotals.prior;
    const grandPct = grandTotals.prior ? (grandTotals.proposed / grandTotals.prior - 1) : 0;
    const grandRow = document.createElement('tr');
    grandRow.className = 'grand-total';
    grandRow.innerHTML = `
        <td></td><td>GRAND TOTAL R&M</td><td></td>
        <td class="number">${fmt(grandTotals.prior)}</td>
        <td class="number">${fmt(grandTotals.ytd)}</td>
        <td></td><td></td>
        <td class="number">${fmt(grandTotals.ytdBudget)}</td>
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

// Expense distribution drill-down
let _expenseCache = null;

async function fetchExpenseData() {
    if (_expenseCache !== null) return _expenseCache;
    try {
        const res = await fetch('/api/expense-dist/' + ENTITY);
        if (!res.ok) { _expenseCache = false; return null; }
        _expenseCache = await res.json();
        return _expenseCache;
    } catch { _expenseCache = false; return null; }
}

async function toggleInvoices(glCode, linkEl) {
    const row = linkEl.closest('tr');
    const existingDetail = row.nextElementSibling;
    if (existingDetail && existingDetail.classList.contains('invoice-detail-row')) {
        existingDetail.remove();
        return;
    }

    const data = await fetchExpenseData();
    if (!data || !data.gl_groups) {
        const noData = document.createElement('tr');
        noData.className = 'invoice-detail-row';
        noData.innerHTML = '<td colspan="10" style="padding:12px 24px; background:#fef3c7; font-size:13px;">No expense distribution data uploaded yet.</td>';
        row.after(noData);
        return;
    }

    // Find invoices for this GL code — gl_groups is an array of {gl_code, gl_name, invoices, total}
    const glGroup = data.gl_groups.find(g => g.gl_code === glCode);
    if (!glGroup || !glGroup.invoices || glGroup.invoices.length === 0) {
        const noInv = document.createElement('tr');
        noInv.className = 'invoice-detail-row';
        noInv.innerHTML = '<td colspan="10" style="padding:12px 24px; background:var(--gray-50); font-size:13px; color:var(--gray-500);">No invoices found for ' + glCode + '</td>';
        row.after(noInv);
        return;
    }

    const detailRow = document.createElement('tr');
    detailRow.className = 'invoice-detail-row';
    let invoiceHtml = '<td colspan="10" style="padding:0;"><div style="padding:8px 24px; background:var(--gray-50); border-left:3px solid var(--blue);">' +
        '<table style="width:100%; font-size:12px; border-collapse:collapse;">' +
        '<tr style="color:var(--gray-500); font-weight:600;"><td style="padding:4px 8px;">Payee</td><td style="padding:4px 8px;">Invoice #</td><td style="padding:4px 8px;">Date</td><td style="padding:4px 8px; text-align:right;">Amount</td></tr>';

    glGroup.invoices.forEach(inv => {
        invoiceHtml += '<tr style="border-top:1px solid var(--gray-200);">' +
            '<td style="padding:4px 8px;">' + (inv.payee_name || inv.payee_code || '') + '</td>' +
            '<td style="padding:4px 8px;">' + (inv.invoice_num || '') + '</td>' +
            '<td style="padding:4px 8px;">' + (inv.invoice_date || '') + '</td>' +
            '<td style="padding:4px 8px; text-align:right;">' + fmt(inv.amount) + '</td></tr>';
    });

    invoiceHtml += '<tr style="border-top:2px solid var(--gray-300); font-weight:700;"><td colspan="3" style="padding:4px 8px;">Total (' + glGroup.invoices.length + ' invoices)</td>' +
        '<td style="padding:4px 8px; text-align:right;">' + fmt(glGroup.total || 0) + '</td></tr>';
    invoiceHtml += '</table></div></td>';
    detailRow.innerHTML = invoiceHtml;
    row.after(detailRow);
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
    const variance = pb - (line.prior_year || 0);
    const pctChange = (line.prior_year || 0) ? (pb / line.prior_year - 1) : 0;

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
        <td colspan="12" style="padding:12px 24px; background:var(--blue-light); border-left:3px solid var(--blue);">
            <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
                <label style="font-size:12px; font-weight:600;">Suggest reclass to:</label>
                <select id="reclass_target_${glCode}" style="font-size:12px; padding:4px 8px; border:1px solid var(--gray-300); border-radius:4px;">
                    <option value="">-- Select target GL --</option>
                    ${glOptions}
                </select>
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
</script>
</body>
</html>
"""
