"""SQLAlchemy models for the budget workflow (the data layer).

Architecture Phase 3, step 7 (2026-06-08). The 18 models + the pure _parse_backup_json
helper were moved VERBATIM out of workflow.py's create_workflow_blueprint(db). They are
still built inside a factory (register_models(db)) because each model binds to the
injected `db`. workflow.py calls register_models(db), rebinds the names locally, and
returns the SAME workflow_models dict, so every route and app.py resolve unchanged.

Every external name the models reference (proven by an AST scan of all 18 classes):
  db                     -> the register_models(db) parameter (enclosing scope)
  datetime, json         -> stdlib, imported here (module globals)
  BUDGET_YEAR            -> budget_config (leaf)
  derive_lifecycle_stage -> budget_status (leaf)
  _parse_backup_json     -> defined here, inside register_models (enclosing scope)
"""
import json
from datetime import datetime

try:
    from budget_config import BUDGET_YEAR
except ImportError:
    from budget_app.budget_config import BUDGET_YEAR
try:
    from budget_status import derive_lifecycle_stage
except ImportError:
    from budget_app.budget_status import derive_lifecycle_stage


def register_models(db):
    """Define the 18 models with the injected db and return them as a name->class dict."""
    def _parse_backup_json(raw):
        """Parse a BudgetLine.backup_json string into a list of line-item dicts; empty list on any error."""
        if not raw:
            return []
        try:
            val = json.loads(raw)
            return val if isinstance(val, list) else []
        except Exception:
            return []

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
        year = db.Column(db.Integer, default=BUDGET_YEAR)
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

        # Budget Wizard state
        assumptions_history_json = db.Column(db.Text, nullable=True)
        wizard_completed_at = db.Column(db.DateTime, nullable=True)
        wizard_step = db.Column(db.Integer, default=0)
        # Timestamp the FA sent the budget to PM review (status -> pm_pending).
        # Surfaced on the Building Detail page so FAs see "Sent on YYYY-MM-DD"
        # next to the PM panel without having to dig into the revisions log.
        pm_sent_at = db.Column(db.DateTime, nullable=True)
        # Staging area for files the FA has SELECTED (but not yet committed) during
        # the wizard. JSON shape: {source_type: {item_id, filename, selected_at, source}}
        # Empty until FA picks files; cleared after Build Budget commits successfully.
        wizard_selections_json = db.Column(db.Text, nullable=True)

        # Foundation gate (Phase E) — set when FA confirms 2025 audit mapping;
        # required before Step 3+ (Yardi sources, assumptions, Build Budget).
        foundation_confirmed_at = db.Column(db.DateTime, nullable=True, index=True)
        foundation_confirmed_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
        # Explicit "No 2026 approved budget exists for this entity" acknowledgment.
        # Unlocks audit extraction with CENTURY_CATEGORIES fallback for the 24 entities
        # that don\'t have a prior-year approved budget XLSX in SharePoint.
        foundation_no_prior_budget = db.Column(db.Boolean, default=False, nullable=False)

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
                "building_type": self.building_type or "",
                "wizard_step": self.wizard_step or 0,
                "wizard_completed_at": self.wizard_completed_at.isoformat() if self.wizard_completed_at else None,
                "pm_sent_at": self.pm_sent_at.isoformat() if self.pm_sent_at else None,
                "lifecycle_stage": derive_lifecycle_stage(self),
                "wizard_selections": _parse_backup_json(self.wizard_selections_json) if False else (json.loads(self.wizard_selections_json) if self.wizard_selections_json else {}),
                "foundation_confirmed_at": self.foundation_confirmed_at.isoformat() if self.foundation_confirmed_at else None,
                "foundation_confirmed_by": self.foundation_confirmed_by,
                "foundation_no_prior_budget": bool(self.foundation_no_prior_budget),
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
        # FA directive 2026-05-11: PM can now enter EITHER a % increase
        # (`increase_pct`) OR a flat $ amount (`increase_dollar`). Either-or:
        # setting one clears the other at save time. NULL = unset; 0 is a
        # legitimate "no change" entry (treated as unset for proposed math).
        increase_dollar = db.Column(db.Float, nullable=True)
        notes = db.Column(db.Text, default="")
        pm_editable = db.Column(db.Boolean, default=False)

        # FA directive 2026-05-11: PM R&M review gate. Every R&M line on the
        # PM portal must have an explicit review action (typed % or $ value,
        # OR a "No change" click) before the PM can submit back to the FA.
        # pm_review_state acts as the single source of truth for "reviewed"
        # without overloading increase_pct semantics (which defaults to 0
        # and is used by every downstream math path).
        #
        #   NULL              = unreviewed (red row, blocks submit)
        #   "typed_pct"       = PM entered a % value
        #   "typed_dollar"    = PM entered a $ amount
        #   "no_change"       = PM clicked the row's "No change" button
        #   "bulk_no_change"  = PM used the section-level bulk action
        #
        # Only enforced for sheet_name = "Repairs & Supplies". Other sections
        # stay edit-optional in the PM portal (PMs can set values, but it's
        # not required and pm_review_state stays NULL on those lines).
        pm_review_state = db.Column(db.String(20), nullable=True)
        pm_reviewed_at = db.Column(db.DateTime, nullable=True)
        pm_reviewed_by = db.Column(db.String(50), nullable=True)

        # Reclassification (PM can propose moving expenses to different GL)
        reclass_to_gl = db.Column(db.String(50), nullable=True)
        reclass_amount = db.Column(db.Float, default=0.0)
        reclass_notes = db.Column(db.Text, default="")

        # FA override fields (when FA manually overrides a formula cell)
        estimate_override = db.Column(db.Float, nullable=True)
        forecast_override = db.Column(db.Float, nullable=True)
        # FA dir 2026-05-17: persist typed formula strings alongside the
        # overrides (parallels proposed_formula). On re-click, the formula bar
        # repopulates with these so the FA can edit "300*12*4" → "300*12*3"
        # without retyping. NULL = no formula stored.
        estimate_formula = db.Column(db.Text, nullable=True)
        forecast_formula = db.Column(db.Text, nullable=True)

        # Ancillary backup worksheet (JSON list of line items for 4130/4135/4250-series income GLs)
        # Shape: [{"label": str, "qty": num, "rate": num, "period": "mo"|"yr", "monthsActive": num, "occupancy": num}, ...]
        backup_json = db.Column(db.Text, nullable=True)

        # Proposed budget (computed or manually entered)
        proposed_budget = db.Column(db.Float, default=0.0)
        proposed_formula = db.Column(db.Text, nullable=True)  # e.g. "=3462.12*1.04*12"

        # FA review of PM proposals
        fa_proposed_status = db.Column(db.String(20), nullable=True)  # null=pending, accepted, rejected, commented
        fa_proposed_note = db.Column(db.Text, default="")
        fa_override_value = db.Column(db.Float, nullable=True)  # FA's override when rejecting

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
                "increase_dollar": (float(self.increase_dollar) if self.increase_dollar is not None else None),
                "notes": self.notes,
                "pm_editable": self.pm_editable,
                "reclass_to_gl": self.reclass_to_gl,
                "reclass_amount": float(self.reclass_amount or 0),
                "reclass_notes": self.reclass_notes or "",
                # FA dir 2026-05-18: preserve null so PM portal can show empty
                # cells for un-entered lines (vs explicit zero). Legacy callers
                # using `l.proposed_budget || 0` still work since null is falsy.
                "proposed_budget": (float(self.proposed_budget) if self.proposed_budget is not None else None),
                "proposed_formula": self.proposed_formula or "",
                "estimate_override": self.estimate_override,
                "forecast_override": self.forecast_override,
                # FA dir 2026-05-17: formula strings parallel to overrides
                "estimate_formula": self.estimate_formula or "",
                "forecast_formula": self.forecast_formula or "",
                "fa_proposed_status": self.fa_proposed_status,
                "fa_proposed_note": self.fa_proposed_note or "",
                "fa_override_value": self.fa_override_value,
                "backup_json": _parse_backup_json(self.backup_json),
                # FA directive 2026-05-11: PM R&M review gate fields.
                "pm_review_state": self.pm_review_state,
                "pm_reviewed_at": self.pm_reviewed_at.isoformat() if self.pm_reviewed_at else None,
                "pm_reviewed_by": self.pm_reviewed_by,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None
            }

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

    class AuditSyncRun(db.Model):
        """One row per (file, run) processed by the audit-sync admin job.

        Logs every PDF the sync touched in the 2025 audit master folder and the
        action taken: copied to the entity folder, skipped (already present and
        identical), replaced (source newer than destination), unmatched (filename
        could not be parsed to an active entity), or errored.
        """
        __tablename__ = "audit_sync_runs"

        id = db.Column(db.Integer, primary_key=True)
        run_id = db.Column(db.String(36), nullable=False, index=True)  # uuid4
        entity_code = db.Column(db.String(20), nullable=True, index=True)  # null for unmatched
        source_filename = db.Column(db.Text, nullable=False)
        source_size = db.Column(db.BigInteger, nullable=True)
        source_sha256 = db.Column(db.String(64), nullable=True)
        source_mtime = db.Column(db.DateTime, nullable=True)
        dest_path = db.Column(db.Text, nullable=True)
        dest_url = db.Column(db.Text, nullable=True)
        action = db.Column(db.String(20), nullable=False)  # copied/skipped/replaced/unmatched/error
        error_text = db.Column(db.Text, nullable=True)
        created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

        def to_dict(self):
            return {
                "id": self.id, "run_id": self.run_id,
                "entity_code": self.entity_code,
                "source_filename": self.source_filename,
                "source_size": self.source_size,
                "source_sha256": self.source_sha256,
                "source_mtime": self.source_mtime.isoformat() if self.source_mtime else None,
                "dest_path": self.dest_path, "dest_url": self.dest_url,
                "action": self.action, "error_text": self.error_text,
                "created_at": self.created_at.isoformat() if self.created_at else None,
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

    class BuildingVisit(db.Model):
        """Per-FA visit tracking for the diff-strip feature.

        FA directive 2026-05-10: when an FA reopens a building they last
        visited >24h ago, surface what's changed since (orphan/duplicate
        deltas, audit status flips, edits by other FAs). Each row is one
        (user, building) visit with a compact JSON snapshot.

        Snapshot shape (v=1):
          {
            "v": 1,
            "orphan_count":           int,
            "duplicate_groups":       int,
            "audit_status":           str,        # uploaded/extracted/mapped/confirmed/null
            "audit_id":               int | null,
            "last_revision_id":       int,        # max(BudgetRevision.id) at visit
            "last_audit_confirmed_at": str | null # ISO timestamp
          }
        """
        __tablename__ = "building_visits"

        id = db.Column(db.Integer, primary_key=True)
        user_id = db.Column(db.Integer, nullable=False, index=True)
        entity_code = db.Column(db.String(50), nullable=False, index=True)
        visited_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
        snapshot_json = db.Column(db.Text, nullable=False)
        diff_dismissed_at = db.Column(db.DateTime, nullable=True)

        def to_dict(self):
            return {
                "id": self.id, "user_id": self.user_id,
                "entity_code": self.entity_code,
                "visited_at": self.visited_at.isoformat() if self.visited_at else None,
                "snapshot_json": self.snapshot_json,
                "diff_dismissed_at": self.diff_dismissed_at.isoformat() if self.diff_dismissed_at else None,
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

    class PayrollPosition(db.Model):
        """Employee positions for the payroll wage calculation engine."""
        __tablename__ = "payroll_positions"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False)
        budget_year = db.Column(db.Integer, nullable=False)
        position_name = db.Column(db.String(100), nullable=False)
        employee_count = db.Column(db.Integer, default=0)
        hourly_rate = db.Column(db.Float, default=0.0)
        bonus_per_employee = db.Column(db.Float, default=0.0)
        effective_week_override = db.Column(db.Float, nullable=True)
        # Per-position wage-increase override. Both null => inherit global from PayrollAssumption.
        # wage_increase_mode: 'pct' | 'dollar' | NULL
        # wage_increase_value: if mode='pct', decimal (0.03 = 3%); if mode='dollar', $/hr
        wage_increase_mode = db.Column(db.String(10), nullable=True)
        wage_increase_value = db.Column(db.Float, nullable=True)
        # Optional extra bonus lines stacked on top of bonus_per_employee.
        # JSON text: [{"label":"Perf","amount":0.02,"basis":"pct_wages"}, ...]
        # basis one of: 'per_emp' | 'lump' | 'pct_wages'
        extra_bonuses_json = db.Column(db.Text, nullable=True)
        # FA directive 2026-05-05: per-position benefit adjustments. Lets the FA
        # express "N of M employees in this position have an extra rate × periods
        # block on welfare/pension/supp_retirement/legal/training/profit_sharing".
        # Math is additive to the building default. NULL = no adjustment.
        # Shape:
        #   {
        #     "adjusted_count": 1,                  // 1 ≤ count ≤ employee_count
        #     "label": "Tenure exception",          // optional
        #     "benefits": {
        #       "welfare":         {"rate": 100.0,  "periods": 6,  "label": "..."} | null,
        #       "pension":         {"rate": 82.50, "periods": 30, "label": "Tenure"} | null,
        #       "supp_retirement": {...} | null,
        #       "legal":           {...} | null,
        #       "training":        {...} | null,
        #       "profit_sharing":  {...} | null
        #     }
        #   }
        benefit_adjustments_json = db.Column(db.Text, nullable=True)
        sort_order = db.Column(db.Integer, default=0)
        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        def to_dict(self):
            try:
                extras = json.loads(self.extra_bonuses_json) if self.extra_bonuses_json else []
                if not isinstance(extras, list):
                    extras = []
            except Exception:
                extras = []
            try:
                adj = json.loads(self.benefit_adjustments_json) if self.benefit_adjustments_json else None
                if adj is not None and not isinstance(adj, dict):
                    adj = None
            except Exception:
                adj = None
            return {
                "id": self.id, "entity_code": self.entity_code,
                "budget_year": self.budget_year,
                "position_name": self.position_name,
                "employee_count": self.employee_count,
                "hourly_rate": float(self.hourly_rate or 0),
                "bonus_per_employee": float(self.bonus_per_employee or 0),
                "effective_week_override": float(self.effective_week_override) if self.effective_week_override is not None else None,
                "wage_increase_mode": self.wage_increase_mode,
                "wage_increase_value": float(self.wage_increase_value) if self.wage_increase_value is not None else None,
                "extra_bonuses": extras,
                "benefit_adjustments": adj,
                "sort_order": self.sort_order
            }

    class PayrollAssumption(db.Model):
        """Payroll-tab-specific assumption overrides (seeded from main assumptions)."""
        __tablename__ = "payroll_assumptions"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False)
        budget_year = db.Column(db.Integer, nullable=False)
        assumptions_json = db.Column(db.Text, default="{}")
        # FA directive 2026-05-17: per-cell overrides for the green formula totals
        # on the Payroll tab (FICA, Welfare, Pension, etc.). Shape:
        #   {"welfare": 80500.00, "fica": null, ...}
        # NULL or missing key = use computed value. Non-NULL = take this value
        # as the FA-set authoritative number; recalcPayroll applies after base math.
        # Single JSON column instead of one DB column per cell because the set of
        # editable cells will grow (and shrink) and we want flex without migrations.
        overrides_json = db.Column(db.Text, default="{}")
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        def to_dict(self):
            import json as _json
            return {
                "id": self.id, "entity_code": self.entity_code,
                "budget_year": self.budget_year,
                "assumptions": _json.loads(self.assumptions_json or "{}"),
                "overrides": _json.loads(self.overrides_json or "{}"),
                "updated_at": self.updated_at.isoformat() if self.updated_at else None
            }

    class CommercialTenant(db.Model):
        """One commercial tenant in a building.

        Identity: tenant_name + unit_label.
        Lease: start/end dates + plain-text notes.
        Escalation: model + share% + base years.
        """
        __tablename__ = "commercial_tenants"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False, index=True)
        budget_year = db.Column(db.Integer, nullable=False, default=BUDGET_YEAR)

        # Identity
        tenant_name = db.Column(db.String(200), nullable=False)
        unit_label = db.Column(db.String(100), nullable=True)

        # Lease
        lease_start = db.Column(db.Date, nullable=True)
        lease_end = db.Column(db.Date, nullable=True)
        lease_notes = db.Column(db.Text, nullable=True)

        # Escalation config — model: 're_tax' | 'utility_billback' | 'opex' | 'none'
        escalation_model = db.Column(db.String(30), nullable=False, default="none")
        # Tenant share is stored as decimal: 0.0104 = 1.04%
        tenant_share_pct = db.Column(db.Float, nullable=True)
        # Base years used by escalation math:
        #   - re_tax: total RE tax in the base year (frozen in lease)
        #   - opex: total operating expenses in the base year
        #   - utility_billback: per-category amounts in CommercialTenantBillback
        base_year_re_tax = db.Column(db.Float, nullable=True)
        base_year_opex = db.Column(db.Float, nullable=True)

        sort_order = db.Column(db.Integer, default=0)
        imported_from_excel = db.Column(db.Boolean, default=False)
        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        def to_dict(self, include_periods=True, include_billbacks=True):
            d = {
                "id": self.id,
                "entity_code": self.entity_code,
                "budget_year": self.budget_year,
                "tenant_name": self.tenant_name,
                "unit_label": self.unit_label,
                "lease_start": self.lease_start.isoformat() if self.lease_start else None,
                "lease_end": self.lease_end.isoformat() if self.lease_end else None,
                "lease_notes": self.lease_notes,
                "escalation_model": self.escalation_model,
                "tenant_share_pct": self.tenant_share_pct,
                "base_year_re_tax": self.base_year_re_tax,
                "base_year_opex": self.base_year_opex,
                "sort_order": self.sort_order,
                "imported_from_excel": bool(self.imported_from_excel),
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            }
            if include_periods:
                d["rent_periods"] = [p.to_dict() for p in
                    CommercialRentPeriod.query.filter_by(tenant_id=self.id)
                    .order_by(CommercialRentPeriod.year, CommercialRentPeriod.sort_order).all()]
            if include_billbacks:
                d["billbacks"] = [b.to_dict() for b in
                    CommercialTenantBillback.query.filter_by(tenant_id=self.id)
                    .order_by(CommercialTenantBillback.sort_order).all()]
            return d

    class CommercialRentPeriod(db.Model):
        """One period of rent for a tenant.

        Mirrors the Excel pattern of multi-row rent schedules per year
        (e.g., Mack Dermatology Jan-Feb at $7,408/mo + Mar-Dec at $7,593/mo).
        Annual rent for the tenant in a year = sum(monthly_rent * months_count)
        across all periods for that year.
        """
        __tablename__ = "commercial_rent_periods"

        id = db.Column(db.Integer, primary_key=True)
        tenant_id = db.Column(db.Integer,
            db.ForeignKey("commercial_tenants.id", ondelete="CASCADE"),
            nullable=False, index=True)
        year = db.Column(db.Integer, nullable=False)
        period_label = db.Column(db.String(50), nullable=False)
        monthly_rent = db.Column(db.Float, nullable=False, default=0)
        months_count = db.Column(db.Integer, nullable=False, default=12)
        sort_order = db.Column(db.Integer, default=0)
        created_at = db.Column(db.DateTime, default=datetime.utcnow)

        def annualized(self):
            return (self.monthly_rent or 0) * (self.months_count or 0)

        def to_dict(self):
            return {
                "id": self.id,
                "tenant_id": self.tenant_id,
                "year": self.year,
                "period_label": self.period_label,
                "monthly_rent": self.monthly_rent,
                "months_count": self.months_count,
                "annualized": self.annualized(),
                "sort_order": self.sort_order,
            }

    class CommercialTenantBillback(db.Model):
        """Per-tenant utility/insurance billback base year amounts.
        Used when tenant.escalation_model = 'utility_billback'.

        E.g., Building 212 City Parking:
          'Gas & Electric' base=$61,800
          'Steam'          base=$172,291
          'Insurance'      base=$56,217
        Tenant's annual billback per category = (current_year_amount - base) * tenant_share_pct
        """
        __tablename__ = "commercial_tenant_billbacks"

        id = db.Column(db.Integer, primary_key=True)
        tenant_id = db.Column(db.Integer,
            db.ForeignKey("commercial_tenants.id", ondelete="CASCADE"),
            nullable=False, index=True)
        category = db.Column(db.String(50), nullable=False)
        base_year_amount = db.Column(db.Float, nullable=False, default=0)
        sort_order = db.Column(db.Integer, default=0)
        created_at = db.Column(db.DateTime, default=datetime.utcnow)

        def to_dict(self):
            return {
                "id": self.id,
                "tenant_id": self.tenant_id,
                "category": self.category,
                "base_year_amount": self.base_year_amount,
                "sort_order": self.sort_order,
            }

    class BudgetSummaryRow(db.Model):
        """
        Budget Summary row — one per line item per building.

        Stores the row framework from yrlycomp import plus:
          - col1_prior_actual: 2024 Actual (imported from Excel, read-only)
          - col6_approved_budget: 2026 Approved Budget (imported from Excel, read-only)
          - col7_proposed_budget: 2027 Budget (FA-editable, starts NULL)

        Columns 2-5 and 8 are computed live from budget_lines via GL prefix aggregation.
        """
        __tablename__ = "budget_summary_rows"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False, index=True)
        budget_year = db.Column(db.Integer, nullable=False, default=BUDGET_YEAR)

        # Row framework (from yrlycomp parser)
        display_order = db.Column(db.Integer, nullable=False)
        label = db.Column(db.String(255), nullable=False)
        section = db.Column(db.String(100), nullable=True)  # Income, Expenses, Non-Operating Income, etc.
        # FA dir 2026-05-20: widened from VARCHAR(20) → 255 after 834 hit
        # StringDataRightTruncation during dry-run import (its yrlycomp put
        # a long value in one of these). Matches the schema migration in
        # app.py _run_idempotent_migrations.
        row_type = db.Column(db.String(255), nullable=False)  # data, subtotal, section_header
        footnote_marker = db.Column(db.String(255), nullable=True)

        # Imported columns (from approved budget Excel)
        col1_prior_actual = db.Column(db.Float, nullable=True)      # 2024 Actual*
        col6_approved_budget = db.Column(db.Float, nullable=True)    # 2026 Budget

        # FA work product (starts NULL, FA fills in)
        col7_proposed_budget = db.Column(db.Float, nullable=True)    # 2027 Budget

        # FA directive 2026-05-05: editable green-tab overrides.
        # When set, take precedence over the GL-aggregation computed values
        # in /api/summary. NULL = use computed. Parallels BudgetLine.estimate_override.
        col3_override = db.Column(db.Float, nullable=True)           # 2026 YTD override
        col4_override = db.Column(db.Float, nullable=True)           # 2026 Estimate override
        col5_override = db.Column(db.Float, nullable=True)           # 2026 Forecast override

        # FA directive 2026-05-17: make ALL summary cells editable. col1 / col6
        # come from the approved-budget Excel import; col2 comes from the
        # confirmed audit's mapped_data. Each override field, when non-NULL,
        # takes precedence over the imported / computed source. NULL = use
        # source. Right-click on the cell reverts (clears the override).
        col1_override = db.Column(db.Float, nullable=True)           # 2024 Actual override
        col2_override = db.Column(db.Float, nullable=True)           # 2025 Actual (audit) override
        col6_override = db.Column(db.Float, nullable=True)           # 2026 Approved Budget override

        # FA directive 2026-05-17 (formula persistence): when the FA types a
        # formula like "=300*12*4" in the formula bar, we store the formula
        # string here alongside the evaluated result in col*_override. On
        # next focus, the formula bar re-shows the formula so the FA can edit
        # "4" → "3" without retyping the whole expression. Shape:
        #   {"col1": "=300*12*4", "col5": "=ytd*1.05"}
        # Empty/missing key = no formula stored; raw override value applies.
        cell_formulas_json = db.Column(db.Text, nullable=True)

        # Metadata for the summary engine
        source_tab = db.Column(db.String(50), nullable=True)        # Income, Payroll, Energy, etc.
        gl_prefixes_json = db.Column(db.Text, nullable=True)        # JSON array of GL prefixes
        source_file = db.Column(db.String(255), nullable=True)

        # Timestamps
        imported_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        __table_args__ = (
            db.UniqueConstraint("entity_code", "budget_year", "display_order",
                                name="uq_summary_entity_year_order"),
        )

        def to_dict(self):
            return {
                "id": self.id,
                "entity_code": self.entity_code,
                "budget_year": self.budget_year,
                "display_order": self.display_order,
                "label": self.label,
                "section": self.section,
                "row_type": self.row_type,
                "footnote_marker": self.footnote_marker,
                "col1_prior_actual": self.col1_prior_actual,
                "col6_approved_budget": self.col6_approved_budget,
                "col7_proposed_budget": self.col7_proposed_budget,
                "source_tab": self.source_tab,
                "gl_prefixes_json": self.gl_prefixes_json,
                "source_file": self.source_file,
                "imported_at": self.imported_at.isoformat() if self.imported_at else None,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            }

    class BuildingInfo(db.Model):
        """
        Building Info — reference/illustrative data per entity.

        Purely a home for manually-maintained reference data that doesn't
        tie into budget calculations: maintenance-history trend, underlying
        mortgage amortization parameters, and room for future sections
        (lease rolls, shareholder counts, etc.). All sections are optional;
        empty entities are allowed. Never affects budget math.
        """
        __tablename__ = "building_info"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False, unique=True, index=True)

        # Section blobs — JSON strings. Add new sections as new columns later.
        maintenance_history_json = db.Column(db.Text, nullable=True)
        # Condo equivalent — auto-populated from the Income tab's GL
        # 4020-0000 block (Common Charges). Same shape as maintenance_history
        # but without shares / perShare since condos use % common interest.
        common_charges_history_json = db.Column(db.Text, nullable=True)
        amort_config_json = db.Column(db.Text, nullable=True)

        # FA dir 2026-05-19: snapshot-on-save undo support. Every PUT pushes
        # the CURRENT state into this JSON array before applying the new
        # values. Last 20 snapshots kept. Each entry:
        #   {ts: ISO timestamp, by: user_id, maintenance_history, amort_config, common_charges_history}
        # Restore endpoint reads from here. Once empty, undo is unavailable
        # (oldest snapshot is the only known previous state).
        snapshots_json = db.Column(db.Text, nullable=True)

        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
        updated_by = db.Column(db.String(120), nullable=True)

        def to_dict(self):
            def _load(raw):
                if not raw:
                    return None
                try:
                    return json.loads(raw)
                except Exception:
                    return None
            return {
                "entity_code": self.entity_code,
                "maintenance_history": _load(self.maintenance_history_json),
                "common_charges_history": _load(self.common_charges_history_json),
                "amort_config": _load(self.amort_config_json),
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
                "updated_by": self.updated_by,
            }

    return {
        "User": User,
        "BuildingAssignment": BuildingAssignment,
        "Budget": Budget,
        "BudgetLine": BudgetLine,
        "DataSource": DataSource,
        "AuditSyncRun": AuditSyncRun,
        "BudgetRevision": BudgetRevision,
        "BuildingVisit": BuildingVisit,
        "PresentationSession": PresentationSession,
        "PresentationEdit": PresentationEdit,
        "ARHandoff": ARHandoff,
        "PayrollPosition": PayrollPosition,
        "PayrollAssumption": PayrollAssumption,
        "CommercialTenant": CommercialTenant,
        "CommercialRentPeriod": CommercialRentPeriod,
        "CommercialTenantBillback": CommercialTenantBillback,
        "BudgetSummaryRow": BudgetSummaryRow,
        "BuildingInfo": BuildingInfo,
    }
