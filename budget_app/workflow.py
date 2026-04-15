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

# Capital GL Prefix Map: first 4 digits → description
# 7xxx codes use entity-specific sub-accounts (e.g. 7110-1409) so we match on prefix
CAPITAL_GL_PREFIX = {
    "7018": "Net Proceeds of Sale",
    "7020": "Real Estate Tax Refund",
    "7025": "Flip Tax - Capital",
    "7030": "J51 Credit - Capital",
    "7035": "Investment Income",
    "7040": "Mitchell Lama Amortization",
    "7045": "Claims Proceeds - Insurance Repairs",
    "7095": "Other Sources of Funds",
    "7105": "Cap - Elevator",
    "7110": "Cap - Boiler/Burner",
    "7115": "Cap - HVAC",
    "7120": "Cap - Pump & Motor",
    "7125": "Cap - Appliances",
    "7130": "Cap - Roofing",
    "7135": "Cap - Chimney",
    "7140": "Cap - Facade Waterproof",
    "7145": "Cap - Plumbing",
    "7150": "Cap - Electrical",
    "7155": "Cap - General Contractor",
    "7160": "Cap - Carpentry",
    "7165": "Cap - Engineering / Architectural",
    "7170": "Cap - Sprinkler",
    "7175": "Cap - Water Meter",
    "7180": "Cap - Water Tank",
    "7185": "Cap - Windows",
    "7190": "Cap - Hallways",
    "7195": "Cap - Lobby",
    "7200": "Cap - Compactor",
    "7205": "Cap - Storage",
    "7210": "Cap - Sidewalk Bridge",
    "7215": "Cap - Paint & Plaster",
    "7220": "Cap - Asbestos Removal",
    "7225": "Cap - Sidewalk / Concrete",
    "7230": "Cap - Driveway",
    "7235": "Cap - Doors",
    "7240": "Cap - Floor / Carpet",
    "7245": "Cap - Garage",
    "7250": "Cap - Canopy / Awning",
    "7255": "Cap - TV / VCR",
    "7260": "Cap - Intercom",
    "7265": "Cap - Security System",
    "7270": "Cap - Garden & Landscape",
    "7273": "Cap - Insurance Repairs",
    "7275": "Cap - Mailbox",
    "7280": "Cap - Signage",
    "7285": "Cap - Parking",
    "7290": "Cap - Pool",
    "7295": "Cap - Pool Furniture",
    "7300": "Cap - STP",
    "7305": "Cap - Whirlpool / Steam Room",
    "7310": "Cap - Tennis Court",
    "7315": "Cap - Fitness Equipment",
    "7320": "Cap - Laundry Room",
    "7325": "Cap - Children's Play Area",
    "7330": "Cap - Gym",
    "7335": "Cap - Aerobics Floor",
    "7340": "Cap - Racquetball Court",
    "7345": "Cap - Great Room",
    "7350": "Cap - Principal Amortization",
    "7355": "Cap - Interest",
    "7360": "Cap - Professional",
    "7370": "Cap - Commissions",
    "7375": "Cap - Legal",
    "7380": "Cap - Loan Financing Fees",
    "7385": "Cap - Building Equipment",
    "7390": "Cap - Office Equipment",
    "7395": "Cap - Computer Equipment",
    "7400": "Cap - Computer Software",
    "7405": "Cap - Furniture & Fixtures",
    "7415": "Cap - Inspection Fees",
    "7490": "Cap - Other",
    "7900": "Cap - Contra",
}

# One-time annual fee GLs — billed once per year, so once YTD > 0 the Mar-Dec
# estimate must be zeroed out (otherwise the forecast gets annualized as if
# it recurred monthly). Forecast then collapses to YTD + Accrual + Unpaid.
# FA can still override via estimate_override if a weird case ever comes up.
ONE_TIME_FEE_GLS = {
    "6722-0000",  # Annual filing fee
    "6762-0000",  # Annual inspection/permit
    "6763-0000",  # Annual inspection/permit
    "6764-0000",  # Annual inspection/permit
}

# Load GL_Mapping.csv (412+ entries) for routing and naming unmapped GLs.
# Indexed by 4-digit prefix so entity-specific sub-accounts (e.g. 4010-1409) match the
# base mapping entry (e.g. 4010-0000). Returns dict: prefix -> (description, sheet_name, category).
# Only codes with an explicit routing rule are included — balance sheet codes and codes
# not present in the mapping file stay Unmapped.
def _csv_row_to_sheet(cat, sub, code):
    """Return (sheet_name, category_key) or None if not explicitly routable.

    Note on R&S sub-categories: the UI groups the Repairs & Supplies tab into
    Supplies / Repairs / Maintenance Contracts buckets by matching BudgetLine.category
    against the strings 'supplies' / 'repairs' / 'maintenance'. Historically this
    function returned a lumped 'rm' bucket which made most R&S lines invisible in
    the Supplies group. We now preserve the sub-category from the CSV.
    """
    if cat == "Income":
        return ("Income", "income")
    if cat == "Gen & Admin Expenses":
        return ("Gen & Admin", "gen_admin")
    # Operating Expenses or blank category — use Sub-Category to pick the sheet
    if sub == "Payroll Expenses":
        return ("Payroll", "payroll")
    if sub == "Utility Expenses":
        # 63xx is Water/Sewer; all other utility codes are Energy
        if code.startswith("63"):
            return ("Water & Sewer", "water_sewer")
        return ("Energy", "energy")
    if sub == "Supplies":
        return ("Repairs & Supplies", "supplies")
    if sub == "Repairs":
        return ("Repairs & Supplies", "repairs")
    if sub == "Maintenance":
        return ("Repairs & Supplies", "maintenance")
    return None

def _load_gl_mapping_csv():
    import csv as _csv
    from pathlib import Path as _Path
    mapping = {}
    candidates = [
        _Path(__file__).parent.parent / "budget_system" / "GL_Mapping.csv",
        _Path(__file__).parent / "GL_Mapping.csv",
    ]
    for p in candidates:
        if p.exists():
            try:
                with open(p, newline="", encoding="utf-8-sig") as f:
                    for row in _csv.DictReader(f):
                        code = (row.get("GL Code") or "").strip()
                        desc = (row.get("Description") or "").strip()
                        cat = (row.get("Category Tab") or "").strip()
                        sub = (row.get("Sub-Category") or "").strip()
                        if not (code and desc):
                            continue
                        routing = _csv_row_to_sheet(cat, sub, code)
                        if routing is None:
                            continue  # Skip rows we can't confidently route
                        prefix = code[:4]
                        # First explicit routing wins (CSV is ordered by category)
                        if prefix not in mapping:
                            mapping[prefix] = (desc, routing[0], routing[1])
            except Exception:
                pass
            break
    return mapping

GL_MAPPING_CSV = _load_gl_mapping_csv()


# ─── SUMMARY ROW PREFIX OVERRIDES ────────────────────────────────────────
# Budget Summary tab rows carry gl_prefixes_json used to aggregate YTD/estimate/
# forecast from budget_lines. Historical push files (generated from the legacy
# GL_TO_SUMMARY_MAP.py) contain stale chart-of-accounts prefixes that predate
# the Yardi re-numbering. These overrides are the canonical Yardi prefixes
# keyed by canonical summary row label. Applied at BOTH import time (so future
# imports auto-correct) and via startup backfill (so existing DB rows are
# fixed). Source of truth: budget_system/GL_Mapping.csv (Utility Expenses +
# Supplies sub-categories). If a label is added to SUMMARY_PREFIX_OVERRIDES
# here, no per-building redeployment is needed.
SUMMARY_PREFIX_OVERRIDES = {
    "Electric": ["5255"],
    "Gas Cooking / Heating": ["5250", "5251", "5252"],
    "Gas": ["5250", "5251", "5252"],
    "Gas - Heating": ["5250", "5251", "5252"],
    "Gas Heating": ["5250", "5251", "5252"],
    "Steam Heating": ["5265"],
    "Steam": ["5265"],
    "Fuel": ["5260"],
    "Oil / Fuel": ["5260"],
    "Fuel Oil": ["5260"],
    "Water & Sewer": ["6305"],
    "Supplies": [
        "5405", "5406", "5408", "5410", "5415", "5420", "5425", "5430",
        "5435", "5440", "5441", "5445", "5450", "5451", "5452", "5453",
        "5455", "5460", "5465", "5466", "5495",
    ],
}


def apply_summary_prefix_override(label, existing_prefixes):
    """Return corrected prefix list for a summary row label.

    Used at import time and in startup backfill. Only overrides labels
    explicitly listed in SUMMARY_PREFIX_OVERRIDES; all other rows pass
    through untouched.
    """
    if not label:
        return existing_prefixes
    override = SUMMARY_PREFIX_OVERRIDES.get(label.strip())
    if override:
        return list(override)
    return existing_prefixes


# ─── FIXED-FORECAST INCOME GLs ──────────────────────────────────────────
# Business rule (from Jacob, 2026-04-14): for Maintenance / Common Charges /
# Commercial Rent income rows on the Budget Summary tab, forecast (Col 5)
# must equal Approved Budget (Col 6) — these are predictable contractual
# amounts, not forecast-from-YTD. Col 4 (Estimate) is then set so the math
# ties out: Col 4 = Col 5 - Col 3. Matched by GL prefix (not label) since
# labels vary across buildings (co-op vs condo).
#
# Full GLs provided: 4010-0000, 4020-0000, 4020-0005, 4030-0000,
#                    4040-0000, 4040-0010
# Stored as 4-digit bases to match _gl_matches_prefixes behavior.
FIXED_FORECAST_GL_BASES = {"4010", "4020", "4030", "4040"}
FIXED_FORECAST_GL_FULL = [
    "4010-0000", "4020-0000", "4020-0005",
    "4030-0000", "4040-0000", "4040-0010",
]


def _row_has_fixed_forecast_gl(gl_prefixes_json):
    """Check if a summary row's stored prefixes intersect the fixed-forecast set."""
    if not gl_prefixes_json:
        return False
    try:
        import json as _j
        prefixes = _j.loads(gl_prefixes_json)
    except Exception:
        return False
    if not isinstance(prefixes, list):
        return False
    for p in prefixes:
        if not p:
            continue
        base = str(p).split("-")[0].strip()
        if base in FIXED_FORECAST_GL_BASES:
            return True
    return False


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

# ─── Budget Year Config ─────────────────────────────────────────────────────
# Change this ONE value each cycle. All routes, queries, and column headers
# derive their years from this.  BY=2027 means:
#   Col 1 = 2024 Actual   (BY-3)
#   Col 2 = 2025 Actual   (BY-2)
#   Col 3 = 2026 YTD      (BY-1)
#   Col 4 = 2026 Est.     (BY-1)
#   Col 5 = 2026 Forecast (BY-1)
#   Col 6 = 2026 Budget   (BY-1)
#   Col 7 = 2027 Budget   (BY)
import os
BUDGET_YEAR = int(os.environ.get("BUDGET_YEAR", 2027))


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
                "notes": self.notes,
                "pm_editable": self.pm_editable,
                "reclass_to_gl": self.reclass_to_gl,
                "reclass_amount": float(self.reclass_amount or 0),
                "reclass_notes": self.reclass_notes or "",
                "proposed_budget": float(self.proposed_budget or 0),
                "proposed_formula": self.proposed_formula or "",
                "estimate_override": self.estimate_override,
                "forecast_override": self.forecast_override,
                "fa_proposed_status": self.fa_proposed_status,
                "fa_proposed_note": self.fa_proposed_note or "",
                "fa_override_value": self.fa_override_value,
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


    # ─── Payroll Models ───────────────────────────────────────────────────

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
        sort_order = db.Column(db.Integer, default=0)
        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        def to_dict(self):
            return {
                "id": self.id, "entity_code": self.entity_code,
                "budget_year": self.budget_year,
                "position_name": self.position_name,
                "employee_count": self.employee_count,
                "hourly_rate": float(self.hourly_rate or 0),
                "bonus_per_employee": float(self.bonus_per_employee or 0),
                "effective_week_override": float(self.effective_week_override) if self.effective_week_override is not None else None,
                "sort_order": self.sort_order
            }


    class PayrollAssumption(db.Model):
        """Payroll-tab-specific assumption overrides (seeded from main assumptions)."""
        __tablename__ = "payroll_assumptions"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False)
        budget_year = db.Column(db.Integer, nullable=False)
        assumptions_json = db.Column(db.Text, default="{}")
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        def to_dict(self):
            import json as _json
            return {
                "id": self.id, "entity_code": self.entity_code,
                "budget_year": self.budget_year,
                "assumptions": _json.loads(self.assumptions_json or "{}"),
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
            budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
            if not budget:
                budget = Budget(
                    entity_code=entity_code,
                    building_name=building_name,
                    year=BUDGET_YEAR,
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
        "Capital": "capital",
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
            budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
            if not budget:
                budget = Budget(
                    entity_code=entity_code,
                    building_name=building_name,
                    year=BUDGET_YEAR,
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
                    # Repairs & Supplies needs sub-category split (supplies/repairs/maintenance)
                    # for the UI grouping. SHEET_TO_CATEGORY returns the lumped "rm" bucket,
                    # so look up the actual sub-category in GL_Mapping.csv by 4-digit prefix.
                    if category == "rm":
                        _csv_hit = GL_MAPPING_CSV.get(gl_code[:4])
                        category = _csv_hit[2] if _csv_hit else "repairs"
                    pm_editable = False
                elif gl_code.startswith("7"):
                    prefix = gl_code[:4]
                    desc = CAPITAL_GL_PREFIX.get(prefix, f"Cap - {prefix}")
                    sheet_name = "Capital"
                    row_num = 0
                    category = "capital"
                    pm_editable = True
                else:
                    # Try GL_Mapping.csv for explicit routing to a real tab.
                    # Only codes present in the mapping file get routed; everything else
                    # (balance sheet codes, codes missing from mapping) stays Unmapped.
                    _csv_hit = GL_MAPPING_CSV.get(gl_code[:4])
                    if _csv_hit:
                        desc, sheet_name, category = _csv_hit
                        row_num = 0
                        pm_editable = True
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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
        where estimate = (ytd_total / ytd_months) * remaining_months
        Note: prior_year arg retained for signature compatibility but no longer used.
        """
        ytd_total = ytd_actual + accrual_adj + unpaid_bills
        remaining = 12 - ytd_months

        if ytd_months > 0:
            estimate = (ytd_total / ytd_months) * remaining
        else:
            estimate = 0

        return ytd_total + estimate


    def forecast_method(ytd_actual, accrual_adj, unpaid_bills, prior_year):
        """Return the forecast method label for display purposes."""
        return 'Annualized'


    def compute_proposed_budget(forecast, increase_pct):
        """Compute proposed budget = forecast * (1 + increase_pct)"""
        return forecast * (1 + increase_pct)


    # ─── Budget Summary Table ───────────────────────────────────────────────

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
        row_type = db.Column(db.String(20), nullable=False)  # data, subtotal, section_header
        footnote_marker = db.Column(db.String(20), nullable=True)

        # Imported columns (from approved budget Excel)
        col1_prior_actual = db.Column(db.Float, nullable=True)      # 2024 Actual*
        col6_approved_budget = db.Column(db.Float, nullable=True)    # 2026 Budget

        # FA work product (starts NULL, FA fills in)
        col7_proposed_budget = db.Column(db.Float, nullable=True)    # 2027 Budget

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
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
        if not budget:
            return "No budget found for this building", 404
        return render_template_string(BUILDING_DETAIL_TEMPLATE, entity_code=entity_code, budget_year=BUDGET_YEAR)


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
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()

        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        # Check if PM can edit this budget
        # fa_review is included so the PM can re-enter and tweak a building
        # after it's been submitted for FA review.
        can_edit = budget.status in ["pm_pending", "pm_in_progress", "returned", "fa_review"]

        # PM sees Repairs & Supplies + Gen & Admin lines
        lines = BudgetLine.query.filter(
            BudgetLine.budget_id == budget.id,
            BudgetLine.sheet_name.in_(["Repairs & Supplies", "Gen & Admin"])
        ).order_by(BudgetLine.row_num).all()
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
            budget_status=budget.status,
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
        # Batch-fetch entities with expenses / audits in one query each (avoids session poisoning from per-row errors)
        try:
            expense_entities = {r[0] for r in db.session.execute(
                db.text("SELECT DISTINCT entity_code FROM expense_reports")
            ).fetchall()}
        except Exception:
            db.session.rollback()
            expense_entities = set()
        try:
            audit_entities = {r[0] for r in db.session.execute(
                db.text("SELECT DISTINCT entity_code FROM audit_uploads WHERE status = 'confirmed'")
            ).fetchall()}
        except Exception:
            db.session.rollback()
            audit_entities = set()

        # Batch-fetch data-loaded timestamps per entity
        # 1) Budget summary import timestamps (earliest imported_at per entity)
        summary_ts = {}
        try:
            rows = db.session.execute(
                db.text("SELECT entity_code, MIN(imported_at) FROM budget_summary_rows GROUP BY entity_code")
            ).fetchall()
            for r in rows:
                summary_ts[r[0]] = r[1].isoformat() if r[1] else None
        except Exception:
            db.session.rollback()

        # 2) YSL data timestamps — use earliest BudgetLine updated_at per entity as proxy
        #    (YSL import creates/updates budget_lines via store_all_lines)
        ysl_ts = {}
        try:
            rows = db.session.execute(
                db.text("""
                    SELECT b.entity_code, MIN(bl.updated_at)
                    FROM budget_lines bl
                    JOIN budgets b ON b.id = bl.budget_id
                    GROUP BY b.entity_code
                """)
            ).fetchall()
            for r in rows:
                ysl_ts[r[0]] = r[1].isoformat() if r[1] else None
        except Exception:
            db.session.rollback()

        # 3) Expense distribution upload timestamps
        expense_ts = {}
        try:
            rows = db.session.execute(
                db.text("SELECT entity_code, MAX(uploaded_at) FROM expense_reports GROUP BY entity_code")
            ).fetchall()
            for r in rows:
                expense_ts[r[0]] = r[1].isoformat() if r[1] else None
        except Exception:
            db.session.rollback()

        # 4) Open AP import timestamps
        open_ap_ts = {}
        try:
            rows = db.session.execute(
                db.text("SELECT entity_code, MAX(uploaded_at) FROM open_ap_reports GROUP BY entity_code")
            ).fetchall()
            for r in rows:
                open_ap_ts[r[0]] = r[1].isoformat() if r[1] else None
        except Exception:
            db.session.rollback()

        result = []
        for b in budgets:
            d = b.to_dict()
            d["has_expenses"] = b.entity_code in expense_entities
            d["has_audit"] = b.entity_code in audit_entities
            ec = b.entity_code
            d["timestamps"] = {
                "budget_summary": summary_ts.get(ec),
                "ysl": ysl_ts.get(ec),
                "expense_dist": expense_ts.get(ec),
                "open_ap": open_ap_ts.get(ec),
            }
            result.append(d)
        return jsonify(result)


    @bp.route("/api/budgets/<entity_code>/status", methods=["POST"])
    def change_budget_status(entity_code):
        """Change budget status with validation using VALID_TRANSITIONS."""
        data = request.get_json()
        new_status = data.get("status")

        if new_status not in BUDGET_STATUSES:
            return jsonify({"error": f"Invalid status. Must be one of {BUDGET_STATUSES}"}), 400

        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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
                db.text("SELECT mapped_data, fiscal_year_end FROM audit_uploads WHERE entity_code = :ec AND status = 'confirmed' ORDER BY fiscal_year_end DESC"),
                {"ec": entity_code}
            ).fetchall()
            if audit_rows:
                years_data = {}
                summary_years_data = {}
                for row in audit_rows:
                    if not row[0]:
                        continue
                    # JSONB columns come back as dict already; plain JSON/TEXT come back as str
                    mapped = row[0] if isinstance(row[0], dict) else _json.loads(row[0])
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
        sheet_order = ["Income", "Payroll", "Energy", "Water & Sewer", "Repairs & Supplies", "Gen & Admin", "RE Taxes", "Capital", "Unmapped"]

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
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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
                base = ytd + accrual + unpaid
                # One-time fee rule: once YTD posted, no projection. Forecast = billed amount.
                if (line.gl_code or "") in ONE_TIME_FEE_GLS and abs(base) > 0.01:
                    estimate = 0
                else:
                    estimate = (base / _ytd_months) * _remaining if _ytd_months > 0 else 0
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
            budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        lines = BudgetLine.query.filter_by(budget_id=budget.id).order_by(BudgetLine.row_num).all()
        return jsonify([l.to_dict() for l in lines])


    @bp.route("/api/lines/<entity_code>", methods=["PUT"])
    def update_lines(entity_code):
        """Update R&M lines for a building (PM data entry)."""
        data = request.get_json()

        # DIAG: log incoming notes
        try:
            _incoming_lines = (data or {}).get("lines", []) or []
            _notes_in = [(l.get("gl_code"), l.get("notes")) for l in _incoming_lines if (l.get("notes") or "").strip()]
            print(f"[update_lines] entity={entity_code} total_lines={len(_incoming_lines)} with_notes={len(_notes_in)} sample={_notes_in[:5]}", flush=True)
        except Exception as _diag_err:
            print(f"[update_lines] diag err: {_diag_err}", flush=True)

        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
        if not budget:
            print(f"[update_lines] entity={entity_code} NOT FOUND for year {BUDGET_YEAR}", flush=True)
            return jsonify({"error": "Budget not found"}), 404

        print(f"[update_lines] entity={entity_code} budget.id={budget.id} status={budget.status}", flush=True)

        # Check if PM can edit
        # fa_review is allowed so the PM can re-enter and save edits after submit.
        if budget.status not in ["pm_pending", "pm_in_progress", "returned", "fa_review"]:
            print(f"[update_lines] REJECTED — status {budget.status} not editable", flush=True)
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

            # Track changes for PM audit trail
            changes = []

            # Float fields that are always present in PM payload
            for fname in ("accrual_adj", "unpaid_bills", "increase_pct"):
                if fname in line_data:
                    new_val = float(line_data.get(fname, 0) or 0)
                    old_val = getattr(line, fname, None) or 0
                    if old_val != new_val:
                        changes.append((fname, str(old_val), str(new_val)))
                    setattr(line, fname, new_val)

            # Notes
            if "notes" in line_data:
                new_val = line_data.get("notes", "")
                if (line.notes or "") != new_val:
                    changes.append(("notes", line.notes or "", new_val))
                    print(f"[update_lines] notes change gl={line.gl_code} '{line.notes or ''}' -> '{new_val}'", flush=True)
                line.notes = new_val

            # Category
            if "category" in line_data and line_data["category"]:
                old_val = line.category or ""
                new_val = line_data["category"]
                if old_val != new_val:
                    changes.append(("category", old_val, new_val))
                line.category = new_val

            # Nullable override fields
            for ofield in ("estimate_override", "forecast_override"):
                if ofield in line_data:
                    raw = line_data[ofield]
                    new_val = float(raw) if raw is not None else None
                    old_val = getattr(line, ofield, None)
                    if old_val != new_val:
                        changes.append((ofield, str(old_val), str(new_val)))
                    setattr(line, ofield, new_val)

            # Proposed budget and formula
            if "proposed_budget" in line_data:
                new_val = float(line_data["proposed_budget"] or 0)
                old_val = line.proposed_budget or 0
                if old_val != new_val:
                    changes.append(("proposed_budget", str(old_val), str(new_val)))
                line.proposed_budget = new_val
            if "proposed_formula" in line_data:
                new_val = line_data["proposed_formula"] or None
                old_val = line.proposed_formula or ""
                if old_val != (new_val or ""):
                    changes.append(("proposed_formula", old_val, new_val or ""))
                line.proposed_formula = new_val

            # Other numeric fields
            for fname in ("prior_year", "ytd_actual", "ytd_budget", "current_budget"):
                if fname in line_data:
                    new_val = float(line_data[fname] or 0)
                    old_val = getattr(line, fname, None) or 0
                    if old_val != new_val:
                        changes.append((fname, str(old_val), str(new_val)))
                    setattr(line, fname, new_val)

            # Write audit trail entries
            for field, old_v, new_v in changes:
                db.session.add(BudgetRevision(
                    budget_id=budget.id, budget_line_id=line.id,
                    action="update", field_name=field,
                    old_value=old_v, new_value=new_v, source="pm"
                ))

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            import logging
            logging.getLogger(__name__).error(f'PM lines save failed: {e}')
            return jsonify({"error": "Failed to save changes"}), 500

        return jsonify(budget.to_dict())


    # ─── FA Line Edit & Reclass Endpoints ────────────────────────────────────

    @bp.route("/api/fa-lines/<entity_code>", methods=["PUT"])
    def update_fa_lines(entity_code):
        """FA edits to any budget line (all sheets, not just R&M)."""
        data = request.get_json()
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            import logging
            logging.getLogger(__name__).error(f'FA lines save failed: {e}')
            return jsonify({"error": "Failed to save changes"}), 500
        return jsonify({"status": "ok"})


    @bp.route("/api/lines/<entity_code>/reclass", methods=["PUT"])
    def update_reclass(entity_code):
        """PM suggests reclassifying a GL line (FA acts on it)."""
        data = request.get_json()
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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


    @bp.route("/api/reclass/accept", methods=["POST"])
    def accept_pm_reclass():
        """FA accepts PM's invoice reclass — moves ytd_actual between GL lines."""
        data = request.get_json()
        entity_code = data.get("entity_code")
        from_gl = data.get("from_gl")
        to_gl = data.get("to_gl")
        amount = float(data.get("amount", 0))

        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        from_line = BudgetLine.query.filter_by(budget_id=budget.id, gl_code=from_gl).first()
        to_line = BudgetLine.query.filter_by(budget_id=budget.id, gl_code=to_gl).first()
        if not from_line or not to_line:
            return jsonify({"error": "GL line not found"}), 404

        old_from_ytd = float(from_line.ytd_actual or 0)
        old_to_ytd = float(to_line.ytd_actual or 0)

        from_line.ytd_actual = old_from_ytd - amount
        to_line.ytd_actual = old_to_ytd + amount

        # Audit trail
        db.session.add(BudgetRevision(
            budget_id=budget.id, budget_line_id=from_line.id,
            action="reclass_accept", field_name="ytd_actual",
            old_value=str(old_from_ytd), new_value=str(from_line.ytd_actual),
            notes=f"FA accepted reclass of ${amount:,.0f} to {to_gl}", source="web"
        ))
        db.session.add(BudgetRevision(
            budget_id=budget.id, budget_line_id=to_line.id,
            action="reclass_accept", field_name="ytd_actual",
            old_value=str(old_to_ytd), new_value=str(to_line.ytd_actual),
            notes=f"FA accepted reclass of ${amount:,.0f} from {from_gl}", source="web"
        ))

        db.session.commit()

        return jsonify({
            "status": "ok",
            "from_line": from_line.to_dict(),
            "to_line": to_line.to_dict()
        })


    @bp.route("/api/budget-proposal/review", methods=["POST"])
    def review_budget_proposal():
        """FA accepts, rejects, or comments on a PM budget proposal for a GL line."""
        data = request.get_json()
        entity_code = data.get("entity_code")
        gl_code = data.get("gl_code")
        action = data.get("action")          # "accepted", "rejected", "commented"
        note = data.get("note", "")
        override_value = data.get("override_value")  # only for reject

        if action not in ("accepted", "rejected", "commented"):
            return jsonify({"error": "Invalid action"}), 400

        budget = Budget.query.filter_by(entity_code=entity_code).order_by(Budget.id.desc()).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        line = BudgetLine.query.filter_by(budget_id=budget.id, gl_code=gl_code).first()
        if not line:
            return jsonify({"error": "GL line not found"}), 404

        old_status = line.fa_proposed_status or "pending"
        line.fa_proposed_status = action
        line.fa_proposed_note = note

        if action == "rejected" and override_value is not None:
            try:
                override_value = float(override_value)
            except (ValueError, TypeError):
                return jsonify({"error": "Invalid override value"}), 400
            line.fa_override_value = override_value
            # When FA rejects with a custom value, write it as proposed_budget
            line.proposed_budget = override_value
        elif action == "accepted":
            line.fa_override_value = None  # clear any prior override

        # Append to notes for visibility in both dashboards
        timestamp = datetime.utcnow().strftime("%m/%d %H:%M")
        if action == "rejected":
            ov_str = f" | FA override: ${override_value:,.0f}" if override_value is not None else ""
            note_entry = f"[FA REJECTED {timestamp}] {note}{ov_str}"
        elif action == "commented":
            note_entry = f"[FA COMMENT {timestamp}] {note}"
        else:
            note_entry = f"[FA ACCEPTED {timestamp}]"

        existing_notes = line.notes or ""
        line.notes = f"{existing_notes}\n{note_entry}".strip() if existing_notes else note_entry

        # Audit trail
        rev = BudgetRevision(
            budget_id=budget.id,
            budget_line_id=line.id,
            action="fa_proposal_review",
            field_name="fa_proposed_status",
            old_value=old_status,
            new_value=action,
            notes=note or "",
            source="web"
        )
        db.session.add(rev)
        db.session.commit()

        return jsonify({"status": "ok", "line": line.to_dict()})


    # ─── Payroll Roster & Assumptions API ────────────────────────────────────

    @bp.route("/api/payroll/positions/<entity_code>", methods=["GET"])
    def get_payroll_positions(entity_code):
        """Get all payroll positions for an entity."""
        positions = PayrollPosition.query.filter_by(
            entity_code=entity_code, budget_year=BUDGET_YEAR
        ).order_by(PayrollPosition.sort_order).all()
        return jsonify([p.to_dict() for p in positions])

    @bp.route("/api/payroll/positions/<entity_code>", methods=["POST"])
    def save_payroll_positions(entity_code):
        """Save/update all payroll positions for an entity (full replace)."""
        data = request.get_json()
        positions_data = data.get("positions", [])
        # Delete existing and re-insert
        PayrollPosition.query.filter_by(entity_code=entity_code, budget_year=BUDGET_YEAR).delete()
        for i, p in enumerate(positions_data):
            pos = PayrollPosition(
                entity_code=entity_code,
                budget_year=BUDGET_YEAR,
                position_name=p.get("position_name", "").strip(),
                employee_count=int(p.get("employee_count", 0) or 0),
                hourly_rate=float(p.get("hourly_rate", 0) or 0),
                bonus_per_employee=float(p.get("bonus_per_employee", 0) or 0),
                effective_week_override=(float(p["effective_week_override"]) if p.get("effective_week_override") not in (None, "", 0) else None),
                sort_order=i
            )
            db.session.add(pos)
        db.session.commit()
        positions = PayrollPosition.query.filter_by(
            entity_code=entity_code, budget_year=BUDGET_YEAR
        ).order_by(PayrollPosition.sort_order).all()
        return jsonify({"status": "ok", "positions": [p.to_dict() for p in positions]})

    @bp.route("/api/payroll/assumptions/<entity_code>", methods=["GET"])
    def get_payroll_assumptions(entity_code):
        """Get payroll-tab-specific assumptions. Falls back to main assumptions if none saved."""
        pa = PayrollAssumption.query.filter_by(entity_code=entity_code, budget_year=BUDGET_YEAR).first()
        if pa:
            return jsonify(pa.to_dict())
        # Fall back: seed from main assumptions tab
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
        if not budget:
            return jsonify({"assumptions": {}})
        import json as _json
        main_a = _json.loads(budget.assumptions_json or "{}")
        # Build payroll-specific structure from main assumptions
        pt = main_a.get("payroll_tax", {})
        ub = main_a.get("union_benefits", {})
        wc = main_a.get("workers_comp", {})
        wi = main_a.get("wage_increase", {})
        seeded = {
            "wage_increase_pct": float(wi.get("percent", 0) or 0),
            "effective_week": wi.get("effective_week", "16"),
            "pre_increase_weeks": int(wi.get("pre_increase_weeks", 15) or 15),
            "post_increase_weeks": int(wi.get("post_increase_weeks", 37) or 37),
            "ot_factor": 0.002,
            "vac_sick_hol_factor": 0.10,
            "fica": float(pt.get("FICA", 0) or 0),
            "sui": float(pt.get("SUI", 0) or 0),
            "fui": float(pt.get("FUI", 0) or 0),
            "mta": float(pt.get("MTA", 0) or 0),
            "nys_disability": float(pt.get("NYS_Disability", 0) or 0),
            "pfl": float(pt.get("PFL", 0) or 0),
            "workers_comp": float(wc.get("percent", 0) or 0),
            "welfare_monthly": float(ub.get("welfare_monthly", 0) or 0),
            "pension_weekly": float(ub.get("pension_weekly", 0) or 0),
            "supp_retirement_weekly": float(ub.get("supp_retirement_weekly", 0) or 0),
            "legal_monthly": float(ub.get("legal_monthly", 0) or 0),
            "training_monthly": float(ub.get("training_monthly", 0) or 0),
            "profit_sharing_quarterly": float(ub.get("profit_sharing_quarterly", 0) or 0),
        }
        return jsonify({"assumptions": seeded, "source": "main_assumptions"})

    @bp.route("/api/payroll/assumptions/<entity_code>", methods=["POST"])
    def save_payroll_assumptions(entity_code):
        """Save payroll-tab-specific assumptions (override main assumptions for this tab)."""
        data = request.get_json()
        assumptions = data.get("assumptions", {})
        import json as _json
        pa = PayrollAssumption.query.filter_by(entity_code=entity_code, budget_year=BUDGET_YEAR).first()
        if not pa:
            pa = PayrollAssumption(entity_code=entity_code, budget_year=BUDGET_YEAR)
            db.session.add(pa)
        pa.assumptions_json = _json.dumps(assumptions)
        db.session.commit()
        return jsonify({"status": "ok", "assumptions": assumptions})


    @bp.route("/api/budget-history/<entity_code>", methods=["GET"])
    def get_budget_history(entity_code):
        """Get change history (revisions) for a budget."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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
            output_path = _Path(tmpdir) / f"{entity_code}_{budget.building_name}_{BUDGET_YEAR}_Budget.xlsx"
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


    # ─── Budget Summary API ──────────────────────────────────────────────

    def _gl_matches_prefixes(gl_code, prefixes):
        """Check if a GL code starts with any of the given prefixes."""
        if not gl_code or not prefixes:
            return False
        gl_base = gl_code.split("-")[0].strip()
        for prefix in prefixes:
            if gl_base.startswith(prefix):
                return True
        return False

    def _section_key(section_label):
        """Map section label to internal key for subtotal grouping."""
        if not section_label:
            return ""
        sl = section_label.lower().strip()
        if "non" in sl and "income" in sl:
            return "non_operating_income"
        if "non" in sl and "expense" in sl:
            return "non_operating_expense"
        if sl == "income":
            return "income"
        if sl == "expenses":
            return "expenses"
        return ""

    def _aggregate_by_prefix(budget_lines_dicts, prefixes, ytd_months):
        """Sum budget_lines matching GL prefixes. Returns ytd/estimate/forecast/current_budget."""
        totals = {"ytd_actual": 0.0, "estimate": 0.0, "forecast": 0.0, "current_budget": 0.0, "proposed_budget": 0.0, "count": 0}
        for line in budget_lines_dicts:
            gl = line.get("gl_code", "")
            if not _gl_matches_prefixes(gl, prefixes):
                continue
            ytd = float(line.get("ytd_actual", 0) or 0)
            accrual = float(line.get("accrual_adj", 0) or 0)
            unpaid = float(line.get("unpaid_bills", 0) or 0)
            prior = float(line.get("prior_year", 0) or 0)
            ytd_total = ytd + accrual + unpaid
            remaining = 12 - ytd_months
            # One-time fee rule: once YTD posted, no more projection
            if gl in ONE_TIME_FEE_GLS and abs(ytd_total) > 0.01:
                est = 0
            elif ytd_months > 0:
                est = (ytd_total / ytd_months) * remaining
            else:
                est = 0
            line_forecast = ytd_total + est
            totals["ytd_actual"] += ytd_total
            totals["estimate"] += est
            totals["forecast"] += line_forecast
            totals["current_budget"] += float(line.get("current_budget", 0) or 0)
            proposed = float(line.get("proposed_budget", 0) or 0)
            if proposed == 0 and line_forecast > 0:
                inc_pct = float(line.get("increase_pct", 0) or 0)
                proposed = line_forecast * (1 + inc_pct)
            totals["proposed_budget"] += proposed
            totals["count"] += 1
        return totals


    @bp.route("/api/budget/ensure", methods=["POST"])
    def api_budget_ensure():
        """Create a Budget record if one doesn't already exist for this entity/year.

        Used by bulk onboarding to seed buildings before importing summary rows.
        """
        data = request.get_json()
        if not data or "entity_code" not in data:
            return jsonify({"error": "entity_code required"}), 400

        entity_code = data["entity_code"]
        building_name = data.get("building_name", "Unknown")
        year = data.get("year", BUDGET_YEAR)

        existing = Budget.query.filter_by(entity_code=entity_code, year=year).first()
        if existing:
            return jsonify({"status": "exists", "entity_code": entity_code, "budget_id": existing.id})

        budget = Budget(
            entity_code=entity_code,
            building_name=building_name,
            year=year,
            status="not_started",
        )
        db.session.add(budget)
        db.session.commit()
        return jsonify({"status": "created", "entity_code": entity_code, "budget_id": budget.id})


    @bp.route("/api/summary/import/<entity_code>", methods=["POST"])
    def api_summary_import(entity_code):
        """Import budget summary row framework + Col 1 / Col 6 from parsed Excel.

        Accepts JSON matching batch_import.extract_importable_data() output.
        Upserts rows by entity_code + budget_year + display_order.
        """
        import json as _json

        data = request.get_json()
        if not data or "rows" not in data:
            return jsonify({"error": "Missing rows data"}), 400

        budget_year = BUDGET_YEAR  # Current cycle year — all imports target BUDGET_YEAR
        source_file = data.get("source_file", "")

        # Auto-create Budget record if missing (belt + suspenders for bulk onboard)
        building_name = data.get("building_name", "Unknown")
        if not Budget.query.filter_by(entity_code=entity_code, year=budget_year).first():
            db.session.add(Budget(
                entity_code=entity_code,
                building_name=building_name,
                year=budget_year,
                status="not_started",
            ))
            db.session.flush()

        imported = 0
        updated = 0

        for i, row in enumerate(data["rows"]):
            display_order = row.get("display_order") or (i + 1)

            existing = BudgetSummaryRow.query.filter_by(
                entity_code=entity_code,
                budget_year=budget_year,
                display_order=display_order,
            ).first()

            # Apply canonical Yardi prefix overrides for known-stale labels.
            # This catches push files generated from the legacy chart-of-accounts
            # (Electric/Steam/Gas/Water & Sewer/Supplies) and auto-corrects them
            # on the way in, so no future per-building redeployment is needed.
            incoming_prefixes = row.get("gl_prefixes") or []
            corrected_prefixes = apply_summary_prefix_override(row.get("label"), incoming_prefixes)
            gl_pj = None
            if corrected_prefixes:
                gl_pj = _json.dumps(corrected_prefixes)

            if existing:
                existing.label = row["label"]
                existing.section = row.get("section")
                existing.row_type = row.get("row_type", "data")
                existing.footnote_marker = row.get("footnote_marker")
                existing.col1_prior_actual = row.get("col1_prior_actual")
                existing.col6_approved_budget = row.get("col6_approved_budget")
                existing.source_tab = row.get("source_tab") or existing.source_tab
                existing.gl_prefixes_json = gl_pj or existing.gl_prefixes_json
                existing.source_file = source_file or existing.source_file
                existing.updated_at = datetime.utcnow()
                updated += 1
            else:
                db.session.add(BudgetSummaryRow(
                    entity_code=entity_code,
                    budget_year=budget_year,
                    display_order=display_order,
                    label=row["label"],
                    section=row.get("section"),
                    row_type=row.get("row_type", "data"),
                    footnote_marker=row.get("footnote_marker"),
                    col1_prior_actual=row.get("col1_prior_actual"),
                    col6_approved_budget=row.get("col6_approved_budget"),
                    col7_proposed_budget=None,
                    source_tab=row.get("source_tab"),
                    gl_prefixes_json=gl_pj,
                    source_file=source_file,
                ))
                imported += 1

        db.session.commit()
        return jsonify({
            "status": "ok",
            "entity_code": entity_code,
            "budget_year": budget_year,
            "imported": imported,
            "updated": updated,
            "total_rows": len(data["rows"]),
        })


    @bp.route("/api/summary/<entity_code>", methods=["GET"])
    def api_summary_get(entity_code):
        """Return full 8-column budget summary for a building.

        Stored columns: col1 (2024 Actual), col6 (2026 Budget), col7 (2027 Budget).
        Computed columns: col2 (2025 Actual — TBD), col3-col5 from budget_lines
        via GL prefix aggregation, col8 = % variance.
        """
        import json as _json

        budget_year = request.args.get("year", BUDGET_YEAR, type=int)

        summary_rows = BudgetSummaryRow.query.filter_by(
            entity_code=entity_code, budget_year=budget_year
        ).order_by(BudgetSummaryRow.display_order).all()

        # Fallback: if no rows for requested year, use latest available year
        if not summary_rows:
            latest = db.session.query(db.func.max(BudgetSummaryRow.budget_year)).filter_by(
                entity_code=entity_code
            ).scalar()
            if latest:
                budget_year = latest
                summary_rows = BudgetSummaryRow.query.filter_by(
                    entity_code=entity_code, budget_year=budget_year
                ).order_by(BudgetSummaryRow.display_order).all()

        if not summary_rows:
            return jsonify({"error": "No summary data found", "entity_code": entity_code}), 404

        # Fetch budget_lines for GL aggregation (cols 3-5)
        budget = Budget.query.filter_by(entity_code=entity_code, year=budget_year).first()
        bl_dicts = []
        ytd_months = 2

        if budget:
            lines = BudgetLine.query.filter_by(budget_id=budget.id).all()
            bl_dicts = [l.to_dict() for l in lines]
            try:
                assumptions = _json.loads(budget.assumptions_json) if budget.assumptions_json else {}
                bp_val = assumptions.get("budget_period", "")
                if "/" in str(bp_val):
                    ytd_months = int(str(bp_val).split("/")[0])
            except Exception:
                pass

        # ── Col 2: 2025 Actual from confirmed audited financials ──────────
        col2_lookup = {}
        col2_meta = {}        # {summary_label: {matched_category, match_type}}
        audit_info = None     # {id, fiscal_year_end, confirmed_at, confirmed_by, pdf_filename}
        try:
            # Label aliases: audit category variant → canonical summary label
            _LABEL_ALIASES = {
                "Common Charges": "Maintenance", "Gas - Heating": "Gas Cooking / Heating",
                "Gas Heating": "Gas Cooking / Heating", "Gas": "Gas Cooking / Heating",
                "Oil / Fuel": "Fuel", "Fuel Oil": "Fuel",
                "RE Taxes": "Real Estate Taxes", "Real Estate Tax": "Real Estate Taxes",
                "Assessment - Operating": "Assessment-Operating",
                "Storage Income": "Storage Room",
                "Garage": "Commercial Rent (Garage)",
                "Interest Income": "Other Income",
            }
            # Query audit_uploads directly (model defined in factory, can't import)
            fy = str(budget_year - 2)  # Col 2 = BY-2 actual
            row_au = db.session.execute(db.text(
                "SELECT id, mapped_data, fiscal_year_end, confirmed_at, confirmed_by, pdf_filename FROM audit_uploads "
                "WHERE entity_code = :ec AND fiscal_year_end = :fy AND status = 'confirmed' "
                "ORDER BY confirmed_at DESC LIMIT 1"
            ), {"ec": entity_code, "fy": fy}).fetchone()
            if row_au and row_au[1]:
                audit_info = {
                    "id": row_au[0],
                    "fiscal_year_end": row_au[2],
                    "confirmed_at": row_au[3].isoformat() if row_au[3] else None,
                    "confirmed_by": row_au[4] or "",
                    "pdf_filename": row_au[5] or "",
                }
                mapped_raw = _json.loads(row_au[1])
                # Extract {category: year_totals[0]} from mapped_data
                confirmed = {}
                for cat, info in mapped_raw.items():
                    if isinstance(info, dict):
                        totals = info.get("year_totals", [])
                        if totals and len(totals) > 0:
                            confirmed[cat] = totals[0]
                        elif info.get("total"):
                            confirmed[cat] = info["total"]
                # Build reverse alias: canonical → [variants]
                alias_reverse = {}
                for variant, canonical in _LABEL_ALIASES.items():
                    alias_reverse.setdefault(canonical, []).append(variant)
                # Build label set from this building's summary rows
                building_labels = {r.label for r in summary_rows if r.row_type == "data"}
                for cat, amount in confirmed.items():
                    if amount is None:
                        continue
                    # Direct match first
                    if cat in building_labels:
                        col2_lookup[cat] = col2_lookup.get(cat, 0) + amount
                        col2_meta[cat] = {"matched_category": cat, "match_type": "direct"}
                    else:
                        # Try alias: audit category might be a variant
                        canonical = _LABEL_ALIASES.get(cat, cat)
                        if canonical in building_labels:
                            col2_lookup[canonical] = col2_lookup.get(canonical, 0) + amount
                            col2_meta[canonical] = {"matched_category": cat, "match_type": "alias"}
                        else:
                            # Try reverse: building label might be a variant of audit category
                            for variant in alias_reverse.get(cat, []):
                                if variant in building_labels:
                                    col2_lookup[variant] = col2_lookup.get(variant, 0) + amount
                                    col2_meta[variant] = {"matched_category": cat, "match_type": "alias_reverse"}
                                    break
        except Exception as _col2_err:
            col2_lookup = {"_error": str(_col2_err)}

        # Helper: per-line detail for a given GL prefix set (lineage breakdown)
        def _lines_for_prefixes(prefixes):
            out = []
            remaining = 12 - ytd_months
            for line in bl_dicts:
                gl = line.get("gl_code", "")
                if not _gl_matches_prefixes(gl, prefixes):
                    continue
                ytd = float(line.get("ytd_actual", 0) or 0)
                accrual = float(line.get("accrual_adj", 0) or 0)
                unpaid = float(line.get("unpaid_bills", 0) or 0)
                ytd_total = ytd + accrual + unpaid
                est = (ytd_total / ytd_months) * remaining if ytd_months > 0 else 0
                out.append({
                    "gl": gl,
                    "desc": line.get("description") or line.get("gl_description") or "",
                    "ytd": round(ytd, 2),
                    "accrual": round(accrual, 2),
                    "unpaid": round(unpaid, 2),
                    "estimate": round(est, 2),
                    "forecast": round(ytd_total + est, 2),
                })
            return out

        # Build response rows
        result_rows = []
        section_data = {"income": [], "expenses": [], "non_operating_income": [], "non_operating_expense": []}

        for row in summary_rows:
            if row.row_type == "section_header":
                result_rows.append({
                    "label": row.label, "row_type": "section_header",
                    "section": row.section, "display_order": row.display_order,
                    "col1": None, "col2": None, "col3": None, "col4": None,
                    "col5": None, "col6": None, "col7": None, "col8": None,
                    "source_tab": None,
                })
                continue

            col1 = row.col1_prior_actual
            col6 = row.col6_approved_budget
            col7 = row.col7_proposed_budget

            # Compute cols 3-5 from budget_lines via GL prefix aggregation
            col2 = col2_lookup.get(row.label) if isinstance(col2_lookup, dict) and "_error" not in col2_lookup else None
            col3 = None   # 2026 YTD actual
            col4 = None   # 2026 estimate
            col5 = None   # 2026 forecast

            prefixes = []
            agg_count = 0
            if row.row_type == "data" and row.gl_prefixes_json and bl_dicts:
                try:
                    prefixes = _json.loads(row.gl_prefixes_json)
                except Exception:
                    prefixes = []
                if prefixes:
                    agg = _aggregate_by_prefix(bl_dicts, prefixes, ytd_months)
                    agg_count = agg.get("count", 0)
                    if agg_count > 0:
                        col3 = round(agg["ytd_actual"], 2)
                        col4 = round(agg["estimate"], 2)
                        col5 = round(agg["forecast"], 2)

            # ── Fixed-forecast GL override ─────────────────────────────
            # Maintenance / Common Charges / Commercial Rent rows: pin
            # Col 5 (Forecast) to Col 6 (Approved Budget), back-solve
            # Col 4 (Estimate) = Col 5 - Col 3. Matched by GL prefix.
            fixed_forecast_applied = False
            if (row.row_type == "data"
                    and _row_has_fixed_forecast_gl(row.gl_prefixes_json)
                    and col6 is not None):
                col5 = round(float(col6), 2)
                col4 = round(col5 - (col3 or 0), 2)
                fixed_forecast_applied = True

            # Col 8: % variance = (col7 - col5) / |col5| * 100
            col8 = None
            if col7 is not None and col5 and col5 != 0:
                col8 = round(((col7 - col5) / abs(col5)) * 100, 1)

            # ── Lineage payload for inspector drill-down ──────────
            lineage = None
            if row.row_type == "data":
                c2_meta = col2_meta.get(row.label, {}) if isinstance(col2_meta, dict) else {}
                lineage = {
                    "c2": {
                        "value": col2,
                        "audit_year": str(budget_year - 2),
                        "matched_category": c2_meta.get("matched_category"),
                        "match_type": c2_meta.get("match_type"),
                        "audit_id": audit_info.get("id") if audit_info else None,
                        "audit_fy": audit_info.get("fiscal_year_end") if audit_info else None,
                        "audit_confirmed_at": audit_info.get("confirmed_at") if audit_info else None,
                        "audit_confirmed_by": audit_info.get("confirmed_by") if audit_info else None,
                        "audit_filename": audit_info.get("pdf_filename") if audit_info else None,
                        "has_audit": bool(audit_info),
                    },
                    "gl": {
                        "prefixes": prefixes,
                        "ytd_months": ytd_months,
                        "remaining_months": 12 - ytd_months,
                        "lines": _lines_for_prefixes(prefixes) if (prefixes and bl_dicts) else [],
                    },
                    "fixed_forecast": {
                        "applied": fixed_forecast_applied,
                        "col5_source": "approved_budget" if fixed_forecast_applied else "gl_aggregation",
                        "col4_formula": "col5 - col3" if fixed_forecast_applied else "gl_aggregation",
                        "note": ("Forecast pinned to Approved Budget "
                                 "(Maintenance / Common Charges / Commercial Rent rule)")
                                 if fixed_forecast_applied else None,
                    },
                }

            rd = {
                "id": row.id,
                "label": row.label,
                "row_type": row.row_type,
                "section": row.section,
                "display_order": row.display_order,
                "footnote_marker": row.footnote_marker,
                "col1": col1, "col2": col2, "col3": col3,
                "col4": col4, "col5": col5, "col6": col6,
                "col7": col7, "col8": col8,
                "source_tab": row.source_tab,
                "lineage": lineage,
            }
            result_rows.append(rd)

            # Track data rows for subtotal computation
            if row.row_type == "data":
                sk = _section_key(row.section)
                if sk in section_data:
                    section_data[sk].append(rd)

        # Recompute subtotal cols (3-5, 7, 8) from data rows
        for rd in result_rows:
            if rd["row_type"] != "subtotal":
                continue
            label_lower = (rd.get("label") or "").lower()
            if "total income" in label_lower:
                data_rows = section_data.get("income", [])
            elif "total expenses" in label_lower and "non" not in label_lower:
                data_rows = section_data.get("expenses", [])
            elif "net operating" in label_lower:
                inc = section_data.get("income", [])
                exp = section_data.get("expenses", [])
                for ck in ["col2", "col3", "col4", "col5", "col7"]:
                    iv = sum(r.get(ck) or 0 for r in inc)
                    ev = sum(r.get(ck) or 0 for r in exp)
                    rd[ck] = round(iv - ev, 2) if (iv or ev) else None
                if rd["col7"] is not None and rd["col5"] and rd["col5"] != 0:
                    rd["col8"] = round(((rd["col7"] - rd["col5"]) / abs(rd["col5"])) * 100, 1)
                continue
            elif "non" in label_lower and "income" in label_lower:
                data_rows = section_data.get("non_operating_income", [])
            elif "non" in label_lower and "expense" in label_lower:
                data_rows = section_data.get("non_operating_expense", [])
            elif "total surplus" in label_lower or "total deficit" in label_lower:
                # Grand total = net operating + non-op income - non-op expense
                inc = section_data.get("income", [])
                exp = section_data.get("expenses", [])
                noi = section_data.get("non_operating_income", [])
                noe = section_data.get("non_operating_expense", [])
                for ck in ["col2", "col3", "col4", "col5", "col7"]:
                    iv = sum(r.get(ck) or 0 for r in inc)
                    ev = sum(r.get(ck) or 0 for r in exp)
                    ni = sum(r.get(ck) or 0 for r in noi)
                    ne = sum(r.get(ck) or 0 for r in noe)
                    rd[ck] = round((iv - ev) + ni - ne, 2) if (iv or ev or ni or ne) else None
                if rd["col7"] is not None and rd["col5"] and rd["col5"] != 0:
                    rd["col8"] = round(((rd["col7"] - rd["col5"]) / abs(rd["col5"])) * 100, 1)
                continue
            else:
                data_rows = []

            # Simple sum for section subtotals
            for ck in ["col2", "col3", "col4", "col5", "col7"]:
                vals = [r.get(ck) or 0 for r in data_rows]
                rd[ck] = round(sum(vals), 2) if any(v != 0 for v in vals) else None
            if rd["col7"] is not None and rd["col5"] and rd["col5"] != 0:
                rd["col8"] = round(((rd["col7"] - rd["col5"]) / abs(rd["col5"])) * 100, 1)

        return jsonify({
            "entity_code": entity_code,
            "budget_year": budget_year,
            "ytd_months": ytd_months,
            "rows": result_rows,
            "stats": {
                "total_rows": len(result_rows),
                "data_rows": len([r for r in result_rows if r["row_type"] == "data"]),
                "has_budget_lines": len(bl_dicts) > 0,
            },
            "_debug_col2": col2_lookup,
        })


    @bp.route("/api/summary/<entity_code>", methods=["PUT"])
    def api_summary_edit(entity_code):
        """FA edits Col 7 (proposed budget) on summary rows.

        Accepts JSON: {"edits": [{"display_order": N, "col7": value}, ...]}
        Logs each change to budget_revisions.
        """
        data = request.get_json()
        if not data or "edits" not in data:
            return jsonify({"error": "Missing edits"}), 400

        budget_year = data.get("budget_year", BUDGET_YEAR)
        user_id = data.get("user_id")

        # Need a budget record for revision logging
        budget = Budget.query.filter_by(entity_code=entity_code, year=budget_year).first()

        updated = 0
        for edit in data["edits"]:
            display_order = edit.get("display_order")
            new_val = edit.get("col7")
            if display_order is None:
                continue

            row = BudgetSummaryRow.query.filter_by(
                entity_code=entity_code,
                budget_year=budget_year,
                display_order=display_order,
            ).first()
            if not row:
                continue

            old_val = row.col7_proposed_budget
            row.col7_proposed_budget = float(new_val) if new_val is not None else None
            row.updated_at = datetime.utcnow()

            # Log to budget_revisions if budget exists
            if budget:
                db.session.add(BudgetRevision(
                    budget_id=budget.id,
                    user_id=user_id,
                    action="summary_edit",
                    field_name=f"col7:{row.label}",
                    old_value=str(old_val) if old_val is not None else "",
                    new_value=str(new_val) if new_val is not None else "",
                    source="web",
                ))
            updated += 1

        db.session.commit()
        return jsonify({"status": "ok", "updated": updated})


    # ─── Presentation Routes ───────────────────────────────────────────────

    @bp.route("/api/presentation/generate/<entity_code>", methods=["POST"])
    def generate_presentation_link(entity_code):
        """Generate a shareable presentation token for a budget."""
        import secrets
        budget = Budget.query.filter_by(entity_code=entity_code, year=BUDGET_YEAR).first()
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

        sheet_order = ["Income", "Payroll", "Energy", "Water & Sewer", "Repairs & Supplies", "Gen & Admin", "Capital"]
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

    return (bp, {"User": User, "BuildingAssignment": BuildingAssignment, "Budget": Budget, "BudgetLine": BudgetLine, "BudgetRevision": BudgetRevision, "PayrollPosition": PayrollPosition, "PayrollAssumption": PayrollAssumption, "BudgetSummaryRow": BudgetSummaryRow},
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
/* Force scrollbars always visible (fixes macOS auto-hide on horizontal/vertical scroll) */
::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
::-webkit-scrollbar-track { background: #f1f5f9; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
::-webkit-scrollbar-corner { background: #f1f5f9; }
* { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
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
/* Force scrollbars always visible (fixes macOS auto-hide on horizontal/vertical scroll) */
::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
::-webkit-scrollbar-track { background: #f1f5f9; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
::-webkit-scrollbar-corner { background: #f1f5f9; }
* { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
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
        <div onclick="toggleBuildingsCollapse()" style="display:flex; align-items:center; gap:10px; cursor:pointer; user-select:none;" title="Click to collapse/expand">
          <span id="buildingsChevron" style="display:inline-block; transition:transform 0.2s; font-size:12px; color:var(--gray-500);">&#9660;</span>
          <h2 style="margin-bottom:0;">All Buildings</h2>
        </div>
        <input type="text" id="budgetSearch" placeholder="Search buildings..." oninput="filterBudgetTable()"
          style="padding:8px 14px; border:1px solid var(--gray-200); border-radius:8px; font-size:14px; width:260px; outline:none;">
      </div>
      <div id="buildingsTableWrap">
        <table id="budgets-table">
          <thead>
            <tr>
              <th data-sort="building_name" onclick="sortBuildings('building_name')" style="cursor:pointer; user-select:none; white-space:nowrap;">Building <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th data-sort="entity_code" onclick="sortBuildings('entity_code')" style="cursor:pointer; user-select:none; white-space:nowrap;">Entity <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th data-sort="pm_name" onclick="sortBuildings('pm_name')" style="cursor:pointer; user-select:none; white-space:nowrap;">PM <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th>Data Status</th>
              <th data-sort="pm_review" onclick="sortBuildings('pm_review')" style="cursor:pointer; user-select:none; white-space:nowrap;">PM Review <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th data-sort="status" onclick="sortBuildings('status')" style="cursor:pointer; user-select:none; white-space:nowrap;">Status <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th data-sort="days" onclick="sortBuildings('days')" style="cursor:pointer; user-select:none; white-space:nowrap;">Days <span class="sort-arrow" style="opacity:0.25;">&#9650;</span></th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
const statusLabels = {
  'not_started': 'Not Started',
  'data_collection': 'Data Collection',
  'data_ready': 'Data Ready',
  'draft': 'Draft',
  'pm_pending': 'Pending PM',
  'pm_in_progress': 'PM In Progress',
  'fa_review': 'FA Review',
  'exec_review': 'Exec Review',
  'presentation': 'Presentation',
  'approved': 'Approved',
  'returned': 'Returned',
  'ar_pending': 'AR Pending',
  'ar_complete': 'AR Complete'
};
// Fallback: any unknown status gets snake_case → Title Case automatically
function formatStatus(s) {
  if (!s) return '';
  if (statusLabels[s]) return statusLabels[s];
  return s.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}

function showToast(msg, type='info') {
  const c = document.getElementById('toastContainer');
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity 0.3s'; setTimeout(() => t.remove(), 300); }, 3000);
}

const _pmByEntity = {};
let _budgetsCache = [];
let _sortState = { column: 'entity_code', direction: 'asc' };
const _pmStatusMap = {
  'draft': 'Not Sent',
  'pm_pending': 'Sent to PM',
  'pm_in_progress': 'PM Working',
  'fa_review': 'Submitted',
  'approved': 'Approved',
  'returned': 'Returned'
};

async function loadBudgets() {
  try {
    const [res, aRes] = await Promise.all([fetch('/api/budgets'), fetch('/api/assignments')]);
    const budgets = await res.json();
    try {
      const assignments = await aRes.json();
      assignments.forEach(a => { if (a.role === 'pm') _pmByEntity[a.entity_code] = a.user_name; });
    } catch(e) { console.warn('Assignments fetch failed:', e); }
    _budgetsCache = budgets;
    renderBudgets(budgets);
    renderStatusSummary(budgets);
    updateSortArrows();
    document.getElementById('loadingState').style.display = 'none';
    document.getElementById('dashboardContent').style.display = '';
    return budgets;
  } catch (err) {
    console.error('Failed to load budgets:', err);
    document.getElementById('loadingState').innerHTML = '<p style="color:var(--red);">Failed to load budgets. Please refresh.</p>';
    return [];
  }
}

function _getSortValue(b, col) {
  if (col === 'entity_code') return Number(b.entity_code) || 0;
  if (col === 'building_name') return (b.building_name || '').toLowerCase();
  if (col === 'pm_name') return (_pmByEntity[b.entity_code] || '').toLowerCase();
  if (col === 'status') return (b.status || '').toLowerCase();
  if (col === 'pm_review') return (_pmStatusMap[b.status] || b.status || '').toLowerCase();
  if (col === 'days') {
    const doneStatuses = ['approved','ar_pending','ar_complete'];
    if (doneStatuses.includes(b.status) || !b.updated_at) return -1;
    return Math.floor((Date.now() - new Date(b.updated_at).getTime()) / 86400000);
  }
  return '';
}

function sortBuildings(col) {
  // Days defaults to desc (most stale first); others default to asc
  const defaultDir = col === 'days' ? 'desc' : 'asc';
  if (_sortState.column === col) {
    _sortState.direction = _sortState.direction === 'asc' ? 'desc' : 'asc';
  } else {
    _sortState.column = col;
    _sortState.direction = defaultDir;
  }
  renderBudgets(_budgetsCache);
  updateSortArrows();
  filterBudgetTable();
}

function updateSortArrows() {
  document.querySelectorAll('#budgets-table th[data-sort]').forEach(th => {
    const arrow = th.querySelector('.sort-arrow');
    if (!arrow) return;
    if (th.dataset.sort === _sortState.column) {
      arrow.innerHTML = _sortState.direction === 'asc' ? '&#9650;' : '&#9660;';
      arrow.style.opacity = '1';
    } else {
      arrow.innerHTML = '&#9650;';
      arrow.style.opacity = '0.25';
    }
  });
}

function toggleBuildingsCollapse() {
  const wrap = document.getElementById('buildingsTableWrap');
  const chevron = document.getElementById('buildingsChevron');
  const isCollapsed = wrap.style.display === 'none';
  if (isCollapsed) {
    wrap.style.display = '';
    chevron.style.transform = 'rotate(0deg)';
    localStorage.setItem('fa-dashboard-buildings-collapsed', 'false');
  } else {
    wrap.style.display = 'none';
    chevron.style.transform = 'rotate(-90deg)';
    localStorage.setItem('fa-dashboard-buildings-collapsed', 'true');
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
      <div class="label">${formatStatus(status)}</div>
    `;
    summary.appendChild(card);
  });
}

function renderBudgets(budgets) {
  const tbody = document.querySelector('#budgets-table tbody');
  tbody.innerHTML = '';

  // Sort by current sort state
  const col = _sortState.column;
  const dir = _sortState.direction === 'asc' ? 1 : -1;
  budgets.sort((a, b) => {
    const va = _getSortValue(a, col);
    const vb = _getSortValue(b, col);
    if (va < vb) return -1 * dir;
    if (va > vb) return 1 * dir;
    return 0;
  });

  budgets.forEach(b => {
    const tr = document.createElement('tr');
    const statusLabel = formatStatus(b.status);
    const statusClass = `pill-${b.status}`;

    // Data completeness - compact inline format
    function dataIcon(ok) { return ok ? '<span style="color:var(--green);">&#10003;</span>' : '<span style="color:var(--gray-300);">&#10007;</span>'; }

    // PM review status pill
    const pmStatusMap = {
      'draft': 'Not Sent',
      'pm_pending': 'Sent to PM',
      'pm_in_progress': 'PM Working',
      'fa_review': 'Submitted',
      'approved': 'Approved',
      'returned': 'Returned'
    };
    const pmLabel = pmStatusMap[b.status] || formatStatus(b.status);

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

    // Compact data status: icons + dates on one line
    const ts = b.timestamps || {};
    function fmtDt(iso) { if (!iso) return '—'; const d = new Date(iso); return (d.getMonth()+1) + '/' + d.getDate(); }
    const dataItems = [
      { label: 'Bud', ok: true, dt: ts.budget_summary, tip: 'Budget summary imported' },
      { label: 'Exp', ok: b.has_expenses, dt: ts.expense_dist, tip: 'Expense distribution' },
      { label: 'YSL', ok: !!ts.ysl, dt: ts.ysl, tip: 'YSL data from Yardi' },
      { label: 'AP', ok: !!ts.open_ap, dt: ts.open_ap, tip: 'AP Aging imported' }
    ];
    const dataHtml = '<div style="font-size:11px; display:flex; gap:8px; flex-wrap:nowrap; white-space:nowrap;">' +
      dataItems.map(i => `<span title="${i.tip}" style="color:${i.ok ? 'var(--green)' : 'var(--gray-300)'};">${i.ok ? '&#10003;' : '&#10007;'}${i.label} <span style="font-size:10px;">${fmtDt(i.dt)}</span></span>`).join('') +
      '</div>';

    // SLA: days in current status (using updated_at as proxy)
    const doneStatuses = ['approved','ar_pending','ar_complete'];
    let daysHtml = '<span style="color:var(--gray-300);">\u2014</span>';
    if (!doneStatuses.includes(b.status) && b.updated_at) {
      const days = Math.floor((Date.now() - new Date(b.updated_at).getTime()) / 86400000);
      const color = days >= 14 ? 'var(--red)' : days >= 7 ? '#d97706' : 'var(--green)';
      const icon = days >= 14 ? ' \uD83D\uDD34' : days >= 7 ? ' \uD83D\uDFE1' : '';
      daysHtml = `<span style="font-weight:700;color:${color};">${days}d</span>${icon}`;
    }

    const pmName = _pmByEntity[b.entity_code] || '\u2014';
    tr.innerHTML = `
      <td><a href="/dashboard/${b.entity_code}" style="color: var(--blue); text-decoration: none; font-weight:500;">${b.building_name}</a></td>
      <td style="font-family:monospace; font-size:13px;">${b.entity_code}</td>
      <td style="font-size:12px; color:var(--gray-500); white-space:nowrap;">${pmName}</td>
      <td>${dataHtml}</td>
      <td><span class="pill ${statusClass}">${pmLabel}</span></td>
      <td><span class="pill ${statusClass}">${statusLabel}</span></td>
      <td style="text-align:center;">${daysHtml}</td>
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
  if (!confirm(`Change status to ${formatStatus(newStatus)}?`)) return;
  try {
    await fetch(`/api/budgets/${entity}/status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: newStatus })
    });
    showToast('Status updated to ' + formatStatus(newStatus), 'success');
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
  // Restore collapse state before loading
  if (localStorage.getItem('fa-dashboard-buildings-collapsed') === 'true') {
    const wrap = document.getElementById('buildingsTableWrap');
    const chevron = document.getElementById('buildingsChevron');
    if (wrap) wrap.style.display = 'none';
    if (chevron) chevron.style.transform = 'rotate(-90deg)';
  }
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
/* Force scrollbars always visible (fixes macOS auto-hide on horizontal/vertical scroll) */
::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
::-webkit-scrollbar-track { background: #f1f5f9; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
::-webkit-scrollbar-corner { background: #f1f5f9; }
* { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
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
  .container { max-width: 1760px; margin: 0 auto; padding: 24px 20px; }
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
  @keyframes pmPulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
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
    overflow: clip;
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

  <!-- PM Review Panel — Notes + Invoice Reclasses -->
  <div class="panel" id="pmReviewPanel" style="display:none; margin-bottom:16px;">
    <div class="panel-header" style="background:linear-gradient(to right,#fefce8,#fef9c3); border-bottom:1px solid #fde68a;" onclick="togglePanel(this)">
      <div style="display:flex; align-items:center; gap:8px;">
        <h3 style="color:var(--gray-800);">PM Review</h3>
        <span id="pmReviewBadge" style="display:inline-flex; align-items:center; gap:4px; background:var(--orange); color:white; font-size:11px; font-weight:700; padding:3px 10px; border-radius:12px;"><span style="width:6px;height:6px;background:white;border-radius:50%;animation:pmPulse 1.5s infinite;"></span> <span id="pmReviewBadgeText"></span></span>
      </div>
      <span class="chevron">▾</span>
    </div>
    <div class="panel-body" style="padding:0;">
      <div id="pmReviewTabs" style="display:flex; border-bottom:1px solid var(--gray-200); background:var(--gray-50);">
        <div class="pm-tab active" onclick="switchPmTab(this,'pmNotesContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--blue); cursor:pointer; border-bottom:2px solid var(--blue); background:white;">PM Notes <span id="pmNotesCount" style="background:var(--blue-light); color:var(--blue); font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
        <div class="pm-tab" onclick="switchPmTab(this,'pmReclassContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--gray-500); cursor:pointer; border-bottom:2px solid transparent;">Invoice Reclasses <span id="pmReclassCount" style="background:#fef3c7; color:#92400e; font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
        <div class="pm-tab" onclick="switchPmTab(this,'pmProposalsContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--gray-500); cursor:pointer; border-bottom:2px solid transparent;">Budget Proposals <span id="pmProposalsCount" style="background:#dbeafe; color:#1e40af; font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
      </div>
      <!-- Tab 1: PM Notes -->
      <div id="pmNotesContent" style="padding:16px 20px;">
        <div id="pmNotesEmpty" style="text-align:center; padding:20px; color:var(--gray-400); font-size:13px; display:none;">No PM notes yet.</div>
        <div id="pmNotesContainer"></div>
      </div>
      <!-- Tab 2: Invoice Reclasses -->
      <div id="pmReclassContent" style="padding:16px 20px; display:none;">
        <div id="pmReclassEmpty" style="text-align:center; padding:20px; color:var(--gray-400); font-size:13px; display:none;">No invoice reclasses pending.</div>
        <div id="pmReclassSummary" style="display:none; display:flex; gap:20px; padding:10px 12px; background:var(--gray-50); border-radius:8px; margin-bottom:14px; font-size:12px;"></div>
        <table id="pmReclassTable" style="width:100%; border-collapse:collapse; font-size:13px;">
          <thead><tr>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">From GL</th>
            <th style="font-size:11px; padding:6px 4px; border-bottom:1px solid var(--gray-200);"></th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">To GL</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Invoices</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Amount</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">PM Note</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Action</th>
          </tr></thead>
          <tbody id="pmReclassBody"></tbody>
        </table>
      </div>
      <!-- Tab 3: Budget Proposals -->
      <div id="pmProposalsContent" style="padding:16px 20px; display:none;">
        <div id="pmProposalsEmpty" style="text-align:center; padding:20px; color:var(--gray-400); font-size:13px; display:none;">No PM budget proposals to review.</div>
        <div id="pmProposalsSummary" style="display:none; gap:20px; padding:10px 12px; background:var(--gray-50); border-radius:8px; margin-bottom:14px; font-size:12px;"></div>
        <table id="pmProposalsTable" style="width:100%; border-collapse:collapse; font-size:13px;">
          <thead><tr>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">GL Code</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Description</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Current Budget</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">PM Proposed</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Change</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Method</th>
            <th style="text-align:center; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Status</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Action</th>
          </tr></thead>
          <tbody id="pmProposalsBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Reject/Comment Modal -->
  <div id="proposalModal" style="display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.5); z-index:9999; align-items:center; justify-content:center;">
    <div style="background:white; border-radius:12px; padding:24px; max-width:420px; width:90%; box-shadow:0 20px 60px rgba(0,0,0,0.3);">
      <h3 id="proposalModalTitle" style="margin:0 0 16px; font-size:16px;"></h3>
      <div id="proposalModalOverrideRow" style="margin-bottom:12px; display:none;">
        <label style="font-size:12px; font-weight:600; color:var(--gray-500);">Override Budget Figure ($)</label>
        <input id="proposalModalOverride" type="text" style="width:100%; padding:8px 12px; border:1px solid var(--gray-200); border-radius:6px; margin-top:4px; font-size:14px;" placeholder="Leave blank to revert to formula">
      </div>
      <div style="margin-bottom:16px;">
        <label style="font-size:12px; font-weight:600; color:var(--gray-500);">Note / Reason</label>
        <textarea id="proposalModalNote" rows="3" style="width:100%; padding:8px 12px; border:1px solid var(--gray-200); border-radius:6px; margin-top:4px; font-size:14px; resize:vertical;" placeholder="Add context for this decision..."></textarea>
      </div>
      <div style="display:flex; gap:8px; justify-content:flex-end;">
        <button onclick="closeProposalModal()" style="padding:8px 16px; border:1px solid var(--gray-200); border-radius:6px; background:white; cursor:pointer; font-size:13px;">Cancel</button>
        <button id="proposalModalSubmit" onclick="submitProposalReview()" style="padding:8px 16px; border:none; border-radius:6px; color:white; cursor:pointer; font-size:13px; font-weight:600;"></button>
      </div>
    </div>
  </div>

  <!-- Budget Workbook (PROMOTED — blue border, primary visual element) -->
  <div class="workbook-section">
    <div class="workbook-header">
      <h2>Budget Workbook</h2>
      <div style="display:flex; gap:8px;">
        <button onclick="openBoardPresentation()" id="presLinkBtn" class="btn" style="background:#1e293b; color:white; border:none; font-size:13px; padding:8px 16px; border-radius:6px; cursor:pointer; display:flex; align-items:center; gap:6px;">📊 Board Presentation</button>
        <a href="" id="downloadExcelBtn" class="btn" style="background:var(--green); color:white; text-decoration:none; font-size:13px; padding:8px 16px; border-radius:6px;">Download Excel</a>
      </div>
    </div>
    <div id="sheetTabs" style="display:flex; gap:4px; border-bottom:2px solid var(--gray-200); margin-bottom:0; flex-wrap:wrap; padding:0 24px; background:var(--gray-50);"></div>
    <div id="sheetContent" style="padding:0 24px;"></div>
    <div id="faSaveIndicator" style="font-size:12px; color:var(--green); margin-top:8px; padding:0 24px 12px;"></div>
  </div>

  </div><!-- end detailContent -->
</div>

<script>
const entityCode = '{{ entity_code }}';
const BY = {{ budget_year }};  // Budget year from server config
const BY1 = BY - 1, BY2 = BY - 2, BY3 = BY - 3;

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

  const variance = totalBudget - totalForecast;
  const pctChange = totalForecast ? ((variance) / totalForecast * 100) : 0;
  const absPct = Math.abs(pctChange);
  const varColor = absPct > 10 ? 'var(--red)' : absPct > 5 ? '#d97706' : 'var(--green)';
  const varBg = absPct > 10 ? '#fef2f2' : absPct > 5 ? '#fffbeb' : '#f0fdf4';
  const varBorder = absPct > 10 ? '#fca5a5' : absPct > 5 ? '#fde68a' : '#86efac';
  const arrow = pctChange > 0 ? ' \u25B2' : pctChange < 0 ? ' \u25BC' : '';

  document.getElementById('summaryCards').innerHTML = `
    <div class="summary-card">
      <div class="card-value">${fmt(totalPrior)}</div>
      <div class="card-label">Prior Year</div>
    </div>
    <div class="summary-card">
      <div class="card-value">${fmt(totalBudget)}</div>
      <div class="card-label">Current Budget</div>
    </div>
    <div class="summary-card" style="background:${varBg};border-color:${varBorder};">
      <div class="card-value" style="color:${varColor};">${fmt(variance)}</div>
      <div class="card-label">Variance</div>
    </div>
    <div class="summary-card" style="background:${varBg};border-color:${varBorder};">
      <div class="card-value" style="color:${varColor};">${totalForecast ? pctChange.toFixed(1) + '%' + arrow : '\u2014'}</div>
      <div class="card-label">% Change</div>
    </div>
  `;

  // PM Track — collapsible panel with badge
  const pmStatusLabels = { draft: 'Not Sent', pm_pending: 'Sent to PM', pm_in_progress: 'PM Working', fa_review: 'Submitted for Review', approved: 'Approved', returned: 'Returned' };
  const pmStatus = pmStatusLabels[b.status] || (b.status ? b.status.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ') : '');
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

  const reviewPct = lines.length ? Math.round(linesWithProposed / lines.length * 100) : 0;
  const checks = [
    { group: 'Data Collection', label: 'YSL Data Imported', done: true, detail: lines.length + ' GL lines loaded' },
    { group: 'Data Collection', label: 'Expense Distribution', done: data.expenses.exists, detail: data.expenses.exists ? data.expenses.invoice_count + ' invoices (' + fmt(data.expenses.total_amount) + ')' : 'Upload via Data Collection' },
    { group: 'Data Collection', label: 'Audited Financials', done: data.audit.exists, detail: data.audit.exists ? Object.keys(data.audit.years || {}).length + ' years of history' : 'Upload via Data Collection' },
    { group: 'Configuration', label: 'Assumptions Configured', done: anyAssumptions, detail: hasBudgetPeriod ? 'Period: ' + assumptions.budget_period : 'Not set — click Assumptions tab', action: !anyAssumptions ? 'openAssumptions' : null },
    { group: 'Review', label: 'Review All Sheets', done: linesWithProposed >= lines.length * 0.5, detail: linesWithProposed + ' of ' + lines.length + ' lines have proposed values (' + reviewPct + '%)', progress: reviewPct },
    { group: 'Review', label: 'PM Review', done: pmDone, detail: pmDone ? 'PM review complete' : (pmSent ? 'Awaiting PM response' : 'Not yet sent'), action: !pmSent ? 'sendToPM' : null },
    { group: 'Approval', label: 'Final Approval', done: b.status === 'approved', detail: '', blocked: true }
  ];

  // Build missing-deps detail for Final Approval
  const missingDeps = [];
  if (!data.audit.exists) missingDeps.push('Audited Financials');
  if (!pmDone) missingDeps.push('PM Review');
  if (linesWithProposed < lines.length * 0.5) missingDeps.push('Review All Sheets');
  const approvalItem = checks[checks.length - 1];
  approvalItem.detail = approvalItem.done ? 'Budget approved' : (missingDeps.length ? 'Requires: ' + missingDeps.join(', ') : 'Ready for approval');
  if (!approvalItem.done && missingDeps.length === 0) approvalItem.blocked = false;

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

  let lastGroup = '';
  checks.forEach((c) => {
    // Group header
    if (c.group !== lastGroup) {
      assemblyHtml += '<div style="font-size:10px; font-weight:700; color:var(--gray-400); text-transform:uppercase; letter-spacing:0.5px; margin:10px 0 4px;">' + c.group + '</div>';
      lastGroup = c.group;
    }
    const iconClass = c.done ? 'check-done' : 'check-pending';
    const iconChar = c.done ? '✓' : '';
    const actionBtn = c.action ? ' <button onclick="' + c.action + '()" style="font-size:11px; padding:2px 8px; background:var(--blue); color:white; border:none; border-radius:4px; cursor:pointer; margin-left:8px;">Go</button>' : '';
    const blockedBadge = (c.blocked && !c.done) ? ' <span style="font-size:10px; padding:1px 6px; border-radius:8px; background:var(--gray-100); color:var(--gray-400); margin-left:6px;">Blocked</span>' : '';
    const dimStyle = (c.blocked && !c.done) ? ' opacity:0.5;' : '';
    // Mini progress bar for items with progress
    let progressBar = '';
    if (c.progress !== undefined && !c.done) {
      const pColor = c.progress >= 50 ? 'var(--blue)' : c.progress > 0 ? '#d97706' : 'var(--gray-300)';
      progressBar = '<div style="height:4px; background:var(--gray-100); border-radius:2px; margin-top:4px; width:120px;"><div style="height:100%; width:' + c.progress + '%; background:' + pColor + '; border-radius:2px;"></div></div>';
    }
    assemblyHtml += '<div class="checklist-item" style="' + dimStyle + '">' +
      '<div class="check-icon ' + iconClass + '">' + iconChar + '</div>' +
      '<div style="flex:1;"><div class="checklist-label">' + c.label + actionBtn + blockedBadge + '</div>' +
      '<div class="checklist-detail">' + c.detail + '</div>' + progressBar + '</div></div>';
  });

  document.getElementById('assemblyContent').innerHTML = assemblyHtml;

  // ── PM Review Panel: Notes + Invoice Reclasses ──────────────────────
  (async function populatePmReview() {
    let totalItems = 0;
    const panel = document.getElementById('pmReviewPanel');

    // Section 1: PM Notes
    const linesWithNotes = lines.filter(l => l.notes && l.notes.trim().length > 0);
    const notesContainer = document.getElementById('pmNotesContainer');
    const notesEmpty = document.getElementById('pmNotesEmpty');
    const notesCount = document.getElementById('pmNotesCount');

    if (linesWithNotes.length > 0) {
      notesEmpty.style.display = 'none';
      notesCount.textContent = linesWithNotes.length;
      notesContainer.innerHTML = linesWithNotes.map(l =>
        '<div style="display:flex; align-items:flex-start; gap:12px; padding:10px 12px; border-radius:8px; margin-bottom:6px;" onmouseover="this.style.background=\'var(--gray-50)\'" onmouseout="this.style.background=\'\'">' +
          '<span onclick="scrollToGlRow(\'' + l.gl_code + '\')" style="font-family:monospace; font-size:12px; font-weight:600; color:var(--blue); background:var(--blue-light); padding:3px 8px; border-radius:4px; white-space:nowrap; cursor:pointer;" title="Click to scroll to row">' + l.gl_code + '</span>' +
          '<span style="font-size:12px; color:var(--gray-500); min-width:140px;">' + (l.description || '') + '</span>' +
          '<div style="flex:1; font-size:13px; color:var(--gray-700); background:#fffbeb; padding:6px 10px; border-radius:6px; border-left:3px solid #fbbf24;">' + (l.notes || '') + '</div>' +
        '</div>'
      ).join('');
      totalItems += linesWithNotes.length;
    } else {
      notesEmpty.style.display = '';
      notesContainer.innerHTML = '';
      notesCount.textContent = '0';
    }

    // Section 2: Invoice Reclasses (aggregated from expense distribution data)
    const reclassCount = document.getElementById('pmReclassCount');
    const reclassBody = document.getElementById('pmReclassBody');
    const reclassEmpty = document.getElementById('pmReclassEmpty');
    const reclassSummary = document.getElementById('pmReclassSummary');

    const expData = await faFetchExpenseData();
    if (expData && expData.gl_groups) {
      // Flatten all invoices across GL groups and find reclassed ones
      const allInvoices = [];
      expData.gl_groups.forEach(g => {
        if (g.invoices) g.invoices.forEach(inv => allInvoices.push(inv));
      });
      const reclassed = allInvoices.filter(inv => inv.reclass_to_gl);

      // Aggregate by from_gl → to_gl
      const reclassMap = {};
      reclassed.forEach(inv => {
        const key = inv.gl_code + '|' + inv.reclass_to_gl;
        if (!reclassMap[key]) {
          reclassMap[key] = { from_gl: inv.gl_code, to_gl: inv.reclass_to_gl, invoices: [], total: 0, notes: '' };
        }
        reclassMap[key].invoices.push(inv);
        reclassMap[key].total += inv.amount || 0;
        if (inv.reclass_notes && !reclassMap[key].notes) reclassMap[key].notes = inv.reclass_notes;
      });
      const groups = Object.values(reclassMap);

      if (groups.length > 0) {
        reclassEmpty.style.display = 'none';
        reclassCount.textContent = groups.length;
        const totalAmt = groups.reduce((s, g) => s + Math.abs(g.total), 0);
        reclassSummary.style.display = 'flex';
        reclassSummary.innerHTML =
          '<div><span style="color:var(--gray-500);">Invoices reclassed:</span> <span style="font-weight:700;">' + reclassed.length + '</span></div>' +
          '<div><span style="color:var(--gray-500);">Total amount moved:</span> <span style="font-weight:700;">' + fmt(totalAmt) + '</span></div>' +
          '<div><span style="color:var(--gray-500);">GL moves:</span> <span style="font-weight:700;">' + groups.length + '</span></div>';

        reclassBody.innerHTML = '';
        groups.forEach((g, gi) => {
          const fromDesc = (lines.find(l => l.gl_code === g.from_gl) || {}).description || '';
          const toDesc = (lines.find(l => l.gl_code === g.to_gl) || {}).description || '';
          const invIds = g.invoices.map(i => i.id).join(',');
          const gid = 'farg_' + gi;
          const tr = document.createElement('tr');
          tr.id = 'pmrc_' + g.from_gl + '_' + g.to_gl;
          tr.style.cssText = 'transition:background 0.15s; cursor:pointer;';
          tr.onmouseover = function() { this.style.background='var(--gray-50)'; };
          tr.onmouseout = function() { this.style.background=''; };
          tr.onclick = function(e) { if (e.target.tagName === 'BUTTON') return; toggleReclassInvDetail(gid); };
          tr.innerHTML =
            '<td style="padding:10px;"><span id="' + gid + '_arrow" style="display:inline-block; font-size:10px; color:var(--gray-400); transition:transform 0.2s; margin-right:6px;">▶</span><span style="font-family:monospace; font-size:12px; font-weight:700;">' + g.from_gl + '</span><div style="padding-left:20px; font-size:11px; color:var(--gray-400);">' + fromDesc + '</div></td>' +
            '<td style="padding:10px 4px; color:var(--orange); font-weight:700; font-size:16px;">→</td>' +
            '<td style="padding:10px;"><span style="font-family:monospace; font-size:12px; font-weight:700;">' + g.to_gl + '</span><div style="font-size:11px; color:var(--gray-400);">' + toDesc + '</div></td>' +
            '<td style="padding:10px;"><span style="font-size:11px; background:var(--orange-light); color:var(--orange); padding:2px 8px; border-radius:10px; font-weight:600;">' + g.invoices.length + ' invoice' + (g.invoices.length !== 1 ? 's' : '') + '</span></td>' +
            '<td style="padding:10px; text-align:right; font-weight:600; font-variant-numeric:tabular-nums;">' + fmt(g.total) + '</td>' +
            '<td style="padding:10px; font-size:12px; color:var(--gray-600); font-style:italic; max-width:200px;">' + (g.notes ? '"' + g.notes + '"' : '') + '</td>' +
            '<td style="padding:10px; text-align:right;" id="pmrc_action_' + g.from_gl + '_' + g.to_gl + '">' +
              '<button onclick="acceptPmReclass(\'' + g.from_gl + '\',\'' + g.to_gl + '\',' + g.total + ',\'' + invIds + '\')" style="padding:5px 12px; font-size:12px; font-weight:600; border-radius:6px; cursor:pointer; background:var(--green-light); color:var(--green); border:1px solid #86efac;">✓ Accept</button> ' +
              '<button onclick="undoPmReclass(\'' + g.from_gl + '\',\'' + g.to_gl + '\',\'' + invIds + '\')" style="padding:5px 12px; font-size:12px; font-weight:600; border-radius:6px; cursor:pointer; background:var(--gray-100); color:var(--gray-600); border:1px solid var(--gray-300); margin-left:6px;">Undo</button>' +
            '</td>';
          reclassBody.appendChild(tr);
          // Add expandable invoice detail rows (hidden by default)
          g.invoices.forEach(inv => {
            const itr = document.createElement('tr');
            itr.className = 'reclass-inv-detail';
            itr.dataset.group = gid;
            itr.style.cssText = 'display:none; background:#fafbfc;';
            const invDate = inv.invoice_date || inv.date || '';
            const cleanDate = invDate ? invDate.split('T')[0] : '';
            const invNum = inv.invoice_num || inv.invoice_number || inv.ref || '';
            const invVendor = inv.payee_name || inv.vendor_name || inv.vendor || '';
            const invDesc = inv.notes || inv.description || '';
            const toGlName = (lines.find(l => l.gl_code === inv.reclass_to_gl) || {}).description || inv.reclass_to_gl;
            itr.innerHTML =
              '<td colspan="7" style="padding:8px 10px 8px 44px; border-bottom:1px solid #f0f1f3;">' +
                '<div style="display:flex; align-items:center; gap:12px; font-size:12px; flex-wrap:wrap;">' +
                  (invNum ? '<span style="font-family:monospace; font-size:11px; color:var(--gray-400); background:#f3f4f6; padding:1px 6px; border-radius:3px;">' + invNum + '</span>' : '') +
                  '<span style="font-weight:600; color:var(--gray-700);">' + invVendor + '</span>' +
                  (invDesc ? '<span style="color:var(--gray-500);">— ' + invDesc + '</span>' : '') +
                  (cleanDate ? '<span style="font-size:11px; color:var(--gray-400);">' + cleanDate + '</span>' : '') +
                  '<span style="font-size:11px; color:var(--orange);">→ ' + toGlName + '</span>' +
                  '<span style="margin-left:auto; font-weight:600; font-variant-numeric:tabular-nums; text-align:right;">' + fmt(inv.amount || 0) + '</span>' +
                  '<button onclick="event.stopPropagation(); undoSingleReclass(' + inv.id + ',\'' + g.from_gl + '\',\'' + g.to_gl + '\',this)" style="margin-left:8px; padding:2px 8px; font-size:10px; font-weight:600; border-radius:4px; cursor:pointer; background:white; color:var(--gray-500); border:1px solid var(--gray-300);">Undo</button>' +
                '</div>' +
              '</td>';
            reclassBody.appendChild(itr);
          });
        });
        totalItems += groups.length;
      } else {
        reclassEmpty.style.display = '';
        reclassSummary.style.display = 'none';
        reclassBody.innerHTML = '';
        reclassCount.textContent = '0';
      }
    } else {
      reclassEmpty.style.display = '';
      reclassSummary.style.display = 'none';
      reclassBody.innerHTML = '';
      reclassCount.textContent = '0';
    }

    // Section 3: Budget Proposals (PM changes to budget figures)
    const proposalsCount = document.getElementById('pmProposalsCount');
    const proposalsBody = document.getElementById('pmProposalsBody');
    const proposalsEmpty = document.getElementById('pmProposalsEmpty');
    const proposalsSummary = document.getElementById('pmProposalsSummary');

    // Detect PM proposals: lines where PM changed the budget via increase_pct, override, or direct proposed_budget
    const proposals = lines.filter(l => {
      if (l.fa_proposed_status === 'accepted' || l.fa_proposed_status === 'rejected') return true;  // show resolved ones too
      const hasPct = (l.increase_pct || 0) !== 0;
      const hasOverride = l.estimate_override !== null && l.estimate_override !== undefined;
      const hasForecastOvr = l.forecast_override !== null && l.forecast_override !== undefined;
      const hasProposed = (l.proposed_budget || 0) !== 0 && Math.abs((l.proposed_budget || 0) - (l.current_budget || 0)) > 0.01;
      return hasPct || hasOverride || hasForecastOvr || hasProposed;
    });

    if (proposals.length > 0) {
      proposalsEmpty.style.display = 'none';
      proposalsCount.textContent = proposals.filter(l => !l.fa_proposed_status || l.fa_proposed_status === 'commented').length;
      const pending = proposals.filter(l => !l.fa_proposed_status || l.fa_proposed_status === 'commented').length;
      const accepted = proposals.filter(l => l.fa_proposed_status === 'accepted').length;
      const rejected = proposals.filter(l => l.fa_proposed_status === 'rejected').length;
      proposalsSummary.style.display = 'flex';
      proposalsSummary.innerHTML =
        '<div><span style="color:var(--gray-500);">Total proposals:</span> <span style="font-weight:700;">' + proposals.length + '</span></div>' +
        '<div><span style="color:var(--gray-500);">Pending:</span> <span style="font-weight:700; color:#b45309;">' + pending + '</span></div>' +
        '<div><span style="color:var(--gray-500);">Accepted:</span> <span style="font-weight:700; color:var(--green);">' + accepted + '</span></div>' +
        (rejected > 0 ? '<div><span style="color:var(--gray-500);">Rejected:</span> <span style="font-weight:700; color:var(--red);">' + rejected + '</span></div>' : '');

      proposalsBody.innerHTML = '';
      proposals.forEach(l => {
        const proposed = l.proposed_budget || 0;
        const current = l.current_budget || 0;
        const change = proposed - current;
        const pct = current !== 0 ? ((change / current) * 100).toFixed(1) : '—';
        let method = '';
        if ((l.increase_pct || 0) !== 0) method = (l.increase_pct > 0 ? '+' : '') + l.increase_pct.toFixed(1) + '% increase';
        else if (l.estimate_override !== null && l.estimate_override !== undefined) method = 'Manual override';
        else method = 'Direct edit';

        const status = l.fa_proposed_status || 'pending';
        let statusBadge = '';
        let actionHtml = '';
        if (status === 'accepted') {
          statusBadge = '<span style="background:#dcfce7; color:#166534; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✓ Accepted</span>';
          actionHtml = '<span style="color:var(--gray-400); font-size:11px;">Done</span>';
        } else if (status === 'rejected') {
          statusBadge = '<span style="background:#fef2f2; color:#991b1b; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✗ Rejected</span>';
          actionHtml = '<span style="color:var(--gray-400); font-size:11px;">Done</span>';
        } else if (status === 'commented') {
          statusBadge = '<span style="background:#fef3c7; color:#92400e; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">💬 Commented</span>';
          actionHtml = proposalActionButtons(l.gl_code);
        } else {
          statusBadge = '<span style="background:#fff7ed; color:#b45309; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">● Pending</span>';
          actionHtml = proposalActionButtons(l.gl_code);
        }

        const tr = document.createElement('tr');
        tr.id = 'prop_' + l.gl_code;
        tr.style.cssText = 'transition:background 0.15s;';
        tr.onmouseover = function() { this.style.background='var(--gray-50)'; };
        tr.onmouseout = function() { this.style.background=''; };
        const changeColor = change > 0 ? 'var(--red)' : change < 0 ? 'var(--green)' : 'var(--gray-500)';
        tr.innerHTML =
          '<td style="padding:10px;"><span onclick="scrollToGlRow(\'' + l.gl_code + '\')" style="font-family:monospace; font-size:12px; font-weight:700; color:var(--blue); cursor:pointer;">' + l.gl_code + '</span></td>' +
          '<td style="padding:10px; font-size:12px; color:var(--gray-600); max-width:180px;">' + (l.description || '') + '</td>' +
          '<td style="padding:10px; text-align:right; font-variant-numeric:tabular-nums; font-size:13px;">' + fmt(current) + '</td>' +
          '<td style="padding:10px; text-align:right; font-weight:700; font-variant-numeric:tabular-nums; font-size:13px;">' + fmt(proposed) + '</td>' +
          '<td style="padding:10px; text-align:right; font-variant-numeric:tabular-nums; font-size:13px; color:' + changeColor + ';">' + (change >= 0 ? '+' : '') + fmt(change) + ' (' + pct + '%)</td>' +
          '<td style="padding:10px; font-size:11px; color:var(--gray-500);">' + method + '</td>' +
          '<td style="padding:10px; text-align:center;">' + statusBadge + '</td>' +
          '<td style="padding:10px; text-align:right; white-space:nowrap;">' + actionHtml + '</td>';
        proposalsBody.appendChild(tr);
      });
      totalItems += pending;
    } else {
      proposalsEmpty.style.display = '';
      proposalsSummary.style.display = 'none';
      proposalsBody.innerHTML = '';
      proposalsCount.textContent = '0';
    }

    // Show/hide the panel
    if (totalItems > 0) {
      panel.style.display = '';
      document.getElementById('pmReviewBadgeText').textContent = totalItems + ' item' + (totalItems !== 1 ? 's' : '') + ' need review';
    }
  })();

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

  {
    // Summary tab is ALWAYS shown — even before detail lines exist
    // (BudgetSummaryRow data may be imported from approved Excel)
    const summaryTab = document.createElement('button');
    summaryTab.textContent = 'Summary';
    summaryTab.className = 'sheet-tab active';
    summaryTab.dataset.sheet = 'Summary';
    summaryTab.style.cssText = 'padding-left:20px; position:relative;';
    // green dot indicator
    const dot = document.createElement('span');
    dot.style.cssText = 'position:absolute;left:6px;top:50%;transform:translateY(-50%);width:6px;height:6px;background:#057a55;border-radius:50%;';
    summaryTab.prepend(dot);
    summaryTab.onclick = () => renderSheet('Summary', null, summaryTab);
    tabsDiv.appendChild(summaryTab);

    sheetOrder.forEach((sheetName) => {
      const tab = document.createElement('button');
      tab.textContent = sheetName;
      tab.className = 'sheet-tab';
      tab.dataset.sheet = sheetName;
      tab.onclick = () => renderSheet(sheetName, sheets[sheetName], tab);
      tabsDiv.appendChild(tab);
    });

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

    // Render Summary first
    renderSheet('Summary', null, summaryTab);
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

// ── Board Presentation Overlay (v2 — charts, exec summary, notes) ─────
function openBoardPresentation() {
  const data = window._data;
  if (!data || !data.budget) { showToast('Budget data not loaded yet', 'error'); return; }

  // Load Chart.js from CDN if not present
  function loadChartJs() {
    return new Promise(resolve => {
      if (typeof Chart !== 'undefined') return resolve();
      const s = document.createElement('script');
      s.src = 'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js';
      s.onload = resolve;
      s.onerror = () => { console.warn('Chart.js failed to load'); resolve(); };
      document.head.appendChild(s);
    });
  }

  // Remove existing overlay if any
  const existing = document.getElementById('boardPresOverlay');
  if (existing) existing.remove();

  loadChartJs().then(() => buildPresentation());

  function buildPresentation() {
    const b = data.budget;
    const sheets = allSheets;
    const sheetOrder = data.sheet_order || Object.keys(sheets);

    // Status
    const statusMap = {draft:'DRAFT', pm_pending:'PM REVIEW', pm_in_progress:'PM REVIEW', fa_review:'FA REVIEW', approved:'APPROVED'};
    const statusLabel = statusMap[b.status] || (b.status || 'DRAFT').toUpperCase();
    const statusColor = b.status === 'approved' ? '#16a34a' : b.status === 'fa_review' ? '#3b82f6' : '#d97706';

    // Helpers
    function sumF(lines, fn) { return lines.reduce((s, l) => s + (fn(l) || 0), 0); }
    function pFmt(n) { return '$' + Math.abs(Math.round(n)).toLocaleString(); }
    function pPct(n) { return (n >= 0 ? '+' : '') + n.toFixed(1) + '%'; }
    function chgCls(val, isExp) {
      if (Math.abs(val) < 0.05) return '';
      return (isExp ? val > 0 : val < 0) ? 'bp-chg-bad' : 'bp-chg-good';
    }
    function getProposed(l) { return l.proposed_budget || (computeForecast(l) * (1 + (l.increase_pct || 0))); }

    // Category defs for expandable detail
    const CATS = {
      'Repairs & Supplies': [{label:'Supplies', match: l => l.category === 'supplies'}, {label:'Repairs', match: l => l.category === 'repairs'}, {label:'Maintenance Contracts', match: l => l.category === 'maintenance'}],
      'Gen & Admin': [{label:'Professional Fees', match: l => l.row_num >= 8 && l.row_num <= 16}, {label:'Administrative & Other', match: l => l.row_num >= 20 && l.row_num <= 49}, {label:'Insurance', match: l => l.row_num >= 53 && l.row_num <= 64}, {label:'Taxes', match: l => l.row_num >= 68 && l.row_num <= 78}, {label:'Financial Expenses', match: l => l.row_num >= 82 && l.row_num <= 90}]
    };

    // Sheet totals
    const stotals = {};
    const expSheets = sheetOrder.filter(s => s !== 'Income');
    sheetOrder.forEach(s => {
      const ln = sheets[s] || [];
      stotals[s] = { prior: sumF(ln, l => l.prior_year), forecast: sumF(ln, l => computeForecast(l)), budget: sumF(ln, l => l.current_budget), proposed: sumF(ln, l => getProposed(l)) };
    });
    const incT = stotals['Income'] || {prior:0, forecast:0, budget:0, proposed:0};
    let expT = {prior:0, forecast:0, budget:0, proposed:0};
    expSheets.forEach(s => { const t = stotals[s] || {prior:0,forecast:0,budget:0,proposed:0}; expT.prior += t.prior; expT.forecast += t.forecast; expT.budget += t.budget; expT.proposed += t.proposed; });
    const noiBudget = incT.budget - expT.budget, noiProposed = incT.proposed - expT.proposed;
    const budgetIncPct = expT.budget ? ((expT.proposed - expT.budget) / Math.abs(expT.budget)) * 100 : 0;

    // Top movers
    const movers = expSheets.map(s => { const t = stotals[s]; const chg = t.proposed - t.budget; return { label: s, chg, pct: t.budget ? (chg / Math.abs(t.budget)) * 100 : 0 }; }).sort((a, b) => Math.abs(b.chg) - Math.abs(a.chg));
    const top3Up = movers.filter(m => m.chg > 0).slice(0, 3);

    // Exec summary
    const drivers = top3Up.slice(0, 2).map(m => m.label).join(' and ') || 'operational adjustments';
    const expDir = budgetIncPct > 0 ? 'increase' : 'decrease';
    const execSummary = 'The proposed ' + b.year + ' operating budget reflects a net expense ' + expDir + ' of ' + pPct(budgetIncPct) + ' (' + pFmt(Math.abs(expT.proposed - expT.budget)) + '), primarily driven by ' + drivers + '. ' +
      (incT.proposed > incT.budget ? 'Income is projected to grow ' + pPct(incT.budget ? ((incT.proposed - incT.budget) / Math.abs(incT.budget)) * 100 : 0) + ' to help offset the change. ' : '') +
      'Net Operating Income is projected at ' + pFmt(noiProposed) + ', a ' + (noiProposed >= noiBudget ? 'gain' : 'reduction') + ' of ' + pFmt(Math.abs(noiProposed - noiBudget)) + ' from the current budget.';

    // In-memory notes store
    const bpNotes = {};

    // Overlay
    const overlay = document.createElement('div');
    overlay.id = 'boardPresOverlay';
    const today = new Date().toLocaleDateString('en-US', {year:'numeric', month:'long', day:'numeric'});
    const displayTabs = ['summary'].concat(sheetOrder);
    const _savedScrollY = window.scrollY;

    // Escape handler
    const escH = (e) => { if (e.key === 'Escape') { document.removeEventListener('keydown', escH); overlay.remove(); document.body.style.overflow=''; document.documentElement.style.overflow=''; window.scrollTo(0, _savedScrollY || 0); } };
    document.addEventListener('keydown', escH);

    overlay.innerHTML = `<style>
#boardPresOverlay { position:fixed; inset:0; z-index:9999; overflow-y:auto; background:white; font-family:'Plus Jakarta Sans',-apple-system,sans-serif; }
#boardPresOverlay * { box-sizing:border-box; }
.bp2-hdr { background:linear-gradient(135deg,#1e293b,#0f172a); padding:28px 48px 22px; display:flex; justify-content:space-between; align-items:flex-start; position:relative; }
.bp2-hdr h1 { font-size:26px; font-weight:300; color:#f8fafc; margin:0; }
.bp2-hdr .sub { font-size:12px; color:#94a3b8; margin-top:5px; text-transform:uppercase; letter-spacing:1.5px; font-weight:500; }
.bp2-badge { display:inline-block; padding:3px 10px; border-radius:10px; font-size:10px; font-weight:700; letter-spacing:.5px; margin-left:10px; vertical-align:middle; }
.bp2-close { position:absolute; top:14px; right:18px; background:rgba(255,255,255,.1); border:none; color:#94a3b8; width:34px; height:34px; border-radius:50%; font-size:18px; cursor:pointer; }
.bp2-close:hover { background:rgba(255,255,255,.2); color:#fff; }
.bp2-right { text-align:right; font-size:13px; color:#94a3b8; }
.bp2-tabs { background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:0 48px; display:flex; gap:0; overflow-x:auto; }
.bp2-tab { padding:13px 20px; font-size:13px; font-weight:500; color:#64748b; border-bottom:2px solid transparent; cursor:pointer; white-space:nowrap; background:none; border-top:none; border-left:none; border-right:none; }
.bp2-tab:hover { color:#1e293b; }
.bp2-tab.active { color:#1e293b; font-weight:600; border-bottom-color:#1e293b; }
.bp2-body { max-width:1400px; padding:0 48px 32px; }
.bp2-exec { background:#eff6ff; border:1px solid #bfdbfe; border-left:4px solid #3b82f6; border-radius:6px; padding:16px 20px; margin:24px 0; font-size:14px; line-height:1.7; color:#1e40af; }
.bp2-exec b { font-weight:700; }
.bp2-cards { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin:20px 0; }
.bp2-card { border:1px solid #e2e8f0; border-radius:10px; padding:18px 22px; position:relative; overflow:hidden; background:#fff; }
.bp2-card-lbl { font-size:10px; text-transform:uppercase; letter-spacing:1px; color:#64748b; font-weight:600; margin-bottom:6px; }
.bp2-card-val { font-size:24px; font-weight:700; color:#0f172a; font-variant-numeric:tabular-nums; }
.bp2-card-sub { font-size:12px; font-weight:600; margin-top:5px; }
.bp2-card-bar { position:absolute; bottom:0; left:0; right:0; height:3px; }
.bp2-card-hl { background:linear-gradient(135deg,#fefce8,#fef9c3); border-color:#fbbf24; }
.bp2-good { color:#16a34a; } .bp2-bad { color:#dc2626; } .bp2-muted { color:#64748b; }
.bp2-top3 { background:#fef3c7; border:1px solid #fcd34d; border-left:4px solid #f59e0b; border-radius:6px; padding:16px 20px; margin:20px 0; }
.bp2-top3 h4 { font-size:13px; font-weight:700; color:#92400e; margin:0 0 12px; }
.bp2-top3-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; }
.bp2-top3-item { background:#fff; border:1px solid #fde68a; border-radius:6px; padding:14px; }
.bp2-top3-item .cat { font-size:11px; color:#64748b; text-transform:uppercase; font-weight:600; }
.bp2-top3-item .amt { font-size:18px; font-weight:700; color:#dc2626; margin:4px 0 2px; font-variant-numeric:tabular-nums; }
.bp2-top3-item .pc { font-size:11px; color:#64748b; }
.bp2-charts { display:grid; grid-template-columns:1fr 1fr; gap:20px; margin:20px 0; }
.bp2-chart-box { border:1px solid #e2e8f0; border-radius:10px; padding:20px; position:relative; }
.bp2-chart-box h4 { font-size:13px; font-weight:700; color:#334155; margin:0 0 12px; }
.bp2-chart-box canvas { width:100% !important; height:100% !important; max-height:260px; }
.bp2-tbl-title { font-size:14px; font-weight:700; color:#0f172a; margin:24px 0 10px; }
.bp2-chip { font-size:10px; font-weight:600; background:#dbeafe; color:#1d4ed8; padding:2px 8px; border-radius:10px; text-transform:uppercase; letter-spacing:.5px; margin-left:8px; }
table.bp2-tbl { width:100%; border-collapse:collapse; }
table.bp2-tbl thead th { text-align:left; padding:9px 12px; font-size:10px; text-transform:uppercase; letter-spacing:.5px; color:#64748b; font-weight:600; border-bottom:2px solid #e2e8f0; background:#f8fafc; }
table.bp2-tbl thead th.num { text-align:right; }
table.bp2-tbl tbody td { padding:9px 12px; font-size:13px; color:#334155; border-bottom:1px solid #f1f5f9; }
table.bp2-tbl tbody td.num { text-align:right; font-variant-numeric:tabular-nums; font-weight:500; }
table.bp2-tbl tbody tr:hover { background:#f8fafc; }
table.bp2-tbl tbody tr.clickable { cursor:pointer; }
table.bp2-tbl tbody tr.clickable:hover { background:#eff6ff; }
.bp2-inc td { color:#166534; }
.bp2-sub td { font-weight:700; color:#0f172a; border-top:2px solid #e2e8f0; border-bottom:2px solid #e2e8f0; background:#f8fafc; }
.bp2-noi td { font-weight:800; color:#0f172a; font-size:14px; border-top:3px double #1e293b; border-bottom:3px double #1e293b; background:#fefce8; }
.bp2-chg-bad { color:#dc2626 !important; font-weight:600; }
.bp2-chg-good { color:#16a34a !important; font-weight:600; }
.bp2-exp td:first-child::before { content:'▶'; font-size:10px; margin-right:8px; color:#94a3b8; display:inline-block; transition:transform .15s; }
.bp2-exp.open td:first-child::before { transform:rotate(90deg); }
.bp2-child td { padding-left:44px !important; font-size:12px; color:#64748b; background:#fafafa; }
.bp2-child td.num { color:#64748b; font-weight:400; }
.bp2-notes { margin:20px 0; padding:16px 20px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; }
.bp2-notes label { font-size:11px; font-weight:700; color:#64748b; text-transform:uppercase; letter-spacing:.5px; display:block; margin-bottom:6px; }
.bp2-notes textarea { width:100%; padding:10px; border:1px solid #e2e8f0; border-radius:6px; font:13px/1.5 inherit; resize:vertical; min-height:60px; }
.bp2-ftr { border-top:1px solid #e2e8f0; padding:14px 48px; font-size:11px; color:#94a3b8; display:flex; justify-content:space-between; }
.bp2-detail-cards { display:flex; gap:14px; margin:20px 0; }
.bp2-detail-cards .bp2-card { flex:1; }
@media print {
  .bp2-close,.bp2-tabs,.bp2-notes { display:none !important; }
  .bp2-hdr { background:#fff !important; border-bottom:2px solid #1e293b; print-color-adjust:exact; -webkit-print-color-adjust:exact; }
  .bp2-hdr h1 { color:#0f172a !important; } .bp2-hdr .sub { color:#334155 !important; }
  .bp2-card-hl,.bp2-noi td { background:#fefce8 !important; print-color-adjust:exact; -webkit-print-color-adjust:exact; }
  .bp2-ftr { position:fixed; bottom:0; left:0; right:0; }
  @page { margin:.5in; size:landscape; }
}
</style>
<div class="bp2-hdr">
  <div><h1>${b.building_name} <span class="bp2-badge" style="background:${statusColor}20;color:${statusColor};border:1px solid ${statusColor}">${statusLabel}</span></h1>
  <div class="sub">${b.year} Operating Budget</div></div>
  <div class="bp2-right"><div style="font-weight:600">Century Management</div><div style="font-size:12px;color:#64748b;margin-top:3px">${today}</div></div>
  <button class="bp2-close" title="Close (Esc)">✕</button>
</div>
<div class="bp2-tabs" id="bp2Tabs"></div>
<div class="bp2-body" id="bp2Body"></div>
<div class="bp2-ftr"><span>Prepared by Century Management · Confidential</span><span>Generated ${today}</span></div>`;

    // Lock body scroll and reset overlay position
    document.body.style.overflow = 'hidden';
    document.documentElement.style.overflow = 'hidden';
    window.scrollTo(0, 0);
    document.body.appendChild(overlay);
    overlay.scrollTop = 0;
    overlay.querySelector('.bp2-close').onclick = () => {
      document.removeEventListener('keydown', escH);
      overlay.remove();
      document.body.style.overflow = '';
      document.documentElement.style.overflow = '';
      window.scrollTo(0, _savedScrollY);
    };

    // Build tabs
    const tabsEl = overlay.querySelector('#bp2Tabs');
    displayTabs.forEach(tn => {
      const btn = document.createElement('button');
      btn.className = 'bp2-tab' + (tn === 'summary' ? ' active' : '');
      btn.dataset.tab = tn;
      btn.textContent = tn === 'summary' ? 'Summary' : tn;
      btn.onclick = () => renderTab(tn);
      tabsEl.appendChild(btn);
    });

    function renderTab(tn) {
      overlay.querySelectorAll('.bp2-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tn));
      if (tn === 'summary') renderSummary(); else renderDetail(tn);
      overlay.scrollTop = 0;
    }

    // ── Summary Tab ──
    function renderSummary() {
      const body = overlay.querySelector('#bp2Body');
      const incPct = incT.budget ? ((incT.proposed - incT.budget) / Math.abs(incT.budget)) * 100 : 0;
      const expPct = expT.budget ? ((expT.proposed - expT.budget) / Math.abs(expT.budget)) * 100 : 0;
      const noiDelta = noiProposed - noiBudget;
      const noiPctV = noiBudget ? (noiDelta / Math.abs(noiBudget)) * 100 : 0;

      let h = '';

      // Executive Summary
      h += '<div class="bp2-exec">📋 <b>Executive Summary</b> — ' + execSummary + '</div>';

      // Hero Cards
      h += '<div class="bp2-cards">';
      h += '<div class="bp2-card"><div class="bp2-card-lbl">Total Income</div><div class="bp2-card-val">' + pFmt(incT.proposed) + '</div><div class="bp2-card-sub ' + (incPct >= 0 ? 'bp2-good' : 'bp2-bad') + '">' + pPct(incPct) + ' vs current</div><div class="bp2-card-bar" style="background:#16a34a"></div></div>';
      h += '<div class="bp2-card"><div class="bp2-card-lbl">Total Expenses</div><div class="bp2-card-val">' + pFmt(expT.proposed) + '</div><div class="bp2-card-sub ' + (expPct > 0 ? 'bp2-bad' : 'bp2-good') + '">' + pPct(expPct) + ' vs current</div><div class="bp2-card-bar" style="background:#dc2626"></div></div>';
      h += '<div class="bp2-card"><div class="bp2-card-lbl">Net Operating Income</div><div class="bp2-card-val">' + pFmt(noiProposed) + '</div><div class="bp2-card-sub ' + (noiDelta >= 0 ? 'bp2-good' : 'bp2-bad') + '">' + (noiDelta >= 0 ? '+' : '-') + pFmt(Math.abs(noiDelta)) + ' vs current</div><div class="bp2-card-bar" style="background:#3b82f6"></div></div>';
      h += '<div class="bp2-card bp2-card-hl"><div class="bp2-card-lbl">Budget Increase</div><div class="bp2-card-val" style="color:#b45309">' + pPct(budgetIncPct) + '</div><div class="bp2-card-sub bp2-muted">' + pFmt(expT.budget) + ' → ' + pFmt(expT.proposed) + '</div><div class="bp2-card-bar" style="background:#f59e0b"></div></div>';
      h += '</div>';

      // Top 3 Increases
      if (top3Up.length > 0) {
        h += '<div class="bp2-top3"><h4>⚡ Top ' + top3Up.length + ' Budget Increases</h4><div class="bp2-top3-grid">';
        top3Up.forEach((m, i) => {
          h += '<div class="bp2-top3-item"><div class="cat">#' + (i + 1) + ' ' + m.label + '</div><div class="amt">+' + pFmt(m.chg) + '</div><div class="pc">' + pPct(m.pct) + '</div></div>';
        });
        h += '</div></div>';
      }

      // Charts
      h += '<div class="bp2-charts">';
      h += '<div class="bp2-chart-box"><h4>Expense Breakdown (Proposed)</h4><div style="position:relative;height:260px;max-height:260px;overflow:hidden;"><canvas id="bp2Donut"></canvas></div></div>';
      h += '<div class="bp2-chart-box"><h4>Current Budget vs Proposed</h4><div style="position:relative;height:260px;max-height:260px;overflow:hidden;"><canvas id="bp2Bar"></canvas></div></div>';
      h += '</div>';

      // Summary Table
      h += '<div class="bp2-tbl-title">Operating Budget Summary <span class="bp2-chip">Current Budget → Proposed</span></div>';
      h += '<table class="bp2-tbl"><thead><tr><th style="width:26%">Category</th><th class="num">Prior Year</th><th class="num">Forecast</th><th class="num">Current Budget</th><th class="num">Proposed</th><th class="num">$ Change</th><th class="num">% Change</th></tr></thead><tbody>';

      // Income row (clickable)
      if (stotals['Income']) {
        const t = stotals['Income'], c = t.proposed - t.budget, p = t.budget ? (c / Math.abs(t.budget)) * 100 : 0;
        h += '<tr class="bp2-inc clickable" data-nav="Income"><td style="font-weight:600">Income</td><td class="num">' + pFmt(t.prior) + '</td><td class="num">' + pFmt(t.forecast) + '</td><td class="num">' + pFmt(t.budget) + '</td><td class="num">' + pFmt(t.proposed) + '</td><td class="num ' + chgCls(c, false) + '">' + (c >= 0 ? '+' : '-') + pFmt(Math.abs(c)) + '</td><td class="num ' + chgCls(p, false) + '">' + pPct(p) + '</td></tr>';
      }
      h += '<tr><td colspan="7" style="height:5px;border:none"></td></tr>';

      expSheets.forEach(s => {
        const t = stotals[s]; if (!t) return;
        const c = t.proposed - t.budget, p = t.budget ? (c / Math.abs(t.budget)) * 100 : 0;
        h += '<tr class="clickable" data-nav="' + s + '"><td style="font-weight:600">' + s + '</td><td class="num">' + pFmt(t.prior) + '</td><td class="num">' + pFmt(t.forecast) + '</td><td class="num">' + pFmt(t.budget) + '</td><td class="num">' + pFmt(t.proposed) + '</td><td class="num ' + chgCls(c, true) + '">' + (c >= 0 ? '+' : '-') + pFmt(Math.abs(c)) + '</td><td class="num ' + chgCls(p, true) + '">' + pPct(p) + '</td></tr>';
      });

      // Total Expenses
      const ec = expT.proposed - expT.budget, ep = expT.budget ? (ec / Math.abs(expT.budget)) * 100 : 0;
      h += '<tr class="bp2-sub"><td>TOTAL EXPENSES</td><td class="num">' + pFmt(expT.prior) + '</td><td class="num">' + pFmt(expT.forecast) + '</td><td class="num">' + pFmt(expT.budget) + '</td><td class="num">' + pFmt(expT.proposed) + '</td><td class="num ' + chgCls(ec, true) + '">' + (ec >= 0 ? '+' : '-') + pFmt(Math.abs(ec)) + '</td><td class="num ' + chgCls(ep, true) + '">' + pPct(ep) + '</td></tr>';
      h += '<tr><td colspan="7" style="height:3px;border:none"></td></tr>';

      // NOI
      h += '<tr class="bp2-noi"><td>NET OPERATING INCOME</td><td class="num">' + pFmt(incT.prior - expT.prior) + '</td><td class="num">' + pFmt(incT.forecast - expT.forecast) + '</td><td class="num">' + pFmt(noiBudget) + '</td><td class="num">' + pFmt(noiProposed) + '</td><td class="num ' + chgCls(noiDelta, false) + '">' + (noiDelta >= 0 ? '+' : '-') + pFmt(Math.abs(noiDelta)) + '</td><td class="num ' + chgCls(noiPctV, false) + '">' + pPct(noiPctV) + '</td></tr>';
      h += '</tbody></table>';

      // Notes
      h += '<div class="bp2-notes"><label>📝 Presentation Notes <span style="font-weight:400;text-transform:none;letter-spacing:0">(FA talking points — not saved)</span></label><textarea placeholder="Add notes for the board meeting...">' + (bpNotes['_general'] || '') + '</textarea></div>';

      body.innerHTML = h;

      // Wire clickable rows
      body.querySelectorAll('tr.clickable').forEach(r => { r.onclick = () => renderTab(r.dataset.nav); });

      // Save notes on change
      const ta = body.querySelector('.bp2-notes textarea');
      if (ta) ta.oninput = () => { bpNotes['_general'] = ta.value; };

      // Render charts
      if (typeof Chart !== 'undefined') {
        setTimeout(() => {
          // Donut
          const donutData = expSheets.filter(s => (stotals[s]||{}).proposed > 0);
          const donutColors = ['#3b82f6','#ef4444','#f59e0b','#10b981','#8b5cf6','#ec4899','#06b6d4','#64748b'];
          const dc = body.querySelector('#bp2Donut');
          if (dc) new Chart(dc, { type:'doughnut', data:{ labels:donutData.map(s=>s), datasets:[{ data:donutData.map(s=>Math.round(stotals[s].proposed)), backgroundColor:donutColors.slice(0,donutData.length), borderWidth:0, hoverOffset:6 }] }, options:{ responsive:true, maintainAspectRatio:false, animation:{duration:0}, cutout:'60%', plugins:{ legend:{ position:'right', labels:{ boxWidth:12, font:{size:11} } }, tooltip:{ callbacks:{ label:ctx=>{ const tot=ctx.dataset.data.reduce((a,b)=>a+b,0); return ctx.label+': '+pFmt(ctx.parsed)+' ('+(ctx.parsed/tot*100).toFixed(1)+'%)'; } } } } } });

          // Bar
          const barData = expSheets.filter(s => (stotals[s]||{}).budget > 0 || (stotals[s]||{}).proposed > 0);
          const bc = body.querySelector('#bp2Bar');
          if (bc) new Chart(bc, { type:'bar', data:{ labels:barData.map(s=>s), datasets:[ { label:'Current Budget', data:barData.map(s=>Math.round(stotals[s].budget)), backgroundColor:'#cbd5e1', borderRadius:3 }, { label:'Proposed', data:barData.map(s=>Math.round(stotals[s].proposed)), backgroundColor:barData.map(s=>stotals[s].proposed>stotals[s].budget?'#fbbf24':'#4ade80'), borderRadius:3 } ] }, options:{ responsive:true, maintainAspectRatio:false, animation:{duration:0}, plugins:{ legend:{ position:'top', align:'end', labels:{ boxWidth:12, font:{size:11} } }, tooltip:{ callbacks:{ label:ctx=>ctx.dataset.label+': '+pFmt(ctx.parsed.y) } } }, scales:{ y:{ beginAtZero:true, ticks:{ callback:v=>'$'+(v/1000).toFixed(0)+'K', font:{size:10} }, grid:{color:'#f1f5f9'} }, x:{ ticks:{font:{size:10},maxRotation:25}, grid:{display:false} } } } });
          // Kill scroll repeatedly to beat any async Chart.js resizes
          overlay.scrollTop = 0;
          setTimeout(() => { overlay.scrollTop = 0; }, 100);
          setTimeout(() => { overlay.scrollTop = 0; }, 300);
        }, 50);
      }
    }

    // ── Detail Tab ──
    function bpIsZero(l) { return !l.prior_year && !l.ytd_actual && !l.accrual_adj && !l.unpaid_bills && !l.current_budget && !l.increase_pct; }
    function renderDetail(sheetName) {
      const body = overlay.querySelector('#bp2Body');
      const lines = sheets[sheetName] || [];
      const t = stotals[sheetName] || {prior:0,forecast:0,budget:0,proposed:0};
      const chg = t.proposed - t.budget, pc = t.budget ? (chg / Math.abs(t.budget)) * 100 : 0;
      const isExp = sheetName !== 'Income';

      let h = '<div class="bp2-detail-cards">';
      h += '<div class="bp2-card"><div class="bp2-card-lbl">Current Budget</div><div class="bp2-card-val" style="font-size:22px">' + pFmt(t.budget) + '</div></div>';
      h += '<div class="bp2-card"><div class="bp2-card-lbl">Proposed Budget</div><div class="bp2-card-val" style="font-size:22px">' + pFmt(t.proposed) + '</div></div>';
      h += '<div class="bp2-card bp2-card-hl"><div class="bp2-card-lbl">Change</div><div class="bp2-card-val" style="font-size:22px;color:#b45309">' + (chg >= 0 ? '+' : '-') + pFmt(Math.abs(chg)) + ' (' + pPct(pc) + ')</div></div>';
      h += '</div>';

      h += '<table class="bp2-tbl"><thead><tr><th style="width:32%">Description</th><th class="num">Prior Year</th><th class="num">Forecast</th><th class="num">Current Budget</th><th class="num">Proposed</th><th class="num">$ Change</th><th class="num">% Change</th></tr></thead><tbody>';

      const cats = CATS[sheetName];
      if (cats) {
        cats.forEach((cat, ci) => {
          const cl = lines.filter(cat.match); if (!cl.length) return;
          const ct = { prior:sumF(cl,l=>l.prior_year), forecast:sumF(cl,l=>computeForecast(l)), budget:sumF(cl,l=>l.current_budget), proposed:sumF(cl,l=>getProposed(l)) };
          const cc = ct.proposed - ct.budget, cp = ct.budget ? (cc / Math.abs(ct.budget)) * 100 : 0;
          const cid = 'bp2c_' + sheetName.replace(/\W/g,'') + ci;
          h += '<tr class="bp2-exp" onclick="document.querySelectorAll(\'.' + cid + '\').forEach(r=>{r.style.display=r.style.display===\'none\'?\'\':\'none\'});this.classList.toggle(\'open\')" style="cursor:pointer"><td style="font-weight:600;padding-left:22px">' + cat.label + '</td><td class="num" style="font-weight:600">' + pFmt(ct.prior) + '</td><td class="num" style="font-weight:600">' + pFmt(ct.forecast) + '</td><td class="num" style="font-weight:600">' + pFmt(ct.budget) + '</td><td class="num" style="font-weight:600">' + pFmt(ct.proposed) + '</td><td class="num ' + chgCls(cc,isExp) + '" style="font-weight:600">' + (cc >= 0 ? '+' : '-') + pFmt(Math.abs(cc)) + '</td><td class="num ' + chgCls(cp,isExp) + '" style="font-weight:600">' + pPct(cp) + '</td></tr>';
          cl.forEach(l => {
            if (bpIsZero(l)) return;
            const lf = computeForecast(l), lp = getProposed(l), lc = lp - (l.current_budget||0), lpc = (l.current_budget||0) ? (lc / Math.abs(l.current_budget)) * 100 : 0;
            h += '<tr class="bp2-child ' + cid + '" style="display:none"><td>' + (l.gl_code||'') + ' · ' + (l.description||'') + '</td><td class="num">' + pFmt(l.prior_year||0) + '</td><td class="num">' + pFmt(lf) + '</td><td class="num">' + pFmt(l.current_budget||0) + '</td><td class="num">' + pFmt(lp) + '</td><td class="num">' + (lc >= 0 ? '+' : '-') + pFmt(Math.abs(lc)) + '</td><td class="num">' + pPct(lpc) + '</td></tr>';
          });
        });
        // Uncategorized
        const matched = new Set(); cats.forEach(c => lines.filter(c.match).forEach(l => matched.add(l.id)));
        lines.filter(l => !matched.has(l.id) && !bpIsZero(l)).forEach(l => {
          const lf = computeForecast(l), lp = getProposed(l), lc = lp - (l.current_budget||0), lpc = (l.current_budget||0) ? (lc / Math.abs(l.current_budget)) * 100 : 0;
          h += '<tr><td style="padding-left:22px">' + (l.gl_code||'') + ' · ' + (l.description||'') + '</td><td class="num">' + pFmt(l.prior_year||0) + '</td><td class="num">' + pFmt(lf) + '</td><td class="num">' + pFmt(l.current_budget||0) + '</td><td class="num">' + pFmt(lp) + '</td><td class="num ' + chgCls(lc,isExp) + '">' + (lc >= 0 ? '+' : '-') + pFmt(Math.abs(lc)) + '</td><td class="num ' + chgCls(lpc,isExp) + '">' + pPct(lpc) + '</td></tr>';
        });
      } else {
        lines.forEach(l => {
          if (bpIsZero(l)) return;
          const lf = computeForecast(l), lp = getProposed(l), lc = lp - (l.current_budget||0), lpc = (l.current_budget||0) ? (lc / Math.abs(l.current_budget)) * 100 : 0;
          h += '<tr><td style="padding-left:22px">' + (l.gl_code||'') + ' · ' + (l.description||'') + '</td><td class="num">' + pFmt(l.prior_year||0) + '</td><td class="num">' + pFmt(lf) + '</td><td class="num">' + pFmt(l.current_budget||0) + '</td><td class="num">' + pFmt(lp) + '</td><td class="num ' + chgCls(lc,isExp) + '">' + (lc >= 0 ? '+' : '-') + pFmt(Math.abs(lc)) + '</td><td class="num ' + chgCls(lpc,isExp) + '">' + pPct(lpc) + '</td></tr>';
        });
      }

      // Total
      h += '<tr class="bp2-sub"><td>TOTAL ' + sheetName.toUpperCase() + '</td><td class="num">' + pFmt(t.prior) + '</td><td class="num">' + pFmt(t.forecast) + '</td><td class="num">' + pFmt(t.budget) + '</td><td class="num">' + pFmt(t.proposed) + '</td><td class="num ' + chgCls(chg,isExp) + '">' + (chg >= 0 ? '+' : '-') + pFmt(Math.abs(chg)) + '</td><td class="num ' + chgCls(pc,isExp) + '">' + pPct(pc) + '</td></tr>';
      h += '</tbody></table>';

      // Notes
      h += '<div class="bp2-notes"><label>📝 Notes — ' + sheetName + '</label><textarea placeholder="Add talking points for ' + sheetName + '...">' + (bpNotes[sheetName] || '') + '</textarea></div>';

      body.innerHTML = h;
      const ta = body.querySelector('.bp2-notes textarea');
      if (ta) ta.oninput = () => { bpNotes[sheetName] = ta.value; };
    }

    renderSummary();
  }
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
    item('Effective Date', '<input type="text" value="' + (ir.effective_date || 'Mar '+BY) + '" style="' + inputStyle + '" onchange="assumAutoSave(\'insurance_renewal\',\'effective_date\', this.value)">') +
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
  const isReadOnly = field === 'variance' || field === 'pct_change' || el.dataset.readonly === 'true';
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
  const colLabels = {prior:'Prior Year', ytd:'YTD Actual', accrual:'Accrual Adj', unpaid:'Unpaid Bills', estimate:'Estimate', forecast:'12 Mo Forecast', budget:'Curr Budget', proposed:'Proposed', variance:'$ Variance', pctchange:'% Change'};
  label.textContent = rowLabel + ' / ' + (colLabels[col] || col);
  label.style.display = 'inline';
  bar.style.display = 'block';
  // Gather GL codes for this row
  const colPrefix = {prior:'pr_', ytd:'ytd_', accrual:'acc_', unpaid:'unp_', estimate:'est_', forecast:'fcst_', budget:'bud_', proposed:'prop_'};
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
        if (badge) { badge.textContent = '✎'; badge.style.background = '#f97316'; badge.style.color = '#fff'; badge.style.borderColor = '#ea580c'; }
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
      if (badge) { badge.textContent = '✎'; badge.style.background = '#f97316'; badge.style.color = '#fff'; badge.style.borderColor = '#ea580c'; }
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

  // Payroll-specific hook: if cell is in prGLContent, sync _payrollGLLines + re-render
  if (el && typeof el.closest === 'function' && el.closest('#prGLContent') && typeof payrollCellEdited === 'function') {
    payrollCellEdited(el, gl, field);
  }

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
    // Formula: (YTD+Accrual+Unpaid) / YTD_MONTHS * REMAINING_MONTHS
    if (YTD_MONTHS > 0) {
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
  // Build updated formula strings: =(YTD+Accrual+Unpaid) / YTD_MONTHS * REMAINING_MONTHS
  let estFormula, estExpr;
  if (YTD_MONTHS > 0) {
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
    const t = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
    glCodes.forEach(gl => {
      const row = document.querySelector('tr[data-gl="' + gl + '"]');
      if (row && row.style.display === 'none') return;
      t.prior += raw('pr_' + gl);
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
    const v = t.budget - t.forecast;
    const p = t.forecast ? ((t.budget - t.forecast) / t.forecast) : 0;
    const cells = rowEl.querySelectorAll('td');
    // With colspan="3" first cell: cells[0]=label, cells[1]=prior, cells[2]=ytd,
    // cells[3]=accrual, cells[4]=unpaid,
    // cells[5]=estimate, cells[6]=forecast, cells[7]=budget, cells[8]=inc%(empty),
    // cells[9]=proposed, cells[10]=variance, cells[11]=pctChange
    function setC(cell, val) {
      const sp = cell.querySelector('.sub-val');
      if (sp) { sp.textContent = fmt(val); cell.dataset.raw = Math.round(val).toString(); }
      else { cell.textContent = fmt(val); }
    }
    if (cells.length >= 12) {
      setC(cells[1], t.prior);
      setC(cells[2], t.ytd);
      setC(cells[3], t.accrual);
      setC(cells[4], t.unpaid);
      setC(cells[5], t.estimate);
      setC(cells[6], t.forecast);
      setC(cells[7], t.budget);
      setC(cells[9], t.proposed);
      const vs = cells[10].querySelector('.sub-val');
      if (vs) { vs.textContent = fmt(v); cells[10].dataset.raw = Math.round(v).toString(); }
      else { cells[10].textContent = fmt(v); }
      cells[10].style.color = v >= 0 ? 'var(--red)' : 'var(--green)';
      const ps = cells[11].querySelector('.sub-val');
      if (ps) { ps.textContent = (p * 100).toFixed(1) + '%'; cells[11].dataset.raw = p.toString(); }
      else { cells[11].textContent = (p * 100).toFixed(1) + '%'; }
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

let _faSavePending = {};
let _faSaveTimer = null;
function faAutoSave(gl, field, value) {
  if (!_faSavePending[gl]) _faSavePending[gl] = {};
  _faSavePending[gl][field] = value;
  clearTimeout(_faSaveTimer);
  _faSaveTimer = setTimeout(async () => {
    const lines = Object.entries(_faSavePending).map(function(entry) {
      var obj = {gl_code: entry[0]};
      var fields = entry[1];
      for (var k in fields) { if (fields.hasOwnProperty(k)) obj[k] = fields[k]; }
      return obj;
    });
    _faSavePending = {};
    const indicator = document.getElementById('faSaveIndicator');
    indicator.textContent = 'Saving...';
    try {
      const resp = await fetch('/api/fa-lines/' + entityCode, {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({lines: lines})
      });
      if (!resp.ok) throw new Error('Save failed: ' + resp.status);
      indicator.textContent = 'Saved';
    } catch(e) {
      indicator.textContent = 'Save failed!';
      indicator.style.color = '#dc2626';
      console.error('FA save error:', e);
      setTimeout(function() { indicator.style.color = ''; }, 3000);
    }
    setTimeout(function() { indicator.textContent = ''; }, 2000);
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

// ── PM Review Panel Functions ──────────────────────────────────────

function switchPmTab(button, tabId) {
  document.getElementById('pmNotesContent').style.display = 'none';
  document.getElementById('pmReclassContent').style.display = 'none';
  document.getElementById('pmProposalsContent').style.display = 'none';
  document.querySelectorAll('#pmReviewTabs .pm-tab').forEach(t => {
    t.style.color = 'var(--gray-500)';
    t.style.borderBottom = '2px solid transparent';
    t.style.background = 'transparent';
  });
  document.getElementById(tabId).style.display = 'block';
  button.style.color = 'var(--blue)';
  button.style.borderBottom = '2px solid var(--blue)';
  button.style.background = 'white';
}

function toggleReclassInvDetail(gid) {
  const rows = document.querySelectorAll('tr[data-group="' + gid + '"]');
  const arrow = document.getElementById(gid + '_arrow');
  if (!rows.length) return;
  const showing = rows[0].style.display !== 'none';
  rows.forEach(r => { r.style.display = showing ? 'none' : ''; });
  if (arrow) arrow.style.transform = showing ? '' : 'rotate(90deg)';
}

function scrollToGlRow(glCode) {
  const row = document.querySelector('tr[data-gl="' + glCode + '"]');
  if (row) {
    row.scrollIntoView({ behavior: 'smooth', block: 'center' });
    row.style.transition = 'background 0.3s';
    row.style.background = '#fef9c3';
    setTimeout(() => { row.style.background = ''; }, 3000);
  }
}

async function acceptPmReclass(fromGl, toGl, amount, invIdStr) {
  if (!confirm('Accept reclass of ' + fmt(amount) + ' from ' + fromGl + ' to ' + toGl + '?\\n\\nThis will move ' + fmt(amount) + ' of YTD Actual from ' + fromGl + ' to ' + toGl + ' and recalculate both lines.')) return;

  try {
    const res = await fetch('/api/reclass/accept', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ entity_code: entityCode, from_gl: fromGl, to_gl: toGl, amount: amount })
    });
    if (!res.ok) { const err = await res.json(); throw new Error(err.error || 'Failed'); }

    // Update action cell
    const actionCell = document.getElementById('pmrc_action_' + fromGl + '_' + toGl);
    if (actionCell) actionCell.innerHTML = '<span style="color:var(--green); font-weight:700; font-size:12px;">✓ Accepted</span>';
    const row = document.getElementById('pmrc_' + fromGl + '_' + toGl);
    if (row) row.style.background = '#f0fdf4';

    // Highlight the affected GL rows in the spreadsheet
    const fromRow = document.querySelector('tr[data-gl="' + fromGl + '"]');
    const toRow = document.querySelector('tr[data-gl="' + toGl + '"]');
    if (fromRow) { fromRow.style.background = '#fef2f2'; setTimeout(() => { fromRow.style.background = ''; }, 4000); }
    if (toRow) { toRow.style.background = '#f0fdf4'; setTimeout(() => { toRow.style.background = ''; }, 4000); }

    showToast('Reclass accepted — ' + fmt(amount) + ' moved from ' + fromGl + ' to ' + toGl, 'success');

    // Refresh data to recalculate all numbers
    _faExpenseCache = null;
    loadDetail();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

async function undoPmReclass(fromGl, toGl, invIdStr) {
  if (!confirm('Undo reclass from ' + fromGl + ' to ' + toGl + '?\\n\\nThis will restore the invoices to their original GL code.')) return;

  try {
    const invIds = invIdStr.split(',').map(s => parseInt(s)).filter(n => n > 0);
    for (const invId of invIds) {
      await fetch('/api/expense-dist/reclass/' + invId, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ reclass_to_gl: '' })
      });
    }

    const actionCell = document.getElementById('pmrc_action_' + fromGl + '_' + toGl);
    if (actionCell) actionCell.innerHTML = '<span style="color:var(--gray-400); font-weight:600; font-size:12px;">Undone</span>';
    const row = document.getElementById('pmrc_' + fromGl + '_' + toGl);
    if (row) { row.style.background = 'var(--gray-50)'; row.style.opacity = '0.5'; }

    showToast('Reclass undone — invoices restored to ' + fromGl, 'success');

    _faExpenseCache = null;
    loadDetail();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

async function undoSingleReclass(invId, fromGl, toGl, btn) {
  if (!confirm('Undo this invoice reclass?')) return;
  try {
    await fetch('/api/expense-dist/reclass/' + invId, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ reclass_to_gl: '' })
    });
    const row = btn.closest('tr');
    if (row) { row.style.opacity = '0.3'; row.style.pointerEvents = 'none'; }
    btn.textContent = 'Undone';
    btn.disabled = true;
    showToast('Invoice restored to ' + fromGl, 'success');
    _faExpenseCache = null;
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

// ─── Budget Proposals helpers ────────────────────────────────────────────
let _proposalModalGl = null;
let _proposalModalAction = null;

function proposalActionButtons(glCode) {
  return '<button onclick="acceptProposal(\'' + glCode + '\')" style="padding:4px 10px; font-size:11px; font-weight:600; border-radius:5px; cursor:pointer; background:#dcfce7; color:#166534; border:1px solid #86efac;">✓ Accept</button> ' +
    '<button onclick="openProposalModal(\'' + glCode + '\',\'rejected\')" style="padding:4px 10px; font-size:11px; font-weight:600; border-radius:5px; cursor:pointer; background:#fef2f2; color:#991b1b; border:1px solid #fca5a5; margin-left:4px;">✗ Reject</button> ' +
    '<button onclick="openProposalModal(\'' + glCode + '\',\'commented\')" style="padding:4px 10px; font-size:11px; font-weight:600; border-radius:5px; cursor:pointer; background:#fef3c7; color:#92400e; border:1px solid #fde68a; margin-left:4px;">💬</button>';
}

async function acceptProposal(glCode) {
  if (!confirm('Accept PM budget proposal for ' + glCode + '?')) return;
  try {
    const resp = await fetch('/api/budget-proposal/review', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ entity_code: entityCode, gl_code: glCode, action: 'accepted', note: '' })
    });
    const result = await resp.json();
    if (result.error) { showToast(result.error, 'error'); return; }

    // Update row in place
    const row = document.getElementById('prop_' + glCode);
    if (row) {
      const cells = row.querySelectorAll('td');
      cells[6].innerHTML = '<span style="background:#dcfce7; color:#166534; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✓ Accepted</span>';
      cells[7].innerHTML = '<span style="color:var(--gray-400); font-size:11px;">Done</span>';
    }
    showToast('Proposal accepted for ' + glCode, 'success');
    updateProposalBadge();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

function openProposalModal(glCode, action) {
  _proposalModalGl = glCode;
  _proposalModalAction = action;
  const modal = document.getElementById('proposalModal');
  const title = document.getElementById('proposalModalTitle');
  const overrideRow = document.getElementById('proposalModalOverrideRow');
  const submitBtn = document.getElementById('proposalModalSubmit');
  const noteEl = document.getElementById('proposalModalNote');
  const overrideEl = document.getElementById('proposalModalOverride');

  noteEl.value = '';
  overrideEl.value = '';

  if (action === 'rejected') {
    title.textContent = 'Reject Proposal — ' + glCode;
    overrideRow.style.display = '';
    submitBtn.textContent = 'Reject & Save';
    submitBtn.style.background = '#dc2626';
  } else {
    title.textContent = 'Comment on Proposal — ' + glCode;
    overrideRow.style.display = 'none';
    submitBtn.textContent = 'Save Comment';
    submitBtn.style.background = '#b45309';
  }
  modal.style.display = 'flex';
}

function closeProposalModal() {
  document.getElementById('proposalModal').style.display = 'none';
  _proposalModalGl = null;
  _proposalModalAction = null;
}

async function submitProposalReview() {
  const note = document.getElementById('proposalModalNote').value.trim();
  const overrideRaw = document.getElementById('proposalModalOverride').value.trim();
  const overrideValue = overrideRaw ? parseFloat(overrideRaw.replace(/[$,]/g, '')) : null;

  if (!note && _proposalModalAction === 'commented') {
    showToast('Please enter a comment', 'error');
    return;
  }

  try {
    const payload = {
      entity_code: entityCode,
      gl_code: _proposalModalGl,
      action: _proposalModalAction,
      note: note
    };
    if (_proposalModalAction === 'rejected' && overrideValue !== null && !isNaN(overrideValue)) {
      payload.override_value = overrideValue;
    }

    const resp = await fetch('/api/budget-proposal/review', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
    const result = await resp.json();
    if (result.error) { showToast(result.error, 'error'); return; }

    // Update row in place
    const row = document.getElementById('prop_' + _proposalModalGl);
    if (row) {
      const cells = row.querySelectorAll('td');
      if (_proposalModalAction === 'rejected') {
        cells[6].innerHTML = '<span style="background:#fef2f2; color:#991b1b; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✗ Rejected</span>';
        cells[7].innerHTML = '<span style="color:var(--gray-400); font-size:11px;">Done</span>';
        // If FA provided override, update the proposed column
        if (overrideValue !== null && !isNaN(overrideValue)) {
          cells[3].innerHTML = '<span style="color:var(--blue); font-weight:700;">' + fmt(overrideValue) + '</span> <span style="font-size:10px; color:var(--gray-400);">FA override</span>';
        }
      } else {
        cells[6].innerHTML = '<span style="background:#fef3c7; color:#92400e; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">💬 Commented</span>';
        // Keep action buttons for commented status
      }
    }

    closeProposalModal();
    const verb = _proposalModalAction === 'rejected' ? 'rejected' : 'comment saved on';
    showToast('Proposal ' + verb + ' ' + _proposalModalGl, 'success');
    updateProposalBadge();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

function updateProposalBadge() {
  // Recount pending proposals from the DOM
  const rows = document.querySelectorAll('#pmProposalsBody tr');
  let pending = 0;
  rows.forEach(r => {
    const statusCell = r.querySelectorAll('td')[6];
    if (statusCell && statusCell.textContent.includes('Pending')) pending++;
    if (statusCell && statusCell.textContent.includes('Commented')) pending++;
  });
  document.getElementById('pmProposalsCount').textContent = pending || '';
  // Update main badge
  const reclassCount = parseInt(document.getElementById('pmReclassCount').textContent) || 0;
  const notesCount = parseInt(document.getElementById('pmNotesCount').textContent) || 0;
  const total = notesCount + reclassCount + pending;
  document.getElementById('pmReviewBadgeText').textContent = total + ' item' + (total !== 1 ? 's' : '') + ' need review';
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

// RE Taxes GL prefix 6315 — forecast is pinned to approved budget (current_budget),
// estimate back-solves from the pinned forecast minus YTD actual. Matches the Excel
// RE Taxes tab logic where Forecast = SUM(YTD, Remaining) and the effective result
// equals the gross tax budget. User-entered estimate/forecast overrides still win.
function faIsFixedToBudget(l) {
  const gl = (l && l.gl_code) || '';
  return gl.indexOf('6315') === 0;
}

// One-time annual fees: once YTD is posted, there is no additional billing
// for the rest of the year, so the Mar-Dec estimate must be zero.
// Kept in sync with Python ONE_TIME_FEE_GLS constant in workflow.py.
const ONE_TIME_FEE_GLS = new Set(['6722-0000','6762-0000','6763-0000','6764-0000']);
function faIsOneTimeFeeBilled(l) {
  if (!l || !l.gl_code) return false;
  if (!ONE_TIME_FEE_GLS.has(l.gl_code)) return false;
  const billed = (l.ytd_actual || 0) + (l.accrual_adj || 0) + (l.unpaid_bills || 0);
  return Math.abs(billed) > 0.01;
}

function faComputeEstimate(l) {
  // Use override if FA set one
  if (l.estimate_override !== null && l.estimate_override !== undefined) return l.estimate_override;
  if (faIsFixedToBudget(l)) {
    const cb = l.current_budget || 0;
    const ytd = l.ytd_actual || 0;
    return cb - ytd;
  }
  // One-time fees with a YTD posted: no more projection
  if (faIsOneTimeFeeBilled(l)) return 0;
  const ytd = l.ytd_actual || 0;
  const accrual = l.accrual_adj || 0;
  const unpaid = l.unpaid_bills || 0;
  const base = ytd + accrual + unpaid;
  // Formula: (YTD+Accrual+Unpaid) / YTD_MONTHS * REMAINING_MONTHS
  if (YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
  return 0;
}

function faComputeForecast(l) {
  // Use override if FA set one
  if (l.forecast_override !== null && l.forecast_override !== undefined) return l.forecast_override;
  if (faIsFixedToBudget(l)) {
    return l.current_budget || 0;
  }
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
    if (faIsFixedToBudget(l)) {
      const cb = l.current_budget || 0;
      return '=' + cb + ' − ' + ytd + '  (current budget − YTD, GL 6315 pinned)';
    }
    if (faIsOneTimeFeeBilled(l)) {
      return '= 0  (one-time fee rule, GL ' + (l.gl_code || '') + ' — already billed YTD)';
    }
    if (YTD_MONTHS > 0) return '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
    return '=0';
  }
  if (field === 'forecast') {
    if (faIsFixedToBudget(l)) {
      const cb = l.current_budget || 0;
      return '=' + cb + '  (pinned to current budget, GL 6315)';
    }
    if (faIsOneTimeFeeBilled(l)) {
      return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+0  (one-time fee — no additional projection)';
    }
    const estExpr = (YTD_MONTHS > 0) ? '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0';
    return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + estExpr + ')';
  }
  if (field === 'proposed') {
    if (l.proposed_formula) return l.proposed_formula;
    const fcstExpr = ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + ((YTD_MONTHS > 0) ? '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0') + ')';
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

  // Handle Payroll tab — enhanced with roster calc engine, assumptions, and GL grouping
  if (sheetName === 'Payroll') {
    renderPayrollTab(sheetLines, contentDiv);
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
  const cellCalc = 'text-align:right; padding:8px 10px; border-bottom:1px solid var(--gray-200); box-shadow:inset 3px 0 0 #16a34a; color:#15803d; font-weight:600;';
  const cellLabel = 'padding:8px 10px; font-size:13px; border-bottom:1px solid var(--gray-200);';
  const cellNote = 'padding:8px 10px; font-size:11px; color:var(--gray-400); border-bottom:1px solid var(--gray-200);';
  const inputDollar = 'width:100%; text-align:right; padding:6px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-weight:500; background:var(--gray-50);';
  const inputRate = 'width:90px; text-align:right; padding:6px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-weight:500; background:var(--gray-50);';
  const fxBadge = '<span class="re-fx-badge" style="display:none; background:#4ade80; color:#fff; font-size:9px; font-weight:700; padding:1px 4px; border-radius:3px; margin-left:4px; vertical-align:middle;"></span>';
  // Wrap calculated values in a span so reCalcTaxes can update value without destroying the fx badge
  // Each fx cell is clickable: onclick populates the formula bar
  const fxCell = (id, val, formula, label) => {
    return '<td style="' + cellCalc + ' cursor:pointer;" id="' + id + '" data-formula="' + formula + '" data-label="' + label + '" onclick="reTaxFxClick(this)" tabindex="0">' +
      '<span class="re-fx-val">' + val + '</span>' + fxBadge + '</td>';
  };

  const d = reTaxes;
  const ex = d.exemptions || {};

  let html = `
  <div style="max-width:100%; margin:0 auto;">
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
        <td style="text-align:right; padding:10px; font-weight:700; font-size:14px; box-shadow:inset 3px 0 0 #16a34a; color:#15803d; cursor:pointer;" id="re_gross" data-formula="= re_h1_tax + re_h2_tax" data-label="Gross Tax" onclick="reTaxFxClick(this)" tabindex="0"><span class="re-fx-val">${fmtD(d.gross_tax)}</span>${fxBadge}</td>
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
        <td style="text-align:right; padding:10px; font-weight:600; cursor:pointer; box-shadow:inset 3px 0 0 #16a34a; color:#15803d;" id="re_ex_total_current" data-formula="= SUM(current exemptions)" data-label="Total Exemptions (Current)" onclick="reTaxFxClick(this)" tabindex="0"><span class="re-fx-val">${fmtD(d.total_exemptions_current)}</span>${fxBadge}</td>
        <td style="text-align:right; padding:10px; font-weight:600; cursor:pointer; box-shadow:inset 3px 0 0 #16a34a; color:#15803d;" id="re_ex_total_budget" data-formula="= SUM(budget exemptions)" data-label="Total Exemptions (Budget)" onclick="reTaxFxClick(this)" tabindex="0"><span class="re-fx-val">${fmtD(d.total_exemptions_budget)}</span>${fxBadge}</td>
      </tr>

      <!-- Net Tax -->
      <tr style="border-top:3px solid var(--gray-400);">
        <td style="padding:12px 10px; font-weight:700; font-size:15px;">NET TAX LIABILITY</td>
        <td style="text-align:right; padding:12px 10px; font-weight:700; font-size:15px; box-shadow:inset 3px 0 0 #16a34a; color:#15803d; cursor:pointer;" id="re_net" data-formula="= re_gross - re_ex_total_budget" data-label="Net Tax Liability" onclick="reTaxFxClick(this)" tabindex="0"><span class="re-fx-val">${fmtD(d.net_tax)}</span>${fxBadge}</td>
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
    if (badge) { badge.textContent = 'fx'; badge.style.background = '#dbeafe'; badge.style.color = 'var(--blue)'; badge.style.borderColor = 'var(--blue)'; badge.style.display = 'inline-block'; }
  } else if (typed !== '' && numericVal !== null && /^[\d$,.\-\s]+$/.test(typed)) {
    // User typed a plain number — store as override
    const rounded = Math.round(numericVal);
    const valSpan = el.querySelector('.re-fx-val');
    if (valSpan) valSpan.textContent = '$' + rounded.toLocaleString();
    el.dataset.override = 'true';
    el.dataset.overrideVal = typed;
    // Badge → pencil (manual override)
    const badge = el.querySelector('.re-fx-badge');
    if (badge) { badge.textContent = '\u270e'; badge.style.background = '#f97316'; badge.style.color = '#fff'; badge.style.borderColor = '#ea580c'; badge.style.display = 'inline-block'; }
  } else if (typed === '' || typed.toLowerCase() === 'auto' || typed.toLowerCase() === 'formula') {
    // Revert to auto formula
    el.dataset.override = 'false';
    el.dataset.overrideVal = '';
    const badge = el.querySelector('.re-fx-badge');
    if (badge) { badge.textContent = ''; badge.style.display = 'none'; }
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
  if (badge) { badge.textContent = ''; badge.style.display = 'none'; }
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


async function renderBudgetSummary(contentDiv) {
  const COLS = ['c1','c2','c3','c4','c5','c6','c7'];
  const COL_NAMES = {c1:'Col 1 \u00b7 '+BY3+' Actual',c2:'Col 2 \u00b7 '+BY2+' Actual',c3:'Col 3 \u00b7 '+BY1+' YTD',
    c4:'Col 4 \u00b7 '+BY1+' Est.',c5:'Col 5 \u00b7 '+BY1+' Forecast',c6:'Col 6 \u00b7 '+BY1+' Budget',c7:'Col 7 \u00b7 '+BY+' Budget'};
  const SUM_TAB_COLORS = {
    "Income":{bg:"rgba(76,175,80,0.15)",color:"#2e7d32"},"Payroll":{bg:"rgba(33,150,243,0.15)",color:"#1565c0"},
    "Energy":{bg:"rgba(255,152,0,0.15)",color:"#e65100"},"Water & Sewer":{bg:"rgba(0,188,212,0.15)",color:"#00838f"},
    "Repairs & Supplies":{bg:"rgba(121,85,72,0.15)",color:"#5d4037"},"Gen & Admin":{bg:"rgba(156,39,176,0.15)",color:"#7b1fa2"},
    "RE Taxes":{bg:"rgba(244,67,54,0.15)",color:"#c62828"},"Manual":{bg:"rgba(255,213,79,0.15)",color:"#f57f17"},
  };
  const SUM_TAB_SHORT = {"Income":"Income","Payroll":"Payroll","Energy":"Energy","Water & Sewer":"Water",
    "Repairs & Supplies":"R&S","Gen & Admin":"Gen&Admin","RE Taxes":"RE Tax","Manual":"Manual"};

  function sfmt(v) {
    if (v===null||v===undefined||v==='') return '\u2014';
    const n=Number(v); if(isNaN(n)||n===0) return '\u2014';
    const s=Math.abs(Math.round(n)).toLocaleString('en-US');
    return n<0?'('+s+')':s;
  }
  function schip(tab) {
    if(!tab) return '<span style="color:var(--gray-400);font-size:11px">\u2014</span>';
    const c=SUM_TAB_COLORS[tab]||{bg:'rgba(158,158,158,0.15)',color:'#757575'};
    return '<span style="display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:600;letter-spacing:0.3px;white-space:nowrap;background:'+c.bg+';color:'+c.color+'">'+(SUM_TAB_SHORT[tab]||tab)+'</span>';
  }

  // Fetch summary data from API
  let sumData = null;
  try {
    const res = await fetch('/api/summary/' + entityCode);
    if (res.ok) sumData = await res.json();
  } catch(e) {}

  if (!sumData || !sumData.rows || sumData.rows.length === 0) {
    contentDiv.innerHTML = '<div style="padding:40px; text-align:center; color:var(--gray-500);"><p style="font-size:16px; margin-bottom:8px;">No Budget Summary data imported yet.</p><p style="font-size:13px;">Import an approved budget Excel to populate the Summary tab.</p></div>';
    return;
  }

  // Build label-keyed lineage map for the inspector drill-down
  window._sumLineage = {};
  window._sumRowMap = {};
  sumData.rows.forEach(r => {
    if (r.lineage && r.label) window._sumLineage[r.label] = r.lineage;
    if (r.label) window._sumRowMap[r.label] = r;
  });

  // Build section-aware data structure
  const rows = sumData.rows;
  const sections = {};
  const hasSectionHeaders = rows.some(r => r.row_type === 'section_header');

  if (hasSectionHeaders) {
    // Buildings WITH section headers: assign sections from header labels
    let currentSec = '';
    rows.forEach(r => {
      if (r.row_type === 'section_header') currentSec = r.label;
      r._sec = currentSec;
      const sk = currentSec.toLowerCase().includes('non') && currentSec.toLowerCase().includes('income') ? 'noi' :
                 currentSec.toLowerCase().includes('non') && currentSec.toLowerCase().includes('expense') ? 'noe' :
                 currentSec.toLowerCase() === 'income' ? 'income' :
                 currentSec.toLowerCase() === 'expenses' ? 'expenses' : '';
      r._sk = sk;
      if (r.row_type === 'data' && sk) {
        if (!sections[sk]) sections[sk] = [];
        sections[sk].push(r);
      }
    });
  } else {
    // Buildings WITHOUT section headers: infer sections from subtotal positions
    // Standard layout: income rows -> Total Income -> expense rows -> Total Expenses
    //   -> Net Operating -> NOI data -> Total NOI -> NOE data -> Total NOE -> Grand Total
    let inferredSk = 'income';
    rows.forEach(r => {
      if (r.row_type === 'subtotal') {
        const lbl = r.label.toLowerCase();
        if (lbl.includes('total income'))                                        { r._sk = 'income';   inferredSk = 'expenses'; }
        else if (lbl.includes('total expense') && !lbl.includes('non'))          { r._sk = 'expenses'; inferredSk = 'noi'; }
        else if (lbl.includes('net operating'))                                  { r._sk = '';          inferredSk = 'noi'; }
        else if (lbl.includes('total non') && lbl.includes('income'))            { r._sk = 'noi';      inferredSk = 'noe'; }
        else if (lbl.includes('total non') && lbl.includes('expense'))           { r._sk = 'noe';      inferredSk = ''; }
        else if (lbl.includes('total surplus') || lbl.includes('total deficit')) { r._sk = ''; }
        else                                                                     { r._sk = ''; }
      } else {
        r._sk = inferredSk;
      }
      r._sec = r._sk;
      if (r.row_type === 'data' && r._sk) {
        if (!sections[r._sk]) sections[r._sk] = [];
        sections[r._sk].push(r);
      }
    });
  }

  // Table
  const thS = 'text-align:right;padding:10px 10px;white-space:nowrap;font-weight:600;border-bottom:2px solid var(--gray-300);background:var(--gray-100);';
  let html = '<div style="background:white;border-radius:12px;border:1px solid var(--gray-200);">' +
    '<div id="sumFBar" style="display:flex;align-items:center;gap:12px;padding:10px 20px;background:white;border:1px solid var(--gray-200);border-radius:8px;margin:8px 8px 0;min-height:44px;transition:all .2s;position:sticky;top:48px;z-index:30;box-shadow:0 2px 4px rgba(0,0,0,0.04);">' +
    '<span style="font-size:11px;font-weight:800;color:white;background:var(--blue);padding:2px 8px;border-radius:4px;font-family:monospace;letter-spacing:1px;">fx</span>' +
    '<span id="sumFBLabel" style="font-size:11px;font-weight:700;color:var(--blue);text-transform:uppercase;white-space:nowrap;min-width:60px;">Click a cell\u2026</span>' +
    '<input id="sumFBInput" type="text" disabled placeholder="Select an editable cell to enter a value or formula\u2026" style="font-family:monospace;font-size:13px;color:var(--gray-700);flex:1;padding:4px 8px;background:var(--gray-50);border:1px solid var(--gray-200);border-radius:4px;outline:none;">' +
    '<span id="sumFBPreview" style="font-size:13px;color:var(--gray-500);font-family:monospace;min-width:100px;text-align:right;"></span>' +
    '<button id="sumFBAccept" style="display:none;padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:none;background:var(--green);color:white;">Accept</button>' +
    '<button id="sumFBCancel" style="display:none;padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid var(--gray-200);background:white;color:var(--gray-600);">Cancel</button>' +
    '<button id="sumFBClear" style="display:none;padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid rgba(224,36,36,0.3);background:white;color:var(--red);">Clear</button>' +
    '<button id="sumFBInspect" style="display:none;padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid #16a34a;background:#f0fdf4;color:#15803d;" title="Show how this number was calculated">\ud83d\udd0d Inspect</button>' +
    '</div>' +
    '<div id="sumDrillPanel" style="display:none;margin:0 8px 8px;background:white;border:1px solid #bbf7d0;border-left:4px solid #16a34a;border-radius:8px;padding:14px 18px;font-size:13px;position:sticky;top:100px;z-index:29;box-shadow:0 4px 12px rgba(0,0,0,0.08);max-height:60vh;overflow-y:auto;"></div>' +
    '<table id="sumTable" style="border-collapse:separate;border-spacing:0;font-size:13px;width:100%;">' +
    '<thead style="position:sticky;top:94px;z-index:20;"><tr>' +
    '<th style="text-align:left;padding:10px;min-width:200px;max-width:240px;position:sticky;left:0;z-index:25;background:var(--gray-100);border-right:2px solid var(--gray-300);border-bottom:2px solid var(--gray-300);box-shadow:2px 0 8px rgba(90,74,63,0.08);">Line Item</th>' +
    '<th style="'+thS+'min-width:80px;">Tab</th>' +
    '<th style="'+thS+'min-width:120px;"><span style="font-size:10px;color:var(--gray-500);display:block;">Col 1</span>'+BY3+' Actual*</th>' +
    '<th style="'+thS+'min-width:120px;color:var(--gray-400);font-style:italic;"><span style="font-size:10px;display:block;">Col 2</span>'+BY2+' Actual</th>' +
    '<th style="'+thS+'min-width:120px;color:var(--gray-400);font-style:italic;"><span style="font-size:10px;display:block;">Col 3</span>'+BY1+' YTD</th>' +
    '<th style="'+thS+'min-width:120px;color:var(--gray-400);font-style:italic;"><span style="font-size:10px;display:block;">Col 4</span>'+BY1+' Est.</th>' +
    '<th style="'+thS+'min-width:120px;color:var(--gray-400);font-style:italic;"><span style="font-size:10px;display:block;">Col 5</span>'+BY1+' Forecast</th>' +
    '<th style="'+thS+'min-width:120px;"><span style="font-size:10px;color:var(--gray-500);display:block;">Col 6</span>'+BY1+' Budget</th>' +
    '<th style="'+thS+'min-width:130px;background:#fffbeb;"><span style="font-size:10px;color:var(--gray-500);display:block;">Col 7 \u270e</span>'+BY+' Budget</th>' +
    '<th style="'+thS+'min-width:80px;"><span style="font-size:10px;color:var(--gray-500);display:block;">Col 8</span>% Var</th>' +
    '<th style="text-align:left;padding:10px;min-width:170px;border-bottom:2px solid var(--gray-300);background:var(--gray-100);">Notes</th>' +
    '</tr></thead><tbody id="sumBody">';

  function makeInput(val, label, col, bg) {
    const raw = (val!==null&&val!==undefined&&val!==0) ? Math.round(val) : '';
    const disp = raw!=='' ? raw.toLocaleString('en-US') : '';
    // Cols c2-c5 are computed from sources (audit / GL lines). Mark them as inspectable
    // with a green left-stripe + data-fx flag so sumCellFocus can show the "Inspect" button.
    const isFx = (col === 'c2' || col === 'c3' || col === 'c4' || col === 'c5');
    // Only Col 7 (proposed budget) persists. c1-c6 are read-only:
    //   c1, c6 = imported from Excel; c2 = audit; c3-c5 = GL aggregation.
    const isReadOnly = (col !== 'c7');
    const roAttr = isReadOnly ? ' readonly' : '';
    const stripe = isFx ? 'box-shadow:inset 3px 0 0 #16a34a;color:#15803d;font-weight:600;' : '';
    const fxAttr = isFx ? ' data-fx="1"' : '';
    // Read-only cells: no border, default cursor, transparent bg (cell bg shows through)
    // Editable c7: keeps gray border + text cursor so it reads as an input
    const inputStyle = isReadOnly
      ? 'width:100px;padding:5px 8px;border:1px solid transparent;border-radius:4px;font-size:13px;text-align:right;background:transparent;font-variant-numeric:tabular-nums;font-family:inherit;cursor:default;color:var(--gray-700);'+stripe
      : 'width:100px;padding:5px 8px;border:1px solid var(--gray-300);border-radius:4px;font-size:13px;text-align:right;background:'+(bg||'#fffbeb')+';font-variant-numeric:tabular-nums;font-family:inherit;cursor:text;';
    return '<td class="number" style="background:'+(bg||'#fbfaf4')+';padding:4px 6px;font-variant-numeric:tabular-nums;text-align:right;">' +
      '<input type="text" value="'+disp+'" placeholder="\u2014" data-label="'+label.replace(/"/g,'&quot;')+'" data-col="'+col+'" data-raw="'+raw+'"'+fxAttr+roAttr+' ' +
      'onfocus="sumCellFocus(this)" onblur="sumCellBlur(this)" onkeydown="sumCellKey(event,this)" ' +
      'style="'+inputStyle+'"></td>';
  }
  const _fxBadge = '<span class="sum-fx" style="display:inline-block;background:#4ade80;color:#fff;font-size:8px;font-weight:700;padding:1px 3px;border-radius:3px;margin-left:4px;vertical-align:middle;">fx</span>';
  function sumTd(col) {
    return '<td class="number" data-sum-col="'+col+'" style="text-align:right;padding:8px 10px;font-weight:700;font-variant-numeric:tabular-nums;cursor:pointer;" onclick="sumSubtotalClick(this)"><span class="sub-val">\u2014</span>'+_fxBadge+'</td>';
  }
  function noteIn(label) {
    return '<td style="padding:4px 6px;"><input type="text" placeholder="Add note\u2026" data-note-label="'+label.replace(/"/g,'&quot;')+'" ' +
      'style="width:100%;padding:5px 8px;border:1px solid var(--gray-200);border-radius:4px;font-size:12px;background:white;font-family:inherit;color:var(--gray-700);"></td>';
  }

  rows.forEach((r, idx) => {
    if (r.row_type === 'section_header') {
      html += '<tr data-sec="'+r._sk+'" style="background:var(--blue-light);">' +
        '<td colspan="11" style="font-weight:700;color:var(--blue);font-size:14px;padding:10px;border-bottom:2px solid var(--blue);position:sticky;left:0;background:var(--blue-light);">' +
        r.label + ' <button onclick="sumShowInsert(\''+r._sk+'\',\''+r.label.replace(/'/g,"\\'")+'\')" style="margin-left:12px;display:inline-flex;align-items:center;gap:4px;padding:3px 10px;border:1px dashed var(--gray-300);border-radius:6px;font-size:11px;font-weight:600;color:var(--gray-500);background:transparent;cursor:pointer;vertical-align:middle;">+ Add Row</button>' +
        '</td></tr>';
    } else if (r.row_type === 'data') {
      const fn = r.footnote_marker ? '<span style="color:var(--gray-500);font-size:11px;font-weight:600;vertical-align:super;margin-left:2px;">'+r.footnote_marker+'</span>' : '';
      html += '<tr data-sec="'+r._sk+'" data-type="d" data-order="'+r.display_order+'">' +
        '<td style="padding:8px 10px;border-bottom:1px solid var(--gray-200);position:sticky;left:0;z-index:15;background:white;min-width:200px;max-width:240px;border-right:2px solid var(--gray-300);box-shadow:2px 0 8px rgba(90,74,63,0.08);">'+r.label+fn+'</td>' +
        '<td style="text-align:right;padding:8px 10px;border-bottom:1px solid var(--gray-200);">'+schip(r.source_tab)+'</td>' +
        makeInput(r.col1, r.label, 'c1', '#fbfaf4') +
        makeInput(r.col2, r.label, 'c2', '#f9f9f7') +
        makeInput(r.col3, r.label, 'c3', '#f9f9f7') +
        makeInput(r.col4, r.label, 'c4', '#f9f9f7') +
        makeInput(r.col5, r.label, 'c5', '#f9f9f7') +
        makeInput(r.col6, r.label, 'c6', '#fbfaf4') +
        makeInput(r.col7, r.label, 'c7', '#fffbeb') +
        '<td style="text-align:right;padding:8px 10px;border-bottom:1px solid var(--gray-200);color:var(--gray-400);font-variant-numeric:tabular-nums;">\u2014</td>' +
        noteIn(r.label) + '</tr>';
    } else if (r.row_type === 'subtotal') {
      const isNet = r.label.toLowerCase().includes('net operating');
      const isGrand = r.label.toLowerCase().includes('total surplus') || r.label.toLowerCase().includes('total deficit');
      const calcAttr = isGrand ? 'data-calc="grand"' : isNet ? 'data-calc="income-expenses"' : 'data-sums="'+r._sk+'"';
      const bgStyle = isGrand ? 'background:#1e3a5f;color:white;' : isNet ? 'background:#f0f4f8;border-top:2px solid var(--gray-400);border-bottom:2px solid var(--gray-400);' : 'background:var(--gray-100);border-top:2px solid var(--gray-300);';
      const tdFrozen = isGrand ? 'background:#1e3a5f;color:white;' : isNet ? 'background:#f0f4f8;' : 'background:var(--gray-100);';

      html += '<tr '+calcAttr+' data-sec="'+r._sk+'" style="'+bgStyle+'">' +
        '<td style="padding:8px 10px;font-weight:700;position:sticky;left:0;z-index:15;'+tdFrozen+'min-width:200px;max-width:240px;border-right:2px solid var(--gray-300);box-shadow:2px 0 8px rgba(90,74,63,0.08);">'+r.label+'</td>' +
        '<td style="'+(isGrand?'background:#1e3a5f;':'')+'"></td>';
      // Option F: green fill for computed cells (subtotal + net), light-green text on dark blue for grand total. No fx badge.
      const computedCellStyle = isGrand
        ? 'background:#1e3a5f;color:#86efac;'
        : 'background:#f0fdf4;color:#16a34a;';
      COLS.forEach(c => {
        html += '<td data-sum-col="'+c+'" style="text-align:right;padding:8px 10px;font-weight:700;font-variant-numeric:tabular-nums;cursor:pointer;'+computedCellStyle+'" onclick="sumSubtotalClick(this)"><span class="sub-val">\u2014</span></td>';
      });
      html += '<td data-sum-col="c8" style="text-align:right;padding:8px 10px;font-weight:700;font-variant-numeric:tabular-nums;cursor:pointer;'+computedCellStyle+'" onclick="sumSubtotalClick(this)"><span class="sub-val">\u2014</span></td>';
      html += '<td style="'+(isGrand?'background:#1e3a5f;':'')+'padding:4px 6px;">'+(isGrand?'':'<input type="text" placeholder="Add note\u2026" style="width:100%;padding:5px 8px;border:1px solid var(--gray-200);border-radius:4px;font-size:12px;background:white;font-family:inherit;">')+'</td>';
      html += '</tr>';
    }
  });

  html += '</tbody></table></div>';
  contentDiv.innerHTML = html;

  // Recalculate totals
  sumRecalcTotals();
}

// ── Summary tab: recalculate all subtotals ──
function sumRecalcTotals() {
  const COLS = ['c1','c2','c3','c4','c5','c6','c7'];
  const tbody = document.getElementById('sumBody');
  if (!tbody) return;

  const secs = {income:[], expenses:[], noi:[], noe:[]};
  tbody.querySelectorAll('tr[data-type="d"]').forEach(tr => {
    const sec = tr.dataset.sec;
    const skMap = {'Income':'income','Expenses':'expenses','Non-Operating Income':'noi','Non-Operating Expense':'noe'};
    const sk = skMap[sec] || tr.closest('[data-sec]')?.dataset.sec || '';
    if (!secs[sk]) return;
    const vals = {};
    COLS.forEach(c => {
      const inp = tr.querySelector('input[data-col="'+c+'"]');
      vals[c] = inp ? (parseFloat(inp.dataset.raw) || 0) : 0;
    });
    secs[sk].push(vals);
  });

  function sumSec(key) {
    const t = {}; COLS.forEach(c => t[c] = 0);
    (secs[key]||[]).forEach(v => { COLS.forEach(c => t[c] += v[c]); });
    return t;
  }
  const inc = sumSec('income'), exp = sumSec('expenses'), noi = sumSec('noi'), noe = sumSec('noe');

  function writeSum(sel, totals) {
    const tr = tbody.querySelector(sel);
    if (!tr) return;
    COLS.forEach(c => {
      const td = tr.querySelector('[data-sum-col="'+c+'"]');
      if (td) {
        const sv = td.querySelector('.sub-val');
        const v = totals[c];
        const txt = (!v && v !== 0) ? '\u2014' : (Math.round(v) === 0 ? '\u2014' : (v < 0 ? '(' + Math.abs(Math.round(v)).toLocaleString('en-US') + ')' : Math.round(v).toLocaleString('en-US')));
        if (sv) sv.textContent = txt; else td.textContent = txt;
      }
    });
    const c8 = tr.querySelector('[data-sum-col="c8"]');
    if (c8) {
      const sv8 = c8.querySelector('.sub-val');
      if (totals.c7 && totals.c5 && totals.c5 !== 0) {
        const pct = ((totals.c7 - totals.c5) / Math.abs(totals.c5)) * 100;
        const pctHtml = '<span style="color:'+(pct>0?'var(--green)':pct<0?'var(--red)':'var(--gray-400)')+'">'+(pct>0?'+':'')+pct.toFixed(1)+'%</span>';
        if (sv8) sv8.innerHTML = pctHtml; else c8.innerHTML = pctHtml;
      } else {
        if (sv8) sv8.textContent = '\u2014'; else c8.textContent = '\u2014';
      }
    }
  }

  writeSum('tr[data-sums="income"]', inc);
  writeSum('tr[data-sums="expenses"]', exp);
  writeSum('tr[data-sums="noi"]', noi);
  writeSum('tr[data-sums="noe"]', noe);

  // Net Operating = Income - Expenses
  const net = {}; COLS.forEach(c => net[c] = inc[c] - exp[c]);
  writeSum('tr[data-calc="income-expenses"]', net);

  // Grand = Net + NOI - NOE
  const grand = {}; COLS.forEach(c => grand[c] = net[c] + noi[c] - noe[c]);
  writeSum('tr[data-calc="grand"]', grand);
}

// ── Summary tab: subtotal fx click ──
let _activeSumSubtotal = null;
function sumSubtotalClick(td) {
  // Clear previous highlight
  if (_activeSumSubtotal && _activeSumSubtotal !== td) {
    _activeSumSubtotal.style.outline = '';
  }
  _activeSumSubtotal = td;
  td.style.outline = '2px solid var(--blue)';
  td.style.outlineOffset = '-2px';

  const col = td.dataset.sumCol;
  const tr = td.closest('tr');
  const COL_NAMES = {c1:BY3+' Actual',c2:BY2+' Actual',c3:BY1+' YTD',c4:BY1+' Est.',c5:BY1+' Forecast',c6:BY1+' Budget',c7:BY+' Budget',c8:'% Var'};
  const rowLabel = tr ? (tr.querySelector('td')?.textContent || 'Total') : 'Total';
  const colLabel = COL_NAMES[col] || col;

  // Build formula from component data rows
  const tbody = document.getElementById('sumBody');
  let formula = '';
  if (col === 'c8') {
    // % Var = (c7 - c5) / |c5|
    formula = '= (Col 7 - Col 5) / |Col 5|';
  } else if (tr.dataset.sums) {
    // Section subtotal: sum all data rows in this section
    const secKey = tr.dataset.sums;
    const vals = [];
    tbody.querySelectorAll('tr[data-type="d"][data-sec="'+secKey+'"]').forEach(dr => {
      const inp = dr.querySelector('input[data-col="'+col+'"]');
      if (inp) { const v = parseFloat(inp.dataset.raw) || 0; if (v !== 0) vals.push(Math.round(v)); }
    });
    formula = vals.length <= 10 ? '= ' + (vals.length ? vals.join(' + ') : '0') : '= SUM of ' + vals.length + ' lines = ' + vals.reduce((a,b)=>a+b,0).toLocaleString();
  } else if (tr.dataset.calc === 'income-expenses') {
    // Net Operating = Income - Expenses
    const incTr = tbody.querySelector('tr[data-sums="income"]');
    const expTr = tbody.querySelector('tr[data-sums="expenses"]');
    const incVal = incTr ? (incTr.querySelector('[data-sum-col="'+col+'"] .sub-val')?.textContent || '0') : '0';
    const expVal = expTr ? (expTr.querySelector('[data-sum-col="'+col+'"] .sub-val')?.textContent || '0') : '0';
    formula = '= Income (' + incVal + ') - Expenses (' + expVal + ')';
  } else if (tr.dataset.calc === 'grand') {
    formula = '= Net Operating + Non-Op Income - Non-Op Expenses';
  }

  // Show in formula bar
  const bar = document.getElementById('sumFBar');
  if (bar) bar.style.borderColor = 'var(--blue)';
  const lbl = document.getElementById('sumFBLabel');
  if (lbl) lbl.textContent = rowLabel.trim() + ' \u2192 ' + colLabel;
  const inp = document.getElementById('sumFBInput');
  if (inp) { inp.disabled = true; inp.value = formula; inp.style.opacity = '0.85'; inp.placeholder = ''; }
  ['sumFBAccept','sumFBCancel','sumFBClear'].forEach(id => { const b = document.getElementById(id); if(b) b.style.display='none'; });
  const prev = document.getElementById('sumFBPreview');
  if (prev) prev.textContent = '';
}
// Clear subtotal highlight on click-away
document.addEventListener('click', function(e) {
  if (!_activeSumSubtotal) return;
  if (_activeSumSubtotal.contains(e.target)) return;
  const bar = document.getElementById('sumFBar');
  if (bar && bar.contains(e.target)) return;
  _activeSumSubtotal.style.outline = '';
  _activeSumSubtotal = null;
  sumResetBar();
});

// ── Summary tab: cell editing ──
let _sumActiveCell = null;

// Build an Excel-style numerical formula string for a read-only summary cell
function sumBuildFormulaText(label, col, lineage, raw) {
  const fmt = (n) => {
    if (n === null || n === undefined || isNaN(n)) return '0';
    const r = Math.round(Number(n));
    return r < 0 ? '(' + Math.abs(r).toLocaleString('en-US') + ')' : r.toLocaleString('en-US');
  };
  // c1 and c6 are direct Excel imports — no formula, just the number
  if (col === 'c1' || col === 'c6') {
    return raw ? Math.round(Number(raw)).toLocaleString('en-US') : '';
  }
  if (!lineage) return raw ? Math.round(Number(raw)).toLocaleString('en-US') : '';
  if (col === 'c2') {
    const c2 = lineage.c2 || {};
    if (!c2.has_audit || !c2.matched_category) return '';
    return '= ' + fmt(c2.value);
  }
  // Cols 3-5: GL aggregation
  const gl = lineage.gl || {};
  const ff = lineage.fixed_forecast || {};
  const rowData = (window._sumRowMap || {})[label] || {};
  // Fixed-forecast override (Maintenance / Common Charges / Commercial Rent)
  if (ff.applied && col === 'c5') {
    return '= ' + fmt(rowData.col6);
  }
  if (ff.applied && col === 'c4') {
    return '= ' + fmt(rowData.col5) + ' - ' + fmt(rowData.col3) + ' = ' + fmt(rowData.col4);
  }
  const allLines = gl.lines || [];
  const ytdM = gl.ytd_months || 0;
  const remM = gl.remaining_months || 0;
  if (col === 'c3') {
    // Sum of per-line YTD values (non-zero only)
    const lines = allLines.filter(l => Math.round(Number(l.ytd)||0) !== 0);
    if (!lines.length) return '';
    if (lines.length === 1) return '= ' + fmt(lines[0].ytd);
    const total = lines.reduce((s,l) => s + (Number(l.ytd)||0), 0);
    return '= ' + lines.map(l => fmt(l.ytd)).join(' + ') + ' = ' + fmt(total);
  }
  if (col === 'c4') {
    // Estimate = (Σ YTD ÷ ytd_months) × remaining_months
    const totalYtd = allLines.reduce((s,l) => s + (Number(l.ytd)||0), 0);
    if (!ytdM || !remM) return '= ' + fmt(totalYtd);
    const totalEst = (totalYtd / ytdM) * remM;
    return '= ' + fmt(totalYtd) + ' / ' + ytdM + ' * ' + remM + ' = ' + fmt(totalEst);
  }
  if (col === 'c5') {
    // Forecast = YTD + Accrual + Unpaid + Estimate (totals)
    const tY = allLines.reduce((s,l) => s + (Number(l.ytd)||0), 0);
    const tA = allLines.reduce((s,l) => s + (Number(l.accrual)||0), 0);
    const tU = allLines.reduce((s,l) => s + (Number(l.unpaid)||0), 0);
    const tE = allLines.reduce((s,l) => s + (Number(l.estimate)||0), 0);
    const tF = tY + tA + tU + tE;
    // Drop any zero terms to keep it readable
    const parts = [];
    if (Math.round(tY) !== 0) parts.push(fmt(tY));
    if (Math.round(tA) !== 0) parts.push(fmt(tA));
    if (Math.round(tU) !== 0) parts.push(fmt(tU));
    if (Math.round(tE) !== 0) parts.push(fmt(tE));
    if (!parts.length) return '';
    if (parts.length === 1) return '= ' + parts[0];
    return '= ' + parts.join(' + ') + ' = ' + fmt(tF);
  }
  return '';
}

function sumCellFocus(el) {
  // Clear any subtotal highlight
  if (_activeSumSubtotal) { _activeSumSubtotal.style.outline = ''; _activeSumSubtotal = null; }
  _sumActiveCell = el;
  const bar = document.getElementById('sumFBar');
  if (bar) bar.style.borderColor = 'var(--blue)';
  const COL_NAMES = {c1:'Col 1 \u00b7 '+BY3+' Actual',c2:'Col 2 \u00b7 '+BY2+' Actual',c3:'Col 3 \u00b7 '+BY1+' YTD',
    c4:'Col 4 \u00b7 '+BY1+' Est.',c5:'Col 5 \u00b7 '+BY1+' Forecast',c6:'Col 6 \u00b7 '+BY1+' Budget',c7:'Col 7 \u00b7 '+BY+' Budget'};
  const cl = COL_NAMES[el.dataset.col] || el.dataset.col;
  const lbl = document.getElementById('sumFBLabel');
  const isReadOnly = (el.dataset.col !== 'c7');
  if (lbl) lbl.textContent = el.dataset.label + ' \u2192 ' + cl;
  const inp = document.getElementById('sumFBInput');
  if (inp) {
    if (isReadOnly) {
      const lineage = (window._sumLineage || {})[el.dataset.label];
      inp.value = sumBuildFormulaText(el.dataset.label, el.dataset.col, lineage, el.dataset.raw);
      inp.disabled = true;
      inp.style.opacity = '0.85';
      inp.placeholder = '';
    } else {
      inp.value = el.dataset.raw || el.value || '';
      inp.disabled = false;
      inp.style.opacity = '1';
      inp.placeholder = 'Enter value or formula (e.g. =9384324*1.035)';
    }
  }
  // Hide Accept/Cancel/Clear for read-only cells
  ['sumFBAccept','sumFBCancel','sumFBClear'].forEach(id => {
    const b = document.getElementById(id);
    if (b) b.style.display = isReadOnly ? 'none' : '';
  });
  // Show Inspect button only for computed cols (c2-c5) when lineage exists for this row
  const isFx = el.dataset.fx === '1';
  const lineage = (window._sumLineage || {})[el.dataset.label];
  const inspBtn = document.getElementById('sumFBInspect');
  if (inspBtn) inspBtn.style.display = (isFx && lineage) ? '' : 'none';
  // Don't strip formatting on read-only cells (no editing happens there)
  if (!isReadOnly) el.value = el.dataset.raw || '';
}

// ── Summary inspector: render lineage drill-down for a c2-c5 cell ──
function sumRenderDrillPanel(label, col) {
  console.log('[inspector] render', {label: label, col: col, hasPanel: !!document.getElementById('sumDrillPanel'), hasLineageMap: !!window._sumLineage, lineageKeys: Object.keys(window._sumLineage||{}).length});
  const panel = document.getElementById('sumDrillPanel');
  if (!panel) { console.warn('[inspector] panel element not found'); return; }
  const lineage = (window._sumLineage || {})[label];
  if (!lineage) {
    // Show visible fallback so user sees something rather than silent failure
    panel.innerHTML = '<div style="color:#92400e;background:#fffbeb;padding:10px 12px;border-radius:6px;">No lineage data for "<b>' + label + '</b>". Available keys: ' + Object.keys(window._sumLineage||{}).slice(0,5).join(', ') + '\u2026</div>';
    panel.style.display = 'block';
    return;
  }
  const fmt = (n) => {
    if (n === null || n === undefined || isNaN(n)) return '\u2014';
    const r = Math.round(Number(n));
    return r < 0 ? '(' + Math.abs(r).toLocaleString('en-US') + ')' : r.toLocaleString('en-US');
  };
  const COL_TITLES = {c2:'Col 2 \u00b7 '+(typeof BY2!=='undefined'?BY2:'')+' Actual',
    c3:'Col 3 \u00b7 '+(typeof BY1!=='undefined'?BY1:'')+' YTD',
    c4:'Col 4 \u00b7 '+(typeof BY1!=='undefined'?BY1:'')+' Est.',
    c5:'Col 5 \u00b7 '+(typeof BY1!=='undefined'?BY1:'')+' Forecast'};
  let html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">' +
    '<div style="font-weight:700;color:#15803d;font-size:14px;">\ud83d\udd0d Inspector \u00b7 ' + label + ' \u2192 ' + (COL_TITLES[col]||col) + '</div>' +
    '<button onclick="document.getElementById(\'sumDrillPanel\').style.display=\'none\'" style="background:transparent;border:none;cursor:pointer;color:var(--gray-500);font-size:18px;line-height:1;">\u00d7</button>' +
    '</div>';

  if (col === 'c2') {
    const c2 = lineage.c2 || {};
    if (!c2.has_audit) {
      html += '<div style="background:#fffbeb;padding:10px 12px;border-radius:6px;color:#92400e;">No confirmed audited financials for FY ' + (c2.audit_year || '?') + '. Upload + confirm an audit on the Audited Financials tab to populate Col 2.</div>';
    } else if (c2.matched_category) {
      html += '<div style="display:grid;grid-template-columns:auto 1fr;gap:6px 14px;font-size:12px;margin-bottom:10px;">' +
        '<div style="color:var(--gray-500);">Source:</div><div><b>Confirmed Audited Financials</b> \u00b7 FY ' + (c2.audit_year || '?') + '</div>' +
        '<div style="color:var(--gray-500);">Matched category:</div><div><code style="background:var(--gray-100);padding:1px 6px;border-radius:3px;">' + c2.matched_category + '</code> <span style="color:var(--gray-500);font-size:11px;">(' + (c2.match_type||'') + ')</span></div>' +
        '<div style="color:var(--gray-500);">Confirmed by:</div><div>' + (c2.audit_confirmed_by || '\u2014') + ' on ' + (c2.audit_confirmed_at ? c2.audit_confirmed_at.slice(0,10) : '\u2014') + '</div>' +
        '<div style="color:var(--gray-500);">Source file:</div><div>' + (c2.audit_filename || '\u2014') + '</div>' +
        '</div>' +
        '<div style="background:#f0fdf4;border:1px solid #bbf7d0;padding:10px 12px;border-radius:6px;font-family:monospace;font-size:13px;">' +
        '<b>= ' + fmt(c2.value) + '</b> <span style="color:var(--gray-500);">(directly from audit total for "' + c2.matched_category + '")</span></div>';
    } else {
      html += '<div style="background:#fffbeb;padding:10px 12px;border-radius:6px;color:#92400e;">Audit is confirmed for FY ' + (c2.audit_year || '?') + ', but no category matched the row label "<b>' + label + '</b>". Add an alias in <code>_LABEL_ALIASES</code> (workflow.py).</div>';
    }
  } else {
    // Cols 3-5: GL aggregation breakdown
    const gl = lineage.gl || {};
    const allLines = gl.lines || [];
    const ytdM = gl.ytd_months || 0;
    const remM = gl.remaining_months || 0;
    // Filter out lines where the inspected column is zero
    const hi = (col === 'c3') ? 'ytd' : (col === 'c4') ? 'estimate' : 'forecast';
    const lines = allLines.filter(l => Math.round(Number(l[hi]) || 0) !== 0);
    const hiddenCount = allLines.length - lines.length;
    // Fixed-forecast override banner (Maintenance / Common Charges / Commercial Rent)
    const ff = lineage.fixed_forecast || {};
    if (ff.applied && (col === 'c4' || col === 'c5')) {
      const rowData = (window._sumRowMap || {})[label] || {};
      const c3v = Number(rowData.col3 || 0);
      const c5v = Number(rowData.col5 || 0);
      const c6v = Number(rowData.col6 || 0);
      const c4v = Number(rowData.col4 || 0);
      const formulaLine = (col === 'c5')
        ? 'Col 5 = Col 6 (Approved Budget) = <b>' + fmt(c6v) + '</b>'
        : 'Col 4 = Col 5 \u2212 Col 3 = ' + fmt(c5v) + ' \u2212 ' + fmt(c3v) + ' = <b>' + fmt(c4v) + '</b>';
      html += '<div style="background:#eff6ff;border:1px solid #bfdbfe;padding:10px 12px;border-radius:6px;margin-bottom:10px;font-size:12px;color:#1e40af;">' +
        '<div style="font-weight:700;margin-bottom:4px;">\ud83d\udccc Forecast pinned to Approved Budget</div>' +
        '<div>This row matches the Maintenance / Common Charges / Commercial Rent rule (GL 4010 / 4020 / 4030 / 4040). Forecast is locked to the approved budget rather than aggregated from YTD.</div>' +
        '<div style="margin-top:6px;font-family:monospace;">' + formulaLine + '</div>' +
        '<div style="margin-top:4px;color:#475569;">GL breakdown below is shown for reference only.</div>' +
        '</div>';
    }
    if (!lines.length) {
      html += '<div style="background:#f4f1eb;padding:10px 12px;border-radius:6px;color:var(--gray-700);">No GL prefixes mapped for this row, or no budget_lines data found. Map GL prefixes in the Budget Setup configuration to populate Cols 3-5.</div>';
    } else {
      const totalYtd = lines.reduce((s,l) => s + l.ytd, 0);
      const totalAcc = lines.reduce((s,l) => s + l.accrual, 0);
      const totalUnp = lines.reduce((s,l) => s + l.unpaid, 0);
      const totalEst = lines.reduce((s,l) => s + l.estimate, 0);
      const totalFc  = lines.reduce((s,l) => s + l.forecast, 0);
      html += '<div style="display:grid;grid-template-columns:auto 1fr;gap:6px 14px;font-size:12px;margin-bottom:10px;">' +
        '<div style="color:var(--gray-500);">Source:</div><div><b>Budget Lines</b> (GL aggregation)</div>' +
        '<div style="color:var(--gray-500);">GL prefixes:</div><div><code style="background:var(--gray-100);padding:1px 6px;border-radius:3px;">' + (gl.prefixes || []).join(', ') + '</code></div>' +
        '<div style="color:var(--gray-500);">YTD period:</div><div>' + ytdM + ' months actual + ' + remM + ' months projected</div>' +
        '</div>';
      // Math box
      const mathLabel = col === 'c3' ? 'YTD Actual' : col === 'c4' ? 'Estimate (remaining ' + remM + ' months)' : 'Forecast (YTD + Estimate)';
      const mathFormula = col === 'c3' ? '\u03a3 ytd_actual' :
                          col === 'c4' ? '(\u03a3 ytd / ' + ytdM + ') \u00d7 ' + remM :
                          '\u03a3 (ytd + accrual + unpaid + estimate)';
      const mathTotal   = col === 'c3' ? totalYtd : col === 'c4' ? totalEst : totalFc;
      html += '<div style="background:#f0fdf4;border:1px solid #bbf7d0;padding:10px 12px;border-radius:6px;font-family:monospace;font-size:13px;margin-bottom:10px;">' +
        '<div style="color:var(--gray-500);font-size:11px;text-transform:uppercase;letter-spacing:0.3px;margin-bottom:4px;">' + mathLabel + '</div>' +
        '<b>' + mathFormula + ' = ' + fmt(mathTotal) + '</b>' +
        '</div>';
      // Per-line table
      html += '<table style="width:100%;border-collapse:collapse;font-size:12px;">' +
        '<thead><tr style="background:var(--gray-100);color:var(--gray-700);">' +
        '<th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--gray-200);">GL</th>' +
        '<th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Description</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">YTD Actual</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Accrual</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Unpaid</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Estimate</th>' +
        '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-200);">Forecast</th>' +
        '</tr></thead><tbody>';
      lines.forEach(l => {
        const cell = (key) => {
          const v = l[key];
          const bg = (key === hi) ? 'background:#f0fdf4;font-weight:700;color:#15803d;' : '';
          return '<td style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--gray-100);font-variant-numeric:tabular-nums;'+bg+'">' + fmt(v) + '</td>';
        };
        html += '<tr>' +
          '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);font-family:monospace;">' + (l.gl||'') + '</td>' +
          '<td style="padding:6px 8px;border-bottom:1px solid var(--gray-100);color:var(--gray-700);">' + (l.desc||'') + '</td>' +
          cell('ytd') + cell('accrual') + cell('unpaid') + cell('estimate') + cell('forecast') +
          '</tr>';
      });
      const tcell = (key, val) => {
        const bg = (key === hi) ? 'background:#f0fdf4;color:#15803d;font-weight:700;' : 'color:var(--gray-500);';
        return '<td style="text-align:right;padding:6px 8px;border-top:2px solid var(--gray-300);font-variant-numeric:tabular-nums;'+bg+'">' + fmt(val) + '</td>';
      };
      const hiddenNote = hiddenCount > 0 ? ' <span style="color:var(--gray-400);font-weight:400;font-size:11px;">(' + hiddenCount + ' zero hidden)</span>' : '';
      html += '<tr style="background:var(--gray-50);">' +
        '<td colspan="2" style="padding:6px 8px;border-top:2px solid var(--gray-300);color:var(--gray-500);">Total (' + lines.length + ' lines)' + hiddenNote + '</td>' +
        tcell('ytd', totalYtd) + tcell('accrual', totalAcc) + tcell('unpaid', totalUnp) +
        tcell('estimate', totalEst) + tcell('forecast', totalFc) +
        '</tr>';
      html += '</tbody></table>';
    }
  }
  panel.innerHTML = html;
  panel.style.display = 'block';
}

function sumCellBlur(el) {
  if (el.value && !el.value.startsWith('=')) {
    const num = parseFloat(el.value.replace(/,/g, ''));
    if (!isNaN(num)) { el.dataset.raw = num; el.value = num.toLocaleString('en-US', {maximumFractionDigits:0}); el.style.background = el.dataset.col === 'c7' ? '#fffbeb' : '#fbfaf4'; }
  } else if (el.value.startsWith('=')) {
    try { const r = Function('"use strict"; return (' + el.value.slice(1) + ')')(); el.dataset.raw = r; el.style.background = '#f0fdf4'; el.style.borderColor = '#bbf7d0'; } catch(e) {}
  } else { el.dataset.raw = ''; }
  sumRecalcTotals();
  // Auto-save col7 edits
  if (el.dataset.col === 'c7' && el.dataset.raw) {
    const tr = el.closest('tr');
    const order = tr ? tr.dataset.order : null;
    if (order) {
      fetch('/api/summary/' + entityCode, {method:'PUT', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({edits:[{display_order:parseInt(order), col7:parseFloat(el.dataset.raw)}]})
      }).catch(()=>{});
    }
  }
}

function sumCellKey(e, el) {
  if (e.key === 'Enter') { el.blur(); sumAcceptFormula(); }
  else if (e.key === 'Escape') { sumCancelFormula(); }
}

function sumAcceptFormula() {
  if (!_sumActiveCell) return;
  const val = document.getElementById('sumFBInput').value;
  if (val.startsWith('=')) {
    try {
      const r = Function('"use strict"; return (' + val.slice(1) + ')')();
      _sumActiveCell.dataset.raw = r;
      _sumActiveCell.value = Math.round(r).toLocaleString('en-US');
      _sumActiveCell.style.background = '#f0fdf4'; _sumActiveCell.style.borderColor = '#bbf7d0';
      document.getElementById('sumFBPreview').textContent = '= ' + Math.round(r).toLocaleString('en-US');
    } catch(e) { document.getElementById('sumFBPreview').textContent = '\u26a0 Error'; }
  }
  sumRecalcTotals();
  sumResetBar();
}

function sumCancelFormula() {
  if (_sumActiveCell) {
    _sumActiveCell.value = _sumActiveCell.dataset.raw ? parseFloat(_sumActiveCell.dataset.raw).toLocaleString('en-US',{maximumFractionDigits:0}) : '';
  }
  sumResetBar();
}

function sumResetBar() {
  const bar = document.getElementById('sumFBar');
  if (bar) bar.style.borderColor = 'var(--gray-200)';
  const inp = document.getElementById('sumFBInput');
  if (inp) { inp.disabled = true; inp.value = ''; inp.style.opacity = '1'; }
  const lbl = document.getElementById('sumFBLabel');
  if (lbl) lbl.textContent = 'Click a cell\u2026';
  const prev = document.getElementById('sumFBPreview');
  if (prev) prev.textContent = '';
  ['sumFBAccept','sumFBCancel','sumFBClear','sumFBInspect'].forEach(id => { const b = document.getElementById(id); if(b) b.style.display='none'; });
  _sumActiveCell = null;
}

// Wire formula bar buttons (called after render)
document.addEventListener('click', function(e) {
  if (e.target.id === 'sumFBAccept') sumAcceptFormula();
  if (e.target.id === 'sumFBCancel') sumCancelFormula();
  if (e.target.id === 'sumFBInspect' || (e.target.closest && e.target.closest('#sumFBInspect'))) {
    console.log('[inspector] Inspect clicked, _sumActiveCell:', _sumActiveCell);
    try {
      if (_sumActiveCell) {
        sumRenderDrillPanel(_sumActiveCell.dataset.label, _sumActiveCell.dataset.col);
      } else {
        const panel = document.getElementById('sumDrillPanel');
        if (panel) {
          panel.innerHTML = '<div style="color:#92400e;background:#fffbeb;padding:10px 12px;border-radius:6px;">No active cell. Click a Col 2-5 cell first, then click Inspect.</div>';
          panel.style.display = 'block';
        }
      }
    } catch (err) {
      console.error('[inspector] render error', err);
      const panel = document.getElementById('sumDrillPanel');
      if (panel) {
        panel.innerHTML = '<div style="color:#991b1b;background:#fee2e2;padding:10px 12px;border-radius:6px;">Inspector error: ' + (err.message || err) + '</div>';
        panel.style.display = 'block';
      }
    }
    return;
  }
  if (e.target.id === 'sumFBClear') {
    if (_sumActiveCell) { _sumActiveCell.value=''; _sumActiveCell.dataset.raw=''; _sumActiveCell.style.background=_sumActiveCell.dataset.col==='c7'?'#fffbeb':'#fbfaf4'; _sumActiveCell.style.borderColor='var(--gray-300)'; }
    sumRecalcTotals(); sumResetBar();
  }
});
document.addEventListener('input', function(e) {
  if (e.target.id === 'sumFBInput') {
    if (_sumActiveCell) _sumActiveCell.value = e.target.value;
    if (e.target.value.startsWith('=')) {
      try { const r = Function('"use strict"; return (' + e.target.value.slice(1) + ')')(); document.getElementById('sumFBPreview').textContent = '= ' + Math.round(r).toLocaleString('en-US'); } catch(ex) { document.getElementById('sumFBPreview').textContent = ''; }
    } else { const p = document.getElementById('sumFBPreview'); if(p) p.textContent = ''; }
  }
});

// ── Summary tab: insert row ──
function sumShowInsert(secKey, secLabel) {
  let modal = document.getElementById('sumInsertModal');
  let overlay = document.getElementById('sumInsertOverlay');
  if (!modal) {
    overlay = document.createElement('div'); overlay.id = 'sumInsertOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.3);z-index:99;';
    overlay.onclick = sumCloseInsert;
    document.body.appendChild(overlay);
    modal = document.createElement('div'); modal.id = 'sumInsertModal';
    modal.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:white;border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,0.2);padding:24px;z-index:100;width:380px;';
    document.body.appendChild(modal);
  }
  overlay.style.display = 'block'; modal.style.display = 'block';
  modal.innerHTML = '<h3 style="font-size:15px;font-weight:700;color:var(--blue-dark);margin-bottom:12px;">Add Row to '+secLabel+'</h3>' +
    '<label style="display:block;font-size:12px;font-weight:600;color:var(--gray-600);margin-bottom:4px;">Line Item Name</label>' +
    '<input id="sumInsLabel" type="text" placeholder="e.g. Lobby Renovation" style="width:100%;padding:8px 10px;border:1px solid var(--gray-200);border-radius:6px;font-size:13px;font-family:inherit;">' +
    '<label style="display:block;font-size:12px;font-weight:600;color:var(--gray-600);margin-bottom:4px;margin-top:10px;">Source Tab</label>' +
    '<select id="sumInsTab" style="width:100%;padding:8px 10px;border:1px solid var(--gray-200);border-radius:6px;font-size:13px;font-family:inherit;">' +
    '<option value="Manual">Manual</option><option value="Income">Income</option><option value="Payroll">Payroll</option>' +
    '<option value="Energy">Energy</option><option value="Water & Sewer">Water & Sewer</option>' +
    '<option value="Repairs & Supplies">Repairs & Supplies</option><option value="Gen & Admin">Gen & Admin</option>' +
    '<option value="RE Taxes">RE Taxes</option></select>' +
    '<div style="display:flex;gap:8px;margin-top:16px;justify-content:flex-end;">' +
    '<button onclick="sumCloseInsert()" style="padding:6px 16px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:1px solid var(--gray-200);background:white;">Cancel</button>' +
    '<button onclick="sumDoInsert(\''+secKey+'\')" style="padding:6px 16px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:none;background:var(--blue);color:white;">Add Row</button></div>';
  setTimeout(() => document.getElementById('sumInsLabel').focus(), 50);
}

function sumCloseInsert() {
  const m = document.getElementById('sumInsertModal');
  const o = document.getElementById('sumInsertOverlay');
  if (m) m.style.display = 'none';
  if (o) o.style.display = 'none';
}

function sumDoInsert(secKey) {
  const label = document.getElementById('sumInsLabel').value.trim();
  if (!label) return;
  const tab = document.getElementById('sumInsTab').value;
  const tbody = document.getElementById('sumBody');
  const subRow = tbody.querySelector('tr[data-sums="'+secKey+'"]') || tbody.querySelector('tr[data-calc]');
  if (!subRow) { sumCloseInsert(); return; }

  const SUM_TAB_COLORS = {"Income":{bg:"rgba(76,175,80,0.15)",color:"#2e7d32"},"Payroll":{bg:"rgba(33,150,243,0.15)",color:"#1565c0"},"Energy":{bg:"rgba(255,152,0,0.15)",color:"#e65100"},"Water & Sewer":{bg:"rgba(0,188,212,0.15)",color:"#00838f"},"Repairs & Supplies":{bg:"rgba(121,85,72,0.15)",color:"#5d4037"},"Gen & Admin":{bg:"rgba(156,39,176,0.15)",color:"#7b1fa2"},"RE Taxes":{bg:"rgba(244,67,54,0.15)",color:"#c62828"},"Manual":{bg:"rgba(255,213,79,0.15)",color:"#f57f17"}};
  const SUM_TAB_SHORT = {"Income":"Income","Payroll":"Payroll","Energy":"Energy","Water & Sewer":"Water","Repairs & Supplies":"R&S","Gen & Admin":"Gen&Admin","RE Taxes":"RE Tax","Manual":"Manual"};
  const tc = SUM_TAB_COLORS[tab]||{bg:'rgba(158,158,158,0.15)',color:'#757575'};
  const chipH = '<span style="display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:600;background:'+tc.bg+';color:'+tc.color+'">'+(SUM_TAB_SHORT[tab]||tab)+'</span>';

  function mkIn(col, bg) {
    return '<td style="text-align:right;padding:4px 6px;background:'+(bg||'#fbfaf4')+';border-bottom:1px solid var(--gray-200);">' +
      '<input type="text" placeholder="\u2014" data-label="'+label.replace(/"/g,'&quot;')+'" data-col="'+col+'" data-raw="" ' +
      'onfocus="sumCellFocus(this)" onblur="sumCellBlur(this)" onkeydown="sumCellKey(event,this)" ' +
      'style="width:100px;padding:5px 8px;border:1px solid var(--gray-300);border-radius:4px;font-size:13px;text-align:right;background:'+(bg||'#fbfaf4')+';font-variant-numeric:tabular-nums;font-family:inherit;"></td>';
  }

  const tr = document.createElement('tr');
  tr.dataset.sec = secKey; tr.dataset.type = 'd';
  tr.innerHTML = '<td style="padding:8px 10px;border-bottom:1px solid var(--gray-200);position:sticky;left:0;z-index:15;background:white;min-width:200px;max-width:240px;border-right:2px solid var(--gray-300);box-shadow:2px 0 8px rgba(90,74,63,0.08);">'+label+' <span style="color:var(--yellow);font-size:10px;font-weight:700;">NEW</span></td>' +
    '<td style="text-align:right;padding:8px 10px;border-bottom:1px solid var(--gray-200);">'+chipH+'</td>' +
    mkIn('c1','#fbfaf4')+mkIn('c2','#f9f9f7')+mkIn('c3','#f9f9f7')+mkIn('c4','#f9f9f7')+mkIn('c5','#f9f9f7')+mkIn('c6','#fbfaf4')+mkIn('c7','#fffbeb') +
    '<td style="text-align:right;padding:8px 10px;border-bottom:1px solid var(--gray-200);color:var(--gray-400);">\u2014</td>' +
    '<td style="padding:4px 6px;border-bottom:1px solid var(--gray-200);"><input type="text" placeholder="Add note\u2026" style="width:100%;padding:5px 8px;border:1px solid var(--gray-200);border-radius:4px;font-size:12px;background:white;font-family:inherit;"></td>';
  subRow.parentNode.insertBefore(tr, subRow);
  sumCloseInsert();
  sumRecalcTotals();
}

function renderReadOnlySheet(sheetName, sheetLines, contentDiv) {
  const thStyle = 'text-align:right; padding:8px; white-space:nowrap;';
  let html = '<table style="width:100%; border-collapse:collapse; font-size:13px;">' +
    '<thead><tr style="background:var(--gray-100); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500);">' +
    '<th style="text-align:left; padding:8px;">GL Code</th>' +
    '<th style="text-align:left; padding:8px;">Description</th>' +
    '<th style="' + thStyle + '">Prior Year<br>Actual</th>' +
    '<th style="' + thStyle + '">YTD<br>Actual</th>' +
    '<th style="' + thStyle + '">Approved<br>Budget</th>' +
    '<th style="' + thStyle + '">Variance</th>' +
    '</tr></thead><tbody>';

  let totals = {prior:0, ytd:0, budget:0};
  sheetLines.forEach(l => {
    const prior = l.prior_year || 0;
    const ytd = l.ytd_actual || 0;
    const budget = l.current_budget || 0;
    const variance = budget - prior;
    totals.prior += prior; totals.ytd += ytd; totals.budget += budget;
    const varColor = variance >= 0 ? 'var(--red)' : 'var(--green)';

    html += '<tr style="border-bottom:1px solid var(--gray-100);">' +
      '<td style="font-family:monospace; font-size:12px; padding:6px 8px;">' + l.gl_code + '</td>' +
      '<td style="padding:6px 8px;">' + l.description + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(prior) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(ytd) + '</td>' +
      '<td style="text-align:right; padding:6px 8px;">' + fmt(budget) + '</td>' +
      '<td style="text-align:right; padding:6px 8px; color:' + varColor + ';">' + fmt(variance) + '</td></tr>';
  });

  const totalVar = totals.budget - totals.prior;
  html += '<tr style="font-weight:700; background:var(--gray-100);"><td style="padding:8px;" colspan="2">Sheet Total</td>' +
    '<td style="text-align:right; padding:8px;">' + fmt(totals.prior) + '</td>' +
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
    noData.innerHTML = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="12" style="padding:0;"><div class="drill-sticky" style="padding:12px 24px; background:#fef3c7; font-size:13px;">No expense data uploaded yet.</div></td>';
    row.after(noData);
    return;
  }

  const glGroup = data.gl_groups.find(g => g.gl_code === glCode);
  if (!glGroup || !glGroup.invoices || glGroup.invoices.length === 0) {
    const noInv = document.createElement('tr');
    noInv.className = 'fa-invoice-detail';
    noInv.innerHTML = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="12" style="padding:0;"><div class="drill-sticky" style="padding:12px 24px; background:var(--gray-50); font-size:13px; color:var(--gray-500);">No invoices for ' + glCode + '</div></td>';
    row.after(noInv);
    return;
  }

  const detailRow = document.createElement('tr');
  detailRow.className = 'fa-invoice-detail';
  let html = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="12" style="padding:0;"><div class="drill-sticky" style="padding:12px 16px 12px 24px; background:linear-gradient(to right, #f0f4ff, #f8faff); border-left:3px solid var(--blue); border-bottom:1px solid var(--gray-200);">';
  html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">';
  html += '<span style="font-weight:600; font-size:13px; color:var(--blue);">' + glCode + ' — ' + (glGroup.gl_name || '') + '</span>';
  html += '<span style="font-size:12px; color:var(--gray-500);">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' · $' + Math.round(glGroup.total || 0).toLocaleString() + '</span>';
  html += '</div>';
  html += '<table style="width:auto; font-size:12px; border-collapse:separate; border-spacing:0; background:white; border-radius:6px; box-shadow:0 1px 2px rgba(0,0,0,0.05); overflow:hidden;">';
  html += '<thead><tr style="background:var(--gray-100); color:var(--gray-600); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
  html += '<td style="padding:7px 16px; min-width:140px; border-bottom:2px solid var(--gray-300);">Payee</td><td style="padding:7px 16px; min-width:140px; border-bottom:2px solid var(--gray-300);">Description</td><td style="padding:7px 16px; min-width:70px; border-bottom:2px solid var(--gray-300);">Inv #</td><td style="padding:7px 16px; min-width:85px; border-bottom:2px solid var(--gray-300);">Date</td><td style="padding:7px 16px; min-width:100px; text-align:right; border-bottom:2px solid var(--gray-300);">Amount</td><td style="padding:7px 16px; min-width:90px; border-bottom:2px solid var(--gray-300);">Check #</td><td style="padding:7px 16px; min-width:90px; text-align:center; border-bottom:2px solid var(--gray-300);">Action</td></tr></thead>';

  glGroup.invoices.forEach(inv => {
    const isReclassed = !!inv.reclass_to_gl;
    html += '<tr style="border-top:1px solid var(--gray-200);' + (isReclassed ? ' opacity:0.5; text-decoration:line-through;' : '') + '">';
    html += '<td style="padding:7px 16px; font-size:12px; white-space:nowrap; border-bottom:1px solid var(--gray-200);">' + (inv.payee_name || inv.payee_code || '—') + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; color:var(--gray-600); border-bottom:1px solid var(--gray-200);">' + (inv.notes || '—') + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; font-family:monospace; border-bottom:1px solid var(--gray-200);">' + (inv.invoice_num || '—') + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; border-bottom:1px solid var(--gray-200);">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; text-align:right; font-size:12px; font-weight:600; font-variant-numeric:tabular-nums; border-bottom:1px solid var(--gray-200);">$' + Math.round(inv.amount).toLocaleString() + '</td>';
    html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; border-bottom:1px solid var(--gray-200);">' + (inv.check_num || '—') + '</td>';
    html += '<td style="padding:7px 16px; text-align:center; border-bottom:1px solid var(--gray-200);">';
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

// ═══════════════════════════════════════════════════════════════════════════
// PAYROLL TAB — Enhanced with Assumptions, Roster Calc, GL Grouping
// ═══════════════════════════════════════════════════════════════════════════

let _payrollAssumptions = {};
let _payrollPositions = [];
let _payrollGLLines = [];

// ── Payroll Zero-Row Toggle (mirrors faShowZeroRows pattern) ─────────────
let _prShowZeroRows = false;

function prUpdateZeroToggleBtn() {
  const btn = document.getElementById('prZeroToggle');
  if (!btn) return;
  const count = document.querySelectorAll('#prGLContent .prgl-zero-row').length;
  if (count === 0) { btn.style.display = 'none'; return; }
  btn.style.display = '';
  btn.textContent = _prShowZeroRows ? 'Hide ' + count + ' Zero Rows' : 'Show ' + count + ' Hidden Rows';
  btn.style.background = _prShowZeroRows ? 'var(--gray-200)' : 'var(--blue-light, #dbeafe)';
  btn.style.color = _prShowZeroRows ? 'var(--gray-600)' : 'var(--blue)';
  btn.style.borderColor = _prShowZeroRows ? 'var(--gray-300)' : 'var(--blue)';
}

function prToggleZeroRows(ev) {
  if (ev) ev.stopPropagation();
  _prShowZeroRows = !_prShowZeroRows;
  document.querySelectorAll('#prGLContent .prgl-zero-row').forEach(row => {
    row.classList.toggle('prgl-zero-show', _prShowZeroRows);
  });
  prUpdateZeroToggleBtn();
}

// True if a Payroll GL line is "all zero" — matches bpIsZero but without
// the accrual_adj/unpaid_bills terms (those are no longer shown on Payroll).
function prGlIsZero(l) {
  return !l.prior_year && !l.ytd_actual && !l.current_budget && !l.increase_pct
    && (l.estimate_override === null || l.estimate_override === undefined)
    && (l.forecast_override === null || l.forecast_override === undefined)
    && !l.proposed_budget && !l.proposed_formula;
}

// Auto-size editable cells inside the payroll tab to fit their values.
// Sets the HTML `size` attribute (character count) on every input.
function prAutoSizeAll() {
  const sel = '#prGLContent input.cell, #prGLContent input.cell-fx, #prGLContent input.cell-pct, #prRosterTable input';
  document.querySelectorAll(sel).forEach(el => {
    const v = el.value || '';
    el.size = Math.max(2, v.length + 1);
  });
}

// Payroll-only compute helpers — no accrual/unpaid in the math.
// Kept local so renderFASheet (other tabs) keeps using faComputeEstimate/Forecast.
function prFaComputeEstimate(l) {
  if (l.estimate_override !== null && l.estimate_override !== undefined) return l.estimate_override;
  if (typeof faIsFixedToBudget === 'function' && faIsFixedToBudget(l)) {
    return (l.current_budget || 0) - (l.ytd_actual || 0);
  }
  const ytd = l.ytd_actual || 0;
  if (typeof YTD_MONTHS !== 'undefined' && YTD_MONTHS > 0) {
    return (ytd / YTD_MONTHS) * REMAINING_MONTHS;
  }
  return 0;
}
function prFaComputeForecast(l) {
  if (l.forecast_override !== null && l.forecast_override !== undefined) return l.forecast_override;
  if (typeof faIsFixedToBudget === 'function' && faIsFixedToBudget(l)) {
    return l.current_budget || 0;
  }
  return (l.ytd_actual || 0) + prFaComputeEstimate(l);
}

async function renderPayrollTab(sheetLines, contentDiv) {
  _payrollGLLines = sheetLines || [];
  const ec = entityCode;

  // Load assumptions and positions in parallel
  const [aResp, pResp] = await Promise.all([
    fetch('/api/payroll/assumptions/' + ec).then(r => r.json()),
    fetch('/api/payroll/positions/' + ec).then(r => r.json())
  ]);
  _payrollAssumptions = aResp.assumptions || {};
  _payrollPositions = pResp || [];

  // If no positions saved yet, seed with 2 placeholder rows
  if (_payrollPositions.length === 0) {
    _payrollPositions = [
      {position_name: 'Resident Manager', employee_count: 0, hourly_rate: 0, bonus_per_employee: 0, effective_week_override: null, sort_order: 0},
      {position_name: 'Handyman', employee_count: 0, hourly_rate: 0, bonus_per_employee: 0, effective_week_override: null, sort_order: 1}
    ];
  }

  const a = _payrollAssumptions;
  const fmtD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const fmtPct = v => (v * 100).toFixed(2) + '%';
  const fmtPctInput = v => (v * 100).toFixed(3);

  // Scrollable wrapper so sticky formula bar has a scroll context (matches R&S behavior)
  let html = '<div style="max-width:100%; margin:0 auto; max-height:calc(100vh - 220px); overflow-y:auto; padding-right:8px;">';

  // Inject Payroll-specific CSS — FA design language (.fa-grid tokens),
  // scoped to #prGLContent and #prRosterTable so other tabs are unaffected.
  html += '<style>' +
    // ── GL Detail: .fa-grid parity (frozen GL Code + Description, navy total, cream cat-hdr) ──
    '#prGLContent { background:white; border-radius:10px; border:1px solid var(--gray-200); overflow:hidden; }' +
    '#prGLContent .prgl-scroll { overflow-x:auto; max-height:75vh; overflow-y:auto; }' +
    '#prGLContent .prgl-scroll::-webkit-scrollbar { width:10px; height:12px; }' +
    '#prGLContent .prgl-scroll::-webkit-scrollbar-track { background:var(--gray-100); border-radius:6px; }' +
    '#prGLContent .prgl-scroll::-webkit-scrollbar-thumb { background:#8b7355; border-radius:6px; min-height:40px; }' +
    '#prGLContent .prgl-scroll::-webkit-scrollbar-thumb:hover { background:#6b5740; }' +
    '#prGLContent table { border-collapse:separate; border-spacing:0; font-size:13px; width:100%; }' +
    '#prGLContent thead { position:sticky; top:0; z-index:20; }' +
    '#prGLContent th { padding:8px 8px; text-align:left; font-weight:600; border-bottom:2px solid var(--gray-300); white-space:nowrap; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); background:var(--gray-100); }' +
    '#prGLContent th.num { text-align:right; }' +
    '#prGLContent td, #prGLContent th { white-space:nowrap; width:1px; }' +
    '#prGLContent td { padding:6px 8px; border-bottom:1px solid var(--gray-200); }' +
    '#prGLContent td.num { text-align:right; font-variant-numeric:tabular-nums; position:relative; }' +
    // num-box: wrap class that mirrors .cell input outer dimensions exactly.
    // Wrapping plain text in <span class="num-box"> aligns its right edge with
    // input.cell text inside body rows, regardless of browser quirks.
    '#prGLContent .num-box { display:inline-block; padding:4px 6px; border:1px solid transparent; box-sizing:content-box; text-align:right; font-variant-numeric:tabular-nums; font-family:inherit; font-size:inherit; line-height:inherit; min-width:50px; }' +
    '#prGLContent tbody tr:hover td { background:#eef2ff; }' +
    '#prGLContent tbody tr:hover td.frozen { background:#ede5d8; }' +
    '#prGLContent th.frozen, #prGLContent td.frozen { position:sticky; z-index:15; background:white; }' +
    '#prGLContent thead th.frozen { z-index:25; background:var(--gray-100); }' +
    '#prGLContent .frozen-gl { left:0; width:115px; min-width:115px; max-width:115px; }' +
    '#prGLContent .frozen-desc { left:115px; width:240px; min-width:240px; max-width:240px; border-right:2px solid var(--gray-300); box-shadow:2px 0 8px rgba(90,74,63,0.08); }' +
    '#prGLContent .cat-hdr td { background:#f5efe7; font-weight:700; color:#5a4a3f; font-size:14px; padding:10px 12px; border-bottom:2px solid #5a4a3f; cursor:pointer; user-select:none; }' +
    '#prGLContent .sub-row td { background:var(--gray-100); font-weight:700; border-top:2px solid var(--gray-300); }' +
    '#prGLContent .sub-row td.frozen { background:var(--gray-100); }' +
    '#prGLContent .total-row td { background:#1e3a5f; color:white; font-weight:700; font-size:14px; padding:10px 8px; }' +
    '#prGLContent .total-row td.frozen { background:#1e3a5f; color:white; }' +
    // Match R&S/Gen&Admin/.fa-grid cell visual: bordered cream box for editable
    // cells, transparent bg with green left bar for fx (formula) cells. Keeps
    // every numeric cell visually distinct and easy to scan. font:inherit avoids
    // browser-default font on form controls.
    '#prGLContent .cell { min-width:50px; width:auto; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font:inherit; font-size:13px; text-align:right; background:#fbfaf4; cursor:text; font-variant-numeric:tabular-nums; box-sizing:content-box; line-height:inherit; }' +
    '#prGLContent .cell:hover { border-color:#a8a29e; }' +
    '#prGLContent .cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px #e1effe; }' +
    // Formula cells: transparent bg, subtle border, green inset left bar as
    // the "formula" indicator (matches .fa-grid .cell-fx).
    '#prGLContent .cell-fx { background:transparent; border-color:#e5e1d8; box-shadow:inset 3px 0 0 #16a34a; color:#15803d; }' +
    '#prGLContent .cell-fx:hover { border-color:#a8a29e; }' +
    '#prGLContent .cell-fx:focus { background:#ecfdf5; }' +
    '#prGLContent .cell-fx-linked { background:#eff6ff !important; border-color:transparent !important; box-shadow:inset 3px 0 0 #2563eb !important; color:#1e40af !important; font-weight:700; }' +
    '#prGLContent .cell-fx-linked:hover { border-color:#93c5fd !important; }' +
    '#prGLContent .cell-pct { width:auto; min-width:45px; font:inherit; font-size:13px; font-variant-numeric:tabular-nums; }' +
    '#prGLContent .cell-pct[disabled] { background:#fbfaf4; color:#6b7280; cursor:not-allowed; opacity:1; -webkit-text-fill-color:#6b7280; }' +
    '#prGLContent .cell-notes { text-align:left; min-width:120px; width:auto; font-size:12px; background:white; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-family:inherit; }' +
    '#prGLContent .fa-fx { display:none !important; }' +
    '#prGLContent tr.prgl-zero-row { display:none; }' +
    '#prGLContent tr.prgl-zero-row.prgl-zero-show { display:table-row; }' +
    // ── Roster: FA tokens (gray-100 header, cream inputs, gray-200 borders) ──
    '#prRosterTable { width:100%; border-collapse:separate; border-spacing:0; font-size:13px; }' +
    '#prRosterTable thead th { padding:8px 8px; font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); background:var(--gray-100); border-bottom:2px solid var(--gray-300); white-space:nowrap; text-align:left; }' +
    '#prRosterTable thead th.r { text-align:right; }' +
    '#prRosterTable tbody td { padding:6px 8px; border-bottom:1px solid var(--gray-200); font-size:13px; font-variant-numeric:tabular-nums; }' +
    '#prRosterTable tbody tr:hover td { background:#eef2ff; }' +
    '#prRosterTable input { padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; background:#fbfaf4; font-size:13px; font-family:inherit; font-variant-numeric:tabular-nums; box-sizing:content-box; text-align:right; }' +
    '#prRosterTable input:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px #e1effe; }' +
    '#prRosterTable .pr-pos-name { text-align:left; }' +
    '#prRosterTable th.filler, #prRosterTable td.filler { width:100%; padding:0 !important; background:transparent; }' +
    '#prRosterTable tfoot td { padding:8px 8px; border-top:2px solid var(--gray-300); border-bottom:2px solid var(--gray-200); background:var(--gray-100); font-weight:700; font-variant-numeric:tabular-nums; }' +
    '</style>';

  // Formula bar — Excel-style with live preview + Accept/Cancel (same as other tabs)
  // Sticky positioning so it stays visible as user scrolls through GL detail
  html += '<div id="faFormulaBarWrap" style="display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200); border-radius:8px; margin-bottom:12px; position:sticky; top:0; z-index:50; box-shadow:0 2px 4px rgba(0,0,0,0.04);">' +
    '<span style="font-size:11px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>' +
    '<span id="faFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:var(--gray-600); white-space:nowrap; min-width:100px;"></span>' +
    '<input id="faFormulaBar" type="text" placeholder="Click a green formula cell to view its formula..." style="display:block; flex:1; padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:monospace; background:white;" oninput="formulaBarPreview()" onkeydown="formulaBarKeydown(event)">' +
    '<span id="faFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:var(--green); white-space:nowrap; min-width:80px; text-align:right;"></span>' +
    '<button id="faFormulaAccept" style="display:none; padding:4px 14px; font-size:12px; font-weight:600; background:var(--green); color:white; border:none; border-radius:4px; cursor:pointer;" onclick="formulaBarAccept()">Accept</button>' +
    '<button id="faFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:var(--gray-200); color:var(--gray-700); border:none; border-radius:4px; cursor:pointer;" onclick="formulaBarCancel()">Cancel</button>' +
    '<button id="faFormulaClear" style="display:none; padding:4px 10px; font-size:11px; background:#fef2f2; color:var(--red); border:1px solid #fecaca; border-radius:4px; cursor:pointer;" onclick="formulaBarClear()" title="Remove formula, revert to auto-calc">Clear</button>' +
    '</div>';

  // ── Section 0: Payroll Assumptions (Editable) ──────────────────────────
  html += `
  <div id="payrollAssumptionsSection" style="background:white; border-radius:10px; border:1px solid var(--gray-200); margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
    <div onclick="togglePayrollSection('prAssump')" style="display:flex; align-items:center; justify-content:space-between; padding:12px 20px; background:#f5efe7; border-bottom:1px solid #e5e0d5; border-radius:10px 10px 0 0; cursor:pointer; user-select:none;">
      <h3 style="font-size:13px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin:0;">Payroll Assumptions <span style="font-size:9px; font-weight:800; color:white; background:#5a4a3f; border-radius:3px; padding:1px 5px; margin-left:6px; vertical-align:middle;">EDITABLE</span></h3>
      <div style="display:flex; align-items:center; gap:12px;">
        <span style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#f5efe7; color:#5a4a3f; border:1px solid #d5cfc5;">Changes flow through all sections below</span>
        <span style="font-size:12px; color:var(--gray-400);" id="prAssumpChev">▾</span>
      </div>
    </div>
    <div id="prAssump">
      <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:0;">

        <!-- Column 1: Wage & Schedule -->
        <div style="padding:14px 20px; border-right:1px solid var(--gray-200);">
          <div style="font-size:10px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:10px; padding-bottom:4px; border-bottom:1px solid #f5efe7;">Wage & Schedule</div>
          ${prAssumpRow('Wage Increase %', 'wage_increase_pct', fmtPctInput(a.wage_increase_pct || 0), '%')}
          ${prAssumpRow('Effective Week', 'effective_week', a.effective_week || '16', '')}
          ${prAssumpRow('Pre-Incr Weeks', 'pre_increase_weeks', a.pre_increase_weeks || 15, '')}
          ${prAssumpRow('Post-Incr Weeks', 'post_increase_weeks', a.post_increase_weeks || 37, '')}
          ${prAssumpRow('OT Factor %', 'ot_factor', ((a.ot_factor || 0.002) * 100).toFixed(1), '%')}
          ${prAssumpRow('Vac/Sick/Hol %', 'vac_sick_hol_factor', ((a.vac_sick_hol_factor || 0.10) * 100).toFixed(1), '%')}
          <div style="margin-top:8px; font-size:10px; color:var(--gray-400); font-style:italic; padding-top:6px; border-top:1px dashed var(--gray-200);">Changing Effective Week auto-updates Pre/Post weeks — you can also edit them directly</div>
        </div>

        <!-- Column 2: Payroll Tax Rates -->
        <div style="padding:14px 20px; border-right:1px solid var(--gray-200);">
          <div style="font-size:10px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:10px; padding-bottom:4px; border-bottom:1px solid #f5efe7;">Payroll Tax Rates</div>
          ${prAssumpRow('FICA', 'fica', fmtPctInput(a.fica || 0), '%')}
          ${prAssumpRow('SUI', 'sui', fmtPctInput(a.sui || 0), '%')}
          ${prAssumpRow('FUI', 'fui', fmtPctInput(a.fui || 0), '%')}
          ${prAssumpRow('MTA', 'mta', fmtPctInput(a.mta || 0), '%')}
          ${prAssumpRow('NYS Disability', 'nys_disability', fmtPctInput(a.nys_disability || 0), '%')}
          ${prAssumpRow('Paid Family Leave', 'pfl', fmtPctInput(a.pfl || 0), '%')}
          ${prAssumpRow('Workers Comp', 'workers_comp', fmtPctInput(a.workers_comp || 0), '%')}
          <div style="margin-top:8px; font-size:10px; color:var(--gray-400); font-style:italic; padding-top:6px; border-top:1px dashed var(--gray-200);">SUI base: $12,000 · FUI base: $7,000</div>
        </div>

        <!-- Column 3: Union Benefits -->
        <div style="padding:14px 20px;">
          <div style="font-size:10px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:10px; padding-bottom:4px; border-bottom:1px solid #f5efe7;">Union Benefits (32BJ)</div>
          ${prAssumpRow('Welfare ($/mo)', 'welfare_monthly', (a.welfare_monthly || 0).toFixed(2), '$')}
          ${prAssumpRow('Pension ($/wk)', 'pension_weekly', (a.pension_weekly || 0).toFixed(2), '$')}
          ${prAssumpRow('Supp Retirement ($/wk)', 'supp_retirement_weekly', (a.supp_retirement_weekly || 0).toFixed(2), '$')}
          ${prAssumpRow('Legal ($/mo)', 'legal_monthly', (a.legal_monthly || 0).toFixed(2), '$')}
          ${prAssumpRow('Training ($/mo)', 'training_monthly', (a.training_monthly || 0).toFixed(2), '$')}
          ${prAssumpRow('Profit Sharing ($/qtr)', 'profit_sharing_quarterly', (a.profit_sharing_quarterly || 0).toFixed(2), '$')}
          <div style="margin-top:8px; font-size:10px; color:var(--gray-400); font-style:italic; padding-top:6px; border-top:1px dashed var(--gray-200);">Rates × headcount × period multiplier = total</div>
        </div>
      </div>
      <div style="padding:8px 20px; background:#f5efe7; border-top:1px solid #e5e0d5; display:flex; align-items:center; gap:12px; border-radius:0 0 10px 10px;">
        <span id="prAssumpStatus" style="font-size:11px; color:#5a4a3f; font-weight:600;">Seeded from Assumptions tab</span>
      </div>
    </div>
  </div>`;

  // ── Section 1: Employee Roster & Wage Calculation ──────────────────────
  html += `
  <div style="background:white; border-radius:10px; border:1px solid var(--gray-200); margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
    <div onclick="togglePayrollSection('prRoster')" style="display:flex; align-items:center; justify-content:space-between; padding:12px 20px; background:var(--gray-50); border-bottom:1px solid var(--gray-200); border-radius:10px 10px 0 0; cursor:pointer; user-select:none;">
      <h3 style="font-size:13px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin:0;">Employee Roster & Wage Calculation</h3>
      <div style="display:flex; align-items:center; gap:12px;">
        <span id="prRosterBadge" style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#eff6ff; color:#2563eb;">0 employees</span>
        <span id="prRosterTotal" style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#dcfce7; color:#16a34a;">Total: $0</span>
        <span style="font-size:12px; color:var(--gray-400);" id="prRosterChev">▾</span>
      </div>
    </div>
    <div id="prRoster">
      <div id="prRosterInfo" style="padding:10px 20px 6px; display:flex; gap:16px; align-items:center; background:#fafbfc; border-bottom:1px solid var(--gray-200);"></div>
      <div style="overflow-x:auto;">
        <table id="prRosterTable" style="width:100%; border-collapse:collapse; font-size:12px;">
          <thead>
            <tr style="background:var(--gray-50);">
              <th style="text-align:left; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Position</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">#</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Hourly Rate</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Bonus $/Emp</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);" title="Override the global Effective Week for this position only. Leave blank to use global.">Eff Wk Override</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Weekly Pay</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Pre-Incr Wages</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Post-Incr Rate</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Post-Incr Wages</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Annual Base</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">OT</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200);">Vac/Sick/Hol</th>
              <th class="r" style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); font-weight:800;">Total Comp</th>
              <th class="filler"></th>
            </tr>
          </thead>
          <tbody id="prRosterBody"></tbody>
          <tfoot id="prRosterFoot"></tfoot>
        </table>
      </div>
    </div>
  </div>`;

  // ── Section 2: Payroll Taxes, Workers Comp & Union Benefits ────────────
  html += `
  <div style="background:white; border-radius:10px; border:1px solid var(--gray-200); margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
    <div onclick="togglePayrollSection('prTaxes')" style="display:flex; align-items:center; justify-content:space-between; padding:12px 20px; background:var(--gray-50); border-bottom:1px solid var(--gray-200); border-radius:10px 10px 0 0; cursor:pointer; user-select:none;">
      <h3 style="font-size:13px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin:0;">Payroll Taxes, Workers Comp & Union Benefits</h3>
      <div style="display:flex; align-items:center; gap:12px;">
        <span style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#fff7ed; color:#ea580c;">Auto-calculated from Assumptions + Roster</span>
        <span id="prTaxTotal" style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#dcfce7; color:#16a34a;">Total: $0</span>
        <span style="font-size:12px; color:var(--gray-400);" id="prTaxesChev">▾</span>
      </div>
    </div>
    <div id="prTaxes">
      <table style="width:100%; border-collapse:collapse; font-size:12px;">
        <thead>
          <tr style="background:var(--gray-50);">
            <th style="text-align:left; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); width:200px;">Category</th>
            <th style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); width:80px;">Rate</th>
            <th style="text-align:left; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); width:220px;">Basis</th>
            <th style="text-align:right; font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; padding:8px 10px; border-bottom:2px solid var(--gray-200); width:120px;">Calculated Total</th>
          </tr>
        </thead>
        <tbody id="prTaxBody"></tbody>
        <tfoot id="prTaxFoot"></tfoot>
      </table>
    </div>
  </div>`;

  // ── Section 3: GL Detail with expandable sub-categories ────────────────
  html += `
  <div style="background:white; border-radius:10px; border:1px solid var(--gray-200); margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
    <div onclick="togglePayrollSection('prGL')" style="display:flex; align-items:center; justify-content:space-between; padding:12px 20px; background:var(--gray-50); border-bottom:1px solid var(--gray-200); border-radius:10px 10px 0 0; cursor:pointer; user-select:none;">
      <h3 style="font-size:13px; font-weight:700; color:#5a4a3f; text-transform:uppercase; letter-spacing:0.5px; margin:0;">GL Detail — Yardi Actuals & Budget</h3>
      <div style="display:flex; align-items:center; gap:12px;">
        <span style="font-size:11px; font-weight:600; padding:2px 10px; border-radius:10px; background:#f5efe7; color:#5a4a3f;">${_payrollGLLines.length} GL lines in 4 groups</span>
        <button id="prZeroToggle" onclick="prToggleZeroRows(event)" style="display:none; font-size:11px; padding:4px 12px; background:var(--blue-light, #dbeafe); color:var(--blue); border:1px solid var(--blue); border-radius:4px; cursor:pointer;"></button>
        <span style="font-size:12px; color:var(--gray-400);" id="prGLChev">▾</span>
      </div>
    </div>
    <div id="prGL">
      <div id="prGLContent"></div>
      <div id="prTieOut"></div>
    </div>
  </div>`;

  html += '</div>';
  contentDiv.innerHTML = html;

  // Now populate dynamic sections
  recalcPayroll();
  renderPayrollGL();
}

// ── Assumption row helpers ────────────────────────────────────────────────

function prAssumpRow(label, key, val, suffix) {
  return '<div style="display:flex; justify-content:space-between; align-items:center; padding:4px 0; font-size:12px;">' +
    '<span style="color:var(--gray-600);">' + label + '</span>' +
    '<div style="display:flex; align-items:center; gap:2px;">' +
    '<input class="pr-assump-input" data-key="' + key + '" value="' + val + '" onchange="payrollAssumptionChanged(this)" style="width:90px; padding:3px 8px; border:1px solid var(--gray-300); border-radius:4px; font-size:12px; text-align:right; background:#fbfaf4; font-variant-numeric:tabular-nums; font-family:inherit;">' +
    '<span style="font-size:11px; color:var(--gray-400); width:12px; display:inline-block;">' + (suffix || '') + '</span>' +
    '</div></div>';
}

function prAssumpRowCalc(label, val) {
  return '<div style="display:flex; justify-content:space-between; align-items:center; padding:4px 0; font-size:12px;">' +
    '<span style="color:var(--gray-600);">' + label + '</span>' +
    '<span style="font-size:12px; font-weight:600; color:#16a34a; background:#f0fdf4; border:1px solid #bbf7d0; border-radius:4px; padding:3px 8px; width:90px; text-align:right; display:inline-block;">' + val + '</span>' +
    '</div>';
}

function togglePayrollSection(id) {
  const el = document.getElementById(id);
  const chev = document.getElementById(id + 'Chev');
  if (!el) return;
  if (el.style.display === 'none') {
    el.style.display = '';
    if (chev) chev.textContent = '▾';
  } else {
    el.style.display = 'none';
    if (chev) chev.textContent = '▸';
  }
}

// ── Assumption change handler ─────────────────────────────────────────────

let _prAssumpSaveTimer = null;
function payrollAssumptionChanged(el) {
  const key = el.dataset.key;
  let val = el.value.trim();

  // Parse value depending on type
  if (key === 'effective_week') {
    _payrollAssumptions[key] = val;
    // Auto-calc pre/post weeks
    const wk = parseInt(val) || 16;
    _payrollAssumptions.pre_increase_weeks = Math.max(wk - 1, 0);
    _payrollAssumptions.post_increase_weeks = 52 - _payrollAssumptions.pre_increase_weeks;
  } else if (['wage_increase_pct','fica','sui','fui','mta','nys_disability','pfl','workers_comp','ot_factor','vac_sick_hol_factor'].includes(key)) {
    _payrollAssumptions[key] = parseFloat(val) / 100 || 0;
  } else if (key === 'pre_increase_weeks' || key === 'post_increase_weeks') {
    _payrollAssumptions[key] = parseInt(val) || 0;
  } else {
    _payrollAssumptions[key] = parseFloat(val) || 0;
  }

  recalcPayroll();

  // Debounced auto-save
  clearTimeout(_prAssumpSaveTimer);
  _prAssumpSaveTimer = setTimeout(savePayrollAssumptions, 800);
}

async function savePayrollAssumptions() {
  const ec = entityCode;
  try {
    await fetch('/api/payroll/assumptions/' + ec, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({assumptions: _payrollAssumptions})
    });
    const st = document.getElementById('prAssumpStatus');
    if (st) st.textContent = 'Saved ✓ — ' + new Date().toLocaleTimeString();
  } catch(e) { console.error('Failed to save payroll assumptions:', e); }
}

// ── Roster change & save ──────────────────────────────────────────────────

function prRosterChanged() {
  // Read all rows from DOM
  const rows = document.querySelectorAll('#prRosterBody tr');
  _payrollPositions = [];
  rows.forEach((tr, i) => {
    const nameInput = tr.querySelector('.pr-pos-name');
    const countInput = tr.querySelector('.pr-pos-count');
    const rateInput = tr.querySelector('.pr-pos-rate');
    const bonusInput = tr.querySelector('.pr-pos-bonus');
    const effWkInput = tr.querySelector('.pr-pos-effwk');
    if (!nameInput) return;
    const effWkRaw = effWkInput ? effWkInput.value.trim() : '';
    _payrollPositions.push({
      position_name: nameInput.value.trim(),
      employee_count: parseInt(countInput.value) || 0,
      hourly_rate: parseFloat(rateInput.value.replace(/[^0-9.]/g, '')) || 0,
      bonus_per_employee: bonusInput ? (parseFloat(bonusInput.value.replace(/[^0-9.]/g, '')) || 0) : 0,
      effective_week_override: effWkRaw === '' ? null : (parseFloat(effWkRaw) || null),
      sort_order: i
    });
  });
  recalcPayroll();

  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 800);
}

let _prRosterSaveTimer = null;
async function savePayrollPositions() {
  const ec = entityCode;
  try {
    await fetch('/api/payroll/positions/' + ec, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({positions: _payrollPositions})
    });
  } catch(e) { console.error('Failed to save payroll positions:', e); }
}

function addPayrollPosition() {
  _payrollPositions.push({position_name: '', employee_count: 0, hourly_rate: 0, bonus_per_employee: 0, effective_week_override: null, sort_order: _payrollPositions.length});
  renderPayrollRoster();
  recalcPayroll();
}

function removePayrollPosition(idx) {
  _payrollPositions.splice(idx, 1);
  renderPayrollRoster();
  recalcPayroll();
  clearTimeout(_prRosterSaveTimer);
  _prRosterSaveTimer = setTimeout(savePayrollPositions, 400);
}

// ── Core Recalculation ────────────────────────────────────────────────────

function recalcPayroll() {
  const a = _payrollAssumptions;
  const wageInc = a.wage_increase_pct || 0;
  const preWks = a.pre_increase_weeks || 15;
  const postWks = a.post_increase_weeks || 37;
  const otFactor = a.ot_factor || 0.002;
  const vshFactor = a.vac_sick_hol_factor || 0.10;

  let totalEmployees = 0;
  let totalAnnualBase = 0;
  let totalOT = 0;
  let totalVSH = 0;
  let totalComp = 0;
  let totalBonus = 0;

  // Calculate per-position wages
  const posCalcs = _payrollPositions.map(p => {
    const count = p.employee_count || 0;
    const rate = p.hourly_rate || 0;
    const bonusPerEmp = p.bonus_per_employee || 0;
    // Per-position effective week override (e.g. one Resident Manager getting a late raise)
    let posPreWks = preWks, posPostWks = postWks;
    if (p.effective_week_override && p.effective_week_override > 0) {
      posPreWks = Math.max(p.effective_week_override - 1, 0);
      posPostWks = 52 - posPreWks;
    }
    const weeklyPay = rate * 40;
    const preIncrWages = weeklyPay * posPreWks * count;
    const postIncrRate = rate * (1 + wageInc);
    const postIncrWages = (postIncrRate * 40) * posPostWks * count;
    const annualBase = preIncrWages + postIncrWages;
    const ot = annualBase * otFactor;
    const vsh = annualBase * vshFactor;
    const bonus = bonusPerEmp * count;
    const comp = annualBase + ot + vsh;

    totalEmployees += count;
    totalAnnualBase += annualBase;
    totalOT += ot;
    totalVSH += vsh;
    totalComp += comp;
    totalBonus += bonus;

    return { count, rate, bonusPerEmp, posPreWks, posPostWks, weeklyPay, preIncrWages, postIncrRate, postIncrWages, annualBase, ot, vsh, bonus, comp };
  });

  // Calculate taxes & benefits
  const grossWages = totalAnnualBase + totalOT + totalVSH;
  const ficaAmt = grossWages * (a.fica || 0);
  const suiAmt = 12000 * (a.sui || 0) * totalEmployees;
  const fuiAmt = 7000 * (a.fui || 0) * totalEmployees;
  const mtaAmt = grossWages * (a.mta || 0);
  const nysDisAmt = (a.nys_disability || 0) * totalEmployees;
  const pflAmt = grossWages * (a.pfl || 0);
  const totalPayrollTax = ficaAmt + suiAmt + fuiAmt + mtaAmt + nysDisAmt + pflAmt;
  const wcAmt = (a.workers_comp || 0) * grossWages;

  const welfareAmt = (a.welfare_monthly || 0) * totalEmployees * 12;
  const pensionAmt = (a.pension_weekly || 0) * totalEmployees * 52;
  const suppRetAmt = (a.supp_retirement_weekly || 0) * totalEmployees * 52;
  const legalAmt = (a.legal_monthly || 0) * totalEmployees * 12;
  const trainingAmt = (a.training_monthly || 0) * totalEmployees * 12;
  const profitShareAmt = (a.profit_sharing_quarterly || 0) * totalEmployees * 4;
  const totalUnion = welfareAmt + pensionAmt + suppRetAmt + legalAmt + trainingAmt + profitShareAmt;

  const totalLaborCalc = grossWages + totalPayrollTax + wcAmt + totalUnion;

  // Store for tie-out
  window._payrollCalcTotal = totalLaborCalc;

  // Publish component breakdown for GL linkage
  window._payrollComponents = {
    annual_base: totalAnnualBase,
    ot: totalOT,
    vsh_vacation: totalVSH / 3,
    vsh_holiday: totalVSH / 3,
    vsh_sick: totalVSH / 3,
    bonus: totalBonus,
    employer_taxes: ficaAmt + suiAmt + fuiAmt + mtaAmt,
    workers_comp: wcAmt,
    nys_disability: nysDisAmt,
    pfl: pflAmt,
    welfare: welfareAmt,
    pension: pensionAmt,
    supp_retirement: suppRetAmt,
    legal_fund: legalAmt,
    training_fund: trainingAmt,
    profit_sharing: profitShareAmt
  };

  // Render roster (pass assumption values for formula strings)
  renderPayrollRoster(posCalcs, totalEmployees, totalAnnualBase, totalOT, totalVSH, totalComp,
    {preWks, postWks, wageInc, otFactor, vshFactor});

  // Render taxes
  renderPayrollTaxes({ficaAmt, suiAmt, fuiAmt, mtaAmt, nysDisAmt, pflAmt, totalPayrollTax, wcAmt,
    welfareAmt, pensionAmt, suppRetAmt, legalAmt, trainingAmt, profitShareAmt, totalUnion, totalLaborCalc,
    grossWages, totalEmployees});

  // Push roster-derived component values to linked GL lines
  pushRosterToGL();

  // Update tie-out
  renderPayrollTieOut(totalLaborCalc);

  // Update info bar
  const infoDiv = document.getElementById('prRosterInfo');
  if (infoDiv) {
    infoDiv.innerHTML =
      '<div style="font-size:11px;"><span style="color:var(--gray-500);">Wage Increase:</span> <strong style="color:#5a4a3f;">' + ((wageInc)*100).toFixed(1) + '%</strong> <span style="font-size:8px; font-weight:800; color:#5a4a3f; background:#f5efe7; border:1px solid #5a4a3f; border-radius:3px; padding:0 3px; vertical-align:super;">from assumptions</span></div>' +
      '<div style="font-size:11px;"><span style="color:var(--gray-500);">Effective:</span> <strong>Wk ' + (a.effective_week || '16') + '</strong></div>' +
      '<div style="font-size:11px;"><span style="color:var(--gray-500);">Pre-Incr Weeks:</span> <strong>' + preWks + '</strong></div>' +
      '<div style="font-size:11px;"><span style="color:var(--gray-500);">Post-Incr Weeks:</span> <strong>' + postWks + '</strong></div>';
  }
}

// ── Render Roster Table ───────────────────────────────────────────────────

function renderPayrollRoster(posCalcs, totalEmp, totalBase, totalOT, totalVSH, totalComp, assumpCtx) {
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const body = document.getElementById('prRosterBody');
  const foot = document.getElementById('prRosterFoot');
  if (!body) return;

  // If no calcs passed, just render empty inputs
  if (!posCalcs) posCalcs = _payrollPositions.map(() => ({count:0,rate:0,weeklyPay:0,preIncrWages:0,postIncrRate:0,postIncrWages:0,annualBase:0,ot:0,vsh:0,comp:0}));

  const ctx = assumpCtx || {preWks:15, postWks:37, wageInc:0, otFactor:0.002, vshFactor:0.10};
  const cs = 'padding:7px 10px; border-bottom:1px solid #f3f4f6;';
  const ns = cs + 'text-align:right; font-variant-numeric:tabular-nums; font-size:12px;';
  const gs = 'color:#16a34a; font-weight:600;';
  const is = 'padding:4px 8px; border:1px solid #d1d5db; border-radius:4px; font-size:12px; text-align:right; background:#fbfaf4; box-sizing:content-box;';

  // fx cell helper for roster calculated fields (click to view formula, read-only)
  // Matches FA `.cell-fx` pattern: transparent bg + inset green left-border + dark green text.
  const rosterFx = (id, field, val, formula, posIdx, bgColor, fontWeight) => {
    const displayVal = (field === 'postIncrRate') ? '$' + val.toFixed(2) : fD(val);
    const tdStyle = 'padding:6px 8px; border-bottom:1px solid var(--gray-200); text-align:right; position:relative; cursor:pointer;';
    const inputStyle = 'cursor:pointer; pointer-events:none; width:100%; padding:4px 6px 4px 9px; border:1px solid #e5e1d8; border-radius:4px; background:transparent; box-shadow:inset 3px 0 0 #16a34a; text-align:right; font-family:inherit; font-size:13px; font-variant-numeric:tabular-nums; box-sizing:border-box; ' + (bgColor || 'color:#15803d;') + ' ' + (fontWeight || 'font-weight:600;');
    return '<td style="' + tdStyle + '" onclick="fxCellFocus(document.getElementById(\'' + id + '\'))">' +
      '<input id="' + id + '" type="text" readonly ' +
        'data-readonly="true" ' +
        'data-gl="Roster[' + posIdx + ']" ' +
        'data-field="' + field + '" ' +
        'data-raw="' + Math.round(val) + '" ' +
        'data-formula="' + formula.replace(/"/g, '&quot;') + '" ' +
        'value="' + displayVal + '" ' +
        'onblur="fxCellBlur(this)" ' +
        'style="' + inputStyle + '">' +
      '</td>';
  };

  let rows = '';
  _payrollPositions.forEach((p, i) => {
    const c = posCalcs[i] || {};
    const count = p.employee_count || 0;
    const rate = p.hourly_rate || 0;
    // Build formulas as parseable math strings with literal values (safeEvalFormula compatible)
    // Uses per-position week overrides if set, otherwise global ctx values
    const usedPreWks = c.posPreWks !== undefined ? c.posPreWks : ctx.preWks;
    const usedPostWks = c.posPostWks !== undefined ? c.posPostWks : ctx.postWks;
    const fWeekly = '=' + rate + '*40';
    const fPreWages = '=' + (c.weeklyPay||0) + '*' + usedPreWks + '*' + count;
    const fPostRate = '=' + rate + '*(1+' + ctx.wageInc.toFixed(4) + ')';
    const fPostWages = '=' + (c.postIncrRate||0).toFixed(4) + '*40*' + usedPostWks + '*' + count;
    const fAnnualBase = '=' + (c.preIncrWages||0) + '+' + (c.postIncrWages||0);
    const fOT = '=' + (c.annualBase||0) + '*' + ctx.otFactor.toFixed(4);
    const fVSH = '=' + (c.annualBase||0) + '*' + ctx.vshFactor.toFixed(4);
    const fComp = '=' + (c.annualBase||0) + '+' + (c.ot||0) + '+' + (c.vsh||0);

    rows += '<tr>' +
      '<td style="' + cs + '"><input class="pr-pos-name" type="text" value="' + (p.position_name || '') + '" onchange="prRosterChanged()" style="padding:4px 8px; border:1px solid #d1d5db; border-radius:4px; font-size:12px; background:#fbfaf4; box-sizing:content-box;"></td>' +
      '<td style="' + ns + '"><input class="pr-pos-count" type="number" value="' + (p.employee_count || 0) + '" onchange="prRosterChanged()" style="' + is + '" min="0"></td>' +
      '<td style="' + ns + '"><input class="pr-pos-rate" type="text" value="' + (p.hourly_rate || 0) + '" onchange="prRosterChanged()" style="' + is + '"></td>' +
      '<td style="' + ns + '"><input class="pr-pos-bonus" type="text" value="' + (p.bonus_per_employee || 0) + '" onchange="prRosterChanged()" style="' + is + '"></td>' +
      '<td style="' + ns + '"><input class="pr-pos-effwk" type="number" min="1" max="52" placeholder="—" value="' + (p.effective_week_override || '') + '" onchange="prRosterChanged()" title="Override global Effective Week for this position only" style="' + is + '"></td>' +
      rosterFx('pr_rost_wk_'+i, 'weeklyPay', c.weeklyPay||0, fWeekly, i) +
      rosterFx('pr_rost_pre_'+i, 'preIncrWages', c.preIncrWages||0, fPreWages, i) +
      rosterFx('pr_rost_pr_'+i, 'postIncrRate', c.postIncrRate||0, fPostRate, i) +
      rosterFx('pr_rost_post_'+i, 'postIncrWages', c.postIncrWages||0, fPostWages, i) +
      rosterFx('pr_rost_base_'+i, 'annualBase', c.annualBase||0, fAnnualBase, i, 'color:#1f2937;', 'font-weight:700;') +
      rosterFx('pr_rost_ot_'+i, 'ot', c.ot||0, fOT, i) +
      rosterFx('pr_rost_vsh_'+i, 'vsh', c.vsh||0, fVSH, i) +
      rosterFx('pr_rost_comp_'+i, 'comp', c.comp||0, fComp, i, 'color:#1e40af;', 'font-weight:800;') +
      '<td style="padding:7px 4px; border-bottom:1px solid #f3f4f6;"><button onclick="removePayrollPosition(' + i + ')" style="padding:2px 6px; font-size:10px; cursor:pointer; background:#fef2f2; color:#dc2626; border:1px solid #fecaca; border-radius:4px;">✕</button></td>' +
      '</tr>';
  });
  body.innerHTML = rows;

  // Footer totals
  foot.innerHTML =
    '<tr style="background:var(--gray-50); font-weight:700;">' +
    '<td style="padding:8px 10px; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;">TOTAL</td>' +
    '<td style="padding:8px 10px; text-align:right; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb; font-weight:700;">' + (totalEmp || 0) + '</td>' +
    '<td style="border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;"></td>' +
    '<td style="padding:8px 10px; text-align:right; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb; font-weight:700;">' + fD(_payrollPositions.reduce((s,p)=> s+((p.bonus_per_employee||0)*(p.employee_count||0)),0)) + '</td>' +
    '<td style="border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;"></td>' +
    '<td style="padding:8px 10px; text-align:right; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;">' + fD(_payrollPositions.reduce((s,p,i)=> s+(posCalcs[i]?.weeklyPay||0),0)) + '</td>' +
    '<td style="padding:8px 10px; text-align:right; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;">' + fD(_payrollPositions.reduce((s,p,i)=> s+(posCalcs[i]?.preIncrWages||0),0)) + '</td>' +
    '<td style="border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;"></td>' +
    '<td style="padding:8px 10px; text-align:right; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;">' + fD(_payrollPositions.reduce((s,p,i)=> s+(posCalcs[i]?.postIncrWages||0),0)) + '</td>' +
    '<td style="padding:8px 10px; text-align:right; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb; font-weight:800;">' + fD(totalBase || 0) + '</td>' +
    '<td style="padding:8px 10px; text-align:right; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;">' + fD(totalOT || 0) + '</td>' +
    '<td style="padding:8px 10px; text-align:right; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;">' + fD(totalVSH || 0) + '</td>' +
    '<td style="padding:8px 10px; text-align:right; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb; font-weight:800; font-size:13px; color:#1e40af;">' + fD(totalComp || 0) + '</td>' +
    '<td style="border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb;"></td>' +
    '</tr>' +
    '<tr><td colspan="14" style="padding:8px 10px;">' +
    '<button onclick="addPayrollPosition()" style="padding:4px 12px; font-size:11px; font-weight:600; border-radius:5px; cursor:pointer; background:white; color:#2563eb; border:1px solid #2563eb;">+ Add Position</button>' +
    '<span style="margin-left:12px; font-size:10px; color:var(--gray-400); font-style:italic;">Flexible positions — each building can have different roles</span>' +
    '</td></tr>';

  // Update badges
  const badge = document.getElementById('prRosterBadge');
  const totBadge = document.getElementById('prRosterTotal');
  if (badge) badge.textContent = (totalEmp || 0) + ' employees';
  if (totBadge) totBadge.textContent = 'Total: ' + fD(totalComp || 0);

  // Auto-size all roster inputs to their content width
  if (typeof prAutoSizeAll === 'function') prAutoSizeAll();
}

// ── Render Taxes/Benefits Table ───────────────────────────────────────────

function renderPayrollTaxes(t) {
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const fP = v => (v * 100).toFixed(3) + '%';
  const body = document.getElementById('prTaxBody');
  const foot = document.getElementById('prTaxFoot');
  if (!body) return;

  const a = _payrollAssumptions;
  const cs = 'padding:7px 10px; border-bottom:1px solid #f3f4f6;';
  const ns = cs + 'text-align:right; font-variant-numeric:tabular-nums;';
  const gs = 'color:#16a34a; font-weight:600;';
  const ps = 'color:#5a4a3f;';
  const catHdr = 'background:#f5efe7; font-weight:700; color:#5a4a3f; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; padding:8px 10px; border-bottom:2px solid #e5e7eb;';
  const subRow = 'background:var(--gray-50); font-weight:700; border-top:2px solid #d1d5db; border-bottom:2px solid #e5e7eb; padding:8px 10px;';

  let html = '';
  // Payroll Taxes
  html += '<tr><td colspan="4" style="' + catHdr + '">Payroll Taxes</td></tr>';
  html += taxRow('FICA', fP(a.fica||0), 'Gross Wages × Rate', fD(t.ficaAmt));
  html += taxRow('SUI', fP(a.sui||0), '$12,000 × Rate × ' + t.totalEmployees + ' emp', fD(t.suiAmt));
  html += taxRow('FUI', fP(a.fui||0), '$7,000 × Rate × ' + t.totalEmployees + ' emp', fD(t.fuiAmt));
  html += taxRow('MTA', fP(a.mta||0), 'Gross Wages × Rate', fD(t.mtaAmt));
  html += taxRow('NYS Disability', fP(a.nys_disability||0), 'Per employee/year', fD(t.nysDisAmt));
  html += taxRow('Paid Family Leave', fP(a.pfl||0), 'Gross Wages × Rate', fD(t.pflAmt));
  html += '<tr><td colspan="3" style="' + subRow + '">Total Payroll Taxes</td><td style="' + subRow + ' text-align:right; font-weight:800;">' + fD(t.totalPayrollTax) + '</td></tr>';

  // Workers Comp
  html += '<tr style="height:8px;"><td colspan="4"></td></tr>';
  html += '<tr><td style="' + cs + ' font-weight:600;">Workers Compensation</td><td style="' + ns + ps + '">' + fP(a.workers_comp||0) + '</td><td style="' + cs + ' font-size:10px; color:var(--gray-400); font-style:italic;">Gross Wages × Rate</td><td style="' + ns + gs + ' font-weight:700;">' + fD(t.wcAmt) + '</td></tr>';

  // Union Benefits
  html += '<tr style="height:8px;"><td colspan="4"></td></tr>';
  html += '<tr><td colspan="4" style="' + catHdr + '">Union Benefits (32BJ)</td></tr>';
  html += taxRow('Welfare', '$' + (a.welfare_monthly||0).toFixed(2) + '/mo', '$' + (a.welfare_monthly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 12 mo', fD(t.welfareAmt));
  html += taxRow('Pension', '$' + (a.pension_weekly||0).toFixed(2) + '/wk', '$' + (a.pension_weekly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 52 wk', fD(t.pensionAmt));
  html += taxRow('Supp. Retirement', '$' + (a.supp_retirement_weekly||0).toFixed(2) + '/wk', '$' + (a.supp_retirement_weekly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 52 wk', fD(t.suppRetAmt));
  html += taxRow('Legal Fund', '$' + (a.legal_monthly||0).toFixed(2) + '/mo', '$' + (a.legal_monthly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 12 mo', fD(t.legalAmt));
  html += taxRow('Training Fund', '$' + (a.training_monthly||0).toFixed(2) + '/mo', '$' + (a.training_monthly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 12 mo', fD(t.trainingAmt));
  html += taxRow('Profit Sharing', '$' + (a.profit_sharing_quarterly||0).toFixed(2) + '/qtr', '$' + (a.profit_sharing_quarterly||0).toFixed(2) + ' × ' + t.totalEmployees + ' emp × 4 qtr', fD(t.profitShareAmt));
  html += '<tr><td colspan="3" style="' + subRow + '">Total Union Benefits</td><td style="' + subRow + ' text-align:right; font-weight:800;">' + fD(t.totalUnion) + '</td></tr>';

  body.innerHTML = html;

  // Grand total footer
  foot.innerHTML = '<tr style="background:#f5efe7; font-weight:800; font-size:13px;">' +
    '<td colspan="3" style="border-top:3px double #5a4a3f; padding:10px;">TOTAL LABOR & RELATED (calculated)</td>' +
    '<td style="border-top:3px double #5a4a3f; padding:10px; text-align:right; font-size:14px;">' + fD(t.totalLaborCalc) + '</td></tr>';

  // Update badge
  const badge = document.getElementById('prTaxTotal');
  if (badge) badge.textContent = 'Total: ' + fD(t.totalPayrollTax + t.wcAmt + t.totalUnion);
}

function taxRow(label, rate, basis, total) {
  const cs = 'padding:7px 10px; border-bottom:1px solid #f3f4f6;';
  const ns = cs + 'text-align:right; font-variant-numeric:tabular-nums;';
  return '<tr><td style="' + cs + '">' + label + '</td>' +
    '<td style="' + ns + ' color:#5a4a3f;">' + rate + '</td>' +
    '<td style="' + cs + ' font-size:10px; color:var(--gray-400); font-style:italic;">' + basis + '</td>' +
    '<td style="' + ns + ' color:#16a34a; font-weight:600;">' + total + '</td></tr>';
}

// ── Render GL Detail with Expandable Groups ───────────────────────────────

const PAYROLL_GL_GROUPS = [
  {key: 'wages', label: 'Wages', glPrefixes: ['5105']},
  {key: 'payroll_taxes', label: 'Payroll Taxes', glPrefixes: ['5140','5145']},
  {key: 'benefits', label: 'Benefits', glPrefixes: ['5150','5155','5160']},
  {key: 'other_payroll', label: 'Other Payroll', glPrefixes: ['5162','5165','5166','5168','5172']}
];

// Maps GL codes to roster/assumption calc components. Mapped GLs have their
// proposed_budget driven automatically by Section 1-2 calculations; unmapped
// GLs retain the manual flat-% behavior.
const PAYROLL_COMPONENT_MAP = {
  '5105-0000': 'annual_base',      // Gross Payroll
  '5105-0010': 'ot',               // Overtime Pay
  '5105-0015': 'vsh_vacation',     // Vacation Pay (1/3 of VSH)
  '5105-0020': 'vsh_holiday',      // Holiday Pay (1/3 of VSH)
  '5105-0025': 'vsh_sick',         // Sick Pay (1/3 of VSH)
  '5105-0035': 'bonus',            // Bonus (flat $/employee × count, per position)
  '5145-0000': 'employer_taxes',   // Employer Payroll Taxes (FICA+SUI+FUI+MTA)
  '5165-0000': 'workers_comp',     // Workers Comp Insurance
  '5166-0000': 'nys_disability',   // Disability Insurance
  '5168-0000': 'pfl',              // Paid Family Leave
  '5155-0015': 'welfare',          // Health Insurance (welfare)
  '5160-0010': 'pension',          // Pension Fund
  '5160-0020': 'supp_retirement',  // Annuity Fund
  '5160-0025': 'legal_fund',       // Legal Fund
  '5160-0030': 'training_fund',    // Training Fund
  '5160-0035': 'profit_sharing'    // Profit Sharing
};

function getPayrollGroup(glCode) {
  const prefix = (glCode || '').split('-')[0];
  for (const g of PAYROLL_GL_GROUPS) {
    if (g.glPrefixes.includes(prefix)) return g.key;
  }
  return 'other_payroll';
}

function renderPayrollGL() {
  const contentDiv = document.getElementById('prGLContent');
  if (!contentDiv) return;

  const lines = _payrollGLLines;
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const fP = v => ((v||0) * 100).toFixed(1) + '%';
  const estLbl = typeof estimateLabel === 'function' ? estimateLabel() : 'Sep-Dec Est';

  // Group lines
  const grouped = {};
  PAYROLL_GL_GROUPS.forEach(g => { grouped[g.key] = []; });
  lines.forEach(l => {
    const gk = getPayrollGroup(l.gl_code);
    grouped[gk].push(l);
  });

  let html = '<div class="prgl-scroll"><table><thead><tr>' +
    '<th class="frozen frozen-gl">GL Code</th>' +
    '<th class="frozen frozen-desc">Description</th>' +
    '<th class="num"><span class="num-box">Prior Year</span></th>' +
    '<th class="num"><span class="num-box">YTD Actual</span></th>' +
    '<th class="num"><span class="num-box">' + estLbl + '</span></th>' +
    '<th class="num"><span class="num-box">12 Mo Forecast</span></th>' +
    '<th class="num"><span class="num-box">Curr Budget</span></th>' +
    '<th class="num"><span class="num-box">Inc %</span></th>' +
    '<th class="num"><span class="num-box">Proposed</span></th>' +
    '<th class="num"><span class="num-box">$ Var</span></th>' +
    '<th class="num"><span class="num-box">% Chg</span></th>' +
    '<th>Notes</th>' +
    '</tr></thead><tbody>';

  let grandTotals = {prior:0, ytd:0, estimate:0, forecast:0, currBudget:0, proposed:0};

  PAYROLL_GL_GROUPS.forEach(g => {
    const gLines = grouped[g.key];
    if (gLines.length === 0) return;

    // Category header (clickable, spans full width, scrolls with content)
    html += '<tr class="cat-hdr" onclick="togglePrGLGroup(\'' + g.key + '\')">' +
      '<td colspan="12">' +
      '<span id="prgl_' + g.key + '_arrow" style="display:inline-block; transition:transform 0.2s; margin-right:6px; font-size:10px;">▶</span>' +
      g.label + '<span style="font-size:10px; font-weight:500; color:var(--gray-400); margin-left:8px; text-transform:none; letter-spacing:0;">' + gLines.length + ' GL lines</span>' +
      '</td></tr>';

    // Individual GL lines (hidden by default, except wages)
    let subTotals = {prior:0, ytd:0, estimate:0, forecast:0, currBudget:0, proposed:0};

    gLines.forEach(l => {
      const est = prFaComputeEstimate(l);
      const fc = prFaComputeForecast(l);
      const prop = float(l.proposed_budget || 0);
      const curr = float(l.current_budget || 0);
      const varD = prop - curr;
      const varP = curr !== 0 ? varD / curr : 0;

      subTotals.prior += float(l.prior_year);
      subTotals.ytd += float(l.ytd_actual);
      subTotals.estimate += est;
      subTotals.forecast += fc;
      subTotals.currBudget += curr;
      subTotals.proposed += prop;

      const hidden = g.key !== 'wages' ? ' style="display:none;"' : '';

      // Linked rows are auto-driven by roster — show 🔗 icon, lock Inc%, highlight Proposed
      const isLinked = !!l._linked;
      const linkIcon = isLinked ? '<span title="Driven by roster calculation" style="color:#2563eb; font-size:11px; margin-right:3px;">🔗</span>' : '';
      const pctDisabled = isLinked ? ' disabled title="Locked — driven by roster calculation"' : '';

      // Build human-readable formulas (Payroll uses simplified base — no accrual/unpaid)
      const pyr = float(l.prior_year), yta = float(l.ytd_actual);
      let estFormula;
      if (YTD_MONTHS > 0) {
        estFormula = '=' + yta + '/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
      } else {
        estFormula = '=0';
      }
      const fcstFormula = '=' + yta + '+' + Math.round(est);
      const componentKey = PAYROLL_COMPONENT_MAP[l.gl_code];
      const propFormulaDisplay = isLinked
        ? '=Roster.' + componentKey + ' (auto-linked)'
        : '=Forecast*(1+IncreasePct)';

      // Determine override states
      const estOverride = l.estimate_override !== null && l.estimate_override !== undefined;
      const fcstOverride = l.forecast_override !== null && l.forecast_override !== undefined;
      const propHasFormula = !!(l.proposed_formula && l.proposed_formula !== 'manual');
      const propManualOverride = l.proposed_formula === 'manual';

      // Cell IDs
      const estId = 'pr_est_' + l.gl_code;
      const fcstId = 'pr_fcst_' + l.gl_code;
      const propId = 'pr_prop_' + l.gl_code;

      // Helper: build fx cell input matching R&S style (class="cell cell-fx" + top-right fx badge)
      const fxInput = (id, val, formula, field, overrideFlag, extraAttr, linkedFlag) => {
        const cellClass = linkedFlag ? 'cell cell-fx cell-fx-linked' : 'cell cell-fx';
        return '<input id="' + id + '" class="' + cellClass + '" type="text" readonly' +
          ' value="' + fD(val) + '"' +
          ' data-raw="' + Math.round(val) + '"' +
          ' data-formula="' + formula.replace(/"/g, '&quot;') + '"' +
          ' data-override="' + (overrideFlag ? 'true' : 'false') + '"' +
          (extraAttr || '') +
          ' data-gl="' + l.gl_code + '" data-field="' + field + '"' +
          ' onblur="fxCellBlur(this)"' +
          ' style="cursor:pointer; pointer-events:none;">';
      };

      // Estimate cell — always editable via formula bar
      const estCellHtml = '<td class="num" onclick="fxCellFocus(document.getElementById(\'' + estId + '\'))">' +
        fxInput(estId, est, estFormula, 'estimate_override', estOverride) + '</td>';

      // Forecast cell — always editable via formula bar
      const fcstCellHtml = '<td class="num" onclick="fxCellFocus(document.getElementById(\'' + fcstId + '\'))">' +
        fxInput(fcstId, fc, fcstFormula, 'forecast_override', fcstOverride) + '</td>';

      // Proposed cell: non-linked rows editable via formula bar; linked rows are read-only linked
      let propCellHtml;
      if (isLinked) {
        // Linked row: read-only blue-styled cell with 🔗fx badge — no click handler (not editable)
        propCellHtml = '<td class="num" title="' + propFormulaDisplay + '">' +
          '<input class="cell cell-fx cell-fx-linked" type="text" readonly value="' + fD(prop) + '" data-raw="' + Math.round(prop) + '"' +
          ' style="cursor:not-allowed; pointer-events:none;">' +
          '</td>';
      } else {
        const pfAttr = propHasFormula ? ' data-proposed-formula="' + l.proposed_formula.replace(/"/g, '&quot;') + '"' : '';
        const propOverride = propHasFormula || propManualOverride;
        propCellHtml = '<td class="num" onclick="fxCellFocus(document.getElementById(\'' + propId + '\'))">' +
          fxInput(propId, prop, propFormulaDisplay, 'proposed_budget', propOverride, pfAttr) + '</td>';
      }

      // Editable $ cell (Prior, YTD, Curr Budget) — matches R&S
      const prDollarCell = (field, val) => {
        return '<td class="num"><input class="cell pr-gl-dollar" type="text" ' +
          'data-gl="' + l.gl_code + '" data-field="' + field + '" ' +
          'value="' + fD(val) + '" data-raw="' + Math.round(val || 0) + '" ' +
          'onfocus="this.value=this.dataset.raw" onblur="prDollarCellBlur(this)"></td>';
      };

      const zeroClass = prGlIsZero(l) ? ' prgl-zero-row' : '';
      html += '<tr class="prgl-row' + zeroClass + '" data-prgroup="' + g.key + '" data-gl="' + l.gl_code + '"' + hidden + '>' +
        '<td class="frozen frozen-gl"><span style="font-size:13px; font-variant-numeric:tabular-nums; font-weight:600;">' + linkIcon + l.gl_code + '</span></td>' +
        '<td class="frozen frozen-desc">' + (l.description || '') + '</td>' +
        prDollarCell('prior_year', l.prior_year) +
        prDollarCell('ytd_actual', l.ytd_actual) +
        estCellHtml +
        fcstCellHtml +
        prDollarCell('current_budget', curr) +
        '<td class="num"><input class="cell cell-pct pr-gl-pct" data-gl="' + l.gl_code + '" value="' + fP(l.increase_pct) + '" onchange="savePrGLIncrease(this)"' + pctDisabled + '></td>' +
        propCellHtml +
        '<td class="num"><span class="num-box" style="' + (varD >= 0 ? 'color:#2563eb;' : 'color:#16a34a;') + '">' + fD(varD) + '</span></td>' +
        '<td class="num"><span class="num-box">' + (varP * 100).toFixed(1) + '%</span></td>' +
        '<td><input class="cell cell-notes pr-gl-note" type="text" data-gl="' + l.gl_code + '" value="' + (l.notes || '').replace(/"/g, '&quot;') + '" onchange="savePrGLNote(this)" placeholder="Add note..."></td>' +
        '</tr>';
    });

    // Subtotal row (frozen GL + Description cells carry the label; numeric cells scroll)
    html += '<tr class="sub-row">' +
      '<td class="frozen frozen-gl"></td>' +
      '<td class="frozen frozen-desc">Total ' + g.label + '</td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.prior) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.ytd) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.estimate) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.forecast) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.currBudget) + '</span></td>' +
      '<td></td>' +
      '<td class="num"><span class="num-box" style="font-weight:800;">' + fD(subTotals.proposed) + '</span></td>' +
      '<td class="num"><span class="num-box">' + fD(subTotals.proposed - subTotals.currBudget) + '</span></td>' +
      '<td class="num"><span class="num-box">' + (subTotals.currBudget ? ((subTotals.proposed - subTotals.currBudget) / subTotals.currBudget * 100).toFixed(1) + '%' : '—') + '</span></td>' +
      '<td></td>' +
      '</tr>';

    // Accumulate grand totals
    Object.keys(grandTotals).forEach(k => { grandTotals[k] += subTotals[k]; });
  });

  // Grand total row (navy #1e3a5f, matches R&S/Repairs/Gen&Admin total-row)
  html += '<tr class="total-row">' +
    '<td class="frozen frozen-gl"></td>' +
    '<td class="frozen frozen-desc">TOTAL PAYROLL</td>' +
    '<td class="num"><span class="num-box">' + fD(grandTotals.prior) + '</span></td>' +
    '<td class="num"><span class="num-box">' + fD(grandTotals.ytd) + '</span></td>' +
    '<td class="num"><span class="num-box">' + fD(grandTotals.estimate) + '</span></td>' +
    '<td class="num"><span class="num-box">' + fD(grandTotals.forecast) + '</span></td>' +
    '<td class="num"><span class="num-box">' + fD(grandTotals.currBudget) + '</span></td>' +
    '<td></td>' +
    '<td class="num"><span class="num-box">' + fD(grandTotals.proposed) + '</span></td>' +
    '<td class="num"><span class="num-box">' + fD(grandTotals.proposed - grandTotals.currBudget) + '</span></td>' +
    '<td class="num"><span class="num-box">' + (grandTotals.currBudget ? ((grandTotals.proposed - grandTotals.currBudget) / grandTotals.currBudget * 100).toFixed(1) + '%' : '—') + '</span></td>' +
    '<td></td>' +
    '</tr>';

  html += '</tbody></table></div>';
  contentDiv.innerHTML = html;

  // Auto-expand wages group arrow
  const wArrow = document.getElementById('prgl_wages_arrow');
  if (wArrow) wArrow.style.transform = 'rotate(90deg)';

  // Auto-size all editable cells + refresh zero-row toggle
  if (typeof prAutoSizeAll === 'function') prAutoSizeAll();
  if (typeof prUpdateZeroToggleBtn === 'function') prUpdateZeroToggleBtn();

  // Store GL total for tie-out
  window._payrollGLTotal = grandTotals.proposed;
  renderPayrollTieOut(window._payrollCalcTotal || 0);
}

function float(v) { return parseFloat(v) || 0; }

function togglePrGLGroup(groupKey) {
  const rows = document.querySelectorAll('tr[data-prgroup="' + groupKey + '"]');
  const arrow = document.getElementById('prgl_' + groupKey + '_arrow');
  if (!rows.length) return;
  const isHidden = rows[0].style.display === 'none';
  rows.forEach(r => { r.style.display = isHidden ? '' : 'none'; });
  if (arrow) arrow.style.transform = isHidden ? 'rotate(90deg)' : '';
}

// ── Tie-Out Bar ───────────────────────────────────────────────────────────

// Push roster-derived component values to linked GL lines.
// Updates _payrollGLLines in memory, then persists to DB via /api/fa-lines.
let _prPushSaveTimer = null;
function pushRosterToGL() {
  const comps = window._payrollComponents;
  if (!comps || !Array.isArray(_payrollGLLines)) return;

  const savePayload = [];
  let changed = false;

  _payrollGLLines.forEach(line => {
    const componentKey = PAYROLL_COMPONENT_MAP[line.gl_code];
    if (!componentKey || comps[componentKey] === undefined) {
      line._linked = false;
      return;
    }
    // Skip rows the user has manually overridden (proposed_formula set)
    if (line.proposed_formula) {
      line._linked = false;
      return;
    }
    const newProposed = Math.round(comps[componentKey]);
    const oldProposed = Math.round(line.proposed_budget || 0);
    line._linked = true;
    line.proposed_budget = newProposed;
    // Back-calc increase_pct from curr_budget so the column stays accurate
    const curr = float(line.current_budget || 0);
    line.increase_pct = curr ? (newProposed / curr - 1) : 0;
    if (newProposed !== oldProposed) {
      changed = true;
      savePayload.push({
        gl_code: line.gl_code,
        proposed_budget: newProposed,
        increase_pct: line.increase_pct
      });
    }
  });

  // Re-render GL section to reflect updated values + linked indicators
  renderPayrollGL();

  // Debounced persist — batches changes from rapid roster edits
  if (changed && savePayload.length > 0) {
    clearTimeout(_prPushSaveTimer);
    _prPushSaveTimer = setTimeout(async () => {
      try {
        await fetch('/api/fa-lines/' + entityCode, {
          method: 'PUT',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({lines: savePayload})
        });
      } catch(e) { console.error('Failed to save roster-linked GL values:', e); }
    }, 800);
  }
}

// Called when a Payroll dollar cell (Prior, YTD, Accrual, Unpaid, Curr Budget)
// loses focus. Parses, saves via faAutoSave, updates _payrollGLLines, and re-renders.
function prDollarCellBlur(el) {
  const raw = parseDollar(el.value);
  const rounded = Math.round(raw);
  el.dataset.raw = rounded;
  const gl = el.dataset.gl, field = el.dataset.field;
  // Save via existing fa-lines endpoint (uses accumulator)
  faAutoSave(gl, field, rounded);
  // Update in-memory line + re-render Payroll GL
  const line = _payrollGLLines.find(l => l.gl_code === gl);
  if (line) {
    line[field] = rounded;
    renderPayrollGL();
    if (window._payrollCalcTotal !== undefined) {
      renderPayrollTieOut(window._payrollCalcTotal);
    }
  }
}

// Called from formulaBarAccept when a Payroll GL cell is edited.
// Syncs the in-memory _payrollGLLines array and triggers re-render.
function payrollCellEdited(el, glCode, field) {
  const line = _payrollGLLines.find(l => l.gl_code === glCode);
  if (!line) return;
  const raw = parseFloat(el.dataset.raw) || 0;
  const overrideSet = el.dataset.override === 'true';

  if (field === 'estimate_override') {
    line.estimate_override = overrideSet ? raw : null;
  } else if (field === 'forecast_override') {
    line.forecast_override = overrideSet ? raw : null;
  } else if (field === 'proposed_budget') {
    line.proposed_budget = raw;
    // Mark as user-overridden so pushRosterToGL won't re-link
    line.proposed_formula = el.dataset.proposedFormula || 'manual';
    line._linked = false;
    // Back-calc increase_pct to keep column accurate
    const curr = float(line.current_budget || 0);
    line.increase_pct = curr ? (raw / curr - 1) : 0;
  }

  // Re-render to refresh totals and any dependent displays
  renderPayrollGL();
  if (window._payrollCalcTotal !== undefined) {
    renderPayrollTieOut(window._payrollCalcTotal);
  }
}

function renderPayrollTieOut(calcTotal) {
  const div = document.getElementById('prTieOut');
  if (!div) return;
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };

  // Break down GL total into linked (roster-driven) vs manual (flat %)
  let linkedTotal = 0;
  let manualTotal = 0;
  let linkedCount = 0;
  if (Array.isArray(_payrollGLLines)) {
    _payrollGLLines.forEach(l => {
      const prop = Math.round(l.proposed_budget || 0);
      if (l._linked) { linkedTotal += prop; linkedCount++; }
      else { manualTotal += prop; }
    });
  }
  const glTotal = linkedTotal + manualTotal;
  window._payrollGLTotal = glTotal;

  // Match: linked total should equal roster calc total (by construction)
  const linkedMatch = Math.abs(linkedTotal - calcTotal) < 1;

  div.innerHTML = '<div style="padding:16px 20px; background:linear-gradient(135deg, #eff6ff 0%, #f0f9ff 100%); border-top:2px solid #93c5fd; border-radius:0 0 10px 10px;">' +
    '<div style="display:flex; gap:20px; align-items:center; flex-wrap:wrap;">' +
      '<div onclick="togglePrLinkedBreakdown()" style="flex:1; min-width:140px; cursor:pointer; padding:8px; margin:-8px; border-radius:6px; transition:background 0.15s;" onmouseover="this.style.background=\'rgba(255,255,255,0.5)\'" onmouseout="this.style.background=\'transparent\'" title="Click to see all linked GLs">' +
        '<div style="font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.5px;">🔗 Linked GLs (Auto) <span id="prLinkedArrow" style="display:inline-block; transition:transform 0.2s; font-size:9px; margin-left:3px;">▶</span></div>' +
        '<div style="font-size:20px; font-weight:800; color:#1e40af;">' + fD(linkedTotal) + '</div>' +
        '<div style="font-size:10px; color:#3b82f6; font-style:italic;">' + linkedCount + ' GLs driven by roster — click to view</div>' +
      '</div>' +
      '<div style="font-size:24px; color:#9ca3af;">+</div>' +
      '<div style="flex:1; min-width:140px;">' +
        '<div style="font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.5px;">Manual GLs</div>' +
        '<div style="font-size:20px; font-weight:800; color:#374151;">' + fD(manualTotal) + '</div>' +
        '<div style="font-size:10px; color:var(--gray-400); font-style:italic;">Flat % applied</div>' +
      '</div>' +
      '<div style="font-size:24px; color:#9ca3af;">=</div>' +
      '<div style="flex:1; min-width:140px;">' +
        '<div style="font-size:10px; font-weight:700; color:#1f2937; text-transform:uppercase; letter-spacing:0.5px;">Total Payroll</div>' +
        '<div style="font-size:22px; font-weight:800; color:#1f2937;">' + fD(glTotal) + '</div>' +
      '</div>' +
      '<div style="margin-left:auto; text-align:right;">' +
        '<div style="font-size:10px; font-weight:700; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.5px;">Roster Calc Check</div>' +
        '<div style="font-size:14px; font-weight:700; color:' + (linkedMatch ? '#059669' : '#dc2626') + ';">' + (linkedMatch ? '✓ Matches ' : '⚠ Diff: ') + fD(calcTotal) + '</div>' +
      '</div>' +
    '</div>' +
    '<div id="prLinkedBreakdown" style="display:none; margin-top:16px; padding-top:16px; border-top:1px solid #bfdbfe;">' + buildLinkedBreakdownHTML() + '</div>' +
    '</div>';
}

// Build breakdown table showing each linked GL with override controls
function buildLinkedBreakdownHTML() {
  if (!Array.isArray(_payrollGLLines)) return '';
  const fD = v => { const n = Math.round(v); return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString(); };
  const linkedLines = _payrollGLLines.filter(l => l._linked);
  if (linkedLines.length === 0) {
    return '<div style="font-size:12px; color:#6b7280; font-style:italic; text-align:center; padding:12px;">No linked GLs yet — update the roster or assumptions to drive GL values.</div>';
  }

  let html = '<div style="font-size:11px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Linked GL Breakdown</div>';
  html += '<table style="width:100%; border-collapse:collapse; font-size:12px;">';
  html += '<thead><tr style="background:rgba(255,255,255,0.6);">' +
    '<th style="text-align:left; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">GL Code</th>' +
    '<th style="text-align:left; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">Description</th>' +
    '<th style="text-align:left; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">Roster Component</th>' +
    '<th style="text-align:right; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">Current Value</th>' +
    '<th style="text-align:center; padding:6px 10px; font-size:10px; font-weight:700; color:#1e40af; text-transform:uppercase; letter-spacing:0.3px;">Override</th>' +
    '</tr></thead><tbody>';

  linkedLines.forEach(l => {
    const compKey = PAYROLL_COMPONENT_MAP[l.gl_code] || '—';
    const val = Math.round(l.proposed_budget || 0);
    html += '<tr style="border-top:1px solid rgba(147,197,253,0.3);">' +
      '<td style="padding:6px 10px; font-family:monospace; font-size:11px; font-weight:600; color:#1e40af;">🔗 ' + l.gl_code + '</td>' +
      '<td style="padding:6px 10px; font-size:12px; color:#1f2937;">' + (l.description || '') + '</td>' +
      '<td style="padding:6px 10px; font-size:11px; color:#3b82f6; font-family:monospace;">' + compKey + '</td>' +
      '<td style="padding:6px 10px; text-align:right; font-weight:700; color:#1e40af; font-variant-numeric:tabular-nums;">' + fD(val) + '</td>' +
      '<td style="padding:6px 10px; text-align:center;">' +
        '<input type="text" placeholder="Enter $" data-gl="' + l.gl_code + '" ' +
          'style="width:90px; padding:3px 6px; border:1px solid #93c5fd; border-radius:4px; font-size:11px; text-align:right; background:white;" ' +
          'onkeydown="if(event.key===\'Enter\'){prOverrideLinkedGL(this);}"> ' +
        '<button onclick="prOverrideLinkedGL(this.previousElementSibling)" ' +
          'style="padding:3px 10px; font-size:10px; font-weight:600; background:#2563eb; color:white; border:none; border-radius:4px; cursor:pointer; margin-left:4px;">Override</button>' +
      '</td>' +
      '</tr>';
  });

  html += '</tbody></table>';
  html += '<div style="margin-top:8px; font-size:10px; color:#6b7280; font-style:italic;">Entering an override value unlinks the row — it will keep that value until you click Clear on the Proposed cell.</div>';
  return html;
}

// Toggle the expand/collapse of the linked GL breakdown
function togglePrLinkedBreakdown() {
  const panel = document.getElementById('prLinkedBreakdown');
  const arrow = document.getElementById('prLinkedArrow');
  if (!panel) return;
  const isShown = panel.style.display !== 'none';
  panel.style.display = isShown ? 'none' : 'block';
  if (arrow) arrow.style.transform = isShown ? '' : 'rotate(90deg)';
}

// Apply a manual override on a linked GL from the breakdown panel
async function prOverrideLinkedGL(input) {
  const gl = input.dataset.gl;
  const raw = parseDollar(input.value);
  if (!raw || isNaN(raw)) { input.focus(); return; }
  const line = _payrollGLLines.find(l => l.gl_code === gl);
  if (!line) return;
  const rounded = Math.round(raw);
  line.proposed_budget = rounded;
  line.proposed_formula = 'manual';
  line._linked = false;
  const curr = float(line.current_budget || 0);
  line.increase_pct = curr ? (rounded / curr - 1) : 0;

  // Persist to DB
  try {
    await fetch('/api/fa-lines/' + entityCode, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lines: [{
        gl_code: gl,
        proposed_budget: rounded,
        proposed_formula: 'manual',
        increase_pct: line.increase_pct
      }]})
    });
  } catch(e) { console.error('Override save failed:', e); }

  // Re-render GL + tie-out
  renderPayrollGL();
  renderPayrollTieOut(window._payrollCalcTotal || 0);
  // Re-open breakdown since render just replaced it
  const panel = document.getElementById('prLinkedBreakdown');
  if (panel) panel.style.display = 'block';
  const arrow = document.getElementById('prLinkedArrow');
  if (arrow) arrow.style.transform = 'rotate(90deg)';
}

// ── GL Note & Increase Save Helpers ───────────────────────────────────────

async function savePrGLNote(el) {
  const glCode = el.dataset.gl;
  const note = el.value;
  const ec = entityCode;
  try {
    await fetch('/api/fa-lines/' + ec, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lines: [{gl_code: glCode, notes: note}]})
    });
  } catch(e) { console.error('Failed to save GL note:', e); }
}

async function savePrGLIncrease(el) {
  const glCode = el.dataset.gl;
  const pctStr = el.value.replace('%', '').trim();
  const pct = parseFloat(pctStr) / 100 || 0;
  const ec = entityCode;
  const line = _payrollGLLines.find(l => l.gl_code === glCode);
  if (!line) return;
  line.increase_pct = pct;
  const curr = float(line.current_budget);
  line.proposed_budget = curr * (1 + pct);
  try {
    await fetch('/api/fa-lines/' + ec, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lines: [{gl_code: glCode, increase_pct: pct, proposed_budget: line.proposed_budget}]})
    });
    renderPayrollGL();
  } catch(e) { console.error('Failed to save GL increase:', e); }
}

// ═══════════════════════════════════════════════════════════════════════════
// END PAYROLL TAB
// ═══════════════════════════════════════════════════════════════════════════

function renderEditableSheet(sheetName, sheetLines, contentDiv) {
  const NC = 15;
  const estLbl = estimateLabel();

  // Inject PM-style CSS if not already present
  if (!document.getElementById('faSheetStyle')) {
    const style = document.createElement('style');
    style.id = 'faSheetStyle';
    style.textContent = `
      .fa-grid { background:white; border-radius:12px; border:1px solid var(--gray-200); overflow:hidden; }
      .fa-grid-scroll { overflow-x:scroll; max-height:75vh; overflow-y:auto; }
      .fa-grid-scroll::-webkit-scrollbar { width:10px; height:12px; }
      .fa-grid-scroll::-webkit-scrollbar-track { background:var(--gray-100); border-radius:6px; }
      .fa-grid-scroll::-webkit-scrollbar-thumb { background:#8b7355; border-radius:6px; min-height:40px; }
      .fa-grid-scroll::-webkit-scrollbar-thumb:hover { background:#6b5740; }
      .fa-grid-scroll::-webkit-scrollbar-corner { background:var(--gray-100); }
      .fa-grid table { border-collapse:separate; border-spacing:0; font-size:13px; width:100%; }
      .fa-grid thead { position:sticky; top:0; z-index:20; }
      .fa-grid th { padding:8px 6px; text-align:left; font-weight:600; border-bottom:2px solid var(--gray-300); white-space:nowrap; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:var(--gray-500); background:var(--gray-100); }
      .fa-grid th.num { text-align:right; }
      .fa-grid td, .fa-grid th { white-space:nowrap; width:1px; }
      .fa-grid td { padding:6px 6px; border-bottom:1px solid var(--gray-200); }
      .fa-grid td.num { text-align:right; font-variant-numeric:tabular-nums; }
      .fa-grid tbody tr:hover td { background:#eef2ff; }
      .fa-grid tbody tr:hover td.frozen { background:#ede5d8; }
      .fa-grid th.frozen, .fa-grid td.frozen { position:sticky; z-index:15; background:white; }
      .fa-grid thead th.frozen { z-index:25; background:var(--gray-100); }
      .fa-grid .frozen-gl { left:0; min-width:80px; }
      .fa-grid .frozen-desc { left:80px; min-width:180px; width:auto; border-right:2px solid var(--gray-300); box-shadow:2px 0 8px rgba(90,74,63,0.08); }
      .fa-grid thead th.frozen.frozen-desc { width:auto; min-width:180px; }
      .fa-grid .col-notes { color:var(--gray-500); font-size:12px; min-width:40px; text-align:center; }
      .fa-grid .cat-hdr td { background:var(--blue-light, #f5efe7); font-weight:700; color:var(--blue, #5a4a3f); font-size:14px; padding:10px 10px; border-bottom:2px solid var(--blue, #5a4a3f); }
      .fa-grid .cat-hdr td.frozen { background:var(--blue-light, #f5efe7); }
      .fa-grid .sub-row td { background:var(--gray-100); font-weight:700; border-top:2px solid var(--gray-300); }
      .fa-grid .sub-row td.frozen { background:var(--gray-100); }
      .fa-grid .total-row td { background:#1e3a5f; color:white; font-weight:700; font-size:14px; }
      .fa-grid .total-row td.frozen { background:#1e3a5f; color:white; }
      .fa-grid tr.drill-row td.frozen { border-right:none; box-shadow:none; }
      .fa-grid .cell { min-width:50px; width:auto; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; text-align:right; background:#fbfaf4; cursor:text; }
      .fa-grid .cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px var(--blue-light, #f5efe7); }
      .fa-grid .cell-fx { background:transparent; border-color:#e5e1d8; box-shadow:inset 3px 0 0 #16a34a; color:#15803d; }
      .fa-grid .cell-fx:focus { background:#ecfdf5; }
      .fa-fx { display:none !important; }
      .fa-grid .sub-row td.fx-td { background:#e8f5e9; }
      .fa-grid .sub-row td.fx-td .sub-val { color:#1b5e20; }
      .fa-grid .total-row td.fx-td { background:#1a3d2e; }
      .fa-grid .total-row td.fx-td .sub-val { color:#a5d6a7; }
      .fa-grid .cell-notes { text-align:left; min-width:100px; width:100%; }
      .fa-grid .cell-pct { min-width:45px; width:auto; }
      .fa-invoice-detail > td { padding:0 !important; }
      .fa-invoice-detail:hover { background:transparent !important; }
      .fa-invoice-detail .drill-sticky, .fa-grid .drill-sticky { position:sticky; left:220px; z-index:10; width:fit-content; min-width:850px; }
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
    '<span><span class="fa-legend-dot" style="background:#fbfaf4;"></span>Editable</span>' +
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
    '<th class="frozen frozen-gl">GL Code</th><th class="frozen frozen-desc">Description</th>' +
    '<th class="num">Prior Year</th><th class="num">YTD Actual</th>' +
    '<th class="num">Accrual Adj</th><th class="num">Unpaid Bills</th>' +
    '<th class="num">' + estLbl + ' Est</th><th class="num">12 Mo Forecast</th>' +
    '<th class="num">Curr Budget</th><th class="num">Inc %</th>' +
    '<th class="num">Proposed</th><th class="num">$ Var</th><th class="num">% Chg</th>' +
    '<th class="col-notes">Notes</th>' +
    '</tr></thead><tbody>';

  const catConfig = SHEET_CATEGORIES[sheetName];

  function buildLineRow(l) {
    const gl = l.gl_code;
    const prior = l.prior_year || 0;
    const ytd = l.ytd_actual || 0;
    const accrual = l.accrual_adj || 0;
    const unpaid = l.unpaid_bills || 0;
    const budget = l.current_budget || 0;
    const isZero = !prior && !ytd && !accrual && !unpaid && !budget && !(l.increase_pct);
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
    const oneTimeBadge = faIsOneTimeFeeBilled(l) ? ' <span title="One-time annual fee — forecast = YTD only" style="background:#ffedd5; color:#ea580c; font-size:10px; font-weight:700; padding:2px 6px; border-radius:8px; border:1px solid #fdba74; letter-spacing:0.5px; cursor:help;">1×</span>' : '';

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
        badge = '<span class="fa-fx" style="background:#f97316; color:#fff; border-color:#ea580c;">✎</span>';
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
      '<td class="frozen frozen-gl"><span style="font-size:13px; font-variant-numeric:tabular-nums;">' + gl + '</span>' + reclassBadge + '</td>' +
      '<td class="frozen frozen-desc"><a href="#" onclick="faToggleInvoices(\'' + gl + '\', this); return false;" style="color:inherit; text-decoration:none; cursor:pointer;" title="Click to view expenses">' + l.description + ' <span class="fa-drill-arrow" style="font-size:10px; color:var(--gray-400);">▶</span></a>' + oneTimeBadge + '</td>' +
      '<td class="num">' + $cell('pr_'+gl, 'prior_year', prior) + '</td>' +
      '<td class="num">' + $cell('ytd_'+gl, 'ytd_actual', ytd) + '</td>' +
      '<td class="num">' + $cell('acc_'+gl, 'accrual_adj', accrual) + '</td>' +
      '<td class="num">' + $cell('unp_'+gl, 'unpaid_bills', unpaid) + '</td>' +
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
        ' style="cursor:pointer; pointer-events:none;"></td>' +
      '<td class="col-notes"><input class="cell cell-notes" type="text" value="' + (l.notes||'').replace(/"/g,'&quot;') + '" data-gl="' + gl + '" data-field="notes" onchange="faAutoSave(\'' + gl + '\',\'notes\',this.value)"></td></tr>';
  }

  function sumLines(lines) {
    const t = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
    lines.forEach(l => {
      t.prior += l.prior_year || 0;
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
    const v = t.budget - t.forecast;
    const p = t.forecast ? ((t.budget - t.forecast)/t.forecast) : 0;
    const idAttr = rowId ? ' id="' + rowId + '"' : '';
    const isTotal = cls === 'total-row';
    const bs = isTotal ? 'background:rgba(255,255,255,0.2); color:white; border-color:rgba(255,255,255,0.4);' : '';
    function fxTd(val, col) {
      return '<td class="num fx-td" style="position:relative; cursor:pointer;" data-col="' + col + '" data-raw="' + Math.round(val) + '" onclick="fxSubtotalFocus(this)">' +
        '<span class="sub-val">' + fmt(val) + '</span></td>';
    }
    const vc = v >= 0 ? 'var(--red)' : 'var(--green)';
    return '<tr class="' + (cls||'sub-row') + '"' + idAttr + '>' +
      '<td class="frozen frozen-gl"></td><td class="frozen frozen-desc">' + label + '</td>' +
      fxTd(t.prior, 'prior') +
      fxTd(t.ytd, 'ytd') +
      fxTd(t.accrual, 'accrual') +
      fxTd(t.unpaid, 'unpaid') +
      fxTd(t.estimate, 'estimate') +
      fxTd(t.forecast, 'forecast') +
      fxTd(t.budget, 'budget') +
      '<td class="num"></td>' +
      fxTd(t.proposed, 'proposed') +
      '<td class="num fx-td" style="position:relative; cursor:pointer; color:' + vc + ';" data-col="variance" data-raw="' + Math.round(v) + '" onclick="fxSubtotalFocus(this)"><span class="sub-val">' + fmt(v) + '</span></td>' +
      '<td class="num fx-td" style="position:relative; cursor:pointer;" data-col="pctchange" data-raw="' + p + '" onclick="fxSubtotalFocus(this)"><span class="sub-val">' + (p*100).toFixed(1) + '%</span></td>' +
      '<td class="col-notes"></td></tr>';
  }

  // Build category groups and populate _catGroupGLs for live recalculation
  window._catGroupGLs = {};
  if (catConfig) {
    catConfig.groups.forEach(grp => {
      const gl = sheetLines.filter(grp.match);
      if (gl.length === 0) return;
      window._catGroupGLs[grp.key] = gl.map(l => l.gl_code);
      html += '<tr class="cat-hdr"><td class="frozen frozen-gl"></td><td class="frozen frozen-desc">' + grp.label + '</td><td colspan="' + (NC - 2) + '"></td></tr>';
      gl.forEach(l => { html += buildLineRow(l); });
      html += subtotalRow('Total ' + grp.label, sumLines(gl), null, 'subtotal_' + grp.key);
    });
    const allGrouped = catConfig.groups.flatMap(g => sheetLines.filter(g.match));
    const ungrouped = sheetLines.filter(l => !allGrouped.includes(l));
    if (ungrouped.length > 0) {
      window._catGroupGLs['other'] = ungrouped.map(l => l.gl_code);
      html += '<tr class="cat-hdr"><td class="frozen frozen-gl"></td><td class="frozen frozen-desc" style="color:var(--gray-500); border-color:var(--gray-300);">Other</td><td colspan="' + (NC - 2) + '"></td></tr>';
      ungrouped.forEach(l => { html += buildLineRow(l); });
      html += subtotalRow('Total Other', sumLines(ungrouped), null, 'subtotal_other');
    }
  } else {
    sheetLines.forEach(l => { html += buildLineRow(l); });
  }

  html += subtotalRow('Sheet Total', sumLines(sheetLines), 'total-row', 'faSheetTotal');
  html += '</tbody></table></div></div>';
  contentDiv.innerHTML = html;
  // Auto-size numeric columns after render
  autoSizeColumns(contentDiv.querySelector('table'));
}

/* ── Grid Viewport Fit — keep horizontal scrollbar visible ────────── */
function faFitGridToViewport() {
  const gs = document.querySelector('.fa-grid-scroll');
  if (!gs) return;
  const rect = gs.getBoundingClientRect();
  const available = window.innerHeight - rect.top - 16;
  gs.style.maxHeight = Math.max(120, available) + 'px';
}
faFitGridToViewport();
window.addEventListener('resize', faFitGridToViewport);
window.addEventListener('scroll', faFitGridToViewport);
document.querySelector('.fa-grid-scroll')?.addEventListener('scroll', faFitGridToViewport);

/* ── Column Auto-Sizer ─────────────────────────────────────────────── */
function autoSizeColumns(table) {
  if (!table) return;
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  ctx.font = '13px Arial';
  const cols = table.querySelectorAll('thead th');
  const colWidths = [];
  cols.forEach((th, ci) => {
    if (th.classList.contains('frozen')) { colWidths.push(null); return; }
    let maxPx = 0;
    table.querySelectorAll('tbody tr').forEach(tr => {
      const td = tr.children[ci];
      if (!td) return;
      const inp = td.querySelector('input');
      if (inp) {
        const w = Math.ceil(ctx.measureText(inp.value || '').width);
        if (w > maxPx) maxPx = w;
      } else {
        const span = td.querySelector('.sub-val') || td;
        const w = Math.ceil(ctx.measureText((span.textContent || '').trim()).width);
        if (w > maxPx) maxPx = w;
      }
    });
    colWidths.push(maxPx + 20);
  });
  table.querySelectorAll('tbody tr').forEach(tr => {
    cols.forEach((th, ci) => {
      if (!colWidths[ci]) return;
      const td = tr.children[ci];
      if (!td) return;
      const inp = td.querySelector('input');
      if (inp && !inp.classList.contains('cell-notes')) {
        inp.style.width = Math.max(colWidths[ci], 55) + 'px';
      }
    });
  });
}

function computeForecast(l) {
  const ytdActual = l.ytd_actual || 0;
  const accrualAdj = l.accrual_adj || 0;
  const unpaidBills = l.unpaid_bills || 0;
  const ytdTotal = ytdActual + accrualAdj + unpaidBills;
  const ytdMonths = (typeof YTD_MONTHS !== 'undefined' && YTD_MONTHS > 0) ? YTD_MONTHS : 2;
  const remaining = (typeof REMAINING_MONTHS !== 'undefined') ? REMAINING_MONTHS : (12 - ytdMonths);
  return ytdTotal + (ytdTotal / ytdMonths) * remaining;
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
/* Force scrollbars always visible (fixes macOS auto-hide on horizontal/vertical scroll) */
::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
::-webkit-scrollbar-track { background: #f1f5f9; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
::-webkit-scrollbar-corner { background: #f1f5f9; }
* { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
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
    max-width: 100%;
    margin: 0 auto;
    padding: 40px 40px;
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
  .summary-bar { display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap; }
  .summary-chip { padding: 6px 14px; border-radius: 20px; font-size: 12px; font-weight: 600; }
  .chip-action { background: #fef3c7; color: #92400e; }
  .chip-waiting { background: #e0e7ff; color: #3730a3; }
  .chip-done { background: #dcfce7; color: #166534; }
  .chip-total { background: var(--gray-100); color: var(--gray-700); }
  .buildings-list { display: flex; flex-direction: column; gap: 12px; margin-top: 16px; }
  .building-card {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 12px;
    padding: 18px 20px;
    text-decoration: none;
    color: var(--gray-900);
    transition: all 0.15s;
    border-left: 4px solid var(--gray-300);
  }
  .building-card:hover {
    border-color: var(--gray-200);
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
  }
  .building-card.card-action { border-left-color: #dc2626; }
  .building-card.card-waiting { border-left-color: #d97706; }
  .building-card.card-done { border-left-color: #16a34a; }
  .building-card.card-notready { border-left-color: var(--gray-300); opacity: 0.6; }
  .card-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
  .card-top h3 {
    font-size: 15px;
    font-weight: 600;
    color: var(--gray-900);
  }
  .card-top h3 span { font-size: 12px; font-weight: 400; color: var(--gray-500); margin-left: 8px; }
  .card-meta { display: flex; gap: 16px; font-size: 11px; color: var(--gray-500); margin-bottom: 8px; }
  .card-actions { display: flex; justify-content: flex-end; }
  .card-btn { font-size: 12px; padding: 5px 14px; border-radius: 6px; border: none; cursor: pointer; font-weight: 600; text-decoration: none; display: inline-block; }
  .card-btn-primary { background: var(--blue); color: white; }
  .card-btn-secondary { background: var(--gray-100); color: var(--gray-700); }
  .days-badge { font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 10px; }
  .days-red { background: #fef2f2; color: #dc2626; }
  .days-yellow { background: #fffbeb; color: #d97706; }
  .days-green { background: #f0fdf4; color: #16a34a; }
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

  <div id="pm-summary" class="summary-bar" style="display:none;"></div>
  <div class="buildings-list" id="buildings-grid" style="display: none;"></div>
</div>

<script>
// fa_review included so PM can re-enter a building after submitting for FA review
const editableStatuses = ['pm_pending', 'pm_in_progress', 'returned', 'fa_review'];
const statusLabels = {
  'not_started': 'Not Started',
  'data_collection': 'Data Collection',
  'data_ready': 'Data Ready',
  'draft': 'Draft',
  'pm_pending': 'Pending PM',
  'pm_in_progress': 'PM In Progress',
  'fa_review': 'FA Review',
  'exec_review': 'Exec Review',
  'presentation': 'Presentation',
  'approved': 'Approved',
  'returned': 'Returned',
  'ar_pending': 'AR Pending',
  'ar_complete': 'AR Complete'
};
// Fallback: any unknown status gets snake_case → Title Case automatically
function formatStatus(s) {
  if (!s) return '';
  if (statusLabels[s]) return statusLabels[s];
  return s.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}

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

function calcDays(updatedAt) {
  if (!updatedAt) return 0;
  return Math.floor((Date.now() - new Date(updatedAt).getTime()) / 86400000);
}

function renderBuildings(userId) {
  const grid = document.getElementById('buildings-grid');
  const summary = document.getElementById('pm-summary');
  const userAssignments = allAssignments.filter(a => a.user_id === userId && a.role === 'pm');

  // If PM has assignments, show those buildings; otherwise show all budgets (demo mode)
  let buildingList = [];
  if (userAssignments.length > 0) {
    buildingList = userAssignments.map(a => {
      const budget = allBudgets.find(b => b.entity_code === a.entity_code);
      return { entity_code: a.entity_code, budget };
    });
  } else {
    buildingList = allBudgets.map(b => ({ entity_code: b.entity_code, budget: b }));
  }

  if (buildingList.length === 0) {
    grid.style.display = 'none';
    summary.style.display = 'none';
    return;
  }

  // Classify each building
  const actionStatuses = ['pm_pending', 'pm_in_progress', 'returned'];
  const doneStatuses = ['approved', 'ar_pending', 'ar_complete'];
  buildingList.forEach(item => {
    const s = item.budget ? item.budget.status : null;
    const days = item.budget ? calcDays(item.budget.updated_at) : 0;
    item.days = days;
    if (actionStatuses.includes(s)) { item.tier = 0; item.cardClass = days >= 14 ? 'card-action' : 'card-waiting'; }
    else if (s === 'fa_review') { item.tier = 1; item.cardClass = 'card-waiting'; }
    else if (doneStatuses.includes(s)) { item.tier = 2; item.cardClass = 'card-done'; }
    else { item.tier = 3; item.cardClass = 'card-notready'; }
  });
  // Sort: action items first (longest waiting), then done, then not-ready
  buildingList.sort((a, b) => a.tier - b.tier || b.days - a.days);

  // Summary chips
  const needReview = buildingList.filter(i => i.tier === 0).length;
  const awaitingFA = buildingList.filter(i => i.tier === 1).length;
  const done = buildingList.filter(i => i.tier === 2).length;
  summary.innerHTML = '<span class="summary-chip chip-total">' + buildingList.length + ' buildings</span>' +
    (needReview ? '<span class="summary-chip chip-action">' + needReview + ' need your review</span>' : '') +
    (awaitingFA ? '<span class="summary-chip chip-waiting">' + awaitingFA + ' awaiting FA</span>' : '') +
    (done ? '<span class="summary-chip chip-done">' + done + ' approved</span>' : '');
  summary.style.display = 'flex';

  grid.innerHTML = '';
  grid.style.display = 'flex';

  buildingList.forEach(item => {
    const buildingName = item.budget ? (item.budget.building_name || getBuildingName(item.entity_code)) : getBuildingName(item.entity_code);
    const budgetStatus = item.budget ? item.budget.status : null;
    const isEditable = editableStatuses.includes(budgetStatus);
    const statusLabel = budgetStatus ? formatStatus(budgetStatus) : 'No Budget';
    const pillClass = budgetStatus ? 'pill-' + budgetStatus : 'pill-draft';

    // Days badge
    let daysBadge = '';
    if (item.tier <= 1 && item.days > 0) {
      const dc = item.days >= 14 ? 'days-red' : item.days >= 7 ? 'days-yellow' : 'days-green';
      daysBadge = '<span class="days-badge ' + dc + '">' + item.days + 'd waiting</span>';
    }

    // Action button
    let btn = '';
    if (item.tier === 0 && budgetStatus === 'pm_pending') {
      btn = '<a href="/pm/' + item.entity_code + '" class="card-btn card-btn-primary">Start Review &rarr;</a>';
    } else if (item.tier === 0) {
      btn = '<a href="/pm/' + item.entity_code + '" class="card-btn card-btn-primary">Continue Review &rarr;</a>';
    } else if (item.tier === 1) {
      btn = '<a href="/pm/' + item.entity_code + '" class="card-btn card-btn-secondary">View &rarr;</a>';
    } else if (item.tier === 2) {
      btn = '<a href="/pm/' + item.entity_code + '" class="card-btn card-btn-secondary">View &rarr;</a>';
    } else {
      btn = '<span style="font-size:11px; color:var(--gray-500);">Not sent to PM yet</span>';
    }

    const card = document.createElement('div');
    card.className = 'building-card ' + item.cardClass;
    card.innerHTML =
      '<div class="card-top"><h3>' + buildingName + '<span>Entity ' + item.entity_code + '</span></h3>' +
        '<div style="display:flex; gap:8px; align-items:center;">' + daysBadge + '<span class="status-pill ' + pillClass + '">' + statusLabel + '</span></div></div>' +
      '<div class="card-actions">' + btn + '</div>';
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
/* Force scrollbars always visible (fixes macOS auto-hide on horizontal/vertical scroll) */
::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
::-webkit-scrollbar-track { background: #f1f5f9; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
::-webkit-scrollbar-corner { background: #f1f5f9; }
* { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
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
    padding: 6px 12px;
    border-radius: 999px;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    transition: all 0.2s ease;
    font-weight: 500;
  }
  .save-indicator.saving {
    color: var(--orange);
    background: #fff7ed;
    border: 1px solid #fed7aa;
  }
  .save-indicator.saving::before {
    content: '';
    width: 10px; height: 10px;
    border-radius: 50%;
    background: var(--orange);
    animation: save-pulse 1s ease-in-out infinite;
  }
  .save-indicator.saved {
    color: var(--green);
    background: #f0fdf4;
    border: 1px solid #bbf7d0;
  }
  .save-indicator.saved::before { content: '\2713'; font-weight: 700; }
  .save-indicator.failed {
    color: white;
    background: var(--red);
    border: 1px solid #991b1b;
    font-weight: 600;
    cursor: pointer;
    animation: save-fail-pulse 1.6s ease-in-out infinite;
  }
  .save-indicator.failed::before { content: '\26A0'; font-size: 14px; }
  @keyframes save-pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.4; transform: scale(0.7); }
  }
  @keyframes save-fail-pulse {
    0%   { box-shadow: 0 0 0 0 rgba(220, 38, 38, 0.7); }
    70%  { box-shadow: 0 0 0 12px rgba(220, 38, 38, 0); }
    100% { box-shadow: 0 0 0 0 rgba(220, 38, 38, 0); }
  }

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
  .grid-container { overflow-x: scroll; max-height: 75vh; overflow-y: auto; }
  .grid-container::-webkit-scrollbar { width:10px; height:12px; }
  .grid-container::-webkit-scrollbar-track { background:var(--gray-100); border-radius:6px; }
  .grid-container::-webkit-scrollbar-thumb { background:#8b7355; border-radius:6px; min-height:40px; }
  .grid-container::-webkit-scrollbar-thumb:hover { background:#6b5740; }
  .grid-container::-webkit-scrollbar-corner { background:var(--gray-100); }

  table { border-collapse: separate; border-spacing: 0; font-size: 13px; width: 100%; }
  .grid-container > table > thead { position: sticky; top: 48px; z-index: 20; }
  /* Inner drill-down tables (invoice details) must NOT inherit sticky thead */
  .invoice-detail-row table thead,
  .invoice-detail-row table thead tr,
  .invoice-detail-row table thead th,
  .invoice-detail-row table thead td { position: static !important; top: auto !important; }
  th {
    padding: 8px 6px;
    text-align: left;
    font-weight: 600;
    border-bottom: 2px solid var(--gray-300);
    white-space: nowrap;
    background: var(--gray-100);
  }
  th.number { text-align: right; }
  td, th { white-space: nowrap; width: 1px; }
  td { padding: 6px 6px; border-bottom: 1px solid var(--gray-200); }
  td.number { text-align: right; font-variant-numeric: tabular-nums; }
  tbody tr:hover td { background: var(--blue-light); }
  tbody tr:hover td.frozen { background: #ede5d8; }
  /* Frozen columns */
  th.frozen, td.frozen { position: sticky; z-index: 15; background: white; }
  thead th.frozen { z-index: 25; background: var(--gray-100); }
  .frozen-gl { left: 0; min-width: 80px; }
  .frozen-desc { left: 80px; min-width: 180px; width: auto; border-right: 2px solid var(--gray-300); box-shadow: 2px 0 8px rgba(90,74,63,0.08); }
  thead th.frozen.frozen-desc { width: auto; min-width: 180px; }
  .col-notes { color: var(--gray-500); font-size: 12px; min-width: 40px; text-align: center; }
  .col-notes input.note-warn { background: #fef3c7; border-color: #fbbf24; }
  .col-notes input.note-warn::placeholder { color: #92400e; font-weight: 500; }

  .category-header td {
    background: var(--blue-light);
    font-weight: 700;
    color: var(--blue);
    font-size: 14px;
    padding: 10px 10px;
    border-bottom: 2px solid var(--blue);
  }
  .category-header td.frozen { background: var(--blue-light); }
  .subtotal-row td {
    background: var(--gray-100);
    font-weight: 700;
    border-top: 2px solid var(--gray-300);
  }
  .subtotal-row td.frozen { background: var(--gray-100); }
  .grand-total td {
    background: #1e3a5f;
    color: white;
    font-weight: 700;
    font-size: 14px;
  }
  .grand-total td.frozen { background: #1e3a5f; color: white; }
  /* Reclass/invoice drill-down rows — clean frozen cell borders */
  tr.drill-row td.frozen { border-right: none; box-shadow: none; }

  input[type="number"], input[type="text"] {
    padding: 5px 8px;
    border: 1px solid var(--gray-300);
    border-radius: 4px;
    font-size: 13px;
    background: #fbfaf4;
  }
  input[type="number"] { text-align: right; min-width: 55px; width: auto; }
  input[type="text"] { min-width: 140px; width: 100%; }
  input.pm-cell, input.pm-cell-fx { width: auto; min-width: 55px; }
  input.pm-cell-pct { width: auto; min-width: 45px; }
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

  .invoice-detail-row > td { padding: 0 !important; }
  .invoice-detail-row:hover { background: transparent !important; }
  .invoice-detail-row .drill-sticky, .drill-sticky { position:sticky; left:220px; z-index:10; width:fit-content; min-width:850px; }

  /* PM Cell Styles */
  .pm-cell { min-width:50px; width:auto; padding:4px 6px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; text-align:right; background:#fbfaf4; cursor:text; font-variant-numeric:tabular-nums; }
  .pm-cell:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 2px var(--blue-light, #f5efe7); }
  input.pm-cell-fx { background:transparent; border:1px solid #e5e1d8; box-shadow:inset 3px 0 0 #16a34a; color:#15803d; }
  input.pm-cell-fx:focus { background:#ecfdf5; }
  .pm-fx { display:none !important; }
  .subtotal-row td.pm-fx-td { background:#e8f5e9; }
  .subtotal-row td.pm-fx-td .sub-val { color:#1b5e20; }
  .grand-total td.pm-fx-td { background:#1a3d2e; }
  .grand-total td.pm-fx-td .sub-val { color:#a5d6a7; }
  .pm-cell-pct { min-width:45px; width:auto; }
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

  <!-- My Changes Summary Panel (read-only) -->
  <div id="pmMyChangesPanel" style="display:none; background:white; border-radius:12px; box-shadow:0 1px 3px rgba(0,0,0,0.1); border:1px solid var(--gray-200); margin-bottom:16px;">
    <div onclick="this.nextElementSibling.classList.toggle('pm-panel-hidden'); this.querySelector('.pm-chev').classList.toggle('pm-chev-closed');" style="display:flex; align-items:center; justify-content:space-between; padding:14px 20px; cursor:pointer; background:linear-gradient(135deg, var(--blue-light) 0%, #e8e0d4 100%); border-radius:12px 12px 0 0; border-bottom:1px solid var(--gray-200);">
      <h3 style="font-size:14px; font-weight:700; color:var(--blue); display:flex; align-items:center; gap:8px;">
        My Changes
        <span id="pmMyChangesBadge" style="background:var(--blue); color:white; font-size:11px; font-weight:700; padding:2px 10px; border-radius:10px;"></span>
      </h3>
      <span class="pm-chev" style="font-size:12px; color:var(--gray-500); transition:transform 0.2s;">▾</span>
    </div>
    <div class="pm-panel-body">
      <div id="pmMyChangesTabs" style="display:flex; border-bottom:1px solid var(--gray-200); background:var(--gray-50);">
        <div class="pm-mc-tab active" onclick="switchPmMcTab(this,'pmMyNotesContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--blue); cursor:pointer; border-bottom:2px solid var(--blue); background:white;">My Notes <span id="pmMyNotesCount" style="background:var(--blue-light); color:var(--blue); font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
        <div class="pm-mc-tab" onclick="switchPmMcTab(this,'pmMyReclassContent')" style="padding:10px 20px; font-size:13px; font-weight:600; color:var(--gray-500); cursor:pointer; border-bottom:2px solid transparent;">My Reclasses <span id="pmMyReclassCount" style="background:#fef3c7; color:#92400e; font-size:11px; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:4px;"></span></div>
      </div>
      <!-- My Notes Tab -->
      <div id="pmMyNotesContent" style="padding:16px 20px;">
        <div id="pmMyNotesEmpty" style="text-align:center; padding:20px; color:var(--gray-500); font-size:13px; display:none;">You haven't added any notes yet.</div>
        <div id="pmMyNotesContainer"></div>
      </div>
      <!-- My Reclasses Tab -->
      <div id="pmMyReclassContent" style="padding:16px 20px; display:none;">
        <div id="pmMyReclassEmpty" style="text-align:center; padding:20px; color:var(--gray-500); font-size:13px; display:none;">No invoice reclasses yet.</div>
        <table id="pmMyReclassTable" style="width:100%; border-collapse:collapse; font-size:13px;">
          <thead><tr>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">From GL</th>
            <th style="font-size:11px; padding:6px 4px; border-bottom:1px solid var(--gray-200);"></th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">To GL</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Invoices</th>
            <th style="text-align:right; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">Amount</th>
            <th style="text-align:left; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">My Note</th>
            <th style="text-align:center; font-size:11px; font-weight:600; color:var(--gray-500); text-transform:uppercase; padding:6px 10px; border-bottom:1px solid var(--gray-200);">FA Status</th>
          </tr></thead>
          <tbody id="pmMyReclassBody"></tbody>
        </table>
      </div>
      <div style="padding:10px 20px; background:var(--gray-50); border-top:1px solid var(--gray-200); border-radius:0 0 12px 12px; font-size:11px; color:var(--gray-500);">
        Read-only summary of your changes. FA will review and accept/reject in their dashboard.
      </div>
    </div>
  </div>
  <style>
    .pm-panel-hidden { display: none !important; }
    .pm-chev-closed { transform: rotate(-90deg); }
    .pm-sheet-tabs { display:flex; gap:4px; padding:0 0 12px 0; }
    .pm-sheet-tab { padding:8px 20px; font-size:13px; font-weight:600; border:1px solid var(--gray-300); border-radius:8px 8px 0 0; cursor:pointer; background:var(--gray-100); color:var(--gray-600); transition:all 0.15s; }
    .pm-sheet-tab:hover { background:var(--gray-200); }
    .pm-sheet-tab.active { background:white; color:var(--blue); border-bottom:2px solid var(--blue); box-shadow:0 -1px 3px rgba(0,0,0,0.06); }
  </style>

  <div class="pm-sheet-tabs">
    <div class="pm-sheet-tab active" onclick="pmSwitchSheet('Repairs &amp; Supplies', this)">Repairs &amp; Supplies <span id="rsCount" style="background:var(--blue);color:white;font-size:10px;padding:1px 6px;border-radius:10px;margin-left:4px;"></span></div>
    <div class="pm-sheet-tab" onclick="pmSwitchSheet('Gen &amp; Admin', this)">General &amp; Admin <span id="gaCount" style="background:var(--blue);color:white;font-size:10px;padding:1px 6px;border-radius:10px;margin-left:4px;"></span></div>
  </div>

  <div class="grid-wrapper">
    <div class="grid-container">
      <div id="pmFormulaBarWrap" style="display:flex; align-items:center; gap:8px; padding:8px 16px; background:#f8fafc; border:1px solid var(--gray-200); border-radius:8px; margin-bottom:0; position:sticky; top:0; z-index:50; box-shadow:0 2px 4px rgba(0,0,0,0.04);">
        <span style="font-size:11px; font-weight:700; color:var(--blue); background:var(--blue-light, #e1effe); border:1px solid var(--blue); border-radius:4px; padding:2px 8px; white-space:nowrap;">fx</span>
        <span id="pmFormulaLabel" style="display:none; font-size:11px; font-weight:600; color:var(--gray-600); white-space:nowrap; min-width:100px;"></span>
        <input id="pmFormulaBar" type="text" placeholder="Click a green formula cell to view its formula..." style="display:block; flex:1; padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; font-family:monospace; background:white;" oninput="pmFormulaBarPreview()" onkeydown="pmFormulaBarKeydown(event)">
        <span id="pmFormulaPreview" style="display:none; font-size:13px; font-weight:600; color:var(--green); white-space:nowrap; min-width:80px; text-align:right;"></span>
        <button id="pmFormulaAccept" style="display:none; padding:4px 14px; font-size:12px; font-weight:600; background:var(--green); color:white; border:none; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarAccept()">Accept</button>
        <button id="pmFormulaCancel" style="display:none; padding:4px 14px; font-size:12px; font-weight:500; background:var(--gray-200); color:var(--gray-700); border:none; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarCancel()">Cancel</button>
        <button id="pmFormulaClear" style="display:none; padding:4px 10px; font-size:11px; background:#fef2f2; color:var(--red); border:1px solid #fecaca; border-radius:4px; cursor:pointer;" onclick="pmFormulaBarClear()" title="Remove formula, revert to auto-calc">Clear</button>
      </div>
      <table id="linesTable">
        <thead>
          <tr>
            <th class="frozen frozen-gl">GL Code</th>
            <th class="frozen frozen-desc">Description</th>
            <th class="number">Prior Year<br>Actual</th>
            <th class="number">YTD<br>Actual</th>
            <th class="number">Accrual<br>Adj</th>
            <th class="number">Unpaid<br>Bills</th>
            <th class="number">{{ estimate_label }}<br>Estimate <span style="font-size:9px; color:var(--blue); background:var(--blue-light, #f5efe7); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">fx</span></th>
            <th class="number">12 Month<br>Forecast <span style="font-size:9px; color:var(--blue); background:var(--blue-light, #f5efe7); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">fx</span></th>
            <th class="number">Current<br>Budget</th>
            <th class="number">Increase<br>%</th>
            <th class="number">Proposed<br>Budget <span style="font-size:9px; color:var(--blue); background:var(--blue-light, #f5efe7); padding:0 3px; border-radius:3px; border:1px solid var(--blue);">fx</span></th>
            <th class="number">$<br>Variance</th>
            <th class="number">%<br>Change</th>
            <th class="col-notes">Notes</th>
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
const BUDGET_STATUS = "{{ budget_status }}";
const LINES = {{ lines_json | safe }};
const ALL_GL_CODES = {{ all_gl_json | safe }};
const YTD_MONTHS = {{ ytd_months }};
const REMAINING_MONTHS = {{ remaining_months }};

// Sheet tab config
let _pmActiveSheet = 'Repairs & Supplies';
const PM_SHEET_CATEGORIES = {
  'Repairs & Supplies': {
    cats: {supplies: [], repairs: [], maintenance: []},
    labels: {supplies: 'Supplies', repairs: 'Repairs', maintenance: 'Maintenance Contracts'},
    match: function(l) { return l.sheet_name === 'Repairs & Supplies'; },
    assign: function(l) { return l.category; },
    grandLabel: 'GRAND TOTAL R&M'
  },
  'Gen & Admin': {
    cats: {prof_fees: [], admin_other: [], insurance: [], taxes: [], financial: []},
    labels: {prof_fees: 'Professional Fees', admin_other: 'Administrative & Other', insurance: 'Insurance', taxes: 'Taxes', financial: 'Financial Expenses'},
    match: function(l) { return l.sheet_name === 'Gen & Admin'; },
    assign: function(l) {
      const r = l.row_num || 0;
      if (r >= 8 && r <= 16) return 'prof_fees';
      if (r >= 20 && r <= 49) return 'admin_other';
      if (r >= 53 && r <= 64) return 'insurance';
      if (r >= 68 && r <= 78) return 'taxes';
      if (r >= 82 && r <= 90) return 'financial';
      return 'admin_other';  // fallback
    },
    grandLabel: 'GRAND TOTAL G&A'
  }
};

// Populate sub-tab count badges
(function() {
  const rs = LINES.filter(l => l.sheet_name === 'Repairs & Supplies').length;
  const ga = LINES.filter(l => l.sheet_name === 'Gen & Admin').length;
  const rsEl = document.getElementById('rsCount');
  const gaEl = document.getElementById('gaCount');
  if (rsEl && rs) rsEl.textContent = rs;
  if (gaEl && ga) gaEl.textContent = ga;
})();

function pmSwitchSheet(sheetName, tabEl) {
  _pmActiveSheet = sheetName;
  document.querySelectorAll('.pm-sheet-tab').forEach(t => t.classList.remove('active'));
  tabEl.classList.add('active');
  renderTable();
  updateZeroToggle();
}

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

/* ── Grid Viewport Fit (PM) — keep horizontal scrollbar visible ──── */
function pmFitGridToViewport() {
  const gs = document.querySelector('.grid-container');
  if (!gs) return;
  const rect = gs.getBoundingClientRect();
  const available = window.innerHeight - rect.top - 16;
  gs.style.maxHeight = Math.max(120, available) + 'px';
}
pmFitGridToViewport();
window.addEventListener('resize', pmFitGridToViewport);
window.addEventListener('scroll', pmFitGridToViewport);
document.querySelector('.grid-container')?.addEventListener('scroll', pmFitGridToViewport);

/* ── Column Auto-Sizer (PM) ───────────────────────────────────────── */
function autoSizeColumns(table) {
  if (!table) return;
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  ctx.font = '13px Arial';
  const cols = table.querySelectorAll('thead th');
  const colWidths = [];
  cols.forEach((th, ci) => {
    if (th.classList.contains('frozen')) { colWidths.push(null); return; }
    let maxPx = 0;
    table.querySelectorAll('tbody tr').forEach(tr => {
      const td = tr.children[ci];
      if (!td) return;
      const inp = td.querySelector('input');
      if (inp) {
        const w = Math.ceil(ctx.measureText(inp.value || '').width);
        if (w > maxPx) maxPx = w;
      } else {
        const span = td.querySelector('.sub-val') || td;
        const w = Math.ceil(ctx.measureText((span.textContent || '').trim()).width);
        if (w > maxPx) maxPx = w;
      }
    });
    colWidths.push(maxPx + 20);
  });
  table.querySelectorAll('tbody tr').forEach(tr => {
    cols.forEach((th, ci) => {
      if (!colWidths[ci]) return;
      const td = tr.children[ci];
      if (!td) return;
      const inp = td.querySelector('input');
      if (inp && !inp.classList.contains('cell-notes') && !inp.classList.contains('pm-cell-notes')) {
        inp.style.width = Math.max(colWidths[ci], 55) + 'px';
      }
    });
  });
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

function isFixedToBudgetLine(line) {
    const gl = (line && line.gl_code) || '';
    return gl.indexOf('6315') === 0;
}

// One-time annual fees — once YTD is posted the Mar-Dec estimate is zero.
// Kept in sync with Python ONE_TIME_FEE_GLS constant in workflow.py.
const PM_ONE_TIME_FEE_GLS = new Set(['6722-0000','6762-0000','6763-0000','6764-0000']);
function isOneTimeFeeBilled(line) {
    if (!line || !line.gl_code) return false;
    if (!PM_ONE_TIME_FEE_GLS.has(line.gl_code)) return false;
    const billed = (line.ytd_actual || 0) + (line.accrual_adj || 0) + (line.unpaid_bills || 0);
    return Math.abs(billed) > 0.01;
}

function computeEstimate(line) {
    if (line.estimate_override !== null && line.estimate_override !== undefined) return line.estimate_override;
    if (isFixedToBudgetLine(line)) {
        return (line.current_budget || 0) - (line.ytd_actual || 0);
    }
    if (isOneTimeFeeBilled(line)) return 0;
    const ytd = line.ytd_actual || 0;
    const accrual = line.accrual_adj || 0;
    const unpaid = line.unpaid_bills || 0;
    const base = ytd + accrual + unpaid;
    // Formula: (YTD+Accrual+Unpaid) / YTD_MONTHS * REMAINING_MONTHS
    if (YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
    return 0;
}

function computeForecast(line) {
    if (line.forecast_override !== null && line.forecast_override !== undefined) return line.forecast_override;
    if (isFixedToBudgetLine(line)) {
        return line.current_budget || 0;
    }
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
    const isEditable = field === 'estimate' || field === 'forecast' || field === 'proposed' || field === 'prior_year' || field === 'ytd_actual' || field === 'accrual_adj' || field === 'unpaid_bills' || field === 'current_budget' || field === 'increase_pct';
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
            badge.style.background = '#f97316';
            badge.style.color = '#fff';
            badge.style.borderColor = '#ea580c';
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
        if (YTD_MONTHS > 0) return '=(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS;
        return '=0';
    }
    if (type === 'forecast') {
        const estExpr = (YTD_MONTHS > 0) ? '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0';
        return '=' + ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + estExpr + ')';
    }
    if (type === 'proposed') {
        if (line.proposed_formula) return line.proposed_formula;
        const fcstExpr = ytd + '+(' + accrual + ')+(' + unpaid + ')+(' + ((YTD_MONTHS > 0) ? '(' + ytd + '+' + accrual + '+' + unpaid + ')/' + YTD_MONTHS + '*' + REMAINING_MONTHS : '0') + ')';
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

    // Refresh material-variance note nudge
    if (typeof pmUpdateNoteWarn === 'function') pmUpdateNoteWarn(gl);

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
    const sheetCfg = PM_SHEET_CATEGORIES[_pmActiveSheet];
    const categories = {};
    Object.keys(sheetCfg.cats).forEach(k => categories[k] = []);
    const catLabels = sheetCfg.labels;
    LINES.forEach(l => {
        if (!sheetCfg.match(l)) return;
        const cat = sheetCfg.assign(l);
        if (categories[cat]) categories[cat].push(l);
    });

    let grandTotals = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};

    for (const [cat, catLines] of Object.entries(categories)) {
        if (catLines.length === 0) continue;

        let catTotals = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
        catLines.forEach(l => {
            catTotals.prior += (l.prior_year || 0);
            catTotals.ytd += (l.ytd_actual || 0);
            catTotals.accrual += (l.accrual_adj || 0);
            catTotals.unpaid += (l.unpaid_bills || 0);
            catTotals.estimate += computeEstimate(l);
            catTotals.forecast += computeForecast(l);
            catTotals.budget += (l.current_budget || 0);
            catTotals.proposed += computeProposed(l);
        });

        // Update subtotal cells
        const subPrior = document.getElementById('pm_subtotal_prior_' + cat);
        const subYtd = document.getElementById('pm_subtotal_ytd_' + cat);
        const subEstimate = document.getElementById('pm_subtotal_estimate_' + cat);
        const subForecast = document.getElementById('pm_subtotal_forecast_' + cat);
        const subBudget = document.getElementById('pm_subtotal_budget_' + cat);
        const subProposed = document.getElementById('pm_subtotal_proposed_' + cat);
        const subVar = document.getElementById('pm_subtotal_variance_' + cat);

        if (subPrior) subPrior.textContent = fmt(catTotals.prior);
        if (subYtd) subYtd.textContent = fmt(catTotals.ytd);
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
    const grandEstimate = document.getElementById('pm_grandtotal_estimate');
    const grandForecast = document.getElementById('pm_grandtotal_forecast');
    const grandBudget = document.getElementById('pm_grandtotal_budget');
    const grandProposed = document.getElementById('pm_grandtotal_proposed');
    const grandVar = document.getElementById('pm_grandtotal_variance');
    const grandPct = document.getElementById('pm_grandtotal_pct');

    if (grandPrior) grandPrior.textContent = fmt(grandTotals.prior);
    if (grandYtd) grandYtd.textContent = fmt(grandTotals.ytd);
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

    // Group by category based on active sheet tab
    const sheetCfg = PM_SHEET_CATEGORIES[_pmActiveSheet];
    const categories = {};
    Object.keys(sheetCfg.cats).forEach(k => categories[k] = []);
    const catLabels = sheetCfg.labels;
    LINES.forEach(l => {
        if (!sheetCfg.match(l)) return;
        const cat = sheetCfg.assign(l);
        if (categories[cat]) categories[cat].push(l);
    });

    let grandTotals = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};
    const NC = 15;

    for (const [cat, catLines] of Object.entries(categories)) {
        if (catLines.length === 0) continue;

        const headerRow = document.createElement('tr');
        headerRow.className = 'category-header';
        headerRow.innerHTML = '<td class="frozen frozen-gl"></td><td class="frozen frozen-desc">' + catLabels[cat] + '</td><td colspan="' + (NC - 2) + '"></td>';
        tbody.appendChild(headerRow);

        let catTotals = {prior:0, ytd:0, accrual:0, unpaid:0, estimate:0, forecast:0, budget:0, proposed:0};

        catLines.forEach(line => {
            const estimate = computeEstimate(line);
            const forecast = computeForecast(line);
            const proposed = computeProposed(line);
            const variance = (line.current_budget || 0) - forecast;
            const pctChange = forecast ? (((line.current_budget || 0) - forecast) / forecast) : 0;

            catTotals.prior += (line.prior_year || 0);
            catTotals.ytd += (line.ytd_actual || 0);
            catTotals.estimate += estimate;
            catTotals.forecast += forecast;
            catTotals.budget += (line.current_budget || 0);
            catTotals.proposed += proposed;

            const reclassBadge = line.reclass_to_gl ? ' <span style="background:var(--orange-light); color:var(--orange); font-size:10px; padding:1px 5px; border-radius:8px;">Reclass</span>' : '';

            const isZero = !(line.prior_year || line.ytd_actual || line.accrual_adj || line.unpaid_bills || line.current_budget || (line.increase_pct && line.increase_pct !== 0));
            const tr = document.createElement('tr');
            if (isZero) { tr.classList.add('zero-row'); if (!_showZeroRows) tr.style.display = 'none'; }

            const gl = line.gl_code;
            const estFormula = pmGetFormulaTooltip(line, 'estimate');
            const fcstFormula = pmGetFormulaTooltip(line, 'forecast');
            const propFormula = pmGetFormulaTooltip(line, 'proposed');

            tr.innerHTML = `
                <td class="frozen frozen-gl"><a href="#" onclick="toggleInvoices('${gl}', this); return false;" style="color:var(--blue); text-decoration:none; font-variant-numeric:tabular-nums;" title="Click to view invoices">${gl}</a>${reclassBadge}</td>
                <td class="frozen frozen-desc"><a href="#" onclick="toggleInvoices('${gl}', this); return false;" style="color:inherit; text-decoration:none; cursor:pointer;" title="Click to view expenses">${line.description} <span class="drill-arrow" style="font-size:10px; color:var(--gray-400); transition:transform 0.2s;">▶</span></a></td>
                <td class="number"><input id="pm_pr_${gl}" class="pm-cell" type="text" value="${fmt(line.prior_year)}" data-raw="${Math.round(line.prior_year || 0)}" data-gl="${gl}" data-field="prior_year" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number"><input id="pm_ytd_${gl}" class="pm-cell" type="text" value="${fmt(line.ytd_actual)}" data-raw="${Math.round(line.ytd_actual || 0)}" data-gl="${gl}" data-field="ytd_actual" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number"><input id="pm_acc_${gl}" class="pm-cell" type="text" value="${fmt(line.accrual_adj)}" data-raw="${Math.round(line.accrual_adj || 0)}" data-gl="${gl}" data-field="accrual_adj" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
                <td class="number"><input id="pm_unp_${gl}" class="pm-cell" type="text" value="${fmt(line.unpaid_bills)}" data-raw="${Math.round(line.unpaid_bills || 0)}" data-gl="${gl}" data-field="unpaid_bills" onfocus="this.value=this.dataset.raw" onblur="pmCellBlur(this)" ${CAN_EDIT ? '' : 'disabled'}></td>
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
                <td class="col-notes"><input type="text" class="${(Math.abs(pctChange) > 0.10 && !(line.notes || '').trim()) ? 'note-warn' : ''}" value="${(line.notes || '').replace(/"/g, '&quot;')}" data-gl="${gl}" data-field="notes" oninput="onInput(this)" onchange="onInput(this)" ${CAN_EDIT ? '' : 'disabled'} placeholder="Why did this change? (context for FA)" maxlength="500" style="min-width:80px;"></td>
            `;
            tbody.appendChild(tr);
        });

        // Subtotal
        const catVar = catTotals.budget - catTotals.forecast;
        const subRow = document.createElement('tr');
        subRow.className = 'subtotal-row';
        subRow.innerHTML = `
            <td class="frozen frozen-gl"></td><td class="frozen frozen-desc">Total ${catLabels[cat]}</td>
            <td class="number pm-fx-td" id="pm_subtotal_prior_${cat}" style="position:relative; cursor:pointer;" data-col="prior" data-raw="${Math.round(catTotals.prior)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.prior)}</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_ytd_${cat}" style="position:relative; cursor:pointer;" data-col="ytd" data-raw="${Math.round(catTotals.ytd)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.ytd)}</span></td>
            <td></td><td></td>
            <td class="number pm-fx-td" id="pm_subtotal_estimate_${cat}" style="position:relative; cursor:pointer;" data-col="estimate" data-raw="${Math.round(catTotals.estimate)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.estimate)}</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_forecast_${cat}" style="position:relative; cursor:pointer;" data-col="forecast" data-raw="${Math.round(catTotals.forecast)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.forecast)}</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_budget_${cat}" style="position:relative; cursor:pointer;" data-col="budget" data-raw="${Math.round(catTotals.budget)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.budget)}</span></td>
            <td></td>
            <td class="number pm-fx-td" id="pm_subtotal_proposed_${cat}" style="position:relative; cursor:pointer;" data-col="proposed" data-raw="${Math.round(catTotals.proposed)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catTotals.proposed)}</span></td>
            <td class="number pm-fx-td" id="pm_subtotal_variance_${cat}" style="position:relative; cursor:pointer; color:${catVar >= 0 ? 'var(--red)' : 'var(--green)'};" data-col="variance" data-raw="${Math.round(catVar)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(catVar)}</span></td>
            <td></td>
            <td class="col-notes"></td>
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
        <td class="frozen frozen-gl"></td><td class="frozen frozen-desc">${sheetCfg.grandLabel}</td>
        <td class="number pm-fx-td" id="pm_grandtotal_prior" style="position:relative; cursor:pointer;" data-col="prior" data-raw="${Math.round(grandTotals.prior)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.prior)}</span></td>
        <td class="number pm-fx-td" id="pm_grandtotal_ytd" style="position:relative; cursor:pointer;" data-col="ytd" data-raw="${Math.round(grandTotals.ytd)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.ytd)}</span></td>
        <td></td><td></td>
        <td class="number pm-fx-td" id="pm_grandtotal_estimate" style="position:relative; cursor:pointer;" data-col="estimate" data-raw="${Math.round(grandTotals.estimate)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.estimate)}</span></td>
        <td class="number pm-fx-td" id="pm_grandtotal_forecast" style="position:relative; cursor:pointer;" data-col="forecast" data-raw="${Math.round(grandTotals.forecast)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.forecast)}</span></td>
        <td class="number pm-fx-td" id="pm_grandtotal_budget" style="position:relative; cursor:pointer;" data-col="budget" data-raw="${Math.round(grandTotals.budget)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.budget)}</span></td>
        <td></td>
        <td class="number pm-fx-td" id="pm_grandtotal_proposed" style="position:relative; cursor:pointer;" data-col="proposed" data-raw="${Math.round(grandTotals.proposed)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandTotals.proposed)}</span></td>
        <td class="number pm-fx-td" id="pm_grandtotal_variance" style="position:relative; cursor:pointer; color:${grandVar >= 0 ? 'var(--red)' : 'var(--green)'};" data-col="variance" data-raw="${Math.round(grandVar)}" onclick="pmSubtotalFocus(this)"><span class="sub-val">${fmt(grandVar)}</span></td>
        <td class="number" id="pm_grandtotal_pct">${(grandPct * 100).toFixed(1)}%</td>
        <td class="col-notes"></td>
    `;
    tbody.appendChild(grandRow);
    // Auto-size numeric columns after render
    autoSizeColumns(document.querySelector('#linesBody')?.closest('table'));
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
        noData.innerHTML = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="13" style="padding:0;"><div class="drill-sticky" style="padding:12px 24px; background:#fef3c7; font-size:13px;">No expense distribution data uploaded yet. <a href="/pm/' + ENTITY + '/expenses" style="color:var(--blue);">Upload here</a></div></td>';
        row.after(noData);
        return;
    }

    const glGroup = data.gl_groups.find(g => g.gl_code === glCode);
    if (!glGroup || !glGroup.invoices || glGroup.invoices.length === 0) {
        const noInv = document.createElement('tr');
        noInv.className = 'invoice-detail-row';
        noInv.innerHTML = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="13" style="padding:0;"><div class="drill-sticky" style="padding:12px 24px; background:var(--gray-50); font-size:13px; color:var(--gray-500);">No invoices found for ' + glCode + '</div></td>';
        row.after(noInv);
        return;
    }

    // Build all GL codes for reclass dropdown
    const allGLs = LINES.map(l => l.gl_code).filter(g => g !== glCode);

    const detailRow = document.createElement('tr');
    detailRow.className = 'invoice-detail-row';
    let html = '<td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="13" style="padding:0;"><div class="drill-sticky" style="padding:12px 16px 12px 24px; background:linear-gradient(to right, #f0f4ff, #f8faff); border-left:3px solid var(--blue); border-bottom:1px solid var(--gray-200);">';
    html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">';
    html += '<span style="font-weight:600; font-size:13px; color:var(--blue);">' + glCode + ' — ' + (glGroup.gl_name || '') + '</span>';
    html += '<span style="font-size:12px; color:var(--gray-500);">' + glGroup.invoices.length + ' invoice' + (glGroup.invoices.length !== 1 ? 's' : '') + ' · ' + fmtAmt(glGroup.total || 0) + '</span>';
    html += '</div>';

    html += '<table style="width:auto; font-size:12px; border-collapse:separate; border-spacing:0; background:white; border-radius:6px; box-shadow:0 1px 2px rgba(0,0,0,0.05); overflow:hidden;">';
    html += '<thead style="position:static;"><tr style="position:static; background:var(--gray-100); color:var(--gray-600); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px;">';
    html += '<td style="padding:7px 16px; min-width:140px; border-bottom:2px solid var(--gray-300);">Payee</td><td style="padding:7px 16px; min-width:140px; border-bottom:2px solid var(--gray-300);">Description</td><td style="padding:7px 16px; min-width:70px; border-bottom:2px solid var(--gray-300);">Inv #</td><td style="padding:7px 16px; min-width:85px; border-bottom:2px solid var(--gray-300);">Date</td><td style="padding:7px 16px; min-width:100px; text-align:right; border-bottom:2px solid var(--gray-300);">Amount</td><td style="padding:7px 16px; min-width:90px; border-bottom:2px solid var(--gray-300);">Check #</td><td style="padding:7px 16px; min-width:90px; text-align:center; border-bottom:2px solid var(--gray-300);">Action</td></tr></thead>';

    glGroup.invoices.forEach(inv => {
        const isReclassed = !!inv.reclass_to_gl;
        html += '<tr style="' + (isReclassed ? ' opacity:0.5; text-decoration:line-through;' : '') + '">';
        html += '<td style="padding:7px 16px; font-size:12px; white-space:nowrap; border-bottom:1px solid var(--gray-200);">' + (inv.payee_name || inv.payee_code || '—') + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; color:var(--gray-600); border-bottom:1px solid var(--gray-200);">' + (inv.notes || '—') + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; font-family:monospace; border-bottom:1px solid var(--gray-200);">' + (inv.invoice_num || '—') + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; border-bottom:1px solid var(--gray-200);">' + (inv.invoice_date ? inv.invoice_date.substring(0,10) : '—') + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; text-align:right; font-size:12px; font-weight:600; font-variant-numeric:tabular-nums; border-bottom:1px solid var(--gray-200);">' + fmtAmt(inv.amount) + '</td>';
        html += '<td style="padding:7px 16px; white-space:nowrap; font-size:12px; border-bottom:1px solid var(--gray-200);">' + (inv.check_num || '—') + '</td>';
        html += '<td style="padding:7px 16px; text-align:center; border-bottom:1px solid var(--gray-200);">';
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
            // Re-apply YTD adjustments so totals reflect the reclass immediately
            await applyReclassAdjustments();
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
            // Re-apply YTD adjustments so totals reflect the undo immediately
            await applyReclassAdjustments();
        } else { showToast('Undo failed', 'error'); }
    } catch(e) { showToast('Undo error: ' + e.message, 'error'); }
}

// Legacy stub — now uses pmLineChanged for cascade system
function pmUpdateNoteWarn(gl) {
    const noteEl = document.querySelector('.col-notes input[data-gl="' + gl + '"]');
    if (!noteEl) return;
    const pctEl = document.getElementById('pm_pct_' + gl);
    const pct = pctEl ? (parseFloat(pctEl.dataset.raw) || 0) : 0;
    const hasNote = (noteEl.value || '').trim().length > 0;
    if (Math.abs(pct) > 0.10 && !hasNote) {
        noteEl.classList.add('note-warn');
    } else {
        noteEl.classList.remove('note-warn');
    }
}

function onInput(el) {
    const gl = el.dataset.gl;
    const field = el.dataset.field;
    const line = LINES.find(l => l.gl_code === gl);
    console.log('[onInput] gl=', gl, 'field=', field, 'value=', el.value, 'lineFound=', !!line);
    if (!line) { console.warn('[onInput] no line for gl', gl); return; }

    if (field === 'increase_pct') {
        line.increase_pct = parseFloat(el.value) / 100 || 0;
    } else if (field === 'accrual_adj') {
        line.accrual_adj = parseFloat(el.value) || 0;
    } else if (field === 'unpaid_bills') {
        line.unpaid_bills = parseFloat(el.value) || 0;
    } else if (field === 'notes') {
        line.notes = el.value;
        console.log('[onInput] notes set on line', gl, '→', line.notes);
        pmUpdateNoteWarn(gl);
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
            ytd_actual: l._db_ytd_actual !== undefined ? l._db_ytd_actual : (l.ytd_actual || 0),
            ytd_budget: l.ytd_budget || 0,
            current_budget: l.current_budget || 0
        }));
        const linesWithNotes = payload.filter(p => p.notes && p.notes.trim().length > 0);
        console.log('[saveAll] PUT /api/lines/' + ENTITY + ' lines=' + payload.length + ' withNotes=' + linesWithNotes.length, linesWithNotes.map(l => ({gl: l.gl_code, notes: l.notes})));
        const resp = await fetch('/api/lines/' + ENTITY, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({lines: payload})
        });
        console.log('[saveAll] response status=', resp.status, resp.statusText);
        if (resp.ok) {
            const body = await resp.json().catch(() => ({}));
            console.log('[saveAll] response body=', body);
            indicator.textContent = 'Saved';
            indicator.className = 'save-indicator saved';
            indicator.onclick = null;
            setTimeout(() => { indicator.textContent = ''; indicator.className = 'save-indicator'; }, 2000);
        } else {
            const errBody = await resp.text().catch(() => '');
            console.error('[saveAll] FAILED status=', resp.status, 'body=', errBody);
            indicator.textContent = 'Save failed (' + resp.status + ') — click to retry';
            indicator.className = 'save-indicator failed';
            indicator.onclick = () => saveAll();
            alert('Save failed: HTTP ' + resp.status + '\n\n' + errBody);
        }
    } catch(e) {
        console.error('[saveAll] EXCEPTION', e);
        indicator.textContent = 'Save error — click to retry';
        indicator.className = 'save-indicator failed';
        indicator.onclick = () => saveAll();
        alert('Save error: ' + e.message);
    }
}

async function submitForReview() {
    // Save first
    await saveAll();

    // If the budget is already in fa_review (PM re-entered to tweak), just
    // save — no status transition needed and the server won't allow fa→fa.
    if (BUDGET_STATUS === 'fa_review') {
        if (!confirm('Save changes and return to portal? (Already submitted for FA review.)')) return;
        showToast('Changes saved.', 'success');
        setTimeout(() => { window.location.href = '/pm'; }, 800);
        return;
    }

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
        <td class="frozen frozen-gl drill-row"></td><td class="frozen frozen-desc drill-row"></td><td colspan="13" style="padding:0;">
            <div class="drill-sticky" style="padding:12px 24px; background:var(--blue-light); border-left:3px solid var(--blue);"><div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
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
            </div></div>
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

// Adjust LINES ytd_actual based on invoice-level reclasses (frontend only — DB unchanged until FA accepts)
// Store original DB values so saveAll() never writes adjusted figures back
LINES.forEach(l => { l._db_ytd_actual = l.ytd_actual || 0; });

async function applyReclassAdjustments() {
    try {
        const data = await fetchExpenseData();
        if (!data || !data.gl_groups) { renderTable(); updateZeroToggle(); return; }

        // Flatten all invoices and find reclassed ones
        const adjustments = {};  // gl_code -> net adjustment
        data.gl_groups.forEach(g => {
            if (!g.invoices) return;
            g.invoices.forEach(inv => {
                if (!inv.reclass_to_gl || inv.reclass_to_gl === inv.gl_code) return;
                const amt = inv.amount || 0;
                // Subtract from original GL
                adjustments[inv.gl_code] = (adjustments[inv.gl_code] || 0) - amt;
                // Add to target GL
                adjustments[inv.reclass_to_gl] = (adjustments[inv.reclass_to_gl] || 0) + amt;
            });
        });

        // Reset all to DB values first, then apply adjustments
        LINES.forEach(l => { l.ytd_actual = l._db_ytd_actual; });
        if (Object.keys(adjustments).length > 0) {
            LINES.forEach(l => {
                if (adjustments[l.gl_code]) {
                    l.ytd_actual = l._db_ytd_actual + adjustments[l.gl_code];
                }
            });
        }
    } catch(e) {
        // If expense data fails to load, just render with original figures
    }
    renderTable();
    updateZeroToggle();
}
applyReclassAdjustments();

// ─── My Changes panel (read-only summary) ───────────────────────────────
function switchPmMcTab(button, tabId) {
  document.getElementById('pmMyNotesContent').style.display = 'none';
  document.getElementById('pmMyReclassContent').style.display = 'none';
  document.querySelectorAll('#pmMyChangesTabs .pm-mc-tab').forEach(t => {
    t.style.color = 'var(--gray-500)';
    t.style.borderBottom = '2px solid transparent';
    t.style.background = 'transparent';
  });
  document.getElementById(tabId).style.display = 'block';
  button.style.color = 'var(--blue)';
  button.style.borderBottom = '2px solid var(--blue)';
  button.style.background = 'white';
}

(async function populateMyChanges() {
  let totalItems = 0;
  const panel = document.getElementById('pmMyChangesPanel');

  // Tab 1: My Notes
  const linesWithNotes = LINES.filter(l => l.notes && l.notes.trim().length > 0);
  const notesContainer = document.getElementById('pmMyNotesContainer');
  const notesEmpty = document.getElementById('pmMyNotesEmpty');
  const notesCount = document.getElementById('pmMyNotesCount');

  if (linesWithNotes.length > 0) {
    notesEmpty.style.display = 'none';
    notesCount.textContent = linesWithNotes.length;
    notesContainer.innerHTML = linesWithNotes.map(l => {
      // Split notes into PM notes vs FA responses
      const parts = (l.notes || '').split('\n');
      let pmHtml = '';
      let faHtml = '';
      parts.forEach(p => {
        if (p.match(/^\[FA (REJECTED|COMMENT|ACCEPTED)/)) {
          faHtml += '<div style="flex:1; font-size:12px; color:var(--gray-600); background:#f0f4ff; padding:6px 10px; border-radius:6px; border-left:3px solid var(--blue); margin-top:4px;">' +
            '<strong>FA Response:</strong> ' + p + '</div>';
        } else if (p.trim()) {
          pmHtml += (pmHtml ? '<br>' : '') + p;
        }
      });
      return '<div style="display:flex; align-items:flex-start; gap:12px; padding:10px 12px; border-radius:8px; margin-bottom:6px;" onmouseover="this.style.background=\'var(--gray-50)\'" onmouseout="this.style.background=\'\'">' +
        '<span style="font-family:monospace; font-size:12px; font-weight:600; color:var(--blue); background:var(--blue-light); padding:3px 8px; border-radius:4px; white-space:nowrap;">' + l.gl_code + '</span>' +
        '<span style="font-size:12px; color:var(--gray-500); min-width:140px;">' + (l.description || '') + '</span>' +
        '<div style="flex:1;">' +
          (pmHtml ? '<div style="font-size:13px; color:var(--gray-700); background:#fffbeb; padding:6px 10px; border-radius:6px; border-left:3px solid #fbbf24;">' + pmHtml + '</div>' : '') +
          faHtml +
        '</div>' +
      '</div>';
    }).join('');
    totalItems += linesWithNotes.length;
  } else {
    notesEmpty.style.display = '';
    notesContainer.innerHTML = '';
    notesCount.textContent = '0';
  }

  // Tab 2: My Reclasses
  const reclassCount = document.getElementById('pmMyReclassCount');
  const reclassBody = document.getElementById('pmMyReclassBody');
  const reclassEmpty = document.getElementById('pmMyReclassEmpty');

  const expData = await fetchExpenseData();
  if (expData && expData.gl_groups) {
    const allInvoices = [];
    expData.gl_groups.forEach(g => {
      if (g.invoices) g.invoices.forEach(inv => allInvoices.push(inv));
    });
    const reclassed = allInvoices.filter(inv => inv.reclass_to_gl);

    const reclassMap = {};
    reclassed.forEach(inv => {
      const key = inv.gl_code + '|' + inv.reclass_to_gl;
      if (!reclassMap[key]) {
        reclassMap[key] = { from_gl: inv.gl_code, to_gl: inv.reclass_to_gl, invoices: [], total: 0, notes: '' };
      }
      reclassMap[key].invoices.push(inv);
      reclassMap[key].total += inv.amount || 0;
      if (inv.reclass_notes && !reclassMap[key].notes) reclassMap[key].notes = inv.reclass_notes;
    });
    const groups = Object.values(reclassMap);

    if (groups.length > 0) {
      reclassEmpty.style.display = 'none';
      reclassCount.textContent = groups.length;
      reclassBody.innerHTML = '';
      groups.forEach((g, gi) => {
        const fromLine = LINES.find(l => l.gl_code === g.from_gl);
        const fromDesc = fromLine ? fromLine.description : '';
        const toLine = LINES.find(l => l.gl_code === g.to_gl);
        const toDesc = toLine ? toLine.description : '';
        const gid = 'pmrg_' + gi;

        // Determine FA status from the notes of the from_gl line
        let faStatus = '<span style="background:#fff7ed; color:#b45309; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">● Pending</span>';
        const fromNotes = fromLine ? (fromLine.notes || '') : '';
        if (fromNotes.includes('[FA ACCEPTED')) faStatus = '<span style="background:#dcfce7; color:#166534; padding:3px 10px; border-radius:10px; font-size:11px; font-weight:600;">✓ Accepted</span>';

        const tr = document.createElement('tr');
        tr.style.cssText = 'transition:background 0.15s; cursor:pointer;';
        tr.onmouseover = function() { this.style.background='var(--gray-50)'; };
        tr.onmouseout = function() { this.style.background=''; };
        tr.onclick = function() { pmToggleReclassInv(gid); };
        tr.innerHTML =
          '<td style="padding:10px;"><span id="' + gid + '_arrow" style="display:inline-block; font-size:10px; color:var(--gray-500); transition:transform 0.2s; margin-right:6px;">▶</span><span style="font-family:monospace; font-size:12px; font-weight:700;">' + g.from_gl + '</span><div style="padding-left:20px; font-size:11px; color:var(--gray-500);">' + fromDesc + '</div></td>' +
          '<td style="padding:10px 4px; color:var(--orange); font-weight:700; font-size:16px;">→</td>' +
          '<td style="padding:10px;"><span style="font-family:monospace; font-size:12px; font-weight:700;">' + g.to_gl + '</span><div style="font-size:11px; color:var(--gray-500);">' + toDesc + '</div></td>' +
          '<td style="padding:10px;"><span style="font-size:11px; background:var(--orange-light); color:var(--orange); padding:2px 8px; border-radius:10px; font-weight:600;">' + g.invoices.length + ' invoice' + (g.invoices.length !== 1 ? 's' : '') + '</span></td>' +
          '<td style="padding:10px; text-align:right; font-weight:600; font-variant-numeric:tabular-nums;">' + fmt(g.total) + '</td>' +
          '<td style="padding:10px; font-size:12px; color:var(--gray-600); font-style:italic; max-width:200px;">' + (g.notes ? '"' + g.notes + '"' : '') + '</td>' +
          '<td style="padding:10px; text-align:center;">' + faStatus + '</td>';
        reclassBody.appendChild(tr);
        // Expandable invoice detail rows
        g.invoices.forEach(inv => {
          const itr = document.createElement('tr');
          itr.dataset.group = gid;
          itr.style.cssText = 'display:none; background:#fafbfc;';
          const invDate = inv.invoice_date || inv.date || '';
          const cleanDate = invDate ? invDate.split('T')[0] : '';
          const invNum = inv.invoice_num || inv.invoice_number || inv.ref || '';
          const invVendor = inv.payee_name || inv.vendor_name || inv.vendor || '';
          const invDesc = inv.notes || inv.description || '';
          const toGlName = (LINES.find(l => l.gl_code === inv.reclass_to_gl) || {}).description || inv.reclass_to_gl;
          itr.innerHTML =
            '<td colspan="7" style="padding:8px 10px 8px 44px; border-bottom:1px solid #f0f1f3;">' +
              '<div style="display:flex; align-items:center; gap:12px; font-size:12px; flex-wrap:wrap;">' +
                (invNum ? '<span style="font-family:monospace; font-size:11px; color:var(--gray-400); background:#f3f4f6; padding:1px 6px; border-radius:3px;">' + invNum + '</span>' : '') +
                '<span style="font-weight:600; color:var(--gray-700);">' + invVendor + '</span>' +
                (invDesc ? '<span style="color:var(--gray-500);">— ' + invDesc + '</span>' : '') +
                (cleanDate ? '<span style="font-size:11px; color:var(--gray-400);">' + cleanDate + '</span>' : '') +
                '<span style="font-size:11px; color:var(--orange);">→ ' + toGlName + '</span>' +
                '<span style="margin-left:auto; font-weight:600; font-variant-numeric:tabular-nums; text-align:right;">' + fmt(inv.amount || 0) + '</span>' +
                '<button onclick="event.stopPropagation(); undoSingleReclass(' + inv.id + ',\'' + g.from_gl + '\',\'' + g.to_gl + '\',this)" style="margin-left:8px; padding:2px 8px; font-size:10px; font-weight:600; border-radius:4px; cursor:pointer; background:white; color:var(--gray-500); border:1px solid var(--gray-300);">Undo</button>' +
              '</div>' +
            '</td>';
          reclassBody.appendChild(itr);
        });
      });
      totalItems += groups.length;
    } else {
      reclassEmpty.style.display = '';
      reclassBody.innerHTML = '';
      reclassCount.textContent = '0';
    }
  } else {
    reclassEmpty.style.display = '';
    reclassBody.innerHTML = '';
    reclassCount.textContent = '0';
  }

  // Show panel if there are items
  if (totalItems > 0) {
    panel.style.display = '';
    document.getElementById('pmMyChangesBadge').textContent = totalItems + ' item' + (totalItems !== 1 ? 's' : '');
  }
})();

async function undoSingleReclass(invId, fromGl, toGl, btn) {
  if (!confirm('Undo this invoice reclass?')) return;
  try {
    await fetch('/api/expense-dist/reclass/' + invId, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ reclass_to_gl: '' })
    });
    const row = btn.closest('tr');
    if (row) { row.style.opacity = '0.3'; row.style.pointerEvents = 'none'; }
    btn.textContent = 'Undone';
    btn.disabled = true;
    showToast('Invoice restored to ' + fromGl, 'success');
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
  }
}

function pmToggleReclassInv(gid) {
  const rows = document.querySelectorAll('tr[data-group="' + gid + '"]');
  const arrow = document.getElementById(gid + '_arrow');
  if (!rows.length) return;
  const showing = rows[0].style.display !== 'none';
  rows.forEach(r => { r.style.display = showing ? 'none' : ''; });
  if (arrow) arrow.style.transform = showing ? '' : 'rotate(90deg)';
}
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
/* Force scrollbars always visible (fixes macOS auto-hide on horizontal/vertical scroll) */
::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
::-webkit-scrollbar-track { background: #1e293b; }
::-webkit-scrollbar-thumb { background: #475569; border-radius: 6px; border: 2px solid #1e293b; }
::-webkit-scrollbar-thumb:hover { background: #64748b; }
::-webkit-scrollbar-corner { background: #1e293b; }
* { scrollbar-width: thin; scrollbar-color: #475569 #1e293b; }
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

function isFixedToBudgetLine(l) {
  const gl = (l && l.gl_code) || '';
  return gl.indexOf('6315') === 0;
}

const BP_ONE_TIME_FEE_GLS = new Set(['6722-0000','6762-0000','6763-0000','6764-0000']);
function isOneTimeFeeBilled(l) {
  if (!l || !l.gl_code) return false;
  if (!BP_ONE_TIME_FEE_GLS.has(l.gl_code)) return false;
  const billed = (l.ytd_actual || 0) + (l.accrual_adj || 0) + (l.unpaid_bills || 0);
  return Math.abs(billed) > 0.01;
}

function computeEstimate(l) {
  if (l.estimate_override !== null && l.estimate_override !== undefined) return l.estimate_override;
  if (isFixedToBudgetLine(l)) return (l.current_budget || 0) - (l.ytd_actual || 0);
  // One-time annual fees: once YTD posts, no more projection (forecast = billed amount)
  if (isOneTimeFeeBilled(l)) return 0;
  // Payroll tab uses a simplified base (no accrual/unpaid). Other tabs unchanged.
  const isPayroll = l.sheet_name === 'Payroll';
  const ytd = l.ytd_actual || 0;
  const accrual = isPayroll ? 0 : (l.accrual_adj || 0);
  const unpaid = isPayroll ? 0 : (l.unpaid_bills || 0);
  const base = ytd + accrual + unpaid;
  if (YTD_MONTHS > 0) return (base / YTD_MONTHS) * REMAINING_MONTHS;
  return 0;
}

function computeForecast(l) {
  if (l.forecast_override !== null && l.forecast_override !== undefined) return l.forecast_override;
  if (isFixedToBudgetLine(l)) return l.current_budget || 0;
  // Payroll: Forecast = YTD + Estimate (no accrual/unpaid). Other tabs unchanged.
  const isPayroll = l.sheet_name === 'Payroll';
  const accrual = isPayroll ? 0 : (l.accrual_adj || 0);
  const unpaid = isPayroll ? 0 : (l.unpaid_bills || 0);
  return (l.ytd_actual || 0) + accrual + unpaid + computeEstimate(l);
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
