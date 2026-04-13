"""
Audited Financials Blueprint for Century Management.

Manages extraction of data from audited financial statement PDFs and mapping
them to Century's budget template categories.

Implements:
- Auditor profile management (5 audit firms across 162 buildings)
- Mapping rules per auditor (line items -> Century budget categories)
- PDF upload and Claude API extraction of Schedule of Expenses/Revenue
- Mapping rule application and reconciliation
- Review and confirmation workflow
"""

from flask import Blueprint, render_template_string, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from decimal import Decimal
import logging
import os
import base64
import json
import csv
from pathlib import Path

logger = logging.getLogger(__name__)

# Century audit categories — each maps to exactly one budget summary row
CENTURY_CATEGORIES = [
    # Income
    "Maintenance",
    "Tax Benefit Credits",
    "Commercial",
    "Garage",
    "Commercial Real Estate Tax",
    "Storage Income",
    "Bicycle Charge",
    "Laundry",
    "Assessment - Operating",
    "Other Income",
    # Expenses
    "Payroll",
    "Electric",
    "Gas Cooking / Heating",
    "Fuel",
    "Water & Sewer",
    "Supplies",
    "Repairs & Maintenance",
    "Insurance",
    "Real Estate Taxes",
    "Real Estate Tax Benefit Credits",
    "Corporate Taxes",
    "Professional Fees",
    "Administrative & Other",
    "Financial Expenses",
    # Non-Operating Income
    "Capital Assessment",
    "Special Assessment",
    "Interest Income",
    "Insurance Proceeds",
    "Real Estate Tax refund",
    "ICON Settlement Proceeds",
    "SBA - PPP Loan Proceeds",
    # Non-Operating Expense
    "Capital Expenses",
    "Cert Fee for Tax Reduction",
]

# Maps every Century audit category → budget summary row label
# This is the canonical bridge between audited financials and the budget summary
CENTURY_TO_SUMMARY = {
    # Income categories → Total Operating Income
    "Maintenance": "Total Operating Income",
    "Tax Benefit Credits": "Total Operating Income",
    "Commercial": "Total Operating Income",
    "Garage": "Total Operating Income",
    "Commercial Real Estate Tax": "Total Operating Income",
    "Storage Income": "Total Operating Income",
    "Bicycle Charge": "Total Operating Income",
    "Laundry": "Total Operating Income",
    "Assessment - Operating": "Total Operating Income",
    "Other Income": "Total Operating Income",
    # Expense categories → specific summary rows
    "Payroll": "Payroll & Related",
    "Electric": "Energy",
    "Gas Cooking / Heating": "Energy",
    "Fuel": "Energy",
    "Water & Sewer": "Water & Sewer",
    "Supplies": "Repairs & Supplies",
    "Repairs & Maintenance": "Repairs & Supplies",
    "Insurance": "Insurance",
    "Real Estate Taxes": "Taxes",
    "Real Estate Tax Benefit Credits": "Taxes",
    "Corporate Taxes": "Taxes",
    "Professional Fees": "Professional Fees",
    "Administrative & Other": "Administrative & Other",
    "Financial Expenses": "Financial Expenses",
    # Non-operating (not on main summary but tracked)
    "Capital Assessment": "Non-Operating Income",
    "Special Assessment": "Non-Operating Income",
    "Interest Income": "Non-Operating Income",
    "Insurance Proceeds": "Non-Operating Income",
    "Real Estate Tax refund": "Non-Operating Income",
    "ICON Settlement Proceeds": "Non-Operating Income",
    "SBA - PPP Loan Proceeds": "Non-Operating Income",
    "Capital Expenses": "Non-Operating Expense",
    "Cert Fee for Tax Reduction": "Non-Operating Expense",
}

# Which summary rows are income vs expense (for reconciliation)
INCOME_SUMMARY_ROWS = {"Total Operating Income", "Non-Operating Income"}
EXPENSE_SUMMARY_ROWS = {"Payroll & Related", "Energy", "Water & Sewer", "Repairs & Supplies",
                         "Professional Fees", "Administrative & Other", "Insurance", "Taxes",
                         "Financial Expenses", "Non-Operating Expense"}


def _category_section(century_category):
    """Return 'revenue' or 'expense' for a given Century category, based on
    CENTURY_TO_SUMMARY. Unknown categories default to 'expense'."""
    if CENTURY_TO_SUMMARY.get(century_category, "") in INCOME_SUMMARY_ROWS:
        return "revenue"
    return "expense"


def create_audited_financials_blueprint(db):
    """
    Create and configure the audited financials blueprint.

    Args:
        db: SQLAlchemy database instance from app.py

    Returns:
        tuple: (blueprint, models_dict, helpers_dict)
    """

    # ─── SQLAlchemy Models ────────────────────────────────────────────────────

    class AuditorProfile(db.Model):
        """Auditor profile for mapping rules."""
        __tablename__ = "auditor_profiles"

        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(255), nullable=False)  # Display name
        firm_name = db.Column(db.String(255), nullable=False)  # e.g. "Marks Paneth LLP"
        notes = db.Column(db.Text, default="")
        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        # Relationships
        rules = db.relationship("MappingRule", back_populates="profile", cascade="all, delete-orphan")
        uploads = db.relationship("AuditUpload", back_populates="profile")

        def to_dict(self):
            return {
                "id": self.id,
                "name": self.name,
                "firm_name": self.firm_name,
                "notes": self.notes,
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None
            }


    class MappingRule(db.Model):
        """Mapping rule for an auditor's line items -> Century categories."""
        __tablename__ = "mapping_rules"

        id = db.Column(db.Integer, primary_key=True)
        profile_id = db.Column(db.Integer, db.ForeignKey("auditor_profiles.id"), nullable=False)
        auditor_line_item = db.Column(db.String(255), nullable=False)
        auditor_category = db.Column(db.String(255), default="")
        century_category = db.Column(db.String(100), nullable=False)
        split_pct = db.Column(db.Float, default=1.0)
        notes = db.Column(db.Text, default="")

        # Relationship
        profile = db.relationship("AuditorProfile", back_populates="rules")

        def to_dict(self):
            return {
                "id": self.id,
                "profile_id": self.profile_id,
                "auditor_line_item": self.auditor_line_item,
                "auditor_category": self.auditor_category,
                "century_category": self.century_category,
                "split_pct": self.split_pct,
                "notes": self.notes
            }


    class AuditUpload(db.Model):
        """Audit PDF upload and extraction tracking."""
        __tablename__ = "audit_uploads"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False, index=True)
        building_name = db.Column(db.String(255), nullable=False)
        profile_id = db.Column(db.Integer, db.ForeignKey("auditor_profiles.id"), nullable=True)
        fiscal_year_end = db.Column(db.String(10), default="")
        pdf_filename = db.Column(db.String(500), default="")
        raw_extraction = db.Column(db.Text, default="")  # JSON from Claude
        mapped_data = db.Column(db.Text, default="")  # JSON after mapping
        status = db.Column(db.String(20), default="uploaded")  # uploaded, extracted, mapped, confirmed
        confirmed_by = db.Column(db.String(255), default="")
        confirmed_at = db.Column(db.DateTime, nullable=True)
        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        # Relationship
        profile = db.relationship("AuditorProfile", back_populates="uploads")

        def to_dict(self):
            return {
                "id": self.id,
                "entity_code": self.entity_code,
                "building_name": self.building_name,
                "profile_id": self.profile_id,
                "profile_name": self.profile.name if self.profile else None,
                "fiscal_year_end": self.fiscal_year_end,
                "pdf_filename": self.pdf_filename,
                "status": self.status,
                "confirmed_by": self.confirmed_by,
                "confirmed_at": self.confirmed_at.isoformat() if self.confirmed_at else None,
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None
            }

    # ─── Helper Functions ────────────────────────────────────────────────────

    def get_buildings_list():
        """Load buildings from CSV."""
        try:
            buildings_csv = Path(__file__).parent.parent / "budget_system" / "buildings.csv"
            buildings = []
            if buildings_csv.exists():
                with open(buildings_csv, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        buildings.append({
                            "entity_code": row["entity_code"],
                            "building_name": row["building_name"]
                        })
            return buildings
        except Exception as e:
            logger.error(f"Error loading buildings: {e}")
            return []


    def get_data_dir():
        """Get the data directory for PDF uploads."""
        data_dir = Path(__file__).parent / "data" / "audit_pdfs"
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir


    def fuzzy_match_rule(line_item, rules, section=None):
        """
        Find best matching rule for a line item (case-insensitive, whitespace-normalized).
        If section ('revenue'|'expense') is provided, only rules whose target
        century_category belongs to that section are considered. This prevents
        crosstalk when the same description (e.g. "Cable service") appears in
        both the revenue and expense sections of an audit.
        Returns (rule, confidence) or (None, 0).
        """
        normalized_item = line_item.lower().strip()
        best_match = None
        best_score = 0

        for rule in rules:
            if section is not None and _category_section(rule.century_category) != section:
                continue
            normalized_rule = rule.auditor_line_item.lower().strip()
            # Simple containment matching; could be enhanced with Levenshtein
            if normalized_item == normalized_rule:
                return (rule, 1.0)
            elif normalized_rule in normalized_item or normalized_item in normalized_rule:
                # Partial match
                score = len(normalized_rule) / max(len(normalized_item), len(normalized_rule))
                if score > best_score:
                    best_match = rule
                    best_score = score

        return (best_match, best_score)


    def apply_mapping_rules(extracted_data, profile_id):
        """
        Apply mapping rules to extracted data.
        Returns (mapped_data_dict, unmapped_items_list).
        """
        profile = AuditorProfile.query.get(profile_id)
        if not profile:
            return None, ["No profile found"]

        rules = profile.rules

        # Initialize mapped categories with per-year tracking
        fiscal_years = []
        try:
            extracted_parsed = json.loads(extracted_data) if isinstance(extracted_data, str) else extracted_data
            fiscal_years = extracted_parsed.get("fiscal_years", [])
        except:
            pass
        num_years = len(fiscal_years) if fiscal_years else 2
        mapped = {cat: {"total": 0, "years": [], "year_totals": [0] * num_years} for cat in CENTURY_CATEGORIES}

        try:
            extracted = json.loads(extracted_data) if isinstance(extracted_data, str) else extracted_data
        except:
            return None, ["Invalid extracted data JSON"]

        unmapped = []

        # Process revenue items
        if "revenue" in extracted and "items" in extracted["revenue"]:
            for item in extracted["revenue"]["items"]:
                description = item.get("description", "")
                amounts = item.get("amounts", [])
                rule, confidence = fuzzy_match_rule(description, rules, section="revenue")

                if rule and confidence > 0.5:
                    cat = rule.century_category
                    pct = rule.split_pct
                    for i, amount in enumerate(amounts):
                        if isinstance(amount, (int, float)):
                            mapped[cat]["total"] += amount * pct
                            if i < len(mapped[cat]["year_totals"]):
                                mapped[cat]["year_totals"][i] += amount * pct
                else:
                    unmapped.append({
                        "type": "revenue",
                        "description": description,
                        "amounts": amounts
                    })

        # Process expense items
        if "expenses" in extracted and "categories" in extracted["expenses"]:
            raw_cats = extracted["expenses"]["categories"]
            # Normalize Phase 2 flat format (object with numeric keys) into array format
            if isinstance(raw_cats, dict):
                flat_items = []
                for key in sorted(raw_cats.keys(), key=lambda k: int(k) if k.isdigit() else k):
                    val = raw_cats[key]
                    if isinstance(val, list):
                        flat_items.extend(val)
                    elif isinstance(val, dict) and "description" in val:
                        flat_items.append(val)
                raw_cats = [{"name": "Expenses", "items": flat_items}]
            for cat_group in raw_cats:
                items = cat_group.get("items", []) if isinstance(cat_group, dict) else []
                for item in items:
                    description = item.get("description", "")
                    amounts = item.get("amounts", [])
                    rule, confidence = fuzzy_match_rule(description, rules, section="expense")

                    if rule and confidence > 0.5:
                        cat = rule.century_category
                        pct = rule.split_pct
                        for i, amount in enumerate(amounts):
                            if isinstance(amount, (int, float)):
                                mapped[cat]["total"] += amount * pct
                                if i < len(mapped[cat]["year_totals"]):
                                    mapped[cat]["year_totals"][i] += amount * pct
                    else:
                        unmapped.append({
                            "type": "expense",
                            "description": description,
                            "amounts": amounts
                        })

        return mapped, unmapped


    def extract_from_pdf(pdf_path, building_name, entity_code=None):
        """
        Extract Schedule of Expenses and Revenue from PDF using Claude API.
        Returns extracted data as dict or None on error.
        """
        try:
            import anthropic
        except ImportError:
            logger.error("anthropic library not installed")
            return None

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            logger.error("ANTHROPIC_API_KEY not set")
            return None

        try:
            # Read PDF and encode to base64
            with open(pdf_path, "rb") as f:
                pdf_data = base64.standard_b64encode(f.read()).decode("utf-8")

            client = anthropic.Anthropic(api_key=api_key)

            # ── Build building-aware category list ─────────────────────────
            budget_categories = None
            if entity_code:
                try:
                    rows = db.session.execute(db.text(
                        "SELECT label, section FROM budget_summary_rows "
                        "WHERE entity_code = :ec AND row_type = 'data' "
                        "ORDER BY display_order"
                    ), {"ec": entity_code}).fetchall()
                    if rows:
                        income_labels = [r[0] for r in rows if r[1] and r[1].lower() == "income"]
                        expense_labels = [r[0] for r in rows if r[1] and r[1].lower() == "expenses"]
                        nonop_inc = [r[0] for r in rows if r[1] and "non" in r[1].lower() and "income" in r[1].lower()]
                        nonop_exp = [r[0] for r in rows if r[1] and "non" in r[1].lower() and "expense" in r[1].lower()]
                        budget_categories = {
                            "income": income_labels,
                            "expenses": expense_labels,
                            "non_operating_income": nonop_inc,
                            "non_operating_expense": nonop_exp,
                        }
                except Exception as e:
                    logger.warning(f"Could not load building categories for {entity_code}: {e}")

            if budget_categories:
                cat_instruction = f"""
IMPORTANT: This building's budget uses these EXACT categories. You MUST map every
auditor line item into one of these categories. Do NOT use the auditor's own category
names — force everything into the categories below.

INCOME categories: {json.dumps(budget_categories['income'])}
EXPENSE categories: {json.dumps(budget_categories['expenses'])}
NON-OPERATING INCOME: {json.dumps(budget_categories['non_operating_income'])}
NON-OPERATING EXPENSE: {json.dumps(budget_categories['non_operating_expense'])}

When the auditor lumps items together (e.g. "Utilities" or "Administrative"),
look for supplementary schedules or notes in the PDF that break them down.
For example:
- "Utilities" should be split into Electric, Gas, Water & Sewer, etc.
- "Administrative" should be split into Insurance, Professional Fees, Administrative & Other, etc.
- "Repairs and maintenance" may need to be split into Supplies vs Repairs & Maintenance.

If a supplementary schedule provides the breakdown, use those numbers.
If no breakdown exists, map the lump sum to the BEST matching single category.

Each item in the JSON output must use one of the exact category names listed above as its "description".
Combine auditor line items that map to the same category into one entry with the summed amount.

CRITICAL: For EVERY item, include a "source_lines" array showing EXACTLY what the
auditor's original line items were and their individual amounts. This lets the user
verify your mapping. Each source line should have the auditor's exact description and amounts.
If a category is a direct 1:1 match (auditor used the same name), still include it in source_lines.
"""
            else:
                cat_instruction = """
Extract all line items using the auditor's own descriptions.
"""

            extraction_prompt = f"""
You are analyzing an audited financial statement for {building_name}.

Find the Schedule of Expenses and Schedule of Revenue pages, AND any
supplementary schedules that break down categories into sub-items.

{cat_instruction}

Return ONLY valid JSON (no markdown, no code blocks) with this structure:
{{
  "building_name": "{building_name}",
  "fiscal_years": [2025, 2024],
  "revenue": {{
    "items": [
      {{"description": "Maintenance", "amounts": [8760380, 8588595], "source_lines": [
        {{"auditor_desc": "Maintenance charges", "amounts": [8760380, 8588595]}}
      ]}},
      {{"description": "Other Income", "amounts": [100000, 95000], "source_lines": [
        {{"auditor_desc": "Interest income", "amounts": [60000, 55000]}},
        {{"auditor_desc": "Miscellaneous revenue", "amounts": [40000, 40000]}}
      ]}}
    ],
    "total": [8860380, 8683595]
  }},
  "expenses": {{
    "categories": {{
      "0": [{{"description": "Payroll", "amounts": [3000000, 2900000], "source_lines": [
        {{"auditor_desc": "Superintendent", "amounts": [120000, 115000]}},
        {{"auditor_desc": "Doorman and security", "amounts": [200000, 190000]}},
        {{"auditor_desc": "Payroll taxes and benefits", "amounts": [2680000, 2595000]}}
      ]}}],
      "1": [{{"description": "Electric", "amounts": [50000, 48000], "source_lines": [
        {{"auditor_desc": "Electricity", "amounts": [50000, 48000]}}
      ]}}]
    }},
    "total_expenses": [3050000, 2948000]
  }}
}}

RULES:
- "description" = the Century budget category name (from the lists above)
- "source_lines" = the auditor's ORIGINAL line items with their exact descriptions and amounts
- source_lines amounts must sum to the parent item's amounts
- Be precise with numbers. Include all line items found.
- Revenue total and expense total must equal the audited totals in the PDF.
"""

            message = client.messages.create(
                model=os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
                max_tokens=4096,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "document",
                                "source": {
                                    "type": "base64",
                                    "media_type": "application/pdf",
                                    "data": pdf_data
                                }
                            },
                            {
                                "type": "text",
                                "text": extraction_prompt
                            }
                        ]
                    }
                ]
            )

            response_text = message.content[0].text.strip()
            # Clean markdown code blocks if present
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            if response_text.startswith("```"):
                response_text = response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()

            extracted = json.loads(response_text)
            return extracted

        except Exception as e:
            logger.error(f"PDF extraction error: {e}", exc_info=True)
            raise  # Re-raise so caller can return the actual error message


    def get_confirmed_actuals(entity_code, year):
        """
        Get confirmed mapped data for a building/year.
        Returns dict of {century_category: amount}.
        """
        upload = AuditUpload.query.filter_by(
            entity_code=entity_code,
            fiscal_year_end=str(year),
            status="confirmed"
        ).first()

        if not upload or not upload.mapped_data:
            return {}

        try:
            mapped = json.loads(upload.mapped_data)
            # Return first year's totals (index 0)
            result = {}
            for cat, data in mapped.items():
                if isinstance(data, dict):
                    totals = data.get("year_totals", data.get("years", []))
                    if totals and len(totals) > 0:
                        result[cat] = totals[0]
                    elif data.get("total"):
                        result[cat] = data["total"]
            return result
        except:
            return {}

    # ─── Flask Blueprint ──────────────────────────────────────────────────────

    bp = Blueprint("audited_financials", __name__)

    # ─── Pages ────────────────────────────────────────────────────────────────

    @bp.route("/audited-financials/bulk-upload", methods=["GET"])
    def bulk_upload_page():
        """Bulk upload page - select multiple PDFs, auto-match entities, upload all at once."""
        try:
            buildings = get_buildings_list()
            profiles = AuditorProfile.query.all()
            buildings_json = json.dumps([{"entity_code": b["entity_code"], "building_name": b["building_name"]} for b in buildings])
            profiles_json = json.dumps([{"id": p.id, "name": p.name + " (" + p.firm_name + ")"} for p in profiles])
        except Exception as e:
            logger.error("Bulk upload page setup error: %s", e)
            import traceback
            return "<pre>" + traceback.format_exc() + "</pre>", 500

        html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Bulk Upload - Audited Financials</title>
    <style>
        /* Force scrollbars always visible (fixes macOS auto-hide) */
        ::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
        ::-webkit-scrollbar-track { background: #f1f5f9; }
        ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
        ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
        ::-webkit-scrollbar-corner { background: #f1f5f9; }
        * { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
        :root { --blue: #1a56db; --green: #057a55; --gray-50: #f9fafb; --gray-100: #f3f4f6; --gray-200: #e5e7eb; --gray-300: #d1d5db; --gray-500: #6b7280; --gray-700: #374151; }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: linear-gradient(135deg, #fdf6ee 0%, #f5ebe0 100%); min-height: 100vh; color: var(--gray-700); }
        header { background: linear-gradient(135deg, #3d1c00 0%, #6b3410 100%); color: white; padding: 16px 24px; }
        header a { color: rgba(255,255,255,0.8); text-decoration: none; font-size: 13px; }
        header h1 { font-size: 28px; font-weight: 700; }
        .container { max-width: 1200px; margin: 0 auto; padding: 32px 20px; }
        .section { background: white; border-radius: 12px; padding: 28px; margin-bottom: 24px; border: 1px solid var(--gray-200); }
        h2 { font-size: 18px; font-weight: 600; margin-bottom: 16px; color: var(--blue); }
        .drop-zone { border: 3px dashed var(--gray-300); border-radius: 12px; padding: 48px; text-align: center; cursor: pointer; transition: all 0.2s; background: var(--gray-50); }
        .drop-zone:hover, .drop-zone.drag-over { border-color: var(--blue); background: #eef2ff; }
        .drop-zone p { font-size: 16px; color: var(--gray-500); margin-bottom: 8px; }
        .drop-zone .big { font-size: 36px; margin-bottom: 12px; }
        table { width: 100%; border-collapse: collapse; margin-top: 16px; }
        th { background: var(--gray-100); padding: 10px 12px; text-align: left; font-weight: 600; font-size: 12px; text-transform: uppercase; color: var(--gray-500); border-bottom: 1px solid var(--gray-200); }
        td { padding: 8px 12px; border-bottom: 1px solid var(--gray-200); font-size: 13px; }
        select { padding: 6px 8px; border: 1px solid var(--gray-300); border-radius: 4px; font-size: 13px; width: 100%; }
        input[type=text] { padding: 6px 8px; border: 1px solid var(--gray-300); border-radius: 4px; font-size: 13px; width: 80px; }
        .btn { background: var(--green); color: white; border: none; padding: 12px 28px; border-radius: 6px; font-size: 15px; font-weight: 600; cursor: pointer; }
        .btn:hover { background: #046c4e; }
        .btn:disabled { background: var(--gray-300); cursor: not-allowed; }
        .status { font-weight: 600; font-size: 12px; }
        .status-pending { color: var(--gray-500); }
        .status-uploading { color: #92400e; }
        .status-done { color: #057a55; }
        .status-error { color: #dc2626; }
        .progress-bar { width: 100%; height: 6px; background: var(--gray-200); border-radius: 3px; margin-top: 16px; overflow: hidden; }
        .progress-fill { height: 100%; background: var(--green); transition: width 0.3s; width: 0%; }
        #summary { margin-top: 16px; font-size: 14px; }
        .remove-btn { background: none; border: none; color: #dc2626; cursor: pointer; font-size: 16px; padding: 2px 6px; }
    </style>
</head>
<body>
<header>
    <a href="/audited-financials">&larr; Back to Audited Financials</a>
    <h1>Bulk Upload</h1>
    <p>Upload multiple audited financial PDFs at once</p>
</header>
<div class="container">
    <div class="section">
        <h2>1. Select PDF Files</h2>
        <div class="drop-zone" id="dropZone" onclick="document.getElementById('fileInput').click()">
            <div class="big">&#128196;</div>
            <p><strong>Click to select files</strong> or drag and drop</p>
            <p style="font-size:13px; color:#999;">PDF files only &middot; Entity codes auto-matched from filenames</p>
        </div>
        <input type="file" id="fileInput" multiple accept=".pdf" style="display:none" onchange="handleFiles(this.files)">
    </div>

    <div class="section" id="fileListSection" style="display:none">
        <h2>2. Review & Assign</h2>
        <p style="font-size:13px; color:var(--gray-500); margin-bottom:12px;">Entity codes are auto-detected from filenames. Adjust as needed.</p>
        <table>
            <thead>
                <tr><th>File</th><th>Entity</th><th>Auditor Profile</th><th>Year</th><th>Status</th><th></th></tr>
            </thead>
            <tbody id="fileTableBody"></tbody>
        </table>
        <div class="progress-bar" id="progressBar" style="display:none"><div class="progress-fill" id="progressFill"></div></div>
        <div id="summary"></div>
        <div style="margin-top:20px; display:flex; gap:12px; align-items:center;">
            <button class="btn" id="uploadAllBtn" onclick="uploadAll()">Upload All</button>
            <span id="uploadCount" style="font-size:13px; color:var(--gray-500);"></span>
        </div>
    </div>
</div>

<script>
const BUILDINGS = __BUILDINGS_JSON__;
const PROFILES = __PROFILES_JSON__;
let fileQueue = [];

const dz = document.getElementById('dropZone');
dz.addEventListener('dragover', function(e) { e.preventDefault(); dz.classList.add('drag-over'); });
dz.addEventListener('dragleave', function() { dz.classList.remove('drag-over'); });
dz.addEventListener('drop', function(e) { e.preventDefault(); dz.classList.remove('drag-over'); handleFiles(e.dataTransfer.files); });

function guessEntity(filename) {
    var m = filename.match(/^(\\d{3})\\s*[-\\s]/);
    if (m) {
        var code = m[1];
        var b = BUILDINGS.find(function(b) { return b.entity_code === code; });
        if (b) return { code: code, name: b.building_name, confident: true };
    }
    var knownMappings = { '147 Waverly': '826', '142 E': '733' };
    for (var pattern in knownMappings) {
        if (filename.indexOf(pattern) !== -1) {
            var code2 = knownMappings[pattern];
            var b2 = BUILDINGS.find(function(b) { return b.entity_code === code2; });
            if (b2) return { code: code2, name: b2.building_name, confident: true };
        }
    }
    return { code: '', name: '', confident: false };
}

function handleFiles(files) {
    for (var fi = 0; fi < files.length; fi++) {
        var f = files[fi];
        if (!f.name.toLowerCase().endsWith('.pdf')) continue;
        var dup = false;
        for (var qi = 0; qi < fileQueue.length; qi++) { if (fileQueue[qi].file.name === f.name) { dup = true; break; } }
        if (dup) continue;
        var guess = guessEntity(f.name);
        fileQueue.push({ file: f, entity: guess.code, confident: guess.confident, profile: '', year: '2025', status: 'pending' });
    }
    renderTable();
    document.getElementById('fileListSection').style.display = 'block';
}

function setEntity(idx, val) { fileQueue[idx].entity = val; }
function setProfile(idx, val) { fileQueue[idx].profile = val; }
function setYear(idx, val) { fileQueue[idx].year = val; }
function removeFile(idx) { fileQueue.splice(idx, 1); renderTable(); }

function renderTable() {
    var tbody = document.getElementById('fileTableBody');
    var html = '';
    for (var i = 0; i < fileQueue.length; i++) {
        var q = fileQueue[i];
        var entityOpts = '';
        for (var bi = 0; bi < BUILDINGS.length; bi++) {
            var b = BUILDINGS[bi];
            entityOpts += '<option value="' + b.entity_code + '"' + (b.entity_code === q.entity ? ' selected' : '') + '>' + b.entity_code + ' - ' + b.building_name + '</option>';
        }
        var profileOpts = '';
        for (var pi = 0; pi < PROFILES.length; pi++) {
            var p = PROFILES[pi];
            profileOpts += '<option value="' + p.id + '"' + (String(p.id) === String(q.profile) ? ' selected' : '') + '>' + p.name + '</option>';
        }
        var statusClass = q.status === 'done' ? 'status-done' : q.status === 'error' ? 'status-error' : q.status === 'uploading' ? 'status-uploading' : 'status-pending';
        var statusText = q.status === 'done' ? '&#10003; Done' : q.status === 'error' ? '&#10007; Error' : q.status === 'uploading' ? 'Uploading...' : 'Pending';
        var dis = q.status !== 'pending' ? ' disabled' : '';
        html += '<tr>';
        html += '<td title="' + q.file.name + '" style="max-width:280px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">' + q.file.name + '</td>';
        html += '<td><select onchange="setEntity(' + i + ',this.value)"' + dis + '><option value="">Select...</option>' + entityOpts + '</select></td>';
        html += '<td><select onchange="setProfile(' + i + ',this.value)"' + dis + '><option value="">None</option>' + profileOpts + '</select></td>';
        html += '<td><input type="text" value="' + q.year + '" onchange="setYear(' + i + ',this.value)"' + dis + '></td>';
        html += '<td><span class="status ' + statusClass + '">' + statusText + '</span></td>';
        html += '<td>' + (q.status === 'pending' ? '<button class="remove-btn" onclick="removeFile(' + i + ')">&times;</button>' : '') + '</td>';
        html += '</tr>';
    }
    tbody.innerHTML = html;
    var matched = 0;
    for (var mi = 0; mi < fileQueue.length; mi++) { if (fileQueue[mi].entity) matched++; }
    document.getElementById('uploadCount').textContent = matched + ' of ' + fileQueue.length + ' files matched';
}

async function uploadAll() {
    var btn = document.getElementById('uploadAllBtn');
    btn.disabled = true;
    btn.textContent = 'Uploading...';
    document.getElementById('progressBar').style.display = 'block';
    var fill = document.getElementById('progressFill');
    var done = 0;
    var total = fileQueue.length;

    for (var i = 0; i < fileQueue.length; i++) {
        var q = fileQueue[i];
        if (q.status === 'done') { done++; continue; }
        if (!q.entity) { q.status = 'error'; renderTable(); done++; continue; }
        q.status = 'uploading';
        renderTable();
        try {
            var fd = new FormData();
            fd.append('entity_code', q.entity);
            fd.append('profile_id', q.profile || '');
            fd.append('fiscal_year_end', q.year);
            fd.append('pdf', q.file);
            var r = await fetch('/api/af/upload', { method: 'POST', body: fd });
            var d = await r.json();
            if (d.success) { q.status = 'done'; q.uploadId = d.upload_id; }
            else { q.status = 'error'; }
        } catch(e) { q.status = 'error'; }
        done++;
        fill.style.width = (done / total * 100) + '%';
        renderTable();
    }

    btn.textContent = 'Done!';
    var doneCount = 0, errCount = 0;
    for (var j = 0; j < fileQueue.length; j++) {
        if (fileQueue[j].status === 'done') doneCount++;
        if (fileQueue[j].status === 'error') errCount++;
    }
    var msg = '<strong>' + doneCount + '</strong> uploaded successfully';
    if (errCount > 0) msg += ', <strong style="color:#dc2626">' + errCount + '</strong> failed';
    msg += '. <a href="/audited-financials">&larr; Back to main page</a>';
    document.getElementById('summary').innerHTML = msg;
}
</script>
</body>
</html>"""
        try:
            html = html.replace('__BUILDINGS_JSON__', buildings_json)
            html = html.replace('__PROFILES_JSON__', profiles_json)
            return html
        except Exception as e:
            logger.error("Bulk upload page render error: %s", e)
            import traceback
            return "<pre>" + traceback.format_exc() + "</pre>", 500

    @bp.route("/audited-financials", methods=["GET"])
    def main_page():
        """Main audited financials page."""
        uploads = AuditUpload.query.order_by(AuditUpload.created_at.desc()).all()
        buildings = get_buildings_list()
        profiles = AuditorProfile.query.all()

        html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Audited Financials - Century Management</title>
    <style>
        /* Force scrollbars always visible (fixes macOS auto-hide) */
        ::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
        ::-webkit-scrollbar-track { background: #f1f5f9; }
        ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
        ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
        ::-webkit-scrollbar-corner { background: #f1f5f9; }
        * { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
        :root { --blue: #5a4a3f; --blue-light: #f5efe7; --green: #057a55; --green-light: #def7ec; --red: #e02424; --gray-50: #f4f1eb; --gray-100: #ede9e1; --gray-200: #e5e0d5; --gray-300: #d5cfc5; --gray-500: #8a7e72; --gray-700: #4a4039; --gray-900: #1a1714; }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--gray-50); color: var(--gray-900); line-height: 1.5; }
        header { background: linear-gradient(135deg, #2c2825 0%, #3d322a 100%); color: white; padding: 30px 20px; }
        header a { color: white; text-decoration: none; font-size: 14px; }
        header a:hover { text-decoration: underline; }
        header h1 { font-size: 28px; font-weight: 700; }
        header p { font-size: 14px; opacity: 0.85; margin-top: 4px; }
        .container { max-width: 1100px; margin: 0 auto; padding: 32px 20px; }
        .section { background: white; border-radius: 12px; padding: 28px; margin-bottom: 24px; border: 1px solid var(--gray-200); }
        .section h2 { font-size: 18px; font-weight: 600; margin-bottom: 20px; color: var(--blue); }
        .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
        .form-group { margin-bottom: 16px; }
        label { display: block; font-size: 13px; font-weight: 600; margin-bottom: 6px; color: var(--gray-700); }
        input, select { width: 100%; padding: 10px 12px; border: 1px solid var(--gray-300); border-radius: 6px; font-size: 14px; }
        input:focus, select:focus { outline: none; border-color: var(--blue); box-shadow: 0 0 0 3px var(--blue-light); }
        button { background: var(--blue); color: white; border: none; padding: 10px 20px; border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.15s; }
        button:hover { background: #1542b8; }
        .btn-green { background: var(--green); }
        .btn-green:hover { background: #046c4e; }
        .btn-small { padding: 6px 12px; font-size: 12px; }
        .btn-delete { background: #e02424; margin-left: 6px; }
        .btn-delete:hover { background: #d01f1f; }
        table { width: 100%; border-collapse: collapse; }
        th { background: var(--gray-100); padding: 10px 12px; text-align: left; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--gray-500); border-bottom: 1px solid var(--gray-200); }
        td { padding: 10px 12px; border-bottom: 1px solid var(--gray-200); font-size: 14px; }
        tr:hover { background: var(--gray-50); }
        .status-pill { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }
        .status-uploaded { background: #fef3c7; color: #92400e; }
        .status-extracted { background: #dbeafe; color: #1e40af; }
        .status-mapped { background: #d1fae5; color: #065f46; }
        .status-confirmed { background: #c7d2fe; color: #3730a3; }
        .alert { padding: 10px 14px; border-radius: 6px; margin: 10px 0; font-size: 13px; }
        .alert-info { background: #fef3c7; color: #92400e; }
        .alert-success { background: var(--green-light); color: #065f46; }
        .alert-error { background: #fde8e8; color: #9b1c1c; }
        .header-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
        .header-row h2 { margin-bottom: 0; }
        .profiles-link { color: var(--blue); text-decoration: none; font-size: 14px; font-weight: 600; }
        .profiles-link:hover { text-decoration: underline; }
    </style>
</head>
<body>
<header>
    <a href="/">← Home</a>
    <h1>Audited Financials</h1>
    <p>Upload and extract audited financial statements</p>
</header>
<div class="container">
    <div class="section">
        <div class="header-row">
            <h2>Upload Audit PDF</h2>
            <a href="/audited-financials/bulk-upload" class="profiles-link" style="margin-right:16px;">Bulk Upload →</a>
            <a href="/audited-financials/profiles" class="profiles-link">Manage Profiles & Rules →</a>
        </div>
        <div class="form-row">
            <div class="form-group">
                <label>Building</label>
                <select id="entityCode">
                    <option value="">Select building...</option>
                    {{ buildings_options }}
                </select>
            </div>
            <div class="form-group">
                <label>Auditor Profile</label>
                <select id="profileId">
                    <option value="">Select auditor...</option>
                    {{ profiles_options }}
                </select>
            </div>
        </div>
        <div class="form-row">
            <div class="form-group">
                <label>Fiscal Year End</label>
                <input type="text" id="fiscalYearEnd" placeholder="2024" />
            </div>
            <div class="form-group">
                <label>PDF File</label>
                <input type="file" id="pdfFile" accept=".pdf" />
            </div>
        </div>
        <button class="btn-green" onclick="uploadPDF()">Upload & Extract</button>
        <div id="uploadStatus"></div>
    </div>

    <div class="section">
        <h2>All Uploads</h2>
        <div id="uploadsTable">
            {{ uploads_table }}
        </div>
    </div>
</div>

<script>
    function uploadPDF() {
        const entityCode = document.getElementById('entityCode').value;
        const profileId = document.getElementById('profileId').value;
        const fiscalYearEnd = document.getElementById('fiscalYearEnd').value;
        const pdfFile = document.getElementById('pdfFile').files[0];

        if (!entityCode || !pdfFile) {
            document.getElementById('uploadStatus').innerHTML = '<div class="alert alert-error">Select building and PDF file</div>';
            return;
        }

        const formData = new FormData();
        formData.append('entity_code', entityCode);
        formData.append('profile_id', profileId || '');
        formData.append('fiscal_year_end', fiscalYearEnd);
        formData.append('pdf', pdfFile);

        document.getElementById('uploadStatus').innerHTML = '<div class="alert alert-info">Uploading...</div>';

        fetch('/api/af/upload', { method: 'POST', body: formData })
        .then(r => r.json())
        .then(data => {
            if (data.success) {
                document.getElementById('uploadStatus').innerHTML = '<div class="alert alert-success">Upload successful. Extracting...</div>';
                extractUpload(data.upload_id);
            } else {
                document.getElementById('uploadStatus').innerHTML = '<div class="alert alert-error">Error: ' + data.error + '</div>';
            }
        })
        .catch(err => {
            document.getElementById('uploadStatus').innerHTML = '<div class="alert alert-error">Error: ' + err.message + '</div>';
        });
    }

    function extractUpload(uploadId) {
        fetch('/api/af/extract/' + uploadId, { method: 'POST' })
        .then(r => r.json())
        .then(data => {
            if (data.success) {
                document.getElementById('uploadStatus').innerHTML += '<div class="alert alert-success">Extraction complete. Applying mapping rules...</div>';
                mapUpload(uploadId);
            } else {
                document.getElementById('uploadStatus').innerHTML += '<div class="alert alert-error">Extraction error: ' + data.error + '</div>';
            }
        });
    }

    function mapUpload(uploadId) {
        fetch('/api/af/map/' + uploadId, { method: 'POST' })
        .then(r => r.json())
        .then(data => {
            if (data.success) {
                document.getElementById('uploadStatus').innerHTML += '<div class="alert alert-success">Mapping complete. Reloading...</div>';
                setTimeout(() => location.reload(), 1500);
            } else {
                document.getElementById('uploadStatus').innerHTML += '<div class="alert alert-error">Mapping error: ' + data.error + '</div>';
            }
        });
    }

    function reviewUpload(uploadId) {
        window.location.href = '/audited-financials/review/' + uploadId;
    }

    function deleteUpload(uploadId) {
        if (!confirm('Delete this upload? This cannot be undone.')) return;
        fetch('/api/af/uploads/' + uploadId, { method: 'DELETE' })
        .then(r => r.json())
        .then(data => {
            if (data.success) {
                const row = document.getElementById('upload-row-' + uploadId);
                if (row) row.remove();
            } else {
                alert('Error: ' + data.error);
            }
        })
        .catch(err => alert('Error: ' + err.message));
    }
</script>
</body>
</html>
        """

        # Build buildings options
        buildings_options = "\n".join([
            f'<option value="{b["entity_code"]}">{b["entity_code"]} - {b["building_name"]}</option>'
            for b in buildings
        ])

        # Build profiles options
        profiles_options = "\n".join([
            f'<option value="{p.id}">{p.name} ({p.firm_name})</option>'
            for p in profiles
        ])

        # Build uploads table
        rows = []
        for u in uploads:
            delete_btn = f'<button class="btn-small btn-delete" onclick="deleteUpload({u.id})">Delete</button>'
            rows.append(f"""
                <tr id="upload-row-{u.id}">
                    <td style="font-weight:600;">{u.entity_code}</td>
                    <td>{u.building_name}</td>
                    <td>{u.profile.name if u.profile else "—"}</td>
                    <td>{u.fiscal_year_end}</td>
                    <td><span class="status-pill status-{u.status}">{u.status.title()}</span></td>
                    <td style="white-space:nowrap;">
                        <button class="btn-small" onclick="reviewUpload({u.id})">Review</button>
                        {delete_btn}
                    </td>
                </tr>
            """)
        uploads_table = f"""
            <table>
                <thead>
                <tr>
                    <th>Entity</th>
                    <th>Building</th>
                    <th>Auditor</th>
                    <th>Year</th>
                    <th>Status</th>
                    <th>Actions</th>
                </tr>
                </thead>
                <tbody>
                {"".join(rows) if rows else "<tr><td colspan='6' style='text-align: center; padding: 30px; color: var(--gray-500);'>No uploads yet</td></tr>"}
                </tbody>
            </table>
        """

        html = html.replace("{{ buildings_options }}", buildings_options)
        html = html.replace("{{ profiles_options }}", profiles_options)
        html = html.replace("{{ uploads_table }}", uploads_table)

        return render_template_string(html)


    @bp.route("/audited-financials/profiles", methods=["GET"])
    def profiles_page():
        """Manage auditor profiles and mapping rules."""
        profiles = AuditorProfile.query.all()

        html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Auditor Profiles - Century Management</title>
    <style>
        /* Force scrollbars always visible (fixes macOS auto-hide) */
        ::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
        ::-webkit-scrollbar-track { background: #f1f5f9; }
        ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
        ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
        ::-webkit-scrollbar-corner { background: #f1f5f9; }
        * { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
        :root { --blue: #5a4a3f; --blue-light: #f5efe7; --green: #057a55; --green-light: #def7ec; --red: #e02424; --gray-50: #f4f1eb; --gray-100: #ede9e1; --gray-200: #e5e0d5; --gray-300: #d5cfc5; --gray-500: #8a7e72; --gray-700: #4a4039; --gray-900: #1a1714; }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--gray-50); color: var(--gray-900); line-height: 1.5; }
        header { background: linear-gradient(135deg, #2c2825 0%, #3d322a 100%); color: white; padding: 30px 20px; }
        header a { color: white; text-decoration: none; font-size: 14px; }
        header a:hover { text-decoration: underline; }
        header h1 { font-size: 28px; font-weight: 700; }
        header p { font-size: 14px; opacity: 0.85; margin-top: 4px; }
        .container { max-width: 1100px; margin: 0 auto; padding: 32px 20px; }
        .section { background: white; border-radius: 12px; padding: 28px; margin-bottom: 24px; border: 1px solid var(--gray-200); }
        .section h2 { font-size: 18px; font-weight: 600; margin-bottom: 20px; color: var(--blue); }
        .form-group { margin-bottom: 16px; }
        label { display: block; font-size: 13px; font-weight: 600; margin-bottom: 6px; color: var(--gray-700); }
        input, select, textarea { width: 100%; padding: 10px 12px; border: 1px solid var(--gray-300); border-radius: 6px; font-size: 14px; }
        input:focus, select:focus, textarea:focus { outline: none; border-color: var(--blue); box-shadow: 0 0 0 3px var(--blue-light); }
        textarea { height: 60px; resize: vertical; }
        button { background: var(--blue); color: white; border: none; padding: 10px 20px; border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.15s; }
        button:hover { background: #1542b8; }
        .btn-green { background: var(--green); }
        .btn-green:hover { background: #046c4e; }
        .btn-danger { background: var(--red); }
        .btn-danger:hover { background: #d01f1f; }
        .btn-small { padding: 6px 12px; font-size: 12px; }
        .btn-edit { background: #eff6ff; color: #2563eb; }
        .btn-edit:hover { background: #dbeafe; }
        .profile-card { background: white; border: 1px solid var(--gray-200); border-radius: 12px; margin-bottom: 16px; overflow: hidden; transition: box-shadow 0.2s; }
        .profile-card:hover { box-shadow: 0 2px 8px rgba(0,0,0,0.04); }
        .profile-header { display: flex; justify-content: space-between; align-items: center; padding: 18px 24px; cursor: pointer; user-select: none; transition: background 0.15s; }
        .profile-header:hover { background: #fafafa; }
        .profile-header-left { display: flex; align-items: center; gap: 14px; }
        .collapse-chevron { width: 20px; height: 20px; transition: transform 0.25s ease; color: var(--gray-500); flex-shrink: 0; display: inline-block; }
        .profile-card.open .collapse-chevron { transform: rotate(90deg); }
        .profile-card.open .profile-header { border-bottom: 1px solid var(--gray-200); }
        .profile-header h3 { font-size: 15px; font-weight: 600; color: var(--gray-900); }
        .profile-meta { font-size: 12px; color: var(--gray-500); margin-top: 2px; }
        .profile-header-right { display: flex; align-items: center; gap: 8px; }
        .rule-count { font-size: 11px; font-weight: 600; color: var(--gray-500); background: var(--gray-100); padding: 3px 10px; border-radius: 12px; }
        .profile-body { max-height: 0; overflow: hidden; transition: max-height 0.35s ease, padding 0.35s ease; padding: 0 24px; }
        .profile-card.open .profile-body { max-height: 3000px; padding: 0 24px 20px; }
        .profile-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 14px; }
        .rules-table { width: 100%; border-collapse: collapse; margin-top: 12px; }
        .rules-table th { background: var(--gray-100); padding: 8px 10px; text-align: left; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--gray-500); border-bottom: 1px solid var(--gray-200); }
        .rules-table td { padding: 8px 10px; border-bottom: 1px solid #f3f4f6; font-size: 13px; color: #374151; }
        .rules-table tr:last-child td { border-bottom: none; }
        .rules-table tr:hover td { background: #fafbfc; }
        .rules-table input, .rules-table select { padding: 6px 8px; font-size: 13px; }
        .category-badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; background: #f0fdf4; color: #166534; }
        .read-field { display: inline; }
        .edit-field { display: none; }
        .profile-card.editing .read-field { display: none; }
        .profile-card.editing .edit-field { display: inline; }
        .profile-card.editing .edit-field select,
        .profile-card.editing .edit-field input { padding: 5px 8px; font-size: 12px; border: 1px solid var(--gray-200); border-radius: 4px; font-family: inherit; }
        .btn-row { display: flex; gap: 10px; margin-top: 14px; padding-top: 14px; border-top: 1px solid var(--gray-200); }
        .alert { padding: 10px 14px; border-radius: 6px; margin: 10px 0; font-size: 13px; }
        .alert-success { background: var(--green-light); color: #065f46; }
        .alert-error { background: #fde8e8; color: #9b1c1c; }
        .back-link { color: var(--blue); text-decoration: none; font-size: 14px; font-weight: 600; }
        .back-link:hover { text-decoration: underline; }
    </style>
</head>
<body>
<header>
    <a href="/">← Home</a>
    <h1>Auditor Profiles & Mapping Rules</h1>
    <p>Configure how audited financial line items map to Century budget categories</p>
</header>
<div class="container">
    <div style="margin-bottom:20px;"><a href="/audited-financials" class="back-link">← Back to Uploads</a></div>

    <div class="section">
        <h2>Create New Profile</h2>
        <div class="form-group">
            <label>Display Name</label>
            <input type="text" id="newProfileName" placeholder="e.g., Prager Metis" />
        </div>
        <div class="form-group">
            <label>Firm Name</label>
            <input type="text" id="newFirmName" placeholder="e.g., Prager Metis CPAs LLC" />
        </div>
        <div class="form-group">
            <label>Notes</label>
            <textarea id="newProfileNotes" placeholder="Optional notes..."></textarea>
        </div>
        <button onclick="createProfile()">Create Profile</button>
        <div id="createStatus"></div>
    </div>

    <div style="margin-bottom:16px;"><h2 style="font-size:18px; font-weight:600; color:var(--blue);">Existing Profiles</h2></div>
    <div id="profilesList">
        {{ profiles_list }}
    </div>

    <script>
        function toggleProfile(id) {
            document.getElementById(id).classList.toggle('open');
        }

        function toggleEdit(id) {
            var card = document.getElementById(id);
            card.classList.toggle('editing');
            if (card.classList.contains('editing') && !card.classList.contains('open')) {
                card.classList.add('open');
            }
        }

        function toggleEditName(id) {
            var profileId = id.replace('profile-', '');
            var row = document.getElementById('nameEdit-' + profileId);
            row.style.display = row.style.display === 'none' ? 'block' : 'none';
        }

        function saveProfileName(profileId) {
            var name = document.getElementById('editName-' + profileId).value;
            var firm = document.getElementById('editFirm-' + profileId).value;
            var notes = document.getElementById('editNotes-' + profileId).value;
            fetch('/api/af/profiles/' + profileId, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: name, firm_name: firm, notes: notes })
            })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) location.reload();
                else alert('Error: ' + data.error);
            });
        }

        function createProfile() {
            const name = document.getElementById('newProfileName').value;
            const firm = document.getElementById('newFirmName').value;
            const notes = document.getElementById('newProfileNotes').value;

            if (!name || !firm) {
                document.getElementById('createStatus').innerHTML = '<div class="alert" style="background: #f8d7da; color: #721c24;">Name and Firm required</div>';
                return;
            }

            fetch('/api/af/profiles', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name, firm_name: firm, notes })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    location.reload();
                } else {
                    document.getElementById('createStatus').innerHTML = '<div class="alert" style="background: #f8d7da; color: #721c24;">Error: ' + data.error + '</div>';
                }
            });
        }

        function deleteProfile(profileId) {
            if (!confirm('Delete this profile and all its rules?')) return;
            fetch('/api/af/profiles/' + profileId, { method: 'DELETE' })
            .then(r => r.json())
            .then(data => {
                if (data.success) location.reload();
            });
        }

        function addRuleRow(profileId) {
            var table = document.getElementById('rules-' + profileId);
            var newRow = document.createElement('tr');
            newRow.className = 'new-rule';
            newRow.innerHTML = '<td><span class="edit-field" style="display:inline;"><input type="text" placeholder="Auditor line item" /></span></td>' +
                '<td><span class="edit-field" style="display:inline;"><input type="text" placeholder="Auditor category" /></span></td>' +
                '<td><span class="edit-field" style="display:inline;"><select><option value="">-- Select --</option>{{ century_categories_options }}</select></span></td>' +
                '<td><span class="edit-field" style="display:inline;"><input type="number" placeholder="1.0" step="0.01" value="1.0" style="width: 60px;" /></span></td>' +
                '<td><span class="edit-field" style="display:inline;"><input type="text" placeholder="Notes" /></span></td>';
            table.appendChild(newRow);
        }

        function saveRules(profileId) {
            var rows = document.querySelectorAll('#rules-' + profileId + ' tr');
            var rules = [];

            rows.forEach(function(row) {
                // Get inputs from edit-field spans
                var inputs = row.querySelectorAll('.edit-field input, .edit-field select');
                if (inputs.length >= 5 && inputs[0].value.trim()) {
                    rules.push({
                        id: row.dataset.ruleId || null,
                        auditor_line_item: inputs[0].value,
                        auditor_category: inputs[1].value,
                        century_category: inputs[2].value,
                        split_pct: parseFloat(inputs[3].value) || 1.0,
                        notes: inputs[4].value
                    });
                }
            });

            fetch('/api/af/profiles/' + profileId + '/rules', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rules })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    alert('Rules saved');
                    location.reload();
                }
            });
        }
    </script>
</body>
</html>
        """

        # Build profiles list
        profiles_html = []
        for p in profiles:
            rules_rows = []
            for r in p.rules:
                cat_options = "".join([f'<option value="{cat}" {"selected" if r.century_category == cat else ""}>{cat}</option>' for cat in CENTURY_CATEGORIES])
                rules_rows.append(f"""
                    <tr data-rule-id="{r.id}">
                        <td>
                            <span class="read-field">{r.auditor_line_item}</span>
                            <span class="edit-field"><input type="text" value="{r.auditor_line_item}" /></span>
                        </td>
                        <td>
                            <span class="read-field">{r.auditor_category}</span>
                            <span class="edit-field"><input type="text" value="{r.auditor_category}" /></span>
                        </td>
                        <td>
                            <span class="read-field"><span class="category-badge">{r.century_category or '—'}</span></span>
                            <span class="edit-field"><select><option value="">-- Select --</option>{cat_options}</select></span>
                        </td>
                        <td>
                            <span class="read-field">{r.split_pct}</span>
                            <span class="edit-field"><input type="number" value="{r.split_pct}" step="0.01" style="width: 60px;" /></span>
                        </td>
                        <td>
                            <span class="read-field">{r.notes or '—'}</span>
                            <span class="edit-field"><input type="text" value="{r.notes or ''}" /></span>
                        </td>
                    </tr>
                """)

            rule_count = len(p.rules)
            rules_table = f"""
                <table class="rules-table" id="rules-{p.id}">
                    <tr>
                        <th>Auditor Line Item</th>
                        <th>Auditor Category</th>
                        <th>Century Category</th>
                        <th>Split %</th>
                        <th>Notes</th>
                    </tr>
                    {"".join(rules_rows)}
                </table>
                <div class="btn-row edit-field">
                    <button class="btn-small" style="background:var(--gray-100);color:var(--gray-700);" onclick="addRuleRow({p.id})">+ Add Rule</button>
                    <button class="btn-green btn-small" onclick="saveRules({p.id})">Save All Rules</button>
                    <button class="btn-small" style="background:var(--gray-100);color:var(--gray-700);margin-left:auto;" onclick="toggleEdit('profile-{p.id}')">Cancel</button>
                </div>
            """

            profile_card = f"""
                <div class="profile-card" id="profile-{p.id}">
                    <div class="profile-header" onclick="toggleProfile('profile-{p.id}')">
                        <div class="profile-header-left">
                            <svg class="collapse-chevron" viewBox="0 0 20 20" fill="currentColor"><path d="M7.293 4.293a1 1 0 011.414 0L14.414 10l-5.707 5.707a1 1 0 01-1.414-1.414L11.586 10 7.293 5.707a1 1 0 010-1.414z"/></svg>
                            <div>
                                <h3>{p.name}</h3>
                                <div class="profile-meta">Firm: {p.firm_name}{(' | ' + p.notes) if p.notes else ''}</div>
                            </div>
                        </div>
                        <div class="profile-header-right">
                            <span class="rule-count">{rule_count} rule{'s' if rule_count != 1 else ''}</span>
                        </div>
                    </div>
                    <div class="profile-body">
                        <div class="profile-actions">
                            <button class="btn-edit btn-small" onclick="event.stopPropagation(); toggleEditName('profile-{p.id}')">&#9998; Edit Name</button>
                            <button class="btn-edit btn-small" onclick="event.stopPropagation(); toggleEdit('profile-{p.id}')">&#9998; Edit Mapping</button>
                            <button class="btn-danger btn-small" onclick="event.stopPropagation(); deleteProfile({p.id})">Delete</button>
                        </div>
                        <div class="name-edit-row" id="nameEdit-{p.id}" style="display:none; margin-top:12px; padding:14px; background:var(--gray-50); border-radius:8px; border:1px solid var(--gray-200);">
                            <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:10px;">
                                <div><label style="font-size:11px;">Display Name</label><input type="text" id="editName-{p.id}" value="{p.name}" style="width:100%; margin-top:4px;" /></div>
                                <div><label style="font-size:11px;">Firm Name</label><input type="text" id="editFirm-{p.id}" value="{p.firm_name}" style="width:100%; margin-top:4px;" /></div>
                                <div><label style="font-size:11px;">Notes</label><input type="text" id="editNotes-{p.id}" value="{p.notes or ''}" style="width:100%; margin-top:4px;" /></div>
                            </div>
                            <div style="margin-top:10px; display:flex; gap:8px;">
                                <button class="btn-green btn-small" onclick="saveProfileName({p.id})">Save</button>
                                <button class="btn-small" style="background:var(--gray-100);color:var(--gray-700);" onclick="toggleEditName('profile-{p.id}')">Cancel</button>
                            </div>
                        </div>
                        {rules_table}
                    </div>
                </div>
            """
            profiles_html.append(profile_card)

        century_categories_options = "".join([
            f'<option value="{cat}">{cat}</option>'
            for cat in CENTURY_CATEGORIES
        ])

        html = html.replace("{{ profiles_list }}", "\n".join(profiles_html) if profiles_html else "<p>No profiles created yet.</p>")
        html = html.replace("{{ century_categories_options }}", century_categories_options)

        return render_template_string(html)


    @bp.route("/audited-financials/review/<int:upload_id>", methods=["GET"])
    def review_page(upload_id):
        """Review and confirm extraction for an upload."""
        try:
            return _review_page_impl(upload_id)
        except Exception as _rp_err:
            import traceback
            tb = traceback.format_exc()
            print(f"[review_page ERROR] upload_id={upload_id}: {_rp_err}\n{tb}")
            safe_tb = (tb or "").replace("<", "&lt;").replace(">", "&gt;")
            return (
                "<h2>Review page error</h2>"
                f"<p><b>upload_id:</b> {upload_id}</p>"
                f"<p><b>error:</b> {str(_rp_err)}</p>"
                f"<pre style='background:#111;color:#0f0;padding:12px;white-space:pre-wrap;'>{safe_tb}</pre>"
                "<p><a href='/audited-financials'>← Back</a></p>"
            ), 500

    def _review_page_impl(upload_id):
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return "Upload not found", 404

        try:
            raw_extraction = json.loads(upload.raw_extraction) if upload.raw_extraction else {}
            mapped_data = json.loads(upload.mapped_data) if upload.mapped_data else {}
        except:
            raw_extraction = {}
            mapped_data = {}

        # Find unmapped items and build existing rules lookup.
        # Section-aware: {description: {"revenue": cat, "expense": cat}} so the
        # same description can pre-populate different dropdowns per section.
        unmapped = []
        existing_rules = {}
        if upload.profile:
            for rule in upload.profile.rules:
                key = rule.auditor_line_item.lower().strip()
                rule_section = _category_section(rule.century_category)
                if key not in existing_rules:
                    existing_rules[key] = {}
                existing_rules[key][rule_section] = rule.century_category
            if upload.status in ["mapped", "confirmed"]:
                try:
                    _, unmapped = apply_mapping_rules(upload.raw_extraction, upload.profile.id)
                    if not isinstance(unmapped, list):
                        unmapped = []
                except Exception as _amr_err:
                    import traceback
                    print(f"[review_page] apply_mapping_rules failed for upload {upload_id}: {_amr_err}\n{traceback.format_exc()}")
                    unmapped = []

        html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Review - {{ building_name }} - Century Management</title>
    <style>
        /* Force scrollbars always visible (fixes macOS auto-hide) */
        ::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
        ::-webkit-scrollbar-track { background: #f1f5f9; }
        ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
        ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
        ::-webkit-scrollbar-corner { background: #f1f5f9; }
        * { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
        :root { --blue: #5a4a3f; --blue-light: #f5efe7; --green: #057a55; --green-light: #def7ec; --red: #e02424; --gray-50: #f4f1eb; --gray-100: #ede9e1; --gray-200: #e5e0d5; --gray-300: #d5cfc5; --gray-500: #8a7e72; --gray-700: #4a4039; --gray-900: #1a1714; }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--gray-50); color: var(--gray-900); line-height: 1.5; }
        header { background: linear-gradient(135deg, #2c2825 0%, #3d322a 100%); color: white; padding: 30px 20px; }
        header a { color: white; text-decoration: none; font-size: 14px; }
        header a:hover { text-decoration: underline; }
        header h1 { font-size: 24px; font-weight: 700; }
        header p { font-size: 14px; opacity: 0.85; margin-top: 4px; }
        .container { max-width: 1400px; margin: 0 auto; padding: 24px 20px; }
        .columns { display: grid; grid-template-columns: 2fr 1fr; gap: 24px; align-items: start; }
        .column { background: white; border-radius: 12px; padding: 24px; border: 1px solid var(--gray-200); }
        .column:last-child { position: sticky; top: 12px; }
        .column h3 { font-size: 16px; font-weight: 600; color: var(--blue); margin-bottom: 16px; padding-bottom: 10px; border-bottom: 2px solid var(--blue-light); }
        @keyframes flashUpdate { 0% { background: #fef08a; } 100% { background: white; } }
        .flash-update { animation: flashUpdate 0.8s ease-out; }
        button { background: var(--blue); color: white; border: none; padding: 10px 20px; border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.15s; }
        button:hover { background: #1542b8; }
        .btn-green { background: var(--green); }
        .btn-green:hover { background: #046c4e; }
        .unmapped { background: #fde8e8; color: #9b1c1c; padding: 10px; margin: 10px 0; border-radius: 6px; font-size: 13px; }
        .success { background: var(--green-light); color: #065f46; padding: 10px; margin: 10px 0; border-radius: 6px; font-size: 13px; }
        table { width: 100%; border-collapse: collapse; }
        table th { background: var(--gray-100); padding: 8px 10px; text-align: left; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--gray-500); border-bottom: 1px solid var(--gray-200); }
        table td { padding: 8px 10px; border-bottom: 1px solid var(--gray-200); font-size: 13px; }
        .amt-cell { background: #fbfaf4; border: 1px solid var(--gray-300); padding: 4px 6px; text-align: right; width: 85px; font-size: 12px; font-family: inherit; border-radius: 3px; }
        .amt-cell:focus { border-color: var(--blue); box-shadow: 0 0 0 2px var(--blue-light); outline: none; }
        .amt-cell-readonly { background: var(--gray-50); border: 1px solid var(--gray-200); color: var(--gray-500); cursor: default; }
        .confirm-section { background: white; border-radius: 12px; padding: 24px; border: 1px solid var(--gray-200); margin-top: 24px; }
        .confirm-section h3 { font-size: 16px; font-weight: 600; color: var(--gray-900); margin-bottom: 8px; }
        .confirm-section p { font-size: 13px; color: var(--gray-500); margin-bottom: 16px; }
        .back-link { color: var(--blue); text-decoration: none; font-size: 14px; font-weight: 600; }
        .back-link:hover { text-decoration: underline; }
    </style>
</head>
<body>
<header>
    <a href="/">← Home</a>
    <h1>Review Extraction</h1>
    <p>{{ building_name }} ({{ entity_code }}) — Fiscal Year {{ fiscal_year }}</p>
</header>
<div class="container">
    <div style="margin-bottom:16px;"><a href="/audited-financials" class="back-link">← Back to Uploads</a></div>

    <div class="columns">
        <div class="column">
            <h3>Extracted Data — Map Each Item</h3>
            <div id="rawData"></div>
            <div style="margin-top:16px; display:flex; gap:10px;">
                <button onclick="saveAllRules()" class="btn-green" style="flex:1;">Save All Mappings</button>
                <button onclick="remapUpload()" style="flex:1;">Re-Apply &amp; Refresh</button>
            </div>
        </div>

        <div class="column">
            <h3>Century Budget Categories</h3>
            <div id="mappedData"></div>
            <div id="reconciliation" style="margin-top:16px;"></div>
        </div>
    </div>

    <div class="confirm-section">
        <h3>Confirm Extraction</h3>
        <p>Accept each line item above, then confirm to save as official actuals for this building/year.</p>
        <button id="confirmBtn" class="btn-green" disabled style="opacity:0.4; cursor:not-allowed;" onclick="confirmExtraction({{ upload_id }}, false)">Confirm & Save</button>
        <button id="overrideBtn" disabled style="opacity:0.4; cursor:not-allowed; margin-left:8px; padding:10px 18px; background:#b45309; color:#fff; border:none; border-radius:4px; font-weight:600;" onclick="confirmExtraction({{ upload_id }}, true)" title="Save anyway when totals don't reconcile — use only if the mismatch is intentional">Override & Save (Skip Validation)</button>
        <div id="confirmStatus"></div>
    </div>
</div>

    <script>
        const rawExtraction = {{ raw_json }};
        const mappedData = {{ mapped_json }};
        const unmappedItems = {{ unmapped_json }};
        const centuryCategories = {{ century_categories_json }};
        const CENTURY_TO_SUMMARY = {{ century_to_summary_json }};
        const existingRules = {{ existing_rules_json }};
        const buildingLabels = {{ building_labels_json }};
        const buildingLabelSet = new Set(buildingLabels);
        const buildingLabelSections = {{ building_label_sections_json }};
        const centuryCatSet = new Set(centuryCategories);
        const profileId = {{ profile_id }};
        let itemIndex = 0;

        function formatAmount(n) {
            if (n === null || n === undefined) return '—';
            return n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
        }

        function parseDollar(v) {
            if (!v || v === '—') return 0;
            return parseFloat(String(v).replace(/[,$\s]/g, '')) || 0;
        }

        // Editable cell for current year (2025)
        function makeAmtInput(amount, rowId) {
            const raw = Math.round(amount || 0);
            return '<input class="amt-cell" type="text" value="' + formatAmount(raw) + '" data-raw="' + raw + '" data-row="' + rowId + '" onfocus="this.value=this.dataset.raw" onblur="amtCellBlur(this)">';
        }

        // Read-only cell for prior year (2024)
        function makeAmtReadonly(amount) {
            return '<input class="amt-cell amt-cell-readonly" type="text" value="' + formatAmount(Math.round(amount || 0)) + '" readonly tabindex="-1">';
        }

        function amtCellBlur(el) {
            const raw = parseDollar(el.value);
            el.dataset.raw = Math.round(raw);
            el.value = formatAmount(raw);
            // Update the linked select's data-amount so reconciliation picks it up
            const rowId = el.dataset.row;
            const sel = document.getElementById(rowId);
            if (sel) sel.dataset.amount = Math.round(raw);
            renderReconciliation();
            recalcLeftTotals();
            updateAcceptState();
        }

        function recalcLeftTotals() {
            // Revenue total = sum of all revenue item editable cells
            let revTotal = 0;
            document.querySelectorAll('tr[data-group="revenue"] .amt-cell:not(.amt-cell-readonly)').forEach(inp => {
                revTotal += parseInt(inp.dataset.raw) || 0;
            });
            const revEl = document.getElementById('total-revenue');
            if (revEl) revEl.textContent = formatAmount(revTotal);

            // Expense subtotals per category
            let expGrandTotal = 0;
            for (let ci = 0; ci < 50; ci++) {
                const rows = document.querySelectorAll('tr[data-group="exp_' + ci + '"] .amt-cell:not(.amt-cell-readonly)');
                if (rows.length === 0 && ci > 0) break;
                let catTotal = 0;
                rows.forEach(inp => { catTotal += parseInt(inp.dataset.raw) || 0; });
                const subEl = document.getElementById('subtotal-exp-' + ci);
                if (subEl) subEl.textContent = formatAmount(catTotal);
                expGrandTotal += catTotal;
            }
            const expEl = document.getElementById('total-expenses');
            if (expEl) expEl.textContent = formatAmount(expGrandTotal);
        }

        // Decorate a Century category with an (inc.)/(exp.) suffix so users
        // can visually tell income vs expense categories in the datalist.
        // The suffix is strictly a display/UX affordance — it's stripped
        // before comparing to CENTURY_TO_SUMMARY, saving rules, etc.
        const INCOME_ROWS = new Set(["Total Operating Income", "Non-Operating Income"]);
        function displayCat(cat) {
            if (!cat) return '';
            const summary = CENTURY_TO_SUMMARY[cat] || buildingLabelSections[cat] || '';
            const tag = INCOME_ROWS.has(summary) ? ' (inc.)' : ' (exp.)';
            return cat + tag;
        }
        function stripCatSuffix(v) {
            if (!v) return '';
            return v.replace(/\s*\((?:inc|exp)\.\)\s*$/, '').trim();
        }

        function renderSourceLines(sourceLines, years, section) {
            if (!sourceLines || sourceLines.length === 0) return '';
            // Single direct match — show inline green tag
            if (sourceLines.length === 1) {
                const sl = sourceLines[0];
                const auditorDesc = sl.auditor_desc || sl.description || '';
                return '<div style="font-size:10px; color:#065f46; margin-top:2px;">Auditor: "' + auditorDesc + '"</div>';
            }
            // Multiple source lines — show expandable list with Split button
            const sourcesJson = JSON.stringify(sourceLines).replace(/'/g, '&#39;').replace(/"/g, '&quot;');
            let html = '<details style="margin-top:3px;">';
            html += '<summary style="font-size:10px; color:#856404; cursor:pointer;">⚠ ' + sourceLines.length + ' auditor items consolidated (click to expand)</summary>';
            html += '<div style="font-size:11px; background:#fff8e1; padding:4px 8px; border-radius:3px; margin-top:2px;">';
            for (let sl of sourceLines) {
                const desc = sl.auditor_desc || sl.description || '?';
                const amts = (sl.amounts || []).map(a => formatAmount(a));
                html += '<div style="display:flex; justify-content:space-between; padding:1px 0; border-bottom:1px solid #f0e6c0;">';
                html += '<span>' + desc + '</span>';
                html += '<span style="white-space:nowrap; margin-left:8px;">' + amts.join(' / ') + '</span>';
                html += '</div>';
            }
            html += '<button onclick="splitRow(this)" data-sources="' + sourcesJson + '" data-section="' + (section || 'expense') + '" style="margin-top:4px; font-size:10px; padding:2px 8px; background:#f59e0b; color:#fff; border:none; border-radius:3px; cursor:pointer;">Split into individual rows</button>';
            html += '</div></details>';
            return html;
        }

        // Pre-compute dropdown option groups (building-specific first, then other Century cats)
        const bldgIncome = buildingLabels.filter(c => {
            const s = (buildingLabelSections[c] || '').toLowerCase();
            return s.includes('income') && !s.includes('non-operating');
        });
        const bldgExpense = buildingLabels.filter(c => {
            const s = (buildingLabelSections[c] || '').toLowerCase();
            return s.includes('expense') || s === '';
        });
        const bldgNonOp = buildingLabels.filter(c => {
            const s = (buildingLabelSections[c] || '').toLowerCase();
            return s.includes('non-operating');
        });
        const bldgSet = new Set(buildingLabels);
        const otherCentury = centuryCategories.filter(c => !bldgSet.has(c)).sort();

        function buildSelectOptions(currentMapping) {
            let opts = '<option value="">— Select category —</option>';
            // Group 1: This building's income labels
            if (bldgIncome.length > 0) {
                opts += '<optgroup label="Income (this building)">';
                for (let c of bldgIncome) {
                    opts += '<option value="' + c + '"' + (c === currentMapping ? ' selected' : '') + '>' + c + '</option>';
                }
                opts += '</optgroup>';
            }
            // Group 2: This building's expense labels
            if (bldgExpense.length > 0) {
                opts += '<optgroup label="Expenses (this building)">';
                for (let c of bldgExpense) {
                    opts += '<option value="' + c + '"' + (c === currentMapping ? ' selected' : '') + '>' + c + '</option>';
                }
                opts += '</optgroup>';
            }
            // Group 3: This building's non-operating labels
            if (bldgNonOp.length > 0) {
                opts += '<optgroup label="Non-Operating (this building)">';
                for (let c of bldgNonOp) {
                    opts += '<option value="' + c + '"' + (c === currentMapping ? ' selected' : '') + '>' + c + '</option>';
                }
                opts += '</optgroup>';
            }
            // Group 4: Other Century categories not in this building's budget
            if (otherCentury.length > 0) {
                opts += '<optgroup label="── Other Century Categories ──">';
                for (let c of otherCentury) {
                    opts += '<option value="' + c + '"' + (c === currentMapping ? ' selected' : '') + '>' + c + '</option>';
                }
                opts += '</optgroup>';
            }
            return opts;
        }

        function makeDropdown(description, amount, section, amount1) {
            const id = 'map_' + itemIndex++;
            const normalized = description.toLowerCase().trim();
            const rulesForDesc = existingRules[normalized] || {};
            let currentMapping = (typeof rulesForDesc === 'string')
                ? rulesForDesc
                : (rulesForDesc[section] || '');

            if (!currentMapping && centuryCatSet.has(description)) {
                currentMapping = description;
            }
            if (!currentMapping && buildingLabelSet.has(description)) {
                currentMapping = description;
            }

            // All start yellow (recommended) — user must Accept to turn green
            const bgStyle = currentMapping ? 'background:#fff3cd;' : '';

            let html = '<div data-section="' + (section || 'expense') + '" style="display:flex; align-items:center; gap:4px;">';
            html += '<select id="' + id + '" data-desc="' + description.replace(/"/g, '&quot;') + '" data-amount="' + (amount || 0) + '" data-amount1="' + (amount1 || 0) + '" data-orig-cat="' + (currentMapping || '').replace(/"/g, '&quot;') + '" data-accepted="false" onchange="onDropdownChange(this); renderReconciliation(); updateAcceptState();" style="flex:1; padding:4px; font-size:12px; border:1px solid #ccc; border-radius:3px; cursor:pointer; ' + bgStyle + '">';
            html += buildSelectOptions(currentMapping);
            html += '</select>';
            html += '<button onclick="acceptRow(this)" class="accept-btn" style="padding:3px 8px; font-size:11px; background:#f59e0b; color:#fff; border:none; border-radius:3px; cursor:pointer; white-space:nowrap;" title="Confirm this mapping">✓ Accept</button>';
            html += '</div>';
            return html;
        }

        // Version for split rows — inherits parent mapping, starts yellow
        function makeDropdownWithDefault(description, amount, section, defaultMapping, amount1) {
            const id = 'map_' + itemIndex++;
            const bgStyle = defaultMapping ? 'background:#fff3cd;' : '';
            let html = '<div data-section="' + (section || 'expense') + '" style="display:flex; align-items:center; gap:4px;">';
            html += '<select id="' + id + '" data-desc="' + description.replace(/"/g, '&quot;') + '" data-amount="' + (amount || 0) + '" data-amount1="' + (amount1 || 0) + '" data-orig-cat="' + (defaultMapping || '').replace(/"/g, '&quot;') + '" data-accepted="false" onchange="onDropdownChange(this); renderReconciliation(); updateAcceptState();" style="flex:1; padding:4px; font-size:12px; border:1px solid #ccc; border-radius:3px; cursor:pointer; ' + bgStyle + '">';
            html += buildSelectOptions(defaultMapping);
            html += '</select>';
            html += '<button onclick="acceptRow(this)" class="accept-btn" style="padding:3px 8px; font-size:11px; background:#f59e0b; color:#fff; border:none; border-radius:3px; cursor:pointer; white-space:nowrap;" title="Confirm this mapping">✓ Accept</button>';
            if (defaultMapping) {
                html += '<div style="font-size:10px; color:#856404; margin-top:1px;">Inherited: ' + defaultMapping + '</div>';
            }
            html += '</div>';
            return html;
        }

        function acceptRow(btn) {
            const wrapper = btn.closest('[data-section]');
            const sel = wrapper.querySelector('select[id^="map_"]');
            if (!sel.value) { alert('Select a category first'); return; }
            sel.dataset.accepted = 'true';
            sel.style.background = '#d4edda';  // green = confirmed
            btn.style.background = '#16a34a';
            btn.textContent = '✓';
            btn.disabled = true;
            btn.title = 'Accepted';
            updateAcceptState();
            renderReconciliation();
        }

        function onDropdownChange(el) {
            // If user changes after accepting, revert to yellow (needs re-accept)
            el.dataset.accepted = 'false';
            el.style.background = el.value ? '#fff3cd' : '';
            const btn = el.parentElement.querySelector('.accept-btn');
            if (btn) {
                btn.style.background = '#f59e0b';
                btn.textContent = '✓ Accept';
                btn.disabled = false;
            }
        }

        function updateAcceptState() {
            // Check if all rows are accepted — controls Confirm & Save button
            const allSelects = document.querySelectorAll('select[id^="map_"]');
            let allAccepted = true;
            let acceptedCount = 0;
            allSelects.forEach(s => {
                if (s.dataset.accepted === 'true') acceptedCount++;
                else allAccepted = false;
            });
            const total = allSelects.length;
            // Update accept counter
            const counter = document.getElementById('acceptCounter');
            if (counter) {
                counter.textContent = acceptedCount + ' / ' + total + ' accepted';
                counter.style.color = allAccepted ? '#16a34a' : '#d97706';
            }
            // Enable/disable confirm button
            const confirmBtn = document.getElementById('confirmBtn');
            if (confirmBtn) {
                confirmBtn.disabled = !allAccepted;
                confirmBtn.style.opacity = allAccepted ? '1' : '0.4';
                confirmBtn.style.cursor = allAccepted ? 'pointer' : 'not-allowed';
            }
            // Override button is gated the same way (all rows must be accepted)
            const overrideBtn = document.getElementById('overrideBtn');
            if (overrideBtn) {
                overrideBtn.disabled = !allAccepted;
                overrideBtn.style.opacity = allAccepted ? '1' : '0.4';
                overrideBtn.style.cursor = allAccepted ? 'pointer' : 'not-allowed';
            }
        }

        // Split a consolidated row into individual source_line rows
        // Each sub-row inherits the parent's current mapping
        function splitRow(btn) {
            const row = btn.closest('tr');
            const sourceData = JSON.parse(btn.dataset.sources);
            const section = btn.dataset.section;
            const years = rawExtraction.fiscal_years || [];
            // Get the parent row's current dropdown selection
            const parentSelect = row.querySelector('select[id^="map_"]');
            const parentMapping = parentSelect ? parentSelect.value : '';
            const parentGroup = row.dataset.group || '';
            let newRows = '';
            for (let sl of sourceData) {
                const desc = sl.auditor_desc || sl.description || '?';
                const amounts = sl.amounts || [];
                const amount0 = amounts[0] || 0;
                const mapId = 'map_' + itemIndex;  // peek before makeDropdownWithDefault increments
                newRows += '<tr data-group="' + parentGroup + '" style="border-bottom:1px solid #eee; background:#fffbeb;">';
                newRows += '<td style="padding:6px 6px 6px 30px; font-style:italic;">' + desc + '</td>';
                for (let yi = 0; yi < amounts.length; yi++) {
                    newRows += '<td style="text-align:right; padding:4px;">';
                    if (yi === 0) { newRows += makeAmtInput(amounts[yi], mapId); }
                    else { newRows += makeAmtReadonly(amounts[yi]); }
                    newRows += '</td>';
                }
                for (let i = amounts.length; i < years.length; i++) { newRows += '<td style="text-align:right; padding:4px;">' + makeAmtReadonly(0) + '</td>'; }
                const amount1 = amounts[1] || 0;
                newRows += '<td style="padding:4px;">' + makeDropdownWithDefault(desc, amount0, section, parentMapping, amount1) + '</td></tr>';
            }
            row.insertAdjacentHTML('afterend', newRows);
            row.remove();
            renderReconciliation();
            recalcLeftTotals();
        }

        function renderRawData() {
            const container = document.getElementById('rawData');
            const years = rawExtraction.fiscal_years || [];
            let html = '';

            // Categories are now rendered as <select> dropdowns in makeDropdown()

            if (years.length > 0) {
                html += '<div style="background:#e8f0fe; padding:8px 12px; border-radius:4px; margin-bottom:12px; font-weight:bold;">Fiscal Years: ' + years.join(', ') + '</div>';
            }

            if (rawExtraction.revenue && rawExtraction.revenue.items) {
                html += '<h5 style="margin:15px 0 5px;">Revenue</h5>';
                html += '<table style="width:100%; font-size:13px; border-collapse:collapse;"><tr><th style="text-align:left; padding:6px;">Line Item</th>';
                for (let yi = 0; yi < years.length; yi++) { html += '<th style="text-align:right; padding:6px; width:100px;">' + years[yi] + (yi === 0 ? ' ✎' : '') + '</th>'; }
                html += '<th style="text-align:left; padding:6px; width:180px;">Map To</th></tr>';
                for (let item of rawExtraction.revenue.items) {
                    const amount0 = item.amounts && item.amounts[0] ? item.amounts[0] : 0;
                    const mapId = 'map_' + itemIndex;  // peek at the next map id
                    html += '<tr data-group="revenue" style="border-bottom:1px solid #eee;"><td style="padding:6px;">' + item.description;
                    html += renderSourceLines(item.source_lines, years, 'revenue');
                    html += '</td>';
                    for (let yi = 0; yi < item.amounts.length; yi++) {
                        html += '<td style="text-align:right; padding:4px;">';
                        if (yi === 0) { html += makeAmtInput(item.amounts[yi], mapId); }
                        else { html += makeAmtReadonly(item.amounts[yi]); }
                        html += '</td>';
                    }
                    html += '<td style="padding:4px;">' + makeDropdown(item.description, amount0, 'revenue', (item.amounts && item.amounts[1]) || 0) + '</td></tr>';
                }
                if (rawExtraction.revenue.total) {
                    html += '<tr style="font-weight:bold; border-top:2px solid #333;"><td style="padding:6px;">Total Revenue</td>';
                    for (let yi = 0; yi < rawExtraction.revenue.total.length; yi++) {
                        if (yi === 0) { html += '<td style="text-align:right; padding:6px;"><span id="total-revenue">' + formatAmount(rawExtraction.revenue.total[yi]) + '</span></td>'; }
                        else { html += '<td style="text-align:right; padding:6px;">' + formatAmount(rawExtraction.revenue.total[yi]) + '</td>'; }
                    }
                    html += '<td></td></tr>';
                }
                html += '</table>';
            }

            if (rawExtraction.expenses && rawExtraction.expenses.categories) {
                html += '<h5 style="margin:15px 0 5px;">Expenses</h5>';
                html += '<table style="width:100%; font-size:13px; border-collapse:collapse;"><tr><th style="text-align:left; padding:6px;">Line Item</th>';
                for (let yi = 0; yi < years.length; yi++) { html += '<th style="text-align:right; padding:6px; width:100px;">' + years[yi] + (yi === 0 ? ' ✎' : '') + '</th>'; }
                html += '<th style="text-align:left; padding:6px; width:180px;">Map To</th></tr>';

                let expenseCategories = rawExtraction.expenses.categories;
                if (!Array.isArray(expenseCategories)) {
                    let flatItems = [];
                    for (let key of Object.keys(expenseCategories).sort((a,b) => parseInt(a) - parseInt(b))) {
                        const arr = expenseCategories[key];
                        if (Array.isArray(arr)) { for (let it of arr) flatItems.push(it); }
                        else if (arr && arr.description) flatItems.push(arr);
                    }
                    expenseCategories = [{ name: 'Expenses', items: flatItems }];
                }

                let catIdx = 0;
                for (let cat of expenseCategories) {
                    html += '<tr><td colspan="' + (years.length + 2) + '" style="font-weight:bold; background:#f0f0f0; padding:8px 6px;">' + cat.name + '</td></tr>';
                    for (let item of (cat.items || [])) {
                        const amount0 = item.amounts && item.amounts[0] ? item.amounts[0] : 0;
                        const mapId = 'map_' + itemIndex;
                        html += '<tr data-group="exp_' + catIdx + '" style="border-bottom:1px solid #eee;"><td style="padding:6px 6px 6px 20px;">' + item.description;
                        html += renderSourceLines(item.source_lines, years, 'expense');
                        html += '</td>';
                        for (let yi = 0; yi < item.amounts.length; yi++) {
                            html += '<td style="text-align:right; padding:4px;">';
                            if (yi === 0) { html += makeAmtInput(item.amounts[yi], mapId); }
                            else { html += makeAmtReadonly(item.amounts[yi]); }
                            html += '</td>';
                        }
                        html += '<td style="padding:4px;">' + makeDropdown(item.description, amount0, 'expense', (item.amounts && item.amounts[1]) || 0) + '</td></tr>';
                    }
                    if (cat.total) {
                        html += '<tr style="font-weight:bold; border-bottom:2px solid #ddd;"><td style="padding:6px 6px 6px 20px;">Subtotal</td>';
                        for (let yi = 0; yi < cat.total.length; yi++) {
                            if (yi === 0) { html += '<td style="text-align:right; padding:6px;"><span id="subtotal-exp-' + catIdx + '">' + formatAmount(cat.total[yi]) + '</span></td>'; }
                            else { html += '<td style="text-align:right; padding:6px;">' + formatAmount(cat.total[yi]) + '</td>'; }
                        }
                        html += '<td></td></tr>';
                    }
                    catIdx++;
                }
                if (rawExtraction.expenses.total_expenses) {
                    html += '<tr style="font-weight:bold; border-top:2px solid #333;"><td style="padding:6px;">Total Expenses</td>';
                    for (let yi = 0; yi < rawExtraction.expenses.total_expenses.length; yi++) {
                        if (yi === 0) { html += '<td style="text-align:right; padding:6px;"><span id="total-expenses">' + formatAmount(rawExtraction.expenses.total_expenses[yi]) + '</span></td>'; }
                        else { html += '<td style="text-align:right; padding:6px;">' + formatAmount(rawExtraction.expenses.total_expenses[yi]) + '</td>'; }
                    }
                    html += '<td></td></tr>';
                }
                html += '</table>';
            }

            container.innerHTML = html;
        }

        function renderMappedData() {
            const container = document.getElementById('mappedData');
            const years = rawExtraction.fiscal_years || [];
            const currentYear = years[0] || 'Current';

            // Build category data with original vs current tracking
            const catData = {};  // { cat: { base: N, adjIn: [{desc,amount,from}], adjOut: [{desc,amount,to}] } }
            const allSelects = document.querySelectorAll('select[id^="map_"]');

            allSelects.forEach(s => {
                const currentCat = stripCatSuffix(s.value) || '';
                const origCat = stripCatSuffix(s.dataset.origCat || '') || '';
                const amount = parseFloat(s.dataset.amount) || 0;
                const desc = s.dataset.desc || '?';

                // Ensure both categories exist in our data
                if (currentCat && !catData[currentCat]) catData[currentCat] = { base: 0, baseItems: [], adjIn: [], adjOut: [] };
                if (origCat && !catData[origCat]) catData[origCat] = { base: 0, baseItems: [], adjIn: [], adjOut: [] };

                if (!currentCat && !origCat) return;

                if (currentCat === origCat) {
                    // Item stayed in its original category
                    catData[currentCat].base += amount;
                    catData[currentCat].baseItems.push({ desc: desc, amount: amount });
                } else {
                    // Item moved
                    if (currentCat) {
                        catData[currentCat].adjIn.push({ desc: desc, amount: amount, from: origCat || 'New' });
                    }
                    if (origCat) {
                        catData[origCat].adjOut.push({ desc: desc, amount: amount, to: currentCat || 'Unmapped' });
                    }
                }
            });

            let html = '<table style="font-size:12px; width:100%; border-collapse:collapse;">';
            html += '<tr><th style="text-align:left; padding:4px 6px; border-bottom:2px solid #333;">Category</th>';
            html += '<th style="text-align:right; padding:4px 6px; border-bottom:2px solid #333;">' + currentYear + '</th></tr>';

            const sortedCats = Object.keys(catData).sort();
            if (sortedCats.length === 0) {
                html += '<tr><td colspan="2" style="text-align:center; color:#999; padding:20px;">Map items on the left to see totals here</td></tr>';
            } else {
                let grandTotal = 0;
                for (let cat of sortedCats) {
                    const cd = catData[cat];
                    const adjInTotal = cd.adjIn.reduce((s, a) => s + a.amount, 0);
                    const adjOutTotal = cd.adjOut.reduce((s, a) => s + a.amount, 0);
                    const originalBase = cd.base + adjOutTotal;
                    const currentTotal = cd.base + adjInTotal;
                    grandTotal += currentTotal;

                    // Category header with current total
                    html += '<tr style="border-top:1px solid #ddd;">';
                    html += '<td style="padding:5px 6px; font-weight:600;">' + cat + '</td>';
                    html += '<td style="text-align:right; padding:5px 6px; font-weight:600;">' + formatAmount(currentTotal) + '</td></tr>';

                    // Formula row if adjustments exist
                    if (cd.adjIn.length > 0 || cd.adjOut.length > 0) {
                        let formula = '= ' + formatAmount(originalBase);
                        for (let a of cd.adjIn) { formula += ' <span style="color:#16a34a; font-weight:600;">+ ' + formatAmount(a.amount) + '</span>'; }
                        for (let a of cd.adjOut) { formula += ' <span style="color:#dc2626; font-weight:600;">− ' + formatAmount(a.amount) + '</span>'; }
                        html += '<tr><td colspan="2" style="padding:2px 6px 2px 14px; font-size:11px; color:#555; font-family:monospace;">' + formula + '</td></tr>';
                    }

                    // Base items (stayed in this category)
                    for (let li of cd.baseItems) {
                        html += '<tr style="color:#666;">';
                        html += '<td style="padding:1px 6px 1px 18px; font-size:11px;">' + li.desc + '</td>';
                        html += '<td style="text-align:right; padding:1px 6px; font-size:11px;">' + formatAmount(li.amount) + '</td></tr>';
                    }
                    // Items moved IN (green)
                    for (let a of cd.adjIn) {
                        html += '<tr style="color:#16a34a;">';
                        html += '<td style="padding:1px 6px 1px 18px; font-size:11px;">+ ' + a.desc + ' <span style="font-size:10px; color:#888;">(from ' + a.from + ')</span></td>';
                        html += '<td style="text-align:right; padding:1px 6px; font-size:11px;">+' + formatAmount(a.amount) + '</td></tr>';
                    }
                    // Items moved OUT (red)
                    for (let a of cd.adjOut) {
                        html += '<tr style="color:#dc2626;">';
                        html += '<td style="padding:1px 6px 1px 18px; font-size:11px;">− ' + a.desc + ' <span style="font-size:10px; color:#888;">(→ ' + a.to + ')</span></td>';
                        html += '<td style="text-align:right; padding:1px 6px; font-size:11px;">−' + formatAmount(a.amount) + '</td></tr>';
                    }
                }
                html += '<tr style="border-top:2px solid #333;">';
                html += '<td style="padding:5px 6px; font-weight:700;">Grand Total</td>';
                html += '<td style="text-align:right; padding:5px 6px; font-weight:700;">' + formatAmount(grandTotal) + '</td></tr>';
            }

            html += '</table>';
            container.innerHTML = html;
            container.classList.remove('flash-update');
            void container.offsetWidth;
            container.classList.add('flash-update');
        }

        function renderReconciliation() {
            // Also refresh the mapped data table so it stays in sync
            renderMappedData();
            const container = document.getElementById('reconciliation');
            const centuryToSummary = CENTURY_TO_SUMMARY;
            const incomeSummaryRows = new Set(["Total Operating Income", "Non-Operating Income"]);

            // Compute mapped totals from current dropdown selections
            let mappedRevenue = 0;
            let mappedExpense = 0;
            let unmappedCount = 0;
            let unmappedRevenue = 0;
            let unmappedExpense = 0;

            const allSelects = document.querySelectorAll('select[id^="map_"]');
            const mismatches = [];
            allSelects.forEach(s => {
                // Clear any prior mismatch styling (idempotent)
                s.style.borderLeft = '';
                const amount = parseFloat(s.dataset.amount) || 0;
                const sectionEl = s.closest('[data-section]');
                const sectionType = sectionEl ? sectionEl.dataset.section : 'expense';
                if (!s.value) {
                    unmappedCount++;
                    if (sectionType === 'revenue') unmappedRevenue += amount;
                    else unmappedExpense += amount;
                } else {
                    const bareCat = stripCatSuffix(s.value);
                    const summaryRow = centuryToSummary[bareCat] || buildingLabelSections[bareCat] || '';
                    const isRevenueCat = incomeSummaryRows.has(summaryRow);
                    const isRevenueSection = sectionType === 'revenue';
                    if (isRevenueCat !== isRevenueSection) {
                        // Section/category type mismatch — flag it
                        s.style.borderLeft = '4px solid #dc2626';
                        mismatches.push({
                            desc: s.dataset.desc,
                            amount: amount,
                            section: sectionType,
                            cat: bareCat,
                            catType: isRevenueCat ? 'revenue' : 'expense'
                        });
                    }
                    if (isRevenueCat) mappedRevenue += amount;
                    else mappedExpense += amount;
                }
            });

            // Get extracted totals from Claude output
            const extractedRevenue = (rawExtraction.revenue && rawExtraction.revenue.total) ? rawExtraction.revenue.total[0] : 0;
            const extractedExpense = (rawExtraction.expenses && rawExtraction.expenses.total_expenses) ? rawExtraction.expenses.total_expenses[0] : 0;

            const revenueDelta = Math.abs(mappedRevenue - extractedRevenue);
            const expenseDelta = Math.abs(mappedExpense - extractedExpense);
            const tolerance = 1; // $1 rounding tolerance
            const revenueOk = revenueDelta <= tolerance;
            const expenseOk = expenseDelta <= tolerance;
            const allTied = revenueOk && expenseOk && unmappedCount === 0;

            let html = '<div style="background:var(--gray-100); padding:16px; border-radius:8px; font-size:13px; margin-top:12px;">';
            html += '<strong style="font-size:14px;">Reconciliation</strong><br/><br/>';

            // Revenue reconciliation
            html += '<div style="display:flex; justify-content:space-between; margin-bottom:6px;">';
            html += '<span>Extracted Revenue:</span><span style="font-weight:600;">' + formatAmount(extractedRevenue) + '</span></div>';
            html += '<div style="display:flex; justify-content:space-between; margin-bottom:6px;">';
            html += '<span>Mapped Revenue:</span><span style="font-weight:600;">' + formatAmount(mappedRevenue) + '</span></div>';
            html += '<div style="display:flex; justify-content:space-between; margin-bottom:12px; color:' + (revenueOk ? 'var(--green)' : 'var(--red)') + '; font-weight:600;">';
            html += '<span>Delta:</span><span>' + (revenueOk ? '✓ Tied' : formatAmount(revenueDelta) + ' off') + '</span></div>';

            // Expense reconciliation
            html += '<div style="display:flex; justify-content:space-between; margin-bottom:6px;">';
            html += '<span>Extracted Expenses:</span><span style="font-weight:600;">' + formatAmount(extractedExpense) + '</span></div>';
            html += '<div style="display:flex; justify-content:space-between; margin-bottom:6px;">';
            html += '<span>Mapped Expenses:</span><span style="font-weight:600;">' + formatAmount(mappedExpense) + '</span></div>';
            html += '<div style="display:flex; justify-content:space-between; margin-bottom:12px; color:' + (expenseOk ? 'var(--green)' : 'var(--red)') + '; font-weight:600;">';
            html += '<span>Delta:</span><span>' + (expenseOk ? '✓ Tied' : formatAmount(expenseDelta) + ' off') + '</span></div>';

            // Unmapped
            if (unmappedCount > 0) {
                html += '<div style="color:var(--red); font-weight:600; margin-bottom:8px;">⚠ ' + unmappedCount + ' items still unmapped</div>';
            }

            // Section/category mismatches
            if (mismatches.length > 0) {
                html += '<div style="background:#fef2f2; border:1px solid #fecaca; border-radius:6px; padding:10px 12px; margin-bottom:8px;">';
                html += '<div style="color:#dc2626; font-weight:700; margin-bottom:6px;">⚠ ' + mismatches.length + ' section/category mismatch' + (mismatches.length > 1 ? 'es' : '') + '</div>';
                html += '<div style="font-size:11px; color:#991b1b; margin-bottom:8px;">Items below are in the <b>revenue</b> section but mapped to an <b>expense</b> category (or vice versa). Remap or leave blank.</div>';
                mismatches.forEach(m => {
                    html += '<div style="font-size:11px; padding:3px 0; border-top:1px dotted #fecaca;">';
                    html += '<b>' + m.desc + '</b> (' + formatAmount(m.amount) + ') — in <i>' + m.section + '</i> section → mapped to <i>' + m.cat + '</i> (' + m.catType + ' category)';
                    html += '</div>';
                });
                html += '</div>';
            }

            // Accept progress counter
            html += '<div id="acceptCounter" style="text-align:center; font-weight:600; margin-bottom:8px; color:#d97706;"></div>';

            // Status
            if (allTied) {
                html += '<div style="background:var(--green-light); color:var(--green); padding:8px 12px; border-radius:6px; font-weight:600; text-align:center;">✓ All totals tied</div>';
            } else {
                html += '<div style="background:var(--red-light, #fde8e8); color:var(--red); padding:8px 12px; border-radius:6px; font-weight:600; text-align:center;">Totals must tie before confirming</div>';
            }
            html += '</div>';
            container.innerHTML = html;

            // Update accept state (controls confirm button)
            updateAcceptState();
        }

        function saveAllRules() {
            if (!profileId) { alert('No auditor profile assigned'); return; }

            const selects = document.querySelectorAll('select[id^="map_"]');
            const rules = [];
            selects.forEach(s => {
                if (s.value) {
                    rules.push({
                        auditor_line_item: s.dataset.desc,
                        century_category: stripCatSuffix(s.value),
                        split_pct: 1.0
                    });
                }
            });

            if (rules.length === 0) { alert('No mappings to save'); return; }

            // Save rules one by one (non-destructive add)
            let saved = 0;
            let errors = 0;
            for (let rule of rules) {
                fetch('/api/af/profiles/' + profileId + '/rules', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(rule)
                })
                .then(r => r.json())
                .then(data => {
                    if (data.success) saved++;
                    else errors++;
                    if (saved + errors === rules.length) {
                        alert(saved + ' rules saved. Click "Re-Apply & Refresh" to see updated mappings.');
                    }
                });
            }
        }

        function remapUpload() {
            fetch('/api/af/map/{{ upload_id }}', { method: 'POST' })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    location.reload();
                } else {
                    alert('Mapping error: ' + data.error);
                }
            });
        }

        function confirmExtraction(uploadId, force) {
            // Build mapped_data from the DOM dropdowns BEFORE confirming.
            // Historical bug: this function used to POST straight to /confirm
            // without saving the user's dropdown selections, leaving
            // mapped_data empty in the DB and breaking col2 on the summary.
            const mapped = {};
            const selects = document.querySelectorAll('select[id^="map_"]');
            selects.forEach(s => {
                const cat = stripCatSuffix(s.value || '');
                if (!cat) return;
                const a0 = parseFloat(s.dataset.amount) || 0;
                const a1 = parseFloat(s.dataset.amount1) || 0;
                if (!mapped[cat]) {
                    mapped[cat] = { total: 0, year_totals: [0, 0], years: [] };
                }
                mapped[cat].total += a0;
                mapped[cat].year_totals[0] += a0;
                mapped[cat].year_totals[1] += a1;
            });

            if (force) {
                const ok = confirm('Override will save this mapping even though the totals do not reconcile against the extracted PDF amounts. Continue?');
                if (!ok) return;
            }

            document.getElementById('confirmStatus').innerHTML = '<div>Saving mapping…</div>';
            fetch('/api/af/uploads/' + uploadId, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mapped_data: mapped, status: 'mapped' })
            })
            .then(r => r.json())
            .then(patchResp => {
                if (!patchResp.success) {
                    throw new Error(patchResp.error || 'Failed to save mapping');
                }
                const confirmUrl = '/api/af/confirm/' + uploadId + (force ? '?force=true' : '');
                return fetch(confirmUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ force: !!force })
                });
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    const msg = force
                        ? 'Extraction confirmed (override — totals not reconciled).'
                        : 'Extraction confirmed and saved!';
                    document.getElementById('confirmStatus').innerHTML = '<div class="success">' + msg + '</div>';
                    setTimeout(() => window.location.href = '/audited-financials', 1500);
                } else {
                    document.getElementById('confirmStatus').innerHTML = '<div class="unmapped">Error: ' + data.error + ' — use <b>Override &amp; Save</b> if the mismatch is intentional.</div>';
                }
            })
            .catch(err => {
                document.getElementById('confirmStatus').innerHTML = '<div class="unmapped">Error: ' + err.message + '</div>';
            });
        }

        renderRawData();
        renderMappedData();
        renderReconciliation();
    </script>
</body>
</html>
        """

        html = html.replace("{{ building_name }}", upload.building_name or "")
        html = html.replace("{{ entity_code }}", upload.entity_code or "")
        html = html.replace("{{ fiscal_year }}", upload.fiscal_year_end or "")
        html = html.replace("{{ upload_id }}", str(upload_id))
        html = html.replace("{{ raw_json }}", json.dumps(raw_extraction))
        html = html.replace("{{ mapped_json }}", json.dumps(mapped_data))
        html = html.replace("{{ unmapped_json }}", json.dumps(unmapped))
        # Query building's own summary row labels + sections for auto-matching
        building_labels = []
        building_label_sections = {}  # label → summary row (income/expense)
        try:
            bl_rows = db.session.execute(db.text(
                "SELECT label, section FROM budget_summary_rows "
                "WHERE entity_code = :ec AND row_type = 'data' "
                "ORDER BY display_order"
            ), {"ec": upload.entity_code}).fetchall()
            building_labels = [r[0] for r in bl_rows]
            for r in bl_rows:
                sec = (r[1] or "").lower()
                if sec == "income":
                    building_label_sections[r[0]] = "Total Operating Income"
                elif sec == "non-operating income":
                    building_label_sections[r[0]] = "Non-Operating Income"
                else:
                    building_label_sections[r[0]] = "Total Operating Expenses"
        except Exception:
            building_labels = []
            building_label_sections = {}

        html = html.replace("{{ century_categories_json }}", json.dumps(sorted(CENTURY_CATEGORIES)))
        html = html.replace("{{ century_to_summary_json }}", json.dumps(CENTURY_TO_SUMMARY))
        html = html.replace("{{ existing_rules_json }}", json.dumps(existing_rules))
        html = html.replace("{{ building_labels_json }}", json.dumps(building_labels))
        html = html.replace("{{ building_label_sections_json }}", json.dumps(building_label_sections))
        html = html.replace("{{ profile_id }}", str(upload.profile_id or 0))

        return render_template_string(html)

    # ─── API Endpoints ────────────────────────────────────────────────────────

    @bp.route("/api/af/profiles", methods=["GET"])
    def api_get_profiles():
        """List all auditor profiles."""
        profiles = AuditorProfile.query.all()
        return jsonify({
            "success": True,
            "profiles": [p.to_dict() for p in profiles]
        })


    @bp.route("/api/af/profiles", methods=["POST"])
    def api_create_profile():
        """Create new auditor profile."""
        data = request.get_json()
        try:
            profile = AuditorProfile(
                name=data.get("name"),
                firm_name=data.get("firm_name"),
                notes=data.get("notes", "")
            )
            db.session.add(profile)
            db.session.commit()
            return jsonify({
                "success": True,
                "profile": profile.to_dict()
            })
        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e)
            }), 400


    @bp.route("/api/af/profiles/<int:profile_id>", methods=["PUT"])
    def api_update_profile(profile_id):
        """Update auditor profile."""
        profile = AuditorProfile.query.get(profile_id)
        if not profile:
            return jsonify({"success": False, "error": "Profile not found"}), 404

        data = request.get_json()
        profile.name = data.get("name", profile.name)
        profile.firm_name = data.get("firm_name", profile.firm_name)
        profile.notes = data.get("notes", profile.notes)
        profile.updated_at = datetime.utcnow()

        db.session.commit()
        return jsonify({
            "success": True,
            "profile": profile.to_dict()
        })


    @bp.route("/api/af/profiles/<int:profile_id>", methods=["DELETE"])
    def api_delete_profile(profile_id):
        """Delete auditor profile."""
        profile = AuditorProfile.query.get(profile_id)
        if not profile:
            return jsonify({"success": False, "error": "Profile not found"}), 404

        db.session.delete(profile)
        db.session.commit()
        return jsonify({"success": True})


    @bp.route("/api/af/profiles/<int:profile_id>/rules", methods=["GET"])
    def api_get_rules(profile_id):
        """Get mapping rules for a profile."""
        profile = AuditorProfile.query.get(profile_id)
        if not profile:
            return jsonify({"success": False, "error": "Profile not found"}), 404

        return jsonify({
            "success": True,
            "rules": [r.to_dict() for r in profile.rules]
        })


    @bp.route("/api/af/profiles/<int:profile_id>/rules", methods=["POST"])
    def api_save_rules(profile_id):
        """Save/update mapping rules for a profile. Supports single rule add or bulk replace."""
        profile = AuditorProfile.query.get(profile_id)
        if not profile:
            return jsonify({"success": False, "error": "Profile not found"}), 404

        data = request.get_json()

        # Single rule add/update (from review page) — upsert by
        # (profile_id, auditor_line_item, section) so the same description
        # (e.g. "Cable service") can have separate rules for the revenue
        # and expense sections of the audit.
        if "auditor_line_item" in data:
            line_item = data.get("auditor_line_item")
            new_cat = data.get("century_category")
            new_section = _category_section(new_cat)
            existing_all = MappingRule.query.filter_by(
                profile_id=profile_id,
                auditor_line_item=line_item
            ).all()
            # Keep one rule per section. Upsert the matching-section rule;
            # leave rules for the OTHER section untouched. Delete any extra
            # duplicates within our section.
            same_section = [r for r in existing_all if _category_section(r.century_category) == new_section]
            rule = same_section[0] if same_section else None
            for dup in same_section[1:]:
                db.session.delete(dup)
            if rule:
                rule.auditor_category = data.get("auditor_category", "")
                rule.century_category = data.get("century_category")
                rule.split_pct = float(data.get("split_pct", 1.0))
                rule.notes = data.get("notes", "")
            else:
                rule = MappingRule(
                    profile_id=profile_id,
                    auditor_line_item=line_item,
                    auditor_category=data.get("auditor_category", ""),
                    century_category=data.get("century_category"),
                    split_pct=float(data.get("split_pct", 1.0)),
                    notes=data.get("notes", "")
                )
                db.session.add(rule)
            db.session.commit()
            return jsonify({"success": True, "rule": rule.to_dict()})

        # Bulk replace (from profiles page)
        rules_data = data.get("rules", [])

        # Delete existing rules not in the new list
        new_ids = {r.get("id") for r in rules_data if r.get("id")}
        for rule in profile.rules:
            if rule.id not in new_ids:
                db.session.delete(rule)

        # Add or update rules
        for rule_data in rules_data:
            rule_id = rule_data.get("id")
            if rule_id:
                rule = MappingRule.query.get(rule_id)
                if rule:
                    rule.auditor_line_item = rule_data.get("auditor_line_item")
                    rule.auditor_category = rule_data.get("auditor_category")
                    rule.century_category = rule_data.get("century_category")
                    rule.split_pct = float(rule_data.get("split_pct", 1.0))
                    rule.notes = rule_data.get("notes", "")
            else:
                rule = MappingRule(
                    profile_id=profile_id,
                    auditor_line_item=rule_data.get("auditor_line_item"),
                    auditor_category=rule_data.get("auditor_category"),
                    century_category=rule_data.get("century_category"),
                    split_pct=float(rule_data.get("split_pct", 1.0)),
                    notes=rule_data.get("notes", "")
                )
                db.session.add(rule)

        db.session.commit()
        return jsonify({
            "success": True,
            "rules": [r.to_dict() for r in profile.rules]
        })


    @bp.route("/api/af/upload", methods=["POST"])
    def api_upload():
        """Upload a PDF and create AuditUpload record."""
        try:
            entity_code = request.form.get("entity_code")
            profile_id = request.form.get("profile_id")
            fiscal_year_end = request.form.get("fiscal_year_end")
            pdf_file = request.files.get("pdf")

            if not entity_code or not pdf_file:
                return jsonify({"success": False, "error": "Missing entity_code or pdf"}), 400

            # Get building name
            buildings = get_buildings_list()
            building_name = next((b["building_name"] for b in buildings if b["entity_code"] == entity_code), "Unknown")

            # Save PDF to disk
            data_dir = get_data_dir()
            filename = f"{entity_code}_{fiscal_year_end}_{pdf_file.filename}"
            filepath = data_dir / filename
            pdf_file.save(str(filepath))

            # Create AuditUpload record
            upload = AuditUpload(
                entity_code=entity_code,
                building_name=building_name,
                profile_id=int(profile_id) if profile_id else None,
                fiscal_year_end=fiscal_year_end,
                pdf_filename=filename,
                status="uploaded"
            )
            db.session.add(upload)
            db.session.commit()

            return jsonify({
                "success": True,
                "upload_id": upload.id
            })
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return jsonify({"success": False, "error": str(e)}), 400


    @bp.route("/api/af/extract/<int:upload_id>", methods=["POST"])
    def api_extract(upload_id):
        """Extract Schedule of Expenses/Revenue from PDF using Claude."""
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404

        try:
            data_dir = get_data_dir()
            pdf_path = data_dir / upload.pdf_filename

            if not pdf_path.exists():
                return jsonify({"success": False, "error": "PDF file not found"}), 404

            # Extract from PDF (pass entity_code for building-aware categories)
            extracted = extract_from_pdf(str(pdf_path), upload.building_name, entity_code=upload.entity_code)
            if not extracted:
                return jsonify({"success": False, "error": "Failed to extract from PDF"}), 400

            upload.raw_extraction = json.dumps(extracted)
            upload.status = "extracted"
            upload.updated_at = datetime.utcnow()
            db.session.commit()

            return jsonify({
                "success": True,
                "extraction": extracted
            })
        except Exception as e:
            logger.error(f"Extract error: {e}")
            return jsonify({"success": False, "error": str(e)}), 400


    @bp.route("/api/af/map/<int:upload_id>", methods=["POST"])
    def api_map(upload_id):
        """Apply mapping rules to extracted data."""
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404

        if not upload.profile_id:
            return jsonify({"success": False, "error": "No auditor profile assigned"}), 400

        try:
            mapped, unmapped = apply_mapping_rules(upload.raw_extraction, upload.profile_id)
            if mapped is None:
                return jsonify({"success": False, "error": unmapped[0]}), 400

            upload.mapped_data = json.dumps(mapped)
            upload.status = "mapped"
            upload.updated_at = datetime.utcnow()
            db.session.commit()

            return jsonify({
                "success": True,
                "mapped": mapped,
                "unmapped_count": len(unmapped)
            })
        except Exception as e:
            logger.error(f"Map error: {e}")
            return jsonify({"success": False, "error": str(e)}), 400


    @bp.route("/api/af/confirm/<int:upload_id>", methods=["POST"])
    def api_confirm(upload_id):
        """Mark extraction as confirmed.

        Pass ``?force=true`` (or JSON body ``{"force": true}``) to bypass the
        revenue/expense total reconciliation check. Use when the mismatch is
        intentional — e.g. the auditor rolled categories together or the user
        moved items in a way that intentionally doesn't tie to the source PDF.
        """
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404

        # Parse force flag from query string or JSON body
        body = request.get_json(silent=True) or {}
        force = (
            str(request.args.get("force", "")).lower() in ("1", "true", "yes")
            or bool(body.get("force"))
        )

        try:
            # Validate totals match before confirming (unless forced)
            if not force and upload.raw_extraction and upload.mapped_data:
                try:
                    raw_extraction = json.loads(upload.raw_extraction)
                    mapped_data = json.loads(upload.mapped_data)

                    # Get extracted totals
                    extracted_revenue = 0
                    extracted_expense = 0
                    if raw_extraction.get("revenue") and raw_extraction["revenue"].get("total"):
                        extracted_revenue = raw_extraction["revenue"]["total"][0] if raw_extraction["revenue"]["total"] else 0
                    if raw_extraction.get("expenses") and raw_extraction["expenses"].get("total_expenses"):
                        extracted_expense = raw_extraction["expenses"]["total_expenses"][0] if raw_extraction["expenses"]["total_expenses"] else 0

                    # Sum mapped income and expense totals.
                    # mapped_data shape: {century_category: {"total": X, "year_totals": [...], ...}}
                    # Classify each category by CENTURY_TO_SUMMARY.
                    income_summary_rows = {"Total Operating Income", "Non-Operating Income"}
                    mapped_revenue = 0
                    mapped_expense = 0
                    for cat, info in mapped_data.items():
                        if not isinstance(info, dict):
                            continue
                        # Use year_totals[0] (most recent year) to compare against extracted total[0]
                        year_totals = info.get("year_totals") or []
                        year0 = year_totals[0] if year_totals else (info.get("total", 0) or 0)
                        if CENTURY_TO_SUMMARY.get(cat, "") in income_summary_rows:
                            mapped_revenue += year0
                        else:
                            mapped_expense += year0

                    # Check deltas within $1 tolerance
                    tolerance = 1
                    revenue_delta = abs(mapped_revenue - extracted_revenue)
                    expense_delta = abs(mapped_expense - extracted_expense)

                    if revenue_delta > tolerance:
                        return jsonify({
                            "success": False,
                            "error": f"Revenue totals do not match: extracted ${extracted_revenue:,.2f}, mapped ${mapped_revenue:,.2f} (delta: ${revenue_delta:,.2f})"
                        }), 400

                    if expense_delta > tolerance:
                        return jsonify({
                            "success": False,
                            "error": f"Expense totals do not match: extracted ${extracted_expense:,.2f}, mapped ${mapped_expense:,.2f} (delta: ${expense_delta:,.2f})"
                        }), 400

                except (json.JSONDecodeError, KeyError, IndexError) as e:
                    logger.warning(f"Validation parse error (upload {upload_id}): {e}")
                    # Don't block confirmation on parse errors

            upload.status = "confirmed"
            data = request.get_json(silent=True) or {}
            upload.confirmed_by = data.get("confirmed_by", "system")
            upload.confirmed_at = datetime.utcnow()
            upload.updated_at = datetime.utcnow()
            db.session.commit()

            return jsonify({"success": True})
        except Exception as e:
            logger.error(f"Confirm error: {e}")
            return jsonify({"success": False, "error": str(e)}), 400


    @bp.route("/api/af/uploads", methods=["GET"])
    def api_get_uploads():
        """List all uploads with optional filters."""
        entity_code = request.args.get("entity_code")
        status = request.args.get("status")

        query = AuditUpload.query
        if entity_code:
            query = query.filter_by(entity_code=entity_code)
        if status:
            query = query.filter_by(status=status)

        uploads = query.order_by(AuditUpload.created_at.desc()).all()
        return jsonify({
            "success": True,
            "uploads": [u.to_dict() for u in uploads]
        })


    @bp.route("/api/af/uploads/<int:upload_id>", methods=["GET"])
    def api_get_upload(upload_id):
        """Get single upload with all data."""
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404

        data = upload.to_dict()
        data["raw_extraction"] = json.loads(upload.raw_extraction) if upload.raw_extraction else {}
        data["mapped_data"] = json.loads(upload.mapped_data) if upload.mapped_data else {}

        return jsonify({
            "success": True,
            "upload": data
        })

    @bp.route("/api/af/uploads/<int:upload_id>", methods=["PATCH"])
    def api_patch_upload(upload_id):
        """Update mapped_data and/or status on an upload."""
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404
        data = request.get_json(silent=True) or {}
        if "mapped_data" in data:
            upload.mapped_data = json.dumps(data["mapped_data"])
        if "status" in data:
            upload.status = data["status"]
        if "profile_id" in data:
            upload.profile_id = int(data["profile_id"]) if data["profile_id"] else None
        upload.updated_at = datetime.utcnow()
        db.session.commit()
        return jsonify({"success": True})

    @bp.route("/api/af/uploads/<int:upload_id>", methods=["DELETE"])
    def api_delete_upload(upload_id):
        """Delete an upload."""
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404

        # Delete the PDF file if it exists
        if upload.pdf_filename:
            try:
                import os
                pdf_path = str(get_data_dir() / upload.pdf_filename)
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
            except Exception:
                pass  # File cleanup is best-effort

        try:
            db.session.delete(upload)
            db.session.commit()
            return jsonify({"success": True})
        except Exception as e:
            db.session.rollback()
            return jsonify({"success": False, "error": str(e)}), 500

    @bp.route("/api/af/bulk-upload", methods=["POST"])
    def api_bulk_upload():
        """Bulk upload PDFs from base64-encoded JSON payload.
        Expects: { "uploads": [ { "entity_code": "138", "profile_id": null, "fiscal_year_end": "2025", "filename": "test.pdf", "data_b64": "base64..." } ] }
        """
        import base64
        try:
            payload = request.get_json()
            uploads_data = payload.get("uploads", [])
            results = []
            buildings = get_buildings_list()
            data_dir = get_data_dir()

            for item in uploads_data:
                entity_code = item.get("entity_code")
                profile_id = item.get("profile_id")
                fiscal_year_end = item.get("fiscal_year_end", "2025")
                filename = item.get("filename", "upload.pdf")
                data_b64 = item.get("data_b64", "")

                if not entity_code or not data_b64:
                    results.append({"entity_code": entity_code, "success": False, "error": "Missing entity_code or data"})
                    continue

                building_name = next((b["building_name"] for b in buildings if b["entity_code"] == entity_code), "Unknown")
                safe_filename = f"{entity_code}_{fiscal_year_end}_{filename}"
                filepath = data_dir / safe_filename

                # Decode and save
                pdf_bytes = base64.b64decode(data_b64)
                with open(str(filepath), "wb") as f:
                    f.write(pdf_bytes)

                upload = AuditUpload(
                    entity_code=entity_code,
                    building_name=building_name,
                    profile_id=int(profile_id) if profile_id else None,
                    fiscal_year_end=fiscal_year_end,
                    pdf_filename=safe_filename,
                    status="uploaded"
                )
                db.session.add(upload)
                db.session.commit()
                results.append({"entity_code": entity_code, "success": True, "upload_id": upload.id})

            return jsonify({"success": True, "results": results})
        except Exception as e:
            db.session.rollback()
            logger.error(f"Bulk upload error: {e}")
            return jsonify({"success": False, "error": str(e)}), 400

    # ─── Return Blueprint and Models ───────────────────────────────────────────

    models = {
        "AuditorProfile": AuditorProfile,
        "MappingRule": MappingRule,
        "AuditUpload": AuditUpload
    }

    helpers = {
        "get_confirmed_actuals": get_confirmed_actuals,
        "get_buildings_list": get_buildings_list,
        "apply_mapping_rules": apply_mapping_rules,
        "extract_from_pdf": extract_from_pdf
    }

    return bp, models, helpers
