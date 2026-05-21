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

from flask import Blueprint, render_template_string, request, jsonify, send_file, abort
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


# FA dir 2026-05-21: heuristic auto-suggest for audit line → Century category.
# Used after extraction so the FA mostly just clicks Accept instead of picking
# every category by hand. 99% of audit lines are obvious ("Maintenance" →
# Maintenance, "Insurance" → Insurance, "Real estate taxes" → Real Estate
# Taxes). We score on token overlap + substring + section-hint, return None
# when confidence is too low so the FA picks manually instead of seeing a
# bad default.
_INFER_STOPWORDS = {
    "and", "or", "the", "a", "an", "of", "to", "for", "in", "on", "with",
    "amp", "from", "by", "at",
}

# Tiny synonym map so token-overlap doesn't fail on simple stem mismatches
# (audit says "Electricity" but the canonical category is "Electric") or on
# common rewrites ("Wages" / "Salaries" → Payroll). Keep this dict short —
# the goal is to nail the obvious 90% cases, not exhaustively map English.
_INFER_SYNONYMS = {
    "electricity": "electric",
    "wages": "payroll",
    "salary": "payroll",
    "salaries": "payroll",
    "compensation": "payroll",
    "legal": "professional",
    "accounting": "professional",
    "audit": "professional",
    "auditing": "professional",
    "admin": "administrative",
    "office": "administrative",
}


def _expand_tokens(tokens):
    """Expand a token set with known synonyms so e.g. 'electricity' also
    matches a category label tokenized as 'electric'."""
    out = set(tokens)
    for t in tokens:
        syn = _INFER_SYNONYMS.get(t)
        if syn:
            out.add(syn)
    return out


# FA dir 2026-05-21: collapse near-duplicate labels seen across the portfolio
# (typos, plural/singular, lowercase variants, common aliases) to one
# canonical form so the picker doesn't show "Common Charge / Common Charges
# / HOA Common Charges / Commercial Common Charge / Commercial Common Charges"
# as five separate options. Conservative — only collapses where the variant
# is clearly the same row. Distinct rows like "Electric Income" vs "Electric"
# (utility expense) or "Fuel" vs "Fuel Oil" stay separate.
_CANON_LABEL_FIXES = {
    # Typos
    "adminsitrative & other": "Administrative & Other",
    "adminstrative & other": "Administrative & Other",
    "contigency": "Contingency",
    "commerical rent": "Commercial Rent",
    # Singular → plural canonical
    "common charge": "Common Charges",
    "commercial common charge": "Commercial Common Charges",
    "corporation tax": "Corporate Taxes",
    "corporation taxes": "Corporate Taxes",
    # Aliases (different label, same row purpose)
    "hoa common charges": "Common Charges",
    "bike": "Bicycle",
    "bike storage": "Bicycle Storage",
    # Lowercase / capitalization fixes (some Excel parsers preserved
    # whatever-case the building had typed it as)
    "capital assessment": "Capital Assessment",
    "cooking gas": "Gas Cooking / Heating",
    "garage rent": "Garage Rent",
    "air conditioner": "Air Conditioner",
    # Gas/Heat label sprawl → one canonical
    "gas cooking": "Gas Cooking / Heating",
    "gas heating": "Gas Cooking / Heating",
    "gas heat": "Gas Cooking / Heating",
    "gas & heat": "Gas Cooking / Heating",
    "gas cooking & heat": "Gas Cooking / Heating",
    "gas and gas heating": "Gas Cooking / Heating",
}


def _canonical_label(label):
    """Map near-duplicates to a single canonical label string. Used to dedupe
    the portfolio-wide picker universe so typos and plural variants don't
    each show up as separate dropdown options.

    Step 1 — whitespace normalize: collapse double spaces, regularize spaces
    around hyphens / slashes / ampersands. Catches "Assessment - Operating"
    vs "Assessment -Operating" vs "Assessment- Operating" (which otherwise
    hash to 3 distinct dict keys and appear 3 times in the dropdown).

    Step 2 — apply the typo/plural/alias fix table.
    """
    import re as _re2
    if not label:
        return label
    l = label.strip()
    # Whitespace + separator normalize. ORDER MATTERS: collapse whitespace
    # around separators first, then collapse interior runs of spaces.
    l = _re2.sub(r"\s*-\s*", " - ", l)
    l = _re2.sub(r"\s*/\s*", " / ", l)
    l = _re2.sub(r"\s*&\s*", " & ", l)
    l = _re2.sub(r"\s+", " ", l).strip()
    return _CANON_LABEL_FIXES.get(l.lower(), l)


def _infer_category(description, candidates, sections_by_label, section_hint=None):
    """Pick the best Century category for an audit line description.

    Args:
        description: auditor's text for the line (e.g. "Repairs and Maintenance")
        candidates: iterable of candidate category labels (the dropdown universe)
        sections_by_label: dict label → 'Total Operating Income' /
            'Total Operating Expenses' / 'Non-Operating Income'. Used to bias
            scoring with the section_hint so revenue lines don't get mapped
            to expense categories and vice versa.
        section_hint: 'revenue' or 'expense' — which P&L section of the audit
            this line was extracted from.

    Returns: best-match label string, or None if confidence is too low.
    """
    import re as _re
    if not description or not candidates:
        return None
    desc_low = description.lower().strip()
    desc_tokens_raw = set(_re.findall(r"[a-z0-9]+", desc_low)) - _INFER_STOPWORDS
    if not desc_tokens_raw:
        return None
    desc_tokens = _expand_tokens(desc_tokens_raw)
    best = None
    best_score = 0.0
    for label in candidates:
        if not label:
            continue
        label_low = label.lower().strip()
        label_tokens = set(_re.findall(r"[a-z0-9]+", label_low)) - _INFER_STOPWORDS
        if not label_tokens:
            continue
        # Expand label tokens too — so a synonym match works in either direction.
        label_tokens_expanded = _expand_tokens(label_tokens)
        overlap = desc_tokens & label_tokens_expanded
        if not overlap:
            continue
        # coverage = fraction of label tokens matched; precision = fraction of
        # description tokens that hit a label. We weight coverage higher so
        # short labels like "Insurance" beat partial matches of longer labels.
        coverage = len(overlap) / len(label_tokens)
        precision = len(overlap) / max(len(desc_tokens), 1)
        score = coverage * 0.7 + precision * 0.3
        # substring bonus — whole-label match in description is a strong signal
        if label_low in desc_low or desc_low in label_low:
            score += 0.3
        # section hint: bias toward labels in the same P&L section, hard-penalize
        # cross-section matches (e.g. revenue line shouldn't map to expense cat)
        if section_hint:
            sec = (sections_by_label.get(label) or "").lower()
            if section_hint == "revenue":
                if "income" in sec:
                    score += 0.2
                elif "expense" in sec:
                    score -= 0.5
            elif section_hint == "expense":
                if "expense" in sec:
                    score += 0.2
                elif "income" in sec and "non-operating" not in sec:
                    score -= 0.5
        if score > best_score:
            best_score = score
            best = label
    return best if best_score >= 0.5 else None


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
        # Per-summary-row source-line overrides set by the FA via the
        # Building Detail Inspector (Phase 2 audit drill-down). Shape:
        #   {summary_label: {total, year_totals, source_lines: [
        #       {id, auditor_desc, amount}, ...
        #   ]}}
        # Takes precedence over mapped_data + raw_extraction backfill when
        # populated. Lets FAs edit/move/delete/add audit lines without
        # touching the original Claude extraction.
        summary_overrides = db.Column(db.Text, nullable=True)
        # SharePoint web URL (Office viewer link) captured at audit-click
        # time. Used for the review page's "Open audit PDF" link so the
        # FA can open the source doc directly in SharePoint instead of
        # streaming bytes through this app — robust against Railway's
        # ephemeral local filesystem.
        sharepoint_web_url = db.Column(db.Text, nullable=True)
        # FA dir 2026-05-21: Claude extracts the auditor firm name from the
        # PDF cover/signature page. Used to auto-select the matching auditor
        # profile on the review page so FAs don't have to pick from a dropdown.
        detected_firm = db.Column(db.String(255), nullable=True)
        status = db.Column(db.String(20), default="uploaded")  # uploaded, extracted, mapped, confirmed
        confirmed_by = db.Column(db.String(255), default="")
        confirmed_at = db.Column(db.DateTime, nullable=True)
        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        # Relationship
        profile = db.relationship("AuditorProfile", back_populates="uploads")

        def to_dict(self):
            so = None
            if self.summary_overrides:
                try:
                    so = json.loads(self.summary_overrides)
                except Exception:
                    so = None
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
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
                "summary_overrides": so,
                "sharepoint_web_url": self.sharepoint_web_url
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

        # FA dir 2026-05-21: tolerant accumulator. If a profile rule points at
        # a category that isn't in CENTURY_CATEGORIES (stale rule, typo, or
        # renamed category), accept it on-the-fly instead of crashing with a
        # KeyError that leaks 'Gas-Heating' up to the FA.
        def _ensure_cat(c):
            if c not in mapped:
                mapped[c] = {"total": 0, "years": [], "year_totals": [0] * num_years,
                              "_stale_category": True}
        # Process revenue items
        if "revenue" in extracted and "items" in extracted["revenue"]:
            for item in extracted["revenue"]["items"]:
                description = item.get("description", "")
                amounts = item.get("amounts", [])
                rule, confidence = fuzzy_match_rule(description, rules, section="revenue")

                if rule and confidence > 0.5:
                    cat = rule.century_category
                    pct = rule.split_pct
                    _ensure_cat(cat)
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
                        _ensure_cat(cat)
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

ALSO identify the auditing firm name from the cover page, signature page,
or any "Independent Auditor's Report" header. Examples of common firms:
  - "PKF O'Connor Davies LLP"
  - "Marks Paneth LLP"
  - "Anchin, Block & Anchin LLP"
  - "Frankel Loughran Starr & Vallone LLP"
Return the EXACT firm name as it appears on the audit (including LLP/P.C./etc.).

{cat_instruction}

Return ONLY valid JSON (no markdown, no code blocks) with this structure:
{{
  "building_name": "{building_name}",
  "auditor_firm": "PKF O'Connor Davies LLP",
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

            # max_tokens=16384: full audit extraction with source_lines for 30+
            # line items easily exceeds 4096 (the prior cap). Hitting the cap
            # produces truncated JSON → json.loads fails AND ("Expecting value:
            # line 1 column 1") if the response is somehow empty. 16384 is the
            # safe ceiling for current Claude models.
            # One retry on empty/truncated response — transient API issues happen.
            last_err = None
            response_text = ""
            stop_reason = None
            for attempt in range(2):
                message = client.messages.create(
                    model=os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
                    max_tokens=16384,
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
                stop_reason = getattr(message, "stop_reason", None)
                # Pull the first text block. content can be a list of TextBlock /
                # ToolUseBlock / ThinkingBlock; only TextBlock has .text.
                response_text = ""
                for block in (message.content or []):
                    t = getattr(block, "text", None)
                    if isinstance(t, str) and t.strip():
                        response_text = t.strip()
                        break
                if response_text:
                    break  # got a non-empty response, exit retry loop
                last_err = f"empty Claude response (attempt {attempt+1}, stop_reason={stop_reason})"
                logger.warning(f"extract_from_pdf: {last_err}")

            if not response_text:
                # Two attempts in, still empty. Give a real error message.
                raise RuntimeError(
                    f"Claude returned no text content for this PDF "
                    f"(stop_reason={stop_reason}). The PDF was {len(pdf_data)//1024} KB base64. "
                    f"Try splitting the PDF or check the model name."
                )

            # Strip markdown code fences if Claude wrapped the JSON (despite the
            # "ONLY valid JSON" instruction — it sometimes still does).
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            if response_text.startswith("```"):
                response_text = response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()

            try:
                extracted = json.loads(response_text)
            except json.JSONDecodeError as jde:
                # Truncated JSON usually has a specific column number. If
                # stop_reason was "max_tokens", we know it was truncation.
                hint = ""
                if stop_reason == "max_tokens":
                    hint = " (response was truncated — JSON cut off mid-generation)"
                head = response_text[:200].replace("\n", " ")
                tail = response_text[-200:].replace("\n", " ")
                raise RuntimeError(
                    f"Claude returned non-JSON response{hint}. "
                    f"JSON error: {jde}. "
                    f"First 200 chars: {head!r} ... last 200 chars: {tail!r}"
                ) from jde

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
            logger.exception("[review_page ERROR] upload_id=%s: %s", upload_id, _rp_err)
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
                    logger.exception("[review_page] apply_mapping_rules failed for upload %s: %s",
                                     upload_id, _amr_err)
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
    <div style="margin-bottom:16px;display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
        <a href="/audited-financials" class="back-link">← Back to Uploads</a>
        {{ pdf_link_html }}
    </div>

    {{ status_banner }}

    <!-- Auditor profile + dropdown scope controls -->
    <div style="background:white; border:1px solid var(--gray-200); border-radius:10px; padding:14px 18px; margin-bottom:16px; display:flex; align-items:center; gap:18px; flex-wrap:wrap;">
        <div style="display:flex; align-items:center; gap:10px;">
            <label for="profilePicker" style="font-size:12px; font-weight:700; letter-spacing:0.05em; text-transform:uppercase; color:var(--gray-500);">Auditor Profile</label>
            <select id="profilePicker" onchange="onProfileChange()" style="padding:6px 10px; border:1px solid var(--gray-300); border-radius:4px; font-size:13px; min-width:240px;">
                <option value="">— Loading profiles… —</option>
            </select>
            <span id="profileStatus" style="font-size:12px; color:var(--gray-500);"></span>
            <a href="/audited-financials/profiles" target="_blank" style="font-size:12px; color:var(--blue); text-decoration:none;">Manage profiles ↗</a>
        </div>
        <div style="margin-left:auto; display:flex; align-items:center; gap:8px;">
            <input type="checkbox" id="showAllCategories" checked onchange="toggleCategoryScope()">
            <label for="showAllCategories" style="font-size:12px; color:var(--gray-700);">Show all Century categories (uncheck to narrow to just this building)</label>
        </div>
    </div>

    <div class="columns">
        <div class="column">
            <h3>Extracted Data — Map Each Item</h3>
            <div id="rawData"></div>
            <!-- FA dir 2026-05-21: Save & Apply Profile Rules removed.
                 Audits drift even within the same auditor — accumulating
                 reusable rules planted wrong defaults on subsequent audits.
                 Each audit now gets mapped fresh; Confirm & Save below is
                 the only path forward. Profile picker (above) still useful
                 for "who audited this" tracking + auto-detect. -->
            <div id="saveApplyStatus" style="margin-top:6px; font-size:12px; color:var(--gray-500);"></div>
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
        // Standard Century = the curated ~30 canonical category set, minus
        // any labels the building already has of its own. Always reachable
        // in the picker so common categories are a fast path even for
        // buildings with sparse own-labels.
        const standardCenturyLabels = {{ standard_century_labels_json }};
        const standardCenturySections = {{ standard_century_sections_json }};
        const standardCenturySet = new Set(standardCenturyLabels);
        const centuryCatSet = new Set(centuryCategories);
        // FA dir 2026-05-21: heuristic-suggested category per audit line.
        // Keyed by normalized (lowercase, trimmed) description. Used in
        // makeDropdown() as the default selection when no explicit mapping
        // is in mapped_data already. Lands in PEACH (needs Accept) so the
        // FA must consciously confirm before it counts.
        const suggestedCategories = {{ suggested_categories_json }};
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
        // Group by section then sort each group alphabetically for findability.
        const bldgIncome = buildingLabels.filter(c => {
            const s = (buildingLabelSections[c] || '').toLowerCase();
            return s.includes('income') && !s.includes('non-operating');
        }).slice().sort();
        const bldgExpense = buildingLabels.filter(c => {
            const s = (buildingLabelSections[c] || '').toLowerCase();
            return s.includes('expense') || s === '';
        }).slice().sort();
        const bldgNonOp = buildingLabels.filter(c => {
            const s = (buildingLabelSections[c] || '').toLowerCase();
            return s.includes('non-operating');
        }).slice().sort();
        const bldgSet = new Set(buildingLabels);
        // "Other portfolio" = everything in the portfolio universe minus
        // this building's own labels AND minus the Standard Century tier
        // (which we'll render as its own group above).
        const otherCentury = centuryCategories.filter(c => !bldgSet.has(c) && !standardCenturySet.has(c)).sort();

        // FA dir 2026-05-21: split otherCentury by section so the dropdown
        // always shows a clear Income / Expenses / Non-Operating breakdown,
        // even when this building has no labels of its own (e.g. 106 / 5 W 14
        // foundation_no_prior_budget). Without this, "Show all Century cats"
        // produced one flat group of 70+ options with no visual hierarchy.
        function _sectionOfCenturyCat(c) {
            const s = (CENTURY_TO_SUMMARY[c] || '').toLowerCase();
            if (s.includes('non-operating')) return 'non-op';
            if (s.includes('income')) return 'income';
            return 'expense';  // default + explicit expense rows
        }
        const otherIncome = otherCentury.filter(c => _sectionOfCenturyCat(c) === 'income');
        const otherExpense = otherCentury.filter(c => _sectionOfCenturyCat(c) === 'expense');
        const otherNonOp = otherCentury.filter(c => _sectionOfCenturyCat(c) === 'non-op');

        // Standard Century tier — partition by section using its own
        // pre-computed sections dict (passed from Python).
        function _sectionOfStdCat(c) {
            const s = (standardCenturySections[c] || '').toLowerCase();
            if (s.includes('non-operating')) return 'non-op';
            if (s.includes('income')) return 'income';
            return 'expense';
        }
        const stdIncome = standardCenturyLabels.filter(c => _sectionOfStdCat(c) === 'income');
        const stdExpense = standardCenturyLabels.filter(c => _sectionOfStdCat(c) === 'expense');
        const stdNonOp = standardCenturyLabels.filter(c => _sectionOfStdCat(c) === 'non-op');

        // Scope toggle declared up-front so buildSelectOptions can read it.
        // toggleCategoryScope() further down flips this and rebuilds dropdowns.
        // Auto-on ONLY when a suggestion lands in the long-tail portfolio
        // (outside both building labels AND Standard Century). With the
        // Standard tier always visible, most common suggestions are already
        // reachable without needing showAll.
        let _showAllScope = Object.values(suggestedCategories).some(s =>
            s && !buildingLabelSet.has(s) && !standardCenturySet.has(s)
        );

        function _renderOptgroup(label, items, currentMapping) {
            if (!items || items.length === 0) return '';
            let opts = '<optgroup label="' + label + '">';
            for (let c of items) {
                opts += '<option value="' + c + '"' + (c === currentMapping ? ' selected' : '') + '>' + c + '</option>';
            }
            opts += '</optgroup>';
            return opts;
        }

        function buildSelectOptions(currentMapping) {
            let opts = '<option value="">— Select category —</option>';
            // This building's labels first (most relevant — pick these
            // when col 2 should land on the FA's exact summary row text).
            opts += _renderOptgroup('Income (this building)', bldgIncome, currentMapping);
            opts += _renderOptgroup('Expenses (this building)', bldgExpense, currentMapping);
            opts += _renderOptgroup('Non-Operating (this building)', bldgNonOp, currentMapping);
            // Standard Century — always visible. Curated canonical list of
            // ~30 categories that exists for every building in the portfolio.
            opts += _renderOptgroup('Standard Century — Income', stdIncome, currentMapping);
            opts += _renderOptgroup('Standard Century — Expenses', stdExpense, currentMapping);
            opts += _renderOptgroup('Standard Century — Non-Operating', stdNonOp, currentMapping);
            // Other portfolio long-tail — only when "Show all" is checked.
            if (_showAllScope) {
                opts += _renderOptgroup('Other portfolio — Income', otherIncome, currentMapping);
                opts += _renderOptgroup('Other portfolio — Expenses', otherExpense, currentMapping);
                opts += _renderOptgroup('Other portfolio — Non-Operating', otherNonOp, currentMapping);
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
            // FA dir 2026-05-21: fall back to heuristic suggestion from Python
            // side. Tracks whether this is a guess vs. a confirmed prior rule
            // so we can render a hint below the dropdown ("(suggested)").
            let isSuggestion = false;
            if (!currentMapping && suggestedCategories[normalized]) {
                currentMapping = suggestedCategories[normalized];
                isSuggestion = true;
            }

            // FA dir 2026-05-21: every unaccepted row gets the peach
            // background so it visually signals "needs your input". Previous
            // logic only colored rows with a recommended mapping — rows
            // missing a recommendation (like entity 106 line items) looked
            // white/done. White is now reserved for confirmed mappings, peach
            // means "still needs Accept", green means "accepted".
            const bgStyle = 'background:#fff3cd;';

            let html = '<div data-section="' + (section || 'expense') + '" style="display:flex; align-items:center; gap:4px;">';
            html += '<select id="' + id + '" data-desc="' + description.replace(/"/g, '&quot;') + '" data-amount="' + (amount || 0) + '" data-amount1="' + (amount1 || 0) + '" data-orig-cat="' + (currentMapping || '').replace(/"/g, '&quot;') + '" data-accepted="false" data-suggested="' + (isSuggestion ? 'true' : 'false') + '" onchange="onDropdownChange(this); renderReconciliation(); updateAcceptState();" style="flex:1; padding:4px; font-size:12px; border:1px solid #ccc; border-radius:3px; cursor:pointer; ' + bgStyle + '">';
            html += buildSelectOptions(currentMapping);
            html += '</select>';
            html += '<button onclick="acceptRow(this)" class="accept-btn" style="padding:3px 8px; font-size:11px; background:#f59e0b; color:#fff; border:none; border-radius:3px; cursor:pointer; white-space:nowrap;" title="Confirm this mapping">✓ Accept</button>';
            html += '</div>';
            if (isSuggestion) {
                html += '<div class="suggested-hint" style="font-size:10px; color:#7c6500; font-style:italic; margin-top:2px;">💡 suggested — please confirm or change</div>';
            }
            return html;
        }

        // Version for split rows — inherits parent mapping, starts yellow
        function makeDropdownWithDefault(description, amount, section, defaultMapping, amount1) {
            const id = 'map_' + itemIndex++;
            // FA dir 2026-05-21: peach for ALL unaccepted rows (consistent
            // with makeDropdown above). Split rows without a default also
            // need attention, not blend into white.
            const bgStyle = 'background:#fff3cd;';
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
            // Combobox trigger needs to mirror the new accepted state.
            if (sel._cb_sync) sel._cb_sync();
            // Clear the "(suggested)" hint — once confirmed, it's no longer a guess.
            const hint = wrapper.parentElement && wrapper.parentElement.querySelector('.suggested-hint');
            if (hint) hint.style.display = 'none';
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
            // Mirror state back to the combobox trigger.
            if (el._cb_sync) el._cb_sync();
            // Once the FA picks something different, the suggestion-flag is
            // no longer meaningful — hide the hint so the row reads as the
            // FA's own choice instead of "computer guessed this".
            el.dataset.suggested = 'false';
            const wrapper = el.closest('[data-section]');
            const hint = wrapper && wrapper.parentElement && wrapper.parentElement.querySelector('.suggested-hint');
            if (hint) hint.style.display = 'none';
            // FA dir 2026-05-21: Add-to-Summary action removed per FA review.
            // Audit mapping no longer auto-creates Summary rows. If a picked
            // category doesn't exist as a Summary row, the audit data lands
            // in mapped_data but doesn't surface in Col 2 until the FA
            // manually adds the row from the Summary tab's "+ Add Row" action.
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
        // FA dir 2026-05-21: caches each split's original parent-row HTML so
        // unsplitRow() can restore it. Keyed by a unique splitId stamped on
        // every child row this split creates. Cleared when unsplit fires.
        window._splitOriginalHTML = window._splitOriginalHTML || {};
        let _splitIdCounter = 0;

        function splitRow(btn) {
            const row = btn.closest('tr');
            const sourceData = JSON.parse(btn.dataset.sources);
            const section = btn.dataset.section;
            const years = rawExtraction.fiscal_years || [];
            // Get the parent row's current dropdown selection
            const parentSelect = row.querySelector('select[id^="map_"]');
            const parentMapping = parentSelect ? parentSelect.value : '';
            const parentGroup = row.dataset.group || '';

            // FA dir 2026-05-21: cache the ORIGINAL parent HTML before we
            // destroy it. unsplitRow() will re-insert this exact HTML to
            // recombine the rows when the FA changes their mind.
            const splitId = 'split-' + (++_splitIdCounter) + '-' + Date.now();
            window._splitOriginalHTML[splitId] = row.outerHTML;

            let newRows = '';
            sourceData.forEach((sl, idx) => {
                const desc = sl.auditor_desc || sl.description || '?';
                const amounts = sl.amounts || [];
                const amount0 = amounts[0] || 0;
                const mapId = 'map_' + itemIndex;  // peek before makeDropdownWithDefault increments
                const isFirst = (idx === 0);
                newRows += '<tr data-group="' + parentGroup + '" data-split-id="' + splitId + '" style="border-bottom:1px solid #eee; background:#fffbeb;">';
                // Wrap the description so we can append the Unsplit link to
                // the FIRST sub-row (not every one — only need one to undo).
                newRows += '<td style="padding:6px 6px 6px 30px; font-style:italic;">' + desc;
                if (isFirst) {
                    newRows += '<a href="javascript:void(0)" onclick="unsplitRow(this); return false;" class="unsplit-link"'
                            + ' data-split-id="' + splitId + '"'
                            + ' style="margin-left:10px; font-size:10px; color:#0369a1; text-decoration:underline; cursor:pointer; font-style:normal;"'
                            + ' title="Recombine these split rows back into the original single row.">'
                            + '↩ Unsplit (recombine into one row)</a>';
                }
                newRows += '</td>';
                for (let yi = 0; yi < amounts.length; yi++) {
                    newRows += '<td style="text-align:right; padding:4px;">';
                    if (yi === 0) { newRows += makeAmtInput(amounts[yi], mapId); }
                    else { newRows += makeAmtReadonly(amounts[yi]); }
                    newRows += '</td>';
                }
                for (let i = amounts.length; i < years.length; i++) { newRows += '<td style="text-align:right; padding:4px;">' + makeAmtReadonly(0) + '</td>'; }
                const amount1 = amounts[1] || 0;
                newRows += '<td style="padding:4px;">' + makeDropdownWithDefault(desc, amount0, section, parentMapping, amount1) + '</td></tr>';
            });
            row.insertAdjacentHTML('afterend', newRows);
            cleanupComboboxesInNode(row);  // remove old popup from body
            row.remove();
            renderReconciliation();
            recalcLeftTotals();
            attachComboboxesAll();  // attach combobox to the newly-inserted sub-row dropdowns
        }

        // FA dir 2026-05-21: undo Split. Removes all sub-rows that share the
        // splitId and re-inserts the original parent row HTML in their place.
        // The original HTML includes the original dropdown (Mapping reverts
        // to whatever was picked at the moment of splitting).
        function unsplitRow(linkEl) {
            const splitId = linkEl.dataset.splitId;
            if (!splitId) return;
            const originalHTML = window._splitOriginalHTML[splitId];
            if (!originalHTML) {
                alert('Could not find the original row to restore.');
                return;
            }
            const subRows = document.querySelectorAll('tr[data-split-id="' + splitId + '"]');
            if (subRows.length === 0) return;
            // Insert the original BEFORE the first sub-row, then delete all
            // sub-rows. This preserves position in the table.
            const firstSub = subRows[0];
            firstSub.insertAdjacentHTML('beforebegin', originalHTML);
            subRows.forEach(r => {
                cleanupComboboxesInNode(r);  // pop popups from body
                r.remove();
            });
            // Clean up the cache entry — same split can be re-triggered later.
            delete window._splitOriginalHTML[splitId];
            renderReconciliation();
            recalcLeftTotals();
            updateAcceptState();
            attachComboboxesAll();  // re-attach combobox to restored dropdown
        }

        // FA dir 2026-05-21: searchable combobox per dropdown.
        // The underlying <select> stays as state (so confirmExtraction,
        // updateAcceptState, renderReconciliation all keep reading sel.value
        // unchanged). On top we render a custom trigger + popup with a search
        // input + filtered list. Solves "FA has to scroll a 100-item dropdown"
        // when picking categories outside this building's own labels.
        //
        // Layout: the trigger replaces the <select> visually inside the same
        // flex container. The <select> stays in the DOM with display:none
        // so all the existing data-attributes (data-amount, data-desc,
        // data-accepted, data-suggested, data-orig-cat) keep working.
        //
        // Picking an item in the popup writes to sel.value and dispatches a
        // 'change' event so onDropdownChange + renderReconciliation fire
        // as if the FA had used the native <select>.
        function attachComboboxesAll() {
            document.querySelectorAll('select[id^="map_"]').forEach(sel => {
                if (sel.dataset.cbAttached === '1') return;
                attachCombobox(sel);
                sel.dataset.cbAttached = '1';
            });
        }

        // When a row is removed (splitRow / unsplitRow / deleteMappingRow),
        // its <select>'s popup is still mounted on document.body. Walk the
        // node we're about to remove and yank popups so they don't leak.
        function cleanupComboboxesInNode(node) {
            if (!node) return;
            const sels = node.matches && node.matches('select[id^="map_"]')
                ? [node]
                : Array.from(node.querySelectorAll('select[id^="map_"]'));
            sels.forEach(s => {
                if (s._cb_popup && s._cb_popup.parentNode) {
                    s._cb_popup.parentNode.removeChild(s._cb_popup);
                }
            });
        }

        // Single popup shared across all comboboxes — only one can be open
        // at a time. Lives on document.body so it floats above the table
        // without absolute-position weirdness inside table cells.
        let _cbActivePopup = null;
        let _cbActiveSel = null;

        function attachCombobox(sel) {
            const wrap = document.createElement('div');
            wrap.className = 'cb-wrap';
            wrap.style.cssText = 'position:relative; flex:1; min-width:160px;';

            const trigger = document.createElement('button');
            trigger.type = 'button';
            trigger.className = 'cb-trigger';
            // !important on color because the page-wide `button { color:white }`
            // CSS rule applies and makes the picked-value text invisible on
            // the peach background otherwise.
            trigger.style.cssText = 'width:100%; text-align:left; padding:4px 24px 4px 8px; font-size:12px; border:1px solid #ccc; border-radius:3px; cursor:pointer; min-height:26px; position:relative; font-family:inherit; color:#1a1714 !important; font-weight:normal;';

            const popup = document.createElement('div');
            popup.className = 'cb-popup';
            // Use display:none so it stays truly hidden until openPopup().
            // Mount on body to escape table-cell clipping/overflow.
            popup.style.cssText = 'position:fixed; z-index:10000; display:none; flex-direction:column; width:320px; background:#fff; border:1px solid #999; border-radius:6px; box-shadow:0 6px 20px rgba(0,0,0,0.2); color:#1a1714;';

            const search = document.createElement('input');
            search.type = 'text';
            search.placeholder = 'Type to search 100+ categories...';
            search.style.cssText = 'padding:8px 10px; font-size:13px; border:none; border-bottom:1px solid #ddd; outline:none; font-family:inherit; color:#1a1714; background:#fff;';
            popup.appendChild(search);

            const listEl = document.createElement('div');
            listEl.className = 'cb-list';
            // Explicit max-height + overflow on the list itself (instead of
            // relying on flex:1 inside a position:fixed parent, which doesn't
            // size reliably across browsers).
            listEl.style.cssText = 'overflow-y:auto; overflow-x:hidden; max-height:340px; color:#1a1714;';
            popup.appendChild(listEl);

            function renderList(filter) {
                listEl.innerHTML = '';
                const flt = (filter || '').toLowerCase().trim();
                // Iterate optgroups in the underlying <select> to preserve
                // the group order and visual hierarchy (this building first,
                // Other Century below).
                for (const og of sel.querySelectorAll('optgroup')) {
                    const groupName = og.getAttribute('label') || '';
                    const items = [];
                    for (const opt of og.querySelectorAll('option')) {
                        const v = opt.value;
                        const t = opt.textContent;
                        if (!v) continue;
                        if (!flt || t.toLowerCase().includes(flt)) {
                            items.push({ v: v, t: t });
                        }
                    }
                    if (items.length === 0) continue;
                    const header = document.createElement('div');
                    header.textContent = groupName;
                    header.style.cssText = 'padding:6px 10px; font-size:10px; font-weight:700; color:#555; background:#f1f5f9; text-transform:uppercase; letter-spacing:0.5px; position:sticky; top:0;';
                    listEl.appendChild(header);
                    for (const it of items) {
                        const itemDiv = document.createElement('div');
                        itemDiv.textContent = it.t;
                        itemDiv.dataset.value = it.v;
                        itemDiv.style.cssText = 'padding:5px 12px; font-size:13px; cursor:pointer;' + (it.v === sel.value ? ' background:#dbeafe; font-weight:600;' : '');
                        itemDiv.addEventListener('mouseenter', () => { itemDiv.style.background = '#e0f2fe'; });
                        itemDiv.addEventListener('mouseleave', () => { itemDiv.style.background = (it.v === sel.value ? '#dbeafe' : ''); });
                        itemDiv.addEventListener('click', () => {
                            sel.value = it.v;
                            syncTrigger();
                            closePopup();
                            sel.dispatchEvent(new Event('change', { bubbles: true }));
                        });
                        listEl.appendChild(itemDiv);
                    }
                }
                if (listEl.children.length === 0) {
                    const empty = document.createElement('div');
                    empty.textContent = 'No matches';
                    empty.style.cssText = 'padding:12px; color:#888; font-size:12px; font-style:italic; text-align:center;';
                    listEl.appendChild(empty);
                }
            }

            function syncTrigger() {
                const v = sel.value;
                // Force dark text on every inner element — the page CSS rule
                // `button { color:white }` cascades into spans inside the
                // trigger, so each span needs an explicit color too.
                const label = v
                    ? '<span class="cb-val" style="color:#1a1714;">' + escapeHtml(v) + '</span>'
                    : '<span style="color:#888;">— Select category —</span>';
                trigger.innerHTML = label + '<span style="position:absolute; right:8px; top:50%; transform:translateY(-50%); color:#555; font-size:10px;">▾</span>';
                // Mirror the <select>'s background (peach/green/white).
                trigger.style.background = sel.style.background || (v ? '#fff3cd' : '#fff');
                // If accepted, lock the trigger so FA can't accidentally re-open
                // while the row is green. They can still click to change.
                if (sel.dataset.accepted === 'true') {
                    trigger.style.borderColor = '#16a34a';
                } else {
                    trigger.style.borderColor = '#ccc';
                }
            }

            function openPopup() {
                // Close any other open popup first — only one combobox open
                // at a time across the page.
                if (_cbActivePopup && _cbActivePopup !== popup) {
                    _cbActivePopup.style.display = 'none';
                }
                _cbActivePopup = popup;
                _cbActiveSel = sel;
                // Position relative to trigger. Compute available viewport
                // space above and below so the popup never extends off-screen
                // (which makes the inner scroll region unreachable).
                const rect = trigger.getBoundingClientRect();
                const VP_PAD = 16;  // viewport edge padding
                const spaceBelow = window.innerHeight - rect.bottom - VP_PAD;
                const spaceAbove = rect.top - VP_PAD;
                const preferDown = spaceBelow >= 220 || spaceBelow >= spaceAbove;
                // Hard cap at 380px so the popup doesn't dominate the screen
                // even when there's tons of room.
                const popupMaxH = Math.max(180, Math.min(380, preferDown ? spaceBelow : spaceAbove));
                popup.style.display = 'flex';
                popup.style.maxHeight = popupMaxH + 'px';
                popup.style.left = rect.left + 'px';
                if (preferDown) {
                    popup.style.top = (rect.bottom + 4) + 'px';
                    popup.style.bottom = 'auto';
                } else {
                    popup.style.top = 'auto';
                    popup.style.bottom = (window.innerHeight - rect.top + 4) + 'px';
                }
                // List takes whatever's left after the search bar (~44px).
                listEl.style.maxHeight = Math.max(120, popupMaxH - 44) + 'px';
                search.value = '';
                renderList('');
                setTimeout(() => search.focus(), 0);
            }
            function closePopup() {
                popup.style.display = 'none';
                if (_cbActivePopup === popup) {
                    _cbActivePopup = null;
                    _cbActiveSel = null;
                }
            }

            trigger.addEventListener('click', (e) => {
                e.stopPropagation();
                const isOpen = popup.style.display === 'flex';
                if (isOpen) closePopup();
                else openPopup();
            });
            // Stop popup clicks from bubbling to the document-level close handler.
            popup.addEventListener('click', (e) => { e.stopPropagation(); });

            search.addEventListener('input', () => renderList(search.value));
            search.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') { closePopup(); }
                if (e.key === 'Enter') {
                    const first = listEl.querySelector('[data-value]');
                    if (first) { first.click(); e.preventDefault(); }
                }
            });

            // Hide the native <select> visually, insert combobox before it.
            sel.style.display = 'none';
            sel.parentElement.insertBefore(wrap, sel);
            wrap.appendChild(trigger);
            // Popup mounts on document.body — escapes table cell clipping
            // and z-index battles.
            document.body.appendChild(popup);

            // Hook so acceptRow/onDropdownChange can call sel._cb_sync()
            // after they mutate the <select>'s state.
            sel._cb_sync = syncTrigger;
            // Track popup so it can be cleaned up if the select is removed
            // (e.g. splitRow replaces the row).
            sel._cb_popup = popup;
            syncTrigger();
        }

        // Single document-level click handler — closes whatever combobox
        // popup is currently open if the click lands outside both the
        // popup and its trigger. One listener for the whole page (registering
        // one per combobox was the previous bug).
        document.addEventListener('click', (e) => {
            if (!_cbActivePopup) return;
            // If the click is inside the popup or inside the active combobox
            // trigger, leave it alone.
            if (_cbActivePopup.contains(e.target)) return;
            const sel = _cbActiveSel;
            const wrap = sel && sel.parentElement && sel.parentElement.querySelector('.cb-wrap');
            if (wrap && wrap.contains(e.target)) return;
            _cbActivePopup.style.display = 'none';
            _cbActivePopup = null;
            _cbActiveSel = null;
        });
        // Close on scroll / resize so the popup doesn't float over old positions.
        window.addEventListener('scroll', () => {
            if (_cbActivePopup) {
                _cbActivePopup.style.display = 'none';
                _cbActivePopup = null;
                _cbActiveSel = null;
            }
        }, true);

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
                html += '<th style="text-align:left; padding:6px; width:180px;">Map To</th><th style="width:32px;"></th></tr>';
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
                    html += '<td style="padding:4px;">' + makeDropdown(item.description, amount0, 'revenue', (item.amounts && item.amounts[1]) || 0) + '</td>';
                    html += '<td style="text-align:center;padding:4px;"><button type="button" title="Delete this row" onclick="deleteMappingRow(this)" style="border:1px solid #fecaca;background:white;color:#b91c1c;padding:2px 6px;border-radius:4px;cursor:pointer;font-size:11px;">×</button></td>';
                    html += '</tr>';
                }
                if (rawExtraction.revenue.total) {
                    html += '<tr style="font-weight:bold; border-top:2px solid #333;"><td style="padding:6px;">Total Revenue</td>';
                    for (let yi = 0; yi < rawExtraction.revenue.total.length; yi++) {
                        if (yi === 0) { html += '<td style="text-align:right; padding:6px;"><span id="total-revenue">' + formatAmount(rawExtraction.revenue.total[yi]) + '</span></td>'; }
                        else { html += '<td style="text-align:right; padding:6px;">' + formatAmount(rawExtraction.revenue.total[yi]) + '</td>'; }
                    }
                    html += '<td></td><td></td></tr>';
                }
                // "+ Add row" footer for revenue — opens a modal with desc + amount + category fields.
                html += '<tr><td colspan="' + (years.length + 3) + '" style="padding:8px 6px;text-align:left;">'
                    + '<button type="button" onclick="addMappingRow(\\\'revenue\\\', null)" style="border:1px solid var(--blue);background:#eff6ff;color:var(--blue);padding:4px 12px;border-radius:4px;cursor:pointer;font-size:12px;font-weight:600;">+ Add Revenue line</button>'
                    + '</td></tr>';
                html += '</table>';
            }

            if (rawExtraction.expenses && rawExtraction.expenses.categories) {
                html += '<h5 style="margin:15px 0 5px;">Expenses</h5>';
                html += '<table style="width:100%; font-size:13px; border-collapse:collapse;"><tr><th style="text-align:left; padding:6px;">Line Item</th>';
                for (let yi = 0; yi < years.length; yi++) { html += '<th style="text-align:right; padding:6px; width:100px;">' + years[yi] + (yi === 0 ? ' ✎' : '') + '</th>'; }
                html += '<th style="text-align:left; padding:6px; width:180px;">Map To</th><th style="width:32px;"></th></tr>';

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
                    html += '<tr><td colspan="' + (years.length + 3) + '" style="font-weight:bold; background:#f0f0f0; padding:8px 6px;">' + cat.name + '</td></tr>';
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
                        html += '<td style="padding:4px;">' + makeDropdown(item.description, amount0, 'expense', (item.amounts && item.amounts[1]) || 0) + '</td>';
                        html += '<td style="text-align:center;padding:4px;"><button type="button" title="Delete this row" onclick="deleteMappingRow(this)" style="border:1px solid #fecaca;background:white;color:#b91c1c;padding:2px 6px;border-radius:4px;cursor:pointer;font-size:11px;">×</button></td>';
                        html += '</tr>';
                    }
                    if (cat.total) {
                        html += '<tr style="font-weight:bold; border-bottom:2px solid #ddd;"><td style="padding:6px 6px 6px 20px;">Subtotal</td>';
                        for (let yi = 0; yi < cat.total.length; yi++) {
                            if (yi === 0) { html += '<td style="text-align:right; padding:6px;"><span id="subtotal-exp-' + catIdx + '">' + formatAmount(cat.total[yi]) + '</span></td>'; }
                            else { html += '<td style="text-align:right; padding:6px;">' + formatAmount(cat.total[yi]) + '</td>'; }
                        }
                        html += '<td></td><td></td></tr>';
                    }
                    // "+ Add row" footer per expense category — places new row at end of this group.
                    html += '<tr><td colspan="' + (years.length + 3) + '" style="padding:8px 6px 8px 20px;text-align:left;">'
                        + '<button type="button" onclick="addMappingRow(\\\'expense\\\', ' + catIdx + ')" style="border:1px solid var(--blue);background:#eff6ff;color:var(--blue);padding:4px 12px;border-radius:4px;cursor:pointer;font-size:12px;font-weight:600;">+ Add line in ' + (cat.name || \'Expenses\') + '</button>'
                        + '</td></tr>';
                    catIdx++;
                }
                if (rawExtraction.expenses.total_expenses) {
                    html += '<tr style="font-weight:bold; border-top:2px solid #333;"><td style="padding:6px;">Total Expenses</td>';
                    for (let yi = 0; yi < rawExtraction.expenses.total_expenses.length; yi++) {
                        if (yi === 0) { html += '<td style="text-align:right; padding:6px;"><span id="total-expenses">' + formatAmount(rawExtraction.expenses.total_expenses[yi]) + '</span></td>'; }
                        else { html += '<td style="text-align:right; padding:6px;">' + formatAmount(rawExtraction.expenses.total_expenses[yi]) + '</td>'; }
                    }
                    html += '<td></td><td></td></tr>';
                }
                html += '</table>';
            }

            container.innerHTML = html;
        }

        // Delete a row from the mapping table. After Confirm, the absent
        // dropdown means the line stops contributing to mapped_data — so this
        // effectively excludes the row when the FA next saves. raw_extraction
        // is unchanged so a re-extract restores Claude's original list.
        function deleteMappingRow(btn) {
            const tr = btn.closest('tr');
            if (!tr) return;
            const desc = tr.querySelector('td')?.textContent?.trim().split('\\n')[0] || 'this row';
            if (!confirm('Delete \"' + desc.slice(0, 60) + '\"?\\n\\nThe line is removed from this mapping but the original auditor extraction is preserved.')) return;
            cleanupComboboxesInNode(tr);
            tr.parentNode.removeChild(tr);
            try { renderReconciliation(); } catch (e) {}
            try { recalcLeftTotals(); } catch (e) {}
            try { updateAcceptState(); } catch (e) {}
        }

        // Add a new row to the mapping table — used when Claude missed a line.
        // section is 'revenue' or 'expense'; catIdx is the expense category
        // index (only relevant for expense — places the row at the end of
        // that category group).
        function addMappingRow(section, catIdx) {
            const overlay = document.createElement('div');
            overlay.style.cssText = 'position:fixed;inset:0;background:rgba(15,23,42,0.45);display:flex;align-items:center;justify-content:center;z-index:1000;';
            overlay.innerHTML =
                '<div style="background:white;border-radius:10px;box-shadow:0 20px 50px rgba(0,0,0,0.25);width:460px;max-width:92vw;padding:20px 22px;">' +
                  '<div style="font-size:11px;font-weight:700;color:var(--gray-500);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;">Add ' + section + ' line</div>' +
                  '<div style="font-size:12px;color:var(--gray-500);margin-bottom:14px;">For lines Claude missed. The new row joins the table and goes through the same Confirm flow as auto-extracted rows.</div>' +
                  '<div style="display:flex;flex-direction:column;gap:10px;">' +
                    '<label style="font-size:12px;font-weight:600;color:var(--gray-700);">Auditor description<input id="addRowDesc" type="text" style="display:block;width:100%;margin-top:4px;padding:7px 9px;font-size:13px;border:1px solid var(--gray-300);border-radius:6px;" placeholder="e.g. Heat / Steam"></label>' +
                    '<div style="display:flex;gap:10px;">' +
                      '<label style="flex:1;font-size:12px;font-weight:600;color:var(--gray-700);">Year-0 amount<input id="addRowAmt0" type="text" style="display:block;width:100%;margin-top:4px;padding:7px 9px;font-size:13px;border:1px solid var(--gray-300);border-radius:6px;font-family:monospace;text-align:right;" placeholder="0"></label>' +
                      '<label style="flex:1;font-size:12px;font-weight:600;color:var(--gray-700);">Year-1 amount<input id="addRowAmt1" type="text" style="display:block;width:100%;margin-top:4px;padding:7px 9px;font-size:13px;border:1px solid var(--gray-300);border-radius:6px;font-family:monospace;text-align:right;" placeholder="0"></label>' +
                    '</div>' +
                  '</div>' +
                  '<div style="display:flex;justify-content:flex-end;gap:8px;margin-top:18px;">' +
                    '<button id="addRowCancel" style="padding:6px 14px;font-size:13px;background:white;color:var(--gray-700);border:1px solid var(--gray-300);border-radius:6px;cursor:pointer;">Cancel</button>' +
                    '<button id="addRowSave" style="padding:6px 14px;font-size:13px;background:var(--blue);color:white;border:none;border-radius:6px;cursor:pointer;font-weight:600;">Add line</button>' +
                  '</div>' +
                '</div>';
            document.body.appendChild(overlay);
            const close = () => { try { document.body.removeChild(overlay); } catch (e) {} };
            document.getElementById('addRowCancel').onclick = close;
            overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
            const escH = (e) => { if (e.key === 'Escape') { close(); document.removeEventListener('keydown', escH); } };
            document.addEventListener('keydown', escH);
            setTimeout(() => document.getElementById('addRowDesc').focus(), 0);

            document.getElementById('addRowSave').onclick = () => {
                const desc = (document.getElementById('addRowDesc').value || '').trim();
                const a0 = parseFloat((document.getElementById('addRowAmt0').value || '0').replace(/[,$\s]/g, ''));
                const a1 = parseFloat((document.getElementById('addRowAmt1').value || '0').replace(/[,$\s]/g, ''));
                if (!desc) { alert('Description required'); return; }
                if (isNaN(a0)) { alert('Year-0 amount must be a number'); return; }
                close();
                document.removeEventListener('keydown', escH);

                // Build a new row matching the existing markup pattern. It
                // gets a fresh map_<itemIndex> id so the Confirm flow picks
                // it up the same as Claude-extracted rows.
                const mapId = 'map_' + itemIndex;
                const dropdownHtml = makeDropdown(desc, a0, section, isNaN(a1) ? 0 : a1);

                const tr = document.createElement('tr');
                tr.setAttribute('data-group', section === 'revenue' ? 'revenue' : ('exp_' + catIdx));
                tr.style.borderBottom = '1px solid #eee';
                tr.style.background = '#fef9c3';  // amber tint = manually added

                const isExpense = section === 'expense';
                const tdLabel = '<td style="padding:6px ' + (isExpense ? '6px 6px 20px' : '6px') + ';">'
                    + desc + ' <span style="display:inline-block;font-size:10px;color:#7c3aed;margin-left:6px;background:#f3e8ff;padding:1px 6px;border-radius:8px;">manual</span></td>';
                const tdAmt0 = '<td style="text-align:right; padding:4px;">' + makeAmtInput(a0, mapId) + '</td>';
                const tdAmt1 = '<td style="text-align:right; padding:4px;">' + makeAmtReadonly(isNaN(a1) ? 0 : a1) + '</td>';
                const tdDrop = '<td style="padding:4px;">' + dropdownHtml + '</td>';
                const tdDel = '<td style="text-align:center;padding:4px;"><button type="button" title="Delete this row" onclick="deleteMappingRow(this)" style="border:1px solid #fecaca;background:white;color:#b91c1c;padding:2px 6px;border-radius:4px;cursor:pointer;font-size:11px;">×</button></td>';
                tr.innerHTML = tdLabel + tdAmt0 + tdAmt1 + tdDrop + tdDel;

                // Insert just before the appropriate "+ Add ..." footer button.
                // Strategy: find the button whose onclick matches our section/cat
                // and put the new tr right above its row.
                const allBtns = document.querySelectorAll('button[onclick^="addMappingRow"]');
                let targetBtn = null;
                allBtns.forEach(b => {
                    const oc = b.getAttribute('onclick') || '';
                    if (section === 'revenue' && oc.indexOf("'revenue'") >= 0) targetBtn = b;
                    else if (section === 'expense' && oc.indexOf("'expense', " + catIdx) >= 0) targetBtn = b;
                });
                if (targetBtn) {
                    const footerTr = targetBtn.closest('tr');
                    footerTr.parentNode.insertBefore(tr, footerTr);
                } else {
                    // Fallback: append at end of any table
                    const tbl = document.querySelector('#rawData table');
                    if (tbl) tbl.querySelector('tbody, table').appendChild(tr);
                }

                try { renderReconciliation(); } catch (e) {}
                try { recalcLeftTotals(); } catch (e) {}
                try { updateAcceptState(); } catch (e) {}
                try { attachComboboxesAll(); } catch (e) {}  // wrap new dropdown
            };
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

        // ─── F2: profile picker + scope toggle ───
        let _profiles = [];
        let _currentProfileId = profileId || null;
        // FA dir 2026-05-21: Claude-detected auditor firm. Used to auto-select
        // the matching profile when the FA hasn't manually picked one yet.
        const DETECTED_FIRM = {{ detected_firm_json }};

        function _fuzzyFirmMatch(detected, profiles) {
            // Return matching profile(s). Lowercase substring match in either
            // direction. Filters out clearly noise (sub-3-char firm names).
            if (!detected) return [];
            const d = String(detected).toLowerCase().trim();
            if (d.length < 3) return [];
            return (profiles || []).filter(p => {
                const f = String(p.firm_name || '').toLowerCase().trim();
                if (!f || f.length < 3) return false;
                return f === d || f.indexOf(d) !== -1 || d.indexOf(f) !== -1;
            });
        }

        function loadProfiles() {
            fetch('/api/af/profiles').then(r => r.json()).then(j => {
                _profiles = j.profiles || [];

                // FA dir 2026-05-21: auto-select on FIRST load only (when no
                // profile is currently assigned). Don't override the FA's
                // explicit pick on subsequent renders.
                let autoSelected = false;
                if (!_currentProfileId && DETECTED_FIRM) {
                    const matches = _fuzzyFirmMatch(DETECTED_FIRM, _profiles);
                    if (matches.length === 1) {
                        _currentProfileId = matches[0].id;
                        autoSelected = true;
                        // Persist the auto-selection to the upload row
                        fetch('/api/af/uploads/' + {{ upload_id }}, {
                            method: 'PATCH',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({profile_id: matches[0].id})
                        }).catch(() => {/* non-fatal */});
                    }
                }

                const sel = document.getElementById('profilePicker');
                const cur = _currentProfileId;
                let opts = '<option value="">— Pick auditor profile to enable Save —</option>';
                _profiles.forEach(p => {
                    const _selAttr = (cur && p.id === cur) ? ' selected' : '';
                    const label = p.firm_name || p.name;
                    opts += '<option value="' + p.id + '"' + _selAttr + '>' + escapeHtml(label) + '</option>';
                });
                sel.innerHTML = opts;
                updateProfileStatus(autoSelected);
            }).catch(err => {
                document.getElementById('profileStatus').textContent = 'Profile load failed: ' + err;
            });
        }
        function escapeHtml(s) {
            return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        }
        function updateProfileStatus(autoSelected) {
            const el = document.getElementById('profileStatus');
            if (!el) return;
            if (_currentProfileId) {
                const p = _profiles.find(x => x.id === _currentProfileId);
                const firmLabel = (p && (p.firm_name || p.name)) || 'profile #' + _currentProfileId;
                if (autoSelected) {
                    el.innerHTML = '<span style="color:#15803d;">✓ Auto-detected: <b>'
                        + escapeHtml(firmLabel) + '</b></span>'
                        + (DETECTED_FIRM ? ' <span style="color:var(--gray-500); font-size:11px;">(from PDF: "'
                            + escapeHtml(DETECTED_FIRM) + '")</span>' : '');
                } else {
                    el.innerHTML = '<span style="color:#15803d;">✓ ' + escapeHtml(firmLabel) + '</span>';
                }
                return;
            }
            // No profile assigned. If Claude detected a firm but we have no
            // matching profile, surface a Create-Profile CTA.
            if (DETECTED_FIRM) {
                const enc = encodeURIComponent(DETECTED_FIRM);
                el.innerHTML = '<span style="color:#b45309;">⚠ Detected '
                    + '<b>"' + escapeHtml(DETECTED_FIRM) + '"</b> in this audit — '
                    + 'no matching profile.</span> '
                    + '<a href="/audited-financials/profiles?prefill_firm=' + enc + '" '
                    + 'target="_blank" style="font-size:12px; color:var(--blue); text-decoration:underline;">'
                    + 'Create profile from this audit ↗</a>';
            } else {
                el.innerHTML = '<span style="color:#b45309;">⚠ No profile assigned — Save Mappings is disabled</span>';
            }
        }
        function onProfileChange() {
            const sel = document.getElementById('profilePicker');
            const newId = parseInt(sel.value || '0', 10) || null;
            if (!newId) return;
            // PATCH the upload
            fetch('/api/af/uploads/' + {{ upload_id }}, {
                method: 'PATCH',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({profile_id: newId})
            }).then(r => r.json()).then(j => {
                if (j.success === false) {
                    alert('Profile update failed: ' + (j.error || 'unknown'));
                    return;
                }
                _currentProfileId = newId;
                updateProfileStatus();
                // Reload so existing rules for the new profile pre-populate dropdowns
                location.reload();
            });
        }
        // FA dir 2026-05-21: rebuild every map_* dropdown's options when the
        // checkbox toggles. Previous version used `optgroup { display:none }`
        // which Safari + some Edge builds ignored, leaving the FA unable to
        // reach Other Century Categories even with the checkbox on.
        // Rebuilding the options DOM is unconditional, browser-independent.
        // (_showAllScope itself is declared near buildSelectOptions above so
        // both the toggle and the option builder share one source of truth.)
        function toggleCategoryScope() {
            _showAllScope = !!document.getElementById('showAllCategories').checked;
            document.querySelectorAll('select[id^="map_"]').forEach(sel => {
                const current = sel.value;  // preserve selection across rebuild
                sel.innerHTML = buildSelectOptions(current);
                // Try to restore — if the previously-selected option is now
                // outside the visible scope, the rebuild will have left it
                // unset and we just leave it that way (FA will see empty).
                if (current) sel.value = current;
            });
        }
        // Initialize on load
        document.addEventListener('DOMContentLoaded', () => {
            loadProfiles();
            // _showAllScope was already computed at script eval time (above),
            // so the initial dropdown render used the right scope. Here we
            // just sync the checkbox UI to match. No rebuild needed.
            const cb = document.getElementById('showAllCategories');
            if (cb) cb.checked = _showAllScope;
        });

        // FA dir 2026-05-21: removed saveAndApplyRules() function.
        // Audits vary too much between cycles even within the same auditor
        // for reusable rules to be reliable. Each audit gets mapped fresh
        // via the dropdowns directly. Confirm & Save reads dropdowns and
        // writes mapped_data on confirm — no rule layer in between.

        function confirmExtraction(uploadId, force) {
            // Build mapped_data from the DOM dropdowns BEFORE confirming.
            // Historical bug: this function used to POST straight to /confirm
            // without saving the user's dropdown selections, leaving
            // mapped_data empty in the DB and breaking col2 on the summary.
            // We also persist source_lines (the auditor's literal description
            // + amounts per row) so the FA Dashboard summary can drill down
            // into Col 2 and show the per-line breakdown without re-running
            // mapping rules. Audits confirmed before this change still work
            // via a backfill from raw_extraction in the summary endpoint.
            const mapped = {};
            const selects = document.querySelectorAll('select[id^="map_"]');
            selects.forEach(s => {
                const cat = stripCatSuffix(s.value || '');
                if (!cat) return;
                const a0 = parseFloat(s.dataset.amount) || 0;
                const a1 = parseFloat(s.dataset.amount1) || 0;
                if (!mapped[cat]) {
                    mapped[cat] = { total: 0, year_totals: [0, 0], years: [], source_lines: [] };
                }
                mapped[cat].total += a0;
                mapped[cat].year_totals[0] += a0;
                mapped[cat].year_totals[1] += a1;
                mapped[cat].source_lines.push({
                    auditor_desc: s.dataset.desc || '',
                    amounts: [a0, a1],
                });
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
                    const _ec = '{{ entity_code }}';
                    setTimeout(() => window.location.href = _ec ? '/wizard/' + _ec : '/audited-financials', 1500);
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
        // Combobox-ify every map_* dropdown after the table is in the DOM.
        attachComboboxesAll();
    </script>
</body>
</html>
        """

        # Status banner — explicit "already confirmed" cue so FAs know they're
        # editing a finalized audit (and that re-confirming will OVERWRITE the
        # mapped_data on this upload). For per-line tweaks they should use the
        # FA Dashboard Inspector instead, which preserves mapped_data.
        status_banner = ""
        if upload.status == "confirmed":
            confirmed_dt = ""
            if upload.confirmed_at:
                try:
                    confirmed_dt = upload.confirmed_at.strftime("%b %-d, %Y")
                except Exception:
                    try:
                        confirmed_dt = upload.confirmed_at.strftime("%b %d, %Y")
                    except Exception:
                        confirmed_dt = upload.confirmed_at.isoformat()[:10]
            confirmed_by = upload.confirmed_by or "system"
            ec = upload.entity_code or ""
            status_banner = (
                '<div style="background:#ecfdf5;border:1px solid #6ee7b7;border-left:4px solid #10b981;'
                'border-radius:8px;padding:14px 18px;margin-bottom:16px;display:flex;align-items:flex-start;gap:14px;">'
                '<div style="font-size:20px;line-height:1;">&#10003;</div>'
                '<div style="flex:1;">'
                f'<div style="font-size:14px;font-weight:700;color:#065f46;margin-bottom:4px;">'
                f'Already confirmed{(" on " + confirmed_dt) if confirmed_dt else ""}'
                f'{(" by " + confirmed_by) if confirmed_by and confirmed_by != "system" else ""}</div>'
                '<div style="font-size:13px;color:#065f46;line-height:1.5;">'
                'This audit has been finalized and the mapping is saved. Editing here and re-confirming will '
                '<b>overwrite the saved mapping</b> for this upload &mdash; suitable for full re-extracts or '
                'wholesale changes.'
                f'<br/>For per-line tweaks (edit amounts, move lines to different summary rows, add manual entries), '
                f'open <a href="/dashboard/{ec}" style="color:#065f46;font-weight:600;text-decoration:underline;">'
                f'Dashboard &rsaquo; Summary tab</a> and use the &#128270; Inspector on the Col 2 cells &mdash; those '
                'edits are preserved across re-confirms.</div></div></div>'
            )
        elif upload.status == "extracted":
            status_banner = (
                '<div style="background:#fffbeb;border:1px solid #fde68a;border-left:4px solid #f59e0b;'
                'border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#92400e;">'
                '<b>Extraction ready for review.</b> Pick an auditor profile, accept each row, then click '
                'Confirm &amp; Save below to finalize.</div>'
            )
        elif upload.status == "mapped":
            status_banner = (
                '<div style="background:#eff6ff;border:1px solid #bfdbfe;border-left:4px solid #3b82f6;'
                'border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#1e3a8a;">'
                '<b>Mapping saved, awaiting confirm.</b> Review the categorization and click Confirm &amp; Save '
                'when ready &mdash; this finalizes Col 2 on the Budget Summary.</div>'
            )

        # Audit PDF link — prefer the SharePoint web viewer (durable across
        # Railway dyno restarts that wipe the local file cache). Fall back
        # to the local serve endpoint only if no SP URL is recorded; if
        # both are missing, show a disabled hint.
        if upload.sharepoint_web_url:
            pdf_link_html = (
                f'<a href="{upload.sharepoint_web_url}" target="_blank" rel="noopener" '
                f'style="font-size:13px;font-weight:600;color:var(--blue);text-decoration:none;'
                f'display:inline-flex;align-items:center;gap:6px;padding:5px 12px;'
                f'border:1px solid var(--blue);border-radius:6px;background:#eff6ff;" '
                f'title="Opens the audit PDF in SharePoint">'
                f'&#128196; Open audit PDF in SharePoint &rsaquo;</a>'
            )
        elif upload.pdf_filename:
            pdf_link_html = (
                f'<a href="/api/af/uploads/{upload_id}/pdf" target="_blank" rel="noopener" '
                f'style="font-size:13px;font-weight:600;color:var(--blue);text-decoration:none;'
                f'display:inline-flex;align-items:center;gap:6px;padding:5px 12px;'
                f'border:1px solid var(--blue);border-radius:6px;background:#eff6ff;" '
                f'title="Opens the locally cached PDF">'
                f'&#128196; Open audit PDF &rsaquo;</a>'
            )
        else:
            pdf_link_html = ''

        html = html.replace("{{ pdf_link_html }}", pdf_link_html)
        html = html.replace("{{ status_banner }}", status_banner)
        html = html.replace("{{ building_name }}", upload.building_name or "")
        html = html.replace("{{ entity_code }}", upload.entity_code or "")
        html = html.replace("{{ fiscal_year }}", upload.fiscal_year_end or "")
        html = html.replace("{{ upload_id }}", str(upload_id))
        html = html.replace("{{ raw_json }}", json.dumps(raw_extraction))
        html = html.replace("{{ mapped_json }}", json.dumps(mapped_data))
        html = html.replace("{{ unmapped_json }}", json.dumps(unmapped))
        # Query building's own summary row labels + sections for auto-matching
        building_labels = []
        building_label_sections = {}  # label → summary row (income/expense/non-operating)

        def _classify_label(label, raw_section):
            """Return one of: 'Total Operating Income', 'Total Operating Expenses',
            'Non-Operating Income'. Resolution order:
              1. budget_summary_rows.section (most specific, set by parser)
              2. CENTURY_TO_SUMMARY (if label matches a known Century category)
              3. Keyword heuristic on the label text
              4. Default to expense (safest fallback)
            """
            sec = (raw_section or "").lower()
            if sec == "income":
                return "Total Operating Income"
            if sec in ("non-operating income", "non-operating"):
                return "Non-Operating Income"
            if sec in ("expense", "expenses"):
                return "Total Operating Expenses"
            # Layer 2: CENTURY_TO_SUMMARY mapping
            mapped = CENTURY_TO_SUMMARY.get(label)
            if mapped == "Non-Operating Income":
                return "Non-Operating Income"
            if mapped in INCOME_SUMMARY_ROWS:
                return "Total Operating Income"
            if mapped:
                return "Total Operating Expenses"
            # Layer 3: keyword heuristic
            low = (label or "").lower()
            if any(k in low for k in ("non-operating", "non operating", "interest income",
                                      "capital assess", "special assess", "tax refund",
                                      "insurance proceeds", "icon settlement", "sba-ppp",
                                      "loan proceeds")):
                return "Non-Operating Income"
            if any(k in low for k in ("income", "rent", "credit", "fee", "charge",
                                      "laundry", "garage", "storage", "bicycle",
                                      "assessment", "tax benefit", "commercial")):
                return "Total Operating Income"
            # Layer 4: default
            return "Total Operating Expenses"

        try:
            bl_rows = db.session.execute(db.text(
                "SELECT label, section FROM budget_summary_rows "
                "WHERE entity_code = :ec AND row_type = 'data' "
                "ORDER BY display_order"
            ), {"ec": upload.entity_code}).fetchall()
            building_labels = [r[0] for r in bl_rows]
            for r in bl_rows:
                building_label_sections[r[0]] = _classify_label(r[0], r[1])
        except Exception:
            building_labels = []
            building_label_sections = {}

        # FA dir 2026-05-21: pull the PORTFOLIO-WIDE label universe so the
        # dropdown shows every category that exists on any 2026 building's
        # 2024 Actuals column, not just the hardcoded CENTURY_CATEGORIES.
        # This way an FA mapping building 212 can still pick "Storage Income"
        # even though 212 doesn't have a Storage row of its own. Sections come
        # from budget_summary_rows.section (set by the 2026-approved parser),
        # falling back to _classify_label heuristics if blank.
        portfolio_label_sections = {}
        try:
            port_rows = db.session.execute(db.text(
                "SELECT DISTINCT label, section FROM budget_summary_rows "
                "WHERE row_type = 'data' AND label IS NOT NULL AND label <> ''"
            )).fetchall()
            for r in port_rows:
                lbl = _canonical_label(r[0])  # dedupe typos/plurals/aliases
                if lbl and lbl not in portfolio_label_sections:
                    portfolio_label_sections[lbl] = _classify_label(lbl, r[1])
        except Exception:
            portfolio_label_sections = {}
        # Always fold in the hardcoded CENTURY_CATEGORIES so the canonical list
        # is never missing (e.g. fresh DB, or a category no building uses yet).
        for c in CENTURY_CATEGORIES:
            canon = _canonical_label(c)
            if canon not in portfolio_label_sections:
                portfolio_label_sections[canon] = _classify_label(canon, None)
        # Master picker universe = sorted union.
        portfolio_labels = sorted(portfolio_label_sections.keys())

        # FA dir 2026-05-21: standard Century tier — the curated canonical
        # list. Always reachable in the picker even when a building's own
        # summary rows are sparse (e.g. 5 West 14th / entity 106 only has
        # "Water & Sewer" in budget_summary_rows). Without this tier the FA
        # would have to dig through 250+ long-tail portfolio entries to find
        # "Maintenance" or "Payroll".
        # Standard set = CENTURY_CATEGORIES minus what's already in the
        # building's own labels (no need to show duplicates).
        building_set = set(building_labels)
        standard_century_labels = [c for c in sorted(CENTURY_CATEGORIES) if c not in building_set]
        standard_century_sections = {
            c: _classify_label(c, None) for c in standard_century_labels
        }

        # FA dir 2026-05-21: auto-suggest a Century category per audit line.
        # Heuristic only (token overlap + substring + section hint). 99% of
        # audit lines map cleanly ("Maintenance" → Maintenance, "Insurance"
        # → Insurance). FA sees the suggestion pre-selected in peach (needs
        # Accept), can override before confirming. Inference candidate order:
        #   1. building's own labels — preferred so col 2 lands on the FA's
        #      actual summary row text.
        #   2. standard Century canonical — curated, well-defined options.
        #   3. portfolio long-tail — catch-all for niche labels.
        def _suggest(desc, section_hint):
            s = _infer_category(desc, building_labels, building_label_sections, section_hint)
            if s:
                return s
            s = _infer_category(desc, standard_century_labels, standard_century_sections, section_hint)
            if s:
                return s
            return _infer_category(desc, portfolio_labels, portfolio_label_sections, section_hint)

        suggested_categories = {}  # description (normalized) → suggested label
        try:
            for sect_key, hint in (("revenue", "revenue"), ("expense", "expense")):
                sect = (raw_extraction.get(sect_key) or {})
                for item in (sect.get("items") or []):
                    desc = (item.get("description") or "").strip()
                    if not desc:
                        continue
                    key = desc.lower().strip()
                    if key in suggested_categories:
                        continue
                    sugg = _suggest(desc, hint)
                    if sugg:
                        suggested_categories[key] = sugg
        except Exception:
            suggested_categories = {}

        # Extend CENTURY_TO_SUMMARY with portfolio labels (the JS dropdown
        # subdivider reads CENTURY_TO_SUMMARY to bucket "Other Century" into
        # Income / Expenses / Non-Op). Without this, portfolio-only labels
        # would land in the default Expenses bucket regardless of section.
        century_to_summary_extended = dict(CENTURY_TO_SUMMARY)
        for lbl, summary_row in portfolio_label_sections.items():
            if lbl not in century_to_summary_extended:
                century_to_summary_extended[lbl] = summary_row

        html = html.replace("{{ century_categories_json }}", json.dumps(portfolio_labels))
        html = html.replace("{{ century_to_summary_json }}", json.dumps(century_to_summary_extended))
        html = html.replace("{{ existing_rules_json }}", json.dumps(existing_rules))
        html = html.replace("{{ building_labels_json }}", json.dumps(building_labels))
        html = html.replace("{{ building_label_sections_json }}", json.dumps(building_label_sections))
        html = html.replace("{{ standard_century_labels_json }}", json.dumps(standard_century_labels))
        html = html.replace("{{ standard_century_sections_json }}", json.dumps(standard_century_sections))
        html = html.replace("{{ suggested_categories_json }}", json.dumps(suggested_categories))
        html = html.replace("{{ profile_id }}", str(upload.profile_id or 0))
        # FA dir 2026-05-21: pass Claude-detected auditor firm name so JS can
        # auto-select the matching profile in the dropdown.
        html = html.replace(
            "{{ detected_firm_json }}",
            json.dumps((getattr(upload, "detected_firm", None) or "")),
        )

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

        # Empty-mapping guard: confirming an upload with no mapped categories
        # leaves Col 2 (audit-derived) blank on the summary tab and produces
        # a row that's status='confirmed' yet structurally useless. Reject
        # outright unless the caller explicitly forces it (e.g. an FA who
        # intentionally has nothing to map for a specific year).
        try:
            md = json.loads(upload.mapped_data) if upload.mapped_data else {}
        except Exception:
            md = {}
        if not force and (not isinstance(md, dict) or len(md) == 0):
            return jsonify({
                "success": False,
                "error": (
                    "mapped_data is empty — open the review page, assign each "
                    "extracted line to a category, save, then confirm. Pass "
                    "force=true to bypass (intentional empty mapping)."
                )
            }), 400

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

                    # Classification must mirror the client-side renderReconciliation
                    # classifier (CENTURY_TO_SUMMARY → buildingLabelSections fallback)
                    # or the server validator disagrees with the on-page panel for
                    # building-specific categories not in the Century-wide map.
                    income_summary_rows = {"Total Operating Income", "Non-Operating Income"}
                    building_label_sections = {}
                    try:
                        bl_rows = db.session.execute(db.text(
                            "SELECT label, section FROM budget_summary_rows "
                            "WHERE entity_code = :ec AND row_type = 'data'"
                        ), {"ec": upload.entity_code}).fetchall()
                        for r in bl_rows:
                            sec = (r[1] or "").lower()
                            if sec == "income":
                                building_label_sections[r[0]] = "Total Operating Income"
                            elif sec == "non-operating income":
                                building_label_sections[r[0]] = "Non-Operating Income"
                            else:
                                building_label_sections[r[0]] = "Total Operating Expenses"
                    except Exception:
                        db.session.rollback()
                        building_label_sections = {}

                    mapped_revenue = 0
                    mapped_expense = 0
                    for cat, info in mapped_data.items():
                        if not isinstance(info, dict):
                            continue
                        # Use year_totals[0] (most recent year) to compare against extracted total[0]
                        year_totals = info.get("year_totals") or []
                        year0 = year_totals[0] if year_totals else (info.get("total", 0) or 0)
                        summary_row = (
                            CENTURY_TO_SUMMARY.get(cat)
                            or building_label_sections.get(cat)
                            or ""
                        )
                        if summary_row in income_summary_rows:
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

            # Phase E hook: confirming the audit also confirms the Foundation
            # for this entity (since audit mapping correctness was the whole
            # point of the foundation gate).
            try:
                db.session.execute(db.text(
                    "UPDATE budgets SET foundation_confirmed_at = NOW() "
                    "WHERE entity_code = :ec AND foundation_confirmed_at IS NULL"
                ), {"ec": upload.entity_code})
            except Exception as _e:
                logger.warning(f"foundation stamp on audit confirm failed (non-fatal): {_e}")

            db.session.commit()

            return jsonify({"success": True, "foundation_confirmed": True,
                            "entity_code": upload.entity_code})
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
        if "fiscal_year_end" in data:
            upload.fiscal_year_end = (data.get("fiscal_year_end") or "").strip()
        if "sharepoint_web_url" in data:
            upload.sharepoint_web_url = (data.get("sharepoint_web_url") or "").strip() or None
        upload.updated_at = datetime.utcnow()
        db.session.commit()
        return jsonify({"success": True})

    # ─── Phase 2: Per-line audit drill-down mutations ────────────────────────
    #
    # Each audit upload's per-summary-row source-line state lives in the
    # `summary_overrides` JSON column (see model). Shape:
    #   {summary_label: {
    #       total: <number>,
    #       year_totals: [<num>, <num>],
    #       source_lines: [{id, auditor_desc, amount, amounts}, ...]
    #   }}
    # Mutations always recompute total and year_totals from source_lines so
    # the JSON stays internally consistent — col2 on the Building Detail
    # summary reflects the current state on next read.

    def _audit_summary_overrides_get(upload):
        try:
            return json.loads(upload.summary_overrides) if upload.summary_overrides else {}
        except Exception:
            return {}

    def _audit_summary_overrides_save(upload, ovr):
        upload.summary_overrides = json.dumps(ovr)
        upload.updated_at = datetime.utcnow()

    def _audit_recompute_totals(category_block):
        """Mutate category_block in place: rewrite total + year_totals from
        source_lines. Caller is responsible for shape (must have a list at
        source_lines). amounts on each line is treated as [year0, year1].
        """
        sls = category_block.get("source_lines") or []
        y0 = 0.0
        y1 = 0.0
        for sl in sls:
            amts = sl.get("amounts")
            if isinstance(amts, list) and amts:
                try:
                    y0 += float(amts[0] or 0)
                except Exception:
                    pass
                if len(amts) > 1:
                    try:
                        y1 += float(amts[1] or 0)
                    except Exception:
                        pass
            else:
                try:
                    y0 += float(sl.get("amount") or 0)
                except Exception:
                    pass
        category_block["total"] = round(y0, 2)
        category_block["year_totals"] = [round(y0, 2), round(y1, 2)]

    def _audit_promote_backfill(upload, summary_label):
        """If summary_overrides has no entry for the requested label, promote
        a backfilled set of source_lines from raw_extraction. IDs match the
        deterministic format generated by workflow.py /api/summary
        (`raw:<top_desc>:<global_idx>`) so the FE can target lines by the
        same ID it received from /api/summary even before they're persisted.
        Returns the (possibly newly created) summary_overrides dict.
        """
        ovr = _audit_summary_overrides_get(upload)
        if summary_label in ovr and isinstance(ovr[summary_label], dict):
            return ovr  # already promoted

        raw_ext = {}
        try:
            raw_ext = json.loads(upload.raw_extraction) if upload.raw_extraction else {}
        except Exception:
            raw_ext = {}

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

        # Walk raw_extraction → emit (top_desc → flat_lines) mapping. Match
        # workflow.py's behavior: per top_item, expand source_lines (or fall
        # back to the top item itself if no nested source_lines).
        cat_lines = {}  # {top_desc: [{auditor_desc, amounts}, ...]}

        def _harvest(items):
            for it in items or []:
                if not isinstance(it, dict):
                    continue
                top_desc = (it.get("description") or "").strip()
                if not top_desc:
                    continue
                bucket = cat_lines.setdefault(top_desc, [])
                nested = it.get("source_lines") or []
                if nested:
                    for sl in nested:
                        if not isinstance(sl, dict):
                            continue
                        amts = sl.get("amounts") or []
                        a0 = amts[0] if (isinstance(amts, list) and amts) else (sl.get("amount") or 0)
                        a1 = amts[1] if (isinstance(amts, list) and len(amts) > 1) else 0
                        try:
                            a0f = float(a0 or 0)
                        except Exception:
                            a0f = 0.0
                        try:
                            a1f = float(a1 or 0)
                        except Exception:
                            a1f = 0.0
                        bucket.append({
                            "auditor_desc": sl.get("auditor_desc") or sl.get("description") or "",
                            "amount": a0f,
                            "amounts": [a0f, a1f],
                        })
                else:
                    amts = it.get("amounts") or []
                    a0 = amts[0] if (isinstance(amts, list) and amts) else 0
                    a1 = amts[1] if (isinstance(amts, list) and len(amts) > 1) else 0
                    try:
                        a0f = float(a0 or 0)
                    except Exception:
                        a0f = 0.0
                    try:
                        a1f = float(a1 or 0)
                    except Exception:
                        a1f = 0.0
                    bucket.append({
                        "auditor_desc": top_desc,
                        "amount": a0f,
                        "amounts": [a0f, a1f],
                    })

        if isinstance(raw_ext.get("revenue"), dict):
            _harvest(raw_ext["revenue"].get("items"))
        if isinstance(raw_ext.get("expenses"), dict):
            cats_node = raw_ext["expenses"].get("categories")
            if isinstance(cats_node, list):
                for grp in cats_node:
                    if isinstance(grp, dict):
                        _harvest(grp.get("items"))
            elif isinstance(cats_node, dict):
                for items in cats_node.values():
                    if isinstance(items, list):
                        _harvest(items)
                    elif isinstance(items, dict):
                        _harvest([items])

        # Find which top_descs feed this summary_label (direct or alias)
        matched_top_descs = []
        for td in cat_lines.keys():
            if td == summary_label or _LABEL_ALIASES.get(td) == summary_label:
                matched_top_descs.append(td)

        # Emit flat_lines with deterministic IDs that match workflow.py:
        # "raw:<top_desc>:<global_idx>" where global_idx is the position
        # within the cumulative flat list across all matched top_descs.
        flat_lines = []
        for td in matched_top_descs:
            base_idx = len(flat_lines)
            for off, sl in enumerate(cat_lines[td]):
                flat_lines.append({
                    "id": "raw:" + td + ":" + str(base_idx + off),
                    "auditor_desc": sl["auditor_desc"],
                    "amount": sl["amount"],
                    "amounts": sl["amounts"],
                })

        block = {"source_lines": flat_lines}
        _audit_recompute_totals(block)
        ovr[summary_label] = block
        _audit_summary_overrides_save(upload, ovr)
        return ovr

    @bp.route("/api/af/uploads/<int:upload_id>/source-line", methods=["PATCH"])
    def api_patch_source_line(upload_id):
        """Edit / move / delete a single source line on a confirmed audit.

        Body: {
          summary_label: "Maintenance",
          line_id: "<uuid>",
          action: "edit"|"move"|"delete",
          new_amount?: <num>          # for edit
          new_amount_y1?: <num>       # for edit (optional, defaults to current)
          new_summary_label?: "..."   # for move
        }
        """
        import uuid as _uuid
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404
        data = request.get_json(silent=True) or {}
        summary_label = (data.get("summary_label") or "").strip()
        line_id = (data.get("line_id") or "").strip()
        action = (data.get("action") or "").strip()
        if not summary_label or not line_id or action not in ("edit", "move", "delete"):
            return jsonify({"success": False, "error": "summary_label, line_id, action required (action in edit/move/delete)"}), 400
        try:
            ovr = _audit_promote_backfill(upload, summary_label)
            block = ovr.get(summary_label) or {}
            sls = block.get("source_lines") or []
            idx = next((i for i, sl in enumerate(sls) if sl.get("id") == line_id), -1)
            if idx < 0:
                return jsonify({"success": False, "error": f"Line id {line_id} not found in '{summary_label}'"}), 404

            if action == "edit":
                if "new_amount" not in data:
                    return jsonify({"success": False, "error": "new_amount required for edit"}), 400
                try:
                    new_amt = float(data["new_amount"])
                except Exception:
                    return jsonify({"success": False, "error": "new_amount must be a number"}), 400
                line = sls[idx]
                old_amts = line.get("amounts") or [line.get("amount") or 0, 0]
                y1 = old_amts[1] if (isinstance(old_amts, list) and len(old_amts) > 1) else 0
                if "new_amount_y1" in data:
                    try:
                        y1 = float(data["new_amount_y1"])
                    except Exception:
                        pass
                line["amount"] = new_amt
                line["amounts"] = [new_amt, y1]
                _audit_recompute_totals(block)

            elif action == "delete":
                sls.pop(idx)
                block["source_lines"] = sls
                _audit_recompute_totals(block)

            elif action == "move":
                target = (data.get("new_summary_label") or "").strip()
                if not target:
                    return jsonify({"success": False, "error": "new_summary_label required for move"}), 400
                if target == summary_label:
                    return jsonify({"success": True, "noop": "source and target identical"})
                # Pop from source, push into target. Promote target if needed.
                line = sls.pop(idx)
                block["source_lines"] = sls
                _audit_recompute_totals(block)
                ovr = _audit_promote_backfill(upload, target)
                # promote may have re-saved; re-read
                ovr = _audit_summary_overrides_get(upload)
                tgt_block = ovr.setdefault(target, {"source_lines": []})
                tgt_block.setdefault("source_lines", []).append(line)
                _audit_recompute_totals(tgt_block)
                # Re-attach the (possibly mutated) source block too
                ovr[summary_label] = block

            _audit_summary_overrides_save(upload, ovr)
            db.session.commit()
            return jsonify({
                "success": True,
                "summary_label": summary_label,
                "category_total": (ovr.get(summary_label) or {}).get("total"),
                "lines": (ovr.get(summary_label) or {}).get("source_lines", []),
            })
        except Exception as e:
            db.session.rollback()
            logger.exception("source-line PATCH failed")
            return jsonify({"success": False, "error": str(e)[:300]}), 500

    @bp.route("/api/af/uploads/<int:upload_id>/source-line", methods=["POST"])
    def api_post_source_line(upload_id):
        """Add a manual source line to a summary row.

        Body: {summary_label, auditor_desc, amount, amount_y1?}
        """
        import uuid as _uuid
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404
        data = request.get_json(silent=True) or {}
        summary_label = (data.get("summary_label") or "").strip()
        desc = (data.get("auditor_desc") or "").strip()
        try:
            amt = float(data.get("amount") or 0)
        except Exception:
            return jsonify({"success": False, "error": "amount must be a number"}), 400
        try:
            amt_y1 = float(data.get("amount_y1") or 0)
        except Exception:
            amt_y1 = 0.0
        if not summary_label or not desc:
            return jsonify({"success": False, "error": "summary_label and auditor_desc required"}), 400
        try:
            _audit_promote_backfill(upload, summary_label)
            ovr = _audit_summary_overrides_get(upload)
            block = ovr.setdefault(summary_label, {"source_lines": []})
            block.setdefault("source_lines", []).append({
                "id": str(_uuid.uuid4()),
                "auditor_desc": desc,
                "amount": amt,
                "amounts": [amt, amt_y1],
                "user_added": True,
            })
            _audit_recompute_totals(block)
            _audit_summary_overrides_save(upload, ovr)
            db.session.commit()
            return jsonify({
                "success": True,
                "summary_label": summary_label,
                "category_total": block.get("total"),
                "lines": block.get("source_lines", []),
            })
        except Exception as e:
            db.session.rollback()
            logger.exception("source-line POST failed")
            return jsonify({"success": False, "error": str(e)[:300]}), 500

    @bp.route("/api/af/uploads/<int:upload_id>/pdf", methods=["GET"])
    def api_serve_audit_pdf(upload_id):
        """Stream the original audit PDF for inline viewing in the browser.
        Used by the review page's "Open PDF" link so FAs can cross-check
        Claude's extraction against the source document.
        """
        upload = AuditUpload.query.get(upload_id)
        if not upload or not upload.pdf_filename:
            abort(404)
        from pathlib import Path as _Path
        pdf_path = _Path(get_data_dir()) / upload.pdf_filename
        if not pdf_path.exists():
            abort(404)
        # Inline display so the browser opens the PDF rather than downloading.
        return send_file(
            str(pdf_path),
            mimetype="application/pdf",
            as_attachment=False,
            download_name=upload.pdf_filename,
        )

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
        "extract_from_pdf": extract_from_pdf,
        "get_data_dir": get_data_dir,
    }

    return bp, models, helpers
