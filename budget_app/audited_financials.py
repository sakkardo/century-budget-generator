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

CENTURY_CATEGORIES = [
    # Income (yrlycomp rows 10-19)
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
    # Expenses (yrlycomp rows 23-36)
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
    # Non-Operating Income (yrlycomp rows 42-49)
    "Capital Assessment",
    "Special Assessment",
    "Interest Income",
    "Insurance Proceeds",
    "Real Estate Tax refund",
    "ICON Settlement Proceeds",
    "SBA - PPP Loan Proceeds",
    # Non-Operating Expense (yrlycomp rows 53-54)
    "Capital Expenses",
    "Cert Fee for Tax Reduction",
]


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


    def fuzzy_match_rule(line_item, rules):
        """
        Find best matching rule for a line item (case-insensitive, whitespace-normalized).
        Returns (rule, confidence) or (None, 0).
        """
        normalized_item = line_item.lower().strip()
        best_match = None
        best_score = 0

        for rule in rules:
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
                rule, confidence = fuzzy_match_rule(description, rules)

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
            for cat_group in extracted["expenses"]["categories"]:
                for item in cat_group.get("items", []):
                    description = item.get("description", "")
                    amounts = item.get("amounts", [])
                    rule, confidence = fuzzy_match_rule(description, rules)

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


    def extract_from_pdf(pdf_path, building_name):
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

            extraction_prompt = f"""
You are analyzing an audited financial statement for {building_name}.

Find the Schedule of Expenses and Schedule of Revenue pages.

Extract all line items and their amounts for all fiscal years present.

Return ONLY valid JSON (no markdown, no code blocks) with this structure:
{{
  "building_name": "{building_name}",
  "fiscal_years": [2024, 2023],
  "revenue": {{
    "items": [
      {{"description": "Maintenance assessments - gross", "amounts": [8760380, 8588595]}},
      {{"description": "Tax benefit credits", "amounts": [100000, 95000]}}
    ],
    "total": [8860380, 8683595]
  }},
  "expenses": {{
    "categories": [
      {{
        "name": "Administrative",
        "items": [
          {{"description": "Accounting and audit fees", "amounts": [22715, 20722]}},
          {{"description": "Legal fees", "amounts": [15000, 14000]}}
        ],
        "total": [37715, 34722]
      }},
      {{
        "name": "Building Operations",
        "items": [
          {{"description": "Payroll", "amounts": [3000000, 2900000]}}
        ],
        "total": [3000000, 2900000]
      }}
    ],
    "total_expenses": [3037715, 2934722]
  }}
}}

Be precise with numbers. Include all line items found.
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
        :root { --blue: #1a56db; --blue-light: #e1effe; --green: #057a55; --green-light: #def7ec; --red: #e02424; --gray-50: #f9fafb; --gray-100: #f3f4f6; --gray-200: #e5e7eb; --gray-300: #d1d5db; --gray-500: #6b7280; --gray-700: #374151; --gray-900: #111827; }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--gray-50); color: var(--gray-900); line-height: 1.5; }
        header { background: linear-gradient(135deg, var(--blue) 0%, #1e429f 100%); color: white; padding: 30px 20px; }
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
        fetch('/api/af/uploads/' + uploadId + '/delete', { method: 'POST' })
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
        :root { --blue: #1a56db; --blue-light: #e1effe; --green: #057a55; --green-light: #def7ec; --red: #e02424; --gray-50: #f9fafb; --gray-100: #f3f4f6; --gray-200: #e5e7eb; --gray-300: #d1d5db; --gray-500: #6b7280; --gray-700: #374151; --gray-900: #111827; }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--gray-50); color: var(--gray-900); line-height: 1.5; }
        header { background: linear-gradient(135deg, var(--blue) 0%, #1e429f 100%); color: white; padding: 30px 20px; }
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
        .profile-card { background: white; border: 1px solid var(--gray-200); border-radius: 12px; padding: 24px; margin-bottom: 20px; }
        .profile-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; }
        .profile-header h3 { font-size: 16px; font-weight: 600; color: var(--gray-900); }
        .profile-meta { font-size: 13px; color: var(--gray-500); margin-top: 2px; }
        .rules-table { width: 100%; border-collapse: collapse; margin-top: 12px; }
        .rules-table th { background: var(--gray-100); padding: 8px 10px; text-align: left; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--gray-500); border-bottom: 1px solid var(--gray-200); }
        .rules-table td { padding: 6px 10px; border-bottom: 1px solid var(--gray-200); }
        .rules-table input, .rules-table select { padding: 6px 8px; font-size: 13px; }
        .btn-row { display: flex; gap: 10px; margin-top: 12px; }
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
            fetch('/api/af/profiles/' + profileId + '/delete', { method: 'POST' })
            .then(r => r.json())
            .then(data => {
                if (data.success) location.reload();
            });
        }

        function addRuleRow(profileId) {
            const container = document.getElementById('rules-' + profileId);
            const newRow = document.createElement('tr');
            newRow.className = 'new-rule';
            newRow.innerHTML = `
                <td><input type="text" placeholder="Auditor line item" /></td>
                <td><input type="text" placeholder="Auditor category" /></td>
                <td>
                    <select>
                        <option value="">-- Select Century Category --</option>
                        {{ century_categories_options }}
                    </select>
                </td>
                <td><input type="number" placeholder="1.0" step="0.01" value="1.0" style="width: 60px;" /></td>
                <td><input type="text" placeholder="Notes" /></td>
            `;
            container.appendChild(newRow);
        }

        function saveRules(profileId) {
            const rows = document.querySelectorAll('#rules-' + profileId + ' tr');
            const rules = [];

            rows.forEach(row => {
                const inputs = row.querySelectorAll('input, select');
                if (inputs[0].value.trim()) {
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
                rules_rows.append(f"""
                    <tr data-rule-id="{r.id}">
                        <td><input type="text" value="{r.auditor_line_item}" /></td>
                        <td><input type="text" value="{r.auditor_category}" /></td>
                        <td>
                            <select>
                                <option value="">-- Select Category --</option>
                                {"".join([f'<option value="{cat}" {"selected" if r.century_category == cat else ""}>{cat}</option>' for cat in CENTURY_CATEGORIES])}
                            </select>
                        </td>
                        <td><input type="number" value="{r.split_pct}" step="0.01" style="width: 60px;" /></td>
                        <td><input type="text" value="{r.notes}" /></td>
                    </tr>
                """)

            rules_table = f"""
                <table class="rules-table">
                    <tr>
                        <th>Auditor Line Item</th>
                        <th>Auditor Category</th>
                        <th>Century Category</th>
                        <th>Split %</th>
                        <th>Notes</th>
                    </tr>
                    {"".join(rules_rows)}
                </table>
                <div class="btn-row">
                    <button class="btn-small" onclick="addRuleRow({p.id})">+ Add Rule</button>
                    <button class="btn-green btn-small" onclick="saveRules({p.id})">Save All Rules</button>
                </div>
            """

            profile_card = f"""
                <div class="profile-card">
                    <div class="profile-header">
                        <div>
                            <h3>{p.name}</h3>
                            <div class="profile-meta">Firm: {p.firm_name}{(' | ' + p.notes) if p.notes else ''}</div>
                        </div>
                        <button class="btn-danger btn-small" onclick="deleteProfile({p.id})">Delete</button>
                    </div>
                    <div id="rules-{p.id}">
                        {rules_table}
                    </div>
                </div>
            """
            profiles_html.append(profile_card)

        century_categories_options = "\n".join([
            f'<option value="{cat}">{cat}</option>'
            for cat in CENTURY_CATEGORIES
        ])

        html = html.replace("{{ profiles_list }}", "\n".join(profiles_html) if profiles_html else "<p>No profiles created yet.</p>")
        html = html.replace("{{ century_categories_options }}", century_categories_options)

        return render_template_string(html)


    @bp.route("/audited-financials/review/<int:upload_id>", methods=["GET"])
    def review_page(upload_id):
        """Review and confirm extraction for an upload."""
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return "Upload not found", 404

        try:
            raw_extraction = json.loads(upload.raw_extraction) if upload.raw_extraction else {}
            mapped_data = json.loads(upload.mapped_data) if upload.mapped_data else {}
        except:
            raw_extraction = {}
            mapped_data = {}

        # Find unmapped items and build existing rules lookup
        unmapped = []
        existing_rules = {}
        if upload.profile:
            for rule in upload.profile.rules:
                existing_rules[rule.auditor_line_item.lower().strip()] = rule.century_category
            if upload.status in ["mapped", "confirmed"]:
                _, unmapped = apply_mapping_rules(upload.raw_extraction, upload.profile.id)

        html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Review - {{ building_name }} - Century Management</title>
    <style>
        :root { --blue: #1a56db; --blue-light: #e1effe; --green: #057a55; --green-light: #def7ec; --red: #e02424; --gray-50: #f9fafb; --gray-100: #f3f4f6; --gray-200: #e5e7eb; --gray-300: #d1d5db; --gray-500: #6b7280; --gray-700: #374151; --gray-900: #111827; }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--gray-50); color: var(--gray-900); line-height: 1.5; }
        header { background: linear-gradient(135deg, var(--blue) 0%, #1e429f 100%); color: white; padding: 30px 20px; }
        header a { color: white; text-decoration: none; font-size: 14px; }
        header a:hover { text-decoration: underline; }
        header h1 { font-size: 24px; font-weight: 700; }
        header p { font-size: 14px; opacity: 0.85; margin-top: 4px; }
        .container { max-width: 1400px; margin: 0 auto; padding: 24px 20px; }
        .columns { display: grid; grid-template-columns: 2fr 1fr; gap: 24px; }
        .column { background: white; border-radius: 12px; padding: 24px; border: 1px solid var(--gray-200); }
        .column h3 { font-size: 16px; font-weight: 600; color: var(--blue); margin-bottom: 16px; padding-bottom: 10px; border-bottom: 2px solid var(--blue-light); }
        button { background: var(--blue); color: white; border: none; padding: 10px 20px; border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.15s; }
        button:hover { background: #1542b8; }
        .btn-green { background: var(--green); }
        .btn-green:hover { background: #046c4e; }
        .unmapped { background: #fde8e8; color: #9b1c1c; padding: 10px; margin: 10px 0; border-radius: 6px; font-size: 13px; }
        .success { background: var(--green-light); color: #065f46; padding: 10px; margin: 10px 0; border-radius: 6px; font-size: 13px; }
        table { width: 100%; border-collapse: collapse; }
        table th { background: var(--gray-100); padding: 8px 10px; text-align: left; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--gray-500); border-bottom: 1px solid var(--gray-200); }
        table td { padding: 8px 10px; border-bottom: 1px solid var(--gray-200); font-size: 13px; }
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
        <p>Review the data above and confirm to save as official actuals for this building/year.</p>
        <button class="btn-green" onclick="confirmExtraction({{ upload_id }})">Confirm & Save</button>
        <div id="confirmStatus"></div>
    </div>
</div>

    <script>
        const rawExtraction = {{ raw_json }};
        const mappedData = {{ mapped_json }};
        const unmappedItems = {{ unmapped_json }};
        const centuryCategories = {{ century_categories_json }};
        const existingRules = {{ existing_rules_json }};
        const profileId = {{ profile_id }};
        let itemIndex = 0;

        function formatAmount(n) {
            if (n === null || n === undefined) return '—';
            return n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
        }

        function makeDropdown(description) {
            const id = 'map_' + itemIndex++;
            const normalized = description.toLowerCase().trim();
            const currentMapping = existingRules[normalized] || '';
            const mapped = currentMapping ? ' style="background:#d4edda;"' : '';

            let html = '<select id="' + id + '" data-desc="' + description.replace(/"/g, '&quot;') + '"' + mapped + ' style="width:100%; padding:3px; font-size:12px; border:1px solid #ccc; border-radius:3px;">';
            html += '<option value="">— unmapped —</option>';
            for (let cat of centuryCategories) {
                const sel = (cat === currentMapping) ? ' selected' : '';
                html += '<option value="' + cat + '"' + sel + '>' + cat + '</option>';
            }
            html += '</select>';
            return html;
        }

        function renderRawData() {
            const container = document.getElementById('rawData');
            const years = rawExtraction.fiscal_years || [];
            let html = '';

            if (years.length > 0) {
                html += '<div style="background:#e8f0fe; padding:8px 12px; border-radius:4px; margin-bottom:12px; font-weight:bold;">Fiscal Years: ' + years.join(', ') + '</div>';
            }

            if (rawExtraction.revenue && rawExtraction.revenue.items) {
                html += '<h5 style="margin:15px 0 5px;">Revenue</h5>';
                html += '<table style="width:100%; font-size:13px; border-collapse:collapse;"><tr><th style="text-align:left; padding:6px;">Line Item</th>';
                for (let y of years) { html += '<th style="text-align:right; padding:6px; width:90px;">' + y + '</th>'; }
                html += '<th style="text-align:left; padding:6px; width:180px;">Map To</th></tr>';
                for (let item of rawExtraction.revenue.items) {
                    html += '<tr style="border-bottom:1px solid #eee;"><td style="padding:6px;">' + item.description + '</td>';
                    for (let a of item.amounts) { html += '<td style="text-align:right; padding:6px;">' + formatAmount(a) + '</td>'; }
                    html += '<td style="padding:4px;">' + makeDropdown(item.description) + '</td></tr>';
                }
                if (rawExtraction.revenue.total) {
                    html += '<tr style="font-weight:bold; border-top:2px solid #333;"><td style="padding:6px;">Total Revenue</td>';
                    for (let a of rawExtraction.revenue.total) { html += '<td style="text-align:right; padding:6px;">' + formatAmount(a) + '</td>'; }
                    html += '<td></td></tr>';
                }
                html += '</table>';
            }

            if (rawExtraction.expenses && rawExtraction.expenses.categories) {
                html += '<h5 style="margin:15px 0 5px;">Expenses</h5>';
                html += '<table style="width:100%; font-size:13px; border-collapse:collapse;"><tr><th style="text-align:left; padding:6px;">Line Item</th>';
                for (let y of years) { html += '<th style="text-align:right; padding:6px; width:90px;">' + y + '</th>'; }
                html += '<th style="text-align:left; padding:6px; width:180px;">Map To</th></tr>';
                for (let cat of rawExtraction.expenses.categories) {
                    html += '<tr><td colspan="' + (years.length + 2) + '" style="font-weight:bold; background:#f0f0f0; padding:8px 6px;">' + cat.name + '</td></tr>';
                    for (let item of cat.items) {
                        html += '<tr style="border-bottom:1px solid #eee;"><td style="padding:6px 6px 6px 20px;">' + item.description + '</td>';
                        for (let a of item.amounts) { html += '<td style="text-align:right; padding:6px;">' + formatAmount(a) + '</td>'; }
                        html += '<td style="padding:4px;">' + makeDropdown(item.description) + '</td></tr>';
                    }
                    if (cat.total) {
                        html += '<tr style="font-weight:bold; border-bottom:2px solid #ddd;"><td style="padding:6px 6px 6px 20px;">Subtotal</td>';
                        for (let a of cat.total) { html += '<td style="text-align:right; padding:6px;">' + formatAmount(a) + '</td>'; }
                        html += '<td></td></tr>';
                    }
                }
                if (rawExtraction.expenses.total_expenses) {
                    html += '<tr style="font-weight:bold; border-top:2px solid #333;"><td style="padding:6px;">Total Expenses</td>';
                    for (let a of rawExtraction.expenses.total_expenses) { html += '<td style="text-align:right; padding:6px;">' + formatAmount(a) + '</td>'; }
                    html += '<td></td></tr>';
                }
                html += '</table>';
            }

            container.innerHTML = html;
        }

        function renderMappedData() {
            const container = document.getElementById('mappedData');
            const years = rawExtraction.fiscal_years || [];
            let html = '<table style="font-size:13px;"><tr><th style="text-align:left;">Category</th>';
            for (let y of years) { html += '<th style="text-align:right;">' + y + '</th>'; }
            html += '</tr>';

            let hasData = false;
            for (let cat in mappedData) {
                const data = mappedData[cat];
                const yearAmounts = data.year_totals || [];
                const total = data.total || 0;
                if (total !== 0) {
                    hasData = true;
                    html += '<tr><td>' + cat + '</td>';
                    if (yearAmounts.length > 0) {
                        for (let a of yearAmounts) { html += '<td style="text-align:right;">' + formatAmount(a) + '</td>'; }
                    } else {
                        html += '<td style="text-align:right;">' + formatAmount(total) + '</td>';
                    }
                    html += '</tr>';
                }
            }

            if (!hasData) {
                html += '<tr><td colspan="' + (years.length + 1) + '" style="text-align:center; color:#999; padding:20px;">Map items on the left, then click "Save All Mappings"</td></tr>';
            }

            html += '</table>';
            container.innerHTML = html;
        }

        function renderReconciliation() {
            const container = document.getElementById('reconciliation');
            let html = '<div style="background:#f8f9fa; padding:12px; border-radius:5px; font-size:13px;">';
            html += '<strong>Totals:</strong><br/>';
            if (rawExtraction.revenue && rawExtraction.revenue.total) {
                html += 'Revenue: $' + formatAmount(rawExtraction.revenue.total[0]) + '<br/>';
            }
            if (rawExtraction.expenses && rawExtraction.expenses.total_expenses) {
                html += 'Expenses: $' + formatAmount(rawExtraction.expenses.total_expenses[0]) + '<br/>';
            }

            // Count unmapped
            const allSelects = document.querySelectorAll('select[id^="map_"]');
            let unmappedCount = 0;
            allSelects.forEach(s => { if (!s.value) unmappedCount++; });
            if (unmappedCount > 0) {
                html += '<div class="unmapped" style="margin-top:8px;">' + unmappedCount + ' items still unmapped</div>';
            } else if (allSelects.length > 0) {
                html += '<div class="success" style="margin-top:8px;">All items mapped!</div>';
            }
            html += '</div>';
            container.innerHTML = html;
        }

        function saveAllRules() {
            if (!profileId) { alert('No auditor profile assigned'); return; }

            const selects = document.querySelectorAll('select[id^="map_"]');
            const rules = [];
            selects.forEach(s => {
                if (s.value) {
                    rules.push({
                        auditor_line_item: s.dataset.desc,
                        century_category: s.value,
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

        function confirmExtraction(uploadId) {
            fetch('/api/af/confirm/' + uploadId, { method: 'POST' })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    document.getElementById('confirmStatus').innerHTML = '<div class="success">Extraction confirmed and saved!</div>';
                    setTimeout(() => window.location.href = '/audited-financials', 1500);
                } else {
                    document.getElementById('confirmStatus').innerHTML = '<div class="unmapped">Error: ' + data.error + '</div>';
                }
            });
        }

        renderRawData();
        renderMappedData();
        renderReconciliation();
    </script>
</body>
</html>
        """

        html = html.replace("{{ building_name }}", upload.building_name)
        html = html.replace("{{ entity_code }}", upload.entity_code)
        html = html.replace("{{ fiscal_year }}", upload.fiscal_year_end or "")
        html = html.replace("{{ upload_id }}", str(upload_id))
        html = html.replace("{{ raw_json }}", json.dumps(raw_extraction))
        html = html.replace("{{ mapped_json }}", json.dumps(mapped_data))
        html = html.replace("{{ unmapped_json }}", json.dumps(unmapped))
        html = html.replace("{{ century_categories_json }}", json.dumps(CENTURY_CATEGORIES))
        html = html.replace("{{ existing_rules_json }}", json.dumps(existing_rules))
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


    @bp.route("/api/af/profiles/<int:profile_id>/delete", methods=["POST"])
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

        # Single rule add (from review page)
        if "auditor_line_item" in data:
            rule = MappingRule(
                profile_id=profile_id,
                auditor_line_item=data.get("auditor_line_item"),
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

            # Extract from PDF
            extracted = extract_from_pdf(str(pdf_path), upload.building_name)
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
        """Mark extraction as confirmed."""
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404

        try:
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

    @bp.route("/api/af/uploads/<int:upload_id>/delete", methods=["POST"])
    def api_delete_upload(upload_id):
        """Delete an upload."""
        upload = AuditUpload.query.get(upload_id)
        if not upload:
            return jsonify({"success": False, "error": "Upload not found"}), 404

        # Delete the PDF file if it exists
        if upload.file_path:
            try:
                import os
                if os.path.exists(upload.file_path):
                    os.remove(upload.file_path)
            except Exception:
                pass  # File cleanup is best-effort

        db.session.delete(upload)
        db.session.commit()
        return jsonify({"success": True})

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
