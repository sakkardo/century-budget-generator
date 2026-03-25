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
    # Non-Operating
    "Capital Assessment",
    "Special Assessment",
    "Interest Income",
    "Insurance Proceeds",
    "Real Estate Tax refund",
    "Capital Expenses",
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

        # Initialize mapped categories
        mapped = {cat: {"total": 0, "years": []} for cat in CENTURY_CATEGORIES}

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
                    for amount in amounts:
                        if isinstance(amount, (int, float)):
                            mapped[cat]["total"] += amount * pct
                            mapped[cat]["years"].append(amount * pct)
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
                        for amount in amounts:
                            if isinstance(amount, (int, float)):
                                mapped[cat]["total"] += amount * pct
                                mapped[cat]["years"].append(amount * pct)
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
                if isinstance(data, dict) and "years" in data and len(data["years"]) > 0:
                    result[cat] = data["years"][0]
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
<html>
<head>
    <title>Audited Financials</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; }
        .upload-section { background: #f9f9f9; padding: 20px; margin: 20px 0; border-radius: 5px; }
        .form-group { margin: 10px 0; }
        label { display: block; font-weight: bold; margin-bottom: 5px; }
        input, select { padding: 8px; margin-right: 10px; }
        button { padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 3px; cursor: pointer; }
        button:hover { background: #0056b3; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
        .status-uploaded { color: #ff9800; }
        .status-extracted { color: #2196f3; }
        .status-mapped { color: #4caf50; }
        .status-confirmed { color: #009688; }
        .actions { white-space: nowrap; }
        .btn-small { padding: 5px 10px; font-size: 12px; margin-right: 5px; }
        .link { color: #007bff; cursor: pointer; text-decoration: underline; }
        .alert { padding: 10px; background: #fff3cd; color: #856404; margin: 10px 0; border-radius: 3px; }
    </style>
</head>
<body>
    <div style="padding:8px 20px;"><a href="/" style="color:#1a56db; text-decoration:none; font-size:14px;">← Home</a></div>
    <div class="header">
        <h1>Audited Financials</h1>
        <a href="/audited-financials/profiles" style="color: #007bff;">Manage Profiles & Rules</a>
    </div>

    <div class="upload-section">
        <h3>Upload New Audit PDF</h3>
        <div class="form-group">
            <label>Building:</label>
            <select id="entityCode">
                <option value="">-- Select Building --</option>
                {{ buildings_options }}
            </select>
        </div>
        <div class="form-group">
            <label>Auditor Profile:</label>
            <select id="profileId">
                <option value="">-- Select Auditor --</option>
                {{ profiles_options }}
            </select>
        </div>
        <div class="form-group">
            <label>Fiscal Year End:</label>
            <input type="text" id="fiscalYearEnd" placeholder="2024" />
        </div>
        <div class="form-group">
            <label>PDF File:</label>
            <input type="file" id="pdfFile" accept=".pdf" />
        </div>
        <button onclick="uploadPDF()">Upload & Extract</button>
        <div id="uploadStatus"></div>
    </div>

    <h3>All Uploads</h3>
    <div id="uploadsTable">
        {{ uploads_table }}
    </div>

    <script>
        function uploadPDF() {
            const entityCode = document.getElementById('entityCode').value;
            const profileId = document.getElementById('profileId').value;
            const fiscalYearEnd = document.getElementById('fiscalYearEnd').value;
            const pdfFile = document.getElementById('pdfFile').files[0];

            if (!entityCode || !pdfFile) {
                document.getElementById('uploadStatus').innerHTML = '<div class="alert">Select building and PDF file</div>';
                return;
            }

            const formData = new FormData();
            formData.append('entity_code', entityCode);
            formData.append('profile_id', profileId || '');
            formData.append('fiscal_year_end', fiscalYearEnd);
            formData.append('pdf', pdfFile);

            document.getElementById('uploadStatus').innerHTML = '<div class="alert">Uploading...</div>';

            fetch('/api/af/upload', {
                method: 'POST',
                body: formData
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    document.getElementById('uploadStatus').innerHTML = '<div class="alert" style="background: #d4edda; color: #155724;">Upload successful. Extracting...</div>';
                    // Auto-extract
                    extractUpload(data.upload_id);
                } else {
                    document.getElementById('uploadStatus').innerHTML = '<div class="alert">Error: ' + data.error + '</div>';
                }
            })
            .catch(err => {
                document.getElementById('uploadStatus').innerHTML = '<div class="alert">Error: ' + err.message + '</div>';
            });
        }

        function extractUpload(uploadId) {
            fetch('/api/af/extract/' + uploadId, { method: 'POST' })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    document.getElementById('uploadStatus').innerHTML += '<div class="alert" style="background: #d4edda; color: #155724;">Extraction complete. Applying mapping rules...</div>';
                    mapUpload(uploadId);
                } else {
                    document.getElementById('uploadStatus').innerHTML += '<div class="alert">Extraction error: ' + data.error + '</div>';
                }
            });
        }

        function mapUpload(uploadId) {
            fetch('/api/af/map/' + uploadId, { method: 'POST' })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    document.getElementById('uploadStatus').innerHTML += '<div class="alert" style="background: #d4edda; color: #155724;">Mapping complete. Reload page to see updates.</div>';
                    setTimeout(() => location.reload(), 2000);
                } else {
                    document.getElementById('uploadStatus').innerHTML += '<div class="alert">Mapping error: ' + data.error + '</div>';
                }
            });
        }

        function reviewUpload(uploadId) {
            window.location.href = '/audited-financials/review/' + uploadId;
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
            rows.append(f"""
                <tr>
                    <td>{u.entity_code}</td>
                    <td>{u.building_name}</td>
                    <td>{u.profile.name if u.profile else "—"}</td>
                    <td>{u.fiscal_year_end}</td>
                    <td><span class="status-{u.status}">{u.status}</span></td>
                    <td class="actions">
                        <button class="btn-small" onclick="reviewUpload({u.id})">Review</button>
                    </td>
                </tr>
            """)
        uploads_table = f"""
            <table>
                <tr>
                    <th>Entity Code</th>
                    <th>Building</th>
                    <th>Auditor</th>
                    <th>Year</th>
                    <th>Status</th>
                    <th>Actions</th>
                </tr>
                {"".join(rows) if rows else "<tr><td colspan='6' style='text-align: center;'>No uploads yet</td></tr>"}
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
<html>
<head>
    <title>Auditor Profiles</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; }
        .section { background: #f9f9f9; padding: 20px; margin: 20px 0; border-radius: 5px; }
        .form-group { margin: 10px 0; }
        label { display: block; font-weight: bold; margin-bottom: 5px; }
        input, select, textarea { padding: 8px; width: 100%; max-width: 400px; }
        button { padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 3px; cursor: pointer; }
        button:hover { background: #0056b3; }
        button.danger { background: #dc3545; }
        button.danger:hover { background: #c82333; }
        .profile-card { border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; background: white; }
        .profile-header { display: flex; justify-content: space-between; align-items: center; }
        .profile-actions { display: flex; gap: 10px; }
        .rules-table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        .rules-table th, .rules-table td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
        .rules-table th { background-color: #f2f2f2; }
        .rules-table input { width: 100%; padding: 4px; }
        .add-rule-btn { margin-top: 10px; }
        textarea { height: 60px; }
        .alert { padding: 10px; background: #d4edda; color: #155724; margin: 10px 0; border-radius: 3px; }
    </style>
</head>
<body>
    <div style="padding:8px 20px;"><a href="/" style="color:#1a56db; text-decoration:none; font-size:14px;">← Home</a></div>
    <div class="header">
        <h1>Auditor Profiles & Mapping Rules</h1>
        <a href="/audited-financials" style="color: #007bff;">Back to Uploads</a>
    </div>

    <div class="section">
        <h3>Create New Auditor Profile</h3>
        <div class="form-group">
            <label>Display Name:</label>
            <input type="text" id="newProfileName" />
        </div>
        <div class="form-group">
            <label>Firm Name (e.g., "Marks Paneth LLP"):</label>
            <input type="text" id="newFirmName" />
        </div>
        <div class="form-group">
            <label>Notes:</label>
            <textarea id="newProfileNotes"></textarea>
        </div>
        <button onclick="createProfile()">Create Profile</button>
        <div id="createStatus"></div>
    </div>

    <h3>Existing Profiles</h3>
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
            fetch('/api/af/profiles/' + profileId, { method: 'DELETE' })
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
                <button class="add-rule-btn" onclick="addRuleRow({p.id})">+ Add Rule</button>
                <button style="background: #28a745;" onclick="saveRules({p.id})">Save All Rules</button>
            """

            profile_card = f"""
                <div class="profile-card">
                    <div class="profile-header">
                        <div>
                            <h4>{p.name}</h4>
                            <p style="margin: 0; color: #666;">Firm: {p.firm_name}</p>
                            <p style="margin: 0; color: #666;">Notes: {p.notes}</p>
                        </div>
                        <div class="profile-actions">
                            <button class="danger" onclick="deleteProfile({p.id})">Delete</button>
                        </div>
                    </div>
                    <h5>Mapping Rules</h5>
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

        # Find unmapped items from mapping
        unmapped = []
        if upload.status in ["mapped", "confirmed"]:
            # Recompute to show unmapped
            profile = upload.profile
            if profile:
                _, unmapped = apply_mapping_rules(upload.raw_extraction, profile.id)

        html = """
<!DOCTYPE html>
<html>
<head>
    <title>Review Extraction - {{ building_name }}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { display: flex; justify-content: space-between; }
        .columns { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-top: 20px; }
        .column { border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
        h4 { border-bottom: 2px solid #007bff; padding-bottom: 10px; }
        .item { padding: 8px; border-bottom: 1px solid #eee; }
        .amount { text-align: right; font-weight: bold; }
        .unmapped { background: #ffebee; color: #c62828; padding: 10px; margin: 10px 0; border-radius: 3px; }
        .success { background: #d4edda; color: #155724; padding: 10px; margin: 10px 0; border-radius: 3px; }
        button { padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 3px; cursor: pointer; margin: 10px 5px 10px 0; }
        button:hover { background: #0056b3; }
        button.success-btn { background: #28a745; }
        button.success-btn:hover { background: #218838; }
        .back-link { color: #007bff; text-decoration: none; }
        .reconciliation { background: #f9f9f9; padding: 15px; margin-top: 20px; border-radius: 5px; }
        table { width: 100%; border-collapse: collapse; }
        table th, table td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
        table th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <div style="padding:8px 20px;"><a href="/" style="color:#1a56db; text-decoration:none; font-size:14px;">← Home</a></div>
    <div class="header">
        <h1>Review Extraction</h1>
        <a href="/audited-financials" class="back-link">Back to Uploads</a>
    </div>
    <p>Building: <strong>{{ building_name }}</strong> | Entity: {{ entity_code }} | Year: {{ fiscal_year }}</p>

    <div class="columns">
        <!-- Raw Extracted Data -->
        <div class="column">
            <h4>Raw Extracted Data</h4>
            <div id="rawData"></div>
        </div>

        <!-- Mapped Categories -->
        <div class="column">
            <h4>Mapped to Century Categories</h4>
            <div id="mappedData"></div>
        </div>

        <!-- Reconciliation -->
        <div class="column">
            <h4>Reconciliation</h4>
            <div id="reconciliation"></div>
        </div>
    </div>

    <div style="margin-top: 30px;">
        <h3>Confirm Extraction</h3>
        <p>Review the data above. Click Confirm to save this extraction as the official actuals for this building/year.</p>
        <button class="success-btn" onclick="confirmExtraction({{ upload_id }})">Confirm & Save</button>
        <div id="confirmStatus"></div>
    </div>

    <script>
        const rawExtraction = {{ raw_json }};
        const mappedData = {{ mapped_json }};
        const unmappedItems = {{ unmapped_json }};

        function formatAmount(n) {
            return n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
        }

        function renderRawData() {
            const container = document.getElementById('rawData');
            let html = '';

            if (rawExtraction.revenue && rawExtraction.revenue.items) {
                html += '<h5>Revenue</h5>';
                for (let item of rawExtraction.revenue.items) {
                    html += `<div class="item">
                        <div>${item.description}</div>
                        <div class="amount">${item.amounts.map(a => formatAmount(a)).join(' / ')}</div>
                    </div>`;
                }
            }

            if (rawExtraction.expenses && rawExtraction.expenses.categories) {
                html += '<h5>Expenses</h5>';
                for (let cat of rawExtraction.expenses.categories) {
                    html += `<div style="font-weight: bold; margin-top: 10px;">${cat.name}</div>`;
                    for (let item of cat.items) {
                        html += `<div class="item" style="margin-left: 10px;">
                            <div>${item.description}</div>
                            <div class="amount">${item.amounts.map(a => formatAmount(a)).join(' / ')}</div>
                        </div>`;
                    }
                }
            }

            container.innerHTML = html;
        }

        function renderMappedData() {
            const container = document.getElementById('mappedData');
            let html = '<table><tr><th>Century Category</th><th>Amount</th></tr>';

            for (let cat in mappedData) {
                const data = mappedData[cat];
                const amount = data.total || 0;
                if (amount !== 0) {
                    html += `<tr><td>${cat}</td><td class="amount">${formatAmount(amount)}</td></tr>`;
                }
            }

            html += '</table>';
            container.innerHTML = html;
        }

        function renderReconciliation() {
            const container = document.getElementById('reconciliation');
            let html = '';

            if (unmappedItems.length > 0) {
                html += '<div class="unmapped"><strong>Unmapped Items Found:</strong><br/>';
                for (let item of unmappedItems) {
                    html += `<div style="margin: 5px 0; font-size: 12px;">• ${item.description}</div>`;
                }
                html += '</div>';
            } else {
                html += '<div class="success">All items mapped successfully</div>';
            }

            html += '<p style="margin-top: 20px;"><strong>Total Extracted:</strong><br/>';
            if (rawExtraction.revenue && rawExtraction.revenue.total) {
                html += `Revenue: ${formatAmount(rawExtraction.revenue.total[0])}<br/>`;
            }
            if (rawExtraction.expenses && rawExtraction.expenses.total_expenses) {
                html += `Expenses: ${formatAmount(rawExtraction.expenses.total_expenses[0])}<br/>`;
            }
            html += '</p>';

            container.innerHTML = html;
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
        html = html.replace("{{ fiscal_year }}", upload.fiscal_year_end)
        html = html.replace("{{ upload_id }}", str(upload_id))
        html = html.replace("{{ raw_json }}", json.dumps(raw_extraction))
        html = html.replace("{{ mapped_json }}", json.dumps(mapped_data))
        html = html.replace("{{ unmapped_json }}", json.dumps(unmapped))

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
        """Save/update mapping rules for a profile."""
        profile = AuditorProfile.query.get(profile_id)
        if not profile:
            return jsonify({"success": False, "error": "Profile not found"}), 404

        data = request.get_json()
        rules_data = data.get("rules", [])

        # Delete existing rules not in the new list
        existing_ids = {r.id for r in profile.rules}
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
            upload.confirmed_by = request.get_json().get("confirmed_by", "system")
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
