"""
Century Management Unified Budget & Assumptions System
Run: python app.py  |  Open: http://localhost:5000
"""
import os
import sys
import csv
import json
import shutil
import logging
import tempfile
import zipfile
from pathlib import Path
from io import BytesIO
from flask import Flask, render_template_string, request, jsonify, send_file, make_response
from flask_sqlalchemy import SQLAlchemy
from functools import wraps

# Detect cloud deployment (Railway sets PORT env var)
IS_CLOUD = "PORT" in os.environ or "RAILWAY_ENVIRONMENT" in os.environ

# Add budget_system to path for pipeline imports
BUDGET_SYSTEM = Path(__file__).parent.parent / "budget_system"
sys.path.insert(0, str(BUDGET_SYSTEM))

# Add budget_app to path so workflow.py can import sibling modules (e.g. dof_taxes)
BUDGET_APP = Path(__file__).parent
sys.path.insert(0, str(BUDGET_APP))

from ysl_parser import parse_ysl_file
from template_populator import populate_template, apply_assumptions, apply_pm_projections

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ─── Database Configuration ──────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///" + str(Path(__file__).parent / "data" / "budget.db")

app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "century-budget-dev-key")

db = SQLAlchemy(app)

# Register workflow blueprint (PM review, admin, dashboard)
try:
    from workflow import create_workflow_blueprint
except ImportError:
    from budget_app.workflow import create_workflow_blueprint
workflow_bp, workflow_models, workflow_helpers = create_workflow_blueprint(db)
app.register_blueprint(workflow_bp)

# Register audited financials blueprint
try:
    from audited_financials import create_audited_financials_blueprint
except ImportError:
    from budget_app.audited_financials import create_audited_financials_blueprint
af_bp, af_models, af_helpers = create_audited_financials_blueprint(db)
app.register_blueprint(af_bp)

# Register expense distribution blueprint
try:
    from expense_distribution import create_expense_distribution_blueprint
except ImportError:
    from budget_app.expense_distribution import create_expense_distribution_blueprint
ed_bp, ed_models, ed_helpers = create_expense_distribution_blueprint(db, workflow_models)
app.register_blueprint(ed_bp)

# Register maintenance proof blueprint
try:
    from maintenance_proof import create_maintenance_proof_blueprint
except ImportError:
    from budget_app.maintenance_proof import create_maintenance_proof_blueprint
mp_bp, mp_models, mp_helpers = create_maintenance_proof_blueprint(db, workflow_models)
app.register_blueprint(mp_bp)

# Register open AP (aging) blueprint
try:
    from open_ap import create_open_ap_blueprint, detect_open_ap_file, parse_open_ap_report
except ImportError:
    from budget_app.open_ap import create_open_ap_blueprint, detect_open_ap_file, parse_open_ap_report
oa_bp, oa_models, oa_helpers = create_open_ap_blueprint(db, workflow_models)
app.register_blueprint(oa_bp)

# Register file repository blueprint
try:
    from file_repository import create_file_repository_blueprint
except ImportError:
    from budget_app.file_repository import create_file_repository_blueprint
fr_bp, fr_models, fr_helpers = create_file_repository_blueprint(db, workflow_models)
app.register_blueprint(fr_bp)

# Ensure every request starts with a clean DB session.
# Prevents poisoned PostgreSQL transactions from leaking via connection pool.
@app.before_request
def _ensure_clean_db_session():
    try:
        db.session.rollback()
    except Exception:
        pass

# Resolve all model relationships after ALL blueprints are registered
try:
    db.configure_mappers()
    logger.info("Model mappers configured successfully")
except Exception as e:
    logger.warning(f"configure_mappers() failed: {e} — will retry on first request")

# Create all tables on startup + migrate missing columns
with app.app_context():
    db.create_all()
    logger.info("Database tables initialized")

    # Auto-migrate: add columns that exist in models but not in the DB
    if IS_CLOUD:
        _migrations = [
            ("budgets", "initiated_by", "INTEGER"),
            ("budgets", "initiated_at", "TIMESTAMP"),
            ("budgets", "return_to_status", "VARCHAR(20)"),
            ("budgets", "presentation_token", "VARCHAR(64)"),
            ("budgets", "approved_by", "INTEGER"),
            ("budgets", "approved_at", "TIMESTAMP"),
            ("budgets", "increase_pct", "FLOAT"),
            ("budgets", "effective_date", "VARCHAR(20)"),
            ("budgets", "ar_notes", "TEXT DEFAULT ''"),
            ("budget_lines", "sheet_name", "VARCHAR(50) DEFAULT ''"),
            ("budget_lines", "pm_editable", "BOOLEAN DEFAULT FALSE"),
            ("budget_lines", "reclass_to_gl", "VARCHAR(50)"),
            ("budget_lines", "reclass_amount", "FLOAT DEFAULT 0"),
            ("budget_lines", "reclass_notes", "TEXT DEFAULT ''"),
            ("budget_lines", "proposed_budget", "FLOAT DEFAULT 0"),
            ("budgets", "assumptions_json", "TEXT DEFAULT '{}'"),
            ("budget_lines", "estimate_override", "FLOAT"),
            ("budget_lines", "forecast_override", "FLOAT"),
            ("budget_lines", "accrual_adj", "FLOAT DEFAULT 0"),
            ("budgets", "building_type", "VARCHAR(50) DEFAULT ''"),
            ("budget_lines", "proposed_formula", "TEXT"),
            ("budgets", "version", "INTEGER DEFAULT 1"),
        ]
        for table, col, col_type in _migrations:
            try:
                db.session.execute(db.text(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}"))
                db.session.commit()
                logger.info(f"Added column {table}.{col}")
            except Exception:
                db.session.rollback()  # Column already exists, skip

        # Migrate unique constraint: drop old, add new with version
        try:
            db.session.execute(db.text("ALTER TABLE budgets DROP CONSTRAINT IF EXISTS uq_entity_year"))
            db.session.commit()
            logger.info("Dropped old uq_entity_year constraint")
        except Exception:
            db.session.rollback()
        try:
            db.session.execute(db.text("ALTER TABLE budgets ADD CONSTRAINT uq_entity_year_ver UNIQUE (entity_code, year, version)"))
            db.session.commit()
            logger.info("Added new uq_entity_year_ver constraint")
        except Exception:
            db.session.rollback()

        # Backfill building_type on existing budgets from buildings.csv
        try:
            import csv as _csv
            _csv_path = BUDGET_SYSTEM / "buildings.csv"
            _type_map = {}
            if _csv_path.exists():
                with open(_csv_path, newline="", encoding="utf-8") as _f:
                    for _r in _csv.DictReader(_f):
                        ec = str(_r.get("entity_code", "")).strip()
                        bt = (_r.get("type", "") or "").strip()
                        if ec and bt:
                            _type_map[ec] = bt
            empty_bt = db.session.execute(
                db.text("SELECT id, entity_code FROM budgets WHERE building_type IS NULL OR building_type = ''")
            ).fetchall()
            if empty_bt and _type_map:
                for row in empty_bt:
                    bt = _type_map.get(str(row[1]).strip(), "")
                    if bt:
                        db.session.execute(
                            db.text("UPDATE budgets SET building_type = :bt WHERE id = :id"),
                            {"bt": bt, "id": row[0]}
                        )
                db.session.commit()
                logger.info(f"Backfilled building_type on {len(empty_bt)} budgets")
        except Exception as e:
            db.session.rollback()
            logger.warning(f"Building type backfill skipped: {e}")

# Health check for Railway
@app.route("/healthz")
def healthz():
    return "ok", 200

# Paths
TEMPLATE_PATH = BUDGET_SYSTEM / "Budget_Final_Template_v2.xlsx"
BUILDINGS_CSV = BUDGET_SYSTEM / "buildings.csv"
SETTINGS_FILE = Path(__file__).parent / "settings.json"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Data files
PORTFOLIO_DEFAULTS_FILE = DATA_DIR / "portfolio_defaults.json"
BUILDING_ASSUMPTIONS_FILE = DATA_DIR / "building_assumptions.json"

# Default save location
DEFAULT_SAVE_DIR = str(BUDGET_SYSTEM / "budgets")

# Console JS script templates — entities/email/period get injected
# Normalize CRLF→LF so string replace patches work on all platforms
CONSOLE_SCRIPT = (BUDGET_SYSTEM / "YSL Budget Script.js").read_text(encoding="utf-8").replace("\r\n", "\n")
EXPENSE_DIST_SCRIPT = (BUDGET_SYSTEM / "Expense Distribution Script.js").read_text(encoding="utf-8").replace("\r\n", "\n")
MAINT_PROOF_SCRIPT = (BUDGET_SYSTEM / "Maintenance Proof Script.js").read_text(encoding="utf-8").replace("\r\n", "\n")
try:
    AP_AGING_SCRIPT = (BUDGET_SYSTEM / "AP Aging Script.js").read_text(encoding="utf-8").replace("\r\n", "\n")
except FileNotFoundError:
    AP_AGING_SCRIPT = None

# Default portfolio values
DEFAULT_PORTFOLIO = {
    "payroll_tax": {
        "FICA": 0.0765,
        "SUI": 0.0525,
        "FUI": 0.006,
        "MTA": 0.0034,
        "NYS_Disability": 0.005,
        "PFL": 0.00455
    },
    "union_benefits": {
        "welfare_monthly": 1072.55,
        "pension_weekly": 82.75,
        "supp_retirement_weekly": 10.00,
        "legal_monthly": 16.63,
        "training_monthly": 14.13,
        "profit_sharing_quarterly": 130.00
    },
    "workers_comp": {
        "percent": 0.15
    },
    "wage_increase": {
        "percent": 0.03,
        "effective_week": "Wk 16",
        "pre_increase_weeks": 15,
        "post_increase_weeks": 37
    },
    "insurance_renewal": {
        "increase_percent": 0.15,
        "effective_date": "Mar 2027",
        "pre_renewal_months": 3,
        "post_renewal_months": 9
    },
    "energy": {
        "gas_esco_rate": 0,
        "electric_esco_rate": 0,
        "oil_price_per_gallon": 0,
        "gas_rate_increase": 0.05,
        "electric_rate_increase": 0.05,
        "oil_rate_increase": 0.05,
        "consumption_basis": "2-Year Average"
    },
    "water_sewer": {
        "rate_increase": 0.05
    }
}

INSURANCE_POLICIES = [
    {"gl_code": "6105-0000", "name": "Package"},
    {"gl_code": "6110-0000", "name": "Gen Liability"},
    {"gl_code": "6115-0000", "name": "Umbrella Excess"},
    {"gl_code": "6120-0000", "name": "Umbrella Primary"},
    {"gl_code": "6125-0000", "name": "D&O"},
    {"gl_code": "6126-0000", "name": "Cyber"},
    {"gl_code": "6135-0000", "name": "Crime"},
    {"gl_code": "6180-0000", "name": "D&O Excess"},
    {"gl_code": "6195-0000", "name": "Other"}
]

ENERGY_GL_ACCOUNTS = [
    {"gl": "5252-0000", "desc": "Gas - Heating"},
    {"gl": "5252-0001", "desc": "Gas - Cooking"},
    {"gl": "5252-0010", "desc": "Gas - Common Area"},
    {"gl": "5253-0000", "desc": "Oil / Fuel"},
    {"gl": "5250-0000", "desc": "Electric"}
]

WATER_GL_ACCOUNTS = [
    {"gl": "6305-0000", "desc": "Water/Sewer"},
    {"gl": "6305-0010", "desc": "Water - Common Area"},
    {"gl": "6305-0020", "desc": "Sewer Charges"}
]


# ─── Settings Functions ───────────────────────────────────────────────────────

def load_settings():
    """Load app settings from disk."""
    if SETTINGS_FILE.exists():
        return json.loads(SETTINGS_FILE.read_text())
    return {"save_dir": DEFAULT_SAVE_DIR}


def save_settings(settings):
    """Persist app settings to disk."""
    SETTINGS_FILE.write_text(json.dumps(settings, indent=2))


# ─── Buildings Functions ──────────────────────────────────────────────────────

def load_buildings():
    """Load building list from CSV."""
    buildings = []
    if BUILDINGS_CSV.exists():
        with open(BUILDINGS_CSV) as f:
            reader = csv.DictReader(f)
            for row in reader:
                buildings.append(row)
    return buildings


def save_buildings(buildings):
    """Save building list back to CSV."""
    if not buildings:
        return
    fieldnames = ["entity_code", "building_name", "address", "city", "zip", "type", "units"]
    with open(BUILDINGS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(buildings)


# ─── Assumptions Functions ────────────────────────────────────────────────────

def load_portfolio_defaults():
    """Load portfolio-wide defaults, or return defaults."""
    if PORTFOLIO_DEFAULTS_FILE.exists():
        return json.loads(PORTFOLIO_DEFAULTS_FILE.read_text())
    return DEFAULT_PORTFOLIO


def save_portfolio_defaults(data):
    """Save portfolio defaults to disk."""
    PORTFOLIO_DEFAULTS_FILE.write_text(json.dumps(data, indent=2))


def load_building_assumptions():
    """Load all building assumptions, or return empty dict."""
    if BUILDING_ASSUMPTIONS_FILE.exists():
        return json.loads(BUILDING_ASSUMPTIONS_FILE.read_text())
    return {}


def save_building_assumptions(data):
    """Save building assumptions to disk."""
    BUILDING_ASSUMPTIONS_FILE.write_text(json.dumps(data, indent=2))


def merge_assumptions(entity_code):
    """Merge portfolio defaults with building-specific overrides."""
    defaults = load_portfolio_defaults()
    building_data = load_building_assumptions().get(entity_code, {})

    # Start with defaults
    merged = json.loads(json.dumps(defaults))

    # Override with building-specific data
    if "payroll_tax" in building_data:
        merged["payroll_tax"].update(building_data["payroll_tax"])
    if "union_benefits" in building_data:
        merged["union_benefits"].update(building_data["union_benefits"])
    if "workers_comp" in building_data:
        merged["workers_comp"].update(building_data["workers_comp"])
    if "wage_increase" in building_data:
        merged["wage_increase"].update(building_data["wage_increase"])
    if "insurance_renewal" in building_data:
        merged["insurance_renewal"].update(building_data["insurance_renewal"])
    if "energy" in building_data:
        merged["energy"].update(building_data["energy"])
    if "water_sewer" in building_data:
        merged["water_sewer"].update(building_data["water_sewer"])

    merged["payroll"] = building_data.get("payroll", {})
    merged["income"] = building_data.get("income", {})
    merged["insurance"] = building_data.get("insurance", [])

    return merged


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Unified home page."""
    return render_template_string(HOME_TEMPLATE)


@app.route("/generate")
def generate():
    """Budget generator page."""
    settings = load_settings()
    return render_template_string(
        GENERATE_TEMPLATE,
        buildings=load_buildings(),
        save_dir=settings.get("save_dir", DEFAULT_SAVE_DIR),
    )


@app.route("/assumptions")
def assumptions():
    """Portfolio defaults page."""
    data = load_portfolio_defaults()
    return render_template_string(ASSUMPTIONS_TEMPLATE, data=json.dumps(data))


@app.route("/assumptions/buildings")
def assumptions_buildings():
    """Building assumptions grid."""
    bldgs = load_buildings()
    policies = INSURANCE_POLICIES
    return render_template_string(
        ASSUMPTIONS_BUILDINGS_TEMPLATE,
        buildings=json.dumps(bldgs),
        policies=json.dumps(policies)
    )


# ─── API Routes ───────────────────────────────────────────────────────────────

@app.route("/api/settings", methods=["GET", "POST"])
def settings_api():
    """Get or update app settings."""
    if request.method == "GET":
        return jsonify(load_settings())

    data = request.json
    settings = load_settings()
    if "save_dir" in data:
        save_dir = data["save_dir"].strip()
        if save_dir:
            # Validate path exists or can be created
            try:
                Path(save_dir).mkdir(parents=True, exist_ok=True)
                settings["save_dir"] = save_dir
            except Exception as e:
                return jsonify({"error": f"Invalid path: {e}"}), 400
        else:
            settings["save_dir"] = DEFAULT_SAVE_DIR

    save_settings(settings)
    return jsonify(settings)


@app.route("/api/buildings", methods=["GET", "POST"])
def buildings_api():
    """Get all buildings or add a new building."""
    if request.method == "GET":
        return jsonify(load_buildings())

    data = request.json
    required_fields = ["entity_code", "building_name", "address", "city", "zip", "type", "units"]
    if not all(f in data for f in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    buildings = load_buildings()
    # Check if entity_code already exists
    if any(b["entity_code"] == data["entity_code"] for b in buildings):
        return jsonify({"error": "Entity code already exists"}), 400

    new_building = {f: data.get(f, "") for f in required_fields}
    buildings.append(new_building)
    save_buildings(buildings)
    return jsonify(new_building), 201


@app.route("/api/buildings/<entity_code>", methods=["PUT"])
def manage_building(entity_code):
    """Edit a building."""
    buildings = load_buildings()
    building_idx = next((i for i, b in enumerate(buildings) if b["entity_code"] == entity_code), None)

    if building_idx is None:
        return jsonify({"error": "Building not found"}), 404

    data = request.json
    building = buildings[building_idx]
    # Update allowed fields
    for field in ["building_name", "address", "city", "zip", "type", "units"]:
        if field in data:
            building[field] = data[field]
    buildings[building_idx] = building
    save_buildings(buildings)
    return jsonify(building), 200


@app.route("/api/buildings/<entity_code>/delete", methods=["POST"])
def delete_building(entity_code):
    """Delete a building."""
    buildings = load_buildings()
    building_idx = next((i for i, b in enumerate(buildings) if b["entity_code"] == entity_code), None)

    if building_idx is None:
        return jsonify({"error": "Building not found"}), 404

    buildings.pop(building_idx)
    save_buildings(buildings)
    return jsonify({"message": "Building deleted"}), 200


# ─── CORS Helper for Yardi Auto-Upload ────────────────────────────────────
def cors_headers(f):
    """Decorator to add CORS headers for cross-origin Yardi→Railway requests."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == "OPTIONS":
            resp = make_response()
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
            resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Upload-Session, X-Entity-Code, X-File-Type, X-Filename"
            resp.headers["Access-Control-Max-Age"] = "3600"
            return resp
        result = f(*args, **kwargs)
        if isinstance(result, tuple):
            resp = make_response(result[0], result[1])
        else:
            resp = make_response(result)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    return decorated


# ─── Auto-Upload: Yardi → Budget App (no manual file upload needed) ───────
_upload_sessions = {}  # session_id → {files: [{name, data, type}], entities: [], period: str}

@app.route("/api/auto-upload", methods=["POST", "OPTIONS"])
@cors_headers
def auto_upload_file():
    """Accept a single file blob from the Yardi console script.

    The Yardi script POSTs each downloaded report here as it completes.
    Files accumulate in a server-side session until /api/auto-process is called.
    """
    if request.method == "OPTIONS":
        return make_response(""), 200

    session_id = request.headers.get("X-Upload-Session", "default")
    entity_code = request.headers.get("X-Entity-Code", "")
    file_type = request.headers.get("X-File-Type", "unknown")  # ysl, expense, maint, ap

    if session_id not in _upload_sessions:
        _upload_sessions[session_id] = {"files": [], "entities": set(), "period": ""}

    sess = _upload_sessions[session_id]

    # Accept the file from the request body
    if request.content_type and "multipart" in request.content_type:
        f = request.files.get("file")
        if not f:
            return jsonify({"error": "No file in request"}), 400
        filename = f.filename or f"upload_{entity_code}_{file_type}.xls"
        file_data = f.read()
    else:
        # Raw binary body
        file_data = request.get_data()
        filename = request.headers.get("X-Filename", f"upload_{entity_code}_{file_type}.xls")

    if not file_data:
        return jsonify({"error": "Empty file"}), 400

    sess["files"].append({
        "name": filename,
        "data": file_data,
        "type": file_type,
        "entity": entity_code,
    })
    if entity_code:
        sess["entities"].add(entity_code)

    logger.info(f"Auto-upload: received {filename} ({len(file_data)} bytes) for entity {entity_code}, session {session_id}")

    return jsonify({
        "status": "received",
        "filename": filename,
        "size": len(file_data),
        "total_files": len(sess["files"]),
    })


@app.route("/api/auto-process", methods=["POST", "OPTIONS"])
@cors_headers
def auto_process():
    """Trigger processing of all files accumulated via auto-upload.

    Called by the Yardi script after all downloads complete.
    Reuses the same processing logic as /api/process.
    """
    if request.method == "OPTIONS":
        return make_response(""), 200

    data = request.get_json(silent=True) or {}
    session_id = data.get("session_id", request.headers.get("X-Upload-Session", "default"))
    period = data.get("period", "02/2026")

    if session_id not in _upload_sessions or not _upload_sessions[session_id]["files"]:
        return jsonify({"error": "No files in session. Upload files first via /api/auto-upload"}), 400

    sess = _upload_sessions.pop(session_id)
    file_list = sess["files"]

    logger.info(f"Auto-process: session {session_id}, {len(file_list)} files, entities: {sess['entities']}")

    # Save files to a temp dir and reuse the existing /api/process logic
    # by writing them as real files and processing them
    save_dir_str = load_settings().get("save_dir", DEFAULT_SAVE_DIR)
    save_dir = Path(save_dir_str)
    ysl_archive = BUDGET_SYSTEM / "ysl_downloads"
    ysl_archive.mkdir(exist_ok=True)

    results = {"success": [], "failed": [], "warnings": [], "save_dir": str(save_dir), "auto_upload": True}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        output_files = []
        saved_files = []
        ysl_entities = set()
        expense_files = []
        open_ap_files = []
        maint_proof_files = []

        # Pass 1: Save all files, classify, process YSL first
        for finfo in file_list:
            filename = finfo["name"]
            if not filename.endswith((".xlsx", ".xls")):
                results["warnings"].append(f"Skipped non-Excel: {filename}")
                continue

            file_path = tmp / filename
            file_path.write_bytes(finfo["data"])

            # Use upload-time file type hint as primary classifier
            upload_type = finfo.get("type", "unknown")

            # If upload type is 'ap', route directly to Open AP pipeline
            if upload_type == "ap":
                logger.info(f"Auto-upload routed by type hint: {filename} → Open AP")
                open_ap_files.append((file_path, filename))
                continue

            # If upload type is 'maint', route to Maintenance Proof pipeline
            if upload_type == "maint":
                logger.info(f"Auto-upload routed by type hint: {filename} → Maintenance Proof")
                maint_proof_files.append((file_path, filename))
                continue

            # Detect Open AP from content (fallback for manual uploads)
            try:
                if detect_open_ap_file(str(file_path)):
                    logger.info(f"Auto-upload detection for {filename}: Open AP (Aging) report")
                    open_ap_files.append((file_path, filename))
                    continue
            except Exception:
                pass

            # Handle .xls files that are actually .xlsx (Yardi naming quirk)
            # Copy to .xlsx temp file if needed for openpyxl
            check_path = file_path
            _tmp_xlsx = None
            if str(file_path).lower().endswith('.xls') and not str(file_path).lower().endswith('.xlsx'):
                import shutil as _shutil
                _tmp_xlsx = tmp / (filename + 'x')  # .xls → .xlsx
                _shutil.copy2(str(file_path), str(_tmp_xlsx))
                check_path = _tmp_xlsx

            # Detect Expense Distribution vs YSL via Row 1
            try:
                from openpyxl import load_workbook as _lwb
                _wb_check = _lwb(str(check_path), data_only=True)
                _ws_check = _wb_check.active
                _row1 = str(_ws_check.cell(row=1, column=1).value or "")
                _row2 = str(_ws_check.cell(row=2, column=1).value or "")
                _wb_check.close()

                is_expense = "expense distribution" in _row1.lower() or "expense dist" in _row1.lower()
                logger.info(f"Auto-upload detection for {filename}: Row1='{_row1}', is_expense={is_expense}")

                if is_expense:
                    expense_files.append((file_path, filename))
                    continue

                is_maint = "maintenance proof" in _row1.lower() or "adhoc_amp" in _row1.lower()
                if is_maint:
                    logger.info(f"Auto-upload detection for {filename}: Maintenance Proof report")
                    maint_proof_files.append((file_path, filename))
                    continue
            except Exception as detect_err:
                logger.error(f"Auto-upload detection error for {filename}: {detect_err}")
                results["warnings"].append(f"Could not read {filename}: {str(detect_err)}")
                continue

            # Process YSL file
            try:
                gl_data, property_info = parse_ysl_file(file_path)
                entity = property_info.get("property_code", "unknown")
                name = property_info.get("property_name", f"Entity_{entity}")
                ysl_entities.add(str(entity))

                building_folder_name = f"{entity} - {name}"
                building_dir = save_dir / building_folder_name
                building_dir.mkdir(parents=True, exist_ok=True)

                output_name = f"{entity}_{name}_2027_Budget.xlsx"
                output_path = building_dir / output_name

                _gen_ytd = 2
                try:
                    _gen_ytd = int(period.split("/")[0])
                except Exception:
                    pass

                success = populate_template(
                    template_path=TEMPLATE_PATH,
                    gl_data=gl_data,
                    property_info=property_info,
                    output_path=output_path,
                    ytd_months=_gen_ytd,
                    remaining_months=12 - _gen_ytd,
                )

                if success and output_path.exists():
                    merged = None
                    try:
                        merged = merge_assumptions(entity)
                    except Exception:
                        pass

                    try:
                        fresh = data.get("fresh_start", False)
                        workflow_helpers["store_all_lines"](entity, name, gl_data, TEMPLATE_PATH, assumptions=merged, fresh_start=fresh)
                    except Exception as wfe:
                        logger.warning(f"Could not store GL data for {entity}: {wfe}")

                    try:
                        if merged:
                            apply_assumptions(output_path, merged)
                    except Exception as ae:
                        logger.warning(f"Could not apply assumptions for {entity}: {ae}")

                    try:
                        pm_proj = workflow_helpers["get_pm_projections"](entity)
                        if pm_proj:
                            apply_pm_projections(output_path, pm_proj)
                    except Exception as pe:
                        logger.warning(f"Could not apply PM projections for {entity}: {pe}")

                    output_files.append((output_name, output_path))
                    shutil.copy2(file_path, ysl_archive / filename)
                    yardi_drops = building_dir / "yardi_drops"
                    yardi_drops.mkdir(exist_ok=True)
                    shutil.copy2(file_path, yardi_drops / filename)

                    results["success"].append({
                        "entity": entity, "name": name, "file": output_name,
                        "size_kb": round(output_path.stat().st_size / 1024),
                        "saved_to": str(building_dir),
                    })
                else:
                    results["failed"].append({"entity": entity, "file": filename, "reason": "Pipeline returned failure"})
            except Exception as e:
                logger.exception(f"Auto-upload error processing {filename}")
                results["failed"].append({"file": filename, "reason": str(e)})

        # Pass 2: Expense Distribution
        for exp_path, exp_filename in expense_files:
            try:
                from expense_distribution import parse_expense_distribution
            except ImportError:
                from budget_app.expense_distribution import parse_expense_distribution
            try:
                exp_entity, exp_from, exp_to, exp_invoices = parse_expense_distribution(str(exp_path))
                if not exp_invoices:
                    results["warnings"].append(f"Expense file {exp_filename}: no invoices found")
                    continue

                import re as _re
                fname_match = _re.search(r'ExpenseDistribution[_-]?(\d+)', exp_filename)
                fname_entity = fname_match.group(1) if fname_match else None

                target_entity = exp_entity
                if fname_entity and fname_entity != exp_entity:
                    if fname_entity in ysl_entities:
                        target_entity = fname_entity
                    elif not ysl_entities:
                        target_entity = fname_entity

                report = ed_helpers["store_expense_report"](target_entity, exp_from, exp_to, exp_invoices, exp_filename)
                accrual_result = {"applied": 0, "accruals": {}}
                if exp_from:
                    try:
                        accrual_result = ed_helpers["apply_accrual_adjustments"](target_entity, report.id, exp_from)
                    except Exception as accrual_err:
                        results["warnings"].append(f"Accrual adjustments failed for {target_entity}: {str(accrual_err)}")

                accrual_msg = f", {accrual_result['applied']} accrual adj" if accrual_result["applied"] > 0 else ""
                results["success"].append(f"Expense Distribution: {target_entity} ({len(exp_invoices)} invoices{accrual_msg})")
            except Exception as exp_err:
                results["warnings"].append(f"Expense file {exp_filename} error: {str(exp_err)}")

        # Pass 3: Open AP
        for ap_path, ap_filename in open_ap_files:
            try:
                ap_entity, ap_invoices = parse_open_ap_report(str(ap_path))
                if not ap_invoices:
                    results["warnings"].append(f"Open AP file {ap_filename}: no invoices found")
                    continue

                import re as _re
                fname_match = _re.search(r'(?:Aging|OpenAP|AP)[_-]?(\d+)', ap_filename, _re.IGNORECASE)
                fname_entity = fname_match.group(1) if fname_match else None

                target_entity = ap_entity
                if fname_entity and fname_entity != ap_entity:
                    if fname_entity in ysl_entities:
                        target_entity = fname_entity
                    elif not ysl_entities:
                        target_entity = fname_entity
                elif not target_entity and fname_entity:
                    target_entity = fname_entity

                if not target_entity:
                    results["warnings"].append(f"Open AP file {ap_filename}: could not determine entity code")
                    continue

                report = oa_helpers["store_open_ap_report"](target_entity, ap_invoices, ap_filename)
                unpaid_result = {"applied": 0, "gl_totals": {}}
                try:
                    unpaid_result = oa_helpers["apply_unpaid_bills"](target_entity)
                    logger.info(f"Applied unpaid bills: {unpaid_result['applied']} GL lines for entity {target_entity}")
                except Exception as unpaid_err:
                    logger.error(f"CRITICAL: apply_unpaid_bills failed for {target_entity}: {unpaid_err}")
                    results["warnings"].append(f"Unpaid bills FAILED for {target_entity}: {str(unpaid_err)}")

                unpaid_msg = f", {unpaid_result['applied']} GL lines updated" if unpaid_result["applied"] > 0 else ""
                results["success"].append(f"Open AP: {target_entity} ({len(ap_invoices)} invoices, ${report.total_amount:,.2f}{unpaid_msg})")
            except Exception as ap_err:
                results["warnings"].append(f"Open AP file {ap_filename} error: {str(ap_err)}")

        # Pass 4: Maintenance Proof
        for mp_path, mp_filename in maint_proof_files:
            try:
                report_title, units, total_shares = mp_helpers["parse_maintenance_proof"](str(mp_path))
                if not units:
                    results["warnings"].append(f"Maintenance Proof file {mp_filename}: no units found")
                    continue

                import re as _re
                fname_match = _re.search(r'(?:Adhoc_AMP|MaintProof|MP)[_-]?(\d+)', mp_filename, _re.IGNORECASE)
                fname_entity = fname_match.group(1) if fname_match else None

                target_entity = fname_entity
                if not target_entity and ysl_entities:
                    target_entity = next(iter(ysl_entities))

                if not target_entity:
                    results["warnings"].append(f"Maintenance Proof file {mp_filename}: could not determine entity code")
                    continue

                report = mp_helpers["store_maintenance_proof"](target_entity, report_title, units, total_shares, mp_filename)
                results["success"].append(f"Maintenance Proof: {target_entity} ({len(units)} units, {report_title})")
            except Exception as mp_err:
                results["warnings"].append(f"Maintenance Proof file {mp_filename} error: {str(mp_err)}")

        return jsonify({"message": f"Auto-processed {len(file_list)} files", **results}), 200


@app.route("/api/generate-script", methods=["POST"])
def generate_script():
    """Generate a customized Console script for selected buildings.

    Combines both the YSL Annual Budget and Expense Distribution scripts
    into a single script that downloads both reports for each entity.
    """
    data = request.json
    entities = data.get("entities", [])
    email = data.get("email", "")
    period = data.get("period", "02/2026")

    if not entities:
        return jsonify({"error": "No buildings selected"}), 400

    entities_js = ', '.join(str(e) for e in entities)

    # Build YSL script with user settings
    ysl_script = CONSOLE_SCRIPT
    ysl_script = ysl_script.replace(
        "const AS_OF_PERIOD = '02/2026';",
        f"const AS_OF_PERIOD = '{period}';"
    )
    ysl_script = ysl_script.replace(
        "const ENTITIES = [148, 204, 206, 805];",
        f"const ENTITIES = [{entities_js}];"
    )
    ysl_script = ysl_script.replace(
        "const EMAIL = 'JSirotkin@Centuryny.com';",
        f"const EMAIL = '{email}';"
    )

    # Expense Distribution removed from combined script — runs separately
    # because ASP.NET ViewState reverts RT=1 to RT=3.
    # See /api/generate-expense-dist-script.

    # Build AP Aging script with user settings (back in combined — safe now
    # that Expense Distribution is separate, so no RT contamination)
    ap_script = ""
    if AP_AGING_SCRIPT:
        ap_script = AP_AGING_SCRIPT
        ap_script = ap_script.replace(
            "const ENTITIES = [148, 204, 206, 805];",
            f"const ENTITIES = [{entities_js}];"
        )
        ap_script = ap_script.replace(
            "const PERIOD_TO = '03/2026';",
            f"const PERIOD_TO = '{period}';"
        )
        # Inject auto-upload into AP Aging triggerDownload
        ap_script = ap_script.replace(
            "    URL.revokeObjectURL(a.href);\n  }",
            "    URL.revokeObjectURL(a.href);\n    if (typeof _autoUpload === 'function') _autoUpload(blob, a.download, entity, 'ap');\n  }"
        )

    # Build Maintenance Proof script with user settings
    # Map entity → charge code based on building type from CSV
    building_charges = {}
    try:
        with open(BUILDINGS_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ec = str(row.get("entity_code", "")).strip()
                btype = (row.get("type", "") or "").strip().lower()
                if btype in ("coop", "cond-op"):
                    building_charges[ec] = "maint"
                elif btype == "condo":
                    building_charges[ec] = "common"
                # Rental, Mitchell Lama, Comm/Retail → skip (no maint proof)
    except Exception:
        pass

    # Only include entities that need a maintenance proof
    mp_entities = [e for e in entities if str(e) in building_charges]
    mp_mapping_js = ", ".join(f"{e}: '{building_charges[str(e)]}'" for e in mp_entities)

    mp_script = MAINT_PROOF_SCRIPT
    mp_script = mp_script.replace(
        "const ENTITY_CHARGES = {204: 'maint', 206: 'common', 148: 'maint', 805: 'maint'};",
        f"const ENTITY_CHARGES = {{{mp_mapping_js}}};"
    )

    # AP Aging is back in the combined script now that Expense Distribution
    # runs separately. No RT contamination risk.
    if ap_script:
        ap_script_block = f"""
  // ── Part 3: AP Aging ──
  console.log('\\n>>> Starting Part 3: AP Aging <<<\\n');
  try {{
    _partResults.ap = await {ap_script}
    console.log('\\n>>> Part 3 (AP Aging) completed successfully <<<\\n');
  }} catch (_e3) {{
    console.error('>>> Part 3 (AP Aging) FAILED with error:', _e3.message);
    console.error(_e3.stack);
    _partResults.ap = 'ERROR: ' + _e3.message;
  }}"""
    else:
        ap_script_block = ""

    # ── Inject auto-upload into each script's triggerDownload function ──
    # The 3 scripts (Expense, MaintProof, AP Aging) all have a triggerDownload(blob, entity)
    # function. We inject a call to the global _autoUpload helper after the local download.
    # YSL has inline download code — we handle that separately.

    # Expense Distribution patching removed — runs as separate standalone script

    # Patch Maintenance Proof triggerDownload
    mp_script = mp_script.replace(
        "    URL.revokeObjectURL(a.href);\n  }",
        "    URL.revokeObjectURL(a.href);\n    if (typeof _autoUpload === 'function') _autoUpload(blob, a.download, entity, 'maint');\n  }"
    )

    # AP Aging patching removed — runs as separate standalone script

    # Patch YSL inline download (it doesn't use triggerDownload function)
    ysl_script = ysl_script.replace(
        "URL.revokeObjectURL(a.href);",
        "URL.revokeObjectURL(a.href);\n          if (typeof _autoUpload === 'function') _autoUpload(blob, a.download, entity, 'ysl');"
    )

    # Get the Railway app URL for auto-upload target
    railway_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if railway_url and not railway_url.startswith("http"):
        railway_url = f"https://{railway_url}"
    if not railway_url:
        # Fallback to known production URL
        railway_url = "https://century-budget-generator-production.up.railway.app"

    # Combine into one script that runs all four sequentially
    # Each part is wrapped in try/catch so errors don't kill subsequent parts
    combined = f"""/**
 * Century Budget — Combined Yardi Download Script
 * Downloads YSL Annual Budget + Maintenance Proof + AP Aging for all selected entities.
 * Expense Distribution runs separately (own button).
 * Generated for: {email}
 * Entities: {entities_js}
 * Period: {period}
 *
 * AUTO-UPLOAD: Files are automatically sent to the budget app for processing.
 * Local downloads are also saved as backup.
 */
(async function() {{
  'use strict';
  const _partResults = {{ysl: null, mp: null, ap: null}};
  const _uploadedFiles = [];
  const _SESSION_ID = 'yardi_' + Date.now();
  const _BUDGET_APP = '{railway_url}';
  const _PERIOD = '{period}';
  const _FRESH_START = {str(data.get('fresh_start', False)).lower()};

  // ── Auto-Upload Helper ──
  // Sends each downloaded blob to the budget app in the background
  async function _autoUpload(blob, filename, entity, fileType) {{
    try {{
      const formData = new FormData();
      formData.append('file', blob, filename);
      const resp = await fetch(_BUDGET_APP + '/api/auto-upload', {{
        method: 'POST',
        headers: {{
          'X-Upload-Session': _SESSION_ID,
          'X-Entity-Code': String(entity),
          'X-File-Type': fileType,
          'X-Filename': filename,
        }},
        body: formData,
      }});
      const data = await resp.json();
      if (resp.ok) {{
        _uploadedFiles.push(filename);
        console.log('  ↑ Auto-uploaded: ' + filename + ' (' + data.total_files + ' files in session)');
      }} else {{
        console.warn('  ↑ Upload failed for ' + filename + ': ' + (data.error || resp.status));
      }}
    }} catch (err) {{
      console.warn('  ↑ Upload error for ' + filename + ': ' + err.message);
    }}
  }}

  // ── Auto-Process: triggers server-side processing after all downloads ──
  async function _autoProcess() {{
    if (_uploadedFiles.length === 0) {{
      console.log('No files uploaded — skipping auto-process.');
      return;
    }}
    try {{
      console.log('\\n>>> Auto-processing ' + _uploadedFiles.length + ' files on server... <<<');
      const resp = await fetch(_BUDGET_APP + '/api/auto-process', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ session_id: _SESSION_ID, period: _PERIOD, fresh_start: _FRESH_START }}),
      }});
      const data = await resp.json();
      if (resp.ok) {{
        console.log('✓ Server processed ' + _uploadedFiles.length + ' files successfully!');
        if (data.success) console.log('  Successes:', data.success);
        if (data.warnings && data.warnings.length) console.log('  Warnings:', data.warnings);
        if (data.failed && data.failed.length) console.log('  Failures:', data.failed);
        // Show a Done banner with link to FA Dashboard
        const banner = document.createElement('div');
        banner.style.cssText = 'position:fixed;top:20px;right:20px;z-index:99999;background:#065f46;color:white;padding:16px 24px;border-radius:12px;font-family:system-ui;font-size:14px;box-shadow:0 8px 24px rgba(0,0,0,0.3);display:flex;flex-direction:column;gap:10px;max-width:360px;';
        banner.innerHTML = '<div style="font-weight:700;font-size:16px;">✓ Budget data updated</div>'
          + '<div style="font-size:13px;opacity:0.9;">' + _uploadedFiles.length + ' files processed'
          + (data.warnings && data.warnings.length ? ' · ' + data.warnings.length + ' warning(s)' : '')
          + '</div>'
          + '<a href="' + _BUDGET_APP + '/dashboard" target="_blank" style="display:inline-block;background:white;color:#065f46;padding:8px 20px;border-radius:8px;text-decoration:none;font-weight:600;font-size:14px;text-align:center;margin-top:4px;">Open FA Dashboard →</a>';
        document.body.appendChild(banner);
        // Auto-dismiss after 30s
        setTimeout(() => banner.remove(), 30000);
      }} else {{
        console.error('✗ Server processing failed:', data.error || resp.status);
      }}
    }} catch (err) {{
      console.error('✗ Auto-process error:', err.message);
      console.log('Files were downloaded locally — you can upload them manually via the Generator page.');
    }}
  }}

  console.log('='.repeat(60));
  console.log('Century Budget — Combined Download + Auto-Upload');
  console.log('Target: ' + _BUDGET_APP);
  console.log('Session: ' + _SESSION_ID);
  console.log('Part 1: YSL Annual Budget reports');
  console.log('Part 2: Maintenance Proof reports');
  console.log('Part 3: AP Aging reports');
  console.log('(Expense Distribution runs separately — use its button)');
  console.log('='.repeat(60));

  // ── Part 1: YSL Annual Budget ──
  console.log('\\n>>> Starting Part 1: YSL Annual Budget <<<\\n');
  try {{
    _partResults.ysl = await {ysl_script}
    console.log('\\n>>> Part 1 (YSL) completed successfully <<<\\n');
  }} catch (_e1) {{
    console.error('>>> Part 1 (YSL) FAILED with error:', _e1.message);
    console.error(_e1.stack);
    _partResults.ysl = 'ERROR: ' + _e1.message;
  }}

  // ── Part 2: Maintenance Proof ({len(mp_entities)} of {len(entities)} buildings — coops/condos only) ──
  console.log('\\n>>> Starting Part 2: Maintenance Proof ({len(mp_entities)} of {len(entities)} buildings — coops/condos only) <<<\\n');
  try {{
    _partResults.mp = await {mp_script}
    console.log('\\n>>> Part 2 (Maintenance Proof) completed successfully <<<\\n');
  }} catch (_e2) {{
    console.error('>>> Part 2 (Maintenance Proof) FAILED with error:', _e2.message);
    console.error(_e2.stack);
    _partResults.mp = 'ERROR: ' + _e2.message;
  }}
{ap_script_block}

  // ── Auto-Process all uploaded files ──
  await _autoProcess();

  console.log('\\n' + '='.repeat(60));
  console.log('ALL DONE — Summary:');
  console.log('  Part 1 (YSL):', _partResults.ysl === null ? 'SKIPPED' : (typeof _partResults.ysl === 'string' && _partResults.ysl.startsWith('ERROR') ? _partResults.ysl : 'OK'));
  console.log('  Part 2 (MP): ', _partResults.mp === null ? 'SKIPPED' : (typeof _partResults.mp === 'string' && _partResults.mp.startsWith('ERROR') ? _partResults.mp : 'OK'));
  console.log('  Part 3 (AP): ', _partResults.ap === null ? 'SKIPPED' : (typeof _partResults.ap === 'string' && _partResults.ap.startsWith('ERROR') ? _partResults.ap : 'OK'));
  console.log('  Auto-Upload:', _uploadedFiles.length + ' files sent to server');
  console.log(_uploadedFiles.length > 0 ? 'Files were auto-processed — check the dashboard!' : 'Files were downloaded locally — upload them via the Generator page.');
  console.log('');
  console.log('Now run the Expense Distribution script separately (use its button on the Generate page).');
  console.log('='.repeat(60));
}})();"""

    return jsonify({"script": combined})


@app.route("/api/generate-expense-dist-script", methods=["POST"])
def generate_expense_dist_script():
    """Generate a standalone Expense Distribution script.

    Separated from the combined script because ASP.NET ViewState on
    APAnalytics.aspx reverts ReportType=1 to RT=3 (Aging) during postbacks.
    The standalone script runs directly on the page after the user manually
    selects "Expense Distribution" in the dropdown.
    """
    data = request.json
    entities = data.get("entities", [])
    email = data.get("email", "")
    period = data.get("period", "02/2026")

    if not entities:
        return jsonify({"error": "No buildings selected"}), 400

    entities_js = ', '.join(str(e) for e in entities)

    exp_script = EXPENSE_DIST_SCRIPT
    exp_script = exp_script.replace(
        "const ENTITIES = [148, 204, 206, 805];",
        f"const ENTITIES = [{entities_js}];"
    )
    exp_script = exp_script.replace(
        "const PERIOD_FROM = '01/2026';",
        f"const PERIOD_FROM = '{period}';"
    )
    exp_script = exp_script.replace(
        "const PERIOD_TO   = '03/2026';",
        f"const PERIOD_TO   = '{period}';"
    )

    # Get the Railway app URL for auto-upload target
    railway_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if railway_url and not railway_url.startswith("http"):
        railway_url = f"https://{railway_url}"
    if not railway_url:
        railway_url = "https://century-budget-generator-production.up.railway.app"

    # Inject auto-upload into triggerDownload
    exp_script = exp_script.replace(
        "    URL.revokeObjectURL(a.href);\n  }",
        "    URL.revokeObjectURL(a.href);\n    if (typeof _autoUpload === 'function') _autoUpload(blob, a.download, entity, 'expense');\n  }"
    )

    # Wrap with auto-upload helper and auto-process
    standalone = f"""/**
 * Century Budget — Expense Distribution Standalone Script
 * IMPORTANT: Open APAnalytics in Yardi and select "Expense Distribution"
 * from the Report Type dropdown BEFORE pasting this script.
 * Entities: {entities_js}
 * Period: {period}
 */
(async function() {{
  'use strict';
  const _uploadedFiles = [];
  const _SESSION_ID = 'yardi_' + Date.now();
  const _BUDGET_APP = '{railway_url}';

  async function _autoUpload(blob, filename, entity, fileType) {{
    try {{
      const formData = new FormData();
      formData.append('file', blob, filename);
      const resp = await fetch(_BUDGET_APP + '/api/auto-upload', {{
        method: 'POST',
        headers: {{
          'X-Upload-Session': _SESSION_ID,
          'X-Entity-Code': String(entity),
          'X-File-Type': fileType,
          'X-Filename': filename,
        }},
        body: formData,
      }});
      const data = await resp.json();
      if (resp.ok) {{
        _uploadedFiles.push(filename);
        console.log('  \\u2191 Auto-uploaded: ' + filename);
      }}
    }} catch (err) {{
      console.warn('  Auto-upload failed for ' + filename + ':', err.message);
    }}
  }}

  async function _autoProcess() {{
    if (!_uploadedFiles.length) return;
    console.log('\\n>>> Auto-processing ' + _uploadedFiles.length + ' files on server... <<<');
    try {{
      const resp = await fetch(_BUDGET_APP + '/api/auto-process', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ session_id: _SESSION_ID, period: '{period}' }})
      }});
      const data = await resp.json();
      if (resp.ok) {{
        console.log('\\u2713 Server processed files successfully!');
        const banner = document.createElement('div');
        banner.style.cssText = 'position:fixed;top:20px;right:20px;z-index:99999;background:#065f46;color:white;padding:16px 24px;border-radius:12px;font-family:system-ui;font-size:14px;box-shadow:0 8px 24px rgba(0,0,0,0.3);';
        banner.innerHTML = '<div style="font-weight:700;">\\u2713 Expense data uploaded</div><div style="font-size:12px;margin-top:4px;">' + _uploadedFiles.length + ' files processed</div>';
        document.body.appendChild(banner);
        setTimeout(() => banner.remove(), 15000);
      }}
    }} catch (err) {{
      console.error('Auto-process error:', err.message);
    }}
  }}

  console.log('='.repeat(60));
  console.log('Expense Distribution — Standalone Download + Auto-Upload');
  console.log('Target: ' + _BUDGET_APP);
  console.log('='.repeat(60));

  try {{
    await {exp_script}
    console.log('\\n>>> Expense Distribution completed successfully <<<');
  }} catch (e) {{
    console.error('>>> Expense Distribution FAILED:', e.message);
  }}

  await _autoProcess();

  console.log('\\n' + '='.repeat(60));
  console.log('Expense Distribution DONE — ' + _uploadedFiles.length + ' files uploaded.');
  console.log('='.repeat(60));
}})();"""

    return jsonify({"script": standalone})


@app.route("/api/generate-ap-aging-script", methods=["POST"])
def generate_ap_aging_script():
    """Generate a standalone AP Aging script that runs independently.

    Separated from the combined script because Yardi's ASP.NET session
    remembers ReportType=2 (Expense Distribution) and contaminates the
    AP Aging download when both run in the same browser session.
    Running AP Aging standalone avoids this session state issue.
    """
    data = request.json
    entities = data.get("entities", [])
    email = data.get("email", "")
    period = data.get("period", "02/2026")

    if not entities:
        return jsonify({"error": "No buildings selected"}), 400

    if not AP_AGING_SCRIPT:
        return jsonify({"error": "AP Aging script not available on server"}), 500

    entities_js = ', '.join(str(e) for e in entities)

    ap_aging_script = AP_AGING_SCRIPT
    ap_aging_script = ap_aging_script.replace(
        "const ENTITIES = [148, 204, 206, 805];",
        f"const ENTITIES = [{entities_js}];"
    )
    ap_aging_script = ap_aging_script.replace(
        "const PERIOD_TO = '03/2026';",
        f"const PERIOD_TO = '{period}';"
    )

    # Get the Railway app URL for auto-upload target
    railway_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if railway_url and not railway_url.startswith("http"):
        railway_url = f"https://{railway_url}"
    if not railway_url:
        railway_url = "https://century-budget-generator-production.up.railway.app"

    # Inject auto-upload into triggerDownload
    ap_aging_script = ap_aging_script.replace(
        "URL.revokeObjectURL(a.href);\n  }",
        "URL.revokeObjectURL(a.href);\n    if (typeof _autoUpload === 'function') _autoUpload(blob, a.download, entity, 'ap');\n  }"
    )

    # Wrap with auto-upload helper and auto-process
    standalone = f"""/**
 * Century Budget — AP Aging (Open AP) Standalone Script
 * Run this AFTER the main Yardi script (YSL + Expense + Maint Proof).
 * Entities: {entities_js}
 * Period: {period}
 */
(async function() {{
  'use strict';
  const _uploadedFiles = [];
  const _SESSION_ID = 'yardi_' + Date.now();
  const _BUDGET_APP = '{railway_url}';

  async function _autoUpload(blob, filename, entity, fileType) {{
    try {{
      const resp = await fetch(_BUDGET_APP + '/api/auto-upload', {{
        method: 'POST',
        headers: {{
          'X-Upload-Session': _SESSION_ID,
          'X-Entity-Code': String(entity),
          'X-File-Type': fileType,
          'X-Filename': filename,
          'Content-Type': 'application/octet-stream'
        }},
        body: blob
      }});
      const data = await resp.json();
      _uploadedFiles.push({{ filename, entity, fileType, ok: resp.ok }});
      console.log('  \\u2191 Auto-uploaded: ' + filename + ' (' + data.files_in_session + ' files in session)');
    }} catch (err) {{
      console.warn('  Auto-upload failed for ' + filename + ':', err.message);
    }}
  }}

  async function _autoProcess() {{
    if (!_uploadedFiles.length) return;
    console.log('\\n>>> Auto-processing ' + _uploadedFiles.length + ' files on server... <<<');
    try {{
      const resp = await fetch(_BUDGET_APP + '/api/auto-process', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ session_id: _SESSION_ID }})
      }});
      const data = await resp.json();
      if (resp.ok) {{
        console.log('\\u2713 Server processed ' + (data.successes?.length || 0) + ' files successfully!');
        if (data.successes?.length) console.log('  Successes:', data.successes);
        if (data.warnings?.length) console.log('  Warnings:', data.warnings);
        if (data.failures?.length) console.log('  Failures:', data.failures);
      }}
    }} catch (err) {{
      console.error('Auto-process error:', err.message);
    }}
  }}

  console.log('='.repeat(60));
  console.log('AP Aging (Open AP) — Standalone Download + Auto-Upload');
  console.log('Target: ' + _BUDGET_APP);
  console.log('='.repeat(60));

  try {{
    await {ap_aging_script}
    console.log('\\n>>> AP Aging completed successfully <<<');
  }} catch (e) {{
    console.error('>>> AP Aging FAILED:', e.message);
  }}

  await _autoProcess();

  console.log('\\n' + '='.repeat(60));
  console.log('AP Aging DONE — ' + _uploadedFiles.length + ' files uploaded.');
  console.log(_uploadedFiles.length > 0 ? 'Check the FA Dashboard for results.' : 'No files uploaded — check console for errors.');
  console.log('='.repeat(60));
}})();"""

    return jsonify({"script": standalone})


@app.route("/api/process", methods=["POST"])
def process_files():
    """Accept YSL uploads, run pipeline, save + return budgets."""
    if "files" not in request.files:
        return jsonify({"error": "No files uploaded"}), 400

    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files uploaded"}), 400

    # Get save location from form data or settings
    save_dir_str = request.form.get("save_dir", "").strip()
    if not save_dir_str:
        save_dir_str = load_settings().get("save_dir", DEFAULT_SAVE_DIR)
    save_dir = Path(save_dir_str)

    # Also archive YSLs to the central ysl_downloads folder
    ysl_archive = BUDGET_SYSTEM / "ysl_downloads"
    ysl_archive.mkdir(exist_ok=True)

    results = {"success": [], "failed": [], "warnings": [], "save_dir": str(save_dir)}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        output_files = []

        # Multi-pass approach: separate file types so we process in correct order
        # Order: YSL first (creates BudgetLines), then Expense Dist, then Open AP
        saved_files = []  # (path, filename, file_type, row1, row2)
        ysl_entities = set()  # entity codes found in YSL files
        expense_files = []  # (path, filename) for expense files to process after YSL
        open_ap_files = []  # (path, filename) for AP Aging files to process after YSL
        maint_proof_files = []  # (path, filename) for Maintenance Proof files

        # Pass 1: Save all files, classify them, process YSL files first
        for f in files:
            if not f.filename or not f.filename.endswith((".xlsx", ".xls")):
                results["warnings"].append(f"Skipped non-Excel: {f.filename}")
                continue

            file_path = tmp / f.filename
            f.save(str(file_path))

            try:
                # Check if it's an Open AP (Aging) file first (uses robust detection)
                if detect_open_ap_file(str(file_path)):
                    logger.info(f"File detection for {f.filename}: Open AP (Aging) report")
                    open_ap_files.append((file_path, f.filename))
                    continue

                # Handle .xls files that are actually .xlsx (Yardi naming quirk)
                _check_path = file_path
                _tmp_xlsx_manual = None
                if str(file_path).lower().endswith('.xls') and not str(file_path).lower().endswith('.xlsx'):
                    import shutil as _shutil_m
                    _tmp_xlsx_manual = tmp / (f.filename + 'x')
                    _shutil_m.copy2(str(file_path), str(_tmp_xlsx_manual))
                    _check_path = _tmp_xlsx_manual

                from openpyxl import load_workbook as _lwb
                _wb_check = _lwb(str(_check_path), data_only=True)
                _ws_check = _wb_check.active
                _row1 = str(_ws_check.cell(row=1, column=1).value or "")
                _row2 = str(_ws_check.cell(row=2, column=1).value or "")
                _wb_check.close()

                is_expense = "expense distribution" in _row1.lower() or "expense dist" in _row1.lower()
                logger.info(f"File detection for {f.filename}: Row1='{_row1}', Row2='{_row2}', is_expense={is_expense}")

                if is_expense:
                    expense_files.append((file_path, f.filename))
                    continue

                is_maint = "maintenance proof" in _row1.lower() or "adhoc_amp" in _row1.lower()
                if is_maint:
                    logger.info(f"File detection for {f.filename}: Maintenance Proof report")
                    maint_proof_files.append((file_path, f.filename))
                    continue
            except Exception as detect_err:
                logger.error(f"File detection error for {f.filename}: {detect_err}")
                results["warnings"].append(f"Could not read {f.filename}: {str(detect_err)}")
                continue

            # Process YSL file immediately
            ysl_path = file_path
            try:
                # Parse YSL
                gl_data, property_info = parse_ysl_file(ysl_path)
                entity = property_info.get("property_code", "unknown")
                name = property_info.get("property_name", f"Entity_{entity}")
                ysl_entities.add(str(entity))

                # Build the building folder name (e.g., "148 - 130 E. 18 Owners Corp.")
                building_folder_name = f"{entity} - {name}"
                building_dir = save_dir / building_folder_name
                building_dir.mkdir(parents=True, exist_ok=True)

                # Generate budget into the building folder
                output_name = f"{entity}_{name}_2027_Budget.xlsx"
                output_path = building_dir / output_name

                # Derive YTD months from period (e.g., "02/2026" → 2)
                _gen_ytd = 2
                try:
                    _gen_ytd = int(period.split("/")[0])
                except Exception:
                    pass

                success = populate_template(
                    template_path=TEMPLATE_PATH,
                    gl_data=gl_data,
                    property_info=property_info,
                    output_path=output_path,
                    ytd_months=_gen_ytd,
                    remaining_months=12 - _gen_ytd,
                )

                if success and output_path.exists():
                    # Merge assumptions for this building
                    merged = None
                    try:
                        merged = merge_assumptions(entity)
                    except Exception:
                        pass

                    # Store ALL GL data in database for budget review workflow
                    try:
                        fresh = request.form.get("fresh_start", "").lower() in ("true", "1", "yes")
                        workflow_helpers["store_all_lines"](entity, name, gl_data, TEMPLATE_PATH, assumptions=merged, fresh_start=fresh)
                        logger.info(f"All GL data stored for entity {entity} (fresh_start={fresh})")
                    except Exception as wfe:
                        logger.warning(f"Could not store GL data for {entity}: {wfe}")

                    # Apply assumptions to Excel file
                    try:
                        if merged:
                            apply_assumptions(output_path, merged)
                            logger.info(f"Assumptions applied for entity {entity}")
                    except Exception as ae:
                        logger.warning(f"Could not apply assumptions for {entity}: {ae}")

                    # Apply PM R&M projections if available
                    try:
                        pm_proj = workflow_helpers["get_pm_projections"](entity)
                        if pm_proj:
                            apply_pm_projections(output_path, pm_proj)
                            logger.info(f"PM projections applied for entity {entity}")
                    except Exception as pe:
                        logger.warning(f"Could not apply PM projections for {entity}: {pe}")

                    output_files.append((output_name, output_path))
                    size_kb = output_path.stat().st_size / 1024

                    # Archive YSL to central ysl_downloads/
                    shutil.copy2(ysl_path, ysl_archive / f.filename)

                    # Archive YSL to building's yardi_drops/
                    yardi_drops = building_dir / "yardi_drops"
                    yardi_drops.mkdir(exist_ok=True)
                    shutil.copy2(ysl_path, yardi_drops / f.filename)

                    results["success"].append({
                        "entity": entity,
                        "name": name,
                        "file": output_name,
                        "size_kb": round(size_kb),
                        "saved_to": str(building_dir),
                    })
                else:
                    results["failed"].append({
                        "entity": entity,
                        "file": f.filename,
                        "reason": "Pipeline returned failure",
                    })

            except Exception as e:
                logger.exception(f"Error processing {f.filename}")
                results["failed"].append({
                    "file": f.filename,
                    "reason": str(e),
                })

        # Pass 2: Process Expense Distribution files
        # Use entity code from filename (our script names them ExpenseDistribution_XXX.xlsx)
        # If that doesn't match the file contents, use the filename entity (Yardi bug workaround)
        for exp_path, exp_filename in expense_files:
            try:
                from expense_distribution import parse_expense_distribution
            except ImportError:
                from budget_app.expense_distribution import parse_expense_distribution
            try:
                exp_entity, exp_from, exp_to, exp_invoices = parse_expense_distribution(str(exp_path))
                logger.info(f"Expense parse: file={exp_filename}, file_entity={exp_entity}, invoices={len(exp_invoices) if exp_invoices else 0}")

                if not exp_invoices:
                    results["warnings"].append(f"Expense file {exp_filename}: no invoices found")
                    continue

                # Determine correct entity code:
                # 1. Try to extract from filename (ExpenseDistribution_204.xlsx → 204)
                # 2. If filename entity matches a YSL entity we just processed, use it
                # 3. Otherwise fall back to what's in the file
                import re as _re
                fname_match = _re.search(r'ExpenseDistribution[_-]?(\d+)', exp_filename)
                fname_entity = fname_match.group(1) if fname_match else None

                target_entity = exp_entity  # default: trust the file
                if fname_entity and fname_entity != exp_entity:
                    # Filename says one entity, file contents say another — Yardi bug
                    if fname_entity in ysl_entities:
                        # The filename matches a YSL we just processed, so trust the filename
                        target_entity = fname_entity
                        logger.warning(f"Yardi entity mismatch: file says {exp_entity}, filename says {fname_entity}. Using {fname_entity} (matches YSL upload)")
                        results["warnings"].append(f"Note: Yardi returned entity {exp_entity} data but file was requested for {fname_entity}. Stored under {fname_entity}.")
                    elif not ysl_entities:
                        # No YSL files uploaded — standalone expense upload, trust filename
                        target_entity = fname_entity
                        logger.warning(f"Yardi entity mismatch: file says {exp_entity}, filename says {fname_entity}. Using {fname_entity} (filename override)")
                    else:
                        # Filename doesn't match any YSL entity — log warning but store as-is
                        logger.warning(f"Yardi entity mismatch: file says {exp_entity}, filename says {fname_entity}. Neither matches YSL entities {ysl_entities}. Using file entity {exp_entity}.")

                report = ed_helpers["store_expense_report"](target_entity, exp_from, exp_to, exp_invoices, exp_filename)
                logger.info(f"Expense distribution stored for entity {target_entity}")

                # Apply accrual adjustments (prior-year invoices backed out of YTD)
                accrual_result = {"applied": 0, "accruals": {}}
                if exp_from:
                    try:
                        accrual_result = ed_helpers["apply_accrual_adjustments"](target_entity, report.id, exp_from)
                        if accrual_result["applied"] > 0:
                            logger.info(f"Auto-applied accrual adjustments to {accrual_result['applied']} GL lines for entity {target_entity}")
                    except Exception as accrual_err:
                        logger.error(f"Accrual adjustment failed for {target_entity}: {accrual_err}")
                        results["warnings"].append(f"Accrual adjustments failed for {target_entity}: {str(accrual_err)}")

                accrual_msg = f", {accrual_result['applied']} accrual adj" if accrual_result["applied"] > 0 else ""
                results["success"].append(f"Expense Distribution: {target_entity} ({len(exp_invoices)} invoices{accrual_msg})")
            except Exception as exp_err:
                logger.error(f"Expense parse error for {exp_filename}: {exp_err}")
                results["warnings"].append(f"Expense file {exp_filename} error: {str(exp_err)}")

        # Pass 3: Process Open AP (Aging) files
        # These provide unpaid invoice data that populates BudgetLine.unpaid_bills
        for ap_path, ap_filename in open_ap_files:
            try:
                ap_entity, ap_invoices = parse_open_ap_report(str(ap_path))
                logger.info(f"Open AP parse: file={ap_filename}, entity={ap_entity}, invoices={len(ap_invoices) if ap_invoices else 0}")

                if not ap_invoices:
                    results["warnings"].append(f"Open AP file {ap_filename}: no invoices found")
                    continue

                # Determine correct entity code
                # 1. Try to extract from filename (e.g., APAging_204.xlsx or OpenAP_204.xlsx)
                import re as _re
                fname_match = _re.search(r'(?:Aging|OpenAP|AP)[_-]?(\d+)', ap_filename, _re.IGNORECASE)
                fname_entity = fname_match.group(1) if fname_match else None

                target_entity = ap_entity  # default: from file data
                if fname_entity and fname_entity != ap_entity:
                    if fname_entity in ysl_entities:
                        target_entity = fname_entity
                        logger.warning(f"Open AP entity mismatch: data says {ap_entity}, filename says {fname_entity}. Using {fname_entity}")
                    elif not ysl_entities:
                        target_entity = fname_entity
                elif not target_entity and fname_entity:
                    target_entity = fname_entity

                if not target_entity:
                    results["warnings"].append(f"Open AP file {ap_filename}: could not determine entity code")
                    continue

                # Store parsed invoices
                report = oa_helpers["store_open_ap_report"](target_entity, ap_invoices, ap_filename)
                logger.info(f"Open AP stored for entity {target_entity}: {len(ap_invoices)} invoices, ${report.total_amount:,.2f}")

                # Auto-apply unpaid bills to BudgetLines
                unpaid_result = {"applied": 0, "gl_totals": {}}
                try:
                    unpaid_result = oa_helpers["apply_unpaid_bills"](target_entity)
                    if unpaid_result["applied"] > 0:
                        logger.info(f"Auto-applied unpaid bills to {unpaid_result['applied']} GL lines for entity {target_entity}")
                except Exception as unpaid_err:
                    logger.error(f"Unpaid bills application failed for {target_entity}: {unpaid_err}")
                    results["warnings"].append(f"Unpaid bills failed for {target_entity}: {str(unpaid_err)}")

                unpaid_msg = f", {unpaid_result['applied']} GL lines updated" if unpaid_result["applied"] > 0 else ""
                results["success"].append(f"Open AP: {target_entity} ({len(ap_invoices)} invoices, ${report.total_amount:,.2f}{unpaid_msg})")
            except Exception as ap_err:
                logger.error(f"Open AP parse error for {ap_filename}: {ap_err}")
                results["warnings"].append(f"Open AP file {ap_filename} error: {str(ap_err)}")

        # Pass 4: Process Maintenance Proof files
        for mp_path, mp_filename in maint_proof_files:
            try:
                report_title, units, total_shares = mp_helpers["parse_maintenance_proof"](str(mp_path))
                if not units:
                    results["warnings"].append(f"Maintenance Proof file {mp_filename}: no units found")
                    continue

                import re as _re
                fname_match = _re.search(r'(?:Adhoc_AMP|MaintProof|MP)[_-]?(\d+)', mp_filename, _re.IGNORECASE)
                fname_entity = fname_match.group(1) if fname_match else None

                target_entity = fname_entity
                if not target_entity and ysl_entities:
                    target_entity = next(iter(ysl_entities))

                if not target_entity:
                    results["warnings"].append(f"Maintenance Proof file {mp_filename}: could not determine entity code")
                    continue

                report = mp_helpers["store_maintenance_proof"](target_entity, report_title, units, total_shares, mp_filename)
                results["success"].append(f"Maintenance Proof: {target_entity} ({len(units)} units, {report_title})")
                logger.info(f"Maintenance proof stored for entity {target_entity}")
            except Exception as mp_err:
                logger.error(f"Maintenance proof parse error for {mp_filename}: {mp_err}")
                results["warnings"].append(f"Maintenance Proof file {mp_filename} error: {str(mp_err)}")

        if not output_files:
            # If expense files were processed successfully, return 200
            if results["success"]:
                return jsonify({"message": "Files processed", **results}), 200
            return jsonify({"error": "No budgets generated", **results}), 400

        # Always return results as JSON now (files are saved to disk)
        # Also provide a download link for convenience
        if len(output_files) == 1:
            name, path = output_files[0]
            buf = BytesIO(path.read_bytes())
            buf.seek(0)
            # Return the file as download, with results in a header
            resp = send_file(
                buf,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name=name,
            )
            resp.headers["X-Budget-Results"] = json.dumps(results)
            return resp
        else:
            zip_buf = BytesIO()
            with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for name, path in output_files:
                    zf.write(path, name)
            zip_buf.seek(0)
            resp = send_file(
                zip_buf,
                mimetype="application/zip",
                as_attachment=True,
                download_name="2027_Budgets.zip",
            )
            resp.headers["X-Budget-Results"] = json.dumps(results)
            return resp


@app.route("/api/defaults", methods=["GET", "POST"])
def api_defaults():
    """Get or save portfolio defaults."""
    if request.method == "GET":
        return jsonify(load_portfolio_defaults())

    data = request.json
    save_portfolio_defaults(data)
    return jsonify({"status": "saved"})


@app.route("/api/building-assumptions/<entity_code>", methods=["GET", "POST"])
def api_building_assumptions(entity_code):
    """Get or save building-specific assumptions."""
    if request.method == "GET":
        assumptions = load_building_assumptions()
        return jsonify(assumptions.get(entity_code, {}))

    data = request.json
    assumptions = load_building_assumptions()
    assumptions[entity_code] = data
    save_building_assumptions(assumptions)
    return jsonify({"status": "saved"})


@app.route("/api/assumptions/<entity_code>")
def merged_assumptions(entity_code):
    """Return merged assumptions (portfolio + building overrides)."""
    return jsonify(merge_assumptions(entity_code))


@app.route("/api/export-assumptions")
def bulk_export():
    """Export all building assumptions."""
    buildings = {b["entity_code"]: merge_assumptions(b["entity_code"]) for b in load_buildings()}
    return jsonify(buildings)


# ─── Monday.com Sync ─────────────────────────────────────────────────────────

MONDAY_API_TOKEN = os.environ.get("MONDAY_API_TOKEN", "")
MONDAY_BOARD_ID = "3473581362"  # Building Master List


@app.route("/api/sync-monday-fetch", methods=["GET"])
def sync_monday_fetch():
    """Fetch buildings, PM/FA assignments from Monday.com Building Master List."""
    if not MONDAY_API_TOKEN:
        return jsonify({"error": "MONDAY_API_TOKEN not configured"}), 500

    import urllib.request
    import ssl

    # Pull entity#, address, city, zip, type, units, PM, FA for all active buildings
    query = """{
      boards(ids: %s) {
        items_page(limit: 500, query_params: {rules: [{column_id: "color", compare_value: [0], operator: any_of}]}) {
          items {
            name
            column_values(ids: ["numeric", "numbers9", "pm8", "people", "text8", "text7", "text03", "status1"]) {
              id
              text
            }
          }
        }
      }
    }""" % MONDAY_BOARD_ID

    req = urllib.request.Request(
        "https://api.monday.com/v2",
        data=json.dumps({"query": query}).encode("utf-8"),
        headers={
            "Authorization": MONDAY_API_TOKEN,
            "Content-Type": "application/json",
            "API-Version": "2024-10",
        },
    )

    # Use default SSL context, fallback to unverified for local dev (macOS)
    ctx = None
    try:
        ctx = ssl.create_default_context()
    except Exception:
        ctx = ssl.create_default_context()
    if not IS_CLOUD:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            result = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.error(f"Monday.com API error: {e}")
        return jsonify({"error": f"Monday.com API error: {str(e)}"}), 500

    items = result.get("data", {}).get("boards", [{}])[0].get("items_page", {}).get("items", [])

    buildings = []
    for item in items:
        cols = {c["id"]: c["text"] for c in item.get("column_values", [])}
        entity = cols.get("numeric", "").replace(" ", "").split(".")[0]
        if not entity:
            continue
        units = cols.get("numbers9", "").replace(" ", "").split(".")[0]
        buildings.append({
            "entity_code": entity,
            "building_name": item["name"],
            "address": cols.get("text8") or "",
            "city": cols.get("text7") or "",
            "zip": cols.get("text03") or "",
            "type": cols.get("status1") or "",
            "units": units,
            "pm": cols.get("pm8") or None,
            "fa": cols.get("people") or None,
        })

    return jsonify({"buildings": buildings, "count": len(buildings)})


@app.route("/api/sync-monday", methods=["POST"])
def sync_monday():
    """
    Sync users and building assignments from Monday.com Building Master List.

    Expects JSON array of buildings:
    [{"entity_code": "204", "building_name": "...", "pm": "Name", "fa": "Name"}, ...]

    Creates/updates User records and BuildingAssignment records.
    """
    User = workflow_models["User"]
    BuildingAssignment = workflow_models["BuildingAssignment"]

    data = request.get_json()
    if not data or not isinstance(data, list):
        return jsonify({"error": "Expected JSON array of buildings"}), 400

    stats = {"users_created": 0, "users_updated": 0, "assignments_created": 0,
             "assignments_removed": 0, "buildings_synced": 0}

    # Collect all unique people names from the data
    people = {}  # name -> set of roles
    for bldg in data:
        pm = (bldg.get("pm") or "").strip()
        fa = (bldg.get("fa") or "").strip()
        if pm:
            people.setdefault(pm, set()).add("pm")
        if fa:
            people.setdefault(fa, set()).add("fa")

    # Ensure all people exist as users
    for name, roles in people.items():
        # A person can be both PM and FA — use their primary role
        primary_role = "pm" if "pm" in roles else "fa"

        user = User.query.filter_by(name=name).first()
        if not user:
            # Generate email from name: "Jacob Sirotkin" → "jsirotkin@centuryny.com"
            parts = name.lower().split()
            email_slug = parts[0][0] + parts[-1] if len(parts) > 1 else parts[0]
            email = f"{email_slug}@centuryny.com"

            # Handle duplicates
            existing = User.query.filter_by(email=email).first()
            if existing:
                # If same name, reuse; otherwise make unique
                if existing.name == name:
                    user = existing
                else:
                    email = f"{parts[0]}.{parts[-1]}@centuryny.com" if len(parts) > 1 else f"{parts[0]}2@centuryny.com"
                    user = User(name=name, email=email, role=primary_role)
                    db.session.add(user)
                    stats["users_created"] += 1
            else:
                user = User(name=name, email=email, role=primary_role)
                db.session.add(user)
                stats["users_created"] += 1

        db.session.flush()

    # Sync building assignments
    for bldg in data:
        entity_code = str(bldg.get("entity_code", "")).strip()
        if not entity_code:
            continue

        pm_name = (bldg.get("pm") or "").strip()
        fa_name = (bldg.get("fa") or "").strip()

        # Remove stale assignments for this entity
        existing_assignments = BuildingAssignment.query.filter_by(entity_code=entity_code).all()
        for a in existing_assignments:
            should_keep = False
            if a.role == "pm" and a.user and a.user.name == pm_name:
                should_keep = True
            if a.role == "fa" and a.user and a.user.name == fa_name:
                should_keep = True
            if not should_keep:
                db.session.delete(a)
                stats["assignments_removed"] += 1

        db.session.flush()

        # Create PM assignment
        if pm_name:
            pm_user = User.query.filter_by(name=pm_name).first()
            if pm_user:
                existing = BuildingAssignment.query.filter_by(
                    entity_code=entity_code, user_id=pm_user.id, role="pm"
                ).first()
                if not existing:
                    db.session.add(BuildingAssignment(
                        entity_code=entity_code, user_id=pm_user.id, role="pm"
                    ))
                    stats["assignments_created"] += 1

        # Create FA assignment
        if fa_name:
            fa_user = User.query.filter_by(name=fa_name).first()
            if fa_user:
                existing = BuildingAssignment.query.filter_by(
                    entity_code=entity_code, user_id=fa_user.id, role="fa"
                ).first()
                if not existing:
                    db.session.add(BuildingAssignment(
                        entity_code=entity_code, user_id=fa_user.id, role="fa"
                    ))
                    stats["assignments_created"] += 1

        stats["buildings_synced"] += 1

    db.session.commit()

    # Also sync buildings.csv from the same data
    csv_buildings = []
    for bldg in data:
        entity_code = str(bldg.get("entity_code", "")).strip()
        if not entity_code:
            continue
        csv_buildings.append({
            "entity_code": entity_code,
            "building_name": bldg.get("building_name", ""),
            "address": bldg.get("address", ""),
            "city": bldg.get("city", ""),
            "zip": bldg.get("zip", ""),
            "type": bldg.get("type", ""),
            "units": bldg.get("units", ""),
        })
    if csv_buildings:
        save_buildings(csv_buildings)
        stats["buildings_csv_updated"] = len(csv_buildings)

    logger.info(f"Monday.com sync complete: {stats}")
    return jsonify({"status": "ok", "stats": stats})


# Cache the template GL list
_template_gls_cache = None
def _get_template_gls():
    global _template_gls_cache
    if _template_gls_cache is None:
        from gl_mapper import build_gl_mapping
        _template_gls_cache = set(build_gl_mapping(TEMPLATE_PATH).keys())
    return _template_gls_cache


# ─── HTML Templates ──────────────────────────────────────────────────────────

HOME_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<title>Century Management Budget System</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f9fafb; }
  header {
    background: linear-gradient(135deg, #5a4a3f 0%, #3d342c 100%);
    color: white;
    padding: 60px 20px;
    text-align: center;
  }
  header h1 { font-size: 36px; font-weight: 700; margin-bottom: 8px; }
  header p { font-size: 16px; opacity: 0.95; }
  .container { max-width: 1200px; margin: 0 auto; padding: 40px 20px; }
  .section-label {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #9ca3af;
    margin-bottom: 12px;
    padding-left: 4px;
  }
  .section-group {
    margin-bottom: 40px;
  }
  .nav-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 20px;
  }
  .nav-card {
    background: white;
    border-radius: 12px;
    padding: 28px 24px;
    border: 1px solid #e5e0d5;
    text-decoration: none;
    color: #1a1714;
    transition: all 0.3s;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    display: flex;
    flex-direction: column;
  }
  .nav-card:hover {
    border-color: #5a4a3f;
    box-shadow: 0 10px 25px rgba(90,74,63, 0.15);
    transform: translateY(-4px);
  }
  .nav-card h2 {
    font-size: 18px;
    color: #5a4a3f;
    margin-bottom: 8px;
    font-weight: 600;
  }
  .nav-card p {
    font-size: 13px;
    color: #8a7e72;
    line-height: 1.5;
    flex-grow: 1;
    margin-bottom: 12px;
  }
  .nav-card .arrow {
    display: inline-block;
    color: #5a4a3f;
    font-weight: 600;
    font-size: 16px;
  }
  .nav-card:hover .arrow { transform: translateX(4px); transition: all 0.2s; }
  .icon {
    font-size: 28px;
    margin-bottom: 8px;
  }
  /* ── Global Nav ── */
  .top-nav { background: white; border-bottom: 1px solid #e5e0d5; padding: 0 20px; display: flex; align-items: center; height: 48px; position: sticky; top: 0; z-index: 100; }
  .top-nav .nav-brand { font-weight: 700; font-size: 15px; color: #5a4a3f; text-decoration: none; margin-right: 32px; }
  .top-nav .nav-links { display: flex; gap: 4px; }
  .top-nav .nav-link { padding: 6px 14px; font-size: 13px; font-weight: 500; color: #8a7e72; text-decoration: none; border-radius: 6px; transition: all 0.15s; }
  .top-nav .nav-link:hover { background: #ede9e1; color: #1a1714; }
  .top-nav .nav-link.active { background: #f5efe7; color: #5a4a3f; }
</style>
</head>
<body>
<!-- Global Nav -->
<nav class="top-nav">
  <a href="/" class="nav-brand">Century Management</a>
  <div class="nav-links">
    <a href="/" class="nav-link active">Home</a>
    <a href="/dashboard" class="nav-link">FA Dashboard</a>
    <a href="/pm" class="nav-link">PM Portal</a>
    <a href="/generate" class="nav-link">Generator</a>
    <a href="/audited-financials" class="nav-link">Audited Financials</a>
    <a href="/files" class="nav-link">Files</a>
  </div>
</nav>
  <header>
    <h1>Century Management</h1>
    <p>Budget & Assumptions System</p>
  </header>
  <div class="container">
    <div class="section-group">
      <div class="section-label">Setup</div>
      <div class="nav-grid">
        <a href="/admin" class="nav-card">
          <div class="icon">👤</div>
          <h2>User Management</h2>
          <p>Sync buildings, FAs, and PMs from Monday.com.</p>
          <span class="arrow">→</span>
        </a>
      </div>
    </div>

    <div class="section-group">
      <div class="section-label">Configuration</div>
      <div class="nav-grid">
        <a href="/assumptions" class="nav-card">
          <div class="icon">⚙️</div>
          <h2>Portfolio Defaults</h2>
          <p>Manage portfolio-wide default values for all buildings.</p>
          <span class="arrow">→</span>
        </a>
        <a href="/assumptions/buildings" class="nav-card">
          <div class="icon">📋</div>
          <h2>Building Assumptions</h2>
          <p>View and edit assumptions for individual buildings.</p>
          <span class="arrow">→</span>
        </a>
      </div>
    </div>

    <div class="section-group">
      <div class="section-label">Budget Process</div>
      <div class="nav-grid">
        <a href="/generate" class="nav-card">
          <div class="icon">📊</div>
          <h2>Budget Generator</h2>
          <p>Download YSL reports from Yardi and generate 2027 budgets in one click.</p>
          <span class="arrow">→</span>
        </a>
        <a href="/pm" class="nav-card">
          <div class="icon">🔧</div>
          <h2>PM Budget Review</h2>
          <p>Property managers: review and enter R&M budget projections.</p>
          <span class="arrow">→</span>
        </a>
        <a href="/dashboard" class="nav-card">
          <div class="icon">📈</div>
          <h2>FA Dashboard</h2>
          <p>Review budget status, manage workflow, approve PM submissions.</p>
          <span class="arrow">→</span>
        </a>
      </div>
    </div>

    <div class="section-group">
      <div class="section-label">Data Sources</div>
      <div class="nav-grid">
        <a href="/audited-financials" class="nav-card">
          <div class="icon">📋</div>
          <h2>Audited Financials</h2>
          <p>Extract and map audited financial data into budget templates.</p>
          <span class="arrow">→</span>
        </a>
        <a href="/files" class="nav-card">
          <div class="icon">📁</div>
          <h2>File Repository</h2>
          <p>Upload, browse, and manage supporting documents — maint proofs, YSL exports, GL detail, and more.</p>
          <span class="arrow">→</span>
        </a>
      </div>
    </div>
  </div>
</body>
</html>
"""

GENERATE_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<title>Budget Generator — Century Management</title>
<style>
  :root {
    --blue: #5a4a3f;
    --blue-light: #f5efe7;
    --green: #057a55;
    --green-light: #def7ec;
    --red: #e02424;
    --red-light: #fde8e8;
    --gray-50: #f4f1eb;
    --gray-100: #ede9e1;
    --gray-200: #e5e0d5;
    --gray-300: #d5cfc5;
    --gray-500: #8a7e72;
    --gray-700: #4a4039;
    --gray-900: #1a1714;
    --radius: 8px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--gray-50);
    color: var(--gray-900);
    line-height: 1.5;
  }
  .container { max-width: 900px; margin: 0 auto; padding: 40px 20px; }
  h1 { font-size: 28px; font-weight: 700; margin-bottom: 4px; }
  .subtitle { color: var(--gray-500); font-size: 15px; margin-bottom: 32px; }
  .back-link {
    color: var(--blue);
    text-decoration: none;
    font-weight: 600;
    padding: 8px 16px;
    border: 1px solid var(--blue);
    border-radius: 6px;
    transition: all 0.15s;
    display: inline-block;
    margin-bottom: 24px;
  }
  .back-link:hover { background: var(--blue-light); }

  /* Steps */
  .step {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: var(--radius);
    padding: 24px;
    margin-bottom: 20px;
  }
  .step-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 16px;
  }
  .step-num {
    background: var(--blue);
    color: white;
    width: 32px;
    height: 32px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: 700;
    font-size: 14px;
    flex-shrink: 0;
  }
  .step-title { font-size: 18px; font-weight: 600; }
  .step-desc { color: var(--gray-500); font-size: 14px; margin-bottom: 16px; }

  /* Building grid */
  .search-box {
    width: 100%;
    padding: 10px 14px;
    border: 1px solid var(--gray-300);
    border-radius: var(--radius);
    font-size: 14px;
    margin-bottom: 12px;
    outline: none;
  }
  .search-box:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(90,74,63,0.1); }
  .building-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 8px;
    max-height: 280px;
    overflow-y: auto;
    padding: 4px;
  }
  .building-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    border: 1px solid var(--gray-200);
    border-radius: 6px;
    cursor: pointer;
    transition: all 0.15s;
    font-size: 13px;
  }
  .building-item:hover { background: var(--blue-light); border-color: var(--blue); }
  .building-item.selected { background: var(--blue-light); border-color: var(--blue); }
  .building-item input { accent-color: var(--blue); }
  .select-actions { display: flex; gap: 8px; margin-bottom: 8px; }
  .select-actions button {
    background: none;
    border: none;
    color: var(--blue);
    cursor: pointer;
    font-size: 13px;
    padding: 4px 0;
  }
  .select-actions button:hover { text-decoration: underline; }

  /* Settings row */
  .settings-row {
    display: flex;
    gap: 16px;
    margin-bottom: 16px;
    flex-wrap: wrap;
  }
  .setting { flex: 1; min-width: 200px; }
  .setting label { display: block; font-size: 13px; font-weight: 600; margin-bottom: 4px; color: var(--gray-700); }
  .setting input {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid var(--gray-300);
    border-radius: 6px;
    font-size: 14px;
  }

  /* Script output */
  .script-box {
    position: relative;
    background: var(--gray-900);
    color: #e5e0d5;
    border-radius: var(--radius);
    padding: 16px;
    font-family: 'SF Mono', Monaco, Consolas, monospace;
    font-size: 12px;
    max-height: 200px;
    overflow: auto;
    white-space: pre-wrap;
    display: none;
  }
  .copy-btn {
    position: absolute;
    top: 8px;
    right: 8px;
    background: var(--gray-700);
    color: white;
    border: none;
    border-radius: 4px;
    padding: 6px 14px;
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
  }
  .copy-btn:hover { background: var(--gray-500); }
  .copy-btn.copied { background: var(--green); }

  /* Upload zone */
  .upload-zone {
    border: 2px dashed var(--gray-300);
    border-radius: var(--radius);
    padding: 40px;
    text-align: center;
    cursor: pointer;
    transition: all 0.2s;
  }
  .upload-zone:hover, .upload-zone.dragover {
    border-color: var(--blue);
    background: var(--blue-light);
  }
  .upload-zone svg { margin-bottom: 8px; }
  .upload-text { color: var(--gray-500); font-size: 14px; }
  .upload-text strong { color: var(--blue); }
  .file-list { margin-top: 12px; }
  .file-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 12px;
    background: var(--gray-100);
    border-radius: 6px;
    font-size: 13px;
    margin-bottom: 4px;
  }
  .file-item .remove { color: var(--red); cursor: pointer; border: none; background: none; font-size: 16px; }

  /* Buttons */
  .btn {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 10px 24px;
    border-radius: var(--radius);
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    border: none;
    transition: all 0.15s;
  }
  .btn-primary { background: var(--blue); color: white; }
  .btn-primary:hover { background: #3d342c; }
  .btn-primary:disabled { background: var(--gray-300); cursor: not-allowed; }
  .btn-green { background: var(--green); color: white; }
  .btn-green:hover { background: #046c4e; }
  .btn-green:disabled { background: var(--gray-300); cursor: not-allowed; }

  /* Results */
  .results { display: none; }
  .result-card {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    border-radius: 6px;
    margin-bottom: 6px;
    font-size: 14px;
  }
  .result-card.success { background: var(--green-light); }
  .result-card.failed { background: var(--red-light); }
  .result-info { display: flex; align-items: center; gap: 8px; }

  /* Progress */
  .progress-bar {
    display: none;
    height: 4px;
    background: var(--gray-200);
    border-radius: 2px;
    overflow: hidden;
    margin: 16px 0;
  }
  .progress-bar .fill {
    height: 100%;
    background: var(--blue);
    border-radius: 2px;
    animation: progress 2s ease-in-out infinite;
    width: 40%;
  }
  @keyframes progress {
    0% { transform: translateX(-100%); }
    100% { transform: translateX(350%); }
  }

  .count-badge {
    background: var(--blue);
    color: white;
    font-size: 12px;
    padding: 2px 10px;
    border-radius: 12px;
    font-weight: 600;
  }
  .page-header {
    background: linear-gradient(135deg, var(--blue) 0%, #3d342c 100%);
    color: white;
    padding: 30px 20px;
    margin-bottom: 0;
  }
  .page-header a { color: white; text-decoration: none; font-size: 14px; }
  .page-header a:hover { text-decoration: underline; }
  .page-header h1 { font-size: 28px; font-weight: 700; }
  .page-header p { font-size: 14px; opacity: 0.85; margin-top: 4px; }
</style>
</head>
<body>
<div class="page-header">
  <a href="/">← Home</a>
  <h1>Budget Generator</h1>
  <p>Download YSL reports from Yardi, then generate 2027 budgets in one click.</p>
</div>
<div class="container">

  <!-- STEP 1 -->
  <div class="step">
    <div class="step-header">
      <div class="step-num">1</div>
      <div class="step-title">Select Buildings & Run from Yardi</div>
    </div>
    <p class="step-desc">Pick buildings, copy the script into your Yardi Console (F12). Reports download locally and auto-upload to the budget system.</p>

    <input type="text" class="search-box" id="buildingSearch" placeholder="Search buildings..." oninput="filterBuildings()">
    <div class="select-actions">
      <button onclick="selectAll()">Select All</button>
      <button onclick="selectNone()">Select None</button>
      <span id="selectedCount" class="count-badge">0 selected</span>
    </div>
    <div class="building-grid" id="buildingGrid">
      {% for b in buildings %}
      <label class="building-item" data-code="{{ b.entity_code }}" data-name="{{ b.building_name|lower }}">
        <input type="checkbox" value="{{ b.entity_code }}" onchange="updateCount()">
        <span><strong>{{ b.entity_code }}</strong> — {{ b.building_name }}</span>
      </label>
      {% endfor %}
    </div>

    <div class="settings-row" style="margin-top: 16px;">
      <div class="setting">
        <label>Your Email</label>
        <input type="email" id="email" placeholder="you@centuryny.com">
      </div>
      <div class="setting">
        <label>Budget Period</label>
        <input type="text" id="period" value="02/2026" placeholder="MM/YYYY">
      </div>
      <div class="setting">
        <label style="display:flex; align-items:center; gap:8px; cursor:pointer;">
          <input type="checkbox" id="freshStart" style="width:16px; height:16px;">
          <span>Fresh Start</span>
        </label>
        <span style="font-size:11px; color:var(--gray-500);">Creates a brand new budget version. Existing budget stays untouched.</span>
      </div>
    </div>

    <div style="display:flex; gap:10px; flex-wrap:wrap;">
      <button class="btn btn-primary" onclick="generateScript()" id="genBtn">
        <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M16 18l2-2-2-2M8 18l-2-2 2-2M14 4l-4 16"/></svg>
        Generate Yardi Script
      </button>
      <button class="btn btn-primary" onclick="generateExpenseDistScript()" id="expBtn" style="background:var(--teal-600, #0d9488);">
        <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M9 7h6m0 10v-3m-3 3v-6m-3 6v-1M5 3h14a2 2 0 012 2v14a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2z"/></svg>
        Expense Distribution
      </button>
    </div>
    <p style="font-size:11px; color:var(--gray-500); margin-top:6px;">1. Run main script (YSL + Maint Proof + AP Aging). 2. Open APAnalytics in Yardi, select "Expense Distribution", run that script separately.</p>

    <div class="script-box" id="scriptBox">
      <button class="copy-btn" id="copyBtn" onclick="copyScript()">Copy</button>
      <code id="scriptCode"></code>
    </div>

    <div class="script-box" id="expScriptBox" style="display:none; border-color:var(--teal-200, #99f6e4);">
      <div style="background:#f0fdfa; padding:8px 12px; margin:-12px -12px 12px; border-radius:6px 6px 0 0; border-bottom:1px solid #99f6e4;">
        <strong style="color:#0d9488;">Expense Distribution</strong> — Open APAnalytics in Yardi, select "Expense Distribution" from the Report Type dropdown, then paste this script in the console (F12).
      </div>
      <button class="copy-btn" id="expCopyBtn" onclick="copyExpScript()">Copy Expense Dist</button>
      <code id="expScriptCode"></code>
    </div>

    <!-- AP Aging script box hidden — AP Aging is now in the combined script -->
    <div class="script-box" id="apScriptBox" style="display:none; border-color:var(--amber-200, #fde68a);">
      <button class="copy-btn" id="apCopyBtn" onclick="copyAPScript()">Copy AP Aging</button>
      <code id="apScriptCode"></code>
    </div>
  </div>

  <!-- Auto-upload status (populated by Yardi script) -->
  <div id="autoStatus" style="display:none; margin-top:16px; padding:16px; background:#f0fdf4; border:1px solid #86efac; border-radius:8px;">
    <p style="margin:0; font-weight:600; color:#166534;">Files auto-uploaded and processed. Check the FA Dashboard for results.</p>
  </div>

  <!-- Manual upload fallback -->
  <div style="margin-top:24px; padding:16px; background:#f8f5f0; border:1px solid #e8e0d4; border-radius:8px;">
    <p style="margin:0 0 8px; font-weight:600; color:#5a4a3f;">Manual Upload</p>
    <p style="margin:0 0 12px; font-size:12px; color:#8a7a6f;">Upload Yardi Excel files directly (YSL, Expense Distribution, AP Aging, Maintenance Proof). The app auto-detects file types.</p>
    <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
      <input type="file" id="manualFiles" multiple accept=".xlsx,.xls" style="font-size:13px;">
      <button class="btn btn-primary" id="uploadBtn" onclick="manualUpload()" style="background:#5a4a3f;">Upload &amp; Process</button>
    </div>
  </div>
</div>

<script>
// Auto-upload: no manual file management needed

function getSelected() {
  return [...document.querySelectorAll('#buildingGrid input:checked')].map(c => parseInt(c.value));
}

function updateCount() {
  const n = getSelected().length;
  document.getElementById('selectedCount').textContent = n + ' selected';
  document.querySelectorAll('.building-item').forEach(el => {
    el.classList.toggle('selected', el.querySelector('input').checked);
  });
}

function selectAll() {
  document.querySelectorAll('#buildingGrid input').forEach(c => {
    if (!c.closest('.building-item').style.display || c.closest('.building-item').style.display !== 'none')
      c.checked = true;
  });
  updateCount();
}

function selectNone() {
  document.querySelectorAll('#buildingGrid input').forEach(c => c.checked = false);
  updateCount();
}

function filterBuildings() {
  const q = document.getElementById('buildingSearch').value.toLowerCase();
  document.querySelectorAll('.building-item').forEach(el => {
    const code = el.dataset.code;
    const name = el.dataset.name;
    el.style.display = (code.includes(q) || name.includes(q)) ? '' : 'none';
  });
}

async function generateScript() {
  const entities = getSelected();
  const email = document.getElementById('email').value;
  const period = document.getElementById('period').value;

  if (!entities.length) { alert('Select at least one building'); return; }
  if (!email) { alert('Enter your email'); return; }

  const freshStart = document.getElementById('freshStart').checked;
  const resp = await fetch('/api/generate-script', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ entities, email, period, fresh_start: freshStart }),
  });

  const data = await resp.json();
  if (data.error) { alert(data.error); return; }

  document.getElementById('scriptCode').textContent = data.script;
  document.getElementById('scriptBox').style.display = 'block';
}

function copyScript() {
  const code = document.getElementById('scriptCode').textContent;
  navigator.clipboard.writeText(code).then(() => {
    const btn = document.getElementById('copyBtn');
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(() => { btn.textContent = 'Copy'; btn.classList.remove('copied'); }, 2000);
  });
}

async function generateExpenseDistScript() {
  const entities = getSelected();
  const email = document.getElementById('email').value;
  const period = document.getElementById('period').value;

  if (!entities.length) { alert('Select at least one building'); return; }

  const resp = await fetch('/api/generate-expense-dist-script', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ entities, email, period }),
  });

  const data = await resp.json();
  if (data.error) { alert(data.error); return; }

  document.getElementById('expScriptCode').textContent = data.script;
  document.getElementById('expScriptBox').style.display = 'block';
}

function copyExpScript() {
  const code = document.getElementById('expScriptCode').textContent;
  navigator.clipboard.writeText(code).then(() => {
    const btn = document.getElementById('expCopyBtn');
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(() => { btn.textContent = 'Copy Expense Dist'; btn.classList.remove('copied'); }, 2000);
  });
}

async function generateAPAgingScript() {
  const entities = getSelected();
  const email = document.getElementById('email').value;
  const period = document.getElementById('period').value;

  if (!entities.length) { alert('Select at least one building'); return; }

  const resp = await fetch('/api/generate-ap-aging-script', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ entities, email, period }),
  });

  const data = await resp.json();
  if (data.error) { alert(data.error); return; }

  document.getElementById('apScriptCode').textContent = data.script;
  document.getElementById('apScriptBox').style.display = 'block';
}

function copyAPScript() {
  const code = document.getElementById('apScriptCode').textContent;
  navigator.clipboard.writeText(code).then(() => {
    const btn = document.getElementById('apCopyBtn');
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(() => { btn.textContent = 'Copy AP Aging'; btn.classList.remove('copied'); }, 2000);
  });
}

// ── Manual Upload (fallback for files that don't auto-upload) ──
async function manualUpload() {
  const input = document.getElementById('manualFiles');
  if (!input.files.length) { alert('Select at least one file'); return; }

  const formData = new FormData();
  for (const f of input.files) formData.append('files', f);

  const btn = document.getElementById('uploadBtn');
  btn.disabled = true;
  btn.textContent = 'Processing...';

  try {
    const resp = await fetch('/api/process', { method: 'POST', body: formData });
    const data = await resp.json();
    if (data.error) { alert('Error: ' + data.error); return; }

    let msg = '';
    if (data.success?.length) msg += 'Success: ' + data.success.join(', ') + '\\n';
    if (data.warnings?.length) msg += 'Warnings: ' + data.warnings.join(', ') + '\\n';
    if (data.failed?.length) msg += 'Failed: ' + data.failed.join(', ');
    alert(msg || 'Files processed.');
    input.value = '';
  } catch (err) {
    alert('Upload failed: ' + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Upload & Process';
  }
}
</script>
</body>
</html>
"""

# MANAGE_TEMPLATE removed — buildings now sync from Monday.com

_MANAGE_REMOVED = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<title>Manage Buildings — Century Management</title>
<style>
  :root {
    --blue: #5a4a3f;
    --blue-light: #f5efe7;
    --green: #057a55;
    --green-light: #def7ec;
    --red: #e02424;
    --red-light: #fde8e8;
    --gray-50: #f4f1eb;
    --gray-100: #ede9e1;
    --gray-200: #e5e0d5;
    --gray-300: #d5cfc5;
    --gray-500: #8a7e72;
    --gray-700: #4a4039;
    --gray-900: #1a1714;
    --radius: 8px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--gray-50);
    color: var(--gray-900);
    line-height: 1.5;
  }
  .container { max-width: 1200px; margin: 0 auto; padding: 40px 20px; }
  h1 { font-size: 28px; font-weight: 700; margin-bottom: 4px; }
  .subtitle { color: var(--gray-500); font-size: 15px; margin-bottom: 32px; }
  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 32px;
  }
  .back-link {
    color: var(--blue);
    text-decoration: none;
    font-weight: 600;
    padding: 8px 16px;
    border: 1px solid var(--blue);
    border-radius: 6px;
    transition: all 0.15s;
  }
  .back-link:hover { background: var(--blue-light); }

  /* Card */
  .card {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: var(--radius);
    padding: 24px;
    margin-bottom: 24px;
  }
  .card-title { font-size: 18px; font-weight: 600; margin-bottom: 16px; }

  /* Form */
  .form-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 12px;
    margin-bottom: 16px;
  }
  .form-group {
    display: flex;
    flex-direction: column;
  }
  .form-group label {
    font-size: 13px;
    font-weight: 600;
    margin-bottom: 4px;
    color: var(--gray-700);
  }
  .form-group input {
    padding: 8px 12px;
    border: 1px solid var(--gray-300);
    border-radius: 6px;
    font-size: 14px;
    outline: none;
  }
  .form-group input:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(90,74,63,0.1); }

  /* Search */
  .search-box {
    width: 100%;
    padding: 10px 14px;
    border: 1px solid var(--gray-300);
    border-radius: var(--radius);
    font-size: 14px;
    margin-bottom: 16px;
    outline: none;
  }
  .search-box:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(90,74,63,0.1); }

  /* Table */
  .table-wrapper {
    overflow-x: auto;
    border: 1px solid var(--gray-200);
    border-radius: var(--radius);
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
  }
  th {
    background: var(--gray-100);
    padding: 12px;
    text-align: left;
    font-weight: 600;
    border-bottom: 1px solid var(--gray-200);
  }
  td {
    padding: 12px;
    border-bottom: 1px solid var(--gray-200);
  }
  tr:hover { background: var(--gray-50); }
  .code-cell { font-weight: 600; color: var(--blue); }

  /* Buttons */
  .btn {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 10px 24px;
    border-radius: var(--radius);
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    border: none;
    transition: all 0.15s;
  }
  .btn-primary { background: var(--blue); color: white; }
  .btn-primary:hover { background: #3d342c; }
  .btn-primary:disabled { background: var(--gray-300); cursor: not-allowed; }
  .btn-sm {
    padding: 6px 12px;
    font-size: 12px;
    gap: 4px;
  }
  .btn-edit {
    background: var(--blue);
    color: white;
    padding: 6px 12px;
    border: none;
    border-radius: 4px;
    font-size: 12px;
    cursor: pointer;
    transition: all 0.15s;
  }
  .btn-edit:hover { background: #3d342c; }
  .btn-delete {
    background: var(--red);
    color: white;
    padding: 6px 12px;
    border: none;
    border-radius: 4px;
    font-size: 12px;
    cursor: pointer;
    transition: all 0.15s;
    margin-left: 4px;
  }
  .btn-delete:hover { background: #c91c1c; }

  /* Modal */
  .modal {
    display: none;
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0,0,0,0.5);
    z-index: 1000;
    align-items: center;
    justify-content: center;
  }
  .modal.active { display: flex; }
  .modal-content {
    background: white;
    border-radius: var(--radius);
    padding: 24px;
    max-width: 500px;
    width: 90%;
    max-height: 90vh;
    overflow-y: auto;
  }
  .modal-title { font-size: 18px; font-weight: 600; margin-bottom: 16px; }
  .modal-actions {
    display: flex;
    gap: 8px;
    margin-top: 24px;
    justify-content: flex-end;
  }

  /* Alert */
  .alert {
    padding: 12px 16px;
    border-radius: 6px;
    margin-bottom: 16px;
    font-size: 14px;
    display: none;
  }
  .alert.active { display: block; }
  .alert.success { background: var(--green-light); color: var(--green); }
  .alert.error { background: var(--red-light); color: var(--red); }

  /* Empty state */
  .empty-state {
    text-align: center;
    padding: 40px;
    color: var(--gray-500);
  }
  .empty-state p { margin-bottom: 8px; }
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <div>
      <h1>Manage Buildings</h1>
      <p class="subtitle">Add, edit, or remove buildings from the budget generator.</p>
    </div>
    <a href="/" class="back-link">← Back to Home</a>
  </div>

  <div id="alert" class="alert"></div>

  <!-- Add Building Card -->
  <div class="card">
    <div class="card-title">Add New Building</div>
    <div class="form-grid">
      <div class="form-group">
        <label>Entity Code</label>
        <input type="text" id="newEntityCode" placeholder="e.g., 148">
      </div>
      <div class="form-group">
        <label>Building Name</label>
        <input type="text" id="newBuildingName" placeholder="e.g., Main Office">
      </div>
      <div class="form-group">
        <label>Address</label>
        <input type="text" id="newAddress" placeholder="e.g., 130 E. 18th St">
      </div>
      <div class="form-group">
        <label>City</label>
        <input type="text" id="newCity" placeholder="e.g., New York">
      </div>
      <div class="form-group">
        <label>ZIP</label>
        <input type="text" id="newZIP" placeholder="e.g., 10003">
      </div>
      <div class="form-group">
        <label>Type</label>
        <input type="text" id="newType" placeholder="e.g., Residential">
      </div>
      <div class="form-group">
        <label>Units</label>
        <input type="text" id="newUnits" placeholder="e.g., 50">
      </div>
    </div>
    <button class="btn btn-primary" onclick="addBuilding()">
      + Add Building
    </button>
  </div>

  <!-- Buildings List Card -->
  <div class="card">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
      <div class="card-title">Buildings (<span id="buildingCount">0</span>)</div>
    </div>
    <input type="text" class="search-box" id="searchBox" placeholder="Search buildings..." oninput="filterTable()">
    <div class="table-wrapper">
      <table id="buildingsTable">
        <thead>
          <tr>
            <th>Entity Code</th>
            <th>Building Name</th>
            <th>Address</th>
            <th>City</th>
            <th>ZIP</th>
            <th>Type</th>
            <th>Units</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody id="buildingsBody">
        </tbody>
      </table>
    </div>
    <div id="emptyState" class="empty-state" style="display: none;">
      <p>No buildings found.</p>
    </div>
  </div>
</div>

<!-- Edit Modal -->
<div id="editModal" class="modal">
  <div class="modal-content">
    <div class="modal-title">Edit Building</div>
    <div class="form-grid" style="grid-template-columns: 1fr;">
      <div class="form-group">
        <label>Entity Code (read-only)</label>
        <input type="text" id="editEntityCode" readonly style="background: var(--gray-100); cursor: not-allowed;">
      </div>
      <div class="form-group">
        <label>Building Name</label>
        <input type="text" id="editBuildingName">
      </div>
      <div class="form-group">
        <label>Address</label>
        <input type="text" id="editAddress">
      </div>
      <div class="form-group">
        <label>City</label>
        <input type="text" id="editCity">
      </div>
      <div class="form-group">
        <label>ZIP</label>
        <input type="text" id="editZIP">
      </div>
      <div class="form-group">
        <label>Type</label>
        <input type="text" id="editType">
      </div>
      <div class="form-group">
        <label>Units</label>
        <input type="text" id="editUnits">
      </div>
    </div>
    <div class="modal-actions">
      <button class="btn" style="background: var(--gray-200); color: var(--gray-900);" onclick="closeEditModal()">Cancel</button>
      <button class="btn btn-primary" onclick="saveEdit()">Save Changes</button>
    </div>
  </div>
</div>

<script>
let buildings = [];

async function loadBuildings() {
  const resp = await fetch('/api/buildings');
  buildings = await resp.json();
  renderTable();
  updateCount();
}

function renderTable() {
  const tbody = document.getElementById('buildingsBody');
  const searchTerm = document.getElementById('searchBox').value.toLowerCase();

  const filtered = buildings.filter(b =>
    b.entity_code.toLowerCase().includes(searchTerm) ||
    b.building_name.toLowerCase().includes(searchTerm) ||
    b.address.toLowerCase().includes(searchTerm) ||
    b.city.toLowerCase().includes(searchTerm)
  );

  const empty = document.getElementById('emptyState');
  if (filtered.length === 0) {
    tbody.innerHTML = '';
    empty.style.display = 'block';
    return;
  }
  empty.style.display = 'none';

  tbody.innerHTML = filtered.map(b =>
    `<tr>
      <td class="code-cell">${escapeHtml(b.entity_code)}</td>
      <td>${escapeHtml(b.building_name)}</td>
      <td>${escapeHtml(b.address)}</td>
      <td>${escapeHtml(b.city)}</td>
      <td>${escapeHtml(b.zip)}</td>
      <td>${escapeHtml(b.type)}</td>
      <td>${escapeHtml(b.units)}</td>
      <td>
        <button class="btn-edit" onclick="openEditModal('${escapeAttr(b.entity_code)}')">Edit</button>
        <button class="btn-delete" onclick="deleteBuilding('${escapeAttr(b.entity_code)}')">Delete</button>
      </td>
    </tr>`
  ).join('');
}

function filterTable() {
  renderTable();
}

function updateCount() {
  document.getElementById('buildingCount').textContent = buildings.length;
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function escapeAttr(text) {
  return text.replace(/'/g, "\\'");
}

async function addBuilding() {
  const code = document.getElementById('newEntityCode').value.trim();
  const name = document.getElementById('newBuildingName').value.trim();
  const address = document.getElementById('newAddress').value.trim();
  const city = document.getElementById('newCity').value.trim();
  const zip = document.getElementById('newZIP').value.trim();
  const type = document.getElementById('newType').value.trim();
  const units = document.getElementById('newUnits').value.trim();

  if (!code || !name) {
    showAlert('Entity code and building name are required', 'error');
    return;
  }

  const resp = await fetch('/api/buildings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ entity_code: code, building_name: name, address, city, zip, type, units }),
  });

  if (resp.ok) {
    document.getElementById('newEntityCode').value = '';
    document.getElementById('newBuildingName').value = '';
    document.getElementById('newAddress').value = '';
    document.getElementById('newCity').value = '';
    document.getElementById('newZIP').value = '';
    document.getElementById('newType').value = '';
    document.getElementById('newUnits').value = '';
    await loadBuildings();
    showAlert(`Building "${name}" added successfully`, 'success');
  } else {
    const data = await resp.json();
    showAlert(data.error || 'Failed to add building', 'error');
  }
}

function openEditModal(code) {
  const building = buildings.find(b => b.entity_code === code);
  if (!building) return;
  document.getElementById('editEntityCode').value = building.entity_code;
  document.getElementById('editBuildingName').value = building.building_name;
  document.getElementById('editAddress').value = building.address;
  document.getElementById('editCity').value = building.city;
  document.getElementById('editZIP').value = building.zip;
  document.getElementById('editType').value = building.type;
  document.getElementById('editUnits').value = building.units;
  document.getElementById('editModal').classList.add('active');
}

function closeEditModal() {
  document.getElementById('editModal').classList.remove('active');
}

async function saveEdit() {
  const code = document.getElementById('editEntityCode').value;
  const name = document.getElementById('editBuildingName').value.trim();
  const address = document.getElementById('editAddress').value.trim();
  const city = document.getElementById('editCity').value.trim();
  const zip = document.getElementById('editZIP').value.trim();
  const type = document.getElementById('editType').value.trim();
  const units = document.getElementById('editUnits').value.trim();

  if (!name) {
    showAlert('Building name is required', 'error');
    return;
  }

  const resp = await fetch(`/api/buildings/${code}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ building_name: name, address, city, zip, type, units }),
  });

  if (resp.ok) {
    closeEditModal();
    await loadBuildings();
    showAlert(`Building "${name}" updated successfully`, 'success');
  } else {
    const data = await resp.json();
    showAlert(data.error || 'Failed to update building', 'error');
  }
}

async function deleteBuilding(code) {
  const building = buildings.find(b => b.entity_code === code);
  if (!confirm(`Are you sure you want to delete "${building.building_name}"?`)) return;

  const resp = await fetch(`/api/buildings/${code}/delete`, { method: 'POST' });

  if (resp.ok) {
    await loadBuildings();
    showAlert(`Building deleted successfully`, 'success');
  } else {
    const data = await resp.json();
    showAlert(data.error || 'Failed to delete building', 'error');
  }
}

function showAlert(msg, type) {
  const el = document.getElementById('alert');
  el.textContent = msg;
  el.className = `alert active ${type}`;
  setTimeout(() => el.classList.remove('active'), 4000);
}

// Load on page load
document.addEventListener('DOMContentLoaded', loadBuildings);
</script>
</body>
</html>
"""

ASSUMPTIONS_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <title>Portfolio Defaults</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; background: #f5f5f5; }
        header { background: #5a4a3f; color: white; padding: 24px; }
        header h1 { font-size: 24px; font-weight: 600; }
        .container { max-width: 900px; margin: 0 auto; padding: 40px 20px; }
        .back-link { display: inline-block; margin-bottom: 24px; color: #5a4a3f; text-decoration: none; font-size: 14px; }
        .back-link:hover { text-decoration: underline; }
        .section { background: white; border-radius: 8px; padding: 24px; margin-bottom: 24px; border: 1px solid #e0e0e0; }
        .section h2 { font-size: 18px; color: #5a4a3f; margin-bottom: 16px; font-weight: 600; }
        .form-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 16px; }
        .form-group { display: flex; flex-direction: column; }
        .form-group label { font-size: 13px; font-weight: 500; color: #333; margin-bottom: 6px; }
        .form-group input, .form-group select { padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; }
        .form-group input:focus, .form-group select:focus { outline: none; border-color: #5a4a3f; box-shadow: 0 0 0 2px rgba(90,74,63, 0.1); }
        .form-group input[type="number"] { font-family: 'Courier New', monospace; }
        .button-row { display: flex; gap: 12px; margin-top: 24px; }
        button { padding: 10px 20px; border: none; border-radius: 4px; font-size: 14px; font-weight: 500; cursor: pointer; }
        .btn-primary { background: #5a4a3f; color: white; }
        .btn-primary:hover { background: #0f1f38; }
        .toast { position: fixed; bottom: 20px; right: 20px; background: #4caf50; color: white; padding: 16px 20px; border-radius: 4px; display: none; z-index: 1000; }
        .toast.show { display: block; animation: slideIn 0.3s ease; }
        @keyframes slideIn { from { transform: translateX(400px); } to { transform: translateX(0); } }
    </style>
</head>
<body>
    <header>
        <h1>Portfolio Defaults</h1>
    </header>
    <div class="container">
        <a href="/" class="back-link">← Back to Home</a>

        <div class="section">
            <h2>Payroll Tax Rates</h2>
            <div class="form-grid">
                <div class="form-group">
                    <label>FICA</label>
                    <input type="number" step="0.0001" id="fica" />
                </div>
                <div class="form-group">
                    <label>SUI</label>
                    <input type="number" step="0.0001" id="sui" />
                </div>
                <div class="form-group">
                    <label>FUI</label>
                    <input type="number" step="0.0001" id="fui" />
                </div>
                <div class="form-group">
                    <label>MTA</label>
                    <input type="number" step="0.0001" id="mta" />
                </div>
                <div class="form-group">
                    <label>NYS Disability</label>
                    <input type="number" step="0.0001" id="nys_disability" />
                </div>
                <div class="form-group">
                    <label>Paid Family Leave</label>
                    <input type="number" step="0.0001" id="pfl" />
                </div>
            </div>
        </div>

        <div class="section">
            <h2>32BJ Union Benefits</h2>
            <div class="form-grid">
                <div class="form-group">
                    <label>Welfare ($/mo)</label>
                    <input type="number" step="0.01" id="welfare_monthly" />
                </div>
                <div class="form-group">
                    <label>Pension ($/wk)</label>
                    <input type="number" step="0.01" id="pension_weekly" />
                </div>
                <div class="form-group">
                    <label>Supp Retirement ($/wk)</label>
                    <input type="number" step="0.01" id="supp_retirement_weekly" />
                </div>
                <div class="form-group">
                    <label>Legal ($/mo)</label>
                    <input type="number" step="0.01" id="legal_monthly" />
                </div>
                <div class="form-group">
                    <label>Training ($/mo)</label>
                    <input type="number" step="0.01" id="training_monthly" />
                </div>
                <div class="form-group">
                    <label>Profit Sharing ($/qtr)</label>
                    <input type="number" step="0.01" id="profit_sharing_quarterly" />
                </div>
            </div>
        </div>

        <div class="section">
            <h2>Workers Comp</h2>
            <div class="form-grid">
                <div class="form-group">
                    <label>Workers Comp %</label>
                    <input type="number" step="0.0001" id="workers_comp_percent" />
                </div>
            </div>
        </div>

        <div class="section">
            <h2>Mid-Year Wage Increase</h2>
            <div class="form-grid">
                <div class="form-group">
                    <label>Increase %</label>
                    <input type="number" step="0.0001" id="wage_increase_percent" />
                </div>
                <div class="form-group">
                    <label>Effective Week</label>
                    <input type="text" id="wage_increase_effective_week" />
                </div>
                <div class="form-group">
                    <label>Pre-increase Weeks</label>
                    <input type="number" id="wage_increase_pre_weeks" />
                </div>
                <div class="form-group">
                    <label>Post-increase Weeks</label>
                    <input type="number" id="wage_increase_post_weeks" />
                </div>
            </div>
        </div>

        <div class="section">
            <h2>Insurance Renewal</h2>
            <div class="form-grid">
                <div class="form-group">
                    <label>Expected Renewal Increase %</label>
                    <input type="number" step="0.0001" id="insurance_renewal_increase_percent" />
                </div>
                <div class="form-group">
                    <label>Renewal Effective Date</label>
                    <input type="text" id="insurance_renewal_effective_date" />
                </div>
                <div class="form-group">
                    <label>Pre-renewal Months</label>
                    <input type="number" id="insurance_renewal_pre_months" />
                </div>
                <div class="form-group">
                    <label>Post-renewal Months</label>
                    <input type="number" id="insurance_renewal_post_months" />
                </div>
            </div>
        </div>

        <div class="section">
            <h2>Energy Rates</h2>
            <div class="form-grid">
                <div class="form-group">
                    <label>Gas ESCO Rate ($/Therm)</label>
                    <input type="number" step="0.0001" id="energy_gas_esco_rate" />
                </div>
                <div class="form-group">
                    <label>Electric ESCO Rate ($/kWh)</label>
                    <input type="number" step="0.0001" id="energy_electric_esco_rate" />
                </div>
                <div class="form-group">
                    <label>Gas Rate Increase %</label>
                    <input type="number" step="0.0001" id="energy_gas_rate_increase" />
                </div>
                <div class="form-group">
                    <label>Electric Rate Increase %</label>
                    <input type="number" step="0.0001" id="energy_electric_rate_increase" />
                </div>
                <div class="form-group">
                    <label>Oil/Fuel Price ($/Gallon)</label>
                    <input type="number" step="0.01" id="energy_oil_price" />
                </div>
                <div class="form-group">
                    <label>Oil/Fuel Rate Increase %</label>
                    <input type="number" step="0.0001" id="energy_oil_rate_increase" />
                </div>
                <div class="form-group">
                    <label>Consumption Basis</label>
                    <select id="energy_consumption_basis">
                        <option value="2-Year Average">2-Year Average</option>
                        <option value="Prior Year">Prior Year</option>
                        <option value="Custom">Custom</option>
                    </select>
                </div>
            </div>
        </div>

        <div class="section">
            <h2>Water & Sewer</h2>
            <div class="form-grid">
                <div class="form-group">
                    <label>Rate Increase %</label>
                    <input type="number" step="0.0001" id="water_sewer_rate_increase" />
                </div>
            </div>
        </div>

        <div class="button-row">
            <button class="btn-primary" onclick="saveDefaults()">Save Defaults</button>
        </div>
    </div>

    <div class="toast" id="toast"></div>

    <script>
        const data = {{ data | safe }};

        function loadUI() {
            // Payroll tax
            document.getElementById('fica').value = data.payroll_tax.FICA;
            document.getElementById('sui').value = data.payroll_tax.SUI;
            document.getElementById('fui').value = data.payroll_tax.FUI;
            document.getElementById('mta').value = data.payroll_tax.MTA;
            document.getElementById('nys_disability').value = data.payroll_tax.NYS_Disability;
            document.getElementById('pfl').value = data.payroll_tax.PFL;

            // Union benefits
            document.getElementById('welfare_monthly').value = data.union_benefits.welfare_monthly;
            document.getElementById('pension_weekly').value = data.union_benefits.pension_weekly;
            document.getElementById('supp_retirement_weekly').value = data.union_benefits.supp_retirement_weekly;
            document.getElementById('legal_monthly').value = data.union_benefits.legal_monthly;
            document.getElementById('training_monthly').value = data.union_benefits.training_monthly;
            document.getElementById('profit_sharing_quarterly').value = data.union_benefits.profit_sharing_quarterly;

            // Workers comp
            document.getElementById('workers_comp_percent').value = data.workers_comp.percent;

            // Wage increase
            document.getElementById('wage_increase_percent').value = data.wage_increase.percent;
            document.getElementById('wage_increase_effective_week').value = data.wage_increase.effective_week;
            document.getElementById('wage_increase_pre_weeks').value = data.wage_increase.pre_increase_weeks;
            document.getElementById('wage_increase_post_weeks').value = data.wage_increase.post_increase_weeks;

            // Insurance renewal
            document.getElementById('insurance_renewal_increase_percent').value = data.insurance_renewal.increase_percent;
            document.getElementById('insurance_renewal_effective_date').value = data.insurance_renewal.effective_date;
            document.getElementById('insurance_renewal_pre_months').value = data.insurance_renewal.pre_renewal_months;
            document.getElementById('insurance_renewal_post_months').value = data.insurance_renewal.post_renewal_months;

            // Energy
            if (data.energy) {
                document.getElementById('energy_gas_esco_rate').value = data.energy.gas_esco_rate || 0;
                document.getElementById('energy_electric_esco_rate').value = data.energy.electric_esco_rate || 0;
                document.getElementById('energy_oil_price').value = data.energy.oil_price_per_gallon || 0;
                document.getElementById('energy_gas_rate_increase').value = data.energy.gas_rate_increase || 0;
                document.getElementById('energy_electric_rate_increase').value = data.energy.electric_rate_increase || 0;
                document.getElementById('energy_oil_rate_increase').value = data.energy.oil_rate_increase || 0;
                document.getElementById('energy_consumption_basis').value = data.energy.consumption_basis || '2-Year Average';
            }

            // Water & Sewer
            if (data.water_sewer) {
                document.getElementById('water_sewer_rate_increase').value = data.water_sewer.rate_increase || 0;
            }
        }

        function saveDefaults() {
            const payload = {
                payroll_tax: {
                    FICA: parseFloat(document.getElementById('fica').value),
                    SUI: parseFloat(document.getElementById('sui').value),
                    FUI: parseFloat(document.getElementById('fui').value),
                    MTA: parseFloat(document.getElementById('mta').value),
                    NYS_Disability: parseFloat(document.getElementById('nys_disability').value),
                    PFL: parseFloat(document.getElementById('pfl').value)
                },
                union_benefits: {
                    welfare_monthly: parseFloat(document.getElementById('welfare_monthly').value),
                    pension_weekly: parseFloat(document.getElementById('pension_weekly').value),
                    supp_retirement_weekly: parseFloat(document.getElementById('supp_retirement_weekly').value),
                    legal_monthly: parseFloat(document.getElementById('legal_monthly').value),
                    training_monthly: parseFloat(document.getElementById('training_monthly').value),
                    profit_sharing_quarterly: parseFloat(document.getElementById('profit_sharing_quarterly').value)
                },
                workers_comp: {
                    percent: parseFloat(document.getElementById('workers_comp_percent').value)
                },
                wage_increase: {
                    percent: parseFloat(document.getElementById('wage_increase_percent').value),
                    effective_week: document.getElementById('wage_increase_effective_week').value,
                    pre_increase_weeks: parseInt(document.getElementById('wage_increase_pre_weeks').value),
                    post_increase_weeks: parseInt(document.getElementById('wage_increase_post_weeks').value)
                },
                insurance_renewal: {
                    increase_percent: parseFloat(document.getElementById('insurance_renewal_increase_percent').value),
                    effective_date: document.getElementById('insurance_renewal_effective_date').value,
                    pre_renewal_months: parseInt(document.getElementById('insurance_renewal_pre_months').value),
                    post_renewal_months: parseInt(document.getElementById('insurance_renewal_post_months').value)
                },
                energy: {
                    gas_esco_rate: parseFloat(document.getElementById('energy_gas_esco_rate').value) || 0,
                    electric_esco_rate: parseFloat(document.getElementById('energy_electric_esco_rate').value) || 0,
                    oil_price_per_gallon: parseFloat(document.getElementById('energy_oil_price').value) || 0,
                    gas_rate_increase: parseFloat(document.getElementById('energy_gas_rate_increase').value) || 0,
                    electric_rate_increase: parseFloat(document.getElementById('energy_electric_rate_increase').value) || 0,
                    oil_rate_increase: parseFloat(document.getElementById('energy_oil_rate_increase').value) || 0,
                    consumption_basis: document.getElementById('energy_consumption_basis').value
                },
                water_sewer: {
                    rate_increase: parseFloat(document.getElementById('water_sewer_rate_increase').value) || 0
                }
            };

            fetch('/api/defaults', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            })
            .then(() => {
                showToast('Defaults saved successfully');
            })
            .catch(e => console.error(e));
        }

        function showToast(msg) {
            const toast = document.getElementById('toast');
            toast.textContent = msg;
            toast.classList.add('show');
            setTimeout(() => toast.classList.remove('show'), 2000);
        }

        loadUI();
    </script>
</body>
</html>
"""

ASSUMPTIONS_BUILDINGS_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <title>Building Assumptions</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; background: #f5f5f5; }
        header { background: #5a4a3f; color: white; padding: 24px; }
        header h1 { font-size: 24px; font-weight: 600; }
        .main { display: flex; height: calc(100vh - 80px); }
        .sidebar {
            width: 280px;
            background: white;
            border-right: 1px solid #e0e0e0;
            overflow-y: auto;
            padding: 16px;
        }
        .sidebar-header { font-size: 13px; font-weight: 600; color: #666; text-transform: uppercase; margin-bottom: 12px; }
        .sidebar-search { display: flex; margin-bottom: 12px; }
        .sidebar-search input { flex: 1; padding: 8px; border: 1px solid #ddd; border-radius: 4px; font-size: 13px; }
        .sidebar-list { display: flex; flex-direction: column; gap: 4px; }
        .building-item {
            padding: 12px;
            border: 1px solid #e0e0e0;
            border-radius: 4px;
            cursor: pointer;
            font-size: 13px;
            background: white;
        }
        .building-item:hover { background: #f9f9f9; border-color: #5a4a3f; }
        .building-item.active { background: #5a4a3f; color: white; border-color: #5a4a3f; }
        .content {
            flex: 1;
            padding: 40px;
            overflow-y: auto;
        }
        .back-link { display: inline-block; margin-bottom: 24px; color: #5a4a3f; text-decoration: none; font-size: 14px; }
        .back-link:hover { text-decoration: underline; }
        .building-title { font-size: 24px; color: #5a4a3f; margin-bottom: 8px; font-weight: 600; }
        .building-code { font-size: 13px; color: #999; margin-bottom: 24px; }
        .tabs {
            display: flex;
            gap: 8px;
            border-bottom: 1px solid #e0e0e0;
            margin-bottom: 24px;
        }
        .tab {
            padding: 12px 16px;
            border: none;
            background: none;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            color: #666;
            border-bottom: 2px solid transparent;
        }
        .tab:hover { color: #5a4a3f; }
        .tab.active { color: #5a4a3f; border-bottom-color: #5a4a3f; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        .section { background: white; border-radius: 8px; padding: 24px; border: 1px solid #e0e0e0; margin-bottom: 24px; }
        .section h3 { font-size: 16px; color: #5a4a3f; margin-bottom: 16px; font-weight: 600; }
        table { width: 100%; border-collapse: collapse; font-size: 14px; }
        th { background: #f5f5f5; padding: 12px; text-align: left; font-weight: 600; color: #333; border-bottom: 1px solid #e0e0e0; }
        td { padding: 12px; border-bottom: 1px solid #e0e0e0; }
        input { padding: 8px; border: 1px solid #ddd; border-radius: 4px; font-size: 13px; }
        input[type="number"] { font-family: 'Courier New', monospace; text-align: right; }
        input:focus { outline: none; border-color: #5a4a3f; box-shadow: 0 0 0 2px rgba(90,74,63, 0.1); }
        .form-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; }
        .form-group { display: flex; flex-direction: column; }
        .form-group label { font-size: 13px; font-weight: 500; color: #333; margin-bottom: 6px; }
        .button-row { display: flex; gap: 12px; margin-top: 24px; }
        button { padding: 10px 20px; border: none; border-radius: 4px; font-size: 14px; font-weight: 500; cursor: pointer; }
        .btn-primary { background: #5a4a3f; color: white; }
        .btn-primary:hover { background: #0f1f38; }
        .btn-secondary { background: #e0e0e0; color: #333; }
        .btn-secondary:hover { background: #d0d0d0; }
        .toast { position: fixed; bottom: 20px; right: 20px; background: #4caf50; color: white; padding: 16px 20px; border-radius: 4px; display: none; z-index: 1000; }
        .toast.show { display: block; animation: slideIn 0.3s ease; }
        @keyframes slideIn { from { transform: translateX(400px); } to { transform: translateX(0); } }
        .empty { padding: 40px; text-align: center; color: #999; }
    </style>
</head>
<body>
    <header>
        <h1>Building Assumptions</h1>
    </header>
    <div class="main">
        <div class="sidebar">
            <div class="sidebar-header">Buildings</div>
            <div class="sidebar-search">
                <input type="text" id="searchInput" placeholder="Search..." />
            </div>
            <div class="sidebar-list" id="buildingList"></div>
        </div>
        <div class="content">
            <a href="/" class="back-link">← Back to Home</a>

            <div id="noBuildingSelected" class="empty">
                <p>Select a building from the list to view and edit assumptions</p>
            </div>

            <div id="buildingContent" style="display: none;">
                <div class="building-title" id="buildingName"></div>
                <div class="building-code" id="buildingCode"></div>

                <div class="tabs">
                    <button class="tab active" onclick="switchTab('payroll')">Payroll</button>
                    <button class="tab" onclick="switchTab('income')">Income</button>
                    <button class="tab" onclick="switchTab('insurance')">Insurance</button>
                    <button class="tab" onclick="switchTab('utilities')">Utilities</button>
                </div>

                <!-- Payroll Tab -->
                <div id="payroll" class="tab-content active">
                    <div class="section">
                        <h3>Position Details</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>Position</th>
                                    <th># Employees</th>
                                    <th>Hourly Rate</th>
                                </tr>
                            </thead>
                            <tbody id="payrollTable"></tbody>
                        </table>
                    </div>
                </div>

                <!-- Income Tab -->
                <div id="income" class="tab-content">
                    <div class="section">
                        <h3>Maintenance</h3>
                        <div class="form-grid">
                            <div class="form-group">
                                <label>Total Shares</label>
                                <input type="number" id="income_maint_shares" />
                            </div>
                            <div class="form-group">
                                <label>$/Share/Mo</label>
                                <input type="number" step="0.01" id="income_maint_per_share" />
                            </div>
                            <div class="form-group">
                                <label>Increase %</label>
                                <input type="number" step="0.0001" id="income_maint_increase" />
                            </div>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Storage Units</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>Size Label</th>
                                    <th># Units</th>
                                    <th># Occupied</th>
                                    <th>$/Month</th>
                                </tr>
                            </thead>
                            <tbody id="storageTable"></tbody>
                        </table>
                    </div>

                    <div class="section">
                        <h3>Bike Storage</h3>
                        <div class="form-grid">
                            <div class="form-group">
                                <label># Racks</label>
                                <input type="number" id="income_bike_racks" />
                            </div>
                            <div class="form-group">
                                <label># Occupied</label>
                                <input type="number" id="income_bike_occupied" />
                            </div>
                            <div class="form-group">
                                <label>$/Month</label>
                                <input type="number" step="0.01" id="income_bike_monthly" />
                            </div>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Laundry/Vending</h3>
                        <div class="form-grid">
                            <div class="form-group">
                                <label>Contract Description</label>
                                <input type="text" id="income_laundry_desc" />
                            </div>
                            <div class="form-group">
                                <label>Monthly Amount</label>
                                <input type="number" step="0.01" id="income_laundry_monthly" />
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Insurance Tab -->
                <div id="insurance" class="tab-content">
                    <div class="section">
                        <h3>Insurance Policies</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>GL Code</th>
                                    <th>Policy Name</th>
                                    <th>Current Annual Premium</th>
                                    <th>Current Year Budget</th>
                                    <th>Expiration Date</th>
                                    <th>Override Increase %</th>
                                </tr>
                            </thead>
                            <tbody id="insuranceTable"></tbody>
                        </table>
                    </div>
                </div>

                <!-- Utilities Tab -->
                <div id="utilities" class="tab-content">
                    <div class="section">
                        <h3>Energy — Rate Overrides</h3>
                        <p style="font-size:13px;color:#666;margin-bottom:16px;">Leave blank or 0 to use portfolio defaults. Only enter values here to override for this building.</p>
                        <div class="form-grid">
                            <div class="form-group">
                                <label>Gas ESCO Rate ($/Therm)</label>
                                <input type="number" step="0.0001" id="util_gas_esco_rate" />
                            </div>
                            <div class="form-group">
                                <label>Electric ESCO Rate ($/kWh)</label>
                                <input type="number" step="0.0001" id="util_electric_esco_rate" />
                            </div>
                            <div class="form-group">
                                <label>Gas Rate Increase % Override</label>
                                <input type="number" step="0.0001" id="util_gas_rate_increase" />
                            </div>
                            <div class="form-group">
                                <label>Electric Rate Increase % Override</label>
                                <input type="number" step="0.0001" id="util_electric_rate_increase" />
                            </div>
                            <div class="form-group">
                                <label>Oil/Fuel Price ($/Gallon) Override</label>
                                <input type="number" step="0.01" id="util_oil_price" />
                            </div>
                            <div class="form-group">
                                <label>Oil/Fuel Rate Increase % Override</label>
                                <input type="number" step="0.0001" id="util_oil_rate_increase" />
                            </div>
                            <div class="form-group">
                                <label>Consumption Basis</label>
                                <select id="util_consumption_basis">
                                    <option value="">Use Default</option>
                                    <option value="2-Year Average">2-Year Average</option>
                                    <option value="Prior Year">Prior Year</option>
                                    <option value="Custom">Custom</option>
                                </select>
                            </div>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Energy — GL Account Adjustments</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>GL Code</th>
                                    <th>Description</th>
                                    <th>Accrual Adjustment</th>
                                    <th>Unpaid Bills</th>
                                    <th>Rate Increase % Override</th>
                                </tr>
                            </thead>
                            <tbody id="energyGLTable"></tbody>
                        </table>
                    </div>

                    <div class="section">
                        <h3>Water & Sewer</h3>
                        <div class="form-grid">
                            <div class="form-group">
                                <label>Rate Increase % Override</label>
                                <input type="number" step="0.0001" id="util_water_rate_increase" />
                            </div>
                        </div>
                    </div>

                    <div class="section">
                        <h3>Water & Sewer — GL Account Adjustments</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>GL Code</th>
                                    <th>Description</th>
                                    <th>Accrual Adjustment</th>
                                    <th>Unpaid Bills</th>
                                    <th>Rate Increase % Override</th>
                                </tr>
                            </thead>
                            <tbody id="waterGLTable"></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div class="toast" id="toast"></div>

    <script>
        const buildings = {{ buildings | safe }};
        const policies = {{ policies | safe }};
        let currentBuilding = null;
        let buildingData = {};
        let debounceTimer = null;

        const ENERGY_GL_ACCOUNTS = [
            { gl: '5252-0000', desc: 'Gas - Heating' },
            { gl: '5252-0001', desc: 'Gas - Cooking' },
            { gl: '5252-0010', desc: 'Gas - Common Area' },
            { gl: '5253-0000', desc: 'Oil / Fuel' },
            { gl: '5250-0000', desc: 'Electric' }
        ];
        const WATER_GL_ACCOUNTS = [
            { gl: '6305-0000', desc: 'Water/Sewer' },
            { gl: '6305-0010', desc: 'Water - Common Area' },
            { gl: '6305-0020', desc: 'Sewer Charges' }
        ];

        function loadBuildingsList() {
            const list = document.getElementById('buildingList');
            list.innerHTML = buildings.map(b =>
                `<div class="building-item" onclick="selectBuilding('${b.entity_code}', '${b.building_name}')">
                    <strong>${b.entity_code}</strong><br/>
                    ${b.building_name.substring(0, 40)}
                </div>`
            ).join('');
        }

        function selectBuilding(code, name) {
            currentBuilding = code;
            document.querySelectorAll('.building-item').forEach(el => el.classList.remove('active'));
            event.currentTarget.classList.add('active');

            fetch(`/api/building-assumptions/${code}`)
                .then(r => r.json())
                .then(data => {
                    buildingData = data;
                    document.getElementById('noBuildingSelected').style.display = 'none';
                    document.getElementById('buildingContent').style.display = 'block';
                    document.getElementById('buildingName').textContent = name;
                    document.getElementById('buildingCode').textContent = `Entity Code: ${code}`;
                    loadPayrollTab();
                    loadIncomeTab();
                    loadInsuranceTab();
                    loadUtilitiesTab();
                });
        }

        function switchTab(tabName) {
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
            document.getElementById(tabName).classList.add('active');
            event.currentTarget.classList.add('active');
        }

        function loadPayrollTab() {
            const payroll = buildingData.payroll || {};
            const positions = payroll.positions || [
                { name: 'Resident Manager', employee_count: 0, hourly_rate: 0 },
                { name: 'Handyman', employee_count: 0, hourly_rate: 0 },
                { name: 'Porter/Doorman', employee_count: 0, hourly_rate: 0 },
                { name: 'Porter 80%', employee_count: 0, hourly_rate: 0 },
                { name: 'Employee 5', employee_count: 0, hourly_rate: 0 },
                { name: 'Employee 6', employee_count: 0, hourly_rate: 0 },
                { name: 'Employee 7', employee_count: 0, hourly_rate: 0 },
                { name: 'Employee 8', employee_count: 0, hourly_rate: 0 }
            ];

            const table = document.getElementById('payrollTable');
            table.innerHTML = positions.map((p, i) => `
                <tr>
                    <td><input type="text" value="${p.name}" class="payroll-name" data-idx="${i}" /></td>
                    <td><input type="number" value="${p.employee_count || 0}" class="payroll-count" data-idx="${i}" /></td>
                    <td><input type="number" step="0.01" value="${p.hourly_rate || 0}" class="payroll-rate" data-idx="${i}" /></td>
                </tr>
            `).join('');

            document.querySelectorAll('.payroll-name, .payroll-count, .payroll-rate').forEach(el => {
                el.addEventListener('change', savePayroll);
            });
        }

        function savePayroll() {
            const positions = [];
            document.querySelectorAll('#payrollTable tr').forEach(tr => {
                const name = tr.querySelector('.payroll-name').value;
                const count = parseInt(tr.querySelector('.payroll-count').value) || 0;
                const rate = parseFloat(tr.querySelector('.payroll-rate').value) || 0;
                positions.push({ name, employee_count: count, hourly_rate: rate });
            });

            buildingData.payroll = { positions };
            debouncedSave();
        }

        function loadIncomeTab() {
            const income = buildingData.income || {};
            const maint = income.maintenance || { total_shares: 0, per_share_monthly: 0, increase_percent: 0 };
            const storage = income.storage || [];
            const bike = income.bike_storage || { racks: 0, occupied: 0, monthly: 0 };
            const laundry = income.laundry_vending || { description: '', monthly: 0 };

            document.getElementById('income_maint_shares').value = maint.total_shares || 0;
            document.getElementById('income_maint_per_share').value = maint.per_share_monthly || 0;
            document.getElementById('income_maint_increase').value = maint.increase_percent || 0;

            const storageTable = document.getElementById('storageTable');
            const storageRows = storage.length > 0 ? storage : [
                { size_label: 'Small', units: 0, occupied: 0, monthly: 0 },
                { size_label: 'Medium', units: 0, occupied: 0, monthly: 0 },
                { size_label: 'Large', units: 0, occupied: 0, monthly: 0 },
                { size_label: 'XL', units: 0, occupied: 0, monthly: 0 }
            ];
            storageTable.innerHTML = storageRows.map((s, i) => `
                <tr>
                    <td><input type="text" value="${s.size_label || ''}" class="storage-label" data-idx="${i}" /></td>
                    <td><input type="number" value="${s.units || 0}" class="storage-units" data-idx="${i}" /></td>
                    <td><input type="number" value="${s.occupied || 0}" class="storage-occupied" data-idx="${i}" /></td>
                    <td><input type="number" step="0.01" value="${s.monthly || 0}" class="storage-monthly" data-idx="${i}" /></td>
                </tr>
            `).join('');

            document.getElementById('income_bike_racks').value = bike.racks || 0;
            document.getElementById('income_bike_occupied').value = bike.occupied || 0;
            document.getElementById('income_bike_monthly').value = bike.monthly || 0;

            document.getElementById('income_laundry_desc').value = laundry.description || '';
            document.getElementById('income_laundry_monthly').value = laundry.monthly || 0;

            document.querySelectorAll('.storage-label, .storage-units, .storage-occupied, .storage-monthly').forEach(el => {
                el.addEventListener('change', saveIncome);
            });
            document.querySelectorAll('#income_maint_shares, #income_maint_per_share, #income_maint_increase, #income_bike_racks, #income_bike_occupied, #income_bike_monthly, #income_laundry_desc, #income_laundry_monthly').forEach(el => {
                el.addEventListener('change', saveIncome);
            });
        }

        function saveIncome() {
            const income = {
                maintenance: {
                    total_shares: parseFloat(document.getElementById('income_maint_shares').value) || 0,
                    per_share_monthly: parseFloat(document.getElementById('income_maint_per_share').value) || 0,
                    increase_percent: parseFloat(document.getElementById('income_maint_increase').value) || 0
                },
                storage: Array.from(document.querySelectorAll('#storageTable tr')).map(tr => ({
                    size_label: tr.querySelector('.storage-label').value,
                    units: parseInt(tr.querySelector('.storage-units').value) || 0,
                    occupied: parseInt(tr.querySelector('.storage-occupied').value) || 0,
                    monthly: parseFloat(tr.querySelector('.storage-monthly').value) || 0
                })),
                bike_storage: {
                    racks: parseInt(document.getElementById('income_bike_racks').value) || 0,
                    occupied: parseInt(document.getElementById('income_bike_occupied').value) || 0,
                    monthly: parseFloat(document.getElementById('income_bike_monthly').value) || 0
                },
                laundry_vending: {
                    description: document.getElementById('income_laundry_desc').value,
                    monthly: parseFloat(document.getElementById('income_laundry_monthly').value) || 0
                }
            };

            buildingData.income = income;
            debouncedSave();
        }

        function loadInsuranceTab() {
            const insurance = buildingData.insurance || [];
            const table = document.getElementById('insuranceTable');

            table.innerHTML = policies.map((policy, i) => {
                const existing = insurance.find(p => p.gl_code === policy.gl_code) || {};
                return `
                    <tr>
                        <td>${policy.gl_code}</td>
                        <td>${policy.name}</td>
                        <td><input type="number" step="0.01" value="${existing.current_premium || 0}" class="ins-current-premium" data-gl="${policy.gl_code}" /></td>
                        <td><input type="number" step="0.01" value="${existing.current_budget || 0}" class="ins-budget" data-gl="${policy.gl_code}" /></td>
                        <td><input type="text" value="${existing.expiration_date || ''}" class="ins-expiration" data-gl="${policy.gl_code}" /></td>
                        <td><input type="number" step="0.0001" value="${existing.override_increase || 0}" class="ins-override-increase" data-gl="${policy.gl_code}" /></td>
                    </tr>
                `;
            }).join('');

            document.querySelectorAll('.ins-current-premium, .ins-budget, .ins-expiration, .ins-override-increase').forEach(el => {
                el.addEventListener('change', saveInsurance);
            });
        }

        function saveInsurance() {
            const insurance = policies.map(policy => {
                const gl = policy.gl_code;
                return {
                    gl_code: gl,
                    name: policy.name,
                    current_premium: parseFloat(document.querySelector(`.ins-current-premium[data-gl="${gl}"]`).value) || 0,
                    current_budget: parseFloat(document.querySelector(`.ins-budget[data-gl="${gl}"]`).value) || 0,
                    expiration_date: document.querySelector(`.ins-expiration[data-gl="${gl}"]`).value,
                    override_increase: parseFloat(document.querySelector(`.ins-override-increase[data-gl="${gl}"]`).value) || 0
                };
            });

            buildingData.insurance = insurance;
            debouncedSave();
        }

        function loadUtilitiesTab() {
            const energy = buildingData.energy || {};
            const water = buildingData.water_sewer || {};

            document.getElementById('util_gas_esco_rate').value = energy.gas_esco_rate || 0;
            document.getElementById('util_electric_esco_rate').value = energy.electric_esco_rate || 0;
            document.getElementById('util_oil_price').value = energy.oil_price_per_gallon || 0;
            document.getElementById('util_gas_rate_increase').value = energy.gas_rate_increase || 0;
            document.getElementById('util_electric_rate_increase').value = energy.electric_rate_increase || 0;
            document.getElementById('util_oil_rate_increase').value = energy.oil_rate_increase || 0;
            document.getElementById('util_consumption_basis').value = energy.consumption_basis || '';
            document.getElementById('util_water_rate_increase').value = water.rate_increase || 0;

            const energyAdj = energy.gl_adjustments || {};
            const energyTable = document.getElementById('energyGLTable');
            energyTable.innerHTML = ENERGY_GL_ACCOUNTS.map(a => {
                const adj = energyAdj[a.gl] || {};
                return `<tr>
                    <td>${a.gl}</td>
                    <td>${a.desc}</td>
                    <td><input type="number" step="0.01" value="${adj.accrual || 0}" class="energy-accrual" data-gl="${a.gl}" /></td>
                    <td><input type="number" step="0.01" value="${adj.unpaid || 0}" class="energy-unpaid" data-gl="${a.gl}" /></td>
                    <td><input type="number" step="0.0001" value="${adj.rate_increase || 0}" class="energy-gl-rate" data-gl="${a.gl}" /></td>
                </tr>`;
            }).join('');

            const waterAdj = water.gl_adjustments || {};
            const waterTable = document.getElementById('waterGLTable');
            waterTable.innerHTML = WATER_GL_ACCOUNTS.map(a => {
                const adj = waterAdj[a.gl] || {};
                return `<tr>
                    <td>${a.gl}</td>
                    <td>${a.desc}</td>
                    <td><input type="number" step="0.01" value="${adj.accrual || 0}" class="water-accrual" data-gl="${a.gl}" /></td>
                    <td><input type="number" step="0.01" value="${adj.unpaid || 0}" class="water-unpaid" data-gl="${a.gl}" /></td>
                    <td><input type="number" step="0.0001" value="${adj.rate_increase || 0}" class="water-gl-rate" data-gl="${a.gl}" /></td>
                </tr>`;
            }).join('');

            // Bind change events
            document.querySelectorAll('#util_gas_esco_rate, #util_electric_esco_rate, #util_oil_price, #util_gas_rate_increase, #util_electric_rate_increase, #util_oil_rate_increase, #util_consumption_basis, #util_water_rate_increase').forEach(el => {
                el.addEventListener('change', saveUtilities);
            });
            document.querySelectorAll('.energy-accrual, .energy-unpaid, .energy-gl-rate, .water-accrual, .water-unpaid, .water-gl-rate').forEach(el => {
                el.addEventListener('change', saveUtilities);
            });
        }

        function saveUtilities() {
            const energyGLAdj = {};
            ENERGY_GL_ACCOUNTS.forEach(a => {
                energyGLAdj[a.gl] = {
                    accrual: parseFloat(document.querySelector(`.energy-accrual[data-gl="${a.gl}"]`).value) || 0,
                    unpaid: parseFloat(document.querySelector(`.energy-unpaid[data-gl="${a.gl}"]`).value) || 0,
                    rate_increase: parseFloat(document.querySelector(`.energy-gl-rate[data-gl="${a.gl}"]`).value) || 0
                };
            });

            const waterGLAdj = {};
            WATER_GL_ACCOUNTS.forEach(a => {
                waterGLAdj[a.gl] = {
                    accrual: parseFloat(document.querySelector(`.water-accrual[data-gl="${a.gl}"]`).value) || 0,
                    unpaid: parseFloat(document.querySelector(`.water-unpaid[data-gl="${a.gl}"]`).value) || 0,
                    rate_increase: parseFloat(document.querySelector(`.water-gl-rate[data-gl="${a.gl}"]`).value) || 0
                };
            });

            buildingData.energy = {
                gas_esco_rate: parseFloat(document.getElementById('util_gas_esco_rate').value) || 0,
                electric_esco_rate: parseFloat(document.getElementById('util_electric_esco_rate').value) || 0,
                oil_price_per_gallon: parseFloat(document.getElementById('util_oil_price').value) || 0,
                gas_rate_increase: parseFloat(document.getElementById('util_gas_rate_increase').value) || 0,
                electric_rate_increase: parseFloat(document.getElementById('util_electric_rate_increase').value) || 0,
                oil_rate_increase: parseFloat(document.getElementById('util_oil_rate_increase').value) || 0,
                consumption_basis: document.getElementById('util_consumption_basis').value,
                gl_adjustments: energyGLAdj
            };

            buildingData.water_sewer = {
                rate_increase: parseFloat(document.getElementById('util_water_rate_increase').value) || 0,
                gl_adjustments: waterGLAdj
            };

            debouncedSave();
        }

        function debouncedSave() {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => {
                fetch(`/api/building-assumptions/${currentBuilding}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildingData)
                }).then(() => showToast('Saved'));
            }, 500);
        }

        function showToast(msg) {
            const toast = document.getElementById('toast');
            toast.textContent = msg;
            toast.classList.add('show');
            setTimeout(() => toast.classList.remove('show'), 2000);
        }

        document.getElementById('searchInput').addEventListener('input', (e) => {
            const search = e.target.value.toLowerCase();
            document.querySelectorAll('.building-item').forEach(item => {
                const text = item.textContent.toLowerCase();
                item.style.display = text.includes(search) ? '' : 'none';
            });
        });

        loadBuildingsList();
    </script>
</body>
</html>
"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    if IS_CLOUD:
        app.run(host="0.0.0.0", port=port)
    else:
        print("\n" + "=" * 50)
        print("  Century Budget & Assumptions System")
        print("  Open http://localhost:5000 in your browser")
        print("=" * 50 + "\n")
        app.run(debug=True, port=port)
