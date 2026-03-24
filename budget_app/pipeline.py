"""
Budget Pipeline Blueprint for Century Management.

Orchestrates the end-to-end budget process:
- Phase 1: Data collection (YSL from Yardi, files from SharePoint, audit data)
- Phase 2: Budget generation (first draft)

Provides a pipeline dashboard and APIs for initiating, collecting, and generating budgets.
"""

import os
import sys
import json
import shutil
import tempfile
import logging
from pathlib import Path
from datetime import datetime
from flask import Blueprint, render_template_string, request, jsonify

logger = logging.getLogger(__name__)

# Budget system imports (added to path in app.py)
from ysl_parser import parse_ysl_file
from template_populator import populate_template, apply_assumptions, apply_pm_projections
from gl_mapper import GLMapper


def create_pipeline_blueprint(db, models, helpers, app_config):
    """
    Create pipeline blueprint.

    Args:
        db: SQLAlchemy database instance
        models: dict of model classes from workflow blueprint
        helpers: dict of helper functions from workflow blueprint
        app_config: dict with paths and functions from app.py
            - template_path: Path to Budget_Final_Template_v2.xlsx
            - buildings_csv: Path to buildings.csv
            - budget_system: Path to budget_system directory
            - load_buildings: function to load buildings from CSV
            - merge_assumptions: function to merge portfolio + building assumptions
            - af_helpers: dict of audited financials helpers

    Returns:
        blueprint
    """
    Budget = models["Budget"]
    BudgetLine = models["BudgetLine"]
    DataSource = models["DataSource"]
    User = models["User"]

    store_all_lines = helpers["store_all_lines"]
    store_rm_lines = helpers["store_rm_lines"]
    get_pm_projections = helpers["get_pm_projections"]
    record_revision = helpers["record_revision"]

    template_path = app_config["template_path"]
    budget_system = app_config["budget_system"]
    load_buildings = app_config["load_buildings"]
    merge_assumptions = app_config["merge_assumptions"]
    af_helpers = app_config.get("af_helpers", {})

    # Default data sources required for each budget
    DEFAULT_SOURCES = ["ysl", "audit"]

    bp = Blueprint("pipeline", __name__)

    # ─── Data Source Collectors ──────────────────────────────────────────────

    def ysl_collector(entity_code, budget_id, file_path=None):
        """
        Collect YSL data for a building.

        If file_path provided, parses that file.
        Otherwise checks ysl_downloads for an existing file.

        Returns: (status, data_or_error)
        """
        ysl_dir = budget_system / "ysl_downloads"

        if file_path:
            src = Path(file_path)
        else:
            # Look for existing YSL file
            candidates = list(ysl_dir.glob(f"YSL_Annual_Budget_{entity_code}*.xlsx"))
            if not candidates:
                return "pending", "No YSL file found. Upload or download from Yardi."
            src = max(candidates, key=lambda p: p.stat().st_mtime)

        try:
            gl_data, property_info = parse_ysl_file(src)
            return "collected", {
                "gl_data": gl_data,
                "property_info": property_info,
                "file": str(src),
                "gl_count": len(gl_data),
            }
        except Exception as e:
            return "failed", str(e)

    def audit_collector(entity_code, budget_id):
        """
        Check if confirmed audit data exists for a building.

        Returns: (status, data_or_error)
        """
        get_confirmed = af_helpers.get("get_confirmed_actuals")
        if not get_confirmed:
            return "not_required", "Audited financials module not configured"

        actuals = get_confirmed(entity_code, "2025")
        if not actuals:
            actuals = get_confirmed(entity_code, "2024")

        if actuals:
            return "collected", {"categories": len(actuals), "data": actuals}
        return "not_required", "No confirmed audit data available"

    COLLECTORS = {
        "ysl": ysl_collector,
        "audit": audit_collector,
    }

    # ─── API Routes ─────────────────────────────────────────────────────────

    @bp.route("/api/pipeline/initiate", methods=["POST"])
    def initiate_budget():
        """
        Start the budget cycle for one or more buildings.

        JSON body: { entity_codes: ["204", "148"], user_id: 1 }
        Creates Budget records and DataSource records for each.
        """
        data = request.get_json()
        entity_codes = data.get("entity_codes", [])
        user_id = data.get("user_id")

        if not entity_codes:
            return jsonify({"error": "No entity codes provided"}), 400

        buildings = {b["entity_code"]: b for b in load_buildings()}
        results = {"initiated": [], "skipped": [], "errors": []}

        for code in entity_codes:
            code = str(code).strip()
            building = buildings.get(code)
            if not building:
                results["errors"].append({"entity_code": code, "reason": "Building not found in CSV"})
                continue

            # Check if budget already exists
            existing = Budget.query.filter_by(entity_code=code, year=2027).first()
            if existing and existing.status != "not_started":
                results["skipped"].append({
                    "entity_code": code,
                    "reason": f"Budget already in '{existing.status}' status",
                })
                continue

            # Create or reset budget
            if existing:
                budget = existing
            else:
                budget = Budget(
                    entity_code=code,
                    building_name=building["building_name"],
                    year=2027,
                )
                db.session.add(budget)
                db.session.flush()

            budget.status = "data_collection"
            budget.initiated_by = user_id
            budget.initiated_at = datetime.utcnow()

            # Create data source records
            for source_type in DEFAULT_SOURCES:
                existing_ds = DataSource.query.filter_by(
                    budget_id=budget.id, source_type=source_type
                ).first()
                if not existing_ds:
                    ds = DataSource(budget_id=budget.id, source_type=source_type)
                    db.session.add(ds)

            record_revision(
                budget_id=budget.id,
                user_id=user_id,
                action="status_change",
                field_name="status",
                old_value="not_started",
                new_value="data_collection",
                notes="Budget cycle initiated",
                source="web",
            )

            results["initiated"].append({
                "entity_code": code,
                "building_name": building["building_name"],
                "budget_id": budget.id,
            })

        db.session.commit()
        return jsonify(results)

    @bp.route("/api/pipeline/initiate-batch", methods=["POST"])
    def initiate_batch():
        """Batch initiate all buildings (or a filtered set)."""
        data = request.get_json() or {}
        user_id = data.get("user_id")
        building_type = data.get("type")  # optional filter: Coop, Condo, Rental, etc.

        buildings = load_buildings()
        if building_type:
            buildings = [b for b in buildings if b.get("type") == building_type]

        entity_codes = [b["entity_code"] for b in buildings]

        # Delegate to the single initiate endpoint logic
        with app_config["app"].test_request_context(
            json={"entity_codes": entity_codes, "user_id": user_id}
        ):
            return initiate_budget()

    @bp.route("/api/pipeline/status/<entity_code>", methods=["GET"])
    def pipeline_status(entity_code):
        """Get collection status for a building."""
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        sources = DataSource.query.filter_by(budget_id=budget.id).all()
        all_collected = all(
            s.status in ("collected", "not_required") for s in sources
        ) if sources else False

        return jsonify({
            "entity_code": entity_code,
            "building_name": budget.building_name,
            "status": budget.status,
            "data_sources": [s.to_dict() for s in sources],
            "all_collected": all_collected,
        })

    @bp.route("/api/pipeline/collect/<entity_code>/<source_type>", methods=["POST"])
    def collect_data(entity_code, source_type):
        """
        Trigger collection for a specific data source.

        For YSL: can upload a file or auto-detect from ysl_downloads.
        For audit: checks for confirmed audit uploads.
        For SharePoint sources: upload file via multipart form.
        """
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        ds = DataSource.query.filter_by(budget_id=budget.id, source_type=source_type).first()
        if not ds:
            ds = DataSource(budget_id=budget.id, source_type=source_type)
            db.session.add(ds)
            db.session.flush()

        ds.status = "collecting"

        # Handle file upload
        file_path = None
        if "file" in request.files:
            f = request.files["file"]
            if f.filename:
                upload_dir = budget_system / "ysl_downloads"
                upload_dir.mkdir(exist_ok=True)
                file_path = str(upload_dir / f.filename)
                f.save(file_path)

        # Run the appropriate collector
        collector = COLLECTORS.get(source_type)
        if not collector:
            # Generic file upload (SharePoint, etc.)
            if file_path:
                ds.status = "collected"
                ds.file_path = file_path
                ds.collected_at = datetime.utcnow()
                db.session.commit()
                return jsonify({"status": "collected", "file": file_path})
            else:
                ds.status = "pending"
                db.session.commit()
                return jsonify({"error": f"No collector for '{source_type}' and no file uploaded"}), 400

        # Collectors have different signatures
        if source_type == "ysl":
            status, result = collector(entity_code, budget.id, file_path)
        else:
            status, result = collector(entity_code, budget.id)

        ds.status = status
        if status == "collected":
            ds.collected_at = datetime.utcnow()
            ds.file_path = result.get("file", "") if isinstance(result, dict) else ""
            ds.metadata_json = json.dumps(result) if isinstance(result, dict) else "{}"
        elif status == "failed":
            ds.error_message = str(result)

        # Check if all sources collected -> auto-advance to data_ready
        all_sources = DataSource.query.filter_by(budget_id=budget.id).all()
        all_done = all(s.status in ("collected", "not_required") for s in all_sources)

        if all_done and budget.status == "data_collection":
            budget.status = "data_ready"
            record_revision(
                budget_id=budget.id,
                action="status_change",
                field_name="status",
                old_value="data_collection",
                new_value="data_ready",
                notes="All data sources collected",
                source="system",
            )

        db.session.commit()

        return jsonify({
            "source_type": source_type,
            "status": status,
            "result": result if isinstance(result, (str, dict)) else str(result),
            "budget_status": budget.status,
        })

    @bp.route("/api/pipeline/generate/<entity_code>", methods=["POST"])
    def generate_budget(entity_code):
        """
        Generate first draft budget for a building.

        Requires status to be 'data_ready'.
        1. Parses YSL data
        2. Populates Excel template
        3. Applies assumptions
        4. Stores all GL lines in database
        5. Sets status to 'draft'
        """
        budget = Budget.query.filter_by(entity_code=entity_code, year=2027).first()
        if not budget:
            return jsonify({"error": "Budget not found"}), 404

        if budget.status != "data_ready":
            return jsonify({"error": f"Budget must be in 'data_ready' status, currently '{budget.status}'"}), 400

        # Get YSL data source
        ysl_ds = DataSource.query.filter_by(budget_id=budget.id, source_type="ysl").first()
        if not ysl_ds or ysl_ds.status != "collected":
            return jsonify({"error": "YSL data not collected"}), 400

        try:
            # Parse YSL
            ysl_meta = json.loads(ysl_ds.metadata_json) if ysl_ds.metadata_json else {}
            ysl_file = ysl_meta.get("file", "")

            if not ysl_file or not Path(ysl_file).exists():
                # Try to find it again
                candidates = list((budget_system / "ysl_downloads").glob(
                    f"YSL_Annual_Budget_{entity_code}*.xlsx"
                ))
                if not candidates:
                    return jsonify({"error": "YSL file not found on disk"}), 400
                ysl_file = str(max(candidates, key=lambda p: p.stat().st_mtime))

            gl_data, property_info = parse_ysl_file(ysl_file)

            # Build GL mapping from template
            mapper = GLMapper(template_path)
            sheet_mapping = mapper.build_mapping()
            mapper.close()

            # Determine output path
            save_dir = budget_system / "budgets"
            building_folder = save_dir / f"{entity_code} - {budget.building_name}"
            building_folder.mkdir(parents=True, exist_ok=True)

            output_name = f"{entity_code}_{budget.building_name}_2027_Budget.xlsx"
            output_path = building_folder / output_name

            # Generate budget from template
            success = populate_template(
                template_path=template_path,
                gl_data=gl_data,
                property_info=property_info,
                output_path=output_path,
                ytd_months=2,
                remaining_months=10,
            )

            if not success:
                return jsonify({"error": "Template population failed"}), 500

            # Apply assumptions
            try:
                merged = merge_assumptions(entity_code)
                if merged:
                    apply_assumptions(output_path, merged)
            except Exception as ae:
                logger.warning(f"Could not apply assumptions for {entity_code}: {ae}")

            # Get and apply audit data if available
            get_confirmed = af_helpers.get("get_confirmed_actuals")
            audit_data = {}
            if get_confirmed:
                audit_data = get_confirmed(entity_code, "2025") or get_confirmed(entity_code, "2024") or {}

            # Store ALL GL lines in database (not just R&M)
            store_all_lines(entity_code, budget.building_name, gl_data, sheet_mapping)

            # Also store R&M lines for backward compatibility
            store_rm_lines(entity_code, budget.building_name, gl_data)

            # Advance status to draft
            old_status = budget.status
            budget.status = "draft"

            record_revision(
                budget_id=budget.id,
                action="status_change",
                field_name="status",
                old_value=old_status,
                new_value="draft",
                notes=f"First draft generated. {len(gl_data)} GL codes processed.",
                source="system",
            )

            db.session.commit()

            line_count = BudgetLine.query.filter_by(budget_id=budget.id).count()

            return jsonify({
                "status": "success",
                "entity_code": entity_code,
                "building_name": budget.building_name,
                "budget_status": "draft",
                "gl_codes_parsed": len(gl_data),
                "lines_stored": line_count,
                "output_file": str(output_path),
                "audit_data_available": bool(audit_data),
            })

        except Exception as e:
            logger.exception(f"Error generating budget for {entity_code}")
            db.session.rollback()
            return jsonify({"error": str(e)}), 500

    @bp.route("/api/pipeline/overview", methods=["GET"])
    def pipeline_overview():
        """Get overview of all buildings and their pipeline status."""
        buildings = load_buildings()
        budgets = {b.entity_code: b.to_dict() for b in Budget.query.filter_by(year=2027).all()}

        overview = []
        for bldg in buildings:
            code = bldg["entity_code"]
            budget_data = budgets.get(code)
            overview.append({
                "entity_code": code,
                "building_name": bldg["building_name"],
                "type": bldg.get("type", ""),
                "units": bldg.get("units", ""),
                "budget_status": budget_data["status"] if budget_data else "not_started",
                "budget_id": budget_data["id"] if budget_data else None,
            })

        # Status summary
        status_counts = {}
        for item in overview:
            s = item["budget_status"]
            status_counts[s] = status_counts.get(s, 0) + 1

        return jsonify({
            "buildings": overview,
            "status_counts": status_counts,
            "total": len(overview),
        })

    # ─── Pipeline Dashboard Page ────────────────────────────────────────────

    @bp.route("/pipeline", methods=["GET"])
    def pipeline_dashboard():
        """Pipeline dashboard - overview of all buildings and their status."""
        return render_template_string(PIPELINE_TEMPLATE)

    return bp


# ─── HTML Template ──────────────────────────────────────────────────────────

PIPELINE_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Budget Pipeline - Century Management</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-light: #e1effe;
    --green: #057a55;
    --green-light: #def7ec;
    --red: #e02424;
    --red-light: #fde8e8;
    --yellow: #f59e0b;
    --yellow-light: #fef3c7;
    --purple: #7c3aed;
    --purple-light: #ede9fe;
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
    padding: 24px 20px;
  }
  header h1 { font-size: 24px; font-weight: 700; }
  header p { opacity: 0.85; margin-top: 4px; }
  .nav { padding: 12px 20px; background: white; border-bottom: 1px solid var(--gray-200); }
  .nav a { color: var(--blue); text-decoration: none; font-size: 14px; margin-right: 16px; }
  .nav a:hover { text-decoration: underline; }
  .container { max-width: 1400px; margin: 0 auto; padding: 24px 20px; }

  /* Status cards */
  .status-cards {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 12px;
    margin-bottom: 24px;
  }
  .status-card {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 8px;
    padding: 14px;
    text-align: center;
  }
  .status-card .count { font-size: 28px; font-weight: 700; color: var(--blue); }
  .status-card .label { font-size: 11px; color: var(--gray-500); text-transform: uppercase; letter-spacing: 0.5px; }

  /* Action bar */
  .action-bar {
    display: flex;
    gap: 12px;
    margin-bottom: 20px;
    align-items: center;
    flex-wrap: wrap;
  }
  .btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 8px 16px;
    border: none;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.15s;
  }
  .btn-primary { background: var(--blue); color: white; }
  .btn-primary:hover { background: #1e429f; }
  .btn-green { background: var(--green); color: white; }
  .btn-green:hover { background: #046c4e; }
  .btn-outline { background: white; color: var(--gray-700); border: 1px solid var(--gray-300); }
  .btn-outline:hover { background: var(--gray-100); }
  .btn-sm { padding: 4px 10px; font-size: 12px; }

  select, input[type="text"] {
    padding: 8px 12px;
    border: 1px solid var(--gray-300);
    border-radius: 6px;
    font-size: 13px;
  }

  /* Table */
  .table-wrap {
    background: white;
    border: 1px solid var(--gray-200);
    border-radius: 8px;
    overflow-x: auto;
  }
  table { width: 100%; border-collapse: collapse; }
  th {
    background: var(--gray-50);
    padding: 10px 14px;
    text-align: left;
    font-size: 12px;
    font-weight: 600;
    color: var(--gray-500);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    border-bottom: 1px solid var(--gray-200);
    position: sticky;
    top: 0;
  }
  td {
    padding: 10px 14px;
    font-size: 13px;
    border-bottom: 1px solid var(--gray-100);
  }
  tr:hover { background: var(--gray-50); }
  tr.selected { background: var(--blue-light); }

  /* Status badges */
  .badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
  }
  .badge-not_started { background: var(--gray-100); color: var(--gray-500); }
  .badge-data_collection { background: var(--yellow-light); color: #92400e; }
  .badge-data_ready { background: var(--blue-light); color: var(--blue); }
  .badge-draft { background: var(--purple-light); color: var(--purple); }
  .badge-fa_first_review { background: #fce7f3; color: #be185d; }
  .badge-pm_pending { background: var(--yellow-light); color: #92400e; }
  .badge-pm_in_progress { background: #fff7ed; color: #c2410c; }
  .badge-fa_second_review { background: #fce7f3; color: #be185d; }
  .badge-exec_review { background: #f0fdf4; color: #166534; }
  .badge-presentation { background: #eff6ff; color: #1e40af; }
  .badge-approved { background: var(--green-light); color: var(--green); }
  .badge-ar_pending { background: var(--yellow-light); color: #92400e; }
  .badge-ar_complete { background: var(--green-light); color: var(--green); }
  .badge-returned { background: var(--red-light); color: var(--red); }

  .checkbox-col { width: 36px; text-align: center; }
  .toast {
    position: fixed;
    bottom: 24px;
    right: 24px;
    background: var(--gray-900);
    color: white;
    padding: 12px 20px;
    border-radius: 8px;
    font-size: 14px;
    display: none;
    z-index: 100;
  }
  .toast.show { display: block; }
  .toast.success { background: var(--green); }
  .toast.error { background: var(--red); }

  .loading { opacity: 0.6; pointer-events: none; }
</style>
</head>
<body>
<header>
  <h1>Budget Pipeline</h1>
  <p>2027 Budget Cycle &mdash; Initiate, collect data, and generate first drafts</p>
</header>
<div class="nav">
  <a href="/">Home</a>
  <a href="/dashboard">FA Dashboard</a>
  <a href="/pipeline">Pipeline</a>
  <a href="/admin">Admin</a>
</div>

<div class="container">
  <!-- Status summary cards -->
  <div class="status-cards" id="statusCards"></div>

  <!-- Action bar -->
  <div class="action-bar">
    <button class="btn btn-primary" onclick="initiateSelected()">Initiate Selected</button>
    <button class="btn btn-green" onclick="collectSelected()">Collect Data</button>
    <button class="btn btn-green" onclick="generateSelected()">Generate Draft</button>
    <span style="flex:1"></span>
    <select id="filterStatus" onchange="applyFilter()">
      <option value="">All Statuses</option>
      <option value="not_started">Not Started</option>
      <option value="data_collection">Data Collection</option>
      <option value="data_ready">Data Ready</option>
      <option value="draft">Draft</option>
      <option value="fa_first_review">FA First Review</option>
      <option value="pm_pending">PM Pending</option>
      <option value="approved">Approved</option>
    </select>
    <select id="filterType" onchange="applyFilter()">
      <option value="">All Types</option>
      <option value="Coop">Coop</option>
      <option value="Condo">Condo</option>
      <option value="Rental">Rental</option>
      <option value="Comm">Commercial</option>
    </select>
    <input type="text" id="searchBox" placeholder="Search buildings..." oninput="applyFilter()">
  </div>

  <!-- Buildings table -->
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th class="checkbox-col"><input type="checkbox" id="selectAll" onchange="toggleAll()"></th>
          <th>Entity</th>
          <th>Building</th>
          <th>Type</th>
          <th>Units</th>
          <th>Status</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="buildingsTable"></tbody>
    </table>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
let allBuildings = [];
let filteredBuildings = [];

async function loadData() {
  try {
    const resp = await fetch('/api/pipeline/overview');
    const data = await resp.json();
    allBuildings = data.buildings;
    renderStatusCards(data.status_counts, data.total);
    applyFilter();
  } catch (e) {
    showToast('Failed to load data: ' + e.message, 'error');
  }
}

function renderStatusCards(counts, total) {
  const cards = document.getElementById('statusCards');
  const order = ['not_started','data_collection','data_ready','draft','fa_first_review',
    'pm_pending','pm_in_progress','fa_second_review','exec_review','approved','ar_pending','ar_complete'];

  let html = `<div class="status-card"><div class="count">${total}</div><div class="label">Total</div></div>`;
  order.forEach(s => {
    if (counts[s]) {
      html += `<div class="status-card"><div class="count">${counts[s]}</div><div class="label">${s.replace(/_/g,' ')}</div></div>`;
    }
  });
  cards.innerHTML = html;
}

function applyFilter() {
  const status = document.getElementById('filterStatus').value;
  const type = document.getElementById('filterType').value;
  const search = document.getElementById('searchBox').value.toLowerCase();

  filteredBuildings = allBuildings.filter(b => {
    if (status && b.budget_status !== status) return false;
    if (type && b.type !== type) return false;
    if (search && !b.building_name.toLowerCase().includes(search) && !b.entity_code.includes(search)) return false;
    return true;
  });

  renderTable();
}

function renderTable() {
  const tbody = document.getElementById('buildingsTable');
  tbody.innerHTML = filteredBuildings.map(b => `
    <tr data-entity="${b.entity_code}">
      <td class="checkbox-col"><input type="checkbox" class="row-check" value="${b.entity_code}"></td>
      <td><strong>${b.entity_code}</strong></td>
      <td>${b.building_name}</td>
      <td>${b.type || '-'}</td>
      <td>${b.units || '-'}</td>
      <td><span class="badge badge-${b.budget_status}">${b.budget_status.replace(/_/g,' ')}</span></td>
      <td>${getActions(b)}</td>
    </tr>
  `).join('');
}

function getActions(b) {
  const s = b.budget_status;
  let btns = '';
  if (s === 'not_started') {
    btns += `<button class="btn btn-primary btn-sm" onclick="initiate('${b.entity_code}')">Initiate</button>`;
  }
  if (s === 'data_collection') {
    btns += `<button class="btn btn-outline btn-sm" onclick="collectYSL('${b.entity_code}')">Collect YSL</button>`;
  }
  if (s === 'data_ready') {
    btns += `<button class="btn btn-green btn-sm" onclick="generate('${b.entity_code}')">Generate</button>`;
  }
  if (s !== 'not_started') {
    btns += ` <a href="/history/${b.entity_code}" class="btn btn-outline btn-sm">History</a>`;
  }
  return btns || '-';
}

function getSelected() {
  return Array.from(document.querySelectorAll('.row-check:checked')).map(c => c.value);
}

function toggleAll() {
  const checked = document.getElementById('selectAll').checked;
  document.querySelectorAll('.row-check').forEach(c => c.checked = checked);
}

async function initiate(entityCode) {
  const codes = entityCode ? [entityCode] : getSelected();
  if (!codes.length) return showToast('Select buildings first', 'error');

  try {
    const resp = await fetch('/api/pipeline/initiate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({entity_codes: codes}),
    });
    const data = await resp.json();
    showToast(`Initiated: ${data.initiated?.length || 0}, Skipped: ${data.skipped?.length || 0}`, 'success');
    loadData();
  } catch (e) {
    showToast('Error: ' + e.message, 'error');
  }
}

function initiateSelected() { initiate(null); }

async function collectYSL(entityCode) {
  try {
    const resp = await fetch(`/api/pipeline/collect/${entityCode}/ysl`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
    });
    const data = await resp.json();
    if (data.status === 'collected') {
      showToast(`YSL collected for ${entityCode}. ${data.result?.gl_count || 0} GL codes found.`, 'success');

      // Also collect audit data
      await fetch(`/api/pipeline/collect/${entityCode}/audit`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
      });
    } else {
      showToast(`YSL: ${data.result || data.error || 'Unknown error'}`, 'error');
    }
    loadData();
  } catch (e) {
    showToast('Error: ' + e.message, 'error');
  }
}

async function collectSelected() {
  const codes = getSelected();
  if (!codes.length) return showToast('Select buildings first', 'error');
  for (const code of codes) {
    await collectYSL(code);
  }
}

async function generate(entityCode) {
  try {
    const resp = await fetch(`/api/pipeline/generate/${entityCode}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
    });
    const data = await resp.json();
    if (data.status === 'success') {
      showToast(`Draft generated for ${entityCode}: ${data.lines_stored} lines stored`, 'success');
    } else {
      showToast(data.error || 'Generation failed', 'error');
    }
    loadData();
  } catch (e) {
    showToast('Error: ' + e.message, 'error');
  }
}

async function generateSelected() {
  const codes = getSelected();
  if (!codes.length) return showToast('Select buildings first', 'error');
  for (const code of codes) {
    await generate(code);
  }
}

function showToast(msg, type) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast show ' + (type || '');
  setTimeout(() => t.className = 'toast', 4000);
}

loadData();
</script>
</body>
</html>
"""
