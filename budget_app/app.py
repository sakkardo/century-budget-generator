"""
Century Management Budget Generator — Web App
Run: python app.py
Open: http://localhost:5000
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
from flask import Flask, render_template_string, request, jsonify, send_file

# Detect cloud deployment (Railway sets PORT env var)
IS_CLOUD = "PORT" in os.environ or "RAILWAY_ENVIRONMENT" in os.environ

# Add budget_system to path for pipeline imports
BUDGET_SYSTEM = Path(__file__).parent.parent / "budget_system"
sys.path.insert(0, str(BUDGET_SYSTEM))

from ysl_parser import parse_ysl_file
from template_populator import populate_template

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

TEMPLATE_PATH = BUDGET_SYSTEM / "Budget_Final_Template_v2.xlsx"
BUILDINGS_CSV = BUDGET_SYSTEM / "buildings.csv"
SETTINGS_FILE = Path(__file__).parent / "settings.json"

# Default save location
DEFAULT_SAVE_DIR = str(BUDGET_SYSTEM / "budgets")

# The Console JS script template — entities/email/period get injected
CONSOLE_SCRIPT = (BUDGET_SYSTEM / "YSL Budget Script.js").read_text()


def load_settings():
    """Load app settings from disk."""
    if SETTINGS_FILE.exists():
        return json.loads(SETTINGS_FILE.read_text())
    return {"save_dir": DEFAULT_SAVE_DIR}


def save_settings(settings):
    """Persist app settings to disk."""
    SETTINGS_FILE.write_text(json.dumps(settings, indent=2))


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


@app.route("/")
def index():
    settings = load_settings()
    return render_template_string(
        HTML_TEMPLATE,
        buildings=load_buildings(),
        save_dir=settings.get("save_dir", DEFAULT_SAVE_DIR),
    )


@app.route("/manage")
def manage():
    return render_template_string(MANAGE_TEMPLATE)


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


@app.route("/api/buildings/<entity_code>", methods=["PUT", "DELETE"])
def manage_building(entity_code):
    """Edit or delete a building."""
    buildings = load_buildings()
    building_idx = next((i for i, b in enumerate(buildings) if b["entity_code"] == entity_code), None)

    if building_idx is None:
        return jsonify({"error": "Building not found"}), 404

    if request.method == "DELETE":
        buildings.pop(building_idx)
        save_buildings(buildings)
        return jsonify({"message": "Building deleted"}), 200

    if request.method == "PUT":
        data = request.json
        building = buildings[building_idx]
        # Update allowed fields
        for field in ["building_name", "address", "city", "zip", "type", "units"]:
            if field in data:
                building[field] = data[field]
        buildings[building_idx] = building
        save_buildings(buildings)
        return jsonify(building), 200


@app.route("/api/generate-script", methods=["POST"])
def generate_script():
    """Generate a customized Console script for selected buildings."""
    data = request.json
    entities = data.get("entities", [])
    email = data.get("email", "")
    period = data.get("period", "02/2026")

    if not entities:
        return jsonify({"error": "No buildings selected"}), 400

    # Replace the configurable lines in the script
    script = CONSOLE_SCRIPT
    script = script.replace(
        "const AS_OF_PERIOD = '02/2026';",
        f"const AS_OF_PERIOD = '{period}';"
    )
    script = script.replace(
        "const ENTITIES = [148, 204, 206, 805];",
        f"const ENTITIES = [{', '.join(str(e) for e in entities)}];"
    )
    script = script.replace(
        "const EMAIL = 'JSirotkin@Centuryny.com';",
        f"const EMAIL = '{email}';"
    )

    return jsonify({"script": script})


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

        for f in files:
            if not f.filename or not f.filename.endswith(".xlsx"):
                results["warnings"].append(f"Skipped non-xlsx: {f.filename}")
                continue

            # Save upload to temp
            ysl_path = tmp / f.filename
            f.save(str(ysl_path))

            try:
                # Parse YSL
                gl_data, property_info = parse_ysl_file(ysl_path)
                entity = property_info.get("property_code", "unknown")
                name = property_info.get("property_name", f"Entity_{entity}")

                # Build the building folder name (e.g., "148 - 130 E. 18 Owners Corp.")
                building_folder_name = f"{entity} - {name}"
                building_dir = save_dir / building_folder_name
                building_dir.mkdir(parents=True, exist_ok=True)

                # Generate budget into the building folder
                output_name = f"{entity}_{name}_2027_Budget.xlsx"
                output_path = building_dir / output_name

                success = populate_template(
                    template_path=TEMPLATE_PATH,
                    gl_data=gl_data,
                    property_info=property_info,
                    output_path=output_path,
                    ytd_months=2,
                    remaining_months=10,
                )

                if success and output_path.exists():
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

        if not output_files:
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


# Cache the template GL list
_template_gls_cache = None
def _get_template_gls():
    global _template_gls_cache
    if _template_gls_cache is None:
        from gl_mapper import build_gl_mapping
        _template_gls_cache = set(build_gl_mapping(TEMPLATE_PATH).keys())
    return _template_gls_cache


# ─── HTML Templates ──────────────────────────────────────────────────────────

MANAGE_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Manage Buildings — Century Budget Generator</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-light: #e1effe;
    --green: #057a55;
    --green-light: #def7ec;
    --red: #e02424;
    --red-light: #fde8e8;
    --gray-50: #f9fafb;
    --gray-100: #f3f4f6;
    --gray-200: #e5e7eb;
    --gray-300: #d1d5db;
    --gray-500: #6b7280;
    --gray-700: #374151;
    --gray-900: #111827;
    --radius: 8px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
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
  .form-group input:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(26,86,219,0.1); }

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
  .search-box:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(26,86,219,0.1); }

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
  .btn-primary:hover { background: #1e429f; }
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
  .btn-edit:hover { background: #1e429f; }
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
    <a href="/" class="back-link">← Back to Generator</a>
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

  const resp = await fetch(`/api/buildings/${code}`, { method: 'DELETE' });

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

HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Century Budget Generator</title>
<style>
  :root {
    --blue: #1a56db;
    --blue-light: #e1effe;
    --green: #057a55;
    --green-light: #def7ec;
    --red: #e02424;
    --red-light: #fde8e8;
    --gray-50: #f9fafb;
    --gray-100: #f3f4f6;
    --gray-200: #e5e7eb;
    --gray-300: #d1d5db;
    --gray-500: #6b7280;
    --gray-700: #374151;
    --gray-900: #111827;
    --radius: 8px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--gray-50);
    color: var(--gray-900);
    line-height: 1.5;
  }
  .container { max-width: 900px; margin: 0 auto; padding: 40px 20px; }
  h1 { font-size: 28px; font-weight: 700; margin-bottom: 4px; }
  .subtitle { color: var(--gray-500); font-size: 15px; margin-bottom: 32px; }

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
  .search-box:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(26,86,219,0.1); }
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
    color: #e5e7eb;
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
  .btn-primary:hover { background: #1e429f; }
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
</style>
</head>
<body>
<div class="container">
  <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
    <div>
      <h1>Century Budget Generator</h1>
      <p class="subtitle">Download YSL reports from Yardi, then generate 2027 budgets in one click.</p>
    </div>
    <a href="/manage" style="color: var(--blue); font-weight: 600; text-decoration: none; padding: 8px 16px; border: 1px solid var(--blue); border-radius: 6px; transition: all 0.15s;"
       onmouseover="this.style.background='var(--blue-light)'" onmouseout="this.style.background='transparent'">
      ⚙️ Manage Buildings
    </a>
  </div>

  <!-- STEP 1 -->
  <div class="step">
    <div class="step-header">
      <div class="step-num">1</div>
      <div class="step-title">Select Buildings & Download from Yardi</div>
    </div>
    <p class="step-desc">Pick the buildings you want budgets for, then copy the script into your Yardi Console.</p>

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
    </div>

    <button class="btn btn-primary" onclick="generateScript()" id="genBtn">
      <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M16 18l2-2-2-2M8 18l-2-2 2-2M14 4l-4 16"/></svg>
      Generate Yardi Script
    </button>

    <div class="script-box" id="scriptBox">
      <button class="copy-btn" id="copyBtn" onclick="copyScript()">Copy</button>
      <code id="scriptCode"></code>
    </div>
  </div>

  <!-- STEP 2 -->
  <div class="step">
    <div class="step-header">
      <div class="step-num">2</div>
      <div class="step-title">Upload YSL Files & Generate Budgets</div>
    </div>
    <p class="step-desc">After downloading from Yardi, drag the YSL files here.</p>

    <div class="upload-zone" id="dropZone" onclick="document.getElementById('fileInput').click()">
      <svg width="40" height="40" fill="none" stroke="#9ca3af" stroke-width="1.5" viewBox="0 0 24 24"><path d="M12 16V4m0 0L8 8m4-4l4 4M4 17v2a1 1 0 001 1h14a1 1 0 001-1v-2"/></svg>
      <p class="upload-text"><strong>Click to browse</strong> or drag & drop YSL files here</p>
      <p class="upload-text" style="font-size:12px; margin-top:4px;">Accepts .xlsx files named YSL_Annual_Budget_*.xlsx</p>
    </div>
    <input type="file" id="fileInput" multiple accept=".xlsx" style="display:none" onchange="handleFiles(this.files)">
    <div class="file-list" id="fileList"></div>

    <div class="settings-row" style="margin-top: 16px;">
      <div class="setting" style="flex: 3;">
        <label>Save Location</label>
        <div style="display: flex; gap: 8px;">
          <input type="text" id="saveDir" value="{{ save_dir }}" placeholder="Path to save completed budgets" style="flex:1;">
          <button class="btn btn-primary" style="padding: 8px 14px; font-size: 12px; white-space: nowrap;" onclick="updateSaveDir()">Set</button>
        </div>
        <p style="font-size: 11px; color: var(--gray-500); margin-top: 4px;">
          Budgets save to subfolders here (e.g., /148 - Building Name/). YSL sources also archived.
        </p>
      </div>
    </div>

    <div style="margin-top: 12px;">
      <button class="btn btn-green" id="processBtn" onclick="processFiles()" disabled>
        <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
        Generate Budgets
      </button>
    </div>

    <div class="progress-bar" id="progressBar"><div class="fill"></div></div>

    <div class="results" id="results"></div>
  </div>
</div>

<script>
let uploadedFiles = [];

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

  const resp = await fetch('/api/generate-script', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ entities, email, period }),
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

// Drag & drop
const dz = document.getElementById('dropZone');
dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('dragover'); });
dz.addEventListener('dragleave', () => dz.classList.remove('dragover'));
dz.addEventListener('drop', e => {
  e.preventDefault();
  dz.classList.remove('dragover');
  handleFiles(e.dataTransfer.files);
});

function handleFiles(fileListObj) {
  for (const f of fileListObj) {
    if (f.name.endsWith('.xlsx') && !uploadedFiles.find(u => u.name === f.name)) {
      uploadedFiles.push(f);
    }
  }
  renderFileList();
}

function removeFile(idx) {
  uploadedFiles.splice(idx, 1);
  renderFileList();
}

function renderFileList() {
  const el = document.getElementById('fileList');
  el.innerHTML = uploadedFiles.map((f, i) =>
    `<div class="file-item">
      <span>📄 ${f.name} (${(f.size/1024).toFixed(0)} KB)</span>
      <button class="remove" onclick="removeFile(${i})">✕</button>
    </div>`
  ).join('');
  document.getElementById('processBtn').disabled = !uploadedFiles.length;
}

async function updateSaveDir() {
  const dir = document.getElementById('saveDir').value.trim();
  if (!dir) { alert('Enter a save path'); return; }
  const resp = await fetch('/api/settings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ save_dir: dir }),
  });
  const data = await resp.json();
  if (data.error) { alert(data.error); return; }
  document.getElementById('saveDir').value = data.save_dir;
  // Brief visual confirmation
  const btn = event.target;
  btn.textContent = 'Saved!';
  btn.style.background = '#057a55';
  setTimeout(() => { btn.textContent = 'Set'; btn.style.background = ''; }, 1500);
}

async function processFiles() {
  const btn = document.getElementById('processBtn');
  btn.disabled = true;
  btn.textContent = 'Processing...';
  document.getElementById('progressBar').style.display = 'block';
  document.getElementById('results').style.display = 'none';

  const fd = new FormData();
  uploadedFiles.forEach(f => fd.append('files', f));
  fd.append('save_dir', document.getElementById('saveDir').value.trim());

  try {
    const resp = await fetch('/api/process', { method: 'POST', body: fd });

    if (resp.ok) {
      // Download the file
      const blob = await resp.blob();
      const cd = resp.headers.get('content-disposition') || '';
      const match = cd.match(/filename="?([^"]+)"?/);
      const filename = match ? match[1].trim() : 'Budget_Output.xlsx';

      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(a.href);

      // Parse results from header
      let results = null;
      try { results = JSON.parse(resp.headers.get('X-Budget-Results')); } catch(e) {}

      document.getElementById('results').style.display = 'block';
      if (results && results.success) {
        let html = '';
        results.success.forEach(s => {
          html += `<div class="result-card success">
            <div class="result-info">
              <span>✅ <strong>${s.entity}</strong> — ${s.name} (${s.size_kb} KB)</span>
            </div>
            <span style="font-size:12px; color:var(--gray-500);">Saved to ${s.saved_to}</span>
          </div>`;
        });
        if (results.failed) results.failed.forEach(f => {
          html += `<div class="result-card failed"><div class="result-info">❌ ${f.file || f.entity} — ${f.reason}</div></div>`;
        });
        document.getElementById('results').innerHTML = html;
      } else {
        document.getElementById('results').innerHTML =
          `<div class="result-card success">
            <div class="result-info">✅ <strong>Success!</strong> ${uploadedFiles.length} budget(s) generated, saved, and downloading.</div>
          </div>`;
      }
    } else {
      const data = await resp.json();
      document.getElementById('results').style.display = 'block';
      let html = '';
      if (data.success) data.success.forEach(s =>
        html += `<div class="result-card success"><div class="result-info">✅ ${s.entity} — ${s.name} (${s.size_kb} KB) → ${s.saved_to}</div></div>`
      );
      if (data.failed) data.failed.forEach(f =>
        html += `<div class="result-card failed"><div class="result-info">❌ ${f.file || f.entity} — ${f.reason}</div></div>`
      );
      document.getElementById('results').innerHTML = html || `<div class="result-card failed"><div class="result-info">❌ ${data.error}</div></div>`;
    }
  } catch (err) {
    document.getElementById('results').style.display = 'block';
    document.getElementById('results').innerHTML =
      `<div class="result-card failed"><div class="result-info">❌ Error: ${err.message}</div></div>`;
  }

  btn.disabled = false;
  btn.textContent = 'Generate Budgets';
  document.getElementById('progressBar').style.display = 'none';
}
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
        print("  Century Budget Generator")
        print("  Open http://localhost:5000 in your browser")
        print("=" * 50 + "\n")
        app.run(debug=True, port=port)
