"""
File Repository for Century Management Budget App.

Central document library for uploading, browsing, downloading, and deleting
files used during the budget workflow (maint proofs, YSL exports, GL detail,
supporting docs, etc.).  Files are stored as binary in PostgreSQL so they
persist on Railway (no ephemeral disk dependency).
"""

from flask import Blueprint, request, jsonify, send_file, render_template_string
from datetime import datetime
from io import BytesIO
import logging
import os

logger = logging.getLogger(__name__)

# ─── File Categories ─────────────────────────────────────────────────────────
FILE_CATEGORIES = [
    "Maint Proof",
    "YSL Export",
    "GL Detail",
    "Budget Draft",
    "Supporting Doc",
    "Other",
]


def create_file_repository_blueprint(db, workflow_models):
    """Factory: returns (blueprint, models_dict, helpers_dict)."""

    bp = Blueprint("file_repository", __name__)

    # ─── DB Model ─────────────────────────────────────────────────────────────

    class RepositoryFile(db.Model):
        """A file stored in the central document repository."""
        __tablename__ = "repository_files"

        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False, index=True)
        file_name = db.Column(db.String(255), nullable=False)
        category = db.Column(db.String(50), nullable=False, default="Other")
        mime_type = db.Column(db.String(100))
        file_size = db.Column(db.Integer, default=0)          # bytes
        file_data = db.Column(db.LargeBinary)                  # actual content
        uploaded_by = db.Column(db.String(100), default="")
        notes = db.Column(db.Text, default="")
        uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)
        deleted = db.Column(db.Boolean, default=False)         # soft-delete

        def to_dict(self, include_data=False):
            d = {
                "id": self.id,
                "entity_code": self.entity_code,
                "file_name": self.file_name,
                "category": self.category,
                "mime_type": self.mime_type,
                "file_size": self.file_size,
                "uploaded_by": self.uploaded_by,
                "notes": self.notes,
                "uploaded_at": self.uploaded_at.isoformat() if self.uploaded_at else None,
            }
            return d

    # ─── API Routes ───────────────────────────────────────────────────────────

    @bp.route("/api/files", methods=["GET"])
    def list_files():
        """List files, optionally filtered by entity_code and/or category."""
        entity = request.args.get("entity_code", "").strip()
        category = request.args.get("category", "").strip()

        q = RepositoryFile.query.filter_by(deleted=False)
        if entity:
            q = q.filter_by(entity_code=entity)
        if category:
            q = q.filter_by(category=category)
        q = q.order_by(RepositoryFile.uploaded_at.desc())

        files = [f.to_dict() for f in q.all()]
        return jsonify({"files": files, "categories": FILE_CATEGORIES})

    @bp.route("/api/files/upload", methods=["POST"])
    def upload_file():
        """Upload a file to the repository."""
        f = request.files.get("file")
        if not f or not f.filename:
            return jsonify({"error": "No file provided"}), 400

        entity_code = request.form.get("entity_code", "").strip()
        if not entity_code:
            return jsonify({"error": "entity_code is required"}), 400

        category = request.form.get("category", "Other").strip()
        notes = request.form.get("notes", "").strip()
        uploaded_by = request.form.get("uploaded_by", "").strip()

        data = f.read()
        repo_file = RepositoryFile(
            entity_code=entity_code,
            file_name=f.filename,
            category=category,
            mime_type=f.content_type or "application/octet-stream",
            file_size=len(data),
            file_data=data,
            uploaded_by=uploaded_by,
            notes=notes,
        )
        db.session.add(repo_file)
        db.session.commit()
        logger.info(f"File uploaded: {f.filename} ({len(data)} bytes) for entity {entity_code}")
        return jsonify({"ok": True, "file": repo_file.to_dict()})

    @bp.route("/api/files/<int:file_id>/download", methods=["GET"])
    def download_file(file_id):
        """Download a file by ID."""
        repo_file = RepositoryFile.query.get(file_id)
        if not repo_file or repo_file.deleted:
            return jsonify({"error": "File not found"}), 404
        return send_file(
            BytesIO(repo_file.file_data),
            download_name=repo_file.file_name,
            mimetype=repo_file.mime_type or "application/octet-stream",
            as_attachment=True,
        )

    @bp.route("/api/files/<int:file_id>", methods=["DELETE"])
    def delete_file(file_id):
        """Soft-delete a file (keeps audit trail)."""
        repo_file = RepositoryFile.query.get(file_id)
        if not repo_file:
            return jsonify({"error": "File not found"}), 404
        repo_file.deleted = True
        db.session.commit()
        logger.info(f"File soft-deleted: {repo_file.file_name} (id={file_id})")
        return jsonify({"ok": True})

    # ─── UI Page ──────────────────────────────────────────────────────────────

    @bp.route("/files")
    def files_page():
        """Render the file repository page."""
        return render_template_string(FILES_PAGE_HTML)

    # ─── Helper for other modules ─────────────────────────────────────────────

    def get_latest_file(entity_code, category):
        """Return the latest non-deleted file for an entity + category."""
        return (RepositoryFile.query
                .filter_by(entity_code=entity_code, category=category, deleted=False)
                .order_by(RepositoryFile.uploaded_at.desc())
                .first())

    return bp, {"RepositoryFile": RepositoryFile}, {"get_latest_file": get_latest_file}


# ─── Full-page HTML/JS for /files ─────────────────────────────────────────────
FILES_PAGE_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>File Repository — Century Management</title>
<style>
:root {
  --bg: #0f1117;
  --surface: #1a1d27;
  --surface-2: #242736;
  --border: #2e3148;
  --text: #e4e5eb;
  --text-dim: #8b8fa3;
  --blue: #5b8af5;
  --green: #4ade80;
  --red: #f87171;
  --orange: #fbbf24;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }

.top-bar {
  display: flex; flex-direction: column; align-items: flex-start; gap: 6px;
  padding: 16px 32px; background: var(--surface); border-bottom: 1px solid var(--border);
}
.top-bar h1 { font-size: 18px; font-weight: 600; }
.top-bar a { color: var(--blue); text-decoration: none; font-size: 13px; }
.top-bar a:hover { text-decoration: underline; }

.container { max-width: 1100px; margin: 0 auto; padding: 24px 32px; }

/* Upload Card */
.upload-card {
  background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
  padding: 24px; margin-bottom: 24px;
}
.upload-card h2 { font-size: 15px; font-weight: 600; margin-bottom: 16px; }
.upload-row { display: flex; gap: 12px; flex-wrap: wrap; align-items: flex-end; }
.field { display: flex; flex-direction: column; gap: 4px; }
.field label { font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.5px; }
.field input, .field select, .field textarea {
  background: var(--surface-2); border: 1px solid var(--border); border-radius: 6px;
  color: var(--text); padding: 8px 12px; font-size: 13px; outline: none;
}
.field input:focus, .field select:focus { border-color: var(--blue); }
.field textarea { resize: vertical; min-height: 36px; }

.btn {
  padding: 8px 20px; border-radius: 6px; border: none; font-size: 13px;
  cursor: pointer; font-weight: 500; transition: opacity 0.15s;
}
.btn:hover { opacity: 0.85; }
.btn-primary { background: var(--blue); color: white; }
.btn-danger { background: var(--red); color: white; }
.btn-ghost { background: transparent; border: 1px solid var(--border); color: var(--text-dim); }

/* Filters */
.filters { display: flex; gap: 12px; margin-bottom: 16px; align-items: center; flex-wrap: wrap; }

/* Table */
.file-table { width: 100%; border-collapse: collapse; }
.file-table th {
  text-align: left; font-size: 11px; color: var(--text-dim); text-transform: uppercase;
  letter-spacing: 0.5px; padding: 10px 12px; border-bottom: 1px solid var(--border);
}
.file-table td {
  padding: 12px; border-bottom: 1px solid var(--border); font-size: 13px; vertical-align: middle;
}
.file-table tr:hover td { background: var(--surface-2); }
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px;
  background: var(--surface-2); border: 1px solid var(--border);
}
.size { color: var(--text-dim); }
.empty-state {
  text-align: center; padding: 48px; color: var(--text-dim); font-size: 14px;
}
.upload-status { font-size: 12px; margin-top: 8px; min-height: 18px; }
</style>
</head>
<body>
<div class="top-bar">
  <a href="/">← Home</a>
  <h1>File Repository</h1>
</div>

<div class="container">

  <!-- Upload Card -->
  <div class="upload-card">
    <h2>Upload File</h2>
    <div class="upload-row">
      <div class="field">
        <label>Entity</label>
        <select id="uploadEntity">
          <option value="">Select entity...</option>
        </select>
      </div>
      <div class="field">
        <label>Category</label>
        <select id="uploadCategory">
          <option value="Maint Proof">Maint Proof</option>
          <option value="YSL Export">YSL Export</option>
          <option value="GL Detail">GL Detail</option>
          <option value="Budget Draft">Budget Draft</option>
          <option value="Supporting Doc">Supporting Doc</option>
          <option value="Other">Other</option>
        </select>
      </div>
      <div class="field">
        <label>File</label>
        <input type="file" id="uploadFile">
      </div>
      <div class="field" style="flex:1; min-width:150px;">
        <label>Notes (optional)</label>
        <input type="text" id="uploadNotes" placeholder="Brief description...">
      </div>
      <button class="btn btn-primary" onclick="doUpload()">Upload</button>
    </div>
    <div class="upload-status" id="uploadStatus"></div>
  </div>

  <!-- Filters -->
  <div class="filters">
    <div class="field">
      <label>Filter Entity</label>
      <select id="filterEntity" onchange="loadFiles()">
        <option value="">All Entities</option>
      </select>
    </div>
    <div class="field">
      <label>Filter Category</label>
      <select id="filterCategory" onchange="loadFiles()">
        <option value="">All Categories</option>
        <option value="Maint Proof">Maint Proof</option>
        <option value="YSL Export">YSL Export</option>
        <option value="GL Detail">GL Detail</option>
        <option value="Budget Draft">Budget Draft</option>
        <option value="Supporting Doc">Supporting Doc</option>
        <option value="Other">Other</option>
      </select>
    </div>
  </div>

  <!-- File Table -->
  <table class="file-table">
    <thead>
      <tr>
        <th>File Name</th>
        <th>Entity</th>
        <th>Category</th>
        <th>Size</th>
        <th>Notes</th>
        <th>Uploaded</th>
        <th style="text-align:right;">Actions</th>
      </tr>
    </thead>
    <tbody id="fileTableBody">
      <tr><td colspan="7" class="empty-state">Loading...</td></tr>
    </tbody>
  </table>
</div>

<script>
const ENTITIES = [];  // populated on load

function formatBytes(bytes) {
  if (!bytes) return '—';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / 1048576).toFixed(1) + ' MB';
}

function formatDate(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
    + ' ' + d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
}

async function loadEntities() {
  try {
    const resp = await fetch('/api/budgets');
    const data = await resp.json();
    const budgets = data.budgets || [];
    const seen = new Set();
    budgets.forEach(b => {
      if (!seen.has(b.entity_code)) {
        seen.add(b.entity_code);
        ENTITIES.push({ code: b.entity_code, name: b.entity_name || b.entity_code });
      }
    });
    ENTITIES.sort((a, b) => a.code.localeCompare(b.code));
    ['uploadEntity', 'filterEntity'].forEach(id => {
      const sel = document.getElementById(id);
      ENTITIES.forEach(e => {
        const opt = document.createElement('option');
        opt.value = e.code;
        opt.textContent = e.code + ' — ' + e.name;
        sel.appendChild(opt);
      });
    });
  } catch (err) {
    console.error('Failed to load entities:', err);
  }
}

async function loadFiles() {
  const entity = document.getElementById('filterEntity').value;
  const category = document.getElementById('filterCategory').value;
  let url = '/api/files?';
  if (entity) url += 'entity_code=' + encodeURIComponent(entity) + '&';
  if (category) url += 'category=' + encodeURIComponent(category);

  const tbody = document.getElementById('fileTableBody');
  try {
    const resp = await fetch(url);
    const data = await resp.json();
    const files = data.files || [];
    if (!files.length) {
      tbody.innerHTML = '<tr><td colspan="7" class="empty-state">No files found. Upload one above.</td></tr>';
      return;
    }
    tbody.innerHTML = files.map(f => `
      <tr>
        <td><strong>${esc(f.file_name)}</strong></td>
        <td>${esc(f.entity_code)}</td>
        <td><span class="badge">${esc(f.category)}</span></td>
        <td class="size">${formatBytes(f.file_size)}</td>
        <td style="color:var(--text-dim); max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${esc(f.notes || '—')}</td>
        <td style="color:var(--text-dim); white-space:nowrap;">${formatDate(f.uploaded_at)}</td>
        <td style="text-align:right; white-space:nowrap;">
          <a href="/api/files/${f.id}/download" class="btn btn-ghost" style="padding:4px 10px; font-size:12px;">Download</a>
          <button onclick="deleteFile(${f.id}, '${esc(f.file_name)}')" class="btn btn-danger" style="padding:4px 10px; font-size:12px; margin-left:4px;">Delete</button>
        </td>
      </tr>
    `).join('');
  } catch (err) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty-state" style="color:var(--red);">Error loading files</td></tr>';
  }
}

function esc(s) {
  if (!s) return '';
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

async function doUpload() {
  const entity = document.getElementById('uploadEntity').value;
  const category = document.getElementById('uploadCategory').value;
  const fileInput = document.getElementById('uploadFile');
  const notes = document.getElementById('uploadNotes').value;
  const status = document.getElementById('uploadStatus');

  if (!entity) { status.innerHTML = '<span style="color:var(--red);">Select an entity</span>'; return; }
  if (!fileInput.files.length) { status.innerHTML = '<span style="color:var(--red);">Choose a file</span>'; return; }

  status.innerHTML = '<span style="color:var(--blue);">Uploading...</span>';
  const fd = new FormData();
  fd.append('file', fileInput.files[0]);
  fd.append('entity_code', entity);
  fd.append('category', category);
  fd.append('notes', notes);

  try {
    const resp = await fetch('/api/files/upload', { method: 'POST', body: fd });
    const result = await resp.json();
    if (result.ok) {
      status.innerHTML = '<span style="color:var(--green);">✓ Uploaded ' + esc(result.file.file_name) + '</span>';
      fileInput.value = '';
      document.getElementById('uploadNotes').value = '';
      loadFiles();
    } else {
      status.innerHTML = '<span style="color:var(--red);">' + esc(result.error || 'Upload failed') + '</span>';
    }
  } catch (err) {
    status.innerHTML = '<span style="color:var(--red);">Upload failed: ' + esc(err.message) + '</span>';
  }
}

async function deleteFile(id, name) {
  if (!confirm('Delete "' + name + '"? It will be removed from the active list.')) return;
  try {
    const resp = await fetch('/api/files/' + id, { method: 'DELETE' });
    const result = await resp.json();
    if (result.ok) loadFiles();
  } catch (err) {
    alert('Delete failed: ' + err.message);
  }
}

// Init
loadEntities().then(() => loadFiles());
</script>
</body>
</html>
"""
