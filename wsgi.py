"""
WSGI entry point for Railway deployment.
Patches db.create_all() to prevent blocking on startup if Postgres is unavailable.
"""
import sys
import flask_sqlalchemy

print("[WSGI] Starting application import...", flush=True)

# Monkey-patch create_all to be a safe no-op during import
_original_create_all = flask_sqlalchemy.SQLAlchemy.create_all

def _safe_create_all(self, **kwargs):
    try:
        _original_create_all(self, **kwargs)
        print("[WSGI] db.create_all() succeeded", flush=True)
    except Exception as e:
        print(f"[WSGI] WARNING: db.create_all() failed: {e}", flush=True)

flask_sqlalchemy.SQLAlchemy.create_all = _safe_create_all

from budget_app.app import app

print("[WSGI] Application imported successfully", flush=True)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    app.run(host="0.0.0.0", port=port)

