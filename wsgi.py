"""
WSGI entry point for Railway deployment.
Patches db.create_all() to prevent blocking on startup if Postgres is unavailable,
then retries table creation after the app is imported.
"""
import sys
import time
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

from budget_app.app import app, db

print("[WSGI] Application imported successfully", flush=True)

# Restore original and retry create_all now that Postgres should be ready
flask_sqlalchemy.SQLAlchemy.create_all = _original_create_all

def _retry_create_tables(max_retries=5, delay=2):
    for attempt in range(1, max_retries + 1):
        try:
            with app.app_context():
                db.create_all()
            print(f"[WSGI] Retry create_all() succeeded on attempt {attempt}", flush=True)
            return True
        except Exception as e:
            print(f"[WSGI] Retry {attempt}/{max_retries} failed: {e}", flush=True)
            if attempt < max_retries:
                time.sleep(delay)
    print("[WSGI] WARNING: All create_all() retries failed. DB tables may not exist.", flush=True)
    return False

_retry_create_tables()

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    app.run(host="0.0.0.0", port=port)
