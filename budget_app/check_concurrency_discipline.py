#!/usr/bin/env python3
"""Deploy gate #7: concurrency discipline (born 2026-06-10, three outages).

Every rule below is a pattern that took the site down that day. The gate
fails the deploy if a change reintroduces one:

  1. wsgi.py must alias module 'app' to 'budget_app.app'. Without it, lazy
     `from app import X` in request handlers imports a SECOND copy of app.py
     and re-runs the entire boot (migrations, ALTERs, seeding) inside a
     request thread — the self-deadlock behind all three outages.
  2. Both migration runners in app.py must SET lock_timeout before their
     ALTER loops (a queued ACCESS EXCLUSIVE makes every later query on that
     table queue too — site-wide hang).
  3. No ALTER TABLE SQL outside app.py. New schema changes go in app.py's
     protected runners, never ad-hoc in blueprints.
  4. No urllib urlopen() without an explicit timeout= anywhere in budget_app.
     An unbounded network call in a request thread eats a gunicorn thread
     forever when the remote stalls (outage #2: 8 threads, then nothing).
  5. Regression locks on the day's fixes: the extract worker's
     close-txn-before-slow-work rollback and the PDF self-heal single-flight
     gate must still exist in audited_financials.py.

Run: python check_concurrency_discipline.py   (from budget_app/ or repo root)
"""
import io
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent          # budget_app/
ROOT = HERE.parent                              # repo root

failures = []


def read(p):
    return io.open(p, encoding="utf-8", errors="replace").read()


# ── 1. wsgi module alias ────────────────────────────────────────────────
wsgi = ROOT / "wsgi.py"
if not wsgi.exists():
    failures.append("wsgi.py missing at repo root")
elif 'sys.modules.setdefault("app"' not in read(wsgi):
    failures.append(
        "wsgi.py lost the module alias sys.modules.setdefault(\"app\", ...) — "
        "lazy `from app import X` would re-boot the app inside request threads")

# ── 2. lock_timeout on both migration runners ───────────────────────────
app_src = read(HERE / "app.py")
n_lock = app_src.count("SET lock_timeout = '5000'")
if n_lock < 2:
    failures.append(
        f"app.py has {n_lock} `SET lock_timeout = '5000'` (need >= 2: the "
        "legacy auto-migrate block AND _run_idempotent_migrations)")

# ── 3. no ALTER TABLE outside app.py ────────────────────────────────────
for py in sorted(HERE.glob("*.py")):
    if py.name in ("app.py", Path(__file__).name):
        continue
    src = read(py)
    for m in re.finditer(r"ALTER\s+TABLE", src):
        line = src.count("\n", 0, m.start()) + 1
        failures.append(
            f"{py.name}:{line} contains ALTER TABLE — schema changes belong "
            "in app.py's lock_timeout-protected migration runners")

# ── 4. urlopen without timeout ──────────────────────────────────────────
# Join each urlopen(...) call's argument span and require timeout= in it.
for py in sorted(HERE.glob("*.py")) + [wsgi]:
    if not py.exists() or py.name == Path(__file__).name:
        continue
    src = read(py)
    for m in re.finditer(r"urlopen\s*\(", src):
        depth, i = 1, m.end()
        while i < len(src) and depth and i - m.end() < 2000:
            if src[i] == "(":
                depth += 1
            elif src[i] == ")":
                depth -= 1
            i += 1
        span = src[m.end():i]
        if "timeout" not in span:
            line = src.count("\n", 0, m.start()) + 1
            failures.append(
                f"{py.name}:{line} urlopen() without timeout= — an unbounded "
                "network call wedges a gunicorn thread when the remote stalls")

# ── 5. regression locks on the 2026-06-10 fixes ─────────────────────────
af_src = read(HERE / "audited_financials.py")
if "end the read txn before slow work" not in af_src:
    failures.append(
        "audited_financials.py lost the extract worker's "
        "rollback-before-slow-work (txn held across the Claude call wedged "
        "audit_uploads site-wide)")
if "_pdf_heal_gate" not in af_src:
    failures.append(
        "audited_financials.py lost the PDF self-heal single-flight gate "
        "(_pdf_heal_gate) — concurrent inline SharePoint fetches ate every "
        "worker thread")

if failures:
    print("\nCONCURRENCY-DISCIPLINE GATE FAILED — an outage pattern is back:")
    for f in failures:
        print(f"  [X] {f}")
    print("\nSee memory: feedback_db_lock_discipline.md (2026-06-10 incidents).")
    sys.exit(1)

print("Concurrency-discipline gate OK (alias + lock_timeouts + bounded "
      "network + fix sentinels verified).")
