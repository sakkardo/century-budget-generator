#!/usr/bin/env python3
"""Route-inventory regression gate (run by deploy.sh before every push).

WHY: full-file rewrites of workflow.py / app.py have silently DROPPED endpoints in
this codebase before. Any refactor (especially de-monolithing) must not make a route
disappear. This canary parses every @app.route / @bp.route decorator from the ACTIVE
budget_app modules (NOT the dead budget_summary/ copies) and compares the set of URL
paths to a frozen snapshot. If a previously-known route is gone, the push aborts.

Adding routes is fine (additive). Removing one on purpose: re-run with --update in the
SAME commit so the snapshot reflects the intentional change.

Usage:
  check_route_inventory.py --update [budget_app_dir]   # regenerate the snapshot
  check_route_inventory.py [budget_app_dir]            # verify; exit 1 if any route dropped
"""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SNAP = os.path.join(HERE, "data", "route_inventory.json")

# Modules that actually register routes on the live app. Deliberately excludes
# budget_summary/{workflow,app}.py (dead duplicate copies, never imported).
ACTIVE_MODULES = [
    "app.py", "workflow.py", "audited_financials.py", "expense_distribution.py",
    "open_ap.py", "maintenance_proof.py", "file_repository.py", "dof_taxes.py",
]

ROUTE_RE = re.compile(r"""@(?:app|bp)\.route\(\s*(['"])(?P<path>.*?)\1""", re.S)


def collect(app_dir):
    paths = set()
    for fn in ACTIVE_MODULES:
        p = os.path.join(app_dir, fn)
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8") as fh:
            txt = fh.read()
        for m in ROUTE_RE.finditer(txt):
            paths.add(m.group("path"))
    return paths


def main():
    args = [a for a in sys.argv[1:] if a != "--update"]
    update = "--update" in sys.argv[1:]
    app_dir = args[0] if args else HERE

    current = collect(app_dir)
    if not current:
        sys.stderr.write("ROUTE GATE: no routes found under %s — wrong path?\n" % app_dir)
        sys.exit(1)

    if update:
        os.makedirs(os.path.dirname(SNAP), exist_ok=True)
        json.dump(sorted(current), open(SNAP, "w"), indent=1)
        print("Route snapshot updated: %d routes -> %s" % (len(current), SNAP))
        return

    if not os.path.exists(SNAP):
        sys.stderr.write("ROUTE GATE: no snapshot at %s — run with --update first.\n" % SNAP)
        sys.exit(1)

    expected = set(json.load(open(SNAP)))
    missing = expected - current
    if missing:
        sys.stderr.write("\nROUTE-INVENTORY GATE FAILED — %d route(s) DROPPED:\n" % len(missing))
        for r in sorted(missing):
            sys.stderr.write("  %s\n" % r)
        sys.stderr.write("If intentional, run: check_route_inventory.py --update  (same commit)\n")
        sys.exit(1)

    added = current - expected
    print("Route-inventory gate OK (%d routes; %d new)." % (len(current), len(added)))


if __name__ == "__main__":
    main()
