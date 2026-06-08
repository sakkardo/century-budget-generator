#!/usr/bin/env python3
"""Model-inventory guard (run by deploy.sh before every push).

The 18 SQLAlchemy models are the data layer. Step 7 of the de-monolith moves them
out of workflow.py into models.py. A botched move could silently drop a model, rename
a table, or lose a column -- and the route/formula/frontend guards would NOT catch it
(they don't introspect the ORM). This guard freezes the schema surface: for every
db.Model class (wherever it lives -- workflow.py today, models.py after Step 7), it
records the table name, column count, and relationship count, and fails the push if
any model disappears, gains/loses a column, or changes its table name without an
explicit --update.

It scans by AST (no app boot, no db needed), so it runs in the same cheap pre-push
slot as the other gates and works identically before and after the extraction.

Usage:
  check_model_inventory.py --update          # re-freeze current model inventory
  check_model_inventory.py                    # verify; exit 1 on any drift
"""
import ast
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SNAP = os.path.join(HERE, "data", "model_inventory.json")

# Files that may contain db.Model classes (now + after Step 7). Missing files skipped.
CANDIDATE_FILES = ["workflow.py", "models.py"]


def _is_model(classdef):
    for b in classdef.bases:
        if isinstance(b, ast.Attribute) and b.attr == "Model":
            return True
    return False


def _call_attr(node):
    """If node is a Call to something.<attr>(...), return <attr>, else None."""
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _tablename(classdef):
    for item in classdef.body:
        if isinstance(item, ast.Assign):
            for t in item.targets:
                if isinstance(t, ast.Name) and t.id == "__tablename__":
                    if isinstance(item.value, ast.Constant):
                        return item.value.value
    return None


def _counts(classdef):
    """Count class-level db.Column(...) and db.relationship(...) assignments."""
    cols = rels = 0
    for item in classdef.body:
        if isinstance(item, (ast.Assign, ast.AnnAssign)):
            attr = _call_attr(item.value) if item.value else None
            if attr == "Column":
                cols += 1
            elif attr == "relationship":
                rels += 1
    return cols, rels


def extract(base_dir=None):
    # base_dir lets deploy.sh point the gate at the /tmp clone it is about to push
    # (matching the route/frontend-math gates), not the local working copy. The
    # snapshot is always read from HERE (the committed baseline).
    base_dir = base_dir or HERE
    inv = {}
    for fn in CANDIDATE_FILES:
        path = os.path.join(base_dir, fn)
        if not os.path.exists(path):
            continue
        tree = ast.parse(open(path, encoding="utf-8").read(), fn)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and _is_model(node):
                cols, rels = _counts(node)
                inv[node.name] = {
                    "table": _tablename(node),
                    "columns": cols,
                    "relationships": rels,
                }
    return inv


def main():
    args = [a for a in sys.argv[1:] if a != "--update"]
    update = "--update" in sys.argv[1:]
    base_dir = args[0] if args else None
    current = extract(base_dir)
    if not current:
        sys.stderr.write("MODEL-INVENTORY GATE: no db.Model classes found.\n")
        sys.exit(1)

    if update:
        os.makedirs(os.path.dirname(SNAP), exist_ok=True)
        json.dump(current, open(SNAP, "w"), indent=1, sort_keys=True)
        print("Model-inventory snapshot updated: %d models frozen." % len(current))
        return

    if not os.path.exists(SNAP):
        sys.stderr.write("MODEL-INVENTORY GATE: no snapshot at %s -- run --update first.\n" % SNAP)
        sys.exit(1)

    expected = json.load(open(SNAP))
    problems = []
    for name in sorted(set(expected) | set(current)):
        if name not in current:
            problems.append("  MODEL DROPPED: %s (was table=%s, %s cols)"
                            % (name, expected[name]["table"], expected[name]["columns"]))
        elif name not in expected:
            problems.append("  MODEL ADDED: %s (table=%s, %s cols) -- intentional? --update"
                            % (name, current[name]["table"], current[name]["columns"]))
        elif current[name] != expected[name]:
            problems.append("  MODEL CHANGED: %s\n      was %s\n      now %s"
                            % (name, expected[name], current[name]))

    if problems:
        sys.stderr.write("\nMODEL-INVENTORY GATE FAILED -- the data layer changed:\n")
        sys.stderr.write("\n".join(problems) + "\n")
        sys.stderr.write("If intentional (e.g. a real migration): run "
                         "check_model_inventory.py --update in the same commit.\n")
        sys.exit(1)

    print("Model-inventory gate OK (%d models; tables/columns/relationships frozen)."
          % len(current))


if __name__ == "__main__":
    main()
