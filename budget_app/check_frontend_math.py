#!/usr/bin/env python3
"""Frontend math drift guard (run by deploy.sh before every push).

The core budget math (estimate / forecast / proposed) is still duplicated across
several embedded-JS variants (FA dashboard, PM portal, bp2 summary) that have
historically DRIFTED apart silently -- the root cause of the variance / income-pin
class of bugs. The real fix is extracting the JS to one shared module (Phase 3).

Until then, this guard FREEZES each variant's current logic: it finds every definition
of the math functions, normalizes away comments + whitespace, hashes the body, and
fails the push if any body changes (or a copy is added/removed) without an explicit
--update. So no copy can be edited or drift unnoticed. When you DO change one on
purpose, the failure reminds you to update the sibling copies to match, then re-snapshot.

Usage:
  check_frontend_math.py --update [path/to/workflow.py]   # re-freeze current state
  check_frontend_math.py [path/to/workflow.py]            # verify; exit 1 on drift
"""
import hashlib
import json
import os
import re
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
SNAP = os.path.join(HERE, "data", "frontend_math_snapshot.json")

# The core math functions whose copies must not drift unnoticed.
MATH_FNS = [
    "faComputeEstimate", "faComputeForecast", "faComputeProposed",
    "computeEstimate", "computeForecast", "computeProposed",
]


def _brace_body(src, start):
    """Return the function text from `start` through its matching closing brace."""
    open_idx = src.index("{", start)
    depth = 0
    for j in range(open_idx, len(src)):
        c = src[j]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return src[start:j + 1]
    return src[start:]


def _norm_hash(body):
    body = re.sub(r"//[^\n]*", "", body)   # drop line comments
    body = re.sub(r"\s+", " ", body)        # collapse whitespace
    return hashlib.sha256(body.strip().encode("utf-8")).hexdigest()[:16]


def extract(src):
    out = []
    for name in MATH_FNS:
        for m in re.finditer(r"function %s\s*\(" % re.escape(name), src):
            out.append((name, _norm_hash(_brace_body(src, m.start()))))
    return sorted(out)


def main():
    args = [a for a in sys.argv[1:] if a != "--update"]
    update = "--update" in sys.argv[1:]
    wf = args[0] if args else os.path.join(HERE, "workflow.py")
    current = extract(open(wf, encoding="utf-8").read())
    if not current:
        sys.stderr.write("FRONTEND-MATH GATE: no math functions found in %s\n" % wf)
        sys.exit(1)

    if update:
        os.makedirs(os.path.dirname(SNAP), exist_ok=True)
        json.dump([[n, h] for n, h in current], open(SNAP, "w"), indent=1)
        print("Frontend-math snapshot updated: %d definitions frozen." % len(current))
        return

    if not os.path.exists(SNAP):
        sys.stderr.write("FRONTEND-MATH GATE: no snapshot at %s -- run --update first.\n" % SNAP)
        sys.exit(1)

    expected = sorted((n, h) for n, h in json.load(open(SNAP)))
    if current != expected:
        sys.stderr.write("\nFRONTEND-MATH DRIFT GATE FAILED -- a budget-math copy changed:\n")
        exp_n, cur_n = Counter(n for n, _ in expected), Counter(n for n, _ in current)
        for name in MATH_FNS:
            if exp_n[name] != cur_n[name]:
                sys.stderr.write("  %s: %d copy(ies) -> %d (a copy was added/removed)\n"
                                 % (name, exp_n[name], cur_n[name]))
        for name in sorted({n for (n, h) in set(current) - set(expected)}):
            sys.stderr.write("  %s: body changed\n" % name)
        sys.stderr.write("If intentional: update the sibling copies to match, then "
                         "run check_frontend_math.py --update (same commit).\n")
        sys.exit(1)

    print("Frontend-math drift gate OK (%d math definitions frozen)." % len(current))


if __name__ == "__main__":
    main()
