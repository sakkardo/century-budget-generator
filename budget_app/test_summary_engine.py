#!/usr/bin/env python3
"""Deterministic gate: summary_engine.compute_summary vs frozen real-building
vectors (clean-architecture tranche 2a, 2026-07-05).

The vectors in summary_test_vectors.json are REAL production inputs (raw
budget_summary_rows columns, BudgetLine.to_dict() dicts, the shell-derived
RE-tax scalars) captured 2026-07-05 for 829 (sectioned + TBC/op-assess pins),
148 (flat format), and 215 (TBC + zero-DOF net-tax edge). Expected outputs are
the live /api/summary responses from the SAME commit, which were byte-proven
identical to the pre-extraction route across all 20 built buildings.

If this fails, the summary math drifted: col7 cascade/pins, ordinal
bucketing, footing, orphan/duplicate scans, or the aggregation. If the change
is INTENTIONAL, regenerate the vectors (scratchpad build_summary_vectors.py
against prod after deploying the intended change) in the same commit.

Runs with no DB and no network: summary rows are rebuilt as SimpleNamespace.
"""
import json
import os
import sys
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from summary_engine import compute_summary  # noqa: E402

VEC = os.path.join(HERE, "summary_test_vectors.json")


def _norm(obj):
    """JSON round-trip so float repr / key order can't cause false diffs."""
    return json.loads(json.dumps(obj, sort_keys=True))


def main():
    vectors = json.load(open(VEC, encoding="utf-8"))
    fails = []
    for v in vectors:
        rows = [SimpleNamespace(**r) for r in v["summary_rows"]]
        got = compute_summary(
            v["entity_code"], v["budget_year"], rows, v["bl_dicts"],
            v["ytd_months"], row_au=v["row_au"],
            op_assess_proposed=v["op_assess_proposed"],
            re_tax_exemptions_budget=v["re_tax_exemptions_budget"],
        )
        got_n, exp_n = _norm(got), _norm(v["expected"])
        if got_n == exp_n:
            print("  %s OK (%d rows)" % (v["entity_code"], len(got_n["rows"])))
            continue
        # pinpoint the first differing cell for the failure message
        detail = "payload keys differ"
        g_rows = {r["label"]: r for r in got_n.get("rows", [])}
        e_rows = {r["label"]: r for r in exp_n.get("rows", [])}
        for lbl in sorted(set(g_rows) | set(e_rows)):
            gr, er = g_rows.get(lbl), e_rows.get(lbl)
            if gr is None or er is None:
                detail = "row %r %s" % (lbl, "missing" if gr is None else "unexpected")
                break
            for c in sorted(set(gr) | set(er)):
                if gr.get(c) != er.get(c):
                    detail = ("row %r %s: got %s expected %s"
                              % (lbl, c, repr(gr.get(c))[:200], repr(er.get(c))[:200]))
                    break
            else:
                continue
            break
        fails.append("%s: %s" % (v["entity_code"], detail))
    if fails:
        sys.stderr.write("SUMMARY-ENGINE GATE FAILED — output drifted from frozen vectors:\n")
        for f in fails:
            sys.stderr.write("  " + f + "\n")
        sys.stderr.write("If intentional: regenerate summary_test_vectors.json in the same commit.\n")
        sys.exit(1)
    print("summary_engine OK: %d real-building vectors match frozen /api/summary outputs." % len(vectors))


if __name__ == "__main__":
    main()
