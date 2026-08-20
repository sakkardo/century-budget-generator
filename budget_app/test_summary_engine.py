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
    # F3 regression (204 dry run 2026-07-06): duplicate-labeled rows must
    # receive the audit value ONCE, with a loud warning — 204's two 'Capital
    # Assessment' rows silently double-counted $1,005,049 before this.
    dup_rows = [
        SimpleNamespace(id=1, label="Capital Assessment", section="Non-Operating Income",
                        row_type="data", display_order=1, gl_prefixes_json=None,
                        source_tab=None, footnote_marker=None, cell_formulas_json=None,
                        col1_prior_actual=None, col6_approved_budget=None,
                        col7_proposed_budget=None, col1_override=None, col2_override=None,
                        col3_override=None, col4_override=None, col5_override=None,
                        col6_override=None),
        SimpleNamespace(id=2, label="Capital Assessment", section="Non-Operating Income",
                        row_type="data", display_order=2, gl_prefixes_json=None,
                        source_tab=None, footnote_marker=None, cell_formulas_json=None,
                        col1_prior_actual=None, col6_approved_budget=None,
                        col7_proposed_budget=None, col1_override=None, col2_override=None,
                        col3_override=None, col4_override=None, col5_override=None,
                        col6_override=None),
    ]
    row_au = (177, json.dumps({"Capital Assessment": {
        "total": 1005049, "year_totals": [1005049, 0], "years": [],
        "source_lines": [{"auditor_desc": "Capital Assessment", "amounts": [1005049, 0]}],
    }}), "2025", None, None, "x.pdf", None, None)
    out = compute_summary("999", 2027, dup_rows, [], 4, row_au=row_au)
    c2s = [r.get("col2") for r in out["rows"]]
    assert c2s[0] == 1005049 and c2s[1] is None, (
        "F3 REGRESSION: duplicate-label col2 = %r (want [1005049, None])" % c2s)
    warn_types = {w.get("type") for w in out.get("warnings", [])}
    assert "duplicate_label_col2" in warn_types, (
        "F3 REGRESSION: duplicate-label warning missing (got %r)" % warn_types)
    print("  F3 dup-label case OK (single assignment + loud warning)")

    # FA 724 #4 (Jennifer 2026-08-18): plan_section_organization vectors —
    # hand-built, no DB. Covers the 724 class (data-only rows), GL-prefix
    # classification of unsectioned rows, and the two noop classes.
    from summary_engine import plan_section_organization

    def _prow(i, label, section, rt="data", pj=None):
        return SimpleNamespace(id=i, label=label, section=section, row_type=rt,
                               display_order=i, gl_prefixes_json=pj)

    rows_a = [
        _prow(1, "Maintenance", "Income"),
        _prow(2, "Payroll", "Expenses"),
        _prow(3, "Storage Room", "Income"),
        _prow(4, "Capital Expenses", "Non-Operating Expense"),
        _prow(5, "Mystery Inc", None, pj='["4130-0010"]'),
        _prow(6, "Mystery Exp", None, pj='["6315"]'),
    ]
    plan = plan_section_organization(rows_a)
    assert plan is not None, "724-class rows must produce a plan"
    assert [rid for rid, _, _ in plan["order"]] == [1, 3, 5, 2, 6, 4], (
        "data order must be income(1,3,5) expenses(2,6) noe(4): %r" % plan["order"])
    omap = {rid: (o, s) for rid, o, s in plan["order"]}
    assert omap[1] == (2, "Income") and omap[3] == (3, "Income") and omap[5] == (4, "Income")
    assert omap[2] == (7, "Expenses") and omap[6] == (8, "Expenses")
    assert omap[4] == (14, "Non-Operating Expense")
    ins = {i["label"]: (i["display_order"], i["section"], i["row_type"]) for i in plan["inserts"]}
    assert ins["Income"] == (1, "Income", "section_header")
    assert ins["Total Income"] == (5, "Income", "subtotal")
    assert ins["Expenses"] == (6, "Expenses", "section_header")
    assert ins["Total Expenses"] == (9, "Expenses", "subtotal")
    assert ins["Net Operating Surplus <Deficit>"] == (10, "Expenses", "subtotal")
    assert ins["Non-Operating Income"] == (11, "Non-Operating Income", "section_header")
    assert ins["Total Non-Operating Income"] == (12, "Non-Operating Income", "subtotal")
    assert ins["Non-Operating Expense"] == (13, "Non-Operating Expense", "section_header")
    assert ins["Total Non-Operating Expenses"] == (15, "Non-Operating Expense", "subtotal")
    assert ins["Total Surplus <Deficit>"] == (16, "Non-Operating Expense", "subtotal")
    _all_orders = sorted([o for _, o, _ in plan["order"]] +
                         [i["display_order"] for i in plan["inserts"]])
    assert _all_orders == list(range(1, 17)), "orders must be contiguous 1..16"

    # Sectioned building (733 class): any subtotal present -> noop.
    assert plan_section_organization(
        rows_a + [_prow(99, "Total Income", "Income", rt="subtotal")]) is None
    # Flat-with-totals building (148 class): subtotals, no headers -> noop.
    assert plan_section_organization([
        _prow(1, "Maintenance", None),
        _prow(2, "Total Income", None, rt="subtotal"),
    ]) is None
    # Degenerate inputs.
    assert plan_section_organization([]) is None
    assert plan_section_organization([_prow(1, "Income", "Income", rt="section_header")]) is None

    # The organized layout must satisfy the ENGINE's own matchers: run
    # compute_summary over the planned structure and check the subtotals
    # actually foot (income 100+50, expenses 30 -> net 120, grand 120-40=80).
    organized = []
    vals = {1: 100.0, 3: 50.0, 5: None, 2: 30.0, 6: None, 4: 40.0}
    for i in plan["inserts"]:
        organized.append(SimpleNamespace(
            id=1000 + i["display_order"], label=i["label"], section=i["section"],
            row_type=i["row_type"], display_order=i["display_order"],
            gl_prefixes_json=None, source_tab=None, footnote_marker=None,
            cell_formulas_json=None, col1_prior_actual=None,
            col6_approved_budget=None, col7_proposed_budget=None,
            col1_override=None, col2_override=None, col3_override=None,
            col4_override=None, col5_override=None, col6_override=None))
    for rid, o, s in plan["order"]:
        src = next(r for r in rows_a if r.id == rid)
        organized.append(SimpleNamespace(
            id=rid, label=src.label, section=s, row_type="data",
            display_order=o, gl_prefixes_json=src.gl_prefixes_json,
            source_tab=None, footnote_marker=None, cell_formulas_json=None,
            col1_prior_actual=vals[rid], col6_approved_budget=vals[rid],
            col7_proposed_budget=None, col1_override=None, col2_override=None,
            col3_override=None, col4_override=None, col5_override=None,
            col6_override=None))
    organized.sort(key=lambda r: r.display_order)
    out2 = compute_summary("998", 2027, organized, [], 4)
    subs = {r["label"]: r for r in out2["rows"] if r["row_type"] == "subtotal"}
    assert subs["Total Income"]["col1"] == 150.0, subs["Total Income"]
    assert subs["Total Expenses"]["col1"] == 30.0, subs["Total Expenses"]
    assert subs["Net Operating Surplus <Deficit>"]["col1"] == 120.0
    assert subs["Total Non-Operating Expenses"]["col1"] == 40.0
    assert subs["Total Surplus <Deficit>"]["col1"] == 80.0
    print("  plan_section_organization vectors OK (724-class plan + engine footing round-trip)")

    print("summary_engine OK: %d real-building vectors match frozen /api/summary outputs." % len(vectors))


if __name__ == "__main__":
    main()
