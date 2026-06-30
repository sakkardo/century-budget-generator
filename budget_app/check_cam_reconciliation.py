#!/usr/bin/env python3
"""CAM-allocation reconciliation guard (run by deploy.sh before every push).

The condo CAM allocation (Schedule A-1) splits each operating-expense GL across
unit classes by proportionate share. The whole feature is worthless if the math
doesn't tie out, so this gate freezes two correctness claims:

  1. STATIC FREEZE — `_cam_compute` in workflow.py still carries its
     reconciliation invariants (per-row residual to the largest cell, per-class
     column totals, Σ-shares == 100% check, Σ-columns == grand-total check). If
     you intentionally change the allocation math, update the sentinels here in
     the same commit.

  2. ALGORITHM SELF-CHECK — a self-contained re-implementation of the split +
     residual-reconcile (no Flask/DB import) proves that every row reconciles to
     its line total to the cent for representative inputs, incl. the live 347 —
     500 Waverly numbers (Res 76.5953 / Retail 16.6464 / Garage 6.7583%).

Usage:
  check_cam_reconciliation.py [path/to/workflow.py]   # verify; exit 1 on regression
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def _func_body(src, fn_name):
    m = re.search(r"\n([ \t]*)def %s\b" % re.escape(fn_name), src)
    if not m:
        return None
    indent = len(m.group(1))
    lines = src[m.start() + 1:].split("\n")
    out = [lines[0]]
    for ln in lines[1:]:
        stripped = ln.lstrip()
        cur_indent = len(ln) - len(stripped)
        if stripped.startswith("def ") and cur_indent <= indent:
            break
        out.append(ln)
    return "\n".join(out)


def _b_split_reconcile(total, shares, target_idx=None):
    """Reference allocation: building-wide split (or 100% to target_idx) with the
    rounding residual pushed to the largest cell, mirroring _cam_compute."""
    n = len(shares)
    if target_idx is not None:
        cells = [round(total, 2) if i == target_idx else 0.0 for i in range(n)]
    else:
        ssum = sum(shares) or 1.0
        cells = [round(total * (s / ssum), 2) for s in shares]
    diff = round(total - sum(cells), 2)
    if abs(diff) >= 0.01:
        # residual → largest cell (by abs value, else largest share)
        order = sorted(range(n), key=lambda i: (abs(cells[i]), shares[i]))
        rc = order[-1]
        cells[rc] = round(cells[rc] + diff, 2)
    return cells


def main():
    wf = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "workflow.py")
    src = open(wf, encoding="utf-8").read()
    fails = []

    # ── 1) Static freeze of the reconciliation invariants ──────────────────
    body = _func_body(src, "_cam_compute")
    if not body:
        sys.stderr.write("CAM GATE: _cam_compute not found in %s\n" % wf)
        sys.exit(1)
    if not _func_body(src, "_cam_line_amount"):
        fails.append("_cam_line_amount (the allocatable-amount helper) is missing.")

    checks = {
        "per-row residual reconcile (diff = round(total - sum(cells...)))":
            re.search(r"diff\s*=\s*round\(\s*total\s*-\s*sum\(\s*cells", body),
        "residual pushed to a cell (cells[rc.id] = round(cells...+ diff))":
            re.search(r"cells\[[^\]]+\]\s*=\s*round\([^\n]*\bdiff\b", body),
        "per-class column totals accumulated":
            "column_totals" in body,
        "Σ-shares == 100% validation (shares_ok / abs(share_sum - 1.0))":
            ("shares_ok" in body and "share_sum" in body),
        "final footing check (reconciles via abs(col_sum - grand_total))":
            ("reconciles" in body and "grand_total" in body),
    }
    for label, ok in checks.items():
        if not ok:
            fails.append("_cam_compute lost: %s." % label)

    # ── 2) Algorithm self-check: rows must reconcile to the cent ───────────
    cases = [
        # (label, total, shares, target_idx)
        ("347 Superintendent B-split", 86734.00, [0.765953, 0.166464, 0.067583], None),
        ("347 Plumbing B-split", 8500.00, [0.765953, 0.166464, 0.067583], None),
        ("drift-prone thirds", 100.00, [1 / 3, 1 / 3, 1 / 3], None),
        ("4-class uneven", 12345.67, [0.6, 0.25, 0.1, 0.05], None),
        ("R 100% to residential", 4000.00, [0.765953, 0.166464, 0.067583], 0),
        ("negative/credit line", -1500.00, [0.5, 0.3, 0.2], None),
    ]
    for label, total, shares, tgt in cases:
        cells = _b_split_reconcile(total, shares, tgt)
        if round(sum(cells), 2) != round(total, 2):
            fails.append("RECONCILE FAIL [%s]: row sums to %.2f, expected %.2f (cells=%s)"
                         % (label, sum(cells), total, cells))
    # 347 cells must match the live Excel to the dollar (sanity on the split).
    sup = _b_split_reconcile(86734.00, [0.765953, 0.166464, 0.067583])
    if not (abs(sup[0] - 66434) < 1 and abs(sup[1] - 14438) < 1 and abs(sup[2] - 5862) < 1):
        fails.append("347 Superintendent B-split %s != Excel 66434/14438/5862." % sup)

    if fails:
        sys.stderr.write("\nCAM RECONCILIATION GATE FAILED:\n")
        for f in fails:
            sys.stderr.write("  - %s\n" % f)
        sys.stderr.write("CAM allocation must tie out (Σ class cells = line total; "
                         "Σ columns = total expense). See CAM_ALLOCATION_DESIGN_2026-06-17.md.\n")
        sys.exit(1)

    print("CAM reconciliation gate OK (_cam_compute invariants frozen; "
          "split+residual reconciles to the cent across 6 cases incl. 347).")


if __name__ == "__main__":
    main()
