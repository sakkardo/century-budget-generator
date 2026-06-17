#!/usr/bin/env python3
"""Excel-export values-snapshot guard (run by deploy.sh before every push).

The client-facing budget export (`/api/export-excel`) MUST write computed
VALUES into the Budget Summary + detail tabs, NOT un-evaluated Excel formula
strings. A formula-only export ships with no cached values, so it opens BLANK
in any viewer that doesn't recalc-on-open (Quick Look, the browser / Google
Drive / Outlook preview, Google Sheets, or Excel set to manual calc). That was
the 2026-06-16 "downloaded file is drastically different from the product" bug
(see project_excel_export_rebuild.md — the values-snapshot pivot).

This gate statically freezes that fix: it verifies the two core builders still
emit values and have not regressed to wiring formula strings into the data
cells. If you intentionally change the export's value layer, update the
sentinels here in the same commit.

Usage:
  check_export_values.py [path/to/workflow.py]   # verify; exit 1 on regression
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def _func_body(src, fn_name):
    """Return the source text of a (nested) `def fn_name(...)` through the line
    just before the next def at the same indentation."""
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


def main():
    wf = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "workflow.py")
    src = open(wf, encoding="utf-8").read()

    detail = _func_body(src, "_export_rewrite_detail_tab")
    summary = _func_body(src, "_export_rewrite_budget_summary")
    if not detail or not summary:
        sys.stderr.write("EXPORT-VALUES GATE: export builder(s) not found in %s\n" % wf)
        sys.exit(1)

    fails = []

    # ── Detail tabs must write VALUES (budget_math), not the =SUMIF lambda ──
    if "sumif = lambda" in detail:
        fails.append("_export_rewrite_detail_tab reintroduced the `sumif = lambda` "
                     "=SUMIF formula wiring (data cells would ship blank).")
    if "budget_math.estimate" not in detail or "_subtot" not in detail:
        fails.append("_export_rewrite_detail_tab lost the values path "
                     "(expected budget_math.estimate + the `_subtot` subtotal sums).")

    # ── Budget Summary must source product VALUES, not wire SUMIFS into cells ──
    if "_pl_by_order" not in summary or "_blockcols" not in summary:
        fails.append("_export_rewrite_budget_summary lost the values path "
                     "(expected `_pl_by_order` product values + `_blockcols` subtotal sums).")
    if re.search(r"write_num\([^)]*\bdetail_sumifs\(", summary) or \
       re.search(r"write_num\([^)]*\bprefix_sumifs_yardi\(", summary):
        fails.append("_export_rewrite_budget_summary wired a SUMIFS formula back "
                     "into a summary cell (data cells would ship blank).")

    if fails:
        sys.stderr.write("\nEXPORT-VALUES GATE FAILED — the Excel export must ship "
                         "computed VALUES, not formulas:\n")
        for f in fails:
            sys.stderr.write("  - %s\n" % f)
        sys.stderr.write("Formula-only exports open blank in non-recalc viewers. "
                         "See project_excel_export_rebuild.md (values-snapshot pivot 2026-06-16).\n")
        sys.exit(1)

    print("Export-values gate OK (Budget Summary + detail tabs ship computed values).")


if __name__ == "__main__":
    main()
