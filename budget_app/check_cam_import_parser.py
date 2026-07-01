#!/usr/bin/env python3
"""CAM Excel-importer parser guard (run by deploy.sh before every push).

`_cam_parse_excel` (workflow.py) reads a condo's Schedule A-1 sheet and turns
it into unit classes + shares. It must (a) correctly extract the two REAL
building layouts confirmed by SharePoint reconnaissance on 2026-07-01 — 347
(3-class: Residential/Retail/Garage, horizontal) and 912 (2-class: Residential/
Commercial, horizontal) — and (b) NEVER fabricate a class list when the sheet
doesn't reconcile to ~100% (a wrong CAM split silently misallocates every
condo's expenses, so "don't guess" is a correctness requirement, not a nicety).

This gate extracts `_cam_parse_excel`'s source out of workflow.py (no Flask/DB
import needed — it's pure openpyxl) and runs it against synthetic workbooks
mirroring the confirmed real layouts plus edge cases.

Usage:
  check_cam_import_parser.py [path/to/workflow.py]   # verify; exit 1 on regression
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


def _load_parser(wf):
    src = open(wf, encoding="utf-8").read()
    body = _func_body(src, "_cam_parse_excel")
    if not body:
        return None
    # Dedent (the source is nested inside register_routes) and exec in an
    # isolated namespace — pure openpyxl logic, no Flask/DB needed.
    lines = body.split("\n")
    indent = len(lines[0]) - len(lines[0].lstrip())
    dedented = "\n".join(ln[indent:] if len(ln) >= indent else ln for ln in lines)
    ns = {}
    exec(dedented, ns)
    return ns.get("_cam_parse_excel")


def _wb(sheet_name, rows):
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name
    for r, row in enumerate(rows, start=1):
        for c, v in enumerate(row, start=1):
            if v is not None:
                ws.cell(row=r, column=c, value=v)
    return wb


def main():
    wf = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "workflow.py")
    parse = _load_parser(wf)
    if parse is None:
        sys.stderr.write("CAM IMPORT-PARSER GATE: _cam_parse_excel not found in %s\n" % wf)
        sys.exit(1)

    fails = []

    def check(label, wb, expect):
        try:
            got = parse(wb)
        except Exception as e:
            fails.append("%s: raised %r" % (label, e))
            return
        if expect is None:
            if got is not None:
                fails.append("%s: expected None (no guessing), got %r" % (label, got))
            return
        if got is None:
            fails.append("%s: expected a result, got None" % label)
            return
        names = {c["name"] for c in got}
        if names != set(expect.keys()):
            fails.append("%s: class names %r != expected %r" % (label, names, set(expect.keys())))
            return
        shares = {c["name"]: c["share_pct"] for c in got}
        for n, s in expect.items():
            if abs(shares[n] - s) > 0.0005:
                fails.append("%s: %s share %.6f != expected %.6f" % (label, n, shares[n], s))
        if abs(sum(shares.values()) - 1.0) > 1e-6:
            fails.append("%s: shares sum to %.6f, not 1.0" % (label, sum(shares.values())))

    # 347 — REAL confirmed layout (SharePoint recon 2026-07-01): horizontal,
    # "Total Expenses | Residential | Retail | Garage | Total" header row,
    # "2026 Budget | 76.5953% | 16.6464% | 6.7583% | 100.0000%" share row.
    check("347 (3-class, real layout)",
          _wb("CAM Allocation", [
              [None, "CAM Allocation"], [None, "Schedule A-1"],
              [None, "Total Expenses", "Residential", "Retail", "Garage", "Total"],
              [None, "2026 Budget", 76.5953, 16.6464, 6.7583, 100.0000],
              [None, "5105-0000", "Superintendent", "B", 86734, 66434, 14438, 5862, 86734],
          ]),
          {"Residential": 0.765953, "Retail": 0.166464, "Garage": 0.067583})

    # 912 Etage — REAL confirmed layout (SharePoint recon 2026-07-01): same
    # horizontal pattern, 2-class.
    check("912 (2-class, real layout)",
          _wb("Schedule A-1", [
              [None, "CAM Allocation"], [None, "Schedule A-1"],
              [None, "Total Expenses", "Residential", "Commercial", "Total"],
              [None, "2026 Budget", 87.4000, 12.6000, 100.0000],
              [None, "5255-0000", "Electric", "R", 12000, 12000, 0, 12000],
          ]),
          {"Residential": 0.874, "Commercial": 0.126})

    # String "%" values must parse the same as numeric percents.
    check("string '%' values",
          _wb("CAM ALLOCATION", [
              [None, "Residential", "Retail", "Garage"],
              [None, "76.5953%", "16.6464%", "6.7583%"],
          ]),
          {"Residential": 0.765953, "Retail": 0.166464, "Garage": 0.067583})

    # Vertical fallback (label col / share col, one row per class).
    check("vertical layout (4-class)",
          _wb("A-1", [
              [None, "Unit Type", "% Common Interest"],
              [None, "Residential", 0.60], [None, "Storage", 0.15],
              [None, "NCU", 0.15], [None, "SCU", 0.10],
          ]),
          {"Residential": 0.60, "Storage": 0.15, "NCU": 0.15, "SCU": 0.10})

    # ── "Don't guess" — must return None, never fabricate ──────────────
    check("no CAM sheet at all",
          _wb("Income", [[None, "Category", "Amount"], [None, "Rent", 50000]]),
          None)
    check("sheet named right but garbage numbers (don't reconcile)",
          _wb("Schedule A-1", [[None, "Foo", "Bar"], [None, 5, 3]]),
          None)
    check("single class only (needs >=2)",
          _wb("Schedule A-1", [[None, "Residential"], [None, 1.0]]),
          None)

    if fails:
        sys.stderr.write("\nCAM IMPORT-PARSER GATE FAILED:\n")
        for f in fails:
            sys.stderr.write("  - %s\n" % f)
        sys.stderr.write("_cam_parse_excel regressed against a confirmed real layout "
                         "or started guessing on a non-reconciling sheet. See "
                         "CAM_ALLOCATION_DESIGN_2026-06-17.md.\n")
        sys.exit(1)

    print("CAM import-parser gate OK (347 + 912 real layouts, %-string, vertical "
          "fallback, and 3 never-guess cases all pass).")


if __name__ == "__main__":
    main()
