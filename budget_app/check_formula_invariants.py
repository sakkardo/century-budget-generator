#!/usr/bin/env python3
"""Regression gate for the formula-bar work (run by deploy.sh before every push).

THE PRODUCT RULE: every cell across the app must behave like an Excel cell — the
formula bar shows the NUMBERS and the EQUATION (never bare text or a lone single
figure), totals flow to the right cell, and edits recompute. This 32k-line
monolith has silently regressed before (full-file rewrites have dropped logic),
so this canary BLOCKS any deploy that reverts one of the fixes below.

If you change one of these on purpose, update the matching check in the SAME
commit — don't just delete the check.

Usage:
    check_formula_invariants.py [path/to/workflow.py] [path/to/expense_distribution.py]
Exit 0 = all invariants hold; exit 1 = a fix regressed (prints which).
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
WF = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "workflow.py")
ED = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "expense_distribution.py")


def load(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return None


wf = load(WF)
ed = load(ED)

# (label, source, must_be_PRESENT, must_be_ABSENT)
#   source: "wf" -> workflow.py, "ed" -> expense_distribution.py
CHECKS = [
    ("FA grid totals addressed by data-col, not positional cells[N] "
     "(the 'proposed column doesn't add up' scramble bug)", "wf",
     ["setCol('proposed'", "Address cells by data-col"],
     ["setC(cells[1], t.prior)"]),

    ("FA subtotal Variance / % cells show the NUMBERS, not text", "wf",
     ["_fxDerived"],
     ["bar.value = '= Curr Budget"]),

    ("RE-tax Forecast column shows '= YTD + Estimate' numbers, not =SUM(D:E)", "wf",
     ["not A1-addressable"],
     []),

    ("PM portal subtotals show the numeric breakdown, not '=SUM(...)'", "wf",
     ["_pmCatGLs"],
     ["bar.value = '=SUM(...)'"]),

    ("Summary formula bar shows the equation (all numbers adding); no "
     "'sum of N lines' collapse", "wf",
     ["dataset._fxeq"],
     ["'= sum of ' + vals.length"]),

    ("PM portal summary shows every GL number; no 'SUM of N GL lines' collapse", "wf",
     ["_buildSumFormula"],
     ["SUM of ' + vals.length + ' GL lines"]),

    ("ExpDist parser guards short rows (no 'tuple index out of range')", "ed",
     ["def _cell(row, i)"],
     []),
]

fails = []
for label, src, present, absent in CHECKS:
    text = wf if src == "wf" else ed
    if text is None:
        fails.append("  [%s] could not read %s" % (label, WF if src == "wf" else ED))
        continue
    for needle in present:
        if needle not in text:
            fails.append("  [%s] MISSING expected code: %r" % (label, needle))
    for needle in absent:
        if needle in text:
            fails.append("  [%s] REGRESSED — old pattern is back: %r" % (label, needle))

# ─── Excel-formula validity (line-scoped) ──────────────────────────────────
# THE PRODUCT RULE, machine-enforced: every formula-string assignment must be a
# valid, Excel-parseable formula — no Unicode math operators (U+2212 '−', U+00D7
# '×'), no old "= ... = result" human prefix. This freezes the "Invalid formula" /
# non-exportable class (fixed across Summary, FA tabs, PM portal in 2026-06-08) so
# it cannot silently regress. Comments may use − / × in prose, so they're skipped.
_FORMULA_MARKERS = ('data-formula="', '.dataset.formula', 'bar.value =', 'barInput.value =')
for _i, _line in enumerate((wf or "").splitlines(), 1):
    _s = _line.lstrip()
    if _s.startswith("//") or _s.startswith("#") or _s.startswith("*"):
        continue  # comment line — prose may use − / ×
    if 'data-formula="= ' in _line:
        fails.append("  [excel-formula] line %d: data-formula uses the old '= ' human prefix; "
                     "emit a bare Excel '=expr' (e.g. via sumExcelExpr/pmXlExpr)" % _i)
    if any(_m in _line for _m in _FORMULA_MARKERS) and ("−" in _line or "×" in _line):
        fails.append("  [excel-formula] line %d: Unicode math operator (− or ×) in a formula "
                     "string; use ASCII '-' / '*'" % _i)

if fails:
    sys.stderr.write("\nFORMULA-INVARIANT GATE FAILED — a formula-bar fix regressed:\n")
    sys.stderr.write("\n".join(fails) + "\n")
    sys.stderr.write("\nIf this change was intentional, update "
                     "budget_app/check_formula_invariants.py in the same commit.\n")
    sys.exit(1)

print("Formula-invariant gate OK (%d invariants verified)." % len(CHECKS))
sys.exit(0)
