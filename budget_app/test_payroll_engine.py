#!/usr/bin/env python3
"""Deterministic gate: payroll_engine vs hand-computed union-math vectors
(dry-run suggestion 3, 2026-07-06).

Expected values below were computed BY HAND from the documented rules —
not by running the engine — so this catches a port drift in either
direction. The roster mirrors the 204 dry-run roster (1 super / 1 handyman /
6 doormen) with the standard 32BJ-style assumption defaults.

If the FA dashboard's recalcPayroll math changes, change payroll_engine.py
AND these expected constants in the same commit.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from payroll_engine import compute_payroll, roster_gl_values  # noqa: E402

POSITIONS = [
    {"position_name": "Superintendent", "employee_count": 1, "hourly_rate": 32.40,
     "bonus_per_employee": 1000, "extra_bonuses": []},
    {"position_name": "Handyman", "employee_count": 1, "hourly_rate": 28.15,
     "bonus_per_employee": 750, "extra_bonuses": []},
    {"position_name": "Doorman", "employee_count": 6, "hourly_rate": 26.55,
     "bonus_per_employee": 500, "extra_bonuses": []},
]
ASSUMPTIONS = {
    "wage_increase_mode": "pct", "wage_increase_value": 0.03,
    "pre_increase_weeks": 15, "post_increase_weeks": 37,
    "ot_factor": 0.002, "vac_sick_hol_factor": 0.10,
    "fica": 0.0765, "sui": 0.021, "fui": 0.006, "mta": 0.0034,
    "nys_disability": 31.20, "pfl": 0.00388,
    "workers_comp": 0.0325,
    "welfare_monthly": 1200.0, "pension_weekly": 82.50,
    "supp_retirement_weekly": 10.0, "legal_monthly": 8.0,
    "training_monthly": 5.0, "profit_sharing_quarterly": 0.0,
}

# ── Hand-computed expectations ────────────────────────────────────────────
# Super:    40h × $32.40 = $1,296/wk. Pre 15wks = $19,440.
#           Post rate 32.40×1.03 = $33.372 → $1,334.88/wk × 37 = $49,390.56.
#           Annual base $68,830.56.
# Handyman: 40h × $28.15 = $1,126/wk. Pre 15 = $16,890.
#           Post 28.15×1.03 = $28.9945 → $1,159.78/wk × 37 = $42,911.86.
#           Annual base $59,801.86.
# Doorman:  40h × $26.55 = $1,062/wk. Pre 15 × 6 = $95,580.
#           Post 26.55×1.03 = $27.3465 → $1,093.86/wk × 37 × 6 = $242,836.92.
#           Annual base $338,416.92.
# Total annual base = 68,830.56 + 59,801.86 + 338,416.92 = $467,049.34
# OT = base × 0.002 = $934.09868 ; VSH = base × 0.10 = $46,704.934
# Gross wages = 467,049.34 + 934.09868 + 46,704.934 = $514,688.37268
# Bonus = 1,000 + 750 + 6×500 = $4,750
# FICA = gross × 0.0765 = $39,373.660...  SUI = 12,000×0.021×8 = $2,016
# FUI = 7,000×0.006×8 = $336              MTA = gross × 0.0034 = $1,749.94...
# NYS dis = 31.20 × 8 = $249.60           PFL = gross × 0.00388 = $1,996.99...
# WC = gross × 0.0325 = $16,727.372...
# Welfare = 1,200×8×12 = $115,200         Pension = 82.50×8×52 = $34,320
# SuppRet = 10×8×52 = $4,160              Legal = 8×8×12 = $768
# Training = 5×8×12 = $480                ProfitShare = 0
EXPECT = {
    "annual_base": 467049.34,
    "ot": 934.09868,
    "vsh_total": 46704.934,
    "gross_wages": 514688.37268,
    "bonus": 4750.0,
    "fica": 514688.37268 * 0.0765,
    "sui": 2016.0,
    "fui": 336.0,
    "mta": 514688.37268 * 0.0034,
    "nys_disability": 249.60,
    "pfl": 514688.37268 * 0.00388,
    "workers_comp": 514688.37268 * 0.0325,
    "welfare": 115200.0,
    "pension": 34320.0,
    "supp_retirement": 4160.0,
    "legal": 768.0,
    "training": 480.0,
}


def close(a, b, tol=0.01):
    return abs(a - b) <= tol


def main():
    fails = []
    r = compute_payroll(POSITIONS, ASSUMPTIONS)
    t = r["totals"]
    c = r["computed"]
    comp = r["components"]

    checks = [
        ("annual_base", t["annual_base"], EXPECT["annual_base"]),
        ("ot", t["ot"], EXPECT["ot"]),
        ("vsh", t["vsh"], EXPECT["vsh_total"]),
        ("gross_wages", t["gross_wages"], EXPECT["gross_wages"]),
        ("bonus", t["bonus"], EXPECT["bonus"]),
        ("fica", c["fica"], EXPECT["fica"]),
        ("sui", c["sui"], EXPECT["sui"]),
        ("fui", c["fui"], EXPECT["fui"]),
        ("mta", c["mta"], EXPECT["mta"]),
        ("nys_disability", c["nys_disability"], EXPECT["nys_disability"]),
        ("pfl", c["pfl"], EXPECT["pfl"]),
        ("workers_comp", c["workers_comp"], EXPECT["workers_comp"]),
        ("welfare", c["welfare"], EXPECT["welfare"]),
        ("pension", c["pension"], EXPECT["pension"]),
        ("supp_retirement", c["supp_retirement"], EXPECT["supp_retirement"]),
        ("legal", c["legal"], EXPECT["legal"]),
        ("training", c["training"], EXPECT["training"]),
    ]
    for name, got, want in checks:
        if not close(got, want):
            fails.append("%s: got %.4f want %.4f" % (name, got, want))

    # VSH split thirds + component wiring
    if not close(comp["vsh_vacation"] * 3, t["vsh"]):
        fails.append("vsh thirds do not reassemble")
    if not close(comp["employer_taxes"], c["fica"] + c["sui"] + c["fui"] + c["mta"]):
        fails.append("employer_taxes != FICA+SUI+FUI+MTA")

    # FA override behavior: overriding welfare cascades into union + labor
    r2 = compute_payroll(POSITIONS, ASSUMPTIONS, overrides={"welfare": 100000})
    if not close(r2["components"]["welfare"], 100000):
        fails.append("welfare override not honored")
    if not close(r2["totals"]["total_union"],
                 100000 + c["pension"] + c["supp_retirement"] + c["legal"] + c["training"] + c["profit_sharing"]):
        fails.append("union total does not cascade the welfare override")

    # roster_gl_values: mapping + formula-skip + empty-roster guard
    gl_lines = [
        {"gl_code": "5105-0000", "current_budget": 759369.0, "proposed_budget": 0.0},
        {"gl_code": "5105-0010", "current_budget": 46686.0, "proposed_budget": 0.0},
        {"gl_code": "5145-0000", "current_budget": 60000.0, "proposed_budget": 0.0,
         "proposed_formula": "=60000*1.03"},   # manual override -> skipped
        {"gl_code": "5199-9999", "current_budget": 1.0},  # unmapped -> skipped
    ]
    pushes = roster_gl_values(r["components"], gl_lines)
    by_gl = {p["gl_code"]: p for p in pushes}
    if "5105-0000" not in by_gl or by_gl["5105-0000"]["proposed_budget"] != round(t["annual_base"]):
        fails.append("gross payroll GL push wrong: %r" % by_gl.get("5105-0000"))
    if "5145-0000" in by_gl:
        fails.append("formula-overridden line was pushed (must skip)")
    if "5199-9999" in by_gl:
        fails.append("unmapped GL was pushed")
    if roster_gl_values({}, gl_lines):
        fails.append("empty components pushed values (437 wipe-guard mirror)")

    if fails:
        sys.stderr.write("PAYROLL-ENGINE GATE FAILED — union math drifted from hand-computed vectors:\n")
        for f in fails:
            sys.stderr.write("  " + f + "\n")
        sys.stderr.write("If the JS math changed on purpose, update payroll_engine.py AND these vectors together.\n")
        sys.exit(1)
    print("payroll_engine OK: %d hand-computed checks + override cascade + GL-push guards." % len(checks))


if __name__ == "__main__":
    main()
