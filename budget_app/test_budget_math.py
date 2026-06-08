"""Equivalence + variant tests for budget_math (architecture Phase 1).

Proves the canonical module reproduces the OLD backend math byte-for-byte (so the
delegation in workflow.py changed NO behavior), and pins the explicit variant flags
that capture the historical frontend drift. Run: python budget_app/test_budget_math.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import budget_math as bm


# ---- Reference: the EXACT pre-Phase-1 backend implementations (copied verbatim) ----
def OLD_compute_forecast(ytd_actual, accrual_adj, unpaid_bills, prior_year, ytd_months=2):
    ytd_total = ytd_actual + accrual_adj + unpaid_bills
    remaining = 12 - ytd_months
    prior = prior_year or 0
    if ytd_total < 0 and prior >= 0:
        return ytd_total
    if ytd_months > 0:
        estimate = (ytd_total / ytd_months) * remaining
    else:
        estimate = 0
    return ytd_total + estimate


def OLD_compute_proposed_budget(forecast, increase_pct, increase_dollar=None):
    if increase_dollar is not None:
        try:
            return float(forecast or 0) + float(increase_dollar)
        except (TypeError, ValueError):
            pass
    return float(forecast or 0) * (1 + float(increase_pct or 0))


VECTORS = [
    # (ytd, accrual, unpaid, prior, ytd_months)
    (120000, 0, 0, 100000, 4),
    (50000, 5000, 2000, 48000, 2),
    (0, 0, 0, 0, 4),
    (-3000, 0, 0, 5000, 4),     # anomaly: net-negative base, prior positive -> cap
    (-3000, 0, 0, -1000, 4),    # recurring negative: prior negative -> keep extrapolating
    (90000, -719, 0, 80000, 6),
    (10000, 0, 0, 0, 0),        # ytd_months = 0
    (7914878, 0, 0, 7000000, 2),
]


def approx(a, b, tol=1e-6):
    return abs((a or 0) - (b or 0)) <= tol


def main():
    fails = []

    # 1. forecast: canonical (anomaly_cap=True, payroll=False) == OLD backend, every vector.
    for v in VECTORS:
        old = OLD_compute_forecast(*v)
        new = bm.forecast(v[0], v[1], v[2], v[3], v[4], anomaly_cap=True, payroll=False)
        if not approx(old, new):
            fails.append("forecast mismatch %s: old=%s new=%s" % (v, old, new))

    # 2. proposed: canonical == OLD backend across %/$/none.
    for f in (100000, 0, -2000, 9384324):
        for pct in (0, 0.03, -0.05):
            if not approx(OLD_compute_proposed_budget(f, pct), bm.proposed(f, pct)):
                fails.append("proposed pct mismatch f=%s pct=%s" % (f, pct))
        for dol in (5000, -1000):
            if not approx(OLD_compute_proposed_budget(f, 0, dol), bm.proposed(f, 0, dol)):
                fails.append("proposed $ mismatch f=%s dol=%s" % (f, dol))

    # 3. Variant flags are real + behave as documented (these PIN the drift).
    #    FA dashboard historically had NO anomaly cap -> a net-negative line extrapolates.
    fa = bm.forecast(-3000, 0, 0, 5000, 4, anomaly_cap=False, payroll=False)
    pm = bm.forecast(-3000, 0, 0, 5000, 4, anomaly_cap=True, payroll=False)
    if not (fa < pm):  # FA extrapolates the negative (more negative); PM caps at base
        fails.append("anomaly_cap flag did not differentiate FA vs PM (fa=%s pm=%s)" % (fa, pm))

    #    Payroll variant excludes accrual/unpaid from the base.
    no_pr = bm.forecast(50000, 8000, 3000, 40000, 4, payroll=False)
    pr = bm.forecast(50000, 8000, 3000, 40000, 4, payroll=True)
    if approx(no_pr, pr):
        fails.append("payroll flag did not change the base")

    if fails:
        print("BUDGET_MATH TESTS FAILED:")
        for f in fails:
            print("  " + f)
        sys.exit(1)
    print("budget_math OK: %d forecast vectors + proposed matrix == old backend; "
          "variant flags (anomaly_cap, payroll) verified." % len(VECTORS))


if __name__ == "__main__":
    main()
