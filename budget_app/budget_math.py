"""Single source of truth for the core budget math: estimate / forecast / proposed.

Architecture Phase 1 (2026-06-08). This arithmetic was previously copy-pasted into
four embedded-JS variants (FA dashboard, PM portal, bp2 summary, + one more) AND the
Python backend. The copies had DRIFTED:
  - the FA dashboard's estimate lacked the FA-#7 anomaly cap that the PM portal +
    backend apply (so FA and PM computed different estimates for a net-negative line);
  - only the bp2 summary special-cased Payroll (no accrual/unpaid in the base).

This module is the one place the *core* formula lives. The per-view differences are
preserved EXACTLY as explicit flags (anomaly_cap, payroll) — no functionality change.
Those flags make the historical drift visible so it can be converged deliberately in a
later correctness pass (see ARCHITECTURE_REVIEW_2026-06-08.md §3.2).

Business rules that sit ON TOP of the core (estimate/forecast overrides, fixed-to-budget
income pinning, capital, one-time-fee) stay with each caller, which passes the resolved
result in. This module only owns the annualization arithmetic.
"""

MONTHS_IN_YEAR = 12


def _base(ytd, accrual, unpaid, payroll):
    if payroll:
        accrual = 0.0
        unpaid = 0.0
    return (ytd or 0.0) + (accrual or 0.0) + (unpaid or 0.0)


def estimate(ytd, accrual, unpaid, prior, ytd_months, *, anomaly_cap=True, payroll=False):
    """Remaining-months estimate = (YTD+Accrual+Unpaid) / ytd_months * remaining_months.

    anomaly_cap: FA #7 — a net-negative base with a non-negative prior year is a one-time
    refund/credit; do not extrapolate it (estimate = 0). Recurring negatives (prior also
    negative, e.g. tax credits) keep extrapolating.
    payroll: Payroll lines annualize on YTD only (accrual/unpaid excluded from the base).
    """
    base = _base(ytd, accrual, unpaid, payroll)
    if anomaly_cap and base < 0 and (prior or 0) >= 0:
        return 0.0
    if ytd_months and ytd_months > 0:
        return base / ytd_months * (MONTHS_IN_YEAR - ytd_months)
    return 0.0


def forecast(ytd, accrual, unpaid, prior, ytd_months, *, anomaly_cap=True, payroll=False):
    """12-month forecast = base + estimate. Under the anomaly cap, forecast = base
    (YTD only), matching the historical backend behavior exactly."""
    base = _base(ytd, accrual, unpaid, payroll)
    if anomaly_cap and base < 0 and (prior or 0) >= 0:
        return base
    return base + estimate(ytd, accrual, unpaid, prior, ytd_months,
                           anomaly_cap=anomaly_cap, payroll=payroll)


def proposed(forecast_val, increase_pct=0.0, increase_dollar=None):
    """Proposed budget. $ increase wins over % when both set; neither => forecast."""
    if increase_dollar is not None:
        try:
            return float(forecast_val or 0) + float(increase_dollar)
        except (TypeError, ValueError):
            pass
    return float(forecast_val or 0) * (1 + float(increase_pct or 0))
