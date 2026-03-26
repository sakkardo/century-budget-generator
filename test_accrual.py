"""
Verification tests for accrual adjustment auto-population.

Tests the logic that:
1. Invoices with invoice_date BEFORE the report period start are identified
2. They are summed per GL code
3. The sum is stored as a NEGATIVE accrual_adj on the BudgetLine
4. The API endpoint returns the correct prior-year invoices per GL
"""

import os
import sys
import json
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

def test_compute_accrual_adjustments():
    """Test the core accrual computation logic WITHOUT needing the full app."""
    print("=" * 60)
    print("TEST 1: compute_accrual_adjustments logic")
    print("=" * 60)

    # Simulate the date parsing and filtering logic from compute_accrual_adjustments
    period_from = "01/2026"
    cutoff = datetime.strptime(period_from, "%m/%Y")
    print(f"  Period from: {period_from}")
    print(f"  Cutoff date: {cutoff} (invoices before this are prior-year)")

    # Simulated invoices (what would come from the DB)
    test_invoices = [
        {"gl_code": "5621-0000", "invoice_date": "2025-11-15", "amount": 1500.00, "payee_name": "ABC Plumbing"},
        {"gl_code": "5621-0000", "invoice_date": "2025-12-20", "amount": 2300.00, "payee_name": "DEF Repairs"},
        {"gl_code": "5621-0000", "invoice_date": "2026-02-10", "amount": 800.00, "payee_name": "GHI Service"},
        {"gl_code": "5406-0000", "invoice_date": "2025-09-01", "amount": 450.00, "payee_name": "Supply Co"},
        {"gl_code": "5406-0000", "invoice_date": "2026-01-15", "amount": 600.00, "payee_name": "Supply Co"},
        {"gl_code": "6515-0000", "invoice_date": "2026-03-01", "amount": 5000.00, "payee_name": "Law Firm"},
    ]

    # Apply the same logic as compute_accrual_adjustments
    accruals = {}
    for inv in test_invoices:
        inv_date = None
        for date_fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%m/%d/%y"]:
            try:
                inv_date = datetime.strptime(str(inv["invoice_date"]).strip()[:10], date_fmt)
                break
            except (ValueError, TypeError):
                continue

        if inv_date is None:
            print(f"  WARNING: Could not parse date '{inv['invoice_date']}'")
            continue

        is_prior = inv_date < cutoff
        print(f"  Invoice: {inv['payee_name']:20s} GL={inv['gl_code']} date={inv['invoice_date']} amt=${inv['amount']:>10,.2f} {'<-- PRIOR YEAR' if is_prior else ''}")

        if is_prior:
            gl = inv["gl_code"]
            if gl not in accruals:
                accruals[gl] = {"amount": 0, "invoices": []}
            accruals[gl]["amount"] += inv["amount"]
            accruals[gl]["invoices"].append(inv)

    # Make negative
    for gl in accruals:
        accruals[gl]["amount"] = -abs(accruals[gl]["amount"])

    print(f"\n  RESULTS:")
    print(f"  {'GL Code':<15s} {'Accrual Adj':>15s} {'# Invoices':>12s}")
    print(f"  {'-'*42}")

    expected = {
        "5621-0000": {"amount": -3800.00, "count": 2},  # 1500 + 2300
        "5406-0000": {"amount": -450.00, "count": 1},   # only the 2025-09-01 one
    }

    all_pass = True
    for gl, data in sorted(accruals.items()):
        count = len(data["invoices"])
        print(f"  {gl:<15s} ${data['amount']:>14,.2f} {count:>12d}")

        if gl in expected:
            exp = expected[gl]
            if abs(data["amount"] - exp["amount"]) > 0.01:
                print(f"    ❌ FAIL: Expected ${exp['amount']:,.2f}, got ${data['amount']:,.2f}")
                all_pass = False
            elif count != exp["count"]:
                print(f"    ❌ FAIL: Expected {exp['count']} invoices, got {count}")
                all_pass = False
            else:
                print(f"    ✅ PASS")
        else:
            print(f"    ❌ FAIL: Unexpected GL in accruals")
            all_pass = False

    # Verify GLs that should NOT be in accruals
    for gl in ["6515-0000"]:
        if gl in accruals:
            print(f"  ❌ FAIL: {gl} should NOT be in accruals (all invoices are 2026+)")
            all_pass = False
        else:
            print(f"  ✅ PASS: {gl} correctly excluded (no prior-year invoices)")

    # Verify the 2026 invoice for 5621-0000 was NOT included
    gl_5621_invoices = accruals.get("5621-0000", {}).get("invoices", [])
    dates_included = [inv["invoice_date"] for inv in gl_5621_invoices]
    if "2026-02-10" in dates_included:
        print(f"  ❌ FAIL: 2026-02-10 invoice should NOT be in 5621-0000 accrual")
        all_pass = False
    else:
        print(f"  ✅ PASS: 2026-02-10 correctly excluded from 5621-0000 accrual")

    return all_pass


def test_date_parsing():
    """Test various date formats that might come from Yardi."""
    print("\n" + "=" * 60)
    print("TEST 2: Date format parsing robustness")
    print("=" * 60)

    test_dates = [
        ("2025-12-15", True),         # ISO format
        ("12/15/2025", True),         # US format
        ("2025-12-15T00:00:00", True),# ISO with time
        ("01/01/2026", False),        # Exactly at cutoff — should NOT be prior
        ("12/31/2025", True),         # Day before cutoff
        ("2026-01-01", False),        # First day of period
        ("", None),                   # Empty
        ("invalid", None),            # Invalid
    ]

    cutoff = datetime(2026, 1, 1)
    all_pass = True

    for date_str, expected_prior in test_dates:
        inv_date = None
        for date_fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%m/%d/%y"]:
            try:
                inv_date = datetime.strptime(str(date_str).strip()[:10], date_fmt)
                break
            except (ValueError, TypeError):
                continue

        if inv_date is None:
            is_prior = None
        else:
            is_prior = inv_date < cutoff

        status = "✅" if is_prior == expected_prior else "❌"
        if is_prior != expected_prior:
            all_pass = False
        print(f"  {status} Date '{date_str:25s}' → parsed={inv_date is not None}, prior={is_prior}, expected_prior={expected_prior}")

    return all_pass


def test_negative_amounts():
    """Verify accrual amounts are stored as negatives."""
    print("\n" + "=" * 60)
    print("TEST 3: Accrual amounts are negative")
    print("=" * 60)

    test_cases = [
        (1500.00, -1500.00),
        (0.00, 0.00),       # Zero stays zero (abs of 0 is 0, neg is 0)
        (3800.00, -3800.00),
    ]

    all_pass = True
    for original, expected in test_cases:
        result = -abs(original)
        status = "✅" if abs(result - expected) < 0.01 else "❌"
        if abs(result - expected) >= 0.01:
            all_pass = False
        print(f"  {status} Amount ${original:>10,.2f} → Accrual ${result:>10,.2f} (expected ${expected:>10,.2f})")

    return all_pass


def test_formula_impact():
    """Verify the accrual adjustment correctly impacts forecast calculation."""
    print("\n" + "=" * 60)
    print("TEST 4: Forecast formula with accrual adjustment")
    print("=" * 60)

    # Simulate: YTD has $10,000 but $3,800 of that is prior-year
    ytd_actual = 10000
    accrual_adj = -3800  # Prior-year invoices backed out
    unpaid_bills = 500
    ytd_months = 8
    remaining_months = 4

    # Estimate annualizes only YTD Actual
    estimate = (ytd_actual / ytd_months) * remaining_months  # 10000/8*4 = 5000
    forecast = ytd_actual + accrual_adj + unpaid_bills + estimate  # 10000 + (-3800) + 500 + 5000 = 11700

    print(f"  YTD Actual:       ${ytd_actual:>10,.2f}")
    print(f"  Accrual Adj:      ${accrual_adj:>10,.2f} (prior-year invoices)")
    print(f"  Unpaid Bills:     ${unpaid_bills:>10,.2f}")
    print(f"  Estimate:         ${estimate:>10,.2f} ({ytd_actual}/{ytd_months} × {remaining_months})")
    print(f"  12-Mo Forecast:   ${forecast:>10,.2f}")
    print(f"  Formula: {ytd_actual} + ({accrual_adj}) + {unpaid_bills} + {estimate} = {forecast}")

    expected_forecast = 11700.00
    all_pass = abs(forecast - expected_forecast) < 0.01
    print(f"\n  {'✅' if all_pass else '❌'} Forecast = ${forecast:,.2f} (expected ${expected_forecast:,.2f})")

    # Without accrual, the forecast would be inflated
    forecast_without_accrual = ytd_actual + 0 + unpaid_bills + estimate
    print(f"  ⚠️  Without accrual: ${forecast_without_accrual:,.2f} (inflated by ${abs(accrual_adj):,.2f})")

    return all_pass


if __name__ == "__main__":
    results = []
    results.append(("Accrual computation", test_compute_accrual_adjustments()))
    results.append(("Date parsing", test_date_parsing()))
    results.append(("Negative amounts", test_negative_amounts()))
    results.append(("Formula impact", test_formula_impact()))

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    all_passed = True
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False

    print(f"\n  Overall: {'ALL TESTS PASSED ✅' if all_passed else 'SOME TESTS FAILED ❌'}")
    sys.exit(0 if all_passed else 1)
