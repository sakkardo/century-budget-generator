"""Smoke tests for the Century budget app API.

FA directive 2026-05-10: a small set of shape-only assertions on the
key endpoints. Catches accidental schema breakage from the next 50
commits without aiming for high coverage. Run via:

    python -m pytest budget_app/test_api_smoke.py -v

Or hit production directly without pytest:

    python budget_app/test_api_smoke.py

The latter mode hits the live Railway deploy with read-only GETs against
seed buildings (168, 148). Safe to run against prod — no writes.
"""
import json
import sys
import urllib.request
import urllib.error
from typing import Any


BASE_URL = "https://century-budget-generator-production.up.railway.app"
TEST_ENTITY = "168"   # has full data: audit confirmed, period set, RE Tax DOF
TEST_ENTITY_2 = "148"  # has no re_taxes_overrides — clean DOF fall-through path


def _http_get(path: str) -> tuple[int, dict]:
    """GET <path>, return (status_code, json_body). Returns (-1, {}) on error."""
    url = BASE_URL + path
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8")
            return e.code, json.loads(body) if body else {}
        except Exception:
            return e.code, {}
    except Exception as e:
        print(f"  HTTP error on {path}: {e}", file=sys.stderr)
        return -1, {}


# ──────────── Smoke checks ────────────

def check_buildings_index():
    """GET /api/buildings — should return a list of buildings with entity_code."""
    code, data = _http_get("/api/buildings")
    assert code == 200, f"/api/buildings returned {code}"
    assert isinstance(data, list), "expected list"
    assert len(data) >= 1, "expected at least one building"
    assert all("entity_code" in b for b in data), "every building must have entity_code"
    return f"{len(data)} buildings"


def check_dashboard():
    """GET /api/dashboard/<entity> — should return budget + lines + sheets +
    audit_summary."""
    code, data = _http_get(f"/api/dashboard/{TEST_ENTITY}")
    assert code == 200, f"/api/dashboard/{TEST_ENTITY} returned {code}"
    for key in ("budget", "lines", "sheets", "sheet_order", "ytd_months",
                "remaining_months", "audit_summary", "assignments"):
        assert key in data, f"dashboard missing key: {key}"
    assert isinstance(data["lines"], list), "lines must be list"
    assert data["budget"].get("entity_code") == TEST_ENTITY
    return f"{len(data['lines'])} lines, {len(data['sheet_order'])} sheets"


def check_readiness():
    """GET /api/readiness/<entity> — should return summary + 8 gates."""
    code, data = _http_get(f"/api/readiness/{TEST_ENTITY}")
    assert code == 200, f"/api/readiness/{TEST_ENTITY} returned {code}"
    assert "summary" in data and "gates" in data
    summary = data["summary"]
    for key in ("ok", "warn", "fail", "total", "ready"):
        assert key in summary, f"summary missing key: {key}"
    gates = data["gates"]
    assert isinstance(gates, list) and len(gates) == 8, \
        f"expected 8 gates, got {len(gates)}"
    for g in gates:
        for key in ("key", "label", "status", "detail"):
            assert key in g, f"gate missing {key}: {g}"
        assert g["status"] in ("ok", "warn", "fail", "skip")
    return f"{summary['ok']}/{summary['total']} gates ok"


def check_summary():
    """GET /api/summary/<entity> — should return rows + warnings."""
    code, data = _http_get(f"/api/summary/{TEST_ENTITY}")
    assert code == 200, f"/api/summary/{TEST_ENTITY} returned {code}"
    assert "rows" in data
    assert isinstance(data["rows"], list)
    # warnings is optional, but if present must be a list
    if "warnings" in data:
        assert isinstance(data["warnings"], list)
    return f"{len(data['rows'])} summary rows"


def check_re_taxes_clean_path():
    """GET /api/re-taxes/<148> — clean path with no overrides, must populate
    AV from live DOF. Regression catcher for the zero-overrides bug class."""
    code, data = _http_get(f"/api/re-taxes/{TEST_ENTITY_2}")
    assert code == 200, f"/api/re-taxes/{TEST_ENTITY_2} returned {code}"
    re = data.get("re_taxes", {})
    # 148 has no overrides — backend must compute non-zero AV from live DOF.
    assert re.get("assessed_value", 0) > 0, \
        f"148 assessed_value should be >0, got {re.get('assessed_value')}"
    assert re.get("tax_rate", 0) > 0, \
        f"148 tax_rate should be >0, got {re.get('tax_rate')}"
    assert re.get("gross_tax", 0) > 0, \
        f"148 gross_tax should be >0, got {re.get('gross_tax')}"
    return f"AV ${re['assessed_value']:,.0f}, gross_tax ${re['gross_tax']:,.0f}"


def check_whoami_no_cookie():
    """GET /api/whoami no cookie — should return user_id: null."""
    code, data = _http_get("/api/whoami")
    assert code == 200
    assert data.get("user_id") is None, \
        f"expected user_id null with no cookie, got {data.get('user_id')}"
    return "anonymous OK"


def check_diff_no_identity():
    """GET /api/diff/<entity> no cookie — should return show:false, no_identity."""
    code, data = _http_get(f"/api/diff/{TEST_ENTITY}")
    assert code == 200
    assert data.get("show") is False
    assert data.get("reason") == "no_identity"
    return "no_identity OK"


def check_active_fa_filter():
    """GET /api/users?role=fa&active=1 — should return ~6 active FAs, no
    comma-named pseudo-users."""
    code, data = _http_get("/api/users?role=fa&active=1")
    assert code == 200
    assert isinstance(data, list)
    assert len(data) > 0, "expected at least one active FA"
    assert len(data) < 15, f"expected <15 (real FAs), got {len(data)} — joint-name filter regressed?"
    for u in data:
        name = u.get("name", "")
        assert "," not in name, f"comma-named pseudo-user leaked: {name!r}"
    return f"{len(data)} active FAs, no joint-name pseudo-users"


CHECKS = [
    ("buildings_index",         check_buildings_index),
    ("dashboard_168",           check_dashboard),
    ("readiness_168",           check_readiness),
    ("summary_168",             check_summary),
    ("re_taxes_148_clean_path", check_re_taxes_clean_path),
    ("whoami_no_cookie",        check_whoami_no_cookie),
    ("diff_no_identity",        check_diff_no_identity),
    ("active_fa_filter",        check_active_fa_filter),
]


def run_all():
    """Run every check; return (pass_count, fail_count, fail_details)."""
    passed, failed, fail_details = 0, 0, []
    for name, fn in CHECKS:
        try:
            detail = fn()
            print(f"  [OK]{name:32s}  {detail or ''}")
            passed += 1
        except AssertionError as e:
            print(f"  [FAIL]{name:32s}  FAIL: {e}")
            failed += 1
            fail_details.append((name, str(e)))
        except Exception as e:
            print(f"  [FAIL]{name:32s}  ERROR: {e}")
            failed += 1
            fail_details.append((name, f"unexpected {type(e).__name__}: {e}"))
    return passed, failed, fail_details


if __name__ == "__main__":
    print(f"=== Century budget API smoke tests ===")
    print(f"Target: {BASE_URL}")
    print()
    passed, failed, _ = run_all()
    print()
    print(f"Result: {passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


# ──────────── pytest integration ────────────
# When run via pytest, expose each check as a separate test function.

def _make_pytest_function(check_fn):
    def _test():
        check_fn()
    _test.__name__ = f"test_{check_fn.__name__.replace('check_', '')}"
    _test.__doc__ = check_fn.__doc__
    return _test

for _name, _fn in CHECKS:
    globals()[f"test_{_name}"] = _make_pytest_function(_fn)
