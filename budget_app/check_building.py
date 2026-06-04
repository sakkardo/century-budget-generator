"""Per-building integrity preflight for the Century budget app.

FA dir 2026-06-04: catches the recurring "every new building has gaps /
commits that didn't go through" class. Read-only — hits the live API with
GETs only, never writes. Run BEFORE an FA works a building (or in bulk) so
data gaps + broken invariants surface up front instead of mid-budget.

    python budget_app/check_building.py 500        # one building
    python budget_app/check_building.py --all      # every built / in-PM building

Severity:
  FAIL = a locked invariant is violated (forecast != budget; capital has a
         proposed budget) — the budget is wrong.
  WARN = a data gap that makes the budget incomplete (operating GLs not
         mapped / dropped; a row with YTD actuals but no 2026 budget).
  OK   = clean.

Exit code 1 if ANY building has a FAIL (so it can gate a script); else 0.
"""
import json
import sys
import urllib.request
import urllib.error

BASE_URL = "https://century-budget-generator-production.up.railway.app"
MATERIAL = 500.0                       # ignore sub-$500 rounding stragglers
FIXED_INCOME_BASES = {"4010", "4020", "4030", "4040"}   # fully-collectible income that pins to budget
OPERATING_FIRST_DIGITS = ("4", "5", "6", "7")           # P&L only; 0-3 are balance-sheet


def _get(path):
    url = BASE_URL + path
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                   "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body else {})
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception:
        return -1, {}


def _bases(row):
    return {str(p).split("-")[0].strip() for p in (row.get("gl_prefixes") or [])}


def _overridden(row, col):
    ov = (row.get("overrides") or {}).get(col) or {}
    return bool(ov.get("is_overridden"))


def check_building(ec):
    """Return {entity, status, issues:[(severity, msg)]}. Read-only."""
    issues = []
    code, summ = _get(f"/api/summary/{ec}")
    if code != 200 or not isinstance(summ, dict):
        return {"entity": ec, "status": "ERR", "issues": [("ERR", f"/api/summary -> HTTP {code}")]}
    rows = [r for r in (summ.get("rows") or []) if r.get("row_type") == "data"]

    # 1. Fully-collectible income must pin forecast (col5) to budget (col6).
    for r in rows:
        if not (_bases(r) & FIXED_INCOME_BASES):
            continue
        c5, c6 = r.get("col5"), r.get("col6")
        if isinstance(c6, (int, float)) and c6 != 0 and not _overridden(r, "col5"):
            if not isinstance(c5, (int, float)) or abs(c5 - c6) > 1:
                issues.append(("FAIL", f"{r.get('label')!r}: forecast {c5} != budget {c6} (income pin)"))

    # 2. Capital rows must have no proposed budget.
    for r in rows:
        if any(str(p).split("-")[0].strip().startswith("7") for p in (r.get("gl_prefixes") or [])):
            c7 = r.get("col7")
            if isinstance(c7, (int, float)) and abs(c7) > 0.01:
                issues.append(("FAIL", f"{r.get('label')!r}: capital has proposed {c7} (should be 0)"))

    # 3. Operating GLs with data not mapped into the Summary (dropped budget).
    code2, dbg = _get(f"/api/admin/summary-debug/{ec}")
    if code2 == 200 and isinstance(dbg, dict):
        orph = dbg.get("orphan_gl_codes") or []
        op = [o for o in orph
              if (o.get("gl_code", "")[:1] in OPERATING_FIRST_DIGITS)
              and (abs(o.get("ytd_actual", 0)) >= MATERIAL or abs(o.get("current_budget", 0)) >= MATERIAL)]
        if op:
            tot = round(sum(o.get("ytd_actual", 0) for o in op))
            sample = ", ".join(o["gl_code"] for o in op[:5])
            issues.append(("WARN", f"{len(op)} operating GL(s) not mapped (~${abs(tot):,} ytd dropped): {sample}"))

    # 4. Rows with material YTD actuals but no 2026 budget (after col6 fallback).
    for r in rows:
        is_operating = any(b and b[0] in OPERATING_FIRST_DIGITS for b in _bases(r))
        if not is_operating:
            continue
        c6, c3 = r.get("col6"), r.get("col3")
        if c6 is None and isinstance(c3, (int, float)) and abs(c3) >= MATERIAL:
            issues.append(("WARN", f"{r.get('label')!r}: YTD ${abs(round(c3)):,} but no 2026 budget"))

    fails = [i for i in issues if i[0] == "FAIL"]
    warns = [i for i in issues if i[0] == "WARN"]
    status = "FAIL" if fails else ("WARN" if warns else "OK")
    return {"entity": ec, "status": status, "issues": issues,
            "n_fail": len(fails), "n_warn": len(warns)}


def _entities_to_scan():
    code, b = _get("/api/budgets")
    bl = b if isinstance(b, list) else (b.get("budgets") or b.get("rows") or [])
    out = []
    for x in bl:
        if not isinstance(x, dict):
            continue
        st = str(x.get("status") or "")
        # built / in-flight buildings — where correctness actually matters now
        if st in ("draft", "pm_pending", "pm_in_progress", "fa_review", "exec_review",
                  "presentation", "approved", "returned"):
            out.append(str(x.get("entity_code")))
        elif isinstance(x.get("readiness"), dict) and x["readiness"].get("tier") == "BUILT":
            out.append(str(x.get("entity_code")))
    return sorted(set(out))


def _print(rep):
    icon = {"OK": "[OK]  ", "WARN": "[WARN]", "FAIL": "[FAIL]", "ERR": "[ERR] "}[rep["status"]]
    print(f"{icon} {rep['entity']}: {rep['status']}"
          + (f" ({rep.get('n_fail',0)} fail, {rep.get('n_warn',0)} warn)" if rep["issues"] else ""))
    for sev, msg in rep["issues"]:
        print(f"        {sev}: {msg}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("usage: python budget_app/check_building.py <entity_code> | --all")
        sys.exit(2)
    if args[0] == "--all":
        ents = _entities_to_scan()
        print(f"=== per-building integrity preflight: {len(ents)} built/in-flight buildings ===\n")
        any_fail = False
        bad = []
        for ec in ents:
            rep = check_building(ec)
            if rep["status"] in ("FAIL", "WARN", "ERR"):
                _print(rep)
                bad.append(rep)
            if rep["status"] == "FAIL":
                any_fail = True
        clean = len(ents) - len(bad)
        print(f"\n{clean} clean, {sum(1 for r in bad if r['status']=='FAIL')} FAIL, "
              f"{sum(1 for r in bad if r['status']=='WARN')} WARN, "
              f"{sum(1 for r in bad if r['status']=='ERR')} ERR")
        sys.exit(1 if any_fail else 0)
    else:
        rep = check_building(args[0])
        _print(rep)
        sys.exit(1 if rep["status"] == "FAIL" else 0)
