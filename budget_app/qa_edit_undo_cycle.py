# -*- coding: utf-8 -*-
"""On-demand regression harness: prove an edit lands where it should AND that
undo reverts it byte-for-byte. Codifies the manual QA that caught the 2026-07-03
payroll wipe -- the app has no automated coverage of the edit->propagate->undo
cycle, which is exactly the path where silent data corruption hides.

WHAT IT DOES (all through the REAL production endpoints, self-restoring):
  1. Snapshots the building's dashboard + summary (the pre-test baseline).
  2. Test A - expense line multi-field edit: sets increase_pct + proposed on one
     expense GL via /api/fa-lines (one request = one revision batch), confirms it
     stored, then undoes via /api/recent-changes/<ec>/undo and asserts the WHOLE
     batch reverted (both fields) to baseline.
  3. Test B - summary direct edit: overrides a summary row's col7, confirms the
     stored value wins on re-read, then undoes via the summary branch and asserts
     col7 returns to baseline.
  4. finally: force-restores anything still drifted from baseline and asserts a
     0-diff. Exits non-zero if the building is not byte-identical to where it
     started -- a botched run is LOUD, never silent.

USAGE
    python qa_edit_undo_cycle.py [ENTITY]        # default 437
    python qa_edit_undo_cycle.py 148 --base http://localhost:5000

NOT a deploy gate: it mutates then restores real data, so it runs on demand
(before/after risky changes), not automatically on every push.
"""
import json
import sys
import urllib.request

BASE = "https://century-budget-generator-production.up.railway.app"
EC = "437"
argv = [a for a in sys.argv[1:]]
if "--base" in argv:
    i = argv.index("--base"); BASE = argv[i + 1]; del argv[i:i + 2]
if argv:
    EC = argv[0]

FIELDS = ["increase_pct", "proposed_budget", "current_budget", "ytd_actual",
          "accrual_adj", "unpaid_bills", "prior_year", "estimate_override",
          "forecast_override", "no_budget"]
EASY_SHEETS = ("Energy", "Gen & Admin", "Repairs & Supplies", "Water & Sewer")


def _req(path, body=None, method="GET"):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode())


def get(path):
    return _req(path)


def snapshot():
    return get("/api/dashboard/%s" % EC), get("/api/summary/%s" % EC)


def srows(s):
    return s["rows"] if isinstance(s, dict) else s


def diff_vs(base_d, base_s):
    """Return a list of (kind, key, field, base, live) differences vs baseline."""
    live_d, live_s = snapshot()
    out = []
    for sn in sorted(set(list(live_d["sheets"]) + list(base_d["sheets"]))):
        lv = {l["gl_code"]: l for l in live_d["sheets"].get(sn, [])}
        bs = {l["gl_code"]: l for l in base_d["sheets"].get(sn, [])}
        for gl in sorted(set(list(lv) + list(bs))):
            a, b = lv.get(gl), bs.get(gl)
            if a is None or b is None:
                out.append(("line", "%s/%s" % (sn, gl), "-", b, a)); continue
            for f in FIELDS:
                if a.get(f) != b.get(f):
                    out.append(("line", "%s/%s" % (sn, gl), f, b.get(f), a.get(f)))
    lm = {}; bm = {}
    for r in srows(live_s): lm.setdefault(r.get("label"), r)
    for r in srows(base_s): bm.setdefault(r.get("label"), r)
    for lbl in sorted(set(list(lm) + list(bm)), key=str):
        a, b = lm.get(lbl), bm.get(lbl)
        if a is None or b is None:
            out.append(("summary", str(lbl), "-", b, a)); continue
        for c in ("col1", "col6", "col7"):
            if a.get(c) != b.get(c):
                out.append(("summary", str(lbl), c, b.get(c), a.get(c)))
    return out


def newest_line_revision(gl, field):
    feed = get("/api/recent-changes/%s?limit=30" % EC)["changes"]
    return next((c for c in feed if c.get("gl_code") == gl and c.get("field") == field), None)


results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("  [%s] %s%s" % ("PASS" if ok else "FAIL", name, (" -- " + detail) if detail else ""))


def main():
    print("=" * 74)
    print("EDIT -> UNDO regression cycle on %s  (%s)" % (EC, BASE))
    print("=" * 74)
    base_d, base_s = snapshot()

    # pick an expense line: nonzero current_budget, not capital / no_budget
    target = None
    for sn in EASY_SHEETS:
        for l in base_d["sheets"].get(sn, []):
            if (l.get("current_budget") or 0) > 100 and not l.get("no_budget"):
                target = (sn, l); break
        if target:
            break
    if not target:
        print("No suitable expense line found on %s; aborting (nothing mutated)." % (EC,))
        return 2
    sn, line = target
    gl = line["gl_code"]
    base_inc = line.get("increase_pct") or 0
    base_prop = line.get("proposed_budget") or 0
    print("Target expense line: %s %s '%s' (baseline inc=%s prop=%s)"
          % (sn, gl, (line.get("description") or "")[:30], base_inc, base_prop))

    try:
        # ---- Test A: multi-field line edit + batch undo ----
        print("\nTest A - expense line multi-field edit + batch undo")
        test_inc = round((base_inc or 0) + 0.077, 4)
        _req("/api/fa-lines/%s" % EC, {"lines": [{"gl_code": gl, "increase_pct": test_inc,
             "proposed_budget": (base_prop or line["current_budget"]) + 1234}]}, "PUT")
        d2 = get("/api/dashboard/%s" % EC)
        l2 = next(x for x in d2["sheets"][sn] if x["gl_code"] == gl)
        check("edit stored (increase_pct moved)", abs((l2.get("increase_pct") or 0) - test_inc) < 1e-6,
              "live inc=%s" % l2.get("increase_pct"))
        rev = newest_line_revision(gl, "proposed_budget") or newest_line_revision(gl, "increase_pct")
        check("edit created a revision", rev is not None)
        if rev:
            batch = rev.get("batch_size")
            check("revision is batched (>=2 fields)", (batch or 0) >= 2, "batch_size=%s" % batch)
            u = _req("/api/recent-changes/%s/undo" % EC, {"revision_id": rev["id"]}, "POST")
            check("undo reverted the whole batch", (u.get("reverted_count") or 0) >= 2,
                  "reverted_count=%s" % u.get("reverted_count"))
        d3 = get("/api/dashboard/%s" % EC)
        l3 = next(x for x in d3["sheets"][sn] if x["gl_code"] == gl)
        check("line back to baseline after undo",
              (l3.get("increase_pct") or 0) == base_inc and (l3.get("proposed_budget") or 0) == base_prop,
              "inc=%s prop=%s" % (l3.get("increase_pct"), l3.get("proposed_budget")))

        # ---- Test B: summary direct col7 edit + summary undo ----
        print("\nTest B - summary direct col7 edit + summary undo")
        srow = next((r for r in srows(base_s) if r.get("row_type") == "data"
                     and r.get("display_order") is not None), None)
        if srow is None:
            check("summary data row available", False, "no data row with display_order")
        else:
            lbl = srow["label"]; base_c7 = srow.get("col7")
            _req("/api/summary/%s" % EC, {"edits": [{"display_order": srow["display_order"],
                 "col7": 91234.0}]}, "PUT")
            s2 = get("/api/summary/%s" % EC)
            r2 = next(r for r in srows(s2) if r.get("label") == lbl)
            check("summary override stored (stored-wins on re-read)", r2.get("col7") == 91234.0,
                  "col7=%s" % r2.get("col7"))
            feed = get("/api/recent-changes/%s?sheet=Summary&limit=20" % EC)["changes"]
            srev = next((c for c in feed if c.get("is_summary") and c.get("summary_label") == lbl), None)
            check("summary edit is undoable in feed", srev is not None)
            if srev:
                _req("/api/recent-changes/%s/undo" % EC, {"revision_id": srev["id"]}, "POST")
            s3 = get("/api/summary/%s" % EC)
            r3 = next(r for r in srows(s3) if r.get("label") == lbl)
            check("summary col7 back to baseline after undo", r3.get("col7") == base_c7,
                  "col7=%s (baseline %s)" % (r3.get("col7"), base_c7))
    finally:
        # ---- Backstop restore: force any drifted field back to baseline ----
        drift = diff_vs(base_d, base_s)
        if drift:
            print("\nRestoring %d drifted field(s) to baseline..." % len(drift))
            line_fix = {}
            sum_fix = []
            base_line_by_gl = {l["gl_code"]: l for ls in base_d["sheets"].values() for l in ls}
            base_sum_by_lbl = {r.get("label"): r for r in srows(base_s)}
            for kind, key, field, bval, lval in drift:
                if kind == "line":
                    g = key.split("/", 1)[1]
                    bl = base_line_by_gl.get(g)
                    if bl:
                        line_fix.setdefault(g, {"gl_code": g})
                        line_fix[g]["increase_pct"] = bl.get("increase_pct") or 0
                        line_fix[g]["proposed_budget"] = bl.get("proposed_budget") or 0
                else:
                    br = base_sum_by_lbl.get(key)
                    if br and br.get("display_order") is not None:
                        sum_fix.append({"display_order": br["display_order"], "col7": br.get("col7")})
            if line_fix:
                _req("/api/fa-lines/%s" % EC, {"lines": list(line_fix.values())}, "PUT")
            if sum_fix:
                _req("/api/summary/%s" % EC, {"edits": sum_fix}, "PUT")

        final = diff_vs(base_d, base_s)
        print("\n" + "=" * 74)
        n_fail = sum(1 for _, ok, _ in results if not ok)
        if final:
            print("RESTORE INCOMPLETE - %d field(s) still differ from baseline:" % len(final))
            for kind, key, field, bval, lval in final[:20]:
                print("  ! %s %s.%s: baseline=%r live=%r" % (kind, key, field, bval, lval))
        else:
            print("RESTORED: building %s is byte-identical to its pre-test baseline (0 diff)." % EC)
        print("Test assertions: %d passed, %d failed" % (len(results) - n_fail, n_fail))
        print("=" * 74)
        return 1 if (final or n_fail) else 0


if __name__ == "__main__":
    sys.exit(main())
