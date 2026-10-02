"""Regression test for the statement parser on real statements.
Run: python budget_app/test_snapshot_parser.py

Every sample must tie all checks against itself. Totals are compared with the figures on the
statement (or the vendor's snapshot), not with whatever the parser produced last time.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fitz

import snapshot_parser as p

SAMPLES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tasks", "snapshot_samples")

# figures read off the statements by hand (YTD), in whole dollars
EXPECTED = {
    "204_2026-08": {"income": 6882295, "expenses": 6832441, "noi": 49853, "net": -157018, "cash_end": 3997475},
    "302_2026-08": {"income": 1059953, "expenses": 989208, "noi": 70744, "net": 120897, "cash_end": 740075},
    "206_2026-05": {"cash_end": 7512030, "accounts": 11},
    "206_2026-08": {"cash_end": 6708639, "accounts": 11},
}


def run():
    for name, exp in EXPECTED.items():
        path = os.path.join(SAMPLES, name + "_statement.pdf")
        if not os.path.exists(path):
            print("skip (sample not present):", name)
            continue
        s = p.build_snapshot(open(path, "rb").read())
        bad = [c["label"] for c in s["checks"] if c["status"] != "tied"]
        assert not bad, (name, bad)
        if "income" in exp:
            assert s["income"]["ytd_actual"] == exp["income"], name
            assert s["expenses"]["ytd_actual"] == exp["expenses"], name
            assert s["noi"]["ytd_actual"] == exp["noi"], name
            assert s["net_income"]["ytd_actual"] == exp["net"], name
        assert s["cash"]["total"]["end"] == exp["cash_end"], name
        if "accounts" in exp:
            assert len(s["cash"]["accounts"]) == exp["accounts"], (name, [a["name"] for a in s["cash"]["accounts"]])
        # the totals row is never an account
        assert not any(a["name"].upper().startswith("CASH ACCOUNT TOTALS") for a in s["cash"]["accounts"]), name
        # every income-statement line that carries figures is read
        doc = fitz.open(path)
        for pg in doc:
            if p._title(pg).startswith("Income Statement Detail"):
                for line in p._lines(pg):
                    if len(p._NUM_RE.findall(line)) >= 6 and not re.match(r"^\d+ of \d+$", line):
                        assert p._split_row(line), (name, line)
    # 206: an account whose name ends in a number keeps it, and its balance is counted
    s = p.build_snapshot(open(os.path.join(SAMPLES, "206_2026-05_statement.pdf"), "rb").read())
    acct = {a["name"]: a for a in s["cash"]["accounts"]}
    assert acct["Principal - Owners Reserve 2"]["end"] == 43949, sorted(acct)
    print("snapshot parser: all tests passed")


if __name__ == "__main__":
    run()
