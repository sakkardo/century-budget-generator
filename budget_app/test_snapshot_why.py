"""Suggested reasons ("why") from the GL. No network: a fake drafter stands in for Claude.
Run: python budget_app/test_snapshot_why.py
"""
import glob
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import snapshot_dev
import snapshot_render as r
import snapshot_why as w
from snapshot_parser import build_snapshot

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLES = os.path.join(HERE, "..", "tasks", "snapshot_samples")
AUGUST = os.path.join(HERE, "..", "tasks", "august_2026_statements")


class FakeDrafter:
    model = "fake"
    available = True

    def __init__(self, reply=None):
        self.reply, self.calls = reply, []

    def __call__(self, items):
        self.calls.append(items)
        if self.reply is not None:
            return w.parse_reply(self.reply, {i["key"] for i in items})
        return {i["key"]: "Because of %s." % i["evidence"]["accounts"][0]["account"] for i in items}


def run():
    # ---- the parser reads the ledger evidence on every statement layout we have
    files = sorted(glob.glob(os.path.join(SAMPLES, "*.pdf"))) + sorted(glob.glob(os.path.join(AUGUST, "*.pdf")))
    assert len(files) >= 10
    for f in files:
        s = build_snapshot(open(f, "rb").read())
        g = s["gl"]
        assert g and g["categories"], f
        assert g["tie"]["tied"] >= 0.9 * g["tie"]["checked"], (f, g["tie"])        # ledger totals tie to the income statement
        for n in r.draft_commentary(s):
            if n["key"].startswith("cat:"):
                assert w.note_evidence(s, n), (f, n["key"])                          # every flagged category has evidence
        names = json.dumps(g)
        assert "Security Deposits Payable" not in names and "Maintenance Receivable" not in names  # expense accounts only

    s204 = build_snapshot(open(os.path.join(SAMPLES, "204_2026-08_statement.pdf"), "rb").read())
    util = {a["name"]: a for a in s204["gl"]["categories"]["Utility Expenses"]}
    gas = util["Gas - Heating"]
    assert gas["acct"] == "5252-0000" and gas["months"] == [40982, 58770, 70589, 40544, 26603, 27739, 89232, 90400]
    assert gas["month_complete"] and [l["vendor"] for l in gas["lines"]] == ["Con Edison", "NRG Business Marketing"]
    assert gas["lines"][0]["amount"] == 65354.0
    notes = r.draft_commentary(s204)
    ev = w.note_evidence(s204, [n for n in notes if n["key"] == "cat:Utility Expenses"][0])
    assert [a["account"] for a in ev["accounts"]] == ["Gas - Heating", "Water/Sewer", "Fuel", "Electricity"]  # worst first
    assert ev["accounts"][0]["actual_by_month"]["Aug"] == 90400 and ev["month"] == "August"
    # this month's budget is labelled with its month, never a bare "month_budget" the AI could apply to other months
    assert ev["accounts"][0]["this_month"] == {"month": "August", "actual": 90400, "budget": 60200, "variance": -30200}
    assert "month_budget" not in ev["accounts"][0] and "never apply this month's budget to another month" in " ".join(w.SYSTEM.split())
    blob = json.dumps(w.build_request(s204, notes))
    for resident in ("Horshinski", "Prokop", "Kirkwood", "Lattanzio"):                # names from 204's receipts and arrears
        assert resident not in blob, resident
    assert w.note_evidence(s204, [n for n in notes if n["key"] == "income"][0]) is None   # income is never sent

    # ---- replies are validated, never guessed
    keys = {"cat:A", "cat:B"}
    assert w.parse_reply('{"notes":[{"key":"cat:A","reason":"Gas bills ran high."}]}', keys) == {"cat:A": "Gas bills ran high."}
    assert w.parse_reply("not json", keys) == {}
    assert w.parse_reply('{"notes":[{"key":"cat:Z","reason":"x"}]}', keys) == {}              # unknown key dropped
    assert w.parse_reply('{"notes":[{"key":"cat:A","reason":"[Cause to be confirmed]"}]}', keys) == {}  # placeholder dropped
    assert w.parse_reply('```json\n{"notes":[{"key":"cat:B","reason":"Ok."}]}\n```', keys) == {"cat:B": "Ok."}
    # "about" figures round to the nearest thousand; exact figures stay exact
    assert w.round_abouts("Steam was about $85,001 in March and $70,101 in April.") == "Steam was about $85,000 in March and $70,101 in April."
    assert w.round_abouts("roughly $27,600 a month; nearly $1,499") == "roughly $28,000 a month; nearly $1,499"  # "nearly" isn't neutral
    assert w.round_abouts("about $950 and about $89,000") == "about $950 and about $89,000"   # under $1,000 / already round
    assert w.parse_reply('{"notes":[{"key":"cat:A","reason":"About $85,001 in March."}]}', keys) == {"cat:A": "About $85,000 in March."}

    # ---- the portal: suggestions fill only empty, unconfirmed notes, as a new version; the FA still confirms
    app = snapshot_dev.make_app(tempfile.mkdtemp())
    svc = app.snapshot_service
    svc.why = FakeDrafter()
    c = app.test_client()
    P = lambda u, b=None: c.post(u, json=b or {})
    rid = c.post("/api/snapshots/generate", data={"entity": "204", "sample": "204_2026-08_statement.pdf", "as": "2"}).json["id"]
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["can"]["suggest"] and not c.get("/api/snapshots/%s?as=17" % rid).json["can"]["suggest"]
    keys = [n["key"] for n in v["commentary"]]
    i_rep = keys.index("cat:Repairs")
    assert [n for n in v["commentary"] if n["key"] == "cat:Utility Expenses"][0]["evidence"]["accounts"][0]["account"] == "Gas - Heating"
    # the FA writes Repairs herself first; a suggestion must never replace it
    assert P("/api/snapshots/%s/note?as=2" % rid, {"index": i_rep, "text": "AC compressor replaced in June."}).status_code == 200
    assert P("/api/snapshots/%s/suggest?as=17" % rid).status_code == 400            # not the FA, not an admin
    r1 = P("/api/snapshots/%s/suggest?as=2" % rid)
    assert r1.status_code == 200 and r1.json["result"] == {"suggested": 1}, r1.json
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    by = {n["key"]: n for n in v["commentary"]}
    assert by["cat:Utility Expenses"]["text"] == "Because of Gas - Heating." and by["cat:Utility Expenses"]["suggested"]["by"] == "Claude"
    assert not by["cat:Utility Expenses"].get("confirmed")                            # the FA still has to confirm it
    assert by["cat:Repairs"]["text"] == "AC compressor replaced in June." and "suggested" not in by["cat:Repairs"]
    assert "Suggested reasons from the GL for 1 note" in v["log"][0]["what"]
    assert len(svc.why.calls) == 1 and [i["key"] for i in svc.why.calls[0]] == ["cat:Utility Expenses"]
    assert P("/api/snapshots/%s/suggest?as=2" % rid).json["result"] == {"suggested": 0}  # nothing left to suggest
    assert len(svc.why.calls) == 1                                                    # ...and no AI call for nothing
    # the FA rewords the suggestion: it becomes hers
    i_util = keys.index("cat:Utility Expenses")
    assert P("/api/snapshots/%s/note?as=2" % rid, {"index": i_util, "text": "Gas heating ran high in July and August."}).status_code == 200
    assert "suggested" not in c.get("/api/snapshots/%s?as=2" % rid).json["commentary"][i_util]
    # a bad reply changes nothing
    rid2 = c.post("/api/snapshots/generate", data={"entity": "148", "sample": "148_2026-08_statement.pdf", "as": "2"}).json["id"]
    svc.why = FakeDrafter(reply="sorry, no JSON here")
    n0 = c.get("/api/snapshots/%s?as=2" % rid2).json["version"]
    assert P("/api/snapshots/%s/suggest?as=8" % rid2).json["result"] == {"suggested": 0}   # admin may ask; reply was junk
    assert c.get("/api/snapshots/%s?as=2" % rid2).json["version"] == n0
    print("snapshot why: all tests passed")


if __name__ == "__main__":
    run()
