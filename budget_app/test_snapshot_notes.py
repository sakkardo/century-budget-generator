"""Notes that don't repeat every month: new / worse / continuing / resolved.
Run: python budget_app/test_snapshot_notes.py

Rule (Jacob 2026-10-03): a known item is "worse" when its variance moved unfavorably by more than 10% AND more than
$5,000 since it was explained; otherwise its explanation carries forward ("continuing"). Settled timing lines
(the whole year's budget booked, nothing posted this month) always continue.
"""
import os
import re
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fitz

import snapshot_dev
import snapshot_render as r

SAMPLES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tasks", "snapshot_samples")


def note(key, variance, settled=False):
    return {"key": key, "title": key, "label": key, "facts": "f", "text": "", "variance": variance, "settled": settled, "status": "new"}


def prior(key, basis, text="explained"):
    return {"key": key, "label": key, "title": key, "text": text, "basis": basis, "variance": basis, "since": "June", "confirmed": {"by": "K"}}


def classify(cur, old):
    return r.classify_notes([dict(n) for n in cur], {p["key"]: p for p in old}, "July")


def run():
    # ---- the rule table
    st = lambda cur, old: {n["key"]: n["status"] for n in classify(cur, old)[0]}
    assert st([note("cat:A", -10000)], []) == {"cat:A": "new"}                                       # no prior note
    assert st([note("cat:A", -90000, settled=True)], [prior("cat:A", -10000)]) == {"cat:A": "continuing"}  # settled timing
    assert st([note("cat:A", -56000)], [prior("cat:A", -50000)]) == {"cat:A": "worse"}               # +12% and +$6k
    assert st([note("cat:A", -28000)], [prior("cat:A", -25000)]) == {"cat:A": "continuing"}          # +12% but only +$3k
    assert st([note("cat:A", -520000)], [prior("cat:A", -500000)]) == {"cat:A": "continuing"}        # +$20k but only +4%
    assert st([note("cat:A", -20000)], [prior("cat:A", -50000)]) == {"cat:A": "continuing"}          # improved
    assert st([note("saving:B", 40000)], [prior("saving:B", 60000)]) == {"saving:B": "worse"}        # saving shrank a lot
    assert st([note("overall", 0)], [prior("overall", 0)]) == {"overall": "new"}                    # never carried
    notes, resolved = classify([note("cat:A", -60000)], [prior("cat:A", -50000), prior("cat:Gone", -9000), prior("saving:X", 5)])
    assert resolved == ["cat:Gone"], resolved                                                        # savings never "resolve"
    a = notes[0]
    assert a["text"] == "explained" and a["since"] == "June" and a["basis"] == -50000 and a["moved"] == 10000
    # old-shape notes (no key/facts) still render
    from snapshot_parser import build_snapshot
    s204 = build_snapshot(open(os.path.join(SAMPLES, "204_2026-08_statement.pdf"), "rb").read())
    old_shape = [{"title": "Overall", "text": "Year to date..."}, {"title": "Utilities", "text": "Gas ran high."}]
    pdf = r.render_pdf(s204, old_shape)
    assert "Gas ran high." in fitz.open(stream=pdf, filetype="pdf")[0].get_text()

    # ---- real two-month run on 148: July all new; August carries July's explanations
    root = tempfile.mkdtemp()
    app = snapshot_dev.make_app(root)
    c = app.test_client()
    P = lambda u, b=None: c.post(u, json=b or {})
    rj = c.post("/api/snapshots/generate", data={"entity": "148", "sample": "148_2026-07_statement.pdf", "as": "2"}).json["id"]
    v = c.get("/api/snapshots/%s?as=2" % rj).json
    assert {n["status"] for n in v["commentary"]} == {"new"}, [n["status"] for n in v["commentary"]]
    idx = {n["key"]: i for i, n in enumerate(v["commentary"])}
    reasons = {"cat:Insurance": "Renewal premium came in above budget; this is the full-year cost.",
               "cat:Utility Expenses": "Steam ran high during the cold winter.",
               "cat:Taxes": "Tax abatement credit was smaller than budgeted."}
    for k, i in idx.items():
        body = {"index": i, "text": reasons[k]} if k in reasons else {"index": i}
        assert P("/api/snapshots/%s/note?as=2" % rj, body).status_code == 200
    # a later month can only see what July CONFIRMED, so July must be fully confirmed first
    assert all(n.get("confirmed") for n in c.get("/api/snapshots/%s?as=2" % rj).json["commentary"])

    ra = c.post("/api/snapshots/generate", data={"entity": "148", "sample": "148_2026-08_statement.pdf", "as": "2"}).json["id"]
    v = c.get("/api/snapshots/%s?as=2" % ra).json
    byk = {n["key"]: n for n in v["commentary"]}
    assert byk["cat:Insurance"]["status"] == "continuing" and byk["cat:Insurance"]["since"] == "July"
    assert byk["cat:Insurance"]["text"] == reasons["cat:Insurance"]
    assert byk["cat:Taxes"]["status"] == "continuing"
    assert byk["cat:Utility Expenses"]["status"] == "worse" and byk["cat:Utility Expenses"]["moved"] == 9273
    assert byk["cat:Utility Expenses"]["prior_text"] == reasons["cat:Utility Expenses"]
    assert byk["income"]["status"] == "continuing"
    assert byk["saving:Payroll Expenses"]["status"] == "new" and byk["overall"]["status"] == "new"
    assert "saving:Repairs" not in byk and v["resolved"] == []

    # one click confirms the continuing notes and keeps their July basis; worse/new still need the FA
    n_cont = sum(1 for n in v["commentary"] if n["status"] == "continuing")
    r1 = P("/api/snapshots/%s/notes/confirm-continuing?as=2" % ra)
    assert r1.status_code == 200 and r1.json["result"]["confirmed"] == n_cont == 3, r1.json
    assert P("/api/snapshots/%s/notes/confirm-continuing?as=2" % ra).status_code == 400  # nothing left
    assert P("/api/snapshots/%s/notes/confirm-continuing?as=8" % ra).status_code == 400  # PM cannot
    v = c.get("/api/snapshots/%s?as=2" % ra).json
    byk = {n["key"]: n for n in v["commentary"]}
    assert byk["income"]["basis"] == 23918  # still measured from July's explained amount
    assert not v["can"]["send"]
    i_util = [i for i, n in enumerate(v["commentary"]) if n["key"] == "cat:Utility Expenses"][0]
    assert P("/api/snapshots/%s/note?as=2" % ra, {"index": i_util, "text": "Steam ran high in the cold winter; August bill also included a catch-up."}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % ra).json
    for i, n in enumerate(v["commentary"]):
        if not n.get("confirmed"):
            assert P("/api/snapshots/%s/note?as=2" % ra, {"index": i}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % ra).json
    byk = {n["key"]: n for n in v["commentary"]}
    assert byk["cat:Utility Expenses"]["basis"] == -55239  # re-explained: basis moves to today
    assert v["can"]["send"]

    # the board report: new/worse in full, insurance only in the compact ongoing table
    t = "\n".join(p.get_text() for p in fitz.open(stream=c.get("/api/snapshots/%s/pdf?as=2" % ra).data, filetype="pdf"))
    assert "Ongoing items, explained previously" in t and "Renewal premium came in above budget" in t
    assert "MOVED: explained in July" in t and "catch-up" in t
    wc = t.split("What changed", 1)[1].split("Ongoing items", 1)[0]
    assert "Insurance, $13,890 over budget" not in wc  # not repeated as a full note
    # the PM email carries the same split
    assert P("/api/snapshots/%s/send?as=2" % ra).status_code == 200
    mail = app.snapshot_service.mailer.outbox[-1]["html"]
    assert "Ongoing items, explained previously" in mail and "Renewal premium" in mail
    link = re.search(r'(/snapshot/confirm/[A-Za-z0-9-]+/[A-Za-z0-9_-]+)"', mail).group(1)
    assert b"Ongoing items, explained previously" in c.get(link).data

    # January starts fresh (no prior month in the same year)
    assert app.snapshot_service._prior_notes("148", 2026, 1) == ({}, None)
    print("snapshot notes: all tests passed")


if __name__ == "__main__":
    run()
