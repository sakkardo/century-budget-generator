"""Notes that don't repeat every month: new / worse / continuing / resolved.
Run: python budget_app/test_snapshot_notes.py

Flag rule (Jacob 2026-10-03): an expense line gets a note only when it is more than 10% over its YTD budget or
more than 15% over this month's budget.
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

    # ---- which lines get a note (Jacob 2026-10-03): >10% over YTD budget, or >15% over this month's budget
    cat = lambda yv, yb, mv, mb: {"ytd_var": yv, "ytd_budget": yb, "month_var": mv, "month_budget": mb}
    assert r.flagged(cat(-11000, 100000, 0, 10000))            # 11% over YTD
    assert not r.flagged(cat(-9000, 100000, -1400, 10000))     # 9% YTD, 14% month: neither crosses
    assert r.flagged(cat(2000, 100000, -1600, 10000))          # 16% over this month alone, YTD favorable
    assert not r.flagged(cat(-500, 0, -500, 0))                # nothing budgeted: no percentage to judge
    assert not r.flagged(cat(-60000, 1000000, 0, 0))           # big dollars but only 6%: no note

    # ---- real two-month run on 148: July all new; August carries July's explanations
    root = tempfile.mkdtemp()
    app = snapshot_dev.make_app(root)
    c = app.test_client()
    P = lambda u, b=None: c.post(u, json=b or {})
    rj = c.post("/api/snapshots/generate", data={"entity": "148", "sample": "148_2026-07_statement.pdf", "as": "2"}).json["id"]
    v = c.get("/api/snapshots/%s?as=2" % rj).json
    assert {n["status"] for n in v["commentary"]} == {"new"}, [n["status"] for n in v["commentary"]]
    idx = {n["key"]: i for i, n in enumerate(v["commentary"])}
    assert "cat:Insurance" not in idx and "cat:Supplies" in idx  # insurance is 6% over: under the new threshold
    reasons = {"income": "Non-recurring income from the refinance closing.",
               "cat:Supplies": "Bulk purchase of cleaning supplies ahead of the summer."}
    for k, i in idx.items():
        body = {"index": i, "text": reasons[k]} if k in reasons else {"index": i}
        assert P("/api/snapshots/%s/note?as=2" % rj, body).status_code == 200
    # a later month can only see what July CONFIRMED, so July must be fully confirmed first
    assert all(n.get("confirmed") for n in c.get("/api/snapshots/%s?as=2" % rj).json["commentary"])

    ra = c.post("/api/snapshots/generate", data={"entity": "148", "sample": "148_2026-08_statement.pdf", "as": "2"}).json["id"]
    v = c.get("/api/snapshots/%s?as=2" % ra).json
    byk = {n["key"]: n for n in v["commentary"]}
    assert byk["income"]["status"] == "continuing" and byk["income"]["since"] == "July"
    assert byk["income"]["text"] == reasons["income"]
    assert byk["cat:Utility Expenses"]["status"] == "new"                       # 11% YTD, 16% in August
    pf = byk["cat:Professional Fees"]                                           # 53% over in August, YTD favorable
    assert pf["status"] == "new" and pf["title"] == "Professional Fees, $7,927 over budget in August", pf["title"]
    assert byk["saving:Payroll Expenses"]["status"] == "new" and byk["overall"]["status"] == "new"
    assert "cat:Supplies" not in byk and v["resolved"] == ["Supplies"], v["resolved"]  # 9% YTD now: drops off

    # one click confirms the continuing notes and keeps their July basis; new notes still need the FA
    r1 = P("/api/snapshots/%s/notes/confirm-continuing?as=2" % ra)
    assert r1.status_code == 200 and r1.json["result"]["confirmed"] == 1, r1.json
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
    assert byk["cat:Utility Expenses"]["basis"] == -55239  # explained: basis is today's variance
    assert v["can"]["send"]

    # the board report: new notes in full, income only in the compact ongoing table
    t = "\n".join(p.get_text() for p in fitz.open(stream=c.get("/api/snapshots/%s/pdf?as=2" % ra).data, filetype="pdf"))
    assert "Ongoing items, explained previously" in t and "refinance closing" in t
    assert "catch-up" in t and "No longer flagged since last month: Supplies" in t
    assert "more than 10 percent over its year-to-date budget or more than 15 percent" in " ".join(t.split())
    wc = t.split("What changed", 1)[1].split("Ongoing items", 1)[0]
    assert "Income, $46,840 above budget" not in wc  # not repeated as a full note
    # the PM email carries the same split
    assert P("/api/snapshots/%s/send?as=2" % ra).status_code == 200
    mail = app.snapshot_service.mailer.outbox[-1]["html"]
    assert "Ongoing items, explained previously" in mail and "refinance closing" in mail
    link = re.search(r'(/snapshot/confirm/[A-Za-z0-9-]+/[A-Za-z0-9_-]+)"', mail).group(1)
    assert b"Ongoing items, explained previously" in c.get(link).data

    # January starts fresh (no prior month in the same year)
    assert app.snapshot_service._prior_notes("148", 2026, 1) == ({}, None)
    print("snapshot notes: all tests passed")


if __name__ == "__main__":
    run()
