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
    # income: same thresholds, either direction (a windfall is news too)
    assert r.income_flagged(cat(11000, 100000, 0, 10000)) and r.income_flagged(cat(-11000, 100000, 0, 10000))
    assert r.income_flagged(cat(0, 100000, -1600, 10000))      # 16% short this month
    assert not r.income_flagged(cat(46840, 3851162, 22922, 496492))  # 148 August: 1.2% / 4.6%, no note
    inc204 = [n for n in r.draft_commentary(s204) if n["key"] == "income"]
    assert inc204 and inc204[0]["title"] == "Income, $800,436 above budget year to date", inc204  # 13% above

    # ---- year-to-date vs this-month-only notes (Jacob 2026-10-05): lead with what triggered the note, grouped on the report
    d204 = {n["key"]: n for n in r.draft_commentary(s204)}
    u = d204["cat:Utility Expenses"]
    assert u["scope"] == "ytd" and u["facts"].startswith("17% over budget year to date ($113,797). August alone was $33,083 over (30%).") , u["facts"]
    assert d204["overall"]["scope"] is None and d204["income"]["scope"] == "ytd"
    assert r.note_scope({"key": "cat:X", "title": "X, $5 over budget in August"}) == "month"        # older notes: read from the title
    assert r.note_scope({"key": "cat:X", "title": "X, $5 over budget year to date"}) == "ytd"
    s148 = build_snapshot(open(os.path.join(SAMPLES, "148_2026-08_statement.pdf"), "rb").read())
    d148 = r.draft_commentary(s148)
    keys = [n["key"] for n in d148]
    pf = [n for n in d148 if n["key"] == "cat:Professional Fees"][0]
    assert pf["scope"] == "month" and pf["facts"].startswith("$7,927 over budget in August (53%). Year to date it is still $1,253 under budget."), pf["facts"]
    assert keys.index("cat:Utility Expenses") < keys.index("cat:Professional Fees")                 # year-to-date items first
    t148 = " ".join(" ".join(p.get_text() for p in fitz.open(stream=r.render_pdf(s148, d148), filetype="pdf")).split())
    assert t148.index("YEAR TO DATE") < t148.index("Utility Expenses, $55,239") < t148.index("THIS MONTH ONLY") < t148.index("Professional Fees, $7,927")
    import snapshot_mail
    body = snapshot_mail._notes([("Overall", "x", None), ("Pro Fees", "y", "month"), ("Utilities", "z", "ytd")])
    assert body.index("Overall") < body.index("Year to date") < body.index("Utilities") < body.index("This month only") < body.index("Pro Fees")
    assert "R&amp;M" in snapshot_mail._notes([("R&M", "two-part tuples still work")])

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
    assert "income" not in idx                                   # 0.7% YTD, 4.9% in July
    reasons = {"cat:Supplies": "Bulk purchase of cleaning supplies ahead of the summer."}
    for k, i in idx.items():
        body = {"index": i, "text": reasons[k]} if k in reasons else {"index": i}
        assert P("/api/snapshots/%s/note?as=2" % rj, body).status_code == 200
    # a later month can only see what July CONFIRMED, so July must be fully confirmed first
    assert all(n.get("confirmed") for n in c.get("/api/snapshots/%s?as=2" % rj).json["commentary"])

    # Under the 10%/15% rule nothing 148 flagged in July is still flagged in August, so add one item "explained in
    # July" on top of the real lookup to exercise carry-forward on real August figures (utilities: -54,000 -> -55,239).
    svc = app.snapshot_service
    real_prior = svc._prior_notes
    def with_utilities(entity, year, month):
        notes, label = real_prior(entity, year, month)
        if notes:
            notes["cat:Utility Expenses"] = {"key": "cat:Utility Expenses", "label": "Utility Expenses", "title": "Utility Expenses",
                                             "text": "Steam ran high during the cold winter.", "basis": -54000, "variance": -54000,
                                             "since": "July", "confirmed": {"by": "Kristy"}}
        return notes, label
    svc._prior_notes = with_utilities

    ra = c.post("/api/snapshots/generate", data={"entity": "148", "sample": "148_2026-08_statement.pdf", "as": "2"}).json["id"]
    v = c.get("/api/snapshots/%s?as=2" % ra).json
    byk = {n["key"]: n for n in v["commentary"]}
    ut = byk["cat:Utility Expenses"]                                            # moved $1,239 (2%): not worse
    assert ut["status"] == "continuing" and ut["since"] == "July" and ut["text"] == "Steam ran high during the cold winter."
    pf = byk["cat:Professional Fees"]                                           # 53% over in August, YTD favorable
    assert pf["status"] == "new" and pf["title"] == "Professional Fees, $7,927 over budget in August", pf["title"]
    assert "income" not in byk                                                  # 1.2% YTD, 4.6% in August
    assert byk["saving:Payroll Expenses"]["status"] == "new" and byk["overall"]["status"] == "new"
    assert "cat:Supplies" not in byk and v["resolved"] == ["Supplies"], v["resolved"]  # 9% YTD now: drops off

    # one click confirms the continuing notes and keeps their July basis; new notes still need the FA
    r1 = P("/api/snapshots/%s/notes/confirm-continuing?as=2" % ra)
    assert r1.status_code == 200 and r1.json["result"]["confirmed"] == 1, r1.json
    assert P("/api/snapshots/%s/notes/confirm-continuing?as=2" % ra).status_code == 400  # nothing left
    assert P("/api/snapshots/%s/notes/confirm-continuing?as=8" % ra).status_code == 400  # PM cannot
    v = c.get("/api/snapshots/%s?as=2" % ra).json
    byk = {n["key"]: n for n in v["commentary"]}
    assert byk["cat:Utility Expenses"]["basis"] == -54000  # still measured from July's explained amount
    assert not v["can"]["send"]
    i_pf = [i for i, n in enumerate(v["commentary"]) if n["key"] == "cat:Professional Fees"][0]
    assert P("/api/snapshots/%s/note?as=2" % ra, {"index": i_pf, "text": "The annual audit fee was billed in August."}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % ra).json
    for i, n in enumerate(v["commentary"]):
        if not n.get("confirmed"):
            assert P("/api/snapshots/%s/note?as=2" % ra, {"index": i}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % ra).json
    byk = {n["key"]: n for n in v["commentary"]}
    assert byk["cat:Professional Fees"]["basis"] == 1253  # explained: basis is today's variance
    assert v["can"]["send"]

    # the board report: new notes in full, utilities only in the compact ongoing table
    t = "\n".join(p.get_text() for p in fitz.open(stream=c.get("/api/snapshots/%s/pdf?as=2" % ra).data, filetype="pdf"))
    assert "Previous notes" in t and "cold winter" in t and "Ongoing items" not in t
    assert "audit fee" in t and "No longer flagged since last month: Supplies" in t
    assert "more than 10 percent over its year-to-date budget or more than 15 percent" in " ".join(t.split())
    wc = t.split("New notes", 1)[1].split("Previous notes", 1)[0]
    assert "Utility Expenses, $55,239 over budget" not in wc and "audit fee" in wc  # continuing not repeated in full
    # the PM email carries the same split
    assert P("/api/snapshots/%s/send?as=2" % ra).status_code == 200
    mail = app.snapshot_service.mailer.outbox[-1]["html"]
    assert "New notes" in mail and "Previous notes" in mail and "cold winter" in mail
    link = re.search(r'(/snapshot/confirm/[A-Za-z0-9-]+/[A-Za-z0-9_-]+)"', mail).group(1)
    page = c.get(link).data
    assert b"/page/1.png" in page and b"Confirm this snapshot" in page  # the PM reads the report itself

    # ---- the FA can remove a suggested note (and restore it); removed notes never print, email or carry forward
    rr = c.post("/api/snapshots/generate", data={"entity": "204", "sample": "204_2026-08_statement.pdf", "as": "2"}).json["id"]
    v = c.get("/api/snapshots/%s?as=2" % rr).json
    keys = [n["key"] for n in v["commentary"]]
    i_rep, i_all = keys.index("cat:Repairs"), keys.index("overall")
    assert P("/api/snapshots/%s/note/remove?as=8" % rr, {"index": i_rep}).status_code == 400   # PM cannot
    assert P("/api/snapshots/%s/note/remove?as=2" % rr, {"index": i_all}).status_code == 400   # the headline stays
    assert P("/api/snapshots/%s/note/remove?as=2" % rr, {"index": i_rep}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % rr).json
    rep = v["commentary"][i_rep]
    assert rep["removed"]["by"] == "Kristy Paxinos" and v["version"] == 2           # a new version, with who removed it
    assert "Removed note" in v["log"][0]["what"]  # newest first
    assert P("/api/snapshots/%s/note?as=2" % rr, {"index": i_rep}).status_code == 400  # cannot confirm a removed note
    for i, n in enumerate(v["commentary"]):
        if not n.get("removed") and not n.get("confirmed"):
            assert P("/api/snapshots/%s/note?as=2" % rr, {"index": i}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % rr).json
    assert v["can"]["send"], v["why_not"]                                           # removed note does not block sending
    t = "\n".join(p.get_text() for p in fitz.open(stream=c.get("/api/snapshots/%s/pdf?as=2" % rr).data, filetype="pdf"))
    assert "Repairs, $53,205 over budget" not in t and "Utility Expenses, $113,797 over budget" in t
    assert P("/api/snapshots/%s/send?as=2" % rr).status_code == 200
    mail = app.snapshot_service.mailer.outbox[-1]["html"]
    assert "Repairs, $53,205" not in mail and "Utility Expenses, $113,797" in mail
    # a removed note is not "explained": next month it is suggested again as new
    sept_prior, _ = real_prior("204", 2026, 9)
    assert "cat:Repairs" not in sept_prior and "cat:Utility Expenses" in sept_prior
    # restore brings it back unconfirmed (the FA reviews it again) and pulls the snapshot back to draft
    assert P("/api/snapshots/%s/note/restore?as=2" % rr, {"index": i_rep}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % rr).json
    rep = v["commentary"][i_rep]
    assert not rep.get("removed") and not rep.get("confirmed") and not v["can"]["send"]
    assert "Repairs, $53,205 over budget" in "\n".join(p.get_text() for p in fitz.open(stream=c.get("/api/snapshots/%s/pdf?as=2" % rr).data, filetype="pdf"))
    # removing changes what the board reads, so the content hash changes
    from snapshot_signoff import content_hash
    notes = [dict(n) for n in v["commentary"]]
    h_all = content_hash(s204, notes)
    notes[i_rep]["removed"] = {"by": "K"}
    assert content_hash(s204, notes) != h_all

    # January starts fresh (no prior month in the same year)
    assert app.snapshot_service._prior_notes("148", 2026, 1) == ({}, None)
    print("snapshot notes: all tests passed")


if __name__ == "__main__":
    run()
