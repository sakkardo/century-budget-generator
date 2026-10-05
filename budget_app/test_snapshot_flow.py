"""End-to-end test of the snapshot flow (temp folder, test people, outbox instead of real email).
Run: python budget_app/test_snapshot_flow.py

Flow (Jacob, 2026-10-02): FA confirms notes -> "Confirm & send to PM" -> PM email with a one-time link
-> PM confirms on that page (POST) -> final -> saved into the building's month folder only.
"""
import io
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import snapshot_dev


def confirm_all(get, post, rid, suffix=""):
    """FA confirms every note; notes with a [placeholder] get real wording first."""
    v = get("/api/snapshots/%s%s" % (rid, suffix))
    for i, c in enumerate(v["commentary"]):
        body = {"index": i}
        if re.search(r"\[[^\]]+\]", c["text"]):
            body["text"] = re.sub(r"\s*\[[^\]]+\]", "", c["text"]) + " Confirmed with the super."
        r = post("/api/snapshots/%s/note%s" % (rid, suffix), body)
        assert r.status_code == 200, r.json


LINK = re.compile(r'/snapshot/confirm/([A-Za-z0-9-]+)/([A-Za-z0-9_-]+)"')


def last_link(outbox, kind=("pm_request", "pm_reminder"), to=None):
    for m in reversed(outbox):
        if m["kind"] in kind and (to is None or to in m["intended"]):
            g = LINK.search(m["html"])
            return "/snapshot/confirm/%s/%s" % (g.group(1), g.group(2))
    raise AssertionError("no PM email in the outbox")


def run():
    root = tempfile.mkdtemp()
    app = snapshot_dev.make_app(root)
    svc = app.snapshot_service
    c = app.test_client()

    def post(url, body=None, **kw):
        return c.post(url, json=body or {}, **kw)

    def stage(rid, as_="2"):
        return c.get("/api/snapshots/%s?as=%s" % (rid, as_)).json["stage"]

    # ---- generate and the FA's notes
    r = c.post("/api/snapshots/generate", data={"entity": "204", "sample": "204_2026-08_statement.pdf", "as": "2"})
    assert r.status_code == 200, r.json
    rid = r.json["id"]
    assert rid == "204-2026-08"
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["stage"] == "draft" and all(x["status"] == "tied" for x in v["checks"]) and not v["can"]["send"]
    assert any("Confirm or edit every note" in w for w in v["why_not"])
    assert c.get("/api/snapshots/%s/pdf" % rid).data[:4] == b"%PDF"
    r = post("/api/snapshots/%s/send?as=2" % rid)
    assert r.status_code == 400 and "Confirm or edit every note" in r.json["error"], r.json
    notes = v["commentary"]
    # drafts never carry "[... to be confirmed ...]" placeholders (Jacob, 2026-10-02)
    assert not any("[" in n["text"] or "confirm" in n["text"].lower() for n in notes), [n["text"] for n in notes]
    # bracketed text typed by the FA still cannot reach the board, and the PM cannot confirm notes
    r = post("/api/snapshots/%s/note?as=2" % rid, {"index": 2, "text": "Gas ran high [check with super]."})
    assert r.status_code == 400 and "placeholder" in r.json["error"], r.json
    assert post("/api/snapshots/%s/note?as=8" % rid, {"index": 0}).status_code == 400
    assert post("/api/snapshots/%s/note?as=2" % rid, {"index": 0}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["version"] == 1 and v["commentary"][0]["confirmed"]["by"] == "Kristy Paxinos"
    assert post("/api/snapshots/%s/note?as=2" % rid, {"index": 2, "text": "Gas heating ran high; confirmed with the super."}).status_code == 200
    assert c.get("/api/snapshots/%s?as=2" % rid).json["version"] == 2
    confirm_all(lambda u: c.get(u).json, post, rid, "?as=2")
    assert c.get("/api/snapshots/%s?as=2" % rid).json["can"]["send"]

    # ---- FA confirms and sends; the PM gets one email with the PDF and a link
    assert post("/api/snapshots/%s/send?as=8" % rid).status_code == 400  # only the FA sends
    assert post("/api/snapshots/%s/sign?as=8" % rid, {"role": "pm", "decision": "approve"}).status_code in (404, 405)  # no portal signing
    n0 = len(svc.mailer.outbox)
    r = post("/api/snapshots/%s/send?as=2" % rid)
    assert r.status_code == 200 and r.json["result"]["pms"] == ["Jacob Sirotkin"], r.json
    mail = svc.mailer.outbox[n0:]
    assert len(mail) == 1 and mail[0]["kind"] == "pm_request" and mail[0]["intended"] == ["jsirotkin@centuryny.com"]
    assert mail[0]["sender"] == "kpaxinos@centuryny.com" and mail[0]["attachments"] == ["204 - 444 East 86th Owners Corp Monthly Financial Snapshot August 2026.pdf"]
    assert "$49,853" in mail[0]["html"] and "Review the August snapshot" in mail[0]["html"]
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["stage"] == "awaiting_pm" and v["can"]["resend"] and not v["can"]["send"] and v["request"]["pms"] == ["Jacob Sirotkin"]
    link1 = last_link(svc.mailer.outbox)

    # ---- opening the link (as a mail scanner would) changes nothing
    for _ in range(3):
        r = c.get(link1)
        assert r.status_code == 200 and b"Confirm this snapshot" in r.data
        assert b"/page/1.png" in r.data and b"/page/2.png" in r.data and b"/page/3.png" not in r.data  # the report, both pages
    png = c.get(link1 + "/page/1.png")
    assert png.status_code == 200 and png.mimetype == "image/png" and png.data[:8] == b"\x89PNG\r\n\x1a\n"
    assert c.get(link1 + "/page/3.png").status_code == 404
    assert b"not valid" in c.get(link1[:-4] + "XXXX/page/1.png").data  # a bad link shows no report
    assert stage(rid) == "awaiting_pm"
    assert c.get(link1 + "/pdf").data[:4] == b"%PDF"
    assert b"not valid" in c.get(link1[:-4] + "XXXX").data

    # ---- PM asks for changes (needs a note); the FA is told; the link is spent
    r = c.post(link1, data={"decision": "request_changes", "note": ""})
    assert b"Not done" in r.data and stage(rid) == "awaiting_pm"
    r = c.post(link1, data={"decision": "request_changes", "note": "Utilities look high, check gas."})
    assert b"Sent back for changes" in r.data and stage(rid) == "changes_requested"
    assert svc.mailer.outbox[-1]["kind"] == "fa_changes" and "check gas" in svc.mailer.outbox[-1]["html"]
    assert b"Already done" in c.post(link1, data={"decision": "approve"}).data
    assert stage(rid) == "changes_requested"

    # ---- FA edits, re-sends; resending again replaces the link but keeps the 48h clock
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["can"]["send"] and v["can"]["edit"]
    assert post("/api/snapshots/%s/note?as=2" % rid, {"index": 2, "text": "Gas heating ran $72,569 over: the July bill was estimated high."}).status_code == 200
    assert post("/api/snapshots/%s/send?as=2" % rid).status_code == 200
    link2 = last_link(svc.mailer.outbox)
    sent1 = svc.store.get(rid)["pm_request"]["sent_iso"]
    assert post("/api/snapshots/%s/send?as=2" % rid).status_code == 200  # resend
    link3 = last_link(svc.mailer.outbox)
    assert link3 != link2 and svc.store.get(rid)["pm_request"]["sent_iso"] == sent1
    assert b"replaced" in c.get(link2).data and b"replaced" in c.post(link2, data={"decision": "approve"}).data
    assert stage(rid) == "awaiting_pm"

    # ---- an FA edit while the PM is reviewing cancels the link
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    com = v["commentary"]
    com[1]["text"] = com[1]["text"] + " (rev)"
    assert post("/api/snapshots/%s/edit?as=2" % rid, {"commentary": com, "board_note": "Figures are unaudited."}).status_code == 200
    assert b"replaced" in c.get(link3).data and stage(rid) == "draft"
    assert post("/api/snapshots/%s/send?as=2" % rid).status_code == 200
    link4 = last_link(svc.mailer.outbox)

    # ---- PM confirms: final, saved into the building's month folder ONLY, FA told; second click is harmless
    r = c.post(link4, data={"decision": "approve"})
    assert b"Confirmed. Thank you." in r.data, r.data[:300]
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["stage"] == "released" and len(v["released"]["files"]) == 1, v["released"]
    f = v["released"]["files"][0]
    assert f == "204 - 444 East 86th Owners Corp/Monthly Financials/2026/08 - August/204 - 444 East 86th Owners Corp Monthly Financial Snapshot August 2026.pdf", f
    assert open(os.path.join(root, "sharepoint_test_copy", f), "rb").read(4) == b"%PDF"
    pm = [s for s in v["signoffs"] if s["role"] == "pm" and s["decision"] == "approve"][-1]
    assert pm["via"]["method"] == "email-link" and pm["name"] == "Jacob Sirotkin"
    assert svc.mailer.outbox[-1]["kind"] == "fa_final"
    assert b"Already done" in c.post(link4, data={"decision": "approve"}).data
    assert len(os.listdir(os.path.dirname(os.path.join(root, "sharepoint_test_copy", f)))) == 1
    # final snapshots cannot be edited or regenerated
    assert post("/api/snapshots/%s/edit?as=2" % rid, {"commentary": com}).status_code == 400
    assert c.post("/api/snapshots/generate", data={"entity": "204", "sample": "204_2026-08_statement.pdf", "as": "2"}).status_code == 400

    # ---- the 48-hour timer on 302: reminder at 24h (once, fresh link), overdue at 48h (once), never auto-final
    rid2 = c.post("/api/snapshots/generate", data={"entity": "302", "sample": "302_2026-08_statement.pdf", "as": "101"}).json["id"]
    confirm_all(lambda u: c.get(u).json, post, rid2, "?as=102")  # either FA may act
    assert post("/api/snapshots/%s/send?as=102" % rid2).status_code == 200
    first = last_link(svc.mailer.outbox, to="gmatos@centuryny.com")
    sent = datetime.fromisoformat(svc.store.get(rid2)["pm_request"]["sent_iso"])
    assert svc.tick(now=sent + timedelta(hours=23))["reminded"] == []
    assert svc.tick(now=sent + timedelta(hours=25))["reminded"] == [rid2]
    assert svc.tick(now=sent + timedelta(hours=26))["reminded"] == []
    assert svc.mailer.outbox[-1]["kind"] == "pm_reminder" and "Reminder" in svc.mailer.outbox[-1]["subject"]
    fresh = last_link(svc.mailer.outbox, to="gmatos@centuryny.com")
    assert fresh != first and b"replaced" in c.get(first).data and b"Confirm this snapshot" in c.get(fresh).data
    t = svc.tick(now=sent + timedelta(hours=49))
    assert t["escalated"] == [rid2] and svc.mailer.outbox[-1]["kind"] == "overdue"
    assert "jsirotkin@centuryny.com" in svc.mailer.outbox[-1]["intended"]
    assert svc.tick(now=sent + timedelta(hours=60))["escalated"] == []
    assert stage(rid2, "101") == "awaiting_pm"  # silence never finalizes
    # an expired link is refused
    def expire(rec):
        rec["pm_request"]["expires_iso"] = (datetime.fromisoformat(rec["pm_request"]["sent_iso"]) - timedelta(hours=1)).isoformat()
        return rec, None
    svc.store.mutate(rid2, expire)
    assert b"expired" in c.get(fresh).data and b"expired" in c.post(fresh, data={"decision": "approve"}).data
    assert stage(rid2, "101") == "awaiting_pm"

    # ---- solo building is blocked; forged confirmation stamps are ignored; bad upload is a message
    rid3 = c.post("/api/snapshots/generate", data={"entity": "999", "sample": "204_2026-08_statement.pdf", "as": "2"}).json["id"]
    r = post("/api/snapshots/%s/send?as=2" % rid3)
    assert r.status_code == 400 and "two different people" in r.json["error"], r.json
    forged = c.get("/api/snapshots/%s?as=2" % rid3).json["commentary"]
    for n in forged:
        n["confirmed"] = {"by": "Kristy Paxinos", "at": "now"}
    assert post("/api/snapshots/%s/edit?as=2" % rid3, {"commentary": forged, "board_note": "x"}).status_code == 200
    assert all(not n.get("confirmed") for n in c.get("/api/snapshots/%s?as=2" % rid3).json["commentary"])
    r = c.post("/api/snapshots/generate", data={"entity": "204", "as": "2", "file": (io.BytesIO(b"not a pdf"), "x.pdf")})
    assert r.status_code == 400
    # the click-through outbox page lists the emails
    assert b"Test outbox" in c.get("/snapshots/dev/outbox").data

    # ---- admin (Jacob, by email): sees all, deletes non-final snapshots, resends from the FA's mailbox
    svc2 = app.snapshot_service
    assert svc2.is_admin(8) and not svc2.is_admin(2) and not svc2.is_admin(0)
    assert c.get("/api/snapshots/meta?as=8").json["is_admin"] is True and c.get("/api/snapshots/meta?as=2").json["is_admin"] is False
    rd = c.post("/api/snapshots/generate", data={"entity": "302", "sample": "302_2026-08_statement.pdf", "as": "101"}).json["id"]
    assert c.delete("/api/snapshots/%s?as=2" % rd).status_code == 400              # not an admin
    assert c.get("/api/snapshots/%s?as=8" % rd).json["can"]["delete"] is True
    assert c.delete("/api/snapshots/%s?as=8" % rd).status_code == 200
    assert c.get("/api/snapshots/%s?as=8" % rd).status_code == 404
    # practice run: the PM email goes to the admin instead of the building's PM (302's PM is George Matos)
    rp = c.post("/api/snapshots/generate", data={"entity": "302", "sample": "302_2026-08_statement.pdf", "as": "101"}).json["id"]
    assert c.post("/api/snapshots/%s/pm-override?as=101" % rp, json={"user_id": 8}).status_code == 400   # FA is not admin
    assert c.post("/api/snapshots/%s/pm-override?as=8" % rp, json={"user_id": 101}).status_code == 400   # PM can't be the FA
    assert c.post("/api/snapshots/%s/pm-override?as=8" % rp, json={"user_id": 8}).status_code == 200
    v = c.get("/api/snapshots/%s?as=101" % rp).json
    assert v["pm_override"]["name"] == "Jacob Sirotkin" and v["pm_override"]["instead_of"] == ["George Matos"]
    assert [a["name"] for a in v["team"] if a["role"] == "pm"] == ["Jacob Sirotkin"]
    for i, n in enumerate(v["commentary"]):
        c.post("/api/snapshots/%s/note?as=101" % rp, json={"index": i})
    n0 = len(svc2.mailer.outbox)
    assert c.post("/api/snapshots/%s/send?as=101" % rp, json={}).status_code == 200
    sent = svc2.mailer.outbox[n0:]
    assert sent and all("jsirotkin@centuryny.com" in m["intended"] and not any("gmatos" in a for a in m["intended"]) for m in sent),         [m["intended"] for m in sent]  # never George
    assert c.post("/api/snapshots/%s/pm-override?as=8" % rp, json={"user_id": None}).status_code == 400  # already sent
    plink = re.search(r'(/snapshot/confirm/[A-Za-z0-9-]+/[A-Za-z0-9_-]+)"', sent[-1]["html"]).group(1)
    assert b"Confirmed. Thank you." in c.post(plink, data={"decision": "approve"}).data   # Jacob confirms as the practice PM
    assert stage(rp) in ("approved", "released")
    final_id = [r["id"] for r in c.get("/api/snapshots?as=8").json if r["stage"] in ("approved", "released")]
    if final_id:
        assert c.delete("/api/snapshots/%s?as=8" % final_id[0]).status_code == 400  # final snapshots are kept
    print("snapshot flow: all tests passed")


if __name__ == "__main__":
    run()
