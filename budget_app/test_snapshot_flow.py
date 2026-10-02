"""End-to-end test of the snapshot flow through the HTTP API (temp folder, test people).
Run: python budget_app/test_snapshot_flow.py
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import snapshot_dev


def confirm_all(get, post, rid, suffix=""):
    """FA confirms every note; notes with a [placeholder] get real wording first."""
    import re
    v = get("/api/snapshots/%s%s" % (rid, suffix))
    for i, c in enumerate(v["commentary"]):
        body = {"index": i}
        if re.search(r"\[[^\]]+\]", c["text"]):
            body["text"] = re.sub(r"\s*\[[^\]]+\]", "", c["text"]) + " Confirmed with the super."
        r = post("/api/snapshots/%s/note%s" % (rid, suffix), body)
        assert r.status_code == 200, r.json


def run():
    root = tempfile.mkdtemp()
    c = snapshot_dev.make_app(root).test_client()

    def post(url, body=None, **kw):
        return c.post(url, json=body or {}, **kw)

    # generate 204 from the sample statement
    r = c.post("/api/snapshots/generate", data={"entity": "204", "sample": "204_2026-08_statement.pdf", "as": "2"})
    assert r.status_code == 200, r.json
    rid = r.json["id"]
    assert rid == "204-2026-08"
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["stage"] == "draft" and all(x["status"] == "tied" for x in v["checks"]) and not v["can"]["send"]
    assert any("Confirm or edit every note" in w for w in v["why_not"])
    assert c.get("/api/snapshots/%s/pdf" % rid).data[:4] == b"%PDF"

    # sending is blocked until every note is confirmed
    r = post("/api/snapshots/%s/send?as=2" % rid)
    assert r.status_code == 400 and "Confirm or edit every note" in r.json["error"], r.json
    notes = v["commentary"]
    # drafts never carry "[... to be confirmed ...]" placeholders (Jacob, 2026-10-02)
    assert not any("[" in n["text"] or "confirm" in n["text"].lower() for n in notes), [n["text"] for n in notes]
    ph = 2  # a note the FA rewrites below
    # bracketed text typed by the FA still cannot reach the board, and the PM cannot confirm notes
    r = post("/api/snapshots/%s/note?as=2" % rid, {"index": ph, "text": "Gas ran high [check with super]."})
    assert r.status_code == 400 and "placeholder" in r.json["error"], r.json
    assert post("/api/snapshots/%s/note?as=8" % rid, {"index": 0}).status_code == 400
    # confirming unchanged text keeps the version; editing makes a new one
    assert post("/api/snapshots/%s/note?as=2" % rid, {"index": 0}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["version"] == 1 and v["commentary"][0]["confirmed"]["by"] == "Kristy Paxinos"
    r = post("/api/snapshots/%s/note?as=2" % rid, {"index": ph, "text": "Gas heating ran high; confirmed with the super."})
    assert r.status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["version"] == 2 and v["commentary"][ph]["confirmed"] and v["commentary"][0]["confirmed"]
    assert "confirmed with the super" in v["commentary"][ph]["text"]
    confirm_all(lambda u: c.get(u).json, post, rid, "?as=2")
    assert c.get("/api/snapshots/%s?as=2" % rid).json["can"]["send"]
    # only the FA can send; PM cannot sign yet
    assert post("/api/snapshots/%s/send?as=8" % rid).status_code == 400
    assert post("/api/snapshots/%s/sign?as=8" % rid, {"role": "pm", "decision": "approve"}).status_code == 400
    assert post("/api/snapshots/%s/send?as=2" % rid).status_code == 200

    # FA cannot sign before PM; PM must give a note to request changes
    assert post("/api/snapshots/%s/sign?as=2" % rid, {"role": "fa", "decision": "approve"}).status_code == 400
    assert post("/api/snapshots/%s/sign?as=8" % rid, {"role": "pm", "decision": "request_changes"}).status_code == 400
    r = post("/api/snapshots/%s/sign?as=8" % rid, {"role": "pm", "decision": "request_changes", "note": "Utilities look high"})
    assert r.status_code == 200
    assert c.get("/api/snapshots/%s?as=2" % rid).json["stage"] == "changes_requested"

    # FA edits the commentary: new version, old request no longer counts
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    com = v["commentary"]
    com[1]["text"] = "Gas heating ran high in the winter months; confirmed with the super."
    assert post("/api/snapshots/%s/edit?as=8" % rid, {"commentary": com}).status_code == 400  # PM cannot edit
    assert post("/api/snapshots/%s/edit?as=2" % rid, {"commentary": com, "board_note": "Figures are unaudited."}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["state"] == "pending_signoff" and v["stale"] == 1
    # confirming an unchanged note after a signature does NOT clear it (the board sees the same words)
    before = v["version"]
    assert post("/api/snapshots/%s/sign?as=8" % rid, {"role": "pm", "decision": "approve", "note": "ok"}).status_code == 200
    assert post("/api/snapshots/%s/note?as=2" % rid, {"index": 0}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["version"] == before and v["stale"] == 1 and v["waiting_on"] == ["fa"], (v["version"], v["stale"], v["waiting_on"])

    # PM already approved above; FA approves -> released to both folders with the APPROVED stamp
    assert c.get("/api/snapshots/%s?as=2" % rid).json["released"] is None
    assert post("/api/snapshots/%s/sign?as=2" % rid, {"role": "fa", "decision": "approve"}).status_code == 200
    v = c.get("/api/snapshots/%s?as=2" % rid).json
    assert v["stage"] == "released" and len(v["released"]["files"]) == 2, v["released"]
    for f in v["released"]["files"]:
        path = os.path.join(root, "sharepoint_test_copy", f)
        assert os.path.exists(path) and open(path, "rb").read(4) == b"%PDF", f
    assert v["released"]["files"][0].startswith("01 - Accounting General/Monthly Financial Snapshots/2026/08-2026/204 - ")
    # released snapshots cannot be edited or regenerated
    assert post("/api/snapshots/%s/edit?as=2" % rid, {"commentary": com}).status_code == 400
    r = c.post("/api/snapshots/generate", data={"entity": "204", "sample": "204_2026-08_statement.pdf", "as": "2"})
    assert r.status_code == 400

    # 302: either of two FAs may act; solo building is blocked
    rid2 = c.post("/api/snapshots/generate", data={"entity": "302", "sample": "302_2026-08_statement.pdf", "as": "101"}).json["id"]
    confirm_all(lambda u: c.get(u).json, post, rid2, "?as=102")
    assert post("/api/snapshots/%s/send?as=102" % rid2).status_code == 200
    assert post("/api/snapshots/%s/sign?as=17" % rid2, {"role": "pm", "decision": "approve"}).status_code == 200
    assert post("/api/snapshots/%s/sign?as=102" % rid2, {"role": "fa", "decision": "approve"}).status_code == 200
    assert c.get("/api/snapshots/%s?as=101" % rid2).json["stage"] == "released"

    rid3 = c.post("/api/snapshots/generate", data={"entity": "999", "sample": "204_2026-08_statement.pdf", "as": "2"}).json["id"]
    r = post("/api/snapshots/%s/send?as=2" % rid3)
    assert r.status_code == 400 and "two different people" in r.json["error"], r.json

    # a confirmation stamp sent by the browser is ignored: only the server confirms notes
    forged = c.get("/api/snapshots/%s?as=2" % rid3).json["commentary"]
    for n in forged:
        n["confirmed"] = {"by": "Kristy Paxinos", "at": "now"}
    assert post("/api/snapshots/%s/edit?as=2" % rid3, {"commentary": forged, "board_note": "x"}).status_code == 200
    assert all(not n.get("confirmed") for n in c.get("/api/snapshots/%s?as=2" % rid3).json["commentary"])

    # a bad upload reaches the screen as a message, not a crash
    import io
    r = c.post("/api/snapshots/generate", data={"entity": "204", "as": "2", "file": (io.BytesIO(b"not a pdf"), "x.pdf")})
    assert r.status_code == 400
    print("snapshot flow: all tests passed")


if __name__ == "__main__":
    run()
