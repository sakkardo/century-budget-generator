"""End-to-end test of the snapshot flow through the HTTP API (temp folder, test people).
Run: python budget_app/test_snapshot_flow.py
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import snapshot_dev


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
    assert v["stage"] == "draft" and all(x["status"] == "tied" for x in v["checks"]) and v["can"]["send"]
    assert c.get("/api/snapshots/%s/pdf" % rid).data[:4] == b"%PDF"

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
    assert v["version"] == 2 and v["state"] == "pending_signoff" and v["stale"] == 1

    # PM approves, FA approves -> released to both folders with the APPROVED stamp
    assert post("/api/snapshots/%s/sign?as=8" % rid, {"role": "pm", "decision": "approve", "note": "ok"}).status_code == 200
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
    assert post("/api/snapshots/%s/send?as=102" % rid2).status_code == 200
    assert post("/api/snapshots/%s/sign?as=17" % rid2, {"role": "pm", "decision": "approve"}).status_code == 200
    assert post("/api/snapshots/%s/sign?as=102" % rid2, {"role": "fa", "decision": "approve"}).status_code == 200
    assert c.get("/api/snapshots/%s?as=101" % rid2).json["stage"] == "released"

    rid3 = c.post("/api/snapshots/generate", data={"entity": "999", "sample": "204_2026-08_statement.pdf", "as": "2"}).json["id"]
    r = post("/api/snapshots/%s/send?as=2" % rid3)
    assert r.status_code == 400 and "two different people" in r.json["error"], r.json

    # a bad upload reaches the screen as a message, not a crash
    import io
    r = c.post("/api/snapshots/generate", data={"entity": "204", "as": "2", "file": (io.BytesIO(b"not a pdf"), "x.pdf")})
    assert r.status_code == 400
    print("snapshot flow: all tests passed")


if __name__ == "__main__":
    run()
