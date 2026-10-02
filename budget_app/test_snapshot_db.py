"""Test the database-backed snapshot blueprint (SQLite in memory, fake SharePoint).
Run: python budget_app/test_snapshot_db.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

import snapshot_db
from test_snapshot_flow import confirm_all

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tasks", "snapshot_samples")


class FakeGraph:
    """In-memory SharePoint: paths -> bytes. Folders are implied by file paths plus `extra` folders."""

    def __init__(self, folders):
        self.files, self.folders = {}, set(folders)

    def list_children(self, path):
        prefix = path + "/" if path else ""
        out = {}
        for f in self.folders:
            if f.startswith(prefix) and f != path:
                out[f[len(prefix):].split("/")[0]] = True
        return [{"name": n, "folder": True} for n in sorted(out)]

    def exists(self, path):
        return path in self.files

    def put_new(self, path, data):
        if path in self.files:
            raise ValueError("exists")
        self.files[path] = data


def helpers_of(app):
    return app.snapshot_helpers


def build(graph, identity_box, strong=True):
    app = Flask(__name__)
    app.config.update(SQLALCHEMY_DATABASE_URI="sqlite://", SECRET_KEY="t")
    db = SQLAlchemy(app)

    class User(db.Model):
        __tablename__ = "users"
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(255), nullable=False)
        email = db.Column(db.String(255))
        role = db.Column(db.String(20))

    class BuildingAssignment(db.Model):
        __tablename__ = "building_assignments"
        id = db.Column(db.Integer, primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False)
        user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
        role = db.Column(db.String(20), nullable=False)

    wm = {"User": User, "BuildingAssignment": BuildingAssignment}
    bf = lambda: [{"entity_code": "204", "building_name": "444 East 86th Owners Corp"},
                  {"entity_code": "302", "building_name": "205 Water Street Condominium"}]
    bp, models, helpers = snapshot_db.create_snapshot_blueprint(
        db, wm, buildings_fn=bf, graph=graph, identity_fn=(lambda: identity_box["id"]) if strong else None)
    app.register_blueprint(bp)
    app.snapshot_helpers = helpers
    with app.app_context():
        db.create_all()
        for uid, name in ((2, "Kristy Paxinos"), (8, "Jacob Sirotkin"), (17, "George Matos"), (42, "Jennifer Murman, Giovanni Lizarazo")):
            db.session.add(User(id=uid, name=name))
        for e, uid, role in (("204", 2, "fa"), ("204", 8, "pm"), ("302", 42, "fa"), ("302", 17, "pm")):
            db.session.add(BuildingAssignment(entity_code=e, user_id=uid, role=role))
        db.session.commit()
    return app


def run():
    graph = FakeGraph(["204 - 444 East 86th Street Owners Corp/Monthly Financials/2026/08 - August",
                       "302 - 205 Water Street Condominium/Monthly Financials/2026/08.2026",
                       "01 - Accounting General/Monthly Financial Snapshots/2026"])
    who = {"id": 2}
    app = build(graph, who)
    c = app.test_client()
    P = lambda url, body=None: c.post(url, json=body or {})
    pdf204 = open(os.path.join(ROOT, "204_2026-08_statement.pdf"), "rb").read()
    pdf302 = open(os.path.join(ROOT, "302_2026-08_statement.pdf"), "rb").read()

    # identity comes from the server, not the URL; unknown identity gets nothing
    who["id"] = None
    assert c.get("/api/snapshots/meta?as=2").json["me"] is None
    who["id"] = 2
    assert c.get("/api/snapshots/meta?as=8").json["me"]["name"] == "Kristy Paxinos"
    assert c.get("/api/snapshots/meta").json["buildings"][0]["entity"] == "204"
    assert P("/api/snapshots/reset").status_code == 404

    # sample statements are click-through only
    assert c.post("/api/snapshots/generate", data={"entity": "204", "sample": "x.pdf"}).status_code == 400
    import io
    r = c.post("/api/snapshots/generate", data={"entity": "204", "file": (io.BytesIO(pdf204), "s.pdf")})
    assert r.status_code == 200, r.json
    rid = r.json["id"]
    assert [s["stage"] for s in c.get("/api/snapshots").json] == ["draft"]

    # full sign-off through the database; release is a dry run (nothing written) while disabled
    confirm_all(lambda u: c.get(u).json, lambda u, b: c.post(u, json=b), rid)
    assert P("/api/snapshots/%s/send" % rid).status_code == 200
    who["id"] = 8
    assert P("/api/snapshots/%s/sign" % rid, {"role": "pm", "decision": "approve"}).status_code == 200
    who["id"] = 2
    assert P("/api/snapshots/%s/sign" % rid, {"role": "fa", "decision": "approve"}).status_code == 200
    v = c.get("/api/snapshots/%s" % rid).json
    # release switched off: approved, NOT locked as released, nothing written, target paths remembered
    assert v["stage"] == "approved" and v["released"] is None and graph.files == {}, (v["stage"], v["released"])
    assert v["release_off"] is True and v["can"]["release"] is False
    assert v["rehearsal"]["files"][0].startswith("01 - Accounting General/Monthly Financial Snapshots/2026/08-2026/")
    assert "204 - 444 East 86th Street Owners Corp/Monthly Financials/2026/08 - August/" in v["rehearsal"]["files"][1]
    assert c.get("/api/snapshots").json[0]["stage"] == "approved"
    assert P("/api/snapshots/%s/release" % rid).status_code == 400  # still switched off
    # switch release on: the FA (only) releases the already-approved snapshot; both files land
    graph_rel = helpers_of(app)["service"].releaser
    graph_rel.enabled = True
    who["id"] = 8
    assert P("/api/snapshots/%s/release" % rid).status_code == 400  # PM cannot release
    who["id"] = 2
    assert c.get("/api/snapshots/%s" % rid).json["can"]["release"] is True
    assert P("/api/snapshots/%s/release" % rid).status_code == 200
    v = c.get("/api/snapshots/%s" % rid).json
    assert v["stage"] == "released" and len(graph.files) == 2
    assert P("/api/snapshots/%s/release" % rid).status_code == 400  # never twice
    graph_rel.enabled = False

    # enabled releaser writes both files, matches the existing month folder name, never overwrites
    os.environ["SNAPSHOT_RELEASE_ENABLED"] = "1"
    graph2 = FakeGraph(["302 - 205 Water Street Condominium/Monthly Financials/2026/08.2026"])
    who2 = {"id": 42}
    app2 = build(graph2, who2)
    c2 = app2.test_client()
    # 302's FA is one combined record: it cannot sign, and the screen says why
    r = c2.post("/api/snapshots/generate", data={"entity": "302", "file": (io.BytesIO(pdf302), "s.pdf")})
    rid2 = r.json["id"]
    v = c2.get("/api/snapshots/" + rid2).json
    assert v["problems"] and "two people" in v["problems"][0], v["problems"]
    r = c2.post("/api/snapshots/%s/send" % rid2, json={})
    assert r.status_code == 400 and "no FA assigned" in r.json["error"], r.json
    # fix the data (split into one person) and it goes through
    with app2.app_context():
        wm = app2.extensions["sqlalchemy"]
        U = [m for m in wm.Model.registry._class_registry.values() if getattr(m, "__tablename__", "") == "users"][0]
        BA = [m for m in wm.Model.registry._class_registry.values() if getattr(m, "__tablename__", "") == "building_assignments"][0]
        wm.session.add(U(id=101, name="Jennifer Murman"))
        for a in wm.session.query(BA).filter_by(entity_code="302", role="fa").all():
            a.user_id = 101
        wm.session.commit()
    who2["id"] = 101
    confirm_all(lambda u: c2.get(u).json, lambda u, b: c2.post(u, json=b), rid2)
    assert c2.post("/api/snapshots/%s/send" % rid2, json={}).status_code == 200
    who2["id"] = 17
    assert c2.post("/api/snapshots/%s/sign" % rid2, json={"role": "pm", "decision": "approve"}).status_code == 200
    who2["id"] = 101
    assert c2.post("/api/snapshots/%s/sign" % rid2, json={"role": "fa", "decision": "approve"}).status_code == 200
    v = c2.get("/api/snapshots/" + rid2).json
    assert v["stage"] == "released" and v["released"]["dry_run"] is False
    assert len(graph2.files) == 2 and all(k.endswith("Monthly Financial Snapshot August 2026.pdf") for k in graph2.files)
    assert any("/Monthly Financials/2026/08.2026/" in k for k in graph2.files), list(graph2.files)
    assert all(b[:4] == b"%PDF" for b in graph2.files.values())
    del os.environ["SNAPSHOT_RELEASE_ENABLED"]

    # picker-only identity: signing is refused until a secure sign-in exists
    who3 = {"id": 2}
    app3 = build(FakeGraph([]), who3, strong=False)
    c3 = app3.test_client()
    with app3.test_request_context():
        pass
    r = c3.post("/api/snapshots/204-2026-08/sign", json={"role": "pm", "decision": "approve"})
    assert r.status_code == 403 and "switched off" in r.json["error"], (r.status_code, r.json)
    print("snapshot db: all tests passed")


if __name__ == "__main__":
    run()
