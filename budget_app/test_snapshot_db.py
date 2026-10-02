"""Test the database-backed snapshot blueprint (SQLite in memory, fake SharePoint, fake mail transport).
Run: python budget_app/test_snapshot_db.py
"""
import io
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

import snapshot_db
from test_snapshot_flow import confirm_all
from test_snapshot_release import Tree

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tasks", "snapshot_samples")
LINK = re.compile(r'(/snapshot/confirm/[A-Za-z0-9-]+/[A-Za-z0-9_-]+)"')


class FakeTransport:
    """Records what would actually leave the server."""

    def __init__(self):
        self.sent = []

    def send(self, sender, to, cc, subject, body_html, attachments, save_sent=True):
        self.sent.append({"sender": sender, "to": list(to), "cc": list(cc or []), "subject": subject, "html": body_html,
                          "attachments": [a["name"] for a in (attachments or [])]})


def helpers_of(app):
    return app.snapshot_helpers


def build(graph, identity_box, strong=True, transport=None, combined_fa=True):
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
        db, wm, buildings_fn=bf, graph=graph, identity_fn=(lambda: identity_box["id"]) if strong else None,
        transport=transport or FakeTransport())
    app.register_blueprint(bp)
    app.snapshot_helpers = helpers
    app.db, app.User, app.BA = db, User, BuildingAssignment
    with app.app_context():
        db.create_all()
        for uid, name, email in ((2, "Kristy Paxinos", "kpaxinos@centuryny.com"), (8, "Jacob Sirotkin", "jsirotkin@centuryny.com"),
                                 (17, "George Matos", "gmatos@centuryny.com"),
                                 (42, "Jennifer Murman, Giovanni Lizarazo", "jlizarazo@centuryny.com"),
                                 (101, "Jennifer Murman", "jmurman@centuryny.com")):
            db.session.add(User(id=uid, name=name, email=email))
        for e, uid, role in (("204", 2, "fa"), ("204", 8, "pm"), ("302", 42 if combined_fa else 101, "fa"), ("302", 17, "pm")):
            db.session.add(BuildingAssignment(entity_code=e, user_id=uid, role=role))
        db.session.commit()
    return app


def run():
    pdf204 = open(os.path.join(ROOT, "204_2026-08_statement.pdf"), "rb").read()
    pdf302 = open(os.path.join(ROOT, "302_2026-08_statement.pdf"), "rb").read()
    os.environ["SNAPSHOT_EMAIL_MODE"] = "test"
    os.environ["SNAPSHOT_EMAIL_TEST_TO"] = "jsirotkin@centuryny.com"
    os.environ["ADMIN_KEY"] = "k-test"

    tree = Tree(["204 - 444 East 86th Street Owners Corp/Monthly Financials/2026/08 - August",
                 "302 - 205 Water Street Condominium/Monthly Financials/2026/08.2026"])
    who = {"id": 2}
    tr = FakeTransport()
    app = build(tree, who, transport=tr)
    c = app.test_client()
    P = lambda url, body=None: c.post(url, json=body or {})

    # ---- identity comes from the server only; every portal call needs sign-in
    who["id"] = None
    assert c.get("/api/snapshots/meta?as=2").json["me"] is None
    assert c.get("/api/snapshots").status_code == 401
    r = c.post("/api/snapshots/generate", data={"entity": "204", "as": "2", "file": (io.BytesIO(pdf204), "s.pdf")})
    assert r.status_code == 401
    who["id"] = 2
    m = c.get("/api/snapshots/meta?as=8").json
    assert m["me"]["name"] == "Kristy Paxinos" and m["my_entities"] == ["204"] and m["email_mode"] == "test"
    assert P("/api/snapshots/reset").status_code == 404
    assert c.post("/api/snapshots/generate", data={"entity": "204", "sample": "x.pdf"}).status_code == 400

    # ---- FA generates, confirms, sends: in TEST mode the email goes only to the test inbox
    rid = c.post("/api/snapshots/generate", data={"entity": "204", "file": (io.BytesIO(pdf204), "s.pdf")}).json["id"]
    confirm_all(lambda u: c.get(u).json, lambda u, b: c.post(u, json=b), rid)
    r = P("/api/snapshots/%s/send" % rid)
    assert r.status_code == 200, r.json
    assert len(tr.sent) == 1
    e = tr.sent[0]
    assert e["to"] == ["jsirotkin@centuryny.com"] and e["sender"] == "kpaxinos@centuryny.com" and e["subject"].startswith("[TEST] ")
    assert "would go to: jsirotkin@centuryny.com" in e["html"] and e["attachments"][0].endswith("August 2026.pdf")
    v = c.get("/api/snapshots/%s" % rid).json
    assert v["stage"] == "awaiting_pm" and v["request"]["emails"][-1]["status"] == "test"

    # ---- the PM confirms from the link (no sign-in needed; GET changes nothing)
    link = LINK.search(e["html"]).group(1)
    who["id"] = None
    assert b"Confirm this snapshot" in c.get(link).data
    assert c.get("/api/snapshots/%s" % rid).status_code == 401  # the portal is still closed to them
    r = c.post(link, data={"decision": "approve"})
    assert b"Confirmed" in r.data
    who["id"] = 2
    v = c.get("/api/snapshots/%s" % rid).json
    # saving is switched off here: final, nothing written, the target remembered
    assert v["stage"] == "approved" and v["released"] is None and tree.writes == [], v["stage"]
    assert v["rehearsal"]["files"] == ["204 - 444 East 86th Street Owners Corp/Monthly Financials/2026/08 - August/"
                                        "204 - 444 East 86th Owners Corp Monthly Financial Snapshot August 2026.pdf"], v["rehearsal"]
    assert c.get("/api/snapshots").json[0]["stage"] == "approved"
    # switch saving on: the FA saves the final snapshot; only the month folder gets it
    helpers_of(app)["service"].releaser.enabled = True
    who["id"] = 8
    assert P("/api/snapshots/%s/release" % rid).status_code == 400  # PM cannot (and never needs to)
    who["id"] = 2
    assert P("/api/snapshots/%s/release" % rid).status_code == 200
    v = c.get("/api/snapshots/%s" % rid).json
    assert v["stage"] == "released" and tree.writes == v["released"]["files"] and len(tree.writes) == 1
    assert P("/api/snapshots/%s/release" % rid).status_code == 400  # never twice

    # ---- OFF mode: nothing leaves the server
    os.environ["SNAPSHOT_EMAIL_MODE"] = "off"
    tr2 = FakeTransport()
    tree2 = Tree(["302 - 205 Water Street Condominium/Monthly Financials/2026/08.2026"])
    who2 = {"id": 42}
    app2 = build(tree2, who2, transport=tr2)
    c2 = app2.test_client()
    rid2 = c2.post("/api/snapshots/generate", data={"entity": "302", "file": (io.BytesIO(pdf302), "s.pdf")}).json["id"]
    v = c2.get("/api/snapshots/" + rid2).json
    assert v["problems"] and "two people" in v["problems"][0], v["problems"]
    r = c2.post("/api/snapshots/%s/send" % rid2, json={})
    assert r.status_code == 400 and "no FA assigned" in r.json["error"], r.json
    with app2.app_context():  # split the shared record: one person each
        for a in app2.db.session.query(app2.BA).filter_by(entity_code="302", role="fa").all():
            a.user_id = 101
        app2.db.session.commit()
    who2["id"] = 101
    confirm_all(lambda u: c2.get(u).json, lambda u, b: c2.post(u, json=b), rid2)
    assert c2.post("/api/snapshots/%s/send" % rid2, json={}).status_code == 200
    assert tr2.sent == []  # off: recorded, not sent
    v = c2.get("/api/snapshots/" + rid2).json
    assert v["stage"] == "awaiting_pm" and v["request"]["emails"][-1]["status"] == "off"
    svc2 = helpers_of(app2)["service"]
    assert svc2.mailer.outbox[-1]["intended"] == ["gmatos@centuryny.com"]

    # ---- the timer endpoint needs the admin key
    assert c2.post("/api/snapshots/cron/tick").status_code == 403
    assert c2.post("/api/snapshots/cron/tick", headers={"X-Admin-Key": "wrong"}).status_code == 403
    r = c2.post("/api/snapshots/cron/tick", headers={"X-Admin-Key": "k-test"})
    assert r.status_code == 200 and r.json == {"reminded": [], "escalated": [], "saved": []}, r.json

    # ---- picker-only identity: sending is refused until a secure sign-in exists
    app3 = build(Tree([]), {"id": 2}, strong=False)
    c3 = app3.test_client()
    r = c3.post("/api/snapshots/204-2026-08/send", json={})
    assert r.status_code == 403 and "switched off" in r.json["error"], (r.status_code, r.json)
    for k in ("SNAPSHOT_EMAIL_MODE", "SNAPSHOT_EMAIL_TEST_TO", "ADMIN_KEY"):
        os.environ.pop(k, None)
    print("snapshot db: all tests passed")


if __name__ == "__main__":
    run()
