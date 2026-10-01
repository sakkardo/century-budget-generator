"""Test Microsoft sign-in for snapshot signers with a fake MSAL (no network).
Run: python budget_app/test_snapshot_auth.py
"""
import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
TENANT = "11111111-2222-3333-4444-555555555555"
os.environ.update(AZURE_TENANT_ID=TENANT, AZURE_CLIENT_ID="cid", AZURE_CLIENT_SECRET="server-only-secret")
os.environ.pop("RAILWAY_PUBLIC_DOMAIN", None)

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from itsdangerous import URLSafeTimedSerializer

import snapshot_auth
import snapshot_db

SAMPLES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tasks", "snapshot_samples")


class FakeMsal:
    """Stands in for msal.ConfidentialClientApplication."""
    next_claims = {}

    def initiate_auth_code_flow(self, scopes, redirect_uri):
        assert scopes == ["User.Read"] and redirect_uri.endswith("/auth/snapshot/callback")
        return {"auth_uri": "https://login.microsoftonline.com/fake?state=abc", "state": "abc"}

    def acquire_token_by_auth_code_flow(self, flow, args):
        if args.get("state") != flow["state"]:
            raise ValueError("state mismatch")
        return {"id_token_claims": dict(FakeMsal.next_claims)}


def build():
    app = Flask(__name__)
    app.config.update(SQLALCHEMY_DATABASE_URI="sqlite://", SECRET_KEY="century-budget-dev-key")
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

    bp, _, _ = snapshot_db.create_snapshot_blueprint(
        db, {"User": User, "BuildingAssignment": BuildingAssignment},
        buildings_fn=lambda: [{"entity_code": "204", "building_name": "444 East 86th Owners Corp"}],
        msal_factory=FakeMsal)
    app.register_blueprint(bp)
    with app.app_context():
        db.create_all()
        db.session.add_all([User(id=2, name="Kristy Paxinos", email="kpaxinos@centuryny.com"),
                            User(id=8, name="Jacob Sirotkin", email="JSirotkin@centuryny.com")])
        db.session.add_all([BuildingAssignment(entity_code="204", user_id=2, role="fa"),
                            BuildingAssignment(entity_code="204", user_id=8, role="pm")])
        db.session.commit()
    return app


def sign_in(c, email, tid=TENANT, state="abc"):
    FakeMsal.next_claims = {"tid": tid, "preferred_username": email, "oid": "oid-" + email, "name": email}
    r = c.get("/auth/snapshot/login")
    assert r.status_code == 302 and r.headers["Location"].startswith("https://login.microsoftonline.com/")
    return c.get("/auth/snapshot/callback?code=x&state=" + state)


def run():
    app = build()
    c = app.test_client()
    m = c.get("/api/snapshots/meta").json
    assert m["me"] is None and m["signin_url"] == "/auth/snapshot/login" and m["signing_allowed"] is True

    # bad state, other tenant, unknown person: no identity
    assert "signin=failed" in sign_in(c, "kpaxinos@centuryny.com", state="evil").headers["Location"]
    assert c.get("/api/snapshots/meta").json["me"] is None
    assert "signin=failed" in sign_in(c, "kpaxinos@centuryny.com", tid="other-tenant").headers["Location"]
    r = sign_in(c, "stranger@centuryny.com")
    assert "signin=unknown" in r.headers["Location"] and "stranger" not in r.headers["Location"]
    assert c.get("/api/snapshots/meta").json["me"] is None

    # callback without a login first (no handshake cookie) is refused
    c2 = app.test_client()
    assert "signin=expired" in c2.get("/auth/snapshot/callback?code=x&state=abc").headers["Location"]

    # a cookie forged with the app's default SECRET_KEY is ignored
    forged = URLSafeTimedSerializer("century-budget-dev-key", salt="snapshot-signin").dumps({"uid": 8})
    c2.set_cookie(snapshot_auth.COOKIE, forged)
    assert c2.get("/api/snapshots/meta").json["me"] is None
    # the old name-picker cookie and ?as= are ignored too
    c2.set_cookie("century_fa_id", "8")
    assert c2.get("/api/snapshots/meta?as=8").json["me"] is None

    # Kristy signs in (email match is case-insensitive) and the cookie is http-only
    r = sign_in(c, "KPaxinos@CenturyNY.com")
    assert r.headers["Location"] == "/snapshots"
    sc = [h for h in r.headers.getlist("Set-Cookie") if h.startswith(snapshot_auth.COOKIE + "=")]
    assert sc and "HttpOnly" in sc[0]
    assert c.get("/api/snapshots/meta?as=8").json["me"]["name"] == "Kristy Paxinos"  # ?as= cannot override

    pdf = open(os.path.join(SAMPLES, "204_2026-08_statement.pdf"), "rb").read()
    rid = c.post("/api/snapshots/generate", data={"entity": "204", "file": (io.BytesIO(pdf), "s.pdf")}).json["id"]
    assert c.post("/api/snapshots/%s/send" % rid, json={}).status_code == 200
    # Kristy cannot sign as PM
    assert c.post("/api/snapshots/%s/sign" % rid, json={"role": "pm", "decision": "approve"}).status_code == 400

    # Jacob signs in on his own browser and signs as PM; his Microsoft email is kept on the signature
    j = app.test_client()
    sign_in(j, "jsirotkin@centuryny.com")
    assert j.post("/api/snapshots/%s/sign" % rid, json={"role": "pm", "decision": "approve", "note": "ok"}).status_code == 200
    v = j.get("/api/snapshots/" + rid).json
    pm = [s for s in v["signoffs"] if s["role"] == "pm"][0]
    assert pm["via"]["email"] == "jsirotkin@centuryny.com" and pm["name"] == "Jacob Sirotkin"

    # Kristy signs as FA: approved; release has no SharePoint here, so it refuses cleanly and nothing is half-done
    r = c.post("/api/snapshots/%s/sign" % rid, json={"role": "fa", "decision": "approve"})
    assert r.status_code == 400 and "SharePoint is not connected" in r.json["error"], r.json
    assert c.get("/api/snapshots/" + rid).json["stage"] == "in_signoff"

    # sign out clears the identity
    c.get("/auth/snapshot/logout")
    assert c.get("/api/snapshots/meta").json["me"] is None
    print("snapshot auth: all tests passed")


if __name__ == "__main__":
    run()
