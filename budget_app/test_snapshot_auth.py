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
from test_snapshot_flow import confirm_all

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

    bp, _, helpers = snapshot_db.create_snapshot_blueprint(
        db, {"User": User, "BuildingAssignment": BuildingAssignment},
        buildings_fn=lambda: [{"entity_code": "204", "building_name": "444 East 86th Owners Corp"}],
        msal_factory=FakeMsal)
    app.register_blueprint(bp)
    app.snapshot_helpers = helpers
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

    # ---- the walkthrough page: any Century account may read it; it never grants portal access
    v = app.test_client()
    r = v.get("/snapshots/how-it-works")
    assert r.status_code == 302 and r.headers["Location"] == "/auth/snapshot/login?next=/snapshots/how-it-works"
    FakeMsal.next_claims = {"tid": TENANT, "preferred_username": "teammate@centuryny.com", "oid": "o-t", "name": "Teammate"}
    assert v.get("/auth/snapshot/login?next=/snapshots/how-it-works").status_code == 302
    r = v.get("/auth/snapshot/callback?code=x&state=abc")
    assert r.headers["Location"] == "/snapshots/how-it-works", r.headers["Location"]  # back to the page, no users row needed
    page = v.get("/snapshots/how-it-works")
    assert page.status_code == 200 and b"<title>Monthly Snapshot Walkthrough</title>" in page.data and page.data.startswith(b"<!doctype html>")
    assert v.get("/api/snapshots/meta").json["me"] is None                     # a viewer is not a portal user
    assert v.get("/api/snapshots").status_code == 401
    # another tenant cannot view it
    o = app.test_client()
    FakeMsal.next_claims = {"tid": "other-tenant", "preferred_username": "x@evil.com", "oid": "o-x", "name": "X"}
    o.get("/auth/snapshot/login?next=/snapshots/how-it-works")
    assert "signin=failed" in o.get("/auth/snapshot/callback?code=x&state=abc").headers["Location"]
    assert o.get("/snapshots/how-it-works").status_code == 302
    # next= only accepts known pages (no open redirect); an outsider without next still gets "unknown"
    e = app.test_client()
    FakeMsal.next_claims = {"tid": TENANT, "preferred_username": "teammate@centuryny.com", "oid": "o-t", "name": "Teammate"}
    e.get("/auth/snapshot/login?next=https://evil.example/")
    assert "signin=unknown" in e.get("/auth/snapshot/callback?code=x&state=abc").headers["Location"]

    # ---- the short FA address: "/" opens the portal; sign-in comes back to the address it started on
    os.environ["RAILWAY_PUBLIC_DOMAIN"] = "century-budget-generator-production.up.railway.app"
    try:
        s = app.test_client()
        r = s.get("/", base_url="https://century-snapshots.up.railway.app")
        assert r.status_code == 302 and r.headers["Location"] == "/snapshots", r.headers.get("Location")
        with app.test_request_context("/auth/snapshot/login", base_url="https://century-snapshots.up.railway.app"):
            assert snapshot_auth._redirect_uri() == "https://century-snapshots.up.railway.app/auth/snapshot/callback"
        with app.test_request_context("/auth/snapshot/login", base_url="https://century-budget-generator-production.up.railway.app"):
            assert snapshot_auth._redirect_uri() == "https://century-budget-generator-production.up.railway.app/auth/snapshot/callback"
        with app.test_request_context("/auth/snapshot/login", base_url="https://evil.example"):  # spoofed host: fall back
            assert snapshot_auth._redirect_uri() == "https://century-budget-generator-production.up.railway.app/auth/snapshot/callback"
    finally:
        os.environ.pop("RAILWAY_PUBLIC_DOMAIN", None)

    # Kristy signs in (email match is case-insensitive) and the cookie is http-only
    r = sign_in(c, "KPaxinos@CenturyNY.com")
    assert r.headers["Location"] == "/snapshots"
    sc = [h for h in r.headers.getlist("Set-Cookie") if h.startswith(snapshot_auth.COOKIE + "=")]
    assert sc and "HttpOnly" in sc[0]
    assert c.get("/api/snapshots/meta?as=8").json["me"]["name"] == "Kristy Paxinos"  # ?as= cannot override

    pdf = open(os.path.join(SAMPLES, "204_2026-08_statement.pdf"), "rb").read()
    rid = c.post("/api/snapshots/generate", data={"entity": "204", "file": (io.BytesIO(pdf), "s.pdf")}).json["id"]
    confirm_all(lambda u: c.get(u).json, lambda u, b: c.post(u, json=b), rid)
    # Kristy confirms and sends; her Microsoft identity is kept on the FA confirmation
    assert c.post("/api/snapshots/%s/send" % rid, json={}).status_code == 200
    v = c.get("/api/snapshots/" + rid).json
    fa = [s for s in v["signoffs"] if s["role"] == "fa"][0]
    assert fa["via"]["email"] == "kpaxinos@centuryny.com" and fa["name"] == "Kristy Paxinos" and v["stage"] == "awaiting_pm"

    # Jacob (PM) is signed in elsewhere, but the portal gives him nothing to sign: the PM confirms from the email
    j = app.test_client()
    sign_in(j, "jsirotkin@centuryny.com")
    assert j.post("/api/snapshots/%s/sign" % rid, json={"role": "pm", "decision": "approve"}).status_code in (404, 405)
    import re
    outbox = app.snapshot_helpers["service"].mailer.outbox
    assert [a.lower() for a in outbox[-1]["intended"]] == ["jsirotkin@centuryny.com"] and outbox[-1]["status"] == "off", outbox[-1]["intended"]  # off by default
    link = re.search(r'(/snapshot/confirm/[A-Za-z0-9-]+/[A-Za-z0-9_-]+)"', outbox[-1]["html"]).group(1)
    assert b"Confirmed" in app.test_client().post(link, data={"decision": "approve"}).data
    v = c.get("/api/snapshots/" + rid).json
    pm = [s for s in v["signoffs"] if s["role"] == "pm"][0]
    assert pm["name"] == "Jacob Sirotkin" and pm["via"]["method"] == "email-link"
    # final even though SharePoint is not connected here; only the save is held, with the reason
    assert v["stage"] == "approved" and v["released"] is None and "SharePoint is not connected" in v["release_blocked"]["reason"], v

    # sign out clears the identity
    c.get("/auth/snapshot/logout")
    assert c.get("/api/snapshots/meta").json["me"] is None
    print("snapshot auth: all tests passed")


if __name__ == "__main__":
    run()
