"""Monthly Financial Snapshot: Microsoft sign-in for signers.

Authorization-code flow against the existing "Century Budget Generator" Azure app (single tenant).
After sign-in the person's Microsoft email is matched to a users row; that user id is the identity the
snapshot rules use. The result is kept in a signed, http-only cookie that expires after 12 hours.

The cookie is NOT signed with the app's SECRET_KEY (unset on Railway, so it falls back to a default
written in the code). It is keyed from AZURE_CLIENT_SECRET, which only the server holds.

Needs one tenant setting (Jacob's click): add this Web redirect URI to the Azure app registration
    https://<public domain>/auth/snapshot/callback

Viewer pages (/snapshots/how-it-works): any account in the Century tenant may read them, even without a users row.
Such a "viewer" cookie carries no user id, so it never grants portal access.
"""
import hashlib
import os

from flask import Blueprint, Response, jsonify, make_response, redirect, request
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

COOKIE = "century_snapshot_signin"
FLOW_COOKIE = "century_snapshot_flow"
NEXT_COOKIE = "century_snapshot_next"
VIEW_PAGES = ("/snapshots/how-it-works",)  # read-only pages any signed-in Century account may open
HERE = os.path.dirname(os.path.abspath(__file__))
MAX_AGE = 12 * 3600
SCOPES = ["User.Read"]


def _key():
    secret = os.environ.get("AZURE_CLIENT_SECRET", "")
    if not secret:
        return None
    return hashlib.sha256(("century-snapshot-signin|" + secret).encode("utf-8")).hexdigest()


def _signer():
    k = _key()
    return URLSafeTimedSerializer(k, salt="snapshot-signin") if k else None


SNAPSHOT_HOST = "century-snapshots.up.railway.app"  # the short address for FAs (2026-10-05); "/" there opens the portal


def _known_hosts():
    """Addresses this app answers on. Sign-in comes back to the one the person started on (its cookies live there).
    Each needs its callback listed on the Azure app registration."""
    hosts = {SNAPSHOT_HOST}
    if os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
        hosts.add(os.environ["RAILWAY_PUBLIC_DOMAIN"])
    hosts.update(h.strip() for h in os.environ.get("SNAPSHOT_HOSTS", "").split(",") if h.strip())
    return hosts


def _redirect_uri():
    dom = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    host = (request.host or "").split(":")[0].lower() if request else ""
    if dom and host in _known_hosts():
        return "https://%s/auth/snapshot/callback" % host  # only known names: a spoofed Host header can't redirect sign-in
    if dom:
        return "https://%s/auth/snapshot/callback" % dom
    return request.url_root.rstrip("/") + "/auth/snapshot/callback"


def _msal_app():
    import msal
    tenant = os.environ["AZURE_TENANT_ID"]
    return msal.ConfidentialClientApplication(
        client_id=os.environ["AZURE_CLIENT_ID"], client_credential=os.environ["AZURE_CLIENT_SECRET"],
        authority="https://login.microsoftonline.com/%s" % tenant)


def configured():
    return all(os.environ.get(k) for k in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"))


def create_auth(find_user_by_email, msal_factory=None):
    """find_user_by_email(email) -> user id or None. Returns (blueprint, identity_fn, detail_fn)."""
    factory = msal_factory or _msal_app
    bp = Blueprint("snapshot_auth", __name__)

    def read_cookie():
        s = _signer()
        raw = request.cookies.get(COOKIE)
        if not (s and raw):
            return None
        try:
            return s.loads(raw, max_age=MAX_AGE)
        except (BadSignature, SignatureExpired):
            return None

    def identity():
        data = read_cookie()
        return int(data["uid"]) if data and data.get("uid") else None

    def detail():
        data = read_cookie() or {}
        return {"email": data.get("email"), "oid": data.get("oid")}

    @bp.route("/auth/snapshot/login")
    def login():
        if not configured() or not _signer():
            return jsonify({"error": "Microsoft sign-in is not configured on this server."}), 503
        flow = factory().initiate_auth_code_flow(SCOPES, redirect_uri=_redirect_uri())
        resp = make_response(redirect(flow["auth_uri"]))
        # the handshake (state, nonce, PKCE verifier) rides in its own short-lived cookie signed with the server-only key
        resp.set_cookie(FLOW_COOKIE, _signer().dumps(flow), max_age=600, httponly=True, samesite="Lax",
                        secure=bool(os.environ.get("RAILWAY_PUBLIC_DOMAIN")))
        nxt = request.args.get("next")
        if nxt in VIEW_PAGES:  # only known pages: never an open redirect
            resp.set_cookie(NEXT_COOKIE, nxt, max_age=600, httponly=True, samesite="Lax",
                            secure=bool(os.environ.get("RAILWAY_PUBLIC_DOMAIN")))
        else:
            resp.set_cookie(NEXT_COOKIE, "", max_age=0)
        return resp

    @bp.route("/auth/snapshot/callback")
    def callback():
        flow = None
        try:
            flow = _signer().loads(request.cookies.get(FLOW_COOKIE, ""), max_age=600) if _signer() else None
        except (BadSignature, SignatureExpired):
            flow = None
        if not flow:
            return redirect("/snapshots?signin=expired")
        try:
            result = factory().acquire_token_by_auth_code_flow(flow, request.args.to_dict())
        except ValueError:  # state mismatch or replay
            return redirect("/snapshots?signin=failed")
        claims = result.get("id_token_claims") or {}
        if "error" in result or not claims:
            return redirect("/snapshots?signin=failed")
        if claims.get("tid") != os.environ.get("AZURE_TENANT_ID"):
            return redirect("/snapshots?signin=failed")
        email = (claims.get("preferred_username") or claims.get("email") or "").strip().lower()
        uid = find_user_by_email(email) if email else None
        dest = request.cookies.get(NEXT_COOKIE)
        dest = dest if dest in VIEW_PAGES else "/snapshots"
        if not uid and email and dest in VIEW_PAGES:
            # a Century account without a budget-app user: may read the viewer page, nothing else
            resp = make_response(redirect(dest))
            resp.set_cookie(FLOW_COOKIE, "", max_age=0)
            resp.set_cookie(NEXT_COOKIE, "", max_age=0)
            resp.set_cookie(COOKIE, _signer().dumps({"viewer": email, "oid": claims.get("oid"), "name": claims.get("name")}),
                            max_age=MAX_AGE, httponly=True, samesite="Lax",
                            secure=request.is_secure or bool(os.environ.get("RAILWAY_PUBLIC_DOMAIN")))
            return resp
        if not uid:
            resp = make_response(redirect("/snapshots?signin=unknown"))
            resp.set_cookie(COOKIE, "", max_age=0)
            resp.set_cookie(FLOW_COOKIE, "", max_age=0)
            return resp
        token = _signer().dumps({"uid": uid, "email": email, "oid": claims.get("oid"), "name": claims.get("name")})
        resp = make_response(redirect(dest))
        resp.set_cookie(FLOW_COOKIE, "", max_age=0)
        resp.set_cookie(NEXT_COOKIE, "", max_age=0)
        resp.set_cookie(COOKIE, token, max_age=MAX_AGE, httponly=True, secure=request.is_secure or bool(os.environ.get("RAILWAY_PUBLIC_DOMAIN")),
                        samesite="Lax")
        return resp

    @bp.before_app_request
    def short_address_home():
        """On the short FA address, the bare link opens the snapshot portal instead of the budget app."""
        if request.path == "/" and (request.host or "").split(":")[0].lower() == SNAPSHOT_HOST:
            return redirect("/snapshots")
        return None

    @bp.route("/snapshots/how-it-works")
    def how_it_works():
        """The animated walkthrough, for Century staff: any account in the Century tenant."""
        data = read_cookie()
        if not (data and (data.get("uid") or data.get("viewer"))):
            if not configured() or not _signer():
                return jsonify({"error": "Microsoft sign-in is not configured on this server."}), 503
            return redirect("/auth/snapshot/login?next=/snapshots/how-it-works")
        with open(os.path.join(HERE, "snapshot_walkthrough.html"), encoding="utf-8") as f:
            body = f.read()
        page = ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
                '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"></head><body>'
                + body + "</body></html>")
        return Response(page, mimetype="text/html", headers={"Cache-Control": "private, no-cache", "X-Robots-Tag": "noindex"})

    @bp.route("/auth/snapshot/logout", methods=["GET", "POST"])
    def logout():
        resp = make_response(redirect("/snapshots"))
        resp.set_cookie(COOKIE, "", max_age=0)
        return resp

    return bp, identity, detail
