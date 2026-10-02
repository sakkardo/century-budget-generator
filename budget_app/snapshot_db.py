"""Monthly Financial Snapshot: database-backed pieces for the Budget Generator.

create_snapshot_blueprint(db, workflow_models, ...) follows the same factory pattern as the other
blueprints. The workflow rules live in snapshot_service.py / snapshot_signoff.py and are shared with
the click-through; this module supplies the production parts:

- SnapshotRecord table (additive; created by the app's db.create_all()), one row per building-month
- DbStore: same get / mutate / summaries / reset methods as the JSON store, with a row lock
- DbDirectory: building teams from users + building_assignments (synced from Monday)
- identity: who is acting (see the warning on IDENTITY below)
- SharePointReleaser: copy-only release through Microsoft Graph, OFF unless SNAPSHOT_RELEASE_ENABLED=1

IDENTITY: the app's `century_fa_id` cookie is set by POST /api/whoami for ANY user id with no check,
so it proves nothing about who is clicking. Signing and releasing are therefore refused unless
SNAPSHOT_ALLOW_PICKER_SIGNING=1 (pilot, trusted team only) or a stronger identity function is supplied.
"""
import json
import os
import re
import threading
from datetime import datetime

from sqlalchemy.exc import IntegrityError

try:
    import snapshot_auth
    import snapshot_mail
    import snapshot_routes
    import snapshot_service
except ImportError:
    from budget_app import snapshot_auth, snapshot_mail, snapshot_routes, snapshot_service


def _env_on(name):
    return (os.environ.get(name) or "").strip() in ("1", "true", "yes")


MONTH_NAMES = ["january", "february", "march", "april", "may", "june", "july", "august",
               "september", "october", "november", "december"]


def month_of_folder(name):
    """Month (1-12) a Monthly Financials folder stands for, or None.

    Real spellings across the library (2026-10-02 survey of 125 buildings): '08 - August', '08.2026',
    '08-August 2026', '08-2026', '8-2026', 'May', '08 August 2026', '04 - Apri' (typo), '8 -2026'.
    A leading 1-2 digit number wins; otherwise a month name (3+ letters) anywhere in the name.
    Four-digit years are never read as months. 'Prior Management' and the like return None.
    """
    by_name = None
    for word in re.findall(r"[A-Za-z]{3,}", name):
        w = word.lower()
        for i, full in enumerate(MONTH_NAMES):
            if full.startswith(w):  # 'Apri', 'Sept', 'August'; not 'Mayor'
                by_name = i + 1
                break
        if by_name:
            break
    if by_name:
        return by_name  # a written month beats a typo'd number ('01-February 2026' is February)
    m = re.match(r"^\s*(\d{1,2})(?!\d)", name)
    if m and 1 <= int(m.group(1)) <= 12:
        return int(m.group(1))
    return None


def folder_year(name):
    m = re.search(r"(?<!\d)((?:19|20)\d\d)(?!\d)", name)
    return int(m.group(1)) if m else None


def month_folder_like(siblings, month, year):
    """Name a new month folder in the same style as the building's existing ones."""
    def consistent(n):  # a sibling whose leading number agrees with its month (skip typos like '01-February')
        lead = re.match(r"^\s*(\d{1,2})(?!\d)", n)
        return not lead or int(lead.group(1)) == month_of_folder(n)
    # prefer a consistent sibling from Jan-Sep: only a one-digit month shows whether the building zero-pads
    for sib in sorted(siblings, key=lambda n: (not consistent(n), (month_of_folder(n) or 0) >= 10)):
        m0 = month_of_folder(sib)
        if not m0:
            continue
        out = sib
        full0 = MONTH_NAMES[m0 - 1]
        full = MONTH_NAMES[month - 1].capitalize()
        pad = m0 >= 10 or bool(re.match(r"^\s*0\d", sib))  # '12 - December' alone: assume zero-padded
        out = re.sub(r"^(\s*)\d{1,2}(?!\d)", lambda g: g.group(1) + ("%02d" % month if pad else str(month)), out)
        out = re.sub(full0[:3] + r"[a-z]*", full, out, flags=re.I)
        out = re.sub(r"(19|20)\d\d", str(year), out)
        if month_of_folder(out) == month:
            return out
    return "%02d - %s" % (month, MONTH_NAMES[month - 1].capitalize())


class SharePointReleaser:
    """Save the final PDF into the building's own Monthly Financials month folder. Never overwrites.
    Dry run (reads folders, writes nothing) unless SNAPSHOT_RELEASE_ENABLED=1. SNAPSHOT_RELEASE_ROOT writes
    under a sandbox folder for testing.

    Building layouts (2026-10-02 survey of 125): <bldg>/Monthly Financials/<yyyy>/<month> (most),
    <bldg>/<yyyy>/<month> (206 and 17 others), month folders straight under the building (939, 944),
    and statement folders spelled 'Monthly financials' / 'Monthly FInancials' / 'Monthly Financial Reports'
    / 'Monthly Financial Statements'. A building with none of these is refused with a clear message.
    """

    CENTRAL = "01 - Accounting General/Monthly Financial Snapshots"

    def __init__(self, graph, enabled=None):
        # graph: object with list_children(path) -> [{"name","folder"}] , exists(path) -> bool,
        #        put_new(path, bytes) -> None (raises if the file already exists)
        self.graph = graph
        self.enabled = _env_on("SNAPSHOT_RELEASE_ENABLED") if enabled is None else enabled

    @property
    def dry_run(self):
        return not self.enabled

    def _children(self, path):
        try:
            return self.graph.list_children(path)
        except RuntimeError as e:
            if "404" in str(e):
                return []
            raise

    def _building_folder(self, entity):
        for c in self._children(""):
            if c.get("folder") and c["name"].startswith(entity + " - "):
                return c["name"]
        raise ValueError("No SharePoint folder found for building %s." % entity)

    def _month_dir(self, bfolder, year):
        """(folder that holds this year's month folders, [month folder names], prior-year names for styling)."""
        kids = [c["name"] for c in self._children(bfolder) if c.get("folder")]
        containers = [bfolder + "/" + n for n in kids if re.match(r"^\s*monthly\s+financial", n, re.I)] + [bfolder]
        for cont in containers:
            sub_kids = [c["name"] for c in self._children(cont) if c.get("folder")] if cont != bfolder else kids
            if str(year) in sub_kids:
                ydir = "%s/%d" % (cont, year)
                months = [c["name"] for c in self._children(ydir) if c.get("folder")]
                prior = [c["name"] for c in self._children("%s/%d" % (cont, year - 1)) if c.get("folder")] if str(year - 1) in sub_kids else []
                return ydir, months, prior
            direct = [n for n in sub_kids if month_of_folder(n) and folder_year(n) == year]
            if direct:  # month folders sit right here, each carrying its year ('08-2026')
                prior = [n for n in sub_kids if month_of_folder(n) and folder_year(n) == year - 1]
                return cont, direct, prior
        for cont in containers[:-1]:  # a statement folder exists but this year has not started yet
            sub_kids = [c["name"] for c in self._children(cont) if c.get("folder")]
            if any(re.match(r"^(19|20)\d\d$", n) for n in sub_kids):
                prior = [c["name"] for c in self._children("%s/%d" % (cont, year - 1)) if c.get("folder")] if str(year - 1) in sub_kids else []
                return "%s/%d" % (cont, year), [], prior
        raise ValueError("Building folder \"%s\" has no Monthly Financials, year or month folders, "
                         "so there is no place to put the snapshot. Nothing was copied." % bfolder)

    def _month_folder(self, mdir, months, prior, year, month):
        hits = [n for n in months if month_of_folder(n) == month]
        if len(hits) > 1:
            # duplicates exist in the real library ('03 - March' and '03-March'): use the one already in use
            used = [n for n in hits if any(not c.get("folder") for c in self._children("%s/%s" % (mdir, n)))]
            if len(used) == 1:
                return used[0], False
            raise ValueError("More than one folder for %s %d in %s: %s. Nothing was copied." % (
                MONTH_NAMES[month - 1].capitalize(), year, mdir, ", ".join(hits)))
        if hits:
            return hits[0], False
        return month_folder_like(months or prior, month, year), True

    def plan(self, name, entity, year, month):
        """Where the PDF goes (the building's month folder only, Jacob 2026-10-02). Read-only.
        Existing files, vendor snapshots included, stay as they are; only an identical name stops the save."""
        bfolder = self._building_folder(entity)
        mdir, months, prior = self._month_dir(bfolder, year)
        mf, new_folder = self._month_folder(mdir, months, prior, year, month)
        target = "%s/%s/%s" % (mdir, mf, name)
        root = (os.environ.get("SNAPSHOT_RELEASE_ROOT") or "").strip().strip("/")
        if root:  # testing: same path, written under a sandbox folder instead of the real building folder
            target = "%s/%s" % (root, target)
        return {"targets": [target], "new_month_folder": "%s/%s" % (mdir, mf) if new_folder else None,
                "blockers": [], "sandbox": root or None}

    def release(self, pdf, name, entity, client, year, month):
        plan = self.plan(name, entity, year, month)
        for t in plan["targets"]:
            if self.graph.exists(t):
                raise ValueError("A file with this name already exists at %s. Nothing was overwritten." % t)
        if self.dry_run:
            return plan["targets"]
        for t in plan["targets"]:
            self.graph.put_new(t, pdf)  # Graph creates a missing month folder on upload
        return plan["targets"]


class AppGraph:
    """Adapter over the app's own Graph helpers (app-only token, default document library)."""

    def __init__(self, graph_get, drive_id, token):
        self._get, self._drive_id, self._token = graph_get, drive_id, token

    def _root(self, path):
        import urllib.parse
        return "drives/%s/root:/%s" % (self._drive_id(), urllib.parse.quote(path, safe="/")) if path else "drives/%s/root" % self._drive_id()

    def list_children(self, path):
        base = self._root(path) + (":/children" if path else "/children")
        out, url = [], None
        data = self._get(base, params={"$top": "999", "$select": "name,folder"})
        while True:
            out += [{"name": i["name"], "folder": "folder" in i} for i in data.get("value", [])]
            url = data.get("@odata.nextLink")
            if not url:
                return out
            data = self._get(url.split("/v1.0/", 1)[1])

    def exists(self, path):
        try:
            self._get(self._root(path))
            return True
        except RuntimeError as e:
            if "Graph 404" in str(e):
                return False
            raise

    def put_new(self, path, data):
        """Create the file; Graph refuses (409) if it already exists, so nothing is ever replaced."""
        import urllib.error
        import urllib.request
        url = "https://graph.microsoft.com/v1.0/%s:/content?@microsoft.graph.conflictBehavior=fail" % self._root(path)
        req = urllib.request.Request(url, data=data, method="PUT", headers={
            "Authorization": "Bearer " + self._token(), "Content-Type": "application/pdf"})
        try:
            urllib.request.urlopen(req, timeout=120).read()
        except urllib.error.HTTPError as e:
            if e.code == 409:
                raise ValueError("A file with this name already exists at %s. Nothing was overwritten." % path)
            raise RuntimeError("Graph %s on PUT %s" % (e.code, path))


def create_snapshot_blueprint(db, workflow_models, buildings_fn=None, graph=None, identity_fn=None, msal_factory=None,
                              token_fn=None, transport=None):
    """buildings_fn() -> [{"entity_code","building_name"}]; graph: see SharePointReleaser;
    identity_fn() -> user id or None (defaults to the signed century_fa_id cookie)."""

    class SnapshotRecord(db.Model):
        __tablename__ = "snapshot_records"
        id = db.Column(db.String(40), primary_key=True)
        entity_code = db.Column(db.String(50), nullable=False, index=True)
        year = db.Column(db.Integer, nullable=False)
        month = db.Column(db.Integer, nullable=False)
        client = db.Column(db.String(255))
        stage = db.Column(db.String(30), nullable=False, default="draft")
        version = db.Column(db.Integer, nullable=False, default=1)
        data = db.Column(db.Text, nullable=False)
        updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    class DbStore:
        def __init__(self):
            self.lock = threading.RLock()

        def get(self, rid):
            row = db.session.get(SnapshotRecord, rid)
            return json.loads(row.data) if row else None

        def summaries(self):
            rows = db.session.query(SnapshotRecord).all()
            return [{"id": r.id, "entity": r.entity_code, "client": r.client, "month": r.month, "year": r.year,
                     "stage": r.stage, "version": r.version} for r in rows]

        def mutate(self, rid, fn):
            """Read-modify-write under a row lock so two workers cannot sign the same version at once."""
            for attempt in (1, 2):
                try:
                    row = db.session.query(SnapshotRecord).filter_by(id=rid).with_for_update().first()
                    rec, result = fn(json.loads(row.data) if row else None)
                    if row is None:
                        row = SnapshotRecord(id=rid)
                        db.session.add(row)
                    row.entity_code, row.year, row.month, row.client = rec["entity"], rec["year"], rec["month"], rec["client"]
                    row.stage, row.version = rec.get("stage", "draft"), rec["versions"][-1]["n"]
                    row.data = json.dumps(rec)
                    db.session.commit()
                    return result
                except IntegrityError:
                    db.session.rollback()  # two first-time generates raced; retry sees the row
                    if attempt == 2:
                        raise
                except Exception:
                    db.session.rollback()
                    raise

        def reset(self):
            raise ValueError("Reset is only available in the click-through.")

    class DbDirectory:
        def __init__(self):
            self._problems = {}

        def _assignments(self, entity):
            BA, User = workflow_models["BuildingAssignment"], workflow_models["User"]
            rows = (db.session.query(BA, User).join(User, User.id == BA.user_id)
                    .filter(BA.entity_code == entity).all())
            return rows

        def known(self, entity):
            if buildings_fn:
                return any(str(b.get("entity_code")) == entity for b in buildings_fn())
            return bool(self._assignments(entity))

        def team(self, entity):
            out, probs = [], []
            for a, u in self._assignments(entity):
                if "," in (u.name or ""):
                    # one shared record for two people cannot sign for either of them
                    probs.append("The %s record \"%s\" is two people. Split it into one record each in Monday/Users." % (a.role.upper(), u.name))
                    continue
                out.append({"user_id": u.id, "name": u.name, "role": a.role})
            self._problems[entity] = probs
            return out

        def problems(self, entity):
            self.team(entity)
            return self._problems.get(entity, [])

        def client(self, entity):
            if buildings_fn:
                for b in buildings_fn():
                    if str(b.get("entity_code")) == entity:
                        return b.get("building_name") or entity
            return entity

        def name(self, uid):
            u = db.session.get(workflow_models["User"], uid)
            return u.name if u else "Unknown"

        def email(self, uid):
            u = db.session.get(workflow_models["User"], uid)
            if not u or "," in (u.name or ""):
                return None  # a shared record for two people has no one person's mailbox
            return (u.email or "").strip() or None

        def entities_for(self, uid):
            BA = workflow_models["BuildingAssignment"]
            return sorted({a.entity_code for a in db.session.query(BA).filter(BA.user_id == uid).all()})

        def users(self):
            return []

        def buildings(self):
            if not buildings_fn:
                return []
            return sorted(({"entity": str(b["entity_code"]), "client": b.get("building_name") or "", "team": []}
                           for b in buildings_fn() if b.get("entity_code")), key=lambda b: b["entity"])

    def default_identity():
        from flask import current_app, request
        from itsdangerous import BadSignature, URLSafeSerializer
        raw = request.cookies.get("century_fa_id")
        if not raw:
            return None
        try:
            uid = int(URLSafeSerializer(current_app.config.get("SECRET_KEY", "century-budget-dev-key"), salt="century-fa-id").loads(raw))
        except (BadSignature, ValueError, TypeError):
            return None
        return uid if db.session.get(workflow_models["User"], uid) else None

    store = DbStore()
    directory = DbDirectory()
    releaser = SharePointReleaser(graph) if graph else _NoGraph()
    if transport is None and token_fn is not None:
        transport = snapshot_mail.GraphTransport(token_fn)
    service = snapshot_service.Service(store, releaser, directory, mailer=snapshot_mail.Mailer(transport))
    def find_user_by_email(email):
        User = workflow_models["User"]
        u = db.session.query(User).filter(db.func.lower(User.email) == email.lower()).first()
        return u.id if u else None

    auth_bp, detail = None, None
    if identity_fn is None and (msal_factory is not None or snapshot_auth.configured()):
        auth_bp, identity_fn, detail = snapshot_auth.create_auth(find_user_by_email, msal_factory=msal_factory)
    strong = identity_fn is not None
    identity = identity_fn or default_identity

    def signing_allowed():
        return strong or _env_on("SNAPSHOT_ALLOW_PICKER_SIGNING")

    bp = snapshot_routes.create_blueprint(service, identity=identity, dev=False, signing_allowed=signing_allowed,
                                          identity_detail=detail, signin_url="/auth/snapshot/login" if auth_bp else None,
                                          admin_key=lambda: os.environ.get("ADMIN_KEY", ""))
    if auth_bp is not None:
        bp.register_blueprint(auth_bp)
    return bp, {"SnapshotRecord": SnapshotRecord}, {"service": service, "store": store, "directory": directory}


class _NoGraph:
    dry_run = True

    def release(self, *a, **k):
        raise ValueError("SharePoint is not connected, so nothing can be saved.")
