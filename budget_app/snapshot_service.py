"""Monthly Financial Snapshot: workflow service.

Flow (Jacob, 2026-10-02):
  FA uploads the statement in the portal -> confirms or edits every note -> "Confirm & send to PM"
  -> the PM gets an email (PDF attached) with a one-time link -> PM confirms (or asks for changes)
  -> the snapshot is final and is saved into the building's Monthly Financials month folder.
  48 hours: reminder to the PM at 24h, "overdue" email to the FA (+ Jacob) at 48h; never auto-final.

The rules live in snapshot_signoff.py. Storage, the people directory, the SharePoint releaser and the
mailer are plugged in, so the same service runs the local click-through and production.
Slow work (email, SharePoint) happens AFTER the record is committed, never while it is locked.
"""
import copy
import hashlib
import json
import os
import re
import secrets
import threading
from datetime import datetime, timedelta, timezone

try:
    import snapshot_mail
    import snapshot_parser
    import snapshot_render
    import snapshot_signoff as so
except ImportError:
    from budget_app import snapshot_mail, snapshot_parser, snapshot_render
    from budget_app import snapshot_signoff as so

MONTHS = snapshot_render.MONTH_NAMES
REMIND_AFTER = timedelta(hours=24)
ESCALATE_AFTER = timedelta(hours=48)
LINK_LIFETIME = timedelta(hours=72)  # 48h window + 24h grace after the escalation


class StaticDirectory:
    """Test people and building teams. The real app uses a directory built from users +
    building_assignments (Monday); anything with these methods can be plugged in."""

    USERS = {
        2: {"name": "Kristy Paxinos", "email": "kpaxinos@centuryny.com"},
        8: {"name": "Jacob Sirotkin", "email": "jsirotkin@centuryny.com"},
        17: {"name": "George Matos", "email": "gmatos@centuryny.com"},
        101: {"name": "Jennifer Murman", "email": "jmurman@centuryny.com"},
        102: {"name": "Giovanni Lizarazo", "email": "glizarazo@centuryny.com"},
    }
    TEAMS = {
        "204": {"client": "444 East 86th Owners Corp", "team": [
            {"user_id": 2, "name": "Kristy Paxinos", "role": "fa"}, {"user_id": 8, "name": "Jacob Sirotkin", "role": "pm"}]},
        "302": {"client": "205 Water Street Condominium", "team": [
            {"user_id": 101, "name": "Jennifer Murman", "role": "fa"}, {"user_id": 102, "name": "Giovanni Lizarazo", "role": "fa"},
            {"user_id": 17, "name": "George Matos", "role": "pm"}]},
        "148": {"client": "130 East 18th Owners Corp", "team": [
            {"user_id": 2, "name": "Kristy Paxinos", "role": "fa"}, {"user_id": 8, "name": "Jacob Sirotkin", "role": "pm"}]},
        "999": {"client": "TEST - one person is FA and PM", "team": [
            {"user_id": 2, "name": "Kristy Paxinos", "role": "fa"}, {"user_id": 2, "name": "Kristy Paxinos", "role": "pm"}]},
    }

    def known(self, entity):
        return entity in self.TEAMS

    def team(self, entity):
        return self.TEAMS[entity]["team"]

    def client(self, entity):
        return self.TEAMS[entity]["client"]

    def name(self, uid):
        return self.USERS.get(uid, {}).get("name", "Unknown")

    def email(self, uid):
        return self.USERS.get(uid, {}).get("email")

    def users(self):
        return [{"id": k, "name": v["name"]} for k, v in self.USERS.items()]

    def buildings(self):
        return [{"entity": k, "client": v["client"], "team": v["team"]} for k, v in self.TEAMS.items()]

    def problems(self, entity):
        return []


class LocalReleaser:
    """Test stand-in for the SharePoint save: the building's month folder under a local root, never overwrite."""

    dry_run = False

    def __init__(self, root):
        self.root = root

    def release(self, pdf, name, entity, client, year, month):
        folder = os.path.join(self.root, "%s - %s" % (entity, client), "Monthly Financials", str(year),
                              "%02d - %s" % (month, MONTHS[month - 1]))
        path = os.path.join(folder, name)
        if os.path.exists(path):
            raise ValueError("A file with this name already exists in %s. Nothing was overwritten." % folder)
        os.makedirs(folder, exist_ok=True)
        with open(path, "wb") as f:
            f.write(pdf)
        return [os.path.relpath(path, self.root).replace(os.sep, "/")]


PLACEHOLDER = re.compile(r"\[[^\]]+\]")


def unconfirmed(commentary):
    return [i for i, c in enumerate(commentary) if not c.get("confirmed")]


def _eastern(dt=None):
    """Wall-clock time in New York (Railway runs in UTC). Falls back to UTC, labelled, if tz data is missing."""
    dt = dt or datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        return dt.astimezone(ZoneInfo("America/New_York")), ""
    except Exception:
        return dt.astimezone(timezone.utc), " UTC"


def label(dt):
    t, suffix = _eastern(dt)
    return t.strftime("%b %d, %Y %I:%M %p").replace(" 0", " ") + suffix


def utcnow():
    return datetime.now(timezone.utc)


def iso_now():
    return utcnow().isoformat()


def now_s():
    return label(utcnow())


def _retire(rec):
    """Remember the fingerprints of links being replaced, so an old link says 'replaced', not 'invalid'."""
    req = rec.get("pm_request")
    if req:
        rec["old_tokens"] = (rec.get("old_tokens", []) + [t["hash"] for t in req["tokens"]])[-60:]


def _hash_token(raw):
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class Store:
    """JSON-file store for the click-through. The database store (snapshot_db.py) has the same four
    methods, so the service never knows which one it is talking to."""

    def __init__(self, path):
        self.path, self.lock = path, threading.RLock()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            self._save({"records": {}})

    def _load(self):
        with open(self.path, encoding="utf-8") as f:
            return json.load(f)

    def _save(self, data):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, self.path)

    def get(self, rid):
        return self._load()["records"].get(rid)

    def mutate(self, rid, fn):
        """fn(rec_or_None) -> (rec, result). Read-modify-write under one lock."""
        with self.lock:
            data = self._load()
            rec, result = fn(data["records"].get(rid))
            data["records"][rid] = rec
            self._save(data)
            return result

    def summaries(self):
        return [{"id": r["id"], "entity": r["entity"], "client": r["client"], "month": r["month"], "year": r["year"],
                 "stage": r.get("stage", "draft"), "version": r["versions"][-1]["n"]} for r in self._load()["records"].values()]

    def reset(self):
        self._save({"records": {}})


class Service:
    def __init__(self, store, releaser, directory, mailer=None, base_url=None, escalate_to=None):
        self.store, self.releaser, self.directory = store, releaser, directory
        self.mailer = mailer or snapshot_mail.Mailer()
        self.base_url = base_url or (lambda: "")
        self._escalate_to = escalate_to

    @property
    def escalate_to(self):
        v = self._escalate_to if self._escalate_to is not None else os.environ.get("SNAPSHOT_ESCALATE_TO", "jsirotkin@centuryny.com")
        return [a.strip() for a in v.split(",") if a.strip()]

    # ------------------------------------------------------------- helpers
    def _team(self, entity):
        return self.directory.team(entity)

    def _cur(self, rec):
        return rec["versions"][-1]

    def _hash(self, v):
        return so.content_hash(v["snapshot"], v["commentary"], v["board_note"])

    def _name(self, uid):
        return self.directory.name(uid)

    def _email(self, uid):
        e = getattr(self.directory, "email", lambda u: None)(uid)
        return (e or "").strip() or None

    def _is_fa(self, rec, uid):
        return uid in [a["user_id"] for a in self._team(rec["entity"]) if a["role"] == "fa"]

    def _approved(self, rec, role, h=None):
        h = h or self._cur(rec)["hash"]
        return [s for s in rec["signoffs"] if s["role"] == role and s["decision"] == "approve" and s["version_hash"] == h]

    def _signoff_block(self, rec):
        out = {}
        team = self._team(rec["entity"])
        for role in ("fa", "pm"):
            sig = self._approved(rec, role)
            if sig:
                out[role] = {"name": self._name(sig[-1]["user_id"]), "signed_at": sig[-1]["at"]}
            else:
                names = [a["name"] for a in team if a["role"] == role]
                out[role] = {"name": " / ".join(names) if names else None}
        return out

    def _request(self, rec):
        """The open PM request, if it is for the current version."""
        req = rec.get("pm_request")
        if req and req["version_hash"] == self._cur(rec)["hash"] and not req.get("closed"):
            return req
        return None

    def stage(self, rec):
        """draft, awaiting_pm, changes_requested, approved (final, not saved yet), released (final and saved)."""
        if rec.get("released"):
            return "released"
        if self._approved(rec, "pm") and self._approved(rec, "fa"):
            return "approved"
        if self._approved(rec, "fa") and self._request(rec):
            return "awaiting_pm"
        req = rec.get("pm_request")
        if req and (req.get("closed") or {}).get("decision") == "request_changes":
            return "changes_requested"  # the latest email was answered with changes and not re-sent yet
        return "draft"

    def render(self, rec):
        v = self._cur(rec)
        stg = self.stage(rec)
        lbl = {"released": "APPROVED", "approved": "APPROVED", "awaiting_pm": "IN REVIEW",
               "changes_requested": "CHANGES REQUESTED"}.get(stg, "DRAFT")
        notes = v["commentary"]
        reviewed = None
        if notes and all(c.get("confirmed") for c in notes):
            reviewed = max((c["confirmed"] for c in notes), key=lambda st: st.get("iso", ""))  # latest confirmation
        return snapshot_render.render_pdf(v["snapshot"], notes, v["board_note"], signoff=self._signoff_block(rec),
                                          status_label=lbl, reviewed=reviewed, resolved=v.get("resolved"))

    def _mutate(self, rid, fn):
        def wrapper(rec):
            if not rec:
                raise ValueError("Snapshot not found.")
            res = fn(rec)
            rec["stage"] = self.stage(rec)
            return rec, res
        return self.store.mutate(rid, wrapper)

    def _not_fa(self, rec, what):
        """Why this person cannot act as FA. Names the data problem when the building has no usable FA."""
        team = self._team(rec["entity"])
        if not any(a["role"] == "fa" for a in team):
            probs = self.directory.problems(rec["entity"])
            return "This building has no FA assigned who can sign. " + " ".join(probs)
        return "Only the building's FA can %s." % what

    def _blocked_text(self, st):
        if st["state"] == "blocked_no_assignment":
            return "This building has no %s assigned in Monday." % " or ".join(r.upper() for r in st["missing_assignments"])
        return "FA and PM must be two different people. Assign a second person in Monday."

    def _month_label(self, rec):
        return "%s %d" % (MONTHS[rec["month"] - 1], rec["year"])

    def _portal_link(self, rec=None):
        return self.base_url().rstrip("/") + "/snapshots"

    def _confirm_link(self, rec, raw):
        return "%s/snapshot/confirm/%s/%s" % (self.base_url().rstrip("/"), rec["id"], raw)

    def _pm_recipients(self, rec):
        """PMs who can receive the email: assigned, a real person record, an email on file."""
        out, missing = [], []
        for a in self._team(rec["entity"]):
            if a["role"] != "pm":
                continue
            e = self._email(a["user_id"])
            (out if e else missing).append({"user_id": a["user_id"], "name": a["name"], "email": e})
        return out, missing

    # ------------------------------------------------------------- generate / edit (FA)
    def generate(self, entity, pdf_bytes, user_id, source):
        if not self.directory.known(entity):
            raise ValueError("Unknown building %s." % entity)
        snap = snapshot_parser.build_snapshot(pdf_bytes)
        m = snap["meta"]
        rid = "%s-%d-%02d" % (entity, m["year"], m["month"])
        prior, prior_month = self._prior_notes(entity, m["year"], m["month"])
        commentary, resolved = snapshot_render.classify_notes(snapshot_render.draft_commentary(snap), prior, prior_month)
        version = {"n": 1, "snapshot": snap, "commentary": commentary, "board_note": "", "acks": {},
                   "resolved": resolved, "by": user_id, "at": now_s(), "source": source}
        version["hash"] = self._hash(version)

        def fn(rec):
            if rec and (rec.get("released") or (self._approved(rec, "pm") and self._approved(rec, "fa"))):
                raise ValueError("This month is already final. Regenerating a final snapshot is not allowed.")
            if rec:
                version["n"] = rec["versions"][-1]["n"] + 1
                rec["versions"].append(version)
                rec["log"].append({"at": now_s(), "who": self._name(user_id), "what": "Regenerated from %s (version %d)" % (source, version["n"])})
            else:
                rec = {"id": rid, "entity": entity, "client": self.directory.client(entity), "month": m["month"], "year": m["year"],
                       "versions": [version], "signoffs": [], "sent": False, "released": None,
                       "log": [{"at": now_s(), "who": self._name(user_id), "what": "Generated from %s" % source}]}
            rec["stage"] = self.stage(rec)
            return rec, None
        self.store.mutate(rid, fn)
        return rid

    def _prior_notes(self, entity, year, month):
        """Confirmed notes from this building's most recent earlier snapshot in the same budget year.
        January (or a building's first month in the system) has none, so every item is new."""
        rows = [r for r in self.store.summaries() if r["entity"] == entity and r["year"] == year and r["month"] < month]
        if not rows:
            return {}, None
        prev = max(rows, key=lambda r: r["month"])
        rec = self.store.get(prev["id"])
        label = MONTHS[prev["month"] - 1]
        notes = {}
        for n in self._cur(rec)["commentary"]:
            if n.get("key") and n.get("confirmed"):
                notes[n["key"]] = dict(n, since=n.get("since") or label)
        return notes, label

    def _confirm_stamp(self, note, user_id, reexplained):
        """Confirm a note. Re-explaining it (new, worse, or rewritten) moves the basis to today's variance;
        confirming a continuing note keeps the basis it was explained at, so slow creep is still caught."""
        note["confirmed"] = {"by": self._name(user_id), "at": now_s(), "iso": iso_now()}
        note["draft"] = False
        if reexplained or "basis" not in note:
            note["basis"] = note.get("variance", 0)
        return note

    def _editable(self, rec):
        if rec.get("released") or self.stage(rec) == "approved":
            raise ValueError("This snapshot is final and cannot be edited.")

    def edit(self, rid, user_id, commentary, board_note):
        def fn(rec):
            self._editable(rec)
            if not self._is_fa(rec, user_id):
                raise ValueError(self._not_fa(rec, "edit the commentary and board note"))
            cur = self._cur(rec)
            if len(commentary) != len(cur["commentary"]):
                raise ValueError("The notes changed since you opened the page. Reload and try again.")
            notes = []
            for c, old in zip(commentary, cur["commentary"]):
                n = copy.deepcopy(old)  # title and confirmation stamps come from the server, never the browser
                text = (c.get("text") or "").strip()
                if text != (old.get("text") or ""):
                    # editing a note is the FA confirming it in their own words
                    if not text and not old.get("facts"):
                        raise ValueError("A note cannot be empty.")
                    if PLACEHOLDER.search(text):
                        raise ValueError("Replace the [bracketed] placeholder in \"%s\" before saving." % old.get("title"))
                    n["text"] = text
                    self._confirm_stamp(n, user_id, reexplained=True)
                notes.append(n)
            if [n.get("text") for n in notes] == [o.get("text") for o in cur["commentary"]] and board_note == cur["board_note"]:
                return
            self._new_version(rec, user_id, notes=notes, board_note=board_note, what="Edited commentary")
        self._mutate(rid, fn)

    def _new_version(self, rec, user_id, notes=None, board_note=None, what="Edited"):
        cur = self._cur(rec)
        v = copy.deepcopy(cur)
        v.update({"n": cur["n"] + 1, "by": user_id, "at": now_s()})
        if notes is not None:
            v["commentary"] = notes
        if board_note is not None:
            v["board_note"] = board_note
        v["hash"] = self._hash(v)
        rec["versions"].append(v)
        stale = len([s for s in rec["signoffs"] if s["version_hash"] == cur["hash"]])
        cancelled = bool(self._request(rec) is None and rec.get("pm_request") and not rec["pm_request"].get("closed")
                         and rec["pm_request"]["version_hash"] == cur["hash"])
        rec["log"].append({"at": now_s(), "who": self._name(user_id), "what": "%s (version %d)%s%s" % (
            what, v["n"], ", %d signature(s) cleared" % stale if stale else "",
            ". The PM's link for the previous version no longer works; send it again" if cancelled else "")})
        return v

    def confirm_note(self, rid, user_id, index, text=None):
        """FA confirms one note, as written or with new text. New text makes a new version (and clears
        signatures, because the board will read different words); confirming unchanged text does not."""
        def fn(rec):
            self._editable(rec)
            if not self._is_fa(rec, user_id):
                raise ValueError(self._not_fa(rec, "confirm the notes"))
            cur = self._cur(rec)
            if not 0 <= index < len(cur["commentary"]):
                raise ValueError("That note no longer exists. Reload the page.")
            note = cur["commentary"][index]
            old_text = note.get("text") or ""
            new_text = old_text if text is None else text.strip()
            if not new_text and not note.get("facts"):
                raise ValueError("A note cannot be empty.")
            if PLACEHOLDER.search(new_text):
                raise ValueError("Replace the [bracketed] placeholder with the real cause before confirming.")
            if new_text == old_text:
                self._confirm_stamp(note, user_id, reexplained=note.get("status") != "continuing")
                rec["log"].append({"at": now_s(), "who": self._name(user_id), "what": 'Confirmed note "%s"' % note["title"]})
                return
            notes = copy.deepcopy(cur["commentary"])
            notes[index]["text"] = new_text
            self._confirm_stamp(notes[index], user_id, reexplained=True)
            self._new_version(rec, user_id, notes=notes, what='Edited and confirmed note "%s"' % note["title"])
        self._mutate(rid, fn)

    def confirm_continuing(self, rid, user_id):
        """One click: confirm every continuing note whose explanation is unchanged (FA only)."""
        def fn(rec):
            self._editable(rec)
            if not self._is_fa(rec, user_id):
                raise ValueError(self._not_fa(rec, "confirm the notes"))
            todo = [n for n in self._cur(rec)["commentary"] if n.get("status") == "continuing" and not n.get("confirmed")]
            if not todo:
                raise ValueError("There are no continuing notes left to confirm.")
            for n in todo:
                self._confirm_stamp(n, user_id, reexplained=False)
            rec["log"].append({"at": now_s(), "who": self._name(user_id), "what": "Confirmed %d continuing note(s): %s" % (
                len(todo), ", ".join(n.get("label") or n["title"] for n in todo))})
            return len(todo)
        return self._mutate(rid, fn)

    def acknowledge(self, rid, user_id, check_id, note):
        def fn(rec):
            if not self._is_fa(rec, user_id):
                raise ValueError(self._not_fa(rec, "add this note"))
            self._cur(rec)["acks"][check_id] = (note or "").strip()
            rec["log"].append({"at": now_s(), "who": self._name(user_id), "what": "Added note on a review check"})
        self._mutate(rid, fn)

    # ------------------------------------------------------------- FA: confirm & send to PM
    def send_to_pm(self, rid, user_id, via=None):
        """FA confirms the snapshot and the PM is emailed a one-time link. Also used to resend."""
        def fn(rec):
            self._editable(rec)
            if not self._is_fa(rec, user_id):
                raise ValueError(self._not_fa(rec, "send it to the PM"))
            v = self._cur(rec)
            ok, why = so.readiness(v["snapshot"], v["acks"])
            if not ok:
                raise ValueError(" ".join(why))
            st = so.status(rec["signoffs"], self._team(rec["entity"]), v["hash"])
            if st["state"] in ("blocked_no_assignment", "blocked_needs_second_person"):
                raise ValueError(self._blocked_text(st))  # a team problem first: confirming notes cannot fix it
            left = unconfirmed(v["commentary"])
            if left:
                raise ValueError("Confirm or edit every note first (%d of %d still to confirm)." % (len(left), len(v["commentary"])))
            pms, missing = self._pm_recipients(rec)
            if not pms:
                raise ValueError("No PM email on file for %s (%s). Add it before sending." % (
                    rec["entity"], ", ".join(m["name"] for m in missing) or "no PM assigned"))
            if not self._email(user_id):
                raise ValueError("No email on file for you, so the PM's email cannot be sent from your mailbox.")
            resend = bool(self._approved(rec, "fa"))
            if not resend:
                entry = so.record_signature(rec["signoffs"], user_id, self._team(rec["entity"]), "fa", "approve", "", v["hash"])
                entry["at"] = now_s()
                if via:
                    entry["via"] = via
                rec["signoffs"].append(entry)
            sent = utcnow()
            old = rec.get("pm_request")
            _retire(rec)
            if resend and old and old["version_hash"] == v["hash"] and not old.get("closed"):
                sent = datetime.fromisoformat(old["sent_iso"])  # resending keeps the original 48h clock
            raw = {pm["user_id"]: secrets.token_urlsafe(32) for pm in pms}
            rec["pm_request"] = {"version_hash": v["hash"], "sent_iso": sent.isoformat(), "by": user_id,
                                 "expires_iso": (utcnow() + LINK_LIFETIME).isoformat(),
                                 "tokens": [{"hash": _hash_token(raw[pm["user_id"]]), "pm_id": pm["user_id"]} for pm in pms],
                                 "reminded_iso": (old or {}).get("reminded_iso") if resend else None,
                                 "escalated_iso": (old or {}).get("escalated_iso") if resend else None,
                                 "emails": [], "closed": None}
            rec["sent"] = True
            rec["log"].append({"at": now_s(), "who": self._name(user_id), "what": "%s to %s (version %d)" % (
                "Resent" if resend else "Confirmed and sent", ", ".join(p["name"] for p in pms), v["n"])})
            return {"pms": pms, "raw": raw, "fa": user_id}
        out = self._mutate(rid, fn)
        self._email_pms(rid, out["pms"], out["raw"], out["fa"], reminder=False)  # after the commit
        return out

    def _email_info(self, rec, pm, link):
        v = self._cur(rec)
        s = v["snapshot"]
        cash = s["cash"]
        cash_x = sum(a["end"] for a in cash.get("accounts", []) if not snapshot_parser.is_security_account(a["name"])) if cash.get("accounts") else None
        m = snapshot_render.money
        req = rec.get("pm_request") or {}
        due = datetime.fromisoformat(req["sent_iso"]) + ESCALATE_AFTER if req.get("sent_iso") else utcnow() + ESCALATE_AFTER
        fa_id = req.get("by")
        return {
            "building": s["meta"].get("building") or rec["client"], "entity": rec["entity"], "month_label": self._month_label(rec),
            "fa_name": self._name(fa_id) if fa_id else "Your FA", "pm_name": pm["name"],
            "kpis": [("Net operating income YTD", m(s["noi"]["ytd_actual"]), "Budget " + m(s["noi"]["ytd_budget"])),
                     ("Net income YTD", m(s["net_income"]["ytd_actual"]), "Budget " + m(s["net_income"]["ytd_budget"])),
                     ("%s NOI" % MONTHS[rec["month"] - 1], m(s["noi"]["month_actual"]), "Budget " + m(s["noi"]["month_budget"])),
                     ("Cash excl. security", m(cash_x) if cash_x is not None else "n/a", "At month end")],
            "notes": [(c["title"], snapshot_render.note_body(c)) for c in v["commentary"] if c.get("status") != "continuing"],
            "ongoing": [(c.get("label") or c["title"], (c.get("text") or c.get("facts") or "").strip(), c.get("since") or "")
                        for c in v["commentary"] if c.get("status") == "continuing"],
            "resolved": v.get("resolved") or [], "board_note": v["board_note"],
            "link": link, "due_label": label(due), "portal_link": self._portal_link(rec),
        }

    def _pdf_name(self, rec):
        return "%s - %s Monthly Financial Snapshot %s.pdf" % (rec["entity"], rec["client"], self._month_label(rec))

    def _email_pms(self, rid, pms, raw, fa_id, reminder):
        rec = self.store.get(rid)
        pdf = self.render(rec)
        sender = self._email(fa_id)
        results = []
        for pm in pms:
            info = self._email_info(rec, pm, self._confirm_link(rec, raw[pm["user_id"]]))
            subj, body = snapshot_mail.pm_request(info, reminder=reminder)
            r = self.mailer.send(sender, [pm["email"]], subj, body, attachments=[{"name": self._pdf_name(rec), "bytes": pdf}],
                                 kind="pm_reminder" if reminder else "pm_request")
            results.append({"pm": pm["name"], "to": pm["email"], "status": r["status"], "error": r.get("error"), "at": now_s()})

        def fn(rec):
            req = rec.get("pm_request")
            if req:
                req["emails"].extend(results)
            for x in results:
                what = {"sent": "Email sent to %s", "test": "Test email for %s sent to the test inbox",
                        "off": "Email to %s not sent (email is switched off)", "failed": "Email to %s FAILED"}[x["status"]] % x["pm"]
                rec["log"].append({"at": x["at"], "who": "System", "what": what + (": " + x["error"] if x.get("error") else "")})
        self._mutate(rid, fn)
        return results

    # ------------------------------------------------------------- PM: the email link
    def _find_token(self, rec, raw):
        req = rec.get("pm_request")
        if not req:
            return None, None
        h = _hash_token(raw)
        for t in req["tokens"]:
            if secrets.compare_digest(t["hash"], h):
                return req, t
        return req, None

    def link_state(self, rid, raw):
        """What the PM sees when opening the link. Never changes anything (scanners open links too)."""
        rec = self.store.get(rid)
        if not rec:
            return {"state": "unknown"}
        return self._state(rec, raw)

    def _state(self, rec, raw):
        req, tok = self._find_token(rec, raw)
        if not tok:
            # an older link for this snapshot, or a link that never existed
            known_old = _hash_token(raw) in rec.get("old_tokens", [])
            return {"state": "replaced" if known_old else "unknown", "rec": rec}
        pm = {"user_id": tok["pm_id"], "name": self._name(tok["pm_id"])}
        base = {"rec": rec, "pm": pm, "req": req, "info": self._email_info(rec, dict(pm, email=None), "")}
        if tok.get("used"):
            return dict(base, state="used", used=tok["used"])
        if req.get("closed"):
            return dict(base, state="closed", closed=req["closed"])
        if req["version_hash"] != self._cur(rec)["hash"]:
            return dict(base, state="replaced")
        if utcnow() > datetime.fromisoformat(req["expires_iso"]):
            return dict(base, state="expired")
        return dict(base, state="ok")

    def pm_decide(self, rid, raw, decision, note="", meta=None):
        """POST from the PM's confirm page. Valid once, for the current version, before the link expires."""
        if decision not in ("approve", "request_changes"):
            raise ValueError("Unknown decision.")

        def fn(rec):
            st = self._state(rec, raw)  # judged on the locked copy, so two clicks cannot both pass
            msgs = {"unknown": "This link is not valid.", "replaced": "This link was replaced by a newer email. Use the latest one.",
                    "used": "This link was already used.", "closed": "This request is already closed.",
                    "expired": "This link has expired. Ask your FA to send it again."}
            if st["state"] != "ok":
                raise ValueError(msgs[st["state"]])
            req = rec["pm_request"]
            _, tok = self._find_token(rec, raw)
            v = self._cur(rec)
            entry = so.record_signature(rec["signoffs"], tok["pm_id"], self._team(rec["entity"]), "pm", decision, note, v["hash"])
            entry["at"] = now_s()
            entry["via"] = dict({"method": "email-link"}, **(meta or {}))
            rec["signoffs"].append(entry)
            tok["used"] = {"at": now_s(), "decision": decision}
            req["closed"] = {"at": now_s(), "by": tok["pm_id"], "decision": decision}
            rec["log"].append({"at": now_s(), "who": self._name(tok["pm_id"]), "what": "PM %s from the email link%s" % (
                "confirmed" if decision == "approve" else "asked for changes", (': "%s"' % entry["note"]) if entry["note"] else "")})
            return {"pm_id": tok["pm_id"], "fa_id": req.get("by"), "note": entry["note"]}
        out = self._mutate(rid, fn)
        rec = self.store.get(rid)
        fa_email = self._email(out["fa_id"]) if out.get("fa_id") else None
        pm_name = self._name(out["pm_id"])
        if decision == "approve":
            saved = self.try_save(rid)
            line = ("It was saved to SharePoint: %s." % saved["files"][0] if saved.get("files") else
                    "Saving to SharePoint is on hold: %s" % saved.get("reason", "")) if saved else ""
            if fa_email:
                subj, body = snapshot_mail.final_saved({"pm_name": pm_name, "entity": rec["entity"], "building": rec["client"],
                                                        "month_label": self._month_label(rec), "save_line": line})
                self._log_mail(rid, self.mailer.send(fa_email, [fa_email], subj, body, kind="fa_final"), "FA")
        elif fa_email:
            subj, body = snapshot_mail.fa_changes_requested({"pm_name": pm_name, "entity": rec["entity"], "building": rec["client"],
                                                             "month_label": self._month_label(rec), "note": out["note"],
                                                             "portal_link": self._portal_link(rec)})
            self._log_mail(rid, self.mailer.send(fa_email, [fa_email], subj, body, kind="fa_changes"), "FA")
        return out

    def _log_mail(self, rid, r, who):
        def fn(rec):
            rec["log"].append({"at": now_s(), "who": "System", "what": "Email to %s: %s%s" % (
                who, r["status"], (" (" + r["error"] + ")") if r.get("error") else "")})
        self._mutate(rid, fn)

    # ------------------------------------------------------------- final save to SharePoint
    def try_save(self, rid):
        """Save the final PDF into the building's month folder. Rendering and the SharePoint write happen
        outside the record lock; the outcome is recorded afterwards. Returns {"files"} or {"reason"} or None."""
        rec = self.store.get(rid)
        if not rec or rec.get("released") or self.stage(rec) != "approved":
            return None
        pdf = self.render(rec)
        name = self._pdf_name(rec)
        try:
            files = self.releaser.release(pdf, name, rec["entity"], rec["client"], rec["year"], rec["month"])
            outcome = {"files": files, "dry_run": bool(getattr(self.releaser, "dry_run", False))}
        except Exception as e:
            reason = str(e) if isinstance(e, ValueError) else "SharePoint could not be reached (%s). Nothing was saved." % e
            outcome = {"reason": reason}

        def fn(rec):
            if rec.get("released"):
                return
            if "reason" in outcome:
                rec["release_blocked"] = {"at": now_s(), "reason": outcome["reason"]}
                rec["log"].append({"at": now_s(), "who": "System", "what": "Final, but saving to SharePoint is on hold: " + outcome["reason"]})
            elif outcome["dry_run"]:
                rec.pop("release_blocked", None)
                rec["rehearsal"] = {"at": now_s(), "files": outcome["files"]}
                rec["log"].append({"at": now_s(), "who": "System", "what": "Final. Saving is switched off, so nothing was written (it would go to %s)" % outcome["files"][0]})
            else:
                rec.pop("release_blocked", None)
                rec["released"] = {"at": now_s(), "files": outcome["files"], "dry_run": False}
                rec["log"].append({"at": now_s(), "who": "System", "what": "Saved to SharePoint: %s" % outcome["files"][0]})
        self._mutate(rid, fn)
        return outcome

    def release_now(self, rid, user_id):
        """FA retries the save of a final snapshot (after a hold, or once saving is switched on)."""
        rec = self.store.get(rid)
        if not rec:
            raise ValueError("Snapshot not found.")
        if rec.get("released"):
            raise ValueError("Already saved.")
        if not self._is_fa(rec, user_id):
            raise ValueError("Only the building's FA can retry the save.")
        if self.stage(rec) != "approved":
            raise ValueError("Only a final snapshot (confirmed by the PM) can be saved.")
        if getattr(self.releaser, "dry_run", False):
            raise ValueError("Saving to SharePoint is switched off.")
        out = self.try_save(rid)
        if out and out.get("reason"):
            raise ValueError(out["reason"])
        return out

    # ------------------------------------------------------------- the hourly tick
    def tick(self, now=None):
        """Reminder at 24h, overdue email at 48h (each once), retry held saves. Safe to run any time."""
        now = now or utcnow()
        done = {"reminded": [], "escalated": [], "saved": []}
        for row in self.store.summaries():
            rid = row["id"]
            rec = self.store.get(rid)
            stg = self.stage(rec)
            if stg == "awaiting_pm":
                req = rec["pm_request"]
                sent = datetime.fromisoformat(req["sent_iso"])
                if not req.get("reminded_iso") and now - sent >= REMIND_AFTER:
                    claim = self._claim(rid, "reminded_iso", now)
                    if claim:
                        self._email_pms(rid, claim["pms"], claim["raw"], req.get("by"), reminder=True)
                        done["reminded"].append(rid)
                if not req.get("escalated_iso") and now - sent >= ESCALATE_AFTER:
                    if self._claim(rid, "escalated_iso", now, new_links=False) is not None:
                        self._escalate(rid)
                        done["escalated"].append(rid)
            elif stg == "approved" and rec.get("release_blocked") and not getattr(self.releaser, "dry_run", False):
                out = self.try_save(rid)
                if out and out.get("files"):
                    done["saved"].append(rid)
        return done

    def _claim(self, rid, field, now, new_links=True):
        """Mark a timer step done before doing it, so two ticks never both send. A reminder gets fresh
        links (only hashes are stored, so the original link cannot be re-sent)."""
        def fn(rec):
            req = rec.get("pm_request")
            if self.stage(rec) != "awaiting_pm" or not req or req.get(field):
                return None
            req[field] = now.isoformat()
            if not new_links:
                return {}
            pms, _ = self._pm_recipients(rec)
            pms = [p for p in pms if any(t["pm_id"] == p["user_id"] for t in req["tokens"])]
            _retire(rec)
            raw = {p["user_id"]: secrets.token_urlsafe(32) for p in pms}
            req["tokens"] = [{"hash": _hash_token(raw[p["user_id"]]), "pm_id": p["user_id"]} for p in pms]
            req["expires_iso"] = (datetime.fromisoformat(req["sent_iso"]) + LINK_LIFETIME).isoformat()
            rec["log"].append({"at": now_s(), "who": "System", "what": "24-hour reminder to %s" % ", ".join(p["name"] for p in pms)})
            return {"pms": pms, "raw": raw}
        return self._mutate(rid, fn)

    def _escalate(self, rid):
        rec = self.store.get(rid)
        req = rec["pm_request"]
        fa_id = req.get("by")
        sender = self._email(fa_id)
        pms = [self._name(t["pm_id"]) for t in req["tokens"]]
        info = {"entity": rec["entity"], "building": rec["client"], "month_label": self._month_label(rec),
                "pm_name": " / ".join(pms), "sent_label": label(datetime.fromisoformat(req["sent_iso"])),
                "portal_link": self._portal_link(rec)}
        subj, body = snapshot_mail.overdue(info)
        r = self.mailer.send(sender, [sender] if sender else [], subj, body, cc=self.escalate_to, kind="overdue")

        def fn(rec):
            rec["log"].append({"at": now_s(), "who": "System", "what": "48 hours passed without PM confirmation. Overdue email: %s%s" % (
                r["status"], (" (" + r["error"] + ")") if r.get("error") else "")})
        self._mutate(rid, fn)

    # ------------------------------------------------------------- views
    def list(self):
        return sorted(self.store.summaries(), key=lambda r: (r["entity"], r["year"], r["month"]))

    def view(self, rid, as_user):
        rec = self.store.get(rid)
        if not rec:
            raise ValueError("Snapshot not found.")
        v = self._cur(rec)
        team = self._team(rec["entity"])
        st = so.status(rec["signoffs"], team, v["hash"])
        ok, why = so.readiness(v["snapshot"], v["acks"])
        roles = so.can_sign(as_user, team)
        stage = self.stage(rec)
        s = v["snapshot"]
        is_fa = "fa" in roles
        final = stage in ("approved", "released")
        pms, missing = self._pm_recipients(rec)
        can = {"edit": is_fa and not final,
               "send": is_fa and stage in ("draft", "changes_requested") and ok and not unconfirmed(v["commentary"])
                       and not st["state"].startswith("blocked") and bool(pms),
               "resend": is_fa and stage == "awaiting_pm",
               "release": is_fa and stage == "approved" and not getattr(self.releaser, "dry_run", False)}
        why_not = []
        if is_fa and stage in ("draft", "changes_requested"):
            why_not = list(why)
            left = unconfirmed(v["commentary"])
            if left:
                why_not.append("Confirm or edit every note below before sending (%d of %d still to confirm)." % (len(left), len(v["commentary"])))
            if st["state"].startswith("blocked"):
                why_not.append(self._blocked_text(st))
            if not pms:
                why_not.append("No PM email on file for this building (%s)." % (", ".join(m["name"] for m in missing) or "no PM assigned"))
        req = rec.get("pm_request")
        request = None
        if req:
            sent = datetime.fromisoformat(req["sent_iso"])
            request = {"current": req["version_hash"] == v["hash"], "sent": label(sent), "due": label(sent + ESCALATE_AFTER),
                       "pms": [self._name(t["pm_id"]) for t in req["tokens"]], "emails": req.get("emails", [])[-6:],
                       "reminded": bool(req.get("reminded_iso")), "overdue": bool(req.get("escalated_iso")),
                       "closed": req.get("closed")}
        return {
            "id": rec["id"], "entity": rec["entity"], "client": rec["client"], "building": s["meta"]["building"],
            "month": rec["month"], "year": rec["year"], "stage": stage, "version": v["n"], "sent": rec["sent"],
            "released": rec["released"], "rehearsal": rec.get("rehearsal"), "release_blocked": rec.get("release_blocked"),
            "release_off": bool(getattr(self.releaser, "dry_run", False)), "state": st["state"], "waiting_on": st["waiting_on"],
            "stale": st["stale_count"], "checks": s["checks"], "acks": v["acks"], "commentary": v["commentary"],
            "board_note": v["board_note"], "can": can, "why_not": why_not, "my_roles": roles,
            "team": team, "problems": self.directory.problems(rec["entity"]), "log": rec["log"][::-1],
            "signoffs": [dict(x, name=self._name(x["user_id"]), current=(x["version_hash"] == v["hash"])) for x in rec["signoffs"]],
            "request": request, "email_mode": self.mailer.mode, "resolved": v.get("resolved") or [],
            "source": v["source"], "generated_at": v["at"],
        }
