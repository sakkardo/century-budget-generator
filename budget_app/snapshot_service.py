"""Monthly Financial Snapshot: workflow service (click-through version).

Storage is a JSON file and the SharePoint step writes to a local folder, so the whole flow can be
clicked through with test people and no database. The rules live in snapshot_signoff.py; this
module wires them to generate / edit / send / sign / release. Swapping the Store for DB tables and
the release for a SharePoint copy changes nothing in the rules.
"""
import json
import os
import threading
from datetime import datetime

try:
    import snapshot_parser
    import snapshot_render
    import snapshot_signoff as so
except ImportError:
    from budget_app import snapshot_parser, snapshot_render
    from budget_app import snapshot_signoff as so

MONTHS = snapshot_render.MONTH_NAMES

class StaticDirectory:
    """Test people and building teams. The real app uses a directory built from users +
    building_assignments (Monday); anything with these methods can be plugged in."""

    USERS = {
        2: {"name": "Kristy Paxinos"}, 8: {"name": "Jacob Sirotkin"}, 17: {"name": "George Matos"},
        101: {"name": "Jennifer Murman"}, 102: {"name": "Giovanni Lizarazo"},
    }
    TEAMS = {
        "204": {"client": "444 East 86th Owners Corp", "team": [
            {"user_id": 2, "name": "Kristy Paxinos", "role": "fa"}, {"user_id": 8, "name": "Jacob Sirotkin", "role": "pm"}]},
        "302": {"client": "205 Water Street Condominium", "team": [
            {"user_id": 101, "name": "Jennifer Murman", "role": "fa"}, {"user_id": 102, "name": "Giovanni Lizarazo", "role": "fa"},
            {"user_id": 17, "name": "George Matos", "role": "pm"}]},
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

    def users(self):
        return [{"id": k, "name": v["name"]} for k, v in self.USERS.items()]

    def buildings(self):
        return [{"entity": k, "client": v["client"], "team": v["team"]} for k, v in self.TEAMS.items()]

    def problems(self, entity):
        return []


class LocalReleaser:
    """Test stand-in for the SharePoint copy: two local folders, never overwrite."""

    dry_run = False

    def __init__(self, root):
        self.root = root

    def release(self, pdf, name, entity, client, year, month):
        m = MONTHS[month - 1]
        folders = [
            os.path.join(self.root, "01 - Accounting General", "Monthly Financial Snapshots", str(year), "%02d-%d" % (month, year)),
            os.path.join(self.root, "%s - %s" % (entity, client), "Monthly Financials", str(year), "%02d - %s" % (month, m)),
        ]
        paths = [os.path.join(f, name) for f in folders]
        for path in paths:  # check both before writing either
            if os.path.exists(path):
                raise ValueError("A file with this name already exists in %s. Nothing was overwritten." % os.path.dirname(path))
        out = []
        for folder, path in zip(folders, paths):
            os.makedirs(folder, exist_ok=True)
            with open(path, "wb") as f:
                f.write(pdf)
            out.append(os.path.relpath(path, self.root).replace(os.sep, "/"))
        return out


def now_s():
    return datetime.now().strftime("%b %d, %Y %I:%M %p").replace(" 0", " ")


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
    def __init__(self, store, releaser, directory):
        self.store, self.releaser, self.directory = store, releaser, directory

    # ------------------------------------------------------------- helpers
    def _team(self, entity):
        return self.directory.team(entity)

    def _cur(self, rec):
        return rec["versions"][-1]

    def _hash(self, v):
        return so.content_hash(v["snapshot"], v["commentary"], v["board_note"])

    def _name(self, uid):
        return self.directory.name(uid)

    def _signoff_block(self, rec):
        v = self._cur(rec)
        h = v["hash"]
        out = {}
        team = self._team(rec["entity"])
        for role in ("fa", "pm"):
            sig = [s for s in rec["signoffs"] if s["role"] == role and s["decision"] == "approve" and s["version_hash"] == h]
            if sig:
                out[role] = {"name": self._name(sig[-1]["user_id"]), "signed_at": sig[-1]["at"]}
            else:
                names = [a["name"] for a in team if a["role"] == role]
                out[role] = {"name": " / ".join(names) if names else None}
        return out

    def stage(self, rec):
        """One word for where this snapshot is: draft, in_signoff, changes_requested, approved, released."""
        if rec.get("released"):
            return "released"
        if not rec["sent"]:
            return "draft"
        st = so.status(rec["signoffs"], self._team(rec["entity"]), self._cur(rec)["hash"])
        return {"approved": "approved", "changes_requested": "changes_requested"}.get(st["state"], "in_signoff")

    def render(self, rec):
        v = self._cur(rec)
        stg = self.stage(rec)
        label = {"released": "APPROVED", "approved": "APPROVED", "in_signoff": "IN REVIEW",
                 "changes_requested": "CHANGES REQUESTED"}.get(stg, "DRAFT")
        return snapshot_render.render_pdf(v["snapshot"], v["commentary"], v["board_note"],
                                          signoff=self._signoff_block(rec), status_label=label)

    # ------------------------------------------------------------- actions
    def generate(self, entity, pdf_bytes, user_id, source):
        if not self.directory.known(entity):
            raise ValueError("Unknown building %s." % entity)
        snap = snapshot_parser.build_snapshot(pdf_bytes)
        m = snap["meta"]
        rid = "%s-%d-%02d" % (entity, m["year"], m["month"])
        commentary = snapshot_render.draft_commentary(snap)
        version = {"n": 1, "snapshot": snap, "commentary": commentary, "board_note": "", "acks": {},
                   "by": user_id, "at": now_s(), "source": source}
        version["hash"] = self._hash(version)
        def fn(rec):
            if rec and rec.get("released"):
                raise ValueError("This month is already released. Regenerating a released snapshot is not allowed.")
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

    def _mutate(self, rid, fn):
        def wrapper(rec):
            if not rec:
                raise ValueError("Snapshot not found.")
            res = fn(rec)
            rec["stage"] = self.stage(rec)
            return rec, res
        return self.store.mutate(rid, wrapper)

    def edit(self, rid, user_id, commentary, board_note):
        def fn(rec):
            if rec.get("released"):
                raise ValueError("Released snapshots cannot be edited.")
            if user_id not in [a["user_id"] for a in self._team(rec["entity"]) if a["role"] == "fa"]:
                raise ValueError(self._not_fa(rec, "edit the commentary and board note"))
            cur = self._cur(rec)
            if commentary == cur["commentary"] and board_note == cur["board_note"]:
                return
            v = dict(cur)
            v.update({"n": cur["n"] + 1, "commentary": commentary, "board_note": board_note, "by": user_id, "at": now_s()})
            v["hash"] = self._hash(v)
            rec["versions"].append(v)
            stale = len([s for s in rec["signoffs"] if s["version_hash"] == cur["hash"]])
            rec["log"].append({"at": now_s(), "who": self._name(user_id),
                               "what": "Edited commentary (version %d)%s" % (v["n"], ", %d signature(s) cleared" % stale if stale else "")})
        self._mutate(rid, fn)

    def acknowledge(self, rid, user_id, check_id, note):
        def fn(rec):
            if user_id not in [a["user_id"] for a in self._team(rec["entity"]) if a["role"] == "fa"]:
                raise ValueError(self._not_fa(rec, "add this note"))
            self._cur(rec)["acks"][check_id] = (note or "").strip()
            rec["log"].append({"at": now_s(), "who": self._name(user_id), "what": "Added note on a review check"})
        self._mutate(rid, fn)

    def send(self, rid, user_id):
        def fn(rec):
            if user_id not in [a["user_id"] for a in self._team(rec["entity"]) if a["role"] == "fa"]:
                raise ValueError(self._not_fa(rec, "send it to sign-off"))
            v = self._cur(rec)
            ok, why = so.readiness(v["snapshot"], v["acks"])
            if not ok:
                raise ValueError(" ".join(why))
            st = so.status(rec["signoffs"], self._team(rec["entity"]), v["hash"])
            if st["state"] in ("blocked_no_assignment", "blocked_needs_second_person"):
                raise ValueError(self._blocked_text(st))
            rec["sent"] = True
            rec["log"].append({"at": now_s(), "who": self._name(user_id), "what": "Sent to sign-off (version %d)" % v["n"]})
        self._mutate(rid, fn)

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

    def sign(self, rid, user_id, role, decision, note):
        def fn(rec):
            if rec.get("released"):
                raise ValueError("Already released.")
            if not rec["sent"]:
                raise ValueError("The FA has not sent this to sign-off yet.")
            v = self._cur(rec)
            entry = so.record_signature(rec["signoffs"], user_id, self._team(rec["entity"]), role, decision, note, v["hash"])
            entry["at"] = now_s()
            rec["signoffs"].append(entry)
            rec["log"].append({"at": now_s(), "who": self._name(user_id),
                               "what": "%s %s%s" % (role.upper(), "approved" if decision == "approve" else "requested changes",
                                                    (': "%s"' % entry["note"]) if entry["note"] else "")})
            if role == "fa" and decision == "approve":
                ok, why = so.can_publish(v["snapshot"], rec["signoffs"], self._team(rec["entity"]), v["hash"], v["acks"])
                if not ok:
                    st = so.status(rec["signoffs"], self._team(rec["entity"]), v["hash"])
                    rec["signoffs"].pop()
                    rec["log"].pop()
                    raise ValueError(self._blocked_text(st) if st["state"].startswith("blocked") else " ".join(why))
                self._release(rec)
        self._mutate(rid, fn)

    def _release(self, rec):
        m = MONTHS[rec["month"] - 1]
        name = "%s - %s Monthly Financial Snapshot %s %d.pdf" % (rec["entity"], rec["client"], m, rec["year"])
        # the APPROVED stamp must be on the file, so mark released before rendering
        rec["released"] = {"at": now_s(), "files": []}
        pdf = self.render(rec)
        try:
            files = self.releaser.release(pdf, name, rec["entity"], rec["client"], rec["year"], rec["month"])
        except Exception:
            rec["released"] = None
            raise
        rec["released"]["files"] = files
        rec["released"]["dry_run"] = bool(getattr(self.releaser, "dry_run", False))
        rec["log"].append({"at": now_s(), "who": "System", "what": (
            "Release rehearsal only, no files written" if rec["released"]["dry_run"] else "Released to the SharePoint folders")})

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
        def approved(role):
            return any(x["role"] == role and x["decision"] == "approve" and x["version_hash"] == v["hash"] for x in rec["signoffs"])
        can = {"edit": "fa" in roles and not rec["released"],
               "send": "fa" in roles and not rec["sent"] and ok and not st["state"].startswith("blocked") and not rec["released"],
               "sign_pm": "pm" in roles and rec["sent"] and not rec["released"] and not approved("pm"),
               "sign_fa": "fa" in roles and rec["sent"] and not rec["released"] and approved("pm") and not approved("fa")}
        why_not = []
        if "fa" in roles and not rec["sent"]:
            why_not = list(why)
            if st["state"].startswith("blocked"):
                why_not.append(self._blocked_text(st))
        if "fa" in roles and rec["sent"] and not can["sign_fa"] and not rec["released"]:
            why_not.append("Waiting for the PM to sign first.")
        return {
            "id": rec["id"], "entity": rec["entity"], "client": rec["client"], "building": s["meta"]["building"],
            "month": rec["month"], "year": rec["year"], "stage": stage, "version": v["n"], "sent": rec["sent"],
            "released": rec["released"], "state": st["state"], "waiting_on": st["waiting_on"],
            "stale": st["stale_count"], "checks": s["checks"], "acks": v["acks"], "commentary": v["commentary"],
            "board_note": v["board_note"], "can": can, "why_not": why_not, "my_roles": roles,
            "team": team, "problems": self.directory.problems(rec["entity"]), "log": rec["log"][::-1],
            "signoffs": [dict(x, name=self._name(x["user_id"]), current=(x["version_hash"] == v["hash"])) for x in rec["signoffs"]],
            "source": v["source"], "generated_at": v["at"],
        }
