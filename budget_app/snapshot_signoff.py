"""Monthly Financial Snapshot: sign-off rules.

Pure logic, no Flask or DB, so the rules can be tested alone. The DB layer (a later phase)
stores one SnapshotVersion row per generated/edited snapshot and one SnapshotSignoff row per
signature, and calls these functions to decide what is allowed.

Rules (Jacob, 2026-10-01; order reversed 2026-10-02)
- Required signers are the building's FA and PM (building_assignments, synced from Monday).
  They must be two different people: one person holding both roles cannot satisfy both.
- The FA confirms first (in the portal, which emails the PM). The PM confirms last, from the
  email, and the PM's approval makes the snapshot final and saves it to the building's
  SharePoint month folder.
- A signature is tied to the content hash of one exact version. Any change to the figures,
  commentary or board note makes a new version with a new hash, and old signatures stop counting.
- Approve may carry a note. Request-changes must carry a note.
- A "board note" prints on the snapshot (so it is part of the hash). An "internal note" never
  leaves the app.
- Failed blocking checks stop the snapshot from going to sign-off. Failed review checks need
  an acknowledgement note from the FA before it goes to sign-off.
- Saving needs: both roles signed on the current hash by two people, no open blocking checks.
  It fires when the PM approval is recorded; the generator never saves anything on its own.
"""
import hashlib
import json

REQUIRED_ROLES = ("fa", "pm")


def content_hash(snapshot, commentary, board_note=""):
    """Stable hash of everything the board will see."""
    notes = [{"title": c.get("title"), "text": c.get("text")} if isinstance(c, dict) else c for c in (commentary or [])]
    payload = {"figures": _figures(snapshot), "commentary": notes, "board_note": board_note or ""}
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _figures(s):
    return {k: s.get(k) for k in ("meta", "income", "expenses", "noi", "nonop_income", "nonop_expense",
                                  "net_income", "capital_assessment", "categories", "capital", "cash", "monthly")}


def readiness(snapshot, review_acknowledgements=None):
    """Can this snapshot be sent to sign-off? Returns (ok, reasons)."""
    reasons = []
    acks = review_acknowledgements or {}
    for c in snapshot.get("checks", []):
        if c["status"] == "tied":
            continue
        if c["severity"] == "block":
            reasons.append("Blocking check failed: %s. %s" % (c["label"], c.get("detail", "")))
        elif c["severity"] == "review" and not (acks.get(c["id"]) or "").strip():
            reasons.append("Needs an FA note before sign-off: %s" % c["label"])
    return (not reasons, reasons)


def required_signers(assignments):
    """assignments: list of {user_id, name, role}. Returns {role: [people]} for required roles."""
    out = {r: [] for r in REQUIRED_ROLES}
    for a in assignments:
        if a["role"] in out:
            out[a["role"]].append(a)
    return out


def missing_assignments(assignments):
    return [r for r, people in required_signers(assignments).items() if not people]


def can_sign(user_id, assignments):
    """Roles this user may sign as on this building."""
    return sorted({a["role"] for a in assignments if a["user_id"] == user_id and a["role"] in REQUIRED_ROLES})


def record_signature(signoffs, user_id, assignments, role, decision, note, version_hash):
    """Return the new signoff entry or raise ValueError. decision: 'approve' | 'request_changes'.

    The FA confirms first. The PM confirms last, and the PM's approval makes it final.
    """
    if role not in can_sign(user_id, assignments):
        raise ValueError("Only the building's %s can sign as %s." % (role.upper(), role.upper()))
    if decision not in ("approve", "request_changes"):
        raise ValueError("Decision must be approve or request_changes.")
    if decision == "request_changes" and not (note or "").strip():
        raise ValueError("Add a note saying what needs to change.")
    if role == "pm":
        fa_ok = any(s["role"] == "fa" and s["decision"] == "approve" and s["version_hash"] == version_hash
                    for s in signoffs)
        if not fa_ok:
            raise ValueError("The FA confirms this version first.")
    return {"user_id": user_id, "role": role, "decision": decision, "note": (note or "").strip(),
            "version_hash": version_hash}


def status(signoffs, assignments, current_hash):
    """Summarise sign-off state against the current content hash."""
    live = [s for s in signoffs if s["version_hash"] == current_hash]
    latest = {}  # latest decision per (person, role) wins
    for s in live:
        latest[(s["user_id"], s["role"])] = s
    changes = [s for s in latest.values() if s["decision"] == "request_changes"]
    approvals = {}
    for s in latest.values():
        if s["decision"] == "approve":
            approvals[s["role"]] = s["user_id"]
    need = [r for r in REQUIRED_ROLES if r not in approvals]
    stale = [s for s in signoffs if s["version_hash"] != current_hash]
    missing = missing_assignments(assignments)
    # The two signatures must come from two different people.
    same_person = len(approvals) == 2 and approvals["fa"] == approvals["pm"]
    people = {a["user_id"] for a in assignments if a["role"] in REQUIRED_ROLES}
    if missing:
        state = "blocked_no_assignment"
    elif len(people) < 2:
        state = "blocked_needs_second_person"
    elif changes:
        state = "changes_requested"
    elif same_person:
        state = "blocked_needs_second_person"
    elif not need:
        state = "approved"
    else:
        state = "pending_signoff"
    return {"state": state, "waiting_on": need, "changes": changes, "stale_count": len(stale),
            "missing_assignments": missing}


def can_publish(snapshot, signoffs, assignments, current_hash, review_acknowledgements=None):
    """Release to the building's SharePoint folder. Called when the FA approval lands."""
    ok, reasons = readiness(snapshot, review_acknowledgements)
    st = status(signoffs, assignments, current_hash)
    if st["state"] != "approved":
        reasons.append("Sign-off is not complete (%s)." % st["state"].replace("_", " "))
    return (ok and st["state"] == "approved", reasons)
