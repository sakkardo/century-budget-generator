"""Tests for snapshot_signoff rules. Run: python budget_app/test_snapshot_signoff.py"""
import copy
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import snapshot_signoff as so

SNAP = {"meta": {"month": 8}, "income": {"ytd_actual": 10}, "categories": [], "capital": [], "cash": {},
        "monthly": {}, "checks": [{"id": "a", "label": "A", "severity": "block", "status": "tied", "detail": ""}]}
TEAM = [{"user_id": 1, "name": "Fran FA", "role": "fa"}, {"user_id": 2, "name": "Pat PM", "role": "pm"}]


def raises(fn):
    try:
        fn()
    except ValueError:
        return True
    return False


def run():
    h = so.content_hash(SNAP, ["note"], "")
    # same content, same hash; any change, new hash
    assert h == so.content_hash(copy.deepcopy(SNAP), ["note"], "")
    s2 = copy.deepcopy(SNAP)
    s2["income"]["ytd_actual"] = 11
    assert h != so.content_hash(s2, ["note"], "")
    assert h != so.content_hash(SNAP, ["note"], "Board note")
    assert h != so.content_hash(SNAP, ["other"], "")

    sig = []
    assert so.status(sig, TEAM, h)["state"] == "pending_signoff"
    # FA cannot release before the PM has signed
    assert raises(lambda: so.record_signature(sig, 1, TEAM, "fa", "approve", "", h))
    sig.append(so.record_signature(sig, 2, TEAM, "pm", "approve", "ok", h))
    st = so.status(sig, TEAM, h)
    assert st["state"] == "pending_signoff" and st["waiting_on"] == ["fa"]
    assert not so.can_publish(SNAP, sig, TEAM, h)[0]
    sig.append(so.record_signature(sig, 1, TEAM, "fa", "approve", "", h))
    assert so.status(sig, TEAM, h)["state"] == "approved"
    assert so.can_publish(SNAP, sig, TEAM, h)[0]

    # editing after approval invalidates everything
    h2 = so.content_hash(SNAP, ["edited"], "")
    st = so.status(sig, TEAM, h2)
    assert st["state"] == "pending_signoff" and st["waiting_on"] == ["fa", "pm"] and st["stale_count"] == 2
    assert not so.can_publish(SNAP, sig, TEAM, h2)[0]

    # request changes needs a note and blocks approval; the PM can then approve the same version
    assert raises(lambda: so.record_signature([], 2, TEAM, "pm", "request_changes", " ", h))
    sig2 = [so.record_signature([], 2, TEAM, "pm", "request_changes", "Utilities number looks off", h)]
    assert so.status(sig2, TEAM, h)["state"] == "changes_requested"
    sig2.append(so.record_signature(sig2, 2, TEAM, "pm", "approve", "Checked with the super", h))
    sig2.append(so.record_signature(sig2, 1, TEAM, "fa", "approve", "", h))
    assert so.status(sig2, TEAM, h)["state"] == "approved"

    # strangers and wrong roles cannot sign
    for uid, role in ((99, "pm"), (1, "pm"), (2, "fa")):
        assert raises(lambda: so.record_signature([], uid, TEAM, role, "approve", "", h))

    # one person holding both roles cannot cover both
    both = [{"user_id": 5, "name": "Solo", "role": "fa"}, {"user_id": 5, "name": "Solo", "role": "pm"}]
    one = [so.record_signature([], 5, both, "pm", "approve", "", h)]
    one.append(so.record_signature(one, 5, both, "fa", "approve", "", h))
    assert so.status(one, both, h)["state"] == "blocked_needs_second_person"
    assert not so.can_publish(SNAP, one, both, h)[0]

    # no PM assigned: blocked, not silently approved
    assert so.status([], [TEAM[0]], h)["state"] == "blocked_no_assignment"

    # failed blocking check keeps it out of sign-off; review check needs a note
    bad = copy.deepcopy(SNAP)
    bad["checks"] = [{"id": "b", "label": "B", "severity": "block", "status": "mismatch", "detail": "x"},
                     {"id": "r", "label": "R", "severity": "review", "status": "mismatch", "detail": ""}]
    ok, why = so.readiness(bad)
    assert not ok and len(why) == 2
    ok, why = so.readiness({"checks": [bad["checks"][1]]}, {"r": "Page excludes security deposits"})
    assert ok
    print("snapshot_signoff: all tests passed")


if __name__ == "__main__":
    run()
