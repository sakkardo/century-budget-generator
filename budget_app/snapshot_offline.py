"""Offline runner: statement PDF in, snapshot PDF + JSON out. No DB, no network.

    python budget_app/snapshot_offline.py <statement.pdf> <out_dir> [label]
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import snapshot_parser
import snapshot_render
import snapshot_signoff


def main(stmt, out_dir, label, fa=None, pm=None):
    os.makedirs(out_dir, exist_ok=True)
    snap = snapshot_parser.build_snapshot(open(stmt, "rb").read())
    commentary = snapshot_render.draft_commentary(snap)
    ok, reasons = snapshot_signoff.readiness(snap)
    pdf = snapshot_render.render_pdf(snap, commentary, status_label="DRAFT",
                                     signoff={"fa": {"name": fa} if fa else None, "pm": {"name": pm} if pm else None})
    base = os.path.join(out_dir, label)
    open(base + ".pdf", "wb").write(pdf)
    json.dump({"snapshot": snap, "commentary": commentary,
               "hash": snapshot_signoff.content_hash(snap, commentary), "ready_for_signoff": ok,
               "reasons": reasons}, open(base + ".json", "w"), indent=1)
    print("%s: %d bytes, %d checks (%d tied), ready_for_signoff=%s" % (
        label, len(pdf), len(snap["checks"]), sum(c["status"] == "tied" for c in snap["checks"]), ok))
    for r in reasons:
        print("  -", r)


if __name__ == "__main__":
    a = sys.argv
    main(a[1], a[2], a[3] if len(a) > 3 else "snapshot", a[4] if len(a) > 4 else None, a[5] if len(a) > 5 else None)
