#!/usr/bin/env python3
"""Client Board Presentation publish-gate guard (run by deploy.sh before every push).

The Board Presentation feature exists specifically so a client never sees a
budget claim the product invented and an FA never confirmed, and so a sent
link can never silently change later. Two invariants make that true, and this
gate freezes both as static source checks (no Flask/DB import needed):

  1. SNAPSHOT ISOLATION — the client-facing route (`board_notice_view`) must
     render ONLY from `PresentationSession.snapshot_data` (a frozen dict
     captured at publish time), filtered on `is_active=True`. It must never
     issue a live `BudgetLine`/`BudgetSummaryRow` query, or an FA editing the
     budget after sending a link could silently change what the client sees.
     Extended 2026-07-01 (plan: "Board Presentation — Full Budget Detail") to
     also ban the RE Taxes/Commercial/CAM compute calls the detail tabs use —
     those must be baked into the snapshot at publish time too, never called
     live from the client route.

  2. REVIEW GATE — `api_board_notice_publish` must refuse to create a
     PresentationSession unless `BudgetNarrative.status` is already
     "reviewed" or "published" AND `reviewed_narrative` is populated. A
     "draft" (system-authored, never confirmed by a human) must never be
     publishable directly, and the failure path must return a 4xx, not
     proceed silently. Must also still call `_generate_client_detail_tabs` to
     populate the snapshot's full-tab detail.

  3. CAM EXCLUSION — `_generate_client_detail_tabs` must never call
     `_cam_compute` or query `CamClass` (CAM hasn't cleared its own
     sample-approval gate; shipping it into a client-facing page is a
     separate decision), and must never call `_commercial_recompute_summary`
     (writes to the DB — the snapshot path must stay read-only).

Usage:
  check_client_narrative_publish.py [path/to/workflow.py]   # verify; exit 1 on regression
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def _func_body(src, fn_name):
    m = re.search(r"\n([ \t]*)def %s\b" % re.escape(fn_name), src)
    if not m:
        return None
    indent = len(m.group(1))
    lines = src[m.start() + 1:].split("\n")
    out = [lines[0]]
    for ln in lines[1:]:
        stripped = ln.lstrip()
        cur_indent = len(ln) - len(stripped)
        if stripped.startswith("def ") and cur_indent <= indent:
            break
        out.append(ln)
    return "\n".join(out)


def main():
    wf = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "workflow.py")
    src = open(wf, encoding="utf-8").read()
    fails = []

    # ── 1) Snapshot isolation: the client route never live-queries budget data ──
    view_body = _func_body(src, "board_notice_view")
    if not view_body:
        sys.stderr.write("NARRATIVE GATE: board_notice_view not found in %s\n" % wf)
        sys.exit(1)

    if "json.loads(session_row.snapshot_data)" not in view_body:
        fails.append("board_notice_view no longer parses PresentationSession.snapshot_data "
                     "-- the client page must render only from the frozen snapshot.")
    if "is_active=True" not in view_body:
        fails.append("board_notice_view no longer filters the session on is_active=True "
                     "-- a deactivated/superseded link could still render.")
    for banned in ("BudgetLine.query", "BudgetSummaryRow.query", "CommercialTenant.query",
                  "CamClass.query", "compute_re_taxes(", "_commercial_compute_escalations(",
                  "_cam_compute("):
        if banned in view_body:
            fails.append("board_notice_view calls/queries %s live -- this reintroduces the exact "
                         "drift the snapshot exists to prevent." % banned)

    # ── 2) Review gate: publish must require a reviewed narrative ──────────────
    publish_body = _func_body(src, "api_board_notice_publish")
    if not publish_body:
        sys.stderr.write("NARRATIVE GATE: api_board_notice_publish not found in %s\n" % wf)
        sys.exit(1)

    m = re.search(r"narrative\.status\s+not in\s*\(([^)]*)\)", publish_body)
    if not m:
        fails.append("api_board_notice_publish lost its narrative.status allow-list gate entirely.")
    else:
        allowed = m.group(1)
        if "reviewed" not in allowed:
            fails.append("publish gate's allowed-status set no longer includes 'reviewed': %r" % allowed)
        if "draft" in allowed:
            fails.append("publish gate's allowed-status set now includes 'draft' -- an unreviewed, "
                         "system-authored narrative could be published directly: %r" % allowed)

    if not re.search(r"not\s+narrative\.reviewed_narrative", publish_body):
        fails.append("publish gate no longer requires reviewed_narrative to be populated -- "
                     "status alone could publish stale or absent review content.")

    if not re.search(r"return\s+jsonify\(.*?\)\s*,\s*40\d", publish_body, re.S):
        fails.append("publish gate's failure path no longer returns a 4xx error response.")

    # The snapshot must be built from real, current per-line/per-row data at
    # publish time -- otherwise it would freeze an empty or stale document.
    if not (re.search(r"BudgetLine\.query", publish_body) and re.search(r"BudgetSummaryRow\.query", publish_body)):
        fails.append("api_board_notice_publish no longer snapshots live BudgetLine/"
                     "BudgetSummaryRow rows at publish time.")
    if "_generate_client_detail_tabs(" not in publish_body:
        fails.append("api_board_notice_publish no longer calls _generate_client_detail_tabs -- "
                     "the Full Budget Detail tabs would silently stop reaching new snapshots.")

    # ── 3) CAM exclusion + no-DB-write invariant on the detail-tabs compute ────
    detail_body = _func_body(src, "_generate_client_detail_tabs")
    if not detail_body:
        sys.stderr.write("NARRATIVE GATE: _generate_client_detail_tabs not found in %s\n" % wf)
        sys.exit(1)
    for banned in ("_cam_compute(", "CamClass.query", "_commercial_recompute_summary("):
        if banned in detail_body:
            fails.append("_generate_client_detail_tabs calls/queries %s -- CAM is excluded until "
                         "it clears its own sample-approval, and the snapshot path must stay "
                         "read-only (never the DB-writing commercial summary sync)." % banned)

    if fails:
        sys.stderr.write("\nCLIENT NARRATIVE PUBLISH GATE FAILED:\n")
        for f in fails:
            sys.stderr.write("  - %s\n" % f)
        sys.stderr.write("A client must never see a budget claim the system invented and an FA "
                         "never confirmed, and a published link must never silently change later. "
                         "See the 'Client Board Presentation' plan.\n")
        sys.exit(1)

    print("Client narrative publish gate OK (client route reads only the frozen, active "
          "snapshot; publish requires an FA-reviewed narrative).")


if __name__ == "__main__":
    main()
