#!/usr/bin/env python3
"""Status-vocabulary gate (run by deploy.sh before every push).

Status UX Phase 1 (2026-06-09): the per-source tile states are the FA's primary
triage language — green = in a BUILT budget, amber = in SharePoint, red =
missing/failed, gray = setup (STATUS_UX_PLAN.md). The 2026-06-09 audit found the
old UI used the same colors for 8+ meanings because every surface invented its
own vocabulary. This gate freezes the canonical one so it cannot drift again:

  1. source_status.py must define EXACTLY the frozen STATES and SOURCE_KEYS.
  2. workflow.py's /api/budgets must still call compute_source_states and emit
     the source_states payload key (the one brain both pages render from).

If you change the vocabulary on purpose, update STATUS_UX_PLAN.md and this
gate in the SAME commit.

Usage: check_status_vocabulary.py [base_dir]   (deploy.sh passes the /tmp clone)
Exit 0 = OK; exit 1 = vocabulary drifted (prints what).
"""
import os
import sys

FROZEN_STATES = ("in_budget", "needs_review", "in_sp", "failed", "missing", "setup")
FROZEN_KEYS = ("approved_2026", "expense_distribution", "ysl", "ap_aging",
               "maint_proof", "audit_2025")

base = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))
fails = []

ss_path = os.path.join(base, "source_status.py")
try:
    ns = {}
    with open(ss_path, encoding="utf-8") as fh:
        exec(compile(fh.read(), ss_path, "exec"), ns)
    if tuple(ns.get("STATES") or ()) != FROZEN_STATES:
        fails.append("  STATES changed: %r (frozen: %r)" % (ns.get("STATES"), FROZEN_STATES))
    if tuple(ns.get("SOURCE_KEYS") or ()) != FROZEN_KEYS:
        fails.append("  SOURCE_KEYS changed: %r (frozen: %r)" % (ns.get("SOURCE_KEYS"), FROZEN_KEYS))
    if not callable(ns.get("compute_source_states")):
        fails.append("  compute_source_states missing from source_status.py")
except OSError as e:
    fails.append("  cannot read source_status.py: %s" % e)

wf_path = os.path.join(base, "workflow.py")
try:
    wf = open(wf_path, encoding="utf-8").read()
    if "compute_source_states(" not in wf:
        fails.append("  workflow.py no longer calls compute_source_states")
    if '"source_states"' not in wf:
        fails.append("  workflow.py no longer emits the source_states payload key")
except OSError as e:
    fails.append("  cannot read workflow.py: %s" % e)

if fails:
    sys.stderr.write("\nSTATUS-VOCABULARY GATE FAILED — the tile-state language drifted:\n")
    sys.stderr.write("\n".join(fails) + "\n")
    sys.stderr.write("If intentional: update STATUS_UX_PLAN.md + this gate in the same commit.\n")
    sys.exit(1)

print("Status-vocabulary gate OK (%d states, %d sources frozen)." % (len(FROZEN_STATES), len(FROZEN_KEYS)))
