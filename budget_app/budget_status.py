"""Budget status state machine + lifecycle vocabulary.

Architecture Phase 3, step 1 (2026-06-08). Extracted verbatim from workflow.py
(no behavior change) as the first slice of de-monolithing the 33k-line file. These
are pure data + one pure function (no db, no app context); workflow.py imports them
back so every existing reference resolves unchanged.

The spine of the app: draft -> pm_pending -> pm_in_progress -> fa_review -> approved,
with archived_inactive reachable from any state (Monday-sync pruning).
"""

BUDGET_STATUSES = [
    "not_started", "data_collection", "data_ready", "draft",
    "pm_pending", "pm_in_progress", "fa_review",
    "exec_review", "presentation", "approved",
    "ar_pending", "ar_complete", "returned",
    # FA directive 2026-05-11: Monday-sync pruning. When a building moves
    # out of the "Active Buildings (non-Lemle)" Monday group, its Budget
    # row gets archived (rather than deleted, which would destroy
    # BudgetLine + audit history). Listed buildings with this status are
    # filtered out of /api/buildings and the FA/PM dashboards but the
    # historical data is preserved for compliance.
    "archived_inactive",
]
USER_ROLES = ["fa", "pm", "admin", "cfo", "director", "ar"]

VALID_TRANSITIONS = {
    "not_started": ["data_collection", "archived_inactive"],
    "data_collection": ["data_ready", "archived_inactive"],
    "data_ready": ["draft", "archived_inactive"],
    "draft": ["pm_pending", "archived_inactive"],
    "pm_pending": ["pm_in_progress", "draft", "archived_inactive"],
    "pm_in_progress": ["fa_review", "archived_inactive"],
    "fa_review": ["approved", "returned", "exec_review", "archived_inactive"],
    "exec_review": ["presentation", "approved", "returned", "archived_inactive"],
    "presentation": ["approved", "returned", "archived_inactive"],
    "approved": ["ar_pending", "archived_inactive"],
    "ar_pending": ["ar_complete", "archived_inactive"],
    "ar_complete": ["archived_inactive"],
    "returned": ["draft", "archived_inactive"],
    # Archived can be un-archived to "draft" if a building comes back
    # into the active Monday group later.
    "archived_inactive": ["draft", "not_started"],
}

# ─── Lifecycle stages (single vocabulary used across all views) ──────────
# Order matters — lower index = earlier in the budget cycle.
# Anywhere a view shows "what stage is this building in?", it MUST use one
# of these strings. Don't introduce new values without updating this tuple.
LIFECYCLE_STAGES = (
    "Setup",                    # Entity exists, not yet started
    "Sources Collected",        # YSL/AP/ExpDist/Maint files staged
    "Assumptions Confirmed",    # Portfolio + building overrides locked
    "Budget Built (draft)",     # wizard_completed_at set; not yet to PM
    "PM Review",                # Sent to PM (pm_pending, pm_in_progress, fa_review)
    "Approved",                 # status approved or downstream of approval
)


def derive_lifecycle_stage(budget):
    """Map a Budget row to a single lifecycle stage string.

    Reads the existing fields (wizard_step, wizard_completed_at, status,
    assumptions_json) — does NOT change how data is stored. Just gives every
    view a consistent label to render.

    Returns one of LIFECYCLE_STAGES.
    """
    status = (budget.status or "").lower()
    # Approved or downstream
    if status in ("approved", "ar_pending", "ar_complete"):
        return "Approved"
    # Currently with PM (or just came back from PM)
    if status in ("pm_pending", "pm_in_progress", "fa_review"):
        return "PM Review"
    # Wizard finished but not yet sent to PM
    if budget.wizard_completed_at is not None:
        return "Budget Built (draft)"
    # Past Step 4 (building overrides done) — assumptions in flight
    if (budget.wizard_step or 0) >= 4:
        return "Assumptions Confirmed"
    # Sources uploaded but not yet through assumptions
    if (budget.wizard_step or 0) >= 2:
        return "Sources Collected"
    # Default — entity selected or earlier
    return "Setup"
