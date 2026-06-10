"""Shared per-source file-status model — the ONE brain for tile colors.

Status UX Phase 1 (2026-06-09, spec: STATUS_UX_PLAN.md). Jacob's rule:
the tile tracks the file's JOURNEY INTO A BUILT BUDGET, not mere staging.

    in_budget    GREEN  the wizard ingested the file AND a budget was created
                        from it. Budgets can be built with SOME files -> tiles
                        flip green individually. Audit exception: green at
                        CONFIRM (the mapping sign-off completes its job).
    needs_review AMBER  audit only — a human action is pending. sub="extract"
                        (PDF present, not extracted) or sub="confirm"
                        (extracted/mapped, awaiting FA sign-off).
    in_sp        AMBER  the file is in SharePoint (primary visible fact; date
                        shown). Auto-staging may have happened but does NOT
                        change the color. via="sharepoint" or "staged" (data
                        in DB but file not visible in the SP cache).
    failed       RED    a build attempted this file and it failed to parse
                        (unresolved build_failures row, no staged data).
    missing      RED    not in SharePoint, not staged — chase PM/vendor.
    setup        GRAY   building not started (lifecycle Setup) and the source
                        is nowhere; quiet, not alarming.

Pure leaf module: stdlib only, no db, no app context. workflow.py feeds it
batch-fetched facts; both the FA dashboard and the wizard render the result.
The deploy guard (check_status_vocabulary.py) freezes STATES and SOURCE_KEYS —
do not rename without updating spec + guard in the same commit.
"""

STATES = ("in_budget", "needs_review", "in_sp", "failed", "missing", "setup")

# Canonical source keys == sp_inventory.source_type values. Order = tile order.
SOURCE_KEYS = ("approved_2026", "expense_distribution", "ysl", "ap_aging",
               "maint_proof", "audit_2025")

# Letters the UI shows, kept here so every surface labels tiles identically.
SOURCE_LETTERS = {
    "approved_2026": "B", "expense_distribution": "E", "ysl": "Y",
    "ap_aging": "A", "maint_proof": "M", "audit_2025": "Au",
}
SOURCE_LABELS = {
    "approved_2026": "2026 Approved Budget",
    "expense_distribution": "Expense Distribution",
    "ysl": "YSL (Yardi)",
    "ap_aging": "AP Aging",
    "maint_proof": "Maintenance Proof",
    "audit_2025": "2025 Audit",
}


def _sp_newer(sp_date, loaded_ts):
    """True if the SharePoint file's modified time postdates the loaded data
    (Jacob 2026-06-10, 733's ExpDist: a new file in SP sat un-ingested behind
    a built budget with no signal). Both sides arrive as ISO-ish strings;
    normalize the space/T variant and compare lexically — safe for ISO."""
    if not sp_date or not loaded_ts:
        return False
    a = str(sp_date).strip().replace(" ", "T")[:19]
    b = str(loaded_ts).strip().replace(" ", "T")[:19]
    return a > b


def compute_source_states(built, is_setup, staged, sp_found, sp_meta,
                          audit, failures):
    """Return {source_key: {state, sub, date, via, filename}} for one entity.

    built     bool — wizard_completed_at set (a budget exists)
    is_setup  bool — lifecycle stage is "Setup" (nothing started)
    staged    {key: {"loaded": bool, "ts": iso|None}} — data present in DB
    sp_found  {source_type: bool} — sp_inventory snapshot
    sp_meta   {source_type: {"modified": iso, "filename": str}} — arrival info
    audit     {"id", "status", "ts"} | None — latest audit_uploads row
    failures  set of source_type with an UNRESOLVED build_failures row
    """
    out = {}
    for key in SOURCE_KEYS:
        meta = sp_meta.get(key) or {}
        in_sp = bool(sp_found.get(key))
        sp_date = meta.get("modified")
        fname = meta.get("filename") or None

        if key == "audit_2025":
            status = (audit or {}).get("status")
            ts = (audit or {}).get("ts")
            if status == "confirmed":
                st = {"state": "in_budget", "sub": "confirmed", "date": ts}
            elif status in ("extracted", "mapped"):
                st = {"state": "needs_review", "sub": "confirm", "date": ts}
            elif status == "uploaded":
                st = {"state": "needs_review", "sub": "extract", "date": ts}
            elif status == "extracting":
                st = {"state": "needs_review", "sub": "extracting", "date": ts}
            elif in_sp:
                st = {"state": "needs_review", "sub": "extract", "date": sp_date}
            elif is_setup:
                st = {"state": "setup", "sub": None, "date": None}
            else:
                st = {"state": "missing", "sub": None, "date": None}
            st["via"] = "sharepoint" if (in_sp and not status) else ("staged" if status else None)
            st["filename"] = fname
            out[key] = st
            continue

        s = staged.get(key) or {}
        loaded = bool(s.get("loaded"))
        loaded_ts = s.get("ts")

        if built and loaded:
            st = {"state": "in_budget", "sub": None, "date": loaded_ts, "via": None}
            # Stale-source flag: the file in SharePoint postdates what was
            # ingested. State stays in_budget (the budget IS built from real
            # data); sub tells the UI to offer a re-ingest.
            if in_sp and _sp_newer(sp_date, loaded_ts):
                st["sub"] = "newer_in_sp"
                st["sp_date"] = sp_date
        elif key in failures and not loaded:
            st = {"state": "failed", "sub": "failed", "date": None, "via": None}
        elif in_sp or loaded:
            st = {"state": "in_sp", "sub": None,
                  "date": sp_date or loaded_ts,
                  "via": "sharepoint" if in_sp else "staged"}
            if in_sp and loaded and _sp_newer(sp_date, loaded_ts):
                st["sub"] = "newer_in_sp"
        elif is_setup:
            st = {"state": "setup", "sub": None, "date": None, "via": None}
        else:
            st = {"state": "missing", "sub": None, "date": None, "via": None}
        st["filename"] = fname
        out[key] = st
    return out
