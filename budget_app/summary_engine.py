"""Pure Budget Summary computation engine (clean-architecture tranche 2a,
2026-07-05). Extracted VERBATIM from workflow.py api_summary_get — the most
defect-prone logic in the app (col7 cascade, pins, ordinal bucketing,
footing). No flask, no SQLAlchemy queries: deterministic over its inputs,
so test_summary_engine.py can freeze real-building vectors against it.

summary_rows may be ORM rows OR any objects with the same attributes
(tests use SimpleNamespace); bl_dicts are BudgetLine.to_dict() dicts;
row_au is the raw audit_uploads tuple prefetched by the route shell.
"""
import json as _json
import logging

try:
    from gl_logic import gl_matches_prefixes
except ImportError:
    from budget_app.gl_logic import gl_matches_prefixes
try:
    from budget_constants import ONE_TIME_FEE_GLS, _row_has_fixed_forecast_gl
except ImportError:
    from budget_app.budget_constants import ONE_TIME_FEE_GLS, _row_has_fixed_forecast_gl

logger = logging.getLogger(__name__)


def _gl_matches_prefixes(gl_code, prefixes):
    """Closure alias for the canonical matcher (budget_app/gl_logic.py).

    Bare token "5260" matches the GL family base (suffix stripped);
    dashed token "4130-0010" matches the full GL string. Kept as a local
    name so the existing call sites in this blueprint stay unchanged.
    """
    return gl_matches_prefixes(gl_code, prefixes)

def _section_key(section_label):
    """Map section label to internal key for subtotal grouping."""
    if not section_label:
        return ""
    sl = section_label.lower().strip()
    if "non" in sl and "income" in sl:
        return "non_operating_income"
    if "non" in sl and "expense" in sl:
        return "non_operating_expense"
    if sl == "income":
        return "income"
    if sl == "expenses":
        return "expenses"
    return ""

def _is_capital_line(line):
    """FA #18: Capital lines must not extrapolate forecast or auto-fill proposed.
    Capital lines are tagged BOTH ways at ingestion (sheet_name='Capital'
    AND category='capital'), so check either."""
    return (line.get("sheet_name") == "Capital"
            or (line.get("category") or "").lower() == "capital")

def _aggregate_by_prefix(budget_lines_dicts, prefixes, ytd_months):
    """Sum budget_lines matching GL prefixes. Returns ytd/estimate/forecast/current_budget.

    FA directive 2026-05-05: COL 3 (YTD) on the Summary tab must show
    raw posted YTD only — accruals and unpaid bills DO NOT belong in COL 3.
    Those mid-period adjustments belong in COL 4 (Estimate). The total
    forecast (COL 5 = COL 3 + COL 4) remains unchanged: it still equals
    ytd + accrual + unpaid + projection. Three new fields surfaced for
    the summary endpoint to assign columns correctly:
      - ytd_only: raw ytd_actual sum (→ COL 3)
      - accrual_unpaid: sum of accrual + unpaid (→ part of COL 4)
      - estimate: remaining-months projection (→ part of COL 4)
    Legacy `ytd_actual` field preserved (= ytd_only + accrual_unpaid)
    for any caller still expecting the old combined value.
    """
    totals = {
        "ytd_actual": 0.0,        # legacy combined ytd+accrual+unpaid
        "ytd_only": 0.0,           # NEW: raw YTD only (col 3)
        "accrual_unpaid": 0.0,     # NEW: accrual + unpaid (part of col 4)
        "estimate": 0.0,
        "forecast": 0.0,
        "current_budget": 0.0,
        "proposed_budget": 0.0,
        "count": 0,
        # FA 2026-06-17 (B1/B4): count of never-budgeted lines so the
        # Summary cascade can pin col7 (2027 budget) to 0 for a row whose
        # contributing GLs are ALL non-budgeted (prepaid / dividend / messenger).
        "no_budget_n": 0,
        # FA #26 (2026-06-15): track contributing sheets so the Summary can
        # pin R+M / Gen&Admin rows' proposed (col7) to col6 (row budget).
        "_sheet_n": {},
        "dominant_sheet": "",
    }
    for line in budget_lines_dicts:
        gl = line.get("gl_code", "")
        if not _gl_matches_prefixes(gl, prefixes):
            continue
        ytd = float(line.get("ytd_actual", 0) or 0)
        accrual = float(line.get("accrual_adj", 0) or 0)
        unpaid = float(line.get("unpaid_bills", 0) or 0)
        prior = float(line.get("prior_year", 0) or 0)
        ytd_total = ytd + accrual + unpaid
        remaining = 12 - ytd_months
        is_capital = _is_capital_line(line)
        # One-time fee rule: once YTD posted, no more projection
        if gl in ONE_TIME_FEE_GLS and abs(ytd_total) > 0.01:
            est = 0
        # FA #18 + 2026-05-05 directive: Capital — no forecast extrapolation,
        # no proposed budget, and forecast formula flips the accrual sign.
        #   forecast = ytd - accrual + unpaid  (NOT ytd + accrual + unpaid)
        #   estimate = 0
        #   proposed = 0 always (overrides any FA-entered value)
        elif is_capital:
            est = 0
        # 210 FA: RE-tax credit income (4105/4110/4115/4120/4125) posts at
        # year-end, not monthly — no May-Dec estimate (forecast = YTD).
        elif gl[:4] in ("4105", "4110", "4115", "4120", "4125"):
            est = 0
        # FA #7 anomaly cap: negative YTD against non-negative prior
        # year is a one-time refund/credit; don't extrapolate.
        elif ytd_total < 0 and prior >= 0:
            est = 0
        elif ytd_months > 0:
            est = (ytd_total / ytd_months) * remaining
        else:
            est = 0

        # FA #21 (2026-06-13): honor the income-tab's per-line ESTIMATE
        # override so a tab edit reaches the Summary. Mirrors
        # faComputeEstimate (the override takes precedence). Capital keeps
        # its pinned est=0 below.
        est_ov = line.get("estimate_override")
        if (not is_capital) and est_ov is not None:
            try:
                est = float(est_ov)
            except (TypeError, ValueError):
                pass

        # Capital forecast = ytd + accrual + unpaid, no estimate
        # (FA directive 2026-06-10 — supersedes 2026-05-05's minus sign,
        # which double-counted accruals that zero out YTD; mirrors the
        # same-day fix in all four faComputeForecast JS copies).
        if is_capital:
            line_forecast = ytd + accrual + unpaid
            # col 4 contribution for capital = forecast − ytd_only
            accrual_unpaid_contrib = accrual + unpaid
        else:
            line_forecast = ytd_total + est
            accrual_unpaid_contrib = accrual + unpaid
            # FA #21: a per-line FORECAST override wins (mirrors
            # faComputeForecast). Re-derive est so the col4 split stays
            # consistent: col5 (= ytd_only + accrual_unpaid + estimate)
            # then equals the overridden forecast.
            fcst_ov = line.get("forecast_override")
            if fcst_ov is not None:
                try:
                    line_forecast = float(fcst_ov)
                    est = line_forecast - ytd - accrual - unpaid
                except (TypeError, ValueError):
                    pass

        totals["ytd_actual"] += ytd_total       # legacy
        totals["ytd_only"] += ytd               # col 3 source
        totals["accrual_unpaid"] += accrual_unpaid_contrib   # part of col 4
        totals["estimate"] += est
        totals["forecast"] += line_forecast
        totals["current_budget"] += float(line.get("current_budget", 0) or 0)
        # FA directive 2026-05-05: Capital lines have NO proposed budget.
        # Always 0 — even an FA-entered value is ignored at the aggregator
        # level. (The line-level value remains stored for safety; we just
        # don't roll it up into the summary tab's COL 7.)
        if is_capital:
            proposed = 0
        # FA 2026-06-17 (B1/B4): never-budgeted income (prepaid / dividend /
        # messenger) proposes $0 — overrides any derivation below.
        elif line.get("no_budget"):
            proposed = 0
        else:
            proposed = float(line.get("proposed_budget", 0) or 0)
            if proposed == 0:
                inc_pct = float(line.get("increase_pct", 0) or 0)
                cb = float(line.get("current_budget", 0) or 0)
                sheet = line.get("sheet_name", "") or ""
                # FA #25 (2026-06-15): Payroll Processing (5172) = 2026
                # budget × 1.03. FA #26: R+M and Gen&Admin propose off the
                # 2026 budget, not the 12-mo forecast. Other expenses keep
                # the forecast basis (income col7 is pinned at the row level).
                if gl == "5172-0000":
                    proposed = cb * 1.03
                elif sheet in ("Repairs & Supplies", "Gen & Admin"):
                    proposed = cb * (1 + inc_pct)
                elif line_forecast > 0:
                    proposed = line_forecast * (1 + inc_pct)
        totals["proposed_budget"] += proposed
        totals["count"] += 1
        if line.get("no_budget"):
            totals["no_budget_n"] += 1
        _sh = line.get("sheet_name") or ""
        if _sh:
            totals["_sheet_n"][_sh] = totals["_sheet_n"].get(_sh, 0) + 1
    if totals["_sheet_n"]:
        totals["dominant_sheet"] = max(totals["_sheet_n"], key=totals["_sheet_n"].get)
    # FA 2026-06-17 (B1/B4): a row whose contributing GLs are ALL
    # never-budgeted gets its 2027 budget (col7) pinned to 0 in the cascade.
    totals["all_no_budget"] = (
        totals["count"] > 0 and totals["no_budget_n"] == totals["count"])
    return totals



def compute_summary(entity_code, budget_year, summary_rows, bl_dicts, ytd_months,
                    row_au=None, col2_sql_error=None,
                    op_assess_proposed=None, re_tax_exemptions_budget=None):
    """Compute the full 8-column summary payload (the /api/summary body).

    Returns the response dict the route jsonifies. Body is the verbatim
    former in-route computation; the leading aliases keep it unchanged.
    """
    _op_assess_proposed = op_assess_proposed
    _re_tax_exemptions_budget = re_tax_exemptions_budget
    # ── Col 2: 2025 Actual from confirmed audited financials ──────────
    # `warnings` is initialized HERE (was previously initialized later for
    # duplicate-row scan) because the Col 2 logic now emits a warning when
    # a confirmed audit has empty mapped_data — see FA #2 fix below.
    warnings = []
    col2_lookup = {}
    _col2_assigned_labels = set()   # F3: each audit category lands on ONE row
    _col2_dup_labels = set()
    col2_meta = {}        # {summary_label: {matched_category, match_type}}
    # source_lines per summary label = the auditor's raw line items that
    # rolled up into Col 2 for this row. Used by the Inspector drill-down
    # so FAs can see "Maintenance $4.66M came from these 3 auditor lines"
    # without leaving the Building Detail page.
    col2_source_lines = {}  # {summary_label: [{auditor_desc, amount, audit_category}, ...]}
    audit_info = None     # {id, fiscal_year_end, confirmed_at, confirmed_by, pdf_filename}
    try:
        # Label aliases: audit category variant → canonical summary label
        _LABEL_ALIASES = {
            "Common Charges": "Maintenance", "Gas - Heating": "Gas Cooking / Heating",
            "Gas Heating": "Gas Cooking / Heating", "Gas": "Gas Cooking / Heating",
            "Oil / Fuel": "Fuel", "Fuel Oil": "Fuel",
            "RE Taxes": "Real Estate Taxes", "Real Estate Tax": "Real Estate Taxes",
            "Assessment - Operating": "Assessment-Operating",
            "Storage Income": "Storage Room",
            "Garage": "Commercial Rent (Garage)",
            "Interest Income": "Other Income",
        }
        # Query audit_uploads directly (model defined in factory, can't import).
        # summary_overrides (Phase 2) is the canonical per-line state set by the
        # FA via the Inspector — takes precedence over mapped_data + raw_extraction
        # when populated for a given summary label.
        # (audit row prefetched by the route shell; a DB error there
        # surfaces exactly like the old in-block failure did)
        if col2_sql_error:
            raise RuntimeError(col2_sql_error)
        # FA #2 (148 working session 2026-05-13): an audit row can land at
        # status='confirmed' with an EMPTY mapped_data ({}). Pre-fd0d170
        # confirms allowed this, and re-extract paths can leave a corrupted
        # row behind on failure. Surface this state as a high-severity
        # warning so the FA sees the actual problem instead of a silently
        # empty Col 2. This is the "AFS confirmed but not on summary"
        # symptom the FA reported.
        if row_au:
            try:
                md_probe = _json.loads(row_au[1]) if row_au[1] else {}
            except Exception:
                md_probe = {}
            try:
                re_probe = _json.loads(row_au[6]) if row_au[6] else {}
            except Exception:
                re_probe = {}
            md_empty = (isinstance(md_probe, dict) and len(md_probe) == 0)
            re_empty = (isinstance(re_probe, dict) and len(re_probe) == 0) \
                or (isinstance(re_probe, list) and len(re_probe) == 0)
            if md_empty:
                warnings.append({
                    "type": "audit_confirmed_but_empty",
                    "severity": "high",
                    "title": "Audit confirmed but has no mapped data",
                    "message": (
                        "The 2025 audit upload is marked confirmed but its category mapping is empty, "
                        "so Col 2 (audited actuals) cannot populate. "
                        + ("The raw extraction is also empty — re-run audit extraction from the Foundation step. "
                            if re_empty
                            else "Open the audit review page and re-run mapping. ")
                        + f"Upload ID: {row_au[0]}."
                    ),
                    "audit_id": row_au[0],
                    "review_url": f"/audited-financials/review/{row_au[0]}",
                    "mapped_data_keys": 0,
                    "raw_extraction_empty": re_empty,
                })
        if row_au and row_au[1]:
            audit_info = {
                "id": row_au[0],
                "fiscal_year_end": row_au[2],
                "confirmed_at": row_au[3].isoformat() if row_au[3] else None,
                "confirmed_by": row_au[4] or "",
                "pdf_filename": row_au[5] or "",
            }
            mapped_raw = _json.loads(row_au[1])
            # raw_extraction has Claude's per-line breakdown (auditor_desc + amounts)
            # nested under each top-level item. Used as a backfill when mapped_data
            # lacks source_lines (audits confirmed before source_lines persistence).
            raw_ext = {}
            try:
                raw_ext = _json.loads(row_au[6]) if row_au[6] else {}
            except Exception:
                raw_ext = {}

            # Build a lookup of nested source_lines from raw_extraction, keyed by
            # the top-level auditor item description. Each top-level item carries
            # its own auditor-granular source_lines (the auditor's literal lines).
            raw_lines_by_desc = {}  # {top_desc: [{auditor_desc, amount}, ...]}
            def _harvest_top_items(items):
                for it in items or []:
                    if not isinstance(it, dict):
                        continue
                    top_desc = (it.get("description") or "").strip()
                    nested = it.get("source_lines") or []
                    flat = []
                    for sl in nested:
                        if not isinstance(sl, dict):
                            continue
                        sl_amts = sl.get("amounts") or []
                        sl_amt = sl_amts[0] if (isinstance(sl_amts, list) and sl_amts) else (sl.get("amount") or 0)
                        try:
                            sl_amt_f = float(sl_amt or 0)
                        except Exception:
                            sl_amt_f = 0.0
                        flat.append({
                            "auditor_desc": sl.get("auditor_desc") or sl.get("description") or "",
                            "amount": sl_amt_f,
                        })
                    # Fallback: top-level item with no nested source_lines becomes
                    # a single-entry source list of itself.
                    if not flat:
                        top_amts = it.get("amounts") or []
                        top_amt = top_amts[0] if (isinstance(top_amts, list) and top_amts) else 0
                        try:
                            top_amt_f = float(top_amt or 0)
                        except Exception:
                            top_amt_f = 0.0
                        flat = [{"auditor_desc": top_desc, "amount": top_amt_f}]
                    if top_desc:
                        raw_lines_by_desc.setdefault(top_desc, []).extend(flat)

            if isinstance(raw_ext.get("revenue"), dict):
                _harvest_top_items(raw_ext["revenue"].get("items"))
            if isinstance(raw_ext.get("expenses"), dict):
                cats_node = raw_ext["expenses"].get("categories")
                if isinstance(cats_node, list):
                    for grp in cats_node:
                        if isinstance(grp, dict):
                            _harvest_top_items(grp.get("items"))
                elif isinstance(cats_node, dict):
                    for _k, items in cats_node.items():
                        if isinstance(items, list):
                            _harvest_top_items(items)
                        elif isinstance(items, dict):
                            _harvest_top_items([items])

            # Extract {category: year_totals[0]} from mapped_data, and
            # capture source_lines per category so we can attribute them
            # to the right summary row below.
            confirmed = {}
            cat_source_lines = {}  # {audit_category: [{auditor_desc, amount}, ...]}
            for cat, info in mapped_raw.items():
                if isinstance(info, dict):
                    totals = info.get("year_totals", [])
                    if totals and len(totals) > 0:
                        confirmed[cat] = totals[0]
                    elif info.get("total"):
                        confirmed[cat] = info["total"]
                    # Preferred: source_lines persisted in mapped_data.
                    sls_raw = info.get("source_lines") or []
                    norm = []
                    for sl in sls_raw:
                        if not isinstance(sl, dict):
                            continue
                        desc = sl.get("auditor_desc") or sl.get("description") or ""
                        amts = sl.get("amounts") or []
                        amt = None
                        if isinstance(amts, list) and amts:
                            amt = amts[0]
                        if amt is None:
                            amt = sl.get("amount")
                        try:
                            amt_f = float(amt) if amt is not None else 0.0
                        except Exception:
                            amt_f = 0.0
                        norm.append({"auditor_desc": desc, "amount": amt_f})
                    # Backfill from raw_extraction if mapped_data lacks source_lines.
                    # Strategy: the auditor's top-level description for THIS category
                    # is most often the cat name itself (Claude classifies items into
                    # canonical buckets). If a direct key match doesn't work, try the
                    # alias map. If still nothing, the FA's per-DOM-dropdown mapping
                    # used a name we can't recover without re-confirming — fall back
                    # to leaving source_lines empty rather than guessing.
                    if not norm:
                        # Direct: top_desc == cat
                        norm = list(raw_lines_by_desc.get(cat, []))
                    if not norm:
                        # Reverse alias: any auditor desc whose canonical equals cat
                        for top_desc, lines in raw_lines_by_desc.items():
                            if _LABEL_ALIASES.get(top_desc) == cat:
                                norm.extend(lines)
                    if norm:
                        cat_source_lines[cat] = norm
            # Build reverse alias: canonical → [variants]
            alias_reverse = {}
            for variant, canonical in _LABEL_ALIASES.items():
                alias_reverse.setdefault(canonical, []).append(variant)
            # Build label set from this building's summary rows
            building_labels = {r.label for r in summary_rows if r.row_type == "data"}
            # Helper: stamp source_lines onto the resolved summary label,
            # tagging each line with the audit_category it came from so the
            # FA can tell which auditor bucket fed the row. Each line gets
            # a deterministic position-based ID so the Phase 2 mutation
            # endpoints can reference it before it's been promoted into
            # summary_overrides. After first edit/move/add the lines have
            # real UUIDs (assigned by _audit_promote_backfill).
            def _attach_lines(summary_label, audit_cat):
                lines_for_cat = cat_source_lines.get(audit_cat) or []
                if not lines_for_cat:
                    return
                bucket = col2_source_lines.setdefault(summary_label, [])
                base_idx = len(bucket)
                for off, sl in enumerate(lines_for_cat):
                    bucket.append({
                        "id": "raw:" + str(audit_cat) + ":" + str(base_idx + off),
                        "auditor_desc": sl.get("auditor_desc") or "",
                        "amount": sl.get("amount") or 0.0,
                        "audit_category": audit_cat,
                    })
            for cat, amount in confirmed.items():
                if amount is None:
                    continue
                # Direct match first
                if cat in building_labels:
                    col2_lookup[cat] = col2_lookup.get(cat, 0) + amount
                    col2_meta[cat] = {"matched_category": cat, "match_type": "direct"}
                    _attach_lines(cat, cat)
                else:
                    # Try alias: audit category might be a variant
                    canonical = _LABEL_ALIASES.get(cat, cat)
                    if canonical in building_labels:
                        col2_lookup[canonical] = col2_lookup.get(canonical, 0) + amount
                        col2_meta[canonical] = {"matched_category": cat, "match_type": "alias"}
                        _attach_lines(canonical, cat)
                    else:
                        # Try reverse: building label might be a variant of audit category
                        for variant in alias_reverse.get(cat, []):
                            if variant in building_labels:
                                col2_lookup[variant] = col2_lookup.get(variant, 0) + amount
                                col2_meta[variant] = {"matched_category": cat, "match_type": "alias_reverse"}
                                _attach_lines(variant, cat)
                                break

            # ── Phase 2: summary_overrides take precedence ──
            # If the FA has used the Inspector to edit/move/add audit lines
            # for a summary label, use those source_lines (with their
            # persisted UUIDs) and the recomputed total — overriding the
            # auto-mapped col2 from above.
            try:
                overrides_raw = row_au[7] if len(row_au) > 7 else None
                overrides = _json.loads(overrides_raw) if overrides_raw else {}
            except Exception:
                overrides = {}
            for ov_label, ov_block in (overrides or {}).items():
                if not isinstance(ov_block, dict):
                    continue
                ov_lines_raw = ov_block.get("source_lines") or []
                norm_lines = []
                for sl in ov_lines_raw:
                    if not isinstance(sl, dict):
                        continue
                    amt = sl.get("amount")
                    if amt is None:
                        amts = sl.get("amounts") or []
                        amt = amts[0] if (isinstance(amts, list) and amts) else 0
                    try:
                        amt_f = float(amt or 0)
                    except Exception:
                        amt_f = 0.0
                    norm_lines.append({
                        "id": sl.get("id"),
                        "auditor_desc": sl.get("auditor_desc") or "",
                        "amount": amt_f,
                        "audit_category": ov_label,
                        "user_added": bool(sl.get("user_added")),
                    })
                # Use the override's total when present (already recomputed
                # by the mutation endpoints), else sum the lines.
                ov_total = ov_block.get("total")
                if ov_total is None:
                    ov_total = sum(l["amount"] for l in norm_lines)
                col2_lookup[ov_label] = ov_total
                col2_meta[ov_label] = {"matched_category": ov_label, "match_type": "override"}
                col2_source_lines[ov_label] = norm_lines
    except Exception as _col2_err:
        col2_lookup = {"_error": str(_col2_err)}

    # Helper: per-line detail for a given GL prefix set (lineage breakdown)
    def _lines_for_prefixes(prefixes):
        out = []
        remaining = 12 - ytd_months
        for line in bl_dicts:
            gl = line.get("gl_code", "")
            if not _gl_matches_prefixes(gl, prefixes):
                continue
            ytd = float(line.get("ytd_actual", 0) or 0)
            accrual = float(line.get("accrual_adj", 0) or 0)
            unpaid = float(line.get("unpaid_bills", 0) or 0)
            prior = float(line.get("prior_year", 0) or 0)
            ytd_total = ytd + accrual + unpaid
            is_cap = _is_capital_line(line)
            # FA #18 + 2026-06-10 directive: Capital — never extrapolate;
            # forecast = ytd + accrual + unpaid (sign fix mirrors
            # _aggregate_by_prefix and the JS copies, same day).
            if is_cap:
                est = 0
                line_forecast = ytd + accrual + unpaid
            # FA #7 anomaly cap: don't extrapolate one-time refund/credit
            elif ytd_total < 0 and prior >= 0:
                est = 0
                line_forecast = ytd_total + est
            else:
                est = (ytd_total / ytd_months) * remaining if ytd_months > 0 else 0
                line_forecast = ytd_total + est
            out.append({
                "gl": gl,
                "desc": line.get("description") or line.get("gl_description") or "",
                "ytd": round(ytd, 2),
                "accrual": round(accrual, 2),
                "unpaid": round(unpaid, 2),
                "estimate": round(est, 2),
                "forecast": round(line_forecast, 2),
            })
        return out

    # Build response rows
    result_rows = []
    section_data = {"income": [], "expenses": [], "non_operating_income": [], "non_operating_expense": []}

    for row in summary_rows:
        if row.row_type == "section_header":
            result_rows.append({
                "label": row.label, "row_type": "section_header",
                "section": row.section, "display_order": row.display_order,
                "col1": None, "col2": None, "col3": None, "col4": None,
                "col5": None, "col6": None, "col7": None, "col8": None,
                "source_tab": None,
            })
            continue

        col1 = row.col1_prior_actual
        col6 = row.col6_approved_budget
        col7 = row.col7_proposed_budget

        # FA dir 2026-05-18 (148 review item #14): Capital lines have no
        # proposed budget — force col7 to $0 (not NULL/blank) for rows
        # whose section is non_operating_expense AND whose label is the
        # canonical "Capital Expenses" row, OR whose gl_prefixes include
        # any 7xxx capital prefix. This mirrors the per-line rule in
        # _aggregate_by_prefix where is_capital lines also get proposed=0.
        try:
            _row_label_lower = (row.label or "").lower()
            _is_capital_row = (
                "capital expense" in _row_label_lower
                or "capital expenses" in _row_label_lower
            )
            # Parse THIS row's prefixes locally. BUG (2026-06-10 audit):
            # the loop-scoped `prefixes` var isn't assigned for the current
            # row until ~20 lines below, so referencing it here read the
            # PREVIOUS row's prefixes — mis-forcing col7=0 on whatever data
            # row followed a capital row, and skipping the very first row.
            _row_prefixes = []
            if row.gl_prefixes_json:
                try:
                    _row_prefixes = _json.loads(row.gl_prefixes_json)
                except Exception:
                    _row_prefixes = []
            if not _is_capital_row and _row_prefixes:
                for _p in _row_prefixes:
                    _base = str(_p).split("-")[0].strip()
                    if _base.startswith("7"):
                        _is_capital_row = True
                        break
            if _is_capital_row and col7 is None:
                col7 = 0.0
        except Exception:
            pass

        # Compute cols 3-5 from budget_lines via GL prefix aggregation
        # 204 dry run 2026-07-06 (F3): a summary with DUPLICATE-labeled rows
        # used to hand the same audit value to every copy — 204's two
        # 'Capital Assessment' rows inflated Non-Op Income col2 by
        # $1,005,049 with zero warnings. The audit value now goes to the
        # FIRST row bearing the label; later duplicates get None and the
        # duplicate is reported loudly in warnings below.
        col2 = col2_lookup.get(row.label) if isinstance(col2_lookup, dict) and "_error" not in col2_lookup else None
        if col2 is not None:
            if row.label in _col2_assigned_labels:
                _col2_dup_labels.add(row.label)
                col2 = None
            else:
                _col2_assigned_labels.add(row.label)
        col3 = None   # 2026 YTD actual (raw — no accruals/unpaid)
        col4 = None   # 2026 estimate (= projection + accruals + unpaid)
        col5 = None   # 2026 forecast (= col3 + col4)

        prefixes = []
        agg_count = 0
        if row.row_type == "data" and row.gl_prefixes_json and bl_dicts:
            try:
                prefixes = _json.loads(row.gl_prefixes_json)
            except Exception:
                prefixes = []
            if prefixes:
                agg = _aggregate_by_prefix(bl_dicts, prefixes, ytd_months)
                agg_count = agg.get("count", 0)
                if agg_count > 0:
                    # FA directive 2026-05-05:
                    #   Col 3 (YTD)      = raw ytd only (no accrual/unpaid)
                    #   Col 4 (Estimate) = accrual + unpaid + remaining-months projection
                    #   Col 5 (Forecast) = Col 3 + Col 4 = today's forecast (unchanged)
                    col3 = round(agg["ytd_only"], 2)
                    col4 = round(agg["accrual_unpaid"] + agg["estimate"], 2)
                    col5 = round(agg["forecast"], 2)

                    # FA dir 2026-06-04: col6 (2026 approved budget) fallback.
                    # The 2026-approved-budget Summary import sets
                    # col6_approved_budget per row, but it can be missing even
                    # when the underlying GL lines DO carry a current_budget
                    # (entity 500 maintenance: GL budget ~$2.8M present, but
                    # Summary col6 was None). Fall back to the aggregated GL
                    # current_budget so (a) the Summary budget column matches
                    # the income tab, and (b) the fixed-forecast pin below
                    # fires (col5 -> col6) for fully-collectible income. Only
                    # fills a blank with a real GL budget — never overwrites an
                    # imported col6; a true $0 stays blank.
                    if col6 is None:
                        _agg_cb = round(agg.get("current_budget", 0) or 0, 2)
                        if abs(_agg_cb) > 0.005:
                            col6 = _agg_cb

                    # FA dir 2026-06-03 (#3): the Summary's 2027 Budget
                    # (col7) must reflect the proposed budget the PM/FA set
                    # on the income/expense tabs. col7_proposed_budget is
                    # the FA's explicit Summary-level OVERRIDE (typed
                    # directly on the Summary cell). When that override is
                    # NULL, fall back to the aggregated line-level
                    # proposed_budget so an accepted/edited proposal "hits"
                    # the Summary page. _aggregate_by_prefix already mirrors
                    # the income-tab display rule (capital → 0; proposed 0
                    # with positive forecast → forecast×(1+inc_pct)), so
                    # col7 ties to what the FA sees on the tabs. Only fall
                    # back to a non-zero aggregate — a true $0/blank stays
                    # blank rather than littering the column with zeros.
                    #
                    # EXCEPTION (FA dir #2/#6): the fixed-forecast income
                    # family (maintenance / common charges / commercial rent
                    # / operating assessment — bases 4010/4020/4030/4040/4200)
                    # must NOT take the raw YTD-annualized aggregate. Their
                    # forecast is pinned to the approved budget just below,
                    # and the proposed follows suit: = budget for maint/CC/
                    # rent (#2), = tax formula for operating assessment (#6).
                    # Skip them here so that pin wins. Operating assessment
                    # (4200) ALWAYS defers to the tax formula (#6), even with
                    # no approved budget. Maint/CC/rent (4010-4040) defer to
                    # the budget pin (#2) only when col6 exists to pin to;
                    # without col6 we still fall back to the aggregate so the
                    # cell isn't left blank (e.g. buildings with no budget).
                    if col7 is None:
                        _bases = {str(p).split("-")[0].strip()
                                  for p in (prefixes or [])}
                        # FA 2026-06-17 (B1/B4): a row built entirely from
                        # never-budgeted GLs (prepaid portfolio-wide;
                        # dividend/messenger on 210) proposes $0. Wins over
                        # the TBC (#18) and income→budget (#19) pins below.
                        if agg.get("all_no_budget"):
                            col7 = 0.0
                        # FA #18 (2026-06-16): the income "Tax Benefit
                        # Credits" row (GL bases all within the 4105-4125
                        # abatement/STAR/veteran/SCHE credit range, stored
                        # negative) proposes the NEGATIVE of the RE-tax
                        # page's 2026-27 exemptions total. Takes priority
                        # over the generic income→budget pin (#19) below.
                        def _in_tbc_range(_b):
                            try:
                                return 4105 <= int(_b) <= 4125
                            except Exception:
                                return False
                        # FA B5=A (2026-06-17): the income Tax Benefit
                        # Credits row's 2027 proposed comes ONLY from the
                        # RE-tax tab (negated exemptions) — $0 when not yet
                        # entered. Fire for ANY TBC row (even with blank
                        # exemptions) so it does NOT fall through to the #19
                        # prior-approved-budget (col6) pin below. 829 (RET
                        # entered) is unchanged; 210 (RET blank) → $0.
                        if _bases and all(_in_tbc_range(b) for b in _bases):
                            col7 = round(-abs(float(_re_tax_exemptions_budget or 0)), 2)
                        _pin_eligible = (
                            bool(_bases & {"4200"})
                            or (bool(_bases & {"4010", "4020", "4030", "4040"})
                                and col6 is not None)
                        )
                        if col7 is None and not _pin_eligible:
                            # FA #19 (2026-06-13): a non-fixed INCOME row
                            # (all GL bases 4xxx) defaults its 2027 proposed
                            # to the 2026 APPROVED BUDGET (col6), NOT the
                            # YTD-annualized aggregate forecast — which
                            # over-extrapolates seasonal income (829 Other
                            # Income forecast $205K vs $63K budget). Mirrors
                            # the fixed-forecast income pin. Only when col6 is
                            # a meaningful (non-zero) budget; a zero/blank
                            # budget still falls back to forecast so real
                            # income (e.g. interest, budgeted $0) isn't zeroed.
                            # Expenses (5xxx/6xxx) keep the forecast basis.
                            _is_income_row = bool(_bases) and all(
                                str(b)[:1] == "4" for b in _bases)
                            if (_is_income_row and col6 is not None
                                    and abs(float(col6)) > 0.005):
                                col7 = round(float(col6), 2)
                            else:
                                # FA #26 (2026-06-15): an R+M / Gen&Admin row
                                # proposes the 2026 approved budget (col6),
                                # matching the FA's "proposed = current budget"
                                # (the displayed budget column), not the sum of
                                # sparse line budgets. Mirrors the income pin.
                                _dom = agg.get("dominant_sheet", "")
                                if (_dom in ("Repairs & Supplies", "Gen & Admin")
                                        and col6 is not None and abs(float(col6)) > 0.005):
                                    col7 = round(float(col6), 2)
                                else:
                                    _agg_proposed = round(agg.get("proposed_budget", 0) or 0, 2)
                                    if abs(_agg_proposed) > 0.005:
                                        col7 = _agg_proposed

        # ── Fixed-forecast GL override ─────────────────────────────
        # Maintenance / Common Charges / Commercial Rent rows: pin
        # Col 5 (Forecast) to Col 6 (Approved Budget), back-solve
        # Col 4 (Estimate) = Col 5 - Col 3. Matched by GL prefix.
        fixed_forecast_applied = False
        if (row.row_type == "data"
                and _row_has_fixed_forecast_gl(row.gl_prefixes_json)
                and col6 is not None):
            col5 = round(float(col6), 2)
            col4 = round(col5 - (col3 or 0), 2)
            fixed_forecast_applied = True
            # FA dir 2026-06-03 (#2): Maintenance / Common Charges (and the
            # contractual commercial-rent income in the same fixed-forecast
            # family — bases 4010/4020/4030/4040) take their proposed budget
            # (Col 7) from the approved budget too, mirroring the forecast
            # pin above. Operating Assessment (4200) is EXCLUDED here: its
            # proposed is the tax-derived formula (#6), left blank until
            # built. Re-parse from the row's stored prefixes (not the `agg`
            # prefixes) so this fires even when no budget lines were loaded.
            # Only default when the FA hasn't typed an explicit Summary
            # proposed (col7 override) — an explicit override always wins.
            if col7 is None:
                try:
                    _ff_prefs = _json.loads(row.gl_prefixes_json) or []
                except Exception:
                    _ff_prefs = []
                _ff_bases = {str(p).split("-")[0].strip()
                             for p in _ff_prefs if p}
                if _ff_bases & {"4010", "4020", "4030", "4040"}:
                    col7 = round(float(col6), 2)

        # FA dir 2026-06-03 (#6): operating-assessment (GL 4200) proposed
        # budget (Col 7) = first-half RE tax × 2 × pct (default 17.5%,
        # editable per-property on the RE Tax page). Computed once above as
        # _op_assess_proposed (co-ops only; None when no DOF data or $0).
        # Applies regardless of approved budget (tax-derived, not budget-
        # derived) and only when the FA hasn't typed an explicit Summary
        # proposed override (col7 non-null always wins).
        if (col7 is None and _op_assess_proposed is not None
                and row.row_type == "data" and row.gl_prefixes_json):
            try:
                _oa_prefs = _json.loads(row.gl_prefixes_json) or []
            except Exception:
                _oa_prefs = []
            if any(str(p).split("-")[0].strip() == "4200" for p in _oa_prefs):
                col7 = _op_assess_proposed

        # FA #18 (2026-06-16): the income "Tax Benefit Credits" row (GL bases
        # all within the 4105-4125 abatement/STAR/veteran/SCHE credit range)
        # proposes the NEGATIVE of the RE-tax page's 2026-27 exemptions
        # total. Row-level (not agg-dependent) because the value comes from
        # the RE-tax page, not the income budget lines — this row usually has
        # no budget lines to aggregate (829: col6 blank, agg_count 0). Only
        # when the FA hasn't typed an explicit Summary proposed (col7) override.
        if (col7 is None and _re_tax_exemptions_budget is not None
                and row.row_type == "data" and row.gl_prefixes_json):
            try:
                _tbc_prefs = _json.loads(row.gl_prefixes_json) or []
            except Exception:
                _tbc_prefs = []
            _tbc_bases = []
            for _p in _tbc_prefs:
                try:
                    _tbc_bases.append(int(str(_p).split("-")[0].strip()))
                except Exception:
                    _tbc_bases = []
                    break
            if _tbc_bases and all(4105 <= _b <= 4125 for _b in _tbc_bases):
                col7 = round(-abs(float(_re_tax_exemptions_budget)), 2)

        # ── FA-set overrides (col3/col4/col5) take precedence over computed ──
        # FA directive 2026-05-05: editable green cells. Stash the computed
        # value in the response so the UI can show "click to revert to
        # computed $X" tooltips when an override is active.
        col3_computed = col3
        col4_computed = col4
        col5_computed = col5
        col3_overridden = row.col3_override is not None
        col4_overridden = row.col4_override is not None
        col5_overridden = row.col5_override is not None
        if col3_overridden:
            col3 = round(float(row.col3_override), 2)
        if col4_overridden:
            col4 = round(float(row.col4_override), 2)
        if col5_overridden:
            col5 = round(float(row.col5_override), 2)

        # FA #17 (2026-06-13): 2026 Forecast (col5) = 2026 YTD (col3) +
        # 2026 Estimate (col4). When the FA edits the estimate (or YTD)
        # but has NOT separately pinned the forecast, re-derive col5 so the
        # forecast reacts to the estimate edit (e.g. set commercial-rent
        # estimate to 0 -> forecast drops from $42K). Strict no-op when
        # nothing is overridden: col3+col4 already equals col5 on every
        # untouched row, including the fixed-forecast pin (col4 = col6-col3).
        if (not col5_overridden) and (col4_overridden or col3_overridden):
            col5 = round((col3 or 0) + (col4 or 0), 2)

        # FA directive 2026-05-17: same override pattern for c1/c2/c6.
        # c1 source = col1_prior_actual (imported), c2 source = audit lookup,
        # c6 source = col6_approved_budget (imported). Each override beats
        # its source when non-NULL.
        col1_computed = col1
        col2_computed = col2
        col6_computed = col6
        col1_overridden = row.col1_override is not None
        col2_overridden = row.col2_override is not None
        col6_overridden = row.col6_override is not None
        if col1_overridden:
            col1 = round(float(row.col1_override), 2)
        if col2_overridden:
            col2 = round(float(row.col2_override), 2)
        if col6_overridden:
            col6 = round(float(row.col6_override), 2)

        # FA dir 2026-05-17: per-cell typed-formula strings (e.g. "=300*12*4").
        # Parsed once per row so we don't hit json.loads in the dict literal.
        try:
            _row_formulas = _json.loads(row.cell_formulas_json) if row.cell_formulas_json else {}
        except Exception:
            _row_formulas = {}

        # Col 8: % variance = (col7 - col5) / |col5| * 100
        col8 = None
        if col7 is not None and col5 and col5 != 0:
            col8 = round(((col7 - col5) / abs(col5)) * 100, 1)

        # ── Lineage payload for inspector drill-down ──────────
        lineage = None
        if row.row_type == "data":
            c2_meta = col2_meta.get(row.label, {}) if isinstance(col2_meta, dict) else {}
            lineage = {
                "c2": {
                    "value": col2,
                    "audit_year": str(budget_year - 2),
                    "matched_category": c2_meta.get("matched_category"),
                    "match_type": c2_meta.get("match_type"),
                    "audit_id": audit_info.get("id") if audit_info else None,
                    "audit_fy": audit_info.get("fiscal_year_end") if audit_info else None,
                    "audit_confirmed_at": audit_info.get("confirmed_at") if audit_info else None,
                    "audit_confirmed_by": audit_info.get("confirmed_by") if audit_info else None,
                    "audit_filename": audit_info.get("pdf_filename") if audit_info else None,
                    "has_audit": bool(audit_info),
                    # Per-line breakdown: each entry is {auditor_desc, amount, audit_category}
                    # Multiple audit categories can roll up to one summary row, so the
                    # audit_category field disambiguates which auditor bucket each line came from.
                    "source_lines": col2_source_lines.get(row.label) or [],
                },
                "gl": {
                    "prefixes": prefixes,
                    "ytd_months": ytd_months,
                    "remaining_months": 12 - ytd_months,
                    "lines": _lines_for_prefixes(prefixes) if (prefixes and bl_dicts) else [],
                },
                "fixed_forecast": {
                    "applied": fixed_forecast_applied,
                    "col5_source": "approved_budget" if fixed_forecast_applied else "gl_aggregation",
                    "col4_formula": "col5 - col3" if fixed_forecast_applied else "gl_aggregation",
                    "note": ("Forecast pinned to Approved Budget "
                             "(Maintenance / Common Charges / Commercial Rent rule)")
                             if fixed_forecast_applied else None,
                },
            }

        # FA dir 2026-06-01: expose the row's GL prefix tokens so the
        # orphan "Add to existing row" modal can run a live double-count
        # guard client-side. Additive field; safe parse (never throws).
        try:
            _rd_prefixes = _json.loads(row.gl_prefixes_json) if row.gl_prefixes_json else []
            if not isinstance(_rd_prefixes, list):
                _rd_prefixes = []
        except Exception:
            _rd_prefixes = []

        rd = {
            "id": row.id,
            "label": row.label,
            "row_type": row.row_type,
            "section": row.section,
            "display_order": row.display_order,
            "gl_prefixes": _rd_prefixes,
            "footnote_marker": row.footnote_marker,
            "col1": col1, "col2": col2, "col3": col3,
            "col4": col4, "col5": col5, "col6": col6,
            "col7": col7, "col8": col8,
            "source_tab": row.source_tab,
            "lineage": lineage,
            # Override metadata: lets UI badge overridden cells and offer
            # one-click revert by exposing the computed-vs-override delta.
            "overrides": {
                "col1": {"is_overridden": col1_overridden, "computed": col1_computed,
                         "override": row.col1_override},
                "col2": {"is_overridden": col2_overridden, "computed": col2_computed,
                         "override": row.col2_override},
                "col3": {"is_overridden": col3_overridden, "computed": col3_computed,
                         "override": row.col3_override},
                "col4": {"is_overridden": col4_overridden, "computed": col4_computed,
                         "override": row.col4_override},
                "col5": {"is_overridden": col5_overridden, "computed": col5_computed,
                         "override": row.col5_override},
                "col6": {"is_overridden": col6_overridden, "computed": col6_computed,
                         "override": row.col6_override},
            },
            # FA dir 2026-05-17: typed-formula strings keyed by col. Empty
            # dict = no formulas. Frontend reads this to populate the
            # formula bar on focus so re-editing "300*12*4" → change 4 to 3.
            "formulas": _row_formulas,
        }
        result_rows.append(rd)

        # Track data rows for subtotal computation
        if row.row_type == "data":
            sk = _section_key(row.section)
            if sk in section_data:
                section_data[sk].append(rd)

    # ── Ordinal bucketing for flat-format buildings (2026-07-05) ────────
    # 829/872/437 class: flat imports carry section=None on data rows, so
    # the sectioned buckets above stay empty and every subtotal recomputes
    # from nothing (Total Income $1,500 on 829 = its one sectioned row; $0
    # totals on 437/148/500; credit-only totals on 872). The flat layout is
    # ordinal: a data row belongs to the first subtotal that follows it.
    # Fill a SEPARATE bucket set so the col1/col6 footing rules below keep
    # reading the sectioned-only buckets (2026-06-06 flat preserve rule).
    section_data_all = {k: list(v) for k, v in section_data.items()}
    _pending_unsectioned = []
    for rd in result_rows:
        _rt = rd.get("row_type")
        if _rt == "data":
            if not _section_key(rd.get("section") or ""):
                _pending_unsectioned.append(rd)
            continue
        if _rt != "subtotal":
            continue
        _ll = (rd.get("label") or "").lower()
        _bucket = None
        if "total income" in _ll:
            _bucket = "income"
        elif "total expenses" in _ll and "non" not in _ll:
            _bucket = "expenses"
        elif "non" in _ll and "income" in _ll:
            _bucket = "non_operating_income"
        elif "non" in _ll and "expense" in _ll:
            _bucket = "non_operating_expense"
        if _bucket:
            section_data_all[_bucket].extend(_pending_unsectioned)
        _pending_unsectioned = []

    # Recompute subtotal cols (1, 3-8) from data rows
    for rd in result_rows:
        if rd["row_type"] != "subtotal":
            continue
        label_lower = (rd.get("label") or "").lower()
        if "total income" in label_lower:
            data_rows = section_data_all.get("income", [])
            sectioned_rows = section_data.get("income", [])
        elif "total expenses" in label_lower and "non" not in label_lower:
            data_rows = section_data_all.get("expenses", [])
            sectioned_rows = section_data.get("expenses", [])
        elif "net operating" in label_lower:
            inc = section_data_all.get("income", [])
            exp = section_data_all.get("expenses", [])
            for ck in ["col2", "col3", "col4", "col5", "col7"]:
                iv = sum(r.get(ck) or 0 for r in inc)
                ev = sum(r.get(ck) or 0 for r in exp)
                rd[ck] = round(iv - ev, 2) if (iv or ev) else None
            # Footing fix 2026-06-06: foot col1 (2024 actual) / col6 (2026
            # budget) ONLY from sectioned data rows — flat-format buildings
            # (e.g. 148) store already-footed col1/col6 on the subtotal row,
            # and the 2026-07-05 ordinal buckets must not disturb that.
            inc_s = section_data.get("income", [])
            exp_s = section_data.get("expenses", [])
            if inc_s or exp_s:
                for ck in ["col1", "col6"]:
                    iv = sum(r.get(ck) or 0 for r in inc_s)
                    ev = sum(r.get(ck) or 0 for r in exp_s)
                    rd[ck] = round(iv - ev, 2)
            if rd["col7"] is not None and rd["col5"] and rd["col5"] != 0:
                rd["col8"] = round(((rd["col7"] - rd["col5"]) / abs(rd["col5"])) * 100, 1)
            continue
        elif "non" in label_lower and "income" in label_lower:
            data_rows = section_data_all.get("non_operating_income", [])
            sectioned_rows = section_data.get("non_operating_income", [])
        elif "non" in label_lower and "expense" in label_lower:
            data_rows = section_data_all.get("non_operating_expense", [])
            sectioned_rows = section_data.get("non_operating_expense", [])
        elif "total surplus" in label_lower or "total deficit" in label_lower:
            # Grand total = net operating + non-op income - non-op expense
            inc = section_data_all.get("income", [])
            exp = section_data_all.get("expenses", [])
            noi = section_data_all.get("non_operating_income", [])
            noe = section_data_all.get("non_operating_expense", [])
            for ck in ["col2", "col3", "col4", "col5", "col7"]:
                iv = sum(r.get(ck) or 0 for r in inc)
                ev = sum(r.get(ck) or 0 for r in exp)
                ni = sum(r.get(ck) or 0 for r in noi)
                ne = sum(r.get(ck) or 0 for r in noe)
                rd[ck] = round((iv - ev) + ni - ne, 2) if (iv or ev or ni or ne) else None
            # Footing fix 2026-06-06: col1/col6 only from sectioned rows —
            # preserve stored values for flat-format buildings.
            inc_s = section_data.get("income", [])
            exp_s = section_data.get("expenses", [])
            noi_s = section_data.get("non_operating_income", [])
            noe_s = section_data.get("non_operating_expense", [])
            if inc_s or exp_s or noi_s or noe_s:
                for ck in ["col1", "col6"]:
                    iv = sum(r.get(ck) or 0 for r in inc_s)
                    ev = sum(r.get(ck) or 0 for r in exp_s)
                    ni = sum(r.get(ck) or 0 for r in noi_s)
                    ne = sum(r.get(ck) or 0 for r in noe_s)
                    rd[ck] = round((iv - ev) + ni - ne, 2)
            if rd["col7"] is not None and rd["col5"] and rd["col5"] != 0:
                rd["col8"] = round(((rd["col7"] - rd["col5"]) / abs(rd["col5"])) * 100, 1)
            continue
        else:
            data_rows = []
            sectioned_rows = []

        # Simple sum for section subtotals — value columns foot from the
        # merged (sectioned + ordinal) buckets.
        for ck in ["col2", "col3", "col4", "col5", "col7"]:
            vals = [r.get(ck) or 0 for r in data_rows]
            rd[ck] = round(sum(vals), 2) if any(v != 0 for v in vals) else None
        # Footing fix 2026-06-06: col1/col6 only when SECTIONED rows exist
        # (flat-format stored, already-footed col1/col6 are never nulled).
        if sectioned_rows:
            for ck in ["col1", "col6"]:
                rd[ck] = round(sum(r.get(ck) or 0 for r in sectioned_rows), 2)
        if rd["col7"] is not None and rd["col5"] and rd["col5"] != 0:
            rd["col8"] = round(((rd["col7"] - rd["col5"]) / abs(rd["col5"])) * 100, 1)

    # ── Duplicate-row warnings ───────────────────────────────────────
    # FA directive 2026-05-05: rather than auto-collapsing duplicate rows
    # at import (which could destroy legitimate distinctions like Gas
    # cooking vs Gas Heating in some buildings), detect duplicates and
    # surface a warning so the FA can review and decide. Two strong
    # signals trigger a warning:
    #   - Same GL prefix list across multiple data rows in the same
    #     section (guaranteed identical aggregation = duplicate)
    #   - Identical non-zero col3/col5 values across data rows (weaker
    #     signal — could be coincidence but worth flagging)
    # NB: `warnings` already initialized above (Col 2 path also writes to it).
    try:
        data_rows = [r for r in summary_rows if r.row_type == "data"]
        # 1) Group by canonicalized gl_prefixes_json (same prefixes = same agg)
        prefix_groups = {}
        for row in data_rows:
            pj = row.gl_prefixes_json or ""
            if not pj or pj in ("[]", "null"):
                continue
            # Sort the prefix list inside the JSON so order doesn't matter
            try:
                plist = sorted(_json.loads(pj))
                key = (row.section or "", _json.dumps(plist))
            except Exception:
                key = (row.section or "", pj)
            prefix_groups.setdefault(key, []).append(row)
        for (section, key), rows in prefix_groups.items():
            if len(rows) > 1:
                warnings.append({
                    "type": "duplicate_prefixes",
                    "severity": "high",
                    "section": section,
                    "labels": [r.label for r in rows],
                    "shared_prefixes": _json.loads(key) if key.startswith("[") else [],
                    "message": (
                        f"{len(rows)} rows in '{section}' share the same GL prefix "
                        f"list — they aggregate identical data. Consider consolidating "
                        f"via the merge_into_label admin action, or differentiate the "
                        f"prefixes if they should track different GLs."
                    ),
                })
        # 2) Identical non-zero col3 (YTD) across data rows in the same section
        #    — secondary check that catches near-duplicates the FA may want
        #    to know about even when prefixes differ.
        col3_groups = {}
        for rd in result_rows:
            if rd.get("row_type") != "data": continue
            v = rd.get("col3")
            if v is None or abs(v) < 0.01: continue
            key = (rd.get("section") or "", round(v, 2))
            col3_groups.setdefault(key, []).append(rd)
        for (section, val), rds in col3_groups.items():
            if len(rds) > 1:
                # Skip if already covered by the prefix-duplicate check (avoid noise)
                labels = sorted(rd.get("label") for rd in rds)
                already_flagged = any(
                    sorted(w.get("labels") or []) == labels
                    for w in warnings if w["type"] == "duplicate_prefixes"
                )
                if not already_flagged:
                    warnings.append({
                        "type": "duplicate_values",
                        "severity": "medium",
                        "section": section,
                        "labels": labels,
                        "value": val,
                        "message": (
                            f"{len(rds)} rows in '{section}' show identical YTD "
                            f"(${val:,.0f}). Could be coincidence, but worth verifying."
                        ),
                    })

        # 3) Orphan GLs — lines with data but no summary row aggregates them.
        #    FA directive 2026-05-05: surface these with auto-suggested labels
        #    (= GL description) and a one-click "Add Row" hint so the FA
        #    can fix immediately.
        try:
            # Build set of GLs covered by any summary row's prefix list.
            matched_gls = set()
            for r in summary_rows:
                if not r.gl_prefixes_json:
                    continue
                try:
                    prefs = _json.loads(r.gl_prefixes_json) or []
                except Exception:
                    continue
                for line in bl_dicts:
                    gl = line.get("gl_code", "")
                    if not gl:
                        continue
                    if _gl_matches_prefixes(gl, prefs):
                        matched_gls.add(gl)

            # Section guess from line.category — drives the Add Row modal's
            # default section pick so the new row lands in the right place.
            def _section_for_category(cat):
                c = (cat or "").lower()
                if c == "income":
                    return "Income"
                if c == "capital":
                    return "Non-Operating Expense"
                return "Expenses"

            seen_orphans = set()
            orphan_warnings = []
            for line in bl_dicts:
                gl = line.get("gl_code", "")
                if not gl or gl in seen_orphans or gl in matched_gls:
                    continue
                desc = (line.get("description") or "").strip()
                # Skip placeholder GLs where description == gl_code or
                # description is empty/null (these are typically balance-
                # sheet or untagged Yardi codes, not budget items).
                if not desc or desc == gl:
                    continue
                ytd = float(line.get("ytd_actual", 0) or 0)
                cb = float(line.get("current_budget", 0) or 0)
                if abs(ytd) < 0.01 and abs(cb) < 0.01:
                    continue
                seen_orphans.add(gl)
                gl_base = gl.split("-")[0]
                orphan_warnings.append({
                    "gl_code": gl,
                    "gl_base": gl_base,
                    "description": desc,
                    "category": line.get("category"),
                    "sheet_name": line.get("sheet_name"),
                    "ytd": round(ytd, 2),
                    "current_budget": round(cb, 2),
                    "suggested_label": desc,         # auto-suggest from description
                    "suggested_section": _section_for_category(line.get("category")),
                    "suggested_prefix": gl_base,
                })

            if orphan_warnings:
                warnings.append({
                    "type": "orphan_gls",
                    "severity": "medium",
                    "count": len(orphan_warnings),
                    "title": f"{len(orphan_warnings)} unmapped GL{'s' if len(orphan_warnings) > 1 else ''} with data",
                    "message": (
                        "These GL codes have data on this building but no summary "
                        "row aggregates them. Click 'Add Row' on any to create the "
                        "row — the GL description is pre-filled as the label."
                    ),
                    "orphans": orphan_warnings,
                })
        except Exception as _orph_err:
            logger.warning(f"orphan-GL scan failed for {entity_code}: {_orph_err}")
    except Exception as _warn_err:
        logger.warning(f"summary duplicate-warning scan failed for {entity_code}: {_warn_err}")

    # F3: loud warning when duplicate-labeled rows collided on audit col2.
    if _col2_dup_labels:
        warnings.append({
            "type": "duplicate_label_col2",
            "severity": "high",
            "title": "Duplicate row labels — audit value assigned once",
            "message": (
                "These labels appear on MORE THAN ONE summary row: "
                + ", ".join(sorted(_col2_dup_labels))
                + ". The 2025 audit value was assigned to the first row only "
                "(it used to be double-counted). Merge or rename the "
                "duplicate rows."
            ),
            "labels": sorted(_col2_dup_labels),
        })

    return {
        "entity_code": entity_code,
        "budget_year": budget_year,
        "ytd_months": ytd_months,
        "rows": result_rows,
        "warnings": warnings,
        "stats": {
            "total_rows": len(result_rows),
            "data_rows": len([r for r in result_rows if r["row_type"] == "data"]),
            "has_budget_lines": len(bl_dicts) > 0,
            "warning_count": len(warnings),
        },
        "_debug_col2": col2_lookup,
    }
