"""
Budget Summary Engine — Option A

Aggregates budget_lines directly by GL prefix to produce the 8-column
Budget Summary tab. No dependency on tab UI subtotals.

Input:
  - Row framework from yrlycomp parser (building-specific rows)
  - budget_lines data (list of dicts with gl_code, ytd_actual, current_budget, etc.)
  - GL_TO_SUMMARY_MAP config (gl_prefix → summary row mapping)
  - Prior year actuals from yrlycomp import
  - RE Taxes data (optional, from compute_re_taxes)

Output:
  - List of summary rows, each with 8 column values + metadata
"""

from GL_TO_SUMMARY_MAP import SUMMARY_ROW_MAP, SUBTOTAL_FORMULAS, LABEL_ALIASES, _CONDO_ROWS


# ── Forecast computation (mirrors workflow.py compute_forecast) ──────────

def compute_forecast(ytd_actual, accrual_adj, unpaid_bills, prior_year, ytd_months=2):
    """12-month forecast: ytd_total + estimate.

    FA item #7 anomaly guard (added 2026-05-03): if YTD is negative but
    prior_year is zero/positive, treat the negative as a one-time event
    (refund, contractor reimbursement, insurance proceeds posted as a credit
    to the expense GL) and don't extrapolate. Recurring negatives — tax
    abatements, STAR/SCHE credits, where prior_year is also negative — keep
    extrapolating normally.
    """
    ytd_total = (ytd_actual or 0) + (accrual_adj or 0) + (unpaid_bills or 0)
    remaining = 12 - ytd_months
    prior = prior_year or 0

    # FA #7 cap
    if ytd_total < 0 and prior >= 0:
        return ytd_total  # estimate = 0; forecast = YTD only

    if ytd_total >= prior_year and prior_year > 0 and ytd_months > 0:
        estimate = (ytd_total / ytd_months) * remaining
    else:
        estimate = max(prior_year - ytd_total, 0)

    return ytd_total + estimate


# ── GL prefix matching ───────────────────────────────────────────────────

def gl_matches_prefixes(gl_code, prefixes):
    """Check if a GL code matches any of the given prefixes.

    Two matching modes (chosen per prefix):
      - Bare prefix like "5260": match against `gl_base` (suffix stripped:
        "5110-0000" -> "5110"). Used for whole-account-family categories.
      - Full GL like "4130-0010": match against the un-stripped gl_code.
        Used to disambiguate sub-accounts (Storage 4130-0010 vs
        Bicycle 4130-0015 vs Laundry 4130-0030).

    Detection: a prefix containing "-" enables exact sub-account mode.
    """
    if not gl_code or not prefixes:
        return False
    gl_str = str(gl_code).strip()
    gl_base = gl_str.split("-")[0].strip()
    for prefix in prefixes:
        p = str(prefix).strip()
        if "-" in p:
            if gl_str.startswith(p):
                return True
        else:
            if gl_base.startswith(p):
                return True
    return False


# ── Core aggregation ─────────────────────────────────────────────────────

def aggregate_lines_for_row(budget_lines, row_config, ytd_months=2):
    """
    Sum budget_lines matching a summary row's GL prefix rules.

    Returns dict with:
      - ytd_actual: sum of ytd_actual for matching lines
      - forecast: sum of per-line forecasts
      - estimate: forecast - ytd_actual
      - current_budget: sum of current_budget
      - proposed_budget: sum of proposed_budget (or forecast * (1 + inc%))
      - matched_gl_codes: list of GL codes that matched
      - line_count: number of matching lines
    """
    prefixes = row_config.get("gl_prefix", [])
    category_filter = row_config.get("category")

    totals = {
        "ytd_actual": 0.0,
        "accrual_adj": 0.0,
        "unpaid_bills": 0.0,
        "prior_year": 0.0,
        "current_budget": 0.0,
        "proposed_budget": 0.0,
        "forecast": 0.0,
        "matched_gl_codes": [],
        "line_count": 0,
    }

    for line in budget_lines:
        gl = line.get("gl_code", "")

        # Match by GL prefix
        if not gl_matches_prefixes(gl, prefixes):
            continue

        # Optional category filter (for R&S: supplies vs repairs vs maintenance)
        if category_filter:
            line_cat = line.get("category", "")
            if isinstance(category_filter, list):
                if line_cat not in category_filter:
                    continue
            elif line_cat != category_filter:
                continue

        # Accumulate
        ytd = float(line.get("ytd_actual", 0) or 0)
        accrual = float(line.get("accrual_adj", 0) or 0)
        unpaid = float(line.get("unpaid_bills", 0) or 0)
        prior = float(line.get("prior_year", 0) or 0)
        curr_budget = float(line.get("current_budget", 0) or 0)
        proposed = float(line.get("proposed_budget", 0) or 0)

        # Per-line forecast
        line_forecast = compute_forecast(ytd, accrual, unpaid, prior, ytd_months)

        # If proposed_budget is 0 but we have a forecast + increase, compute it
        if proposed == 0 and line_forecast > 0:
            inc_pct = float(line.get("increase_pct", 0) or 0)
            proposed = line_forecast * (1 + inc_pct)

        totals["ytd_actual"] += ytd
        totals["accrual_adj"] += accrual
        totals["unpaid_bills"] += unpaid
        totals["prior_year"] += prior
        totals["current_budget"] += curr_budget
        totals["proposed_budget"] += proposed
        totals["forecast"] += line_forecast
        totals["matched_gl_codes"].append(gl)
        totals["line_count"] += 1

    # Estimate = forecast - ytd_total
    ytd_total = totals["ytd_actual"] + totals["accrual_adj"] + totals["unpaid_bills"]
    totals["estimate"] = totals["forecast"] - ytd_total

    return totals


# ── Build full summary ───────────────────────────────────────────────────

def build_summary(parsed_yrlycomp, budget_lines, re_taxes_data=None, ytd_months=2):
    """
    Build the complete Budget Summary from:
      - parsed_yrlycomp: output of budget_summary_parser.parse_yrlycomp()
      - budget_lines: list of BudgetLine dicts (from the app DB)
      - re_taxes_data: optional dict from compute_re_taxes()
      - ytd_months: how many months of actual data are loaded

    Returns list of summary rows, each with:
      - label, section, row_type
      - col1_prior_actual (from yrlycomp)
      - col2_ytd_actual (from budget_lines)
      - col3_estimate (= forecast - actual)
      - col4_forecast (GL prefix SUM)
      - col5_current_budget (from budget_lines)
      - col6_new_budget (proposed)
      - col7_dollar_var (= new_budget - forecast)
      - col8_pct_var (= dollar_var / forecast)
      - source_tab, gl_prefixes (metadata)
    """
    rows = parsed_yrlycomp.get("rows", [])
    budget_year = parsed_yrlycomp.get("budget_year", 2026)
    columns = parsed_yrlycomp.get("columns", [])

    # Find the prior year actual column (last "audited_actual" year)
    prior_actual_col = None
    for col in reversed(columns):
        if col["type"] == "audited_actual":
            prior_actual_col = col["col_num"]
            break

    summary_rows = []

    # Section accumulators for subtotals
    section_sums = {
        "income": [],
        "expenses": [],
        "non_operating_income": [],
        "non_operating_expense": [],
    }

    for row in rows:
        label = row["label"]
        row_type = row["row_type"]

        # Section headers pass through
        if row_type == "section_header":
            summary_rows.append({
                "label": label,
                "row_type": "section_header",
                "section": row.get("section"),
                "col1": None, "col2": None, "col3": None, "col4": None,
                "col5": None, "col6": None, "col7": None, "col8": None,
                "source_tab": None, "gl_prefixes": None,
            })
            continue

        # Look up this row in SUMMARY_ROW_MAP (with alias fallback)
        row_config = SUMMARY_ROW_MAP.get(label)
        if not row_config:
            # Try alias → canonical label
            canonical = LABEL_ALIASES.get(label)
            if canonical and canonical != label:
                row_config = SUMMARY_ROW_MAP.get(canonical)
        if not row_config:
            # Try condo-specific rows
            row_config = _CONDO_ROWS.get(label)

        # Col 1: Prior year actual from yrlycomp
        col1 = None
        if prior_actual_col is not None:
            col1 = row["values"].get(str(prior_actual_col)) or row["values"].get(prior_actual_col)

        if row_type == "data" and row_config:
            special = row_config.get("special", "")
            source_tab = row_config.get("sheet") or "Manual"
            gl_prefixes = row_config.get("gl_prefix", [])

            # ── RE Taxes special handling ──
            if special == "re_taxes_gross" and re_taxes_data:
                col4 = re_taxes_data.get("gross_tax", 0)
                col2 = re_taxes_data.get("first_half_tax", 0)  # approximate ytd
                col3 = col4 - col2
                col5 = 0  # current budget from budget_lines if available
                col6 = re_taxes_data.get("gross_tax", 0)  # proposed = projected
                source_tab = "RE Taxes"
            elif special == "re_taxes_credits" and re_taxes_data:
                col4 = -(re_taxes_data.get("total_exemptions_budget", 0))
                col2 = -(re_taxes_data.get("total_exemptions_budget", 0)) * (ytd_months / 12)
                col3 = col4 - col2
                col5 = 0
                col6 = col4
                source_tab = "RE Taxes"
            elif special == "re_taxes_credits_expense" and re_taxes_data:
                col4 = -(re_taxes_data.get("total_exemptions_budget", 0))
                col2 = -(re_taxes_data.get("total_exemptions_budget", 0)) * (ytd_months / 12)
                col3 = col4 - col2
                col5 = 0
                col6 = col4
                source_tab = "RE Taxes"
            elif special in ("manual", "unmapped_gl") or not gl_prefixes:
                # Manual/FA entry rows — use yrlycomp values as placeholder
                # In the live app, these will be FA-editable fields
                col2 = _yrly_val(row, columns, "partial_actual")
                col3 = _yrly_val(row, columns, "estimate")
                col4 = _yrly_val(row, columns, "forecast")
                col5 = _yrly_val(row, columns, "budget", budget_year - 1)
                col6 = _yrly_val(row, columns, "budget", budget_year)
                source_tab = "Manual"
            else:
                # ── Standard GL prefix aggregation ──
                agg = aggregate_lines_for_row(budget_lines, row_config, ytd_months)

                if agg["line_count"] > 0:
                    col2 = agg["ytd_actual"]
                    col3 = agg["estimate"]
                    col4 = agg["forecast"]
                    col5 = agg["current_budget"]
                    col6 = agg["proposed_budget"]
                else:
                    # No matching budget_lines — fall back to yrlycomp values
                    col2 = _yrly_val(row, columns, "partial_actual")
                    col3 = _yrly_val(row, columns, "estimate")
                    col4 = _yrly_val(row, columns, "forecast")
                    col5 = _yrly_val(row, columns, "budget", budget_year - 1)
                    col6 = _yrly_val(row, columns, "budget", budget_year)

            # Col 7 & 8: Variance
            col7 = (col6 or 0) - (col4 or 0)
            col8 = (col7 / col4 * 100) if col4 and col4 != 0 else None

            summary_row = {
                "label": label,
                "row_type": "data",
                "section": row.get("section"),
                "footnote_marker": row.get("footnote_marker"),
                "col1": col1,
                "col2": round(col2 or 0, 2),
                "col3": round(col3 or 0, 2),
                "col4": round(col4 or 0, 2),
                "col5": round(col5 or 0, 2),
                "col6": round(col6 or 0, 2),
                "col7": round(col7, 2),
                "col8": round(col8, 1) if col8 is not None else None,
                "source_tab": source_tab,
                "gl_prefixes": gl_prefixes,
            }
            summary_rows.append(summary_row)

            # Track for subtotals
            section = row_config.get("section", "")
            if section in section_sums:
                section_sums[section].append(summary_row)

        elif row_type == "data" and not row_config:
            # Row exists in yrlycomp but not in our map — building-specific
            # Use yrlycomp values and mark as manual
            col2 = _yrly_val(row, columns, "partial_actual")
            col3 = _yrly_val(row, columns, "estimate")
            col4 = _yrly_val(row, columns, "forecast")
            col5 = _yrly_val(row, columns, "budget", budget_year - 1)
            col6 = _yrly_val(row, columns, "budget", budget_year)
            col7 = (col6 or 0) - (col4 or 0)
            col8 = (col7 / col4 * 100) if col4 and col4 != 0 else None

            summary_row = {
                "label": label,
                "row_type": "data",
                "section": row.get("section"),
                "footnote_marker": row.get("footnote_marker"),
                "col1": col1,
                "col2": round(col2 or 0, 2),
                "col3": round(col3 or 0, 2),
                "col4": round(col4 or 0, 2),
                "col5": round(col5 or 0, 2),
                "col6": round(col6 or 0, 2),
                "col7": round(col7, 2),
                "col8": round(col8, 1) if col8 is not None else None,
                "source_tab": "Unknown",
                "gl_prefixes": [],
            }
            summary_rows.append(summary_row)

            # Try to guess section from yrlycomp position
            section = row.get("section", "")
            if section:
                sec_key = _section_to_key(section)
                if sec_key in section_sums:
                    section_sums[sec_key].append(summary_row)

        elif row_type == "subtotal":
            # Compute subtotals from accumulated data rows
            subtotal_row = _compute_subtotal(label, row, columns, prior_actual_col,
                                              section_sums, budget_year)
            summary_rows.append(subtotal_row)

    return {
        "entity_code": parsed_yrlycomp.get("entity_code"),
        "building_name": parsed_yrlycomp.get("building_name"),
        "budget_year": budget_year,
        "ytd_months": ytd_months,
        "rows": summary_rows,
        "stats": {
            "total_rows": len(summary_rows),
            "data_rows": len([r for r in summary_rows if r["row_type"] == "data"]),
            "subtotal_rows": len([r for r in summary_rows if r["row_type"] == "subtotal"]),
        }
    }


# ── Helpers ──────────────────────────────────────────────────────────────

def _yrly_val(row, columns, col_type, year=None):
    """Extract a value from yrlycomp row by column type (and optionally year)."""
    for col in columns:
        if col["type"] == col_type:
            if year is None or col["year"] == year:
                val = row["values"].get(str(col["col_num"])) or row["values"].get(col["col_num"])
                if val is not None and not isinstance(val, str):
                    return float(val)
    return 0.0


def _section_to_key(section_label):
    """Map yrlycomp section label to our internal key."""
    if not section_label:
        return ""
    sl = section_label.lower().strip()
    if sl == "income":
        return "income"
    elif sl == "expenses":
        return "expenses"
    elif "non-operating income" in sl or "non- operating income" in sl:
        return "non_operating_income"
    elif "non-operating expense" in sl or "non- operating expense" in sl:
        return "non_operating_expense"
    return ""


def _compute_subtotal(label, row, columns, prior_actual_col, section_sums, budget_year):
    """Compute a subtotal row by summing its constituent data rows."""
    label_lower = label.lower()

    # Determine which section this subtotal covers
    if "total income" in label_lower:
        data_rows = section_sums.get("income", [])
    elif "total expenses" in label_lower and "non" not in label_lower:
        data_rows = section_sums.get("expenses", [])
    elif "net operating" in label_lower:
        # Net Operating = Total Income - Total Expenses
        inc_rows = section_sums.get("income", [])
        exp_rows = section_sums.get("expenses", [])
        return _make_net_row(label, row, prior_actual_col, inc_rows, exp_rows, "net_operating")
    elif "non" in label_lower and "operating income" in label_lower:
        data_rows = section_sums.get("non_operating_income", [])
    elif "non" in label_lower and "operating expense" in label_lower:
        data_rows = section_sums.get("non_operating_expense", [])
    elif "total surplus" in label_lower or "total deficit" in label_lower:
        # Grand total = Net Operating + Non-Op Income - Non-Op Expenses
        return _make_grand_total(label, row, prior_actual_col, section_sums)
    else:
        data_rows = []

    # Sum the data rows
    col1 = row["values"].get(str(prior_actual_col)) or row["values"].get(prior_actual_col) if prior_actual_col else None
    sums = {"col2": 0, "col3": 0, "col4": 0, "col5": 0, "col6": 0}
    for dr in data_rows:
        for k in sums:
            sums[k] += dr.get(k, 0) or 0

    col7 = sums["col6"] - sums["col4"]
    col8 = (col7 / sums["col4"] * 100) if sums["col4"] != 0 else None

    return {
        "label": label,
        "row_type": "subtotal",
        "section": row.get("section"),
        "col1": col1,
        "col2": round(sums["col2"], 2),
        "col3": round(sums["col3"], 2),
        "col4": round(sums["col4"], 2),
        "col5": round(sums["col5"], 2),
        "col6": round(sums["col6"], 2),
        "col7": round(col7, 2),
        "col8": round(col8, 1) if col8 is not None else None,
        "source_tab": None,
        "gl_prefixes": None,
    }


def _make_net_row(label, row, prior_actual_col, inc_rows, exp_rows, row_subtype):
    """Net Operating = Total Income - Total Expenses."""
    col1 = row["values"].get(str(prior_actual_col)) or row["values"].get(prior_actual_col) if prior_actual_col else None

    net = {}
    for k in ["col2", "col3", "col4", "col5", "col6"]:
        inc_sum = sum(r.get(k, 0) or 0 for r in inc_rows)
        exp_sum = sum(r.get(k, 0) or 0 for r in exp_rows)
        net[k] = inc_sum - exp_sum

    col7 = net["col6"] - net["col4"]
    col8 = (col7 / net["col4"] * 100) if net["col4"] != 0 else None

    return {
        "label": label,
        "row_type": "subtotal",
        "subtype": row_subtype,
        "section": row.get("section"),
        "col1": col1,
        "col2": round(net["col2"], 2),
        "col3": round(net["col3"], 2),
        "col4": round(net["col4"], 2),
        "col5": round(net["col5"], 2),
        "col6": round(net["col6"], 2),
        "col7": round(col7, 2),
        "col8": round(col8, 1) if col8 is not None else None,
        "source_tab": None,
        "gl_prefixes": None,
    }


def _make_grand_total(label, row, prior_actual_col, section_sums):
    """Grand total = Net Operating + Non-Op Income - Non-Op Expenses."""
    col1 = row["values"].get(str(prior_actual_col)) or row["values"].get(prior_actual_col) if prior_actual_col else None

    inc = section_sums.get("income", [])
    exp = section_sums.get("expenses", [])
    noi = section_sums.get("non_operating_income", [])
    noe = section_sums.get("non_operating_expense", [])

    grand = {}
    for k in ["col2", "col3", "col4", "col5", "col6"]:
        inc_sum = sum(r.get(k, 0) or 0 for r in inc)
        exp_sum = sum(r.get(k, 0) or 0 for r in exp)
        noi_sum = sum(r.get(k, 0) or 0 for r in noi)
        noe_sum = sum(r.get(k, 0) or 0 for r in noe)
        grand[k] = (inc_sum - exp_sum) + noi_sum - noe_sum

    col7 = grand["col6"] - grand["col4"]
    col8 = (col7 / grand["col4"] * 100) if grand["col4"] != 0 else None

    return {
        "label": label,
        "row_type": "subtotal",
        "subtype": "grand_total",
        "section": row.get("section"),
        "col1": col1,
        "col2": round(grand["col2"], 2),
        "col3": round(grand["col3"], 2),
        "col4": round(grand["col4"], 2),
        "col5": round(grand["col5"], 2),
        "col6": round(grand["col6"], 2),
        "col7": round(col7, 2),
        "col8": round(col8, 1) if col8 is not None else None,
        "source_tab": None,
        "gl_prefixes": None,
    }


# ── Tab resolution (for display) ────────────────────────────────────────

TAB_COLORS = {
    "Income":            {"bg": "rgba(76,175,80,0.18)",  "color": "#81c784"},
    "Payroll":           {"bg": "rgba(33,150,243,0.18)", "color": "#64b5f6"},
    "Energy":            {"bg": "rgba(255,152,0,0.18)",  "color": "#ffb74d"},
    "Water & Sewer":     {"bg": "rgba(0,188,212,0.18)",  "color": "#4dd0e1"},
    "Repairs & Supplies":{"bg": "rgba(121,85,72,0.18)",  "color": "#a1887f"},
    "Gen & Admin":       {"bg": "rgba(156,39,176,0.18)", "color": "#ba68c8"},
    "RE Taxes":          {"bg": "rgba(244,67,54,0.18)",  "color": "#e57373"},
    "Manual":            {"bg": "rgba(255,213,79,0.18)", "color": "#ffd54f"},
    "Unknown":           {"bg": "rgba(158,158,158,0.18)","color": "#bdbdbd"},
}

TAB_SHORT = {
    "Income": "Income",
    "Payroll": "Payroll",
    "Energy": "Energy",
    "Water & Sewer": "Water",
    "Repairs & Supplies": "R&S",
    "Gen & Admin": "Gen&Admin",
    "RE Taxes": "RE Tax",
    "Manual": "Manual",
    "Unknown": "?",
}


# ── Standalone test ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    from budget_summary_parser import parse_yrlycomp

    # Parse 204 yrlycomp
    filepath_204 = "/sessions/ecstatic-sleepy-thompson/mnt/Budgets/budget_app/204 -  444 East 86th Street 2026 Operating Budget  - Approved.xlsx"
    parsed = parse_yrlycomp(filepath_204)

    # No budget_lines from DB in standalone mode — use empty list
    # The engine will fall back to yrlycomp values for all rows
    budget_lines = []

    result = build_summary(parsed, budget_lines, ytd_months=8)

    print(f"\n{'='*80}")
    print(f"BUDGET SUMMARY: {result['building_name']} (Entity {result['entity_code']})")
    print(f"Budget Year: {result['budget_year']} | YTD Months: {result['ytd_months']}")
    print(f"Stats: {result['stats']}")
    print(f"{'='*80}\n")

    for row in result["rows"]:
        if row["row_type"] == "section_header":
            print(f"\n  [{row['label']}]")
        elif row["row_type"] == "subtotal":
            print(f"  {'─'*60}")
            tab = ""
            c1 = f"{row['col1']:>12,.0f}" if row['col1'] else f"{'':>12}"
            c4 = f"{row['col4']:>12,.0f}" if row['col4'] else f"{'':>12}"
            c6 = f"{row['col6']:>12,.0f}" if row['col6'] else f"{'':>12}"
            print(f"  {row['label']:<35} {c1} {c4} {c6}")
        else:
            tab = f"[{TAB_SHORT.get(row['source_tab'], '?'):>8}]"
            c1 = f"{row['col1']:>12,.0f}" if row['col1'] else f"{'':>12}"
            c4 = f"{row['col4']:>12,.0f}" if row['col4'] else f"{'':>12}"
            c6 = f"{row['col6']:>12,.0f}" if row['col6'] else f"{'':>12}"
            pct = f"{row['col8']:>6.1f}%" if row['col8'] is not None else f"{'—':>7}"
            print(f"  {tab} {row['label']:<35} {c1} {c4} {c6} {pct}")

    # Save JSON
    with open("/sessions/ecstatic-sleepy-thompson/mnt/Budgets/budget_summary/204_summary_output.json", "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\nJSON saved to 204_summary_output.json")
