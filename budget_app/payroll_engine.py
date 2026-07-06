"""Pure payroll computation engine (dry-run suggestion 3, 2026-07-06).

Faithful Python port of the FA dashboard's client-side payroll math
(recalcPayroll + applyWageIncrease + PAYROLL_COMPONENT_MAP in
page_templates/building_detail.py). The browser remains the editor; this
module exists so the union math is TESTABLE server-side (test vectors like
budget_math/summary_engine) and so parity between tab and server can be
checked on any building via GET /api/payroll/compute/<ec>.

Keep the two implementations in lockstep: any change to the JS math must
land here (and in test_payroll_engine.py's expected values) in the same
commit — the deploy test will fail loudly otherwise.

No flask, no SQLAlchemy: deterministic over its inputs.
"""

# GL -> component linkage (mirror of PAYROLL_COMPONENT_MAP in the JS).
PAYROLL_COMPONENT_MAP = {
    "5105-0000": "annual_base",      # Gross Payroll
    "5105-0010": "ot",               # Overtime Pay
    "5105-0015": "vsh_vacation",     # Vacation Pay (1/3 of VSH)
    "5105-0020": "vsh_holiday",      # Holiday Pay (1/3 of VSH)
    "5105-0025": "vsh_sick",         # Sick Pay (1/3 of VSH)
    "5105-0035": "bonus",            # Bonus
    "5145-0000": "employer_taxes",   # Employer Payroll Taxes (FICA+SUI+FUI+MTA)
    "5165-0000": "workers_comp",     # Workers Comp Insurance
    "5166-0000": "nys_disability",   # Disability Insurance
    "5168-0000": "pfl",              # Paid Family Leave
    "5160-0015": "welfare",          # Health Fund (union welfare calc; FA #24)
    "5160-0010": "pension",          # Pension Fund
    "5160-0020": "supp_retirement",  # Annuity Fund
    "5160-0025": "legal_fund",       # Legal Fund
    "5160-0030": "training_fund",    # Training Fund
    "5160-0035": "profit_sharing",   # Profit Sharing
}


def apply_wage_increase(rate, mode, value):
    """Mirror of applyWageIncrease: 'dollar' adds $/hr, anything else is pct."""
    m = "dollar" if mode == "dollar" else "pct"
    v = value or 0
    return (rate + v) if m == "dollar" else rate * (1 + v)


def _num(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float(default)


def compute_payroll(positions, assumptions, overrides=None):
    """Mirror of recalcPayroll. positions = PayrollPosition.to_dict() dicts;
    assumptions = the PayrollAssumption 'assumptions' dict; overrides = the
    FA per-cell override map {cell_key: value}.

    Returns {"positions": [...], "components": {...}, "computed": {...},
             "totals": {...}} — components carry FA overrides (what the GL
    push uses); computed carries pre-override values (lineage panel truth).
    """
    a = assumptions or {}
    ov = overrides or {}

    global_mode = a.get("wage_increase_mode") or "pct"
    global_value = a.get("wage_increase_value")
    if global_value is None:
        global_value = a.get("wage_increase_pct") or 0
    pre_wks = _num(a.get("pre_increase_weeks"), 15) or 15
    post_wks = _num(a.get("post_increase_weeks"), 37) or 37
    ot_factor = _num(a.get("ot_factor"), 0.002) or 0.002
    vsh_factor = _num(a.get("vac_sick_hol_factor"), 0.10) or 0.10

    total_employees = 0
    total_annual_base = 0.0
    total_ot = 0.0
    total_vsh = 0.0
    total_comp = 0.0
    total_bonus = 0.0

    pos_calcs = []
    for p in positions or []:
        count = int(p.get("employee_count") or 0)
        rate = _num(p.get("hourly_rate"))
        bonus_per_emp = _num(p.get("bonus_per_employee"))
        pos_pre, pos_post = pre_wks, post_wks
        eff = _num(p.get("effective_week_override"))
        if eff and eff > 0:
            pos_pre = max(eff - 1, 0)
            pos_post = 52 - pos_pre
        pos_mode = p.get("wage_increase_mode") or global_mode
        pos_value = p.get("wage_increase_value")
        if pos_value is None:
            pos_value = global_value
        weekly_pay = rate * 40
        pre_wages = weekly_pay * pos_pre * count
        post_rate = apply_wage_increase(rate, pos_mode, pos_value)
        post_wages = (post_rate * 40) * pos_post * count
        annual_base = pre_wages + post_wages
        ot = annual_base * ot_factor
        vsh = annual_base * vsh_factor
        bonus = bonus_per_emp * count
        extras = p.get("extra_bonuses") or []
        if isinstance(extras, list):
            for e in extras:
                amt = _num((e or {}).get("amount"))
                basis = (e or {}).get("basis")
                if basis == "per_emp":
                    bonus += amt * count
                elif basis == "lump":
                    bonus += amt
                elif basis == "pct_wages":
                    bonus += amt * annual_base
        comp = annual_base + ot + vsh

        total_employees += count
        total_annual_base += annual_base
        total_ot += ot
        total_vsh += vsh
        total_comp += comp
        total_bonus += bonus

        pos_calcs.append({
            "position_name": p.get("position_name"), "count": count,
            "rate": rate, "pre_wks": pos_pre, "post_wks": pos_post,
            "post_rate": post_rate, "annual_base": annual_base,
            "ot": ot, "vsh": vsh, "bonus": bonus, "comp": comp,
        })

    gross_wages = total_annual_base + total_ot + total_vsh
    fica = gross_wages * _num(a.get("fica"))
    sui = 12000 * _num(a.get("sui")) * total_employees
    fui = 7000 * _num(a.get("fui")) * total_employees
    mta = gross_wages * _num(a.get("mta"))
    nys_dis = _num(a.get("nys_disability")) * total_employees
    pfl = gross_wages * _num(a.get("pfl"))
    wc = _num(a.get("workers_comp")) * gross_wages

    adj = {"welfare": 0.0, "pension": 0.0, "supp_retirement": 0.0,
           "legal": 0.0, "training": 0.0, "profit_sharing": 0.0}
    for p in positions or []:
        block = p.get("benefit_adjustments") or {}
        benefits = (block or {}).get("benefits") or {}
        if not benefits:
            continue
        cnt = min(max(int(_num(block.get("adjusted_count"))), 0),
                  int(p.get("employee_count") or 0))
        if cnt <= 0:
            continue
        for key in adj:
            b = benefits.get(key)
            if b:
                adj[key] += _num(b.get("rate")) * _num(b.get("periods")) * cnt

    welfare = _num(a.get("welfare_monthly")) * total_employees * 12 + adj["welfare"]
    pension = _num(a.get("pension_weekly")) * total_employees * 52 + adj["pension"]
    supp_ret = _num(a.get("supp_retirement_weekly")) * total_employees * 52 + adj["supp_retirement"]
    legal = _num(a.get("legal_monthly")) * total_employees * 12 + adj["legal"]
    training = _num(a.get("training_monthly")) * total_employees * 12 + adj["training"]
    profit_share = _num(a.get("profit_sharing_quarterly")) * total_employees * 4 + adj["profit_sharing"]

    computed = {
        "fica": fica, "sui": sui, "fui": fui, "mta": mta,
        "nys_disability": nys_dis, "pfl": pfl, "workers_comp": wc,
        "welfare": welfare, "pension": pension, "supp_retirement": supp_ret,
        "legal": legal, "training": training, "profit_sharing": profit_share,
        "total_payroll_tax": fica + sui + fui + mta + nys_dis + pfl,
        "total_union": welfare + pension + supp_ret + legal + training + profit_share,
    }
    computed["total_labor"] = (gross_wages + computed["total_payroll_tax"]
                               + wc + computed["total_union"])

    def _ov(key, fallback):
        v = ov.get(key)
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return fallback
        return fv

    o_fica = _ov("fica", fica)
    o_sui = _ov("sui", sui)
    o_fui = _ov("fui", fui)
    o_mta = _ov("mta", mta)
    o_nys = _ov("nys_disability", nys_dis)
    o_pfl = _ov("pfl", pfl)
    o_wc = _ov("workers_comp", wc)
    o_welfare = _ov("welfare", welfare)
    o_pension = _ov("pension", pension)
    o_supp = _ov("supp_retirement", supp_ret)
    o_legal = _ov("legal", legal)
    o_training = _ov("training", training)
    o_profit = _ov("profit_sharing", profit_share)
    o_tax_total = _ov("total_payroll_tax", o_fica + o_sui + o_fui + o_mta + o_nys + o_pfl)
    o_union_total = _ov("total_union", o_welfare + o_pension + o_supp + o_legal + o_training + o_profit)
    total_labor = _ov("total_labor", gross_wages + o_tax_total + o_wc + o_union_total)

    components = {
        "annual_base": total_annual_base,
        "ot": total_ot,
        "vsh_vacation": total_vsh / 3,
        "vsh_holiday": total_vsh / 3,
        "vsh_sick": total_vsh / 3,
        "bonus": total_bonus,
        "employer_taxes": o_fica + o_sui + o_fui + o_mta,
        "workers_comp": o_wc,
        "nys_disability": o_nys,
        "pfl": o_pfl,
        "welfare": o_welfare,
        "pension": o_pension,
        "supp_retirement": o_supp,
        "legal_fund": o_legal,
        "training_fund": o_training,
        "profit_sharing": o_profit,
    }

    return {
        "positions": pos_calcs,
        "components": components,
        "computed": computed,
        "totals": {
            "employees": total_employees,
            "annual_base": total_annual_base,
            "ot": total_ot, "vsh": total_vsh, "bonus": total_bonus,
            "gross_wages": gross_wages,
            "total_payroll_tax": o_tax_total,
            "total_union": o_union_total,
            "total_labor": total_labor,
        },
    }


def roster_gl_values(components, gl_lines):
    """Mirror of pushRosterToGL's selection: which GL lines the roster would
    write, and the values. Skips lines with a proposed_formula (manual
    override wins) and returns nothing for an empty components dict —
    mirroring the 437 empty-roster wipe guard.
    """
    if not components:
        return []
    out = []
    for line in gl_lines or []:
        gl = line.get("gl_code")
        key = PAYROLL_COMPONENT_MAP.get(gl)
        if not key or key not in components:
            continue
        if line.get("proposed_formula"):
            continue
        new_proposed = round(components[key])
        curr = _num(line.get("current_budget"))
        out.append({
            "gl_code": gl,
            "component": key,
            "proposed_budget": new_proposed,
            "increase_pct": (new_proposed / curr - 1) if curr else 0,
            "stored_proposed": line.get("proposed_budget"),
        })
    return out
