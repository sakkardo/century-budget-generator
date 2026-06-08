"""Pure GL-classification + budget-category constants.

Architecture Phase 3, step 2 (2026-06-08). Extracted verbatim from workflow.py
(no behavior change). Pure data + one pure helper (no db, no app context);
workflow.py imports them back so every existing reference resolves unchanged.
"""

# Income GLs whose 12-month forecast is pinned to the approved budget rather than
# an annualized-from-YTD figure. Matched by GL prefix 4200 — this deliberately
# EXCLUDES Capital Assessment and Tax-Abatement Assessment, which are unmapped
# manual Summary rows (no GL), not GL-4200 operating assessments.
# Stored as 4-digit bases to match _gl_matches_prefixes behavior.
FIXED_FORECAST_GL_BASES = {"4010", "4020", "4030", "4040", "4200"}
FIXED_FORECAST_GL_FULL = [
    "4010-0000", "4020-0000", "4020-0005",
    "4030-0000", "4040-0000", "4040-0010",
    "4200-0000", "4200-0005", "4200-0010",
]


def _row_has_fixed_forecast_gl(gl_prefixes_json):
    """Check if a summary row's stored prefixes intersect the fixed-forecast set."""
    if not gl_prefixes_json:
        return False
    try:
        import json as _j
        prefixes = _j.loads(gl_prefixes_json)
    except Exception:
        return False
    if not isinstance(prefixes, list):
        return False
    for p in prefixes:
        if not p:
            continue
        base = str(p).split("-")[0].strip()
        if base in FIXED_FORECAST_GL_BASES:
            return True
    return False


# Comprehensive mapping: budget_line category → Century audit category
BUDGET_CAT_TO_CENTURY = {
    "supplies": "Supplies",
    "repairs": "Repairs & Maintenance",
    "maintenance": "Repairs & Maintenance",
    "payroll": "Payroll",
    "electric": "Electric",
    "gas": "Gas Cooking / Heating",
    "fuel": "Fuel",
    "oil": "Fuel",
    "water": "Water & Sewer",
    "sewer": "Water & Sewer",
    "insurance": "Insurance",
    "re_taxes": "Real Estate Taxes",
    "professional": "Professional Fees",
    "admin": "Administrative & Other",
    "financial": "Financial Expenses",
}
