"""
GL Code → Budget Summary Row Mapping

This is the config that tells the Budget Summary tab how to aggregate
GL-level budget_lines into yrlycomp summary rows.

Each entry: summary_row_label → { sheet_names, gl_prefixes, category_filter }
The app will SUM(budget_lines) matching these filters to produce each row's value.
"""

# ─── LABEL ALIASES ───────────────────────────────────────────────────────
# Condo/rental buildings use different labels for the same summary rows.
# Maps variant label → canonical label used as key in SUMMARY_ROW_MAP.
LABEL_ALIASES = {
    # Income variants
    "Common Charges": "Maintenance",
    "Common Charges (License Fee)": "Maintenance",     # sub-line, same GL
    "Common Charges - Storage Room": "Storage Income",
    "Common Charges - Parking": "Garage",
    # FA #9 (2026-05-03): yrlycomp uses bare "Storage" / "Bicycle" labels;
    # canonical map keys are "Storage Income" / "Bicycle Charge". Without
    # these aliases the rows render with no GL prefix → empty col3-5.
    "Storage": "Storage Income",
    "Bicycle": "Bicycle Charge",
    "Bike": "Bicycle Charge",
    "Bike Storage": "Bicycle Charge",
    # FA directive 2026-05-13 (from 148 orphan investigation): the yrlycomp
    # for 148 (and likely other coops) uses these label variants. Without
    # the alias, resolve-aliases can't stamp GL prefixes on the row → the
    # FA sees an empty row and "Add Row" silently no-ops because the row
    # already exists.
    "Bicycle Storage": "Bicycle Charge",
    "Commercial Rent": "Commercial",
    # Assessment / commercial label variants seen on 168
    "Assessment-Operating": "Assessment - Operating",
    # Energy variants
    "Gas - Heating": "Gas Cooking / Heating",
    "Gas Heating": "Gas Cooking / Heating",
    "Gas": "Gas Cooking / Heating",
    "Oil / Fuel": "Fuel",
    "Fuel Oil": "Fuel",
    "Oil": "Fuel",
    "Heating Oil": "Fuel",
    # RE Taxes variants
    "Real Estate Taxes/Supers Unit": "Real Estate Taxes",
    "RE Taxes": "Real Estate Taxes",
    "Real Estate Tax": "Real Estate Taxes",
    # Tax Benefit Credits (Income side) — variants seen across yrlycomps.
    # Each maps to the canonical "Tax Benefit Credits (Abatement, Star,etc)" row
    # which uses GL prefixes 4105–4125 (income side only).
    "Tax Benefit Credits": "Tax Benefit Credits (Abatement, Star,etc)",
    "Tax Benefit Credits (Abatement, STAR, etc.)": "Tax Benefit Credits (Abatement, Star,etc)",
    "Tax Benefit Credits (Abatement, STAR, etc)": "Tax Benefit Credits (Abatement, Star,etc)",
    "Tax Abatement Credits": "Tax Benefit Credits (Abatement, Star,etc)",
    "Tax Abatements": "Tax Benefit Credits (Abatement, Star,etc)",
    # Non-operating variants
    "Transfer to Reserve": "Transfer to Reserve",       # condo-specific, keep as-is with config below
    "Reserve Funding": "Reserve Funding",               # condo-specific
    "Working Capital Contribution": "Working Capital Contribution",
    "Claims Proceeds - Insurance Repairs": "Insurance Proceeds",
    "Insurance Repairs": "Capital Expenses",            # non-op expense variant
}

# Condo-specific rows that don't exist in co-op map — add them here
_CONDO_ROWS = {
    "Common Charges (License Fee)": {
        "sheet": "Income",
        "gl_prefix": ["4010"],  # Same GL as Common Charges, sub-account
        "section": "income",
        "notes": "Condo license fee portion of common charges."
    },
    "Transfer to Reserve": {
        "sheet": None,
        "gl_prefix": [],
        "section": "expenses",
        "special": "manual",
        "notes": "Condo board reserve allocation. FA-entered."
    },
    "Reserve Funding": {
        "sheet": None,
        "gl_prefix": [],
        "section": "non_operating_income",
        "special": "manual",
        "notes": "Condo reserve funding (mirrors Transfer to Reserve). FA-entered."
    },
    "Working Capital Contribution": {
        "sheet": None,
        "gl_prefix": [],
        "section": "non_operating_income",
        "special": "manual",
        "notes": "Condo working capital contributions. FA-entered."
    },
    "Claims Proceeds - Insurance Repairs": {
        "sheet": None,
        "gl_prefix": [],
        "section": "non_operating_income",
        "special": "manual",
        "notes": "Insurance claim proceeds. Building-specific."
    },
    "Insurance Repairs": {
        "sheet": None,
        "gl_prefix": [],
        "section": "non_operating_expense",
        "special": "manual",
        "notes": "Insurance repair expenses. Building-specific."
    },
}


# Master mapping: yrlycomp summary row → how to aggregate from budget_lines
SUMMARY_ROW_MAP = {

    # ─── INCOME ──────────────────────────────────────────────────────────
    "Maintenance": {
        "sheet": "Income",
        "gl_prefix": ["4010"],
        "section": "income",
        "notes": "Primary revenue. building_assumptions has shares × rate for forecast."
    },
    "Tax Benefit Credits (Abatement, Star,etc)": {
        "sheet": "RE Taxes",
        # FA directive 2026-05-05: this Income-section row pulls ONLY income-side
        # tax credit GLs in range 4105-4125 (full integer range). Previous version
        # also included 6315, but 6315 is expense-side (Real Estate Tax) and was
        # polluting Income with $361k of expense data on 168 (and $1.1M on 148).
        # Expense-side credits live separately under "Real Estate Tax Benefit Credits".
        # Standard GLs in this range: 4105 (RE Tax Abatement), 4110 (STAR),
        # 4115 (Veterans), 4120 (SCRIE), 4125 (SCHE). Full range covers any
        # non-standard sub-codes future buildings might use.
        "gl_prefix": [str(n) for n in range(4105, 4126)],
        "section": "income",
        "special": "re_taxes_credits",  # Pull from dof_taxes.total_exemptions_budget, negate
        "notes": "Computed by RE Taxes tab. Show as negative. "
                 "Income-side ONLY: GLs 4105–4125 (4105 RE Tax Abatement, 4110 STAR, "
                 "4115 Veterans, 4120 SCRIE, 4125 SCHE). "
                 "Expense-side credits go under 'Real Estate Tax Benefit Credits' (6315)."
    },
    "Commercial": {
        "sheet": "Income",
        "gl_prefix": ["4040"],
        "section": "income",
        "notes": "Commercial rent. May also pull from Comm Rent & Escalations sheet."
    },
    "Garage": {
        "sheet": "Income",
        "gl_prefix": ["4135", "4520"],
        "section": "income",
        "notes": "Garage/parking income. 4135=garage contract, 4520=parking."
    },
    "Commercial Real Estate Tax": {
        "sheet": "Income",
        "gl_prefix": ["4045"],  # varies by building
        "section": "income",
        "notes": "Not all buildings have this. Zero for most."
    },
    "Storage Income": {
        "sheet": "Income",
        # Use full sub-account codes to avoid greedy match on "4130" claiming
        # 4130-0015 (Bicycle) and 4130-0030 (Laundry).
        "gl_prefix": ["4130-0000", "4130-0010", "4130-0020"],
        "section": "income",
        "notes": "building_assumptions has unit count × rate × occupancy. 4130-0010 = Storage Room Rental seen on 168."
    },
    "Bicycle Charge": {
        "sheet": "Income",
        "gl_prefix": ["4130-0015", "4132"],  # 4130-0015 seen on 168; 4132 legacy
        "section": "income",
        "notes": "building_assumptions has rack count × rate."
    },
    "Laundry": {
        "sheet": "Income",
        "gl_prefix": ["4130-0030", "4140", "4150"],   # 4130-0030 seen on 168
        "section": "income",
        "notes": "Laundry/vending. building_assumptions has monthly amount."
    },
    "Assessment - Operating": {
        "sheet": "Income",
        "gl_prefix": ["4200"],
        "section": "income",
        "notes": "Board decision. Often the 'plug' that balances the budget."
    },
    "Other Income": {
        "sheet": "Income",
        "gl_prefix": ["4070", "4250", "4700", "4705", "4710", "4715", "4720", "4725",
                       "4803", "4812", "4815", "4818", "4911", "4917", "4922",
                       "4926", "4932", "4956", "4990"],
        "section": "income",
        "notes": "Catch-all. SUM of all Income GL codes not covered above. "
                 "Added 2026-05-03 from 168 orphan scan: 4070 (Prepaid Income), "
                 "4818 (Flip Tax - Operating), 4911 (Messenger), 4917 (Credit Check), "
                 "4926 (Administrative Fees). "
                 "Added 2026-05-13 from 148 orphan scan: 4956 (Security Account Admin)."
    },

    # ─── EXPENSES ────────────────────────────────────────────────────────
    "Payroll": {
        "sheet": "Payroll",
        "gl_prefix": ["5000", "5010", "5015", "5020", "5025", "5030", "5035",
                       "5040", "5045", "5050", "5055", "5060", "5065", "5070",
                       "5100", "5105", "5110", "5115", "5120", "5125", "5130",
                       "5135", "5140", "5145", "5150", "5155", "5160", "5165",
                       "5166", "5168", "5170", "5172"],
        "section": "expenses",
        "notes": "SUM of entire Payroll sheet. Includes wages, benefits, taxes, workers comp. "
                 "FA #11 (2026-05-03): added 5168, 5172 (Payroll Processing) — were orphans on 168."
    },
    "Electric": {
        "sheet": "Energy",
        "gl_prefix": ["5255"],
        "section": "expenses",
        "notes": "Yardi GL 5255. Multi-meter buildings aggregate here."
    },
    "Gas Cooking / Heating": {
        "sheet": "Energy",
        "gl_prefix": ["5250", "5251", "5252"],
        "section": "expenses",
        "notes": "Yardi GL 5250 (Gas), 5251 (Gas Transport), 5252 (Gas - Heating)."
    },
    "Steam Heating": {
        "sheet": "Energy",
        "gl_prefix": ["5265"],
        "section": "expenses",
        "notes": "Yardi GL 5265. Steam from utility (e.g. Con Ed Steam)."
    },
    "Fuel": {
        "sheet": "Energy",
        "gl_prefix": ["5260"],
        "section": "expenses",
        "notes": "Yardi GL 5260. Oil/fuel. Not all buildings have this."
    },
    "Water & Sewer": {
        "sheet": "Water & Sewer",
        "gl_prefix": ["6305"],
        "section": "expenses",
        "notes": "Yardi GL 6305 (Water/Sewer)."
    },
    "Supplies": {
        "sheet": "Repairs & Supplies",
        "gl_prefix": [
            "5405", "5406", "5408", "5410", "5415", "5420", "5425", "5430",
            "5435", "5440", "5441", "5445", "5450", "5451", "5452", "5453",
            "5455", "5460", "5465", "5466", "5495"
        ],
        "category": "supplies",
        "section": "expenses",
        "notes": "All Yardi Supplies sub-category GL codes (5405-5495). Source: GL_Mapping.csv."
    },
    "Repairs & Maintenance": {
        "sheet": "Repairs & Supplies",
        # Comprehensive 5606-5874 range. The prior list missed odd-numbered
        # codes (5627, 5633, 5642, 5666, 5682, 5695, 5812, 5821, etc.) —
        # diagnosed via /api/admin/summary-debug/168 orphan scan 2026-05-03.
        # 2026-05-13: added 5622 (HVAC Repairs), 5636 (Air Conditioning Repairs),
        # 5670 (Intercom Repairs), 5678 (Roof Tank/Water Tank Repairs) — all
        # orphans on 148.
        "gl_prefix": [
            # Repairs (5606-5699)
            "5606", "5610", "5612", "5615", "5620", "5622", "5625", "5627", "5630",
            "5633", "5635", "5636", "5639", "5640", "5642", "5645", "5648", "5650",
            "5655", "5660", "5665", "5666", "5668", "5670", "5678", "5680", "5682", "5690",
            "5695",
            # Maintenance contracts (5800-5874)
            "5803", "5806", "5809", "5810", "5812", "5815", "5818", "5820",
            "5821", "5825", "5828", "5830", "5831", "5834", "5835", "5837",
            "5840", "5845", "5850", "5852", "5855", "5860", "5865", "5870",
            "5874",
        ],
        "category": ["repairs", "maintenance"],
        "section": "expenses",
        "notes": "SUM of repairs + maintenance categories from R&M sheet. "
                 "Prefix list expanded 2026-05-03 to cover odd-numbered sub-GLs. "
                 "Expanded again 2026-05-13 with 5622, 5636, 5670, 5678 (148 orphans)."
    },
    "Insurance": {
        "sheet": "Gen & Admin",
        # 2026-05-13: added 6145 (Errors & Omissions Insurance) — orphan on 148.
        "gl_prefix": ["6105", "6110", "6115", "6120", "6125", "6126", "6135",
                       "6145", "6180", "6195"],
        "section": "expenses",
        "gl_range": ("6100-0000", "6199-9999"),
        "notes": "All 61xx codes. building_assumptions has per-policy overrides. "
                 "Added 6145 (Errors & Omissions) 2026-05-13."
    },
    "Real Estate Taxes": {
        "sheet": "RE Taxes",
        "gl_prefix": ["6315"],
        "section": "expenses",
        "special": "re_taxes_gross",  # Pull from dof_taxes.gross_tax
        "notes": "Computed by RE Taxes tab. GL 6315-0000 = base tax."
    },
    "Real Estate Tax Benefit Credits": {
        "sheet": "RE Taxes",
        "gl_prefix": ["6315"],
        "section": "expenses",
        "special": "re_taxes_credits_expense",  # Pull from dof_taxes, negate
        "notes": "Same GL codes as income Tax Benefits but on expense side. Negative."
    },
    "Corporate Taxes": {
        "sheet": "Gen & Admin",
        "gl_prefix": ["6310"],
        "section": "expenses",
        "notes": "Corporate/franchise taxes."
    },
    "Professional Fees": {
        "sheet": "Gen & Admin",
        "gl_prefix": ["6505", "6510", "6515", "6520", "6525", "6590"],
        "section": "expenses",
        "gl_range": ("6500-0000", "6599-9999"),
        "notes": "Legal, accounting, management fees."
    },
    "Administrative & Other": {
        "sheet": "Gen & Admin",
        # FA #16 (2026-05-03): expanded from 5-step to full 6700-6799 range.
        # 168 had real data on 6714, 6718, 6722, 6726, 6728, 6734, 6738,
        # 6742, 6754, 6762, 6763, 6764, 6768, 6774 — all orphans before.
        # 2026-05-13: added 6708 (Stationery & Printing), 6712 (Computer
        # Software), 6716 (Cable TV Expense), 6746 (Lobby & Hallway
        # Decorations) — all orphans on 148.
        "gl_prefix": [
            "6706", "6708", "6710", "6712", "6714", "6715", "6716", "6718", "6720", "6722", "6725",
            "6726", "6728", "6730", "6734", "6735", "6738", "6740", "6742",
            "6745", "6746", "6750", "6754", "6755", "6760", "6762", "6763", "6764",
            "6765", "6768", "6770", "6774", "6775", "6780", "6785", "6790",
            "6795",
        ],
        "section": "expenses",
        "gl_range": ("6700-0000", "6899-9999"),
        "notes": "Office, telephone, permits, board expenses, misc admin. "
                 "Prefix list expanded 2026-05-03 from FA #16 orphan diagnosis. "
                 "Expanded again 2026-05-13 with 6708, 6712, 6716, 6746 (148 orphans)."
    },
    "Financial Expenses": {
        "sheet": "Gen & Admin",
        "gl_prefix": ["2510", "6905", "6925", "6995"],
        "section": "expenses",
        "gl_range": ("6900-0000", "6999-9999"),
        "notes": "Mortgage interest, bank fees. 2510 = mortgage note payable."
    },

    # ─── NON-OPERATING INCOME ────────────────────────────────────────────
    "Capital Assessment": {
        "sheet": None,
        "gl_prefix": [],
        "section": "non_operating_income",
        "special": "manual",
        "notes": "Board decision. No GL in most years. FA enters manually."
    },
    "Special Assessment": {
        "sheet": "Income",
        "gl_prefix": ["4200"],
        "section": "non_operating_income",
        "notes": "GL 4200-0000. The non-operating portion of assessment (vs operating portion above)."
    },
    "Interest Income": {
        "sheet": "Income",
        "gl_prefix": ["4800", "4803", "4812"],
        "section": "non_operating_income",
        "notes": "Bank interest on reserves. Via VLookup Data SUMIF."
    },
    "Insurance Proceeds": {
        "sheet": None,
        "gl_prefix": ["7045"],
        "section": "non_operating_income",
        "special": "unmapped_gl",  # Currently goes to Unmapped
        "notes": "GL 7045-0000. Not in current template. Needs routing."
    },
    "Real Estate Tax refund": {
        "sheet": None,
        "gl_prefix": ["4809"],
        "section": "non_operating_income",
        "notes": "Tax commission refunds. Often zero. GL 4809 if it exists."
    },
    "ICON Settlement Proceeds": {
        "sheet": None,
        "gl_prefix": [],
        "section": "non_operating_income",
        "special": "manual",
        "notes": "Building-specific one-time item. No GL. Manual entry."
    },
    "SBA - PPP Loan Proceeds": {
        "sheet": None,
        "gl_prefix": ["2520"],
        "section": "non_operating_income",
        "special": "unmapped_gl",  # Currently goes to Unmapped
        "notes": "GL 2520-0000. COVID-era. Usually zero going forward."
    },

    # ─── NON-OPERATING EXPENSE ───────────────────────────────────────────
    "Capital Expenses": {
        "sheet": "Capital",
        # FA #17 (2026-05-03): the prior list missed real Yardi sub-codes
        # like 7140-1419 (Cap - Facade Waterproof) and 7165-1409 (Cap -
        # Engineering / Architectural). Switched to single-digit "7" prefix
        # which matches every 7xxx GL via the gl_base.startswith() rule.
        "gl_prefix": ["7"],
        "section": "non_operating_expense",
        "gl_range": ("7000-0000", "7999-9999"),
        "notes": "All 7xxx codes. Single-prefix '7' covers the entire capital range "
                 "(matched against gl_base which strips sub-account suffixes)."
    },
    "Cert Fee for Tax Reduction": {
        "sheet": None,
        "gl_prefix": [],
        "section": "non_operating_expense",
        "special": "manual",
        "notes": "Tax certiorari fees. Often zero. No GL in most years."
    },
}

# ─── SUBTOTAL ROWS (calculated, not mapped to GL) ────────────────────────
SUBTOTAL_FORMULAS = {
    "Total Income": "SUM(income rows)",
    "Total Expenses": "SUM(expense rows)",
    "Net Operating Surplus <Deficit>": "Total Income - Total Expenses",
    "Total Non- Operating Income": "SUM(non_operating_income rows)",
    "Total Non-Operating Expenses": "SUM(non_operating_expense rows)",
    "Total Surplus <Deficit>": "Net Operating + Total Non-Op Income - Total Non-Op Expenses",
}


# ─── GAP ANALYSIS ────────────────────────────────────────────────────────
"""
CURRENT STATE vs NEEDED:

GL CODES THE APP HANDLES TODAY (200 codes across 6 sheets):
  ✅ Income: 48 codes (4010-4990) → Income sheet
  ✅ Payroll: 30 codes (5000-5170) → Payroll sheet
  ✅ Energy: 11 codes (5105, 5110, 5115) → Energy sheet
  ✅ Water & Sewer: 3 codes (5200-5210) → Water & Sewer sheet
  ✅ Repairs & Supplies: 51 codes (5406-5874) → R&S sheet (via RM_GL_MAP)
  ✅ Gen & Admin: 57 codes (6105-6995) → Gen & Admin sheet

GL CODES THAT CURRENTLY GO TO "UNMAPPED":
  ⚠️ 7xxx codes (Capital Expenses) — 20 codes, no CAP sheet in app
  ⚠️ 7045 (Insurance Proceeds) — non-operating, rare
  ⚠️ 2510 (Mortgage Note) — financial expense, should route to Gen & Admin
  ⚠️ 2520 (SBA PPP Loan) — non-operating, usually zero

WHAT NEEDS TO CHANGE:
  1. Route 2510 → Gen & Admin (Financial Expenses) — 1 line change
  2. Route 7xxx → new "Capital" sheet OR keep in Unmapped but tag for summary
  3. Route 7045 → tag for non-operating income in summary
  4. Route 2520 → tag for non-operating income in summary
  5. Manual entry fields for: Capital Assessment, ICON Settlement, Cert Fee

BUILDING-SPECIFIC ROWS:
  The non-operating section varies by building:
  - 204 has ICON Settlement, SBA-PPP (building-specific history)
  - 302 has Reserve Funding, Working Capital Contribution
  - 168 has Claim Proceeds - Insurance Repairs
  These rows come from the budget Excel import — the app doesn't need to
  pre-define them. The row framework IS the building's budget file.
"""
