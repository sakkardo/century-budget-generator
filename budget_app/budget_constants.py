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


# -- Phase 3 step 3 (2026-06-08): GL classification maps moved from workflow.py --
RM_GL_MAP = {
    "5406-0000": ("Supplies & Hardware", 8, "supplies"),
    "5445-0000": ("Uniforms Purchased", 13, "supplies"),
    "5606-0000": ("Insurance Repairs", 15, "repairs"),
    "5609-0000": ("Appliance Repairs", 16, "repairs"),
    "5621-0000": ("Equipment Repairs", 17, "repairs"),
    "5622-0000": ("HVAC Repairs", 18, "repairs"),
    "5624-0000": ("Heating Repairs", 19, "repairs"),
    "5627-0000": ("Boiler/Burner Repairs", 20, "repairs"),
    "5630-0000": ("Plumbing Repairs", 21, "repairs"),
    "5630-0001": ("Pump/Motor Repair", 22, "repairs"),
    "5633-0000": ("Electrical Repairs", 23, "repairs"),
    "5636-0000": ("Air Conditioning Repairs", 24, "repairs"),
    "5639-0000": ("Elevator Repairs", 25, "repairs"),
    "5642-0000": ("Compactor / Incinerator Repairs", 26, "repairs"),
    "5645-0000": ("Window Repairs", 27, "repairs"),
    "5648-0000": ("Floor / Carpet Repair", 28, "repairs"),
    "5657-0000": ("Lobby Repairs", 29, "repairs"),
    "5660-0000": ("Paint & Plaster Repairs", 30, "repairs"),
    "5666-0000": ("Door Repairs", 31, "repairs"),
    "5670-0000": ("Intercom Repairs", 32, "repairs"),
    "5672-0000": ("Time Recorder Repairs", 33, "repairs"),
    "5674-0000": ("Facade/Waterproofing Repairs", 34, "repairs"),
    "5678-0000": ("Roof Tank / Water Tank Repairs", 35, "repairs"),
    "5680-0000": ("Roof Exhaust Repairs", 36, "repairs"),
    "5682-0000": ("Sprinkler System Repairs", 37, "repairs"),
    "5692-0000": ("Fitness Equipment Repair", 38, "repairs"),
    "5695-0000": ("Other Building Repairs", 39, "repairs"),
    "5603-0000": ("Apartment Repairs", 40, "repairs"),
    "5668-0000": ("Canopy Repairs", 41, "repairs"),
    "5684-0000": ("Sidewalk Concrete Repairs", 42, "repairs"),
    "5803-0000": ("Cleaning & Maintenance", 46, "maintenance"),
    "5806-0000": ("Air Conditioning Maintenance", 47, "maintenance"),
    "5810-0000": ("HVAC Maintenance", 48, "maintenance"),
    "5812-0000": ("Elevator Maintenance", 49, "maintenance"),
    "5815-0000": ("Floor/Carpet Cleaning", 50, "maintenance"),
    "5818-0000": ("Window Cleaning", 51, "maintenance"),
    "5821-0000": ("Fire Extinguisher Maintenance", 52, "maintenance"),
    "5825-0000": ("Uniform Cleaning & Maintenance", 53, "maintenance"),
    "5828-0000": ("Rubbish Removal", 54, "maintenance"),
    "5831-0000": ("Exterminating", 55, "maintenance"),
    "5834-0000": ("Cooling Tower Maint", 56, "maintenance"),
    "5837-0000": ("Alarm / Security System Maintenance", 57, "maintenance"),
    "5840-0000": ("Metal/Marble/Wood Maintenance", 58, "maintenance"),
    "5846-0000": ("Garage Maintenance", 59, "maintenance"),
    "5852-0000": ("Water Treatment Maintenance", 60, "maintenance"),
    "5856-0000": ("Equipment Maint - Software", 61, "maintenance"),
    "5865-0000": ("Landscaping", 62, "maintenance"),
    "5870-0000": ("Rooftop Garden & Landscaping", 63, "maintenance"),
    "5809-0010": ("Boiler Maintenance", 64, "maintenance"),
    "5874-0000": ("Other Maintenance", 65, "maintenance"),
    "5809-0016": ("Sprinkler Maintenance", 66, "maintenance"),
}

CAPITAL_GL_PREFIX = {
    "7018": "Net Proceeds of Sale",
    "7020": "Real Estate Tax Refund",
    "7025": "Flip Tax - Capital",
    "7030": "J51 Credit - Capital",
    "7035": "Investment Income",
    "7040": "Mitchell Lama Amortization",
    "7045": "Claims Proceeds - Insurance Repairs",
    "7095": "Other Sources of Funds",
    "7105": "Cap - Elevator",
    "7110": "Cap - Boiler/Burner",
    "7115": "Cap - HVAC",
    "7120": "Cap - Pump & Motor",
    "7125": "Cap - Appliances",
    "7130": "Cap - Roofing",
    "7135": "Cap - Chimney",
    "7140": "Cap - Facade Waterproof",
    "7145": "Cap - Plumbing",
    "7150": "Cap - Electrical",
    "7155": "Cap - General Contractor",
    "7160": "Cap - Carpentry",
    "7165": "Cap - Engineering / Architectural",
    "7170": "Cap - Sprinkler",
    "7175": "Cap - Water Meter",
    "7180": "Cap - Water Tank",
    "7185": "Cap - Windows",
    "7190": "Cap - Hallways",
    "7195": "Cap - Lobby",
    "7200": "Cap - Compactor",
    "7205": "Cap - Storage",
    "7210": "Cap - Sidewalk Bridge",
    "7215": "Cap - Paint & Plaster",
    "7220": "Cap - Asbestos Removal",
    "7225": "Cap - Sidewalk / Concrete",
    "7230": "Cap - Driveway",
    "7235": "Cap - Doors",
    "7240": "Cap - Floor / Carpet",
    "7245": "Cap - Garage",
    "7250": "Cap - Canopy / Awning",
    "7255": "Cap - TV / VCR",
    "7260": "Cap - Intercom",
    "7265": "Cap - Security System",
    "7270": "Cap - Garden & Landscape",
    "7273": "Cap - Insurance Repairs",
    "7275": "Cap - Mailbox",
    "7280": "Cap - Signage",
    "7285": "Cap - Parking",
    "7290": "Cap - Pool",
    "7295": "Cap - Pool Furniture",
    "7300": "Cap - STP",
    "7305": "Cap - Whirlpool / Steam Room",
    "7310": "Cap - Tennis Court",
    "7315": "Cap - Fitness Equipment",
    "7320": "Cap - Laundry Room",
    "7325": "Cap - Children's Play Area",
    "7330": "Cap - Gym",
    "7335": "Cap - Aerobics Floor",
    "7340": "Cap - Racquetball Court",
    "7345": "Cap - Great Room",
    "7350": "Cap - Principal Amortization",
    "7355": "Cap - Interest",
    "7360": "Cap - Professional",
    "7370": "Cap - Commissions",
    "7375": "Cap - Legal",
    "7380": "Cap - Loan Financing Fees",
    "7385": "Cap - Building Equipment",
    "7390": "Cap - Office Equipment",
    "7395": "Cap - Computer Equipment",
    "7400": "Cap - Computer Software",
    "7405": "Cap - Furniture & Fixtures",
    "7415": "Cap - Inspection Fees",
    "7490": "Cap - Other",
    "7900": "Cap - Contra",
}

ONE_TIME_FEE_GLS = {
    "6722-0000",  # Annual filing fee
    "6762-0000",  # Annual inspection/permit
    "6763-0000",  # Annual inspection/permit
    "6764-0000",  # Annual inspection/permit
}

SUMMARY_PREFIX_OVERRIDES = {
    "Electric": ["5255"],
    "Gas Cooking / Heating": ["5250", "5251", "5252"],
    "Gas": ["5250", "5251", "5252"],
    "Gas - Heating": ["5250", "5251", "5252"],
    "Gas Heating": ["5250", "5251", "5252"],
    "Steam Heating": ["5265"],
    "Steam": ["5265"],
    "Fuel": ["5260"],
    "Oil / Fuel": ["5260"],
    "Fuel Oil": ["5260"],
    "Water & Sewer": ["6305"],
    "Supplies": [
        "5405", "5406", "5408", "5410", "5415", "5420", "5425", "5430",
        "5435", "5440", "5441", "5445", "5450", "5451", "5452", "5453",
        "5455", "5460", "5465", "5466", "5495",
    ],
}
