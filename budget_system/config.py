"""
Configuration for the budget automation system.

Contains template paths, output paths, GL code mappings, and column mappings.
"""

import os
from pathlib import Path
from typing import Dict, List

# Base paths
BASE_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = BASE_DIR.parent.parent  # Navigate to Budgets folder
TEMPLATE_PATH = PROJECT_ROOT / "template" / "template.xlsx"
OUTPUT_BASE_DIR = PROJECT_ROOT / "output"
YARDI_DOWNLOADS_DIR = PROJECT_ROOT / "yardi_downloads"

# Yardi credentials and base URL
YARDI_BASE_URL = "https://www.yardiasp13.com/03578cms"
YARDI_ENTITY_ID = "03578"

# YSL column mapping for 2027 budget
# Maps YSL column letters to template column letters and descriptions
YSL_COLUMN_MAPPING = {
    "E": {"template_col": "D", "description": "Prior Year Actual (2025 Actual for 2027 budget)"},
    "F": {"template_col": "E", "description": "YTD Actual"},
    "G": {"template_col": "H", "description": "YTD Budget"},
}

# GL code to template sheet and row mapping
# Format: "sheet_name": [list of GL codes that belong on that sheet]
GL_CODE_TO_SHEET = {
    "Income": [
        "4010-0000", "4010-0001", "4040-0000", "4050-0000", "4060-0000",
        "4070-0000", "4105-0000", "4110-0000", "4115-0000", "4125-0000",
        "4130-0010", "4130-0015", "4130-0030", "4200-0000", "4250-0010",
        "4250-0035", "4520-0000", "4605-0000", "4705-0000", "4715-0000",
        "4720-0010", "4720-0030", "4720-0050", "4725-0015", "4725-0025",
        "4725-0035", "4725-0040", "4800-0000", "4803-0000", "4917-0000",
        "4922-0000", "4926-0000", "4932-0000", "4933-0000", "4935-0000",
        "4956-0000", "4990-0000",
    ],
    "Payroll": [
        "5105-0000", "5140-0000", "5145-0000", "5150-0000", "5155-0000",
        "5160-0000", "5162-0000", "5165-0000", "5166-0000", "5168-0000",
        "5172-0000",
    ],
    "Energy": [
        "5250-0000", "5252-0000", "5253-0000", "5255-0000", "5256-0000",
        "5265-0000",
    ],
    "Water & Sewer": [
        "6305-0000",
    ],
    "Repairs & Supplies": [
        "5406-0000", "5606-0000", "5611-0000", "5612-0000", "5620-0000",
        "5625-0000", "5630-0000", "5632-0000", "5635-0000", "5640-0000",
        "5645-0000", "5650-0000", "5655-0000", "5660-0000", "5665-0000",
        "5670-0000", "5675-0000", "5680-0000", "5685-0000", "5690-0000",
        "5695-0000", "5803-0000", "5805-0000", "5810-0000", "5815-0000",
        "5820-0000", "5825-0000", "5830-0000", "5835-0000", "5840-0000",
        "5845-0000", "5850-0000", "5855-0000", "5860-0000", "5865-0000",
        "5870-0000", "5874-0000",
    ],
    "Gen & Admin": [
        # Professional Fees
        "6505-0000", "6510-0000", "6515-0000", "6520-0000", "6525-0000",
        "6590-0000",
        # Administrative
        "6706-0020", "6708-0000", "6710-0000", "6712-0000", "6714-0000",
        "6720-0000", "6740-0000", "6745-0000", "6750-0000", "6755-0000",
        "6760-0000", "6765-0000", "6770-0000", "6780-0000", "6785-0000",
        "6790-0000", "6795-0000",
        # Insurance
        "6105-0000", "6110-0000", "6115-0000", "6120-0000", "6125-0000",
        "6126-0000", "6135-0000", "6180-0000", "6195-0000",
        # Taxes
        "6310-0000", "6315-0000", "6315-0010", "6315-0020", "6315-0025",
        "6315-0035", "6320-0000", "6325-0000",
        # Financial
        "2510-0000", "6905-0000", "6920-0000", "6925-0000", "6999-0000",
    ],
}

# Template column configuration
# Maps template column letters to their purpose
TEMPLATE_COLUMNS = {
    "A": "GL Code",
    "B": "Description",
    "C": "Notes",
    "D": "Prior Year Actual",
    "E": "YTD Actual",
    "F": "YTD % of Year",
    "G": "Full Year Estimate",
    "H": "YTD Budget",
    "I": "Budget Variance",
    "J": "Full Year Budget",
    "K": "Comments",
}

# Setup sheet field mappings
SETUP_SHEET_FIELDS = {
    "B4": "property_code",
    "B5": "property_name",
    "B10": "ytd_months",
    "B11": "remaining_months",
}

# YSL metadata row indicators (based on column A values)
YSL_ROW_TYPES = {
    "H": "header",  # $t=H in column A metadata
    "R": "detail",  # $t=R in column A metadata
    "T": "total",   # $t=T in column A metadata
}

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = "INFO"

# Yardi request configuration
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 0.5  # seconds between requests
MAX_RETRIES = 3

# Validation thresholds
VALIDATION_ZERO_VALUE_THRESHOLD = 0.01  # Alert if prior year > this but current is zero
