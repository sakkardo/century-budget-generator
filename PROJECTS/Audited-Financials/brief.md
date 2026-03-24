# Audited Financials Module — Project Brief

## Problem
Every new budget requires a "Yearly Comparison" (yrlycomp) tab with 4-5 years of historical actuals. Those actuals come from audited financial statements (PDFs from external audit firms). Today, FAs manually read the PDF's Schedule of Expenses, mentally map ~35 auditor line items into Century's ~15 budget categories, and hand-key the numbers. This is slow, error-prone, and repeated for 162 buildings every budget cycle.

## Goal
Build an "Audited Financials" module in the budget app that:
1. Accepts PDF uploads of audited financial statements
2. Extracts the Schedule of Expenses using Claude API (vision/PDF)
3. Maps extracted line items to Century's yrlycomp categories using saved auditor profiles
4. Presents a reconciliation screen for FA confirmation (exception-based review)
5. Auto-populates confirmed figures into the budget template's yrlycomp "Actual" columns

## Source Data
- **Input**: Audited financial statement PDFs (from ~5 audit firms covering 162 buildings)
- **Key page**: "Schedules of Expenses" — typically near the end under "Supplementary Information"
- **Structure**: Line items with amounts for 2 years (current + prior), grouped into auditor-defined categories
- **Example (444 E 86th, 2024 audit)**:
  - Administrative: Accounting fees, Corporate/office, Legal fees, Mgmt fees, Telephone → Total $258,537
  - Operating: Electric, Heating, Payroll, Permits, Uniforms, Union pension, Water/sewer → Total $2,308,724
  - Maintenance: AC repairs, Boiler, Elevator, Exterminating, General repairs, HVAC, Landscaping, Painting, Plumbing, Supplies, Window → Total $768,962
  - Taxes & Insurance: General insurance, State/local taxes, Payroll taxes, RE taxes → Total $4,670,430
  - Mortgage interest: $605,232
  - **Total expenses**: $8,611,885

## Target Data (yrlycomp tab)
Century's budget categories on the yrlycomp tab (columns D-G for historical actuals):

| Century Category | Auditor Source Lines (444 E 86th example) | 2024 Value |
|---|---|---|
| Payroll | Payroll + Union pension + Uniforms + Permits | 1,407,172 |
| Electric | Electric | 206,874 |
| Gas Cooking / Heating | Heating | 468,018 |
| Water & Sewer | Water and sewer charges | 265,390 |
| Supplies | Supplies (from Maintenance section) | 108,458* |
| Repairs & Maintenance | All Maintenance items except Supplies | 691,364* |
| Insurance | General insurance | 393,544 |
| Real Estate Taxes | Real estate taxes | 4,167,342* |
| Real Estate Tax Benefit Credits | (from income side / Note 12) | -750,999 |
| Corporate Taxes | State and local income taxes | 32,746 |
| Professional Fees | Accounting/audit fees + Legal fees + Mgmt fees | 172,223 |
| Administrative & Other | Corporate/office expense + Telephone | 93,522* |
| Financial Expenses | Mortgage interest + Payroll taxes | 799,669* |

*Note: Some values differ between raw audit totals and yrlycomp because the FA exercises judgment in allocation (e.g., splitting Supplies from Maintenance, allocating payroll taxes to Financial vs Payroll). This is the subjective element.

## Architecture

### Extraction (Claude API)
- Send the PDF (or specific pages) to Claude API with a structured prompt
- Return JSON: `{ "auditor": "...", "building": "...", "years": [2024, 2023], "categories": [ { "name": "Administrative", "items": [ { "description": "Accounting and audit fees", "amounts": [22715, 20722] }, ... ] } ] }`
- Store raw extraction in DB for audit trail

### Mapping Profiles (per auditor)
- Each auditor gets a saved mapping profile
- Profile maps: auditor line item description → Century category + optional split rules
- Example: `"Payroll" → "Payroll"`, `"Union pension" → "Payroll"`, `"Supplies" → "Supplies"`
- Profiles are created once per auditor via guided setup, then reused
- ~5 profiles needed to cover all 162 buildings

### Reconciliation Review
- After extraction + mapping, show FA a confirmation screen:
  - Left column: Raw extracted totals by auditor category
  - Right column: Mapped Century category totals
  - Bottom: Total check (extracted total must equal mapped total)
  - Red flags for: totals mismatch, unmapped items, new line items not in profile
- FA confirms or adjusts individual mappings
- Any adjustments update the auditor profile for future use

### Template Population
- On confirmation, write values to yrlycomp columns (D for oldest year, E/F/G for newer years)
- Match by Century category name to row in yrlycomp
- Respect existing data — only overwrite the specific year columns being imported

## Database Schema (new tables)

```
auditor_profiles
  id, name, firm_name, created_at, updated_at

mapping_rules
  id, profile_id (FK), auditor_line_item (text), century_category (text),
  split_pct (float, default 1.0), notes (text)

audit_uploads
  id, entity_code, building_name, auditor_profile_id (FK),
  fiscal_year_end (date), pdf_filename, raw_extraction (JSON),
  mapped_data (JSON), status (extracted/mapped/confirmed),
  confirmed_by (text), confirmed_at (datetime),
  created_at, updated_at
```

## UI Flow
1. FA navigates to /audited-financials
2. Selects building, selects auditor firm (or creates new profile)
3. Uploads PDF
4. System extracts Schedule of Expenses → shows raw data for verification
5. System applies mapping profile → shows reconciliation screen
6. FA confirms (or adjusts) → data saved as "confirmed"
7. On next budget generation, confirmed data auto-populates yrlycomp columns

## Constraints
- ~5 audit firms, but each may change format slightly year to year
- FA judgment required for some allocations (not fully automatable)
- Must handle: missing line items, new line items, format variations
- Claude API cost: ~$0.01-0.03 per PDF extraction (negligible at 162 buildings)
- Need Anthropic API key stored as env var on Railway

## Open Questions
- [ ] Where does the Anthropic API key come from? (Jacob's existing key or new one?)
- [ ] Should we store the PDF files themselves, or just the extracted data?
- [ ] Do we need to handle income items too, or just expenses?
- [ ] How many years back do we need to populate? (yrlycomp shows 4-5 years)
