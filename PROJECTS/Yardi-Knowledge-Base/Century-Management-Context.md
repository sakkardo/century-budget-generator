# Century Management Context

## Company Overview

**Century Management** is a property management company based in New York City specializing in managing residential multi-family buildings.

### Portfolio
- Approximately **140 buildings** under management
- Mix of **co-ops and condos**
- Primarily located in **New York City**
- Buildings range from small residential complexes to large multi-unit properties

### Key Characteristics
- Long-standing, established management company
- Uses Yardi Voyager as the primary property management system
- Maintains centralized budgeting and accounting via Yardi
- Manages budgets for multiple buildings independently

---

## Yardi Instance

### Connection Details
- **Instance URL:** `https://www.yardiasp13.com/03578cms/`
- **Instance ID:** `03578cms`
- **Server:** `yardiasp13.com` (Yardi cloud)
- **Application:** Yardi Voyager (full-featured property management suite)

### Access
- Only accessible while logged in to a valid Century Management Yardi account
- Session-based authentication (cookies)
- Console scripts inherit session automatically via `credentials: 'include'`

---

## Entity Codes (Building Codes)

Buildings and properties in the Century Management Yardi instance are identified by **3-digit entity codes**.

### Examples
- `148` → A building (exact building name would be in Yardi)
- `204` → Another property
- `206` → Another property
- `805` → Another property

### Important Notes
- Codes are **numeric** (not zero-padded strings)
- Always pass as integers in fetch requests: `entityCode: 148` (not `"0148"` or `"148"`)
- There are approximately 140 unique entity codes (one per building)
- Entity codes may not be sequential (gaps in numbering are normal)

### Finding a Building's Entity Code
1. Log into Yardi
2. Navigate to Properties or Buildings
3. Look for the property by name
4. The entity code will be displayed in the property details or in the URL

---

## Budget Cycle and Years

### Annual Budget
- Century Management operates on an **annual budget cycle**
- Budgets are typically prepared **in advance of the fiscal year** (planning phase starts months before the year begins)
- Each building has its own annual operating budget

### Current Year Reference (as of March 2026)

| Year | Status | Usage |
|------|--------|-------|
| **2024** | Prior prior year | Historical comparison (rarely used) |
| **2025** | Prior year | Baseline for variance analysis; full-year actuals |
| **2026** | Current year | Mid-year actual results; used for forecasting and variance analysis |
| **2027** | Budget year | The target annual budget currently being used or planned |

**Note:** When reports show "Period 2" (prior year) and "Period 5" (full-year budget), they're typically comparing 2025 actual to 2027 budget.

---

## Budget Template Structure

The Century Management budget template in Yardi contains approximately **200 GL codes** organized across multiple sections:

### Budget Sections

| Section | GL Code Range | Purpose | Typical GL Codes |
|---------|---------------|---------|-----------------|
| **Income** | 4000-4999 | Revenue sources | 4010 (rent), 4020 (maint fees), 4030 (late fees), 4040 (parking), 4050 (laundry) |
| **Payroll** | 5000-5099 | Salaries and wages | 5010 (staff payroll), 5020 (payroll taxes), 5030 (benefits) |
| **Energy** | 5100-5199 | Utilities | 5110 (electricity), 5120 (gas), 5130 (water base rate) |
| **Water & Sewer** | 5200-5299 | Water usage and sewer charges | 5210 (water usage), 5220 (sewer charges) |
| **Repairs & Supplies** | 5300-5999 | Building maintenance | 5310 (repairs), 5320 (supplies), 5330 (equipment) |
| **General & Admin** | 6000-6999 | Administrative expenses | 6105-6195 (insurance codes), 6200 (legal), 6300 (accounting) |
| **Insurance Schedule** | 6105-6195 (subset) | Detailed insurance breakdown | 6105 (general liability), 6110 (property), 6115 (directors & officers), etc. |

### GL Code Count
- Total of approximately **200 GL codes** across all sections
- Individual buildings may not use all codes
- Some buildings have customized GL codes for specific expenses

---

## Key Report: YSL Annual Budget

### Report Details
- **Report Name:** YSL Annual Budget
- **Report ID in Yardi:** `rs_YSL_CMS_Annual_Budget_60612`
- **Format:** Excel file (.xlsx)
- **Purpose:** Annual budget planning and variance analysis
- **Scope:** Can be run for a single building or a range of buildings

### What It Contains
- GL codes from all sections (Income through General & Admin)
- Five periods of data:
  - Period 1: Prior prior year actual (usually empty)
  - Period 2: Prior year full-year actual
  - Period 3: Current year YTD actual (through as-of date)
  - Period 4: Current year YTD budget (through as-of date)
  - Period 5: Full-year approved budget
- Property metadata (code, name)
- Section headers and totals

### How to Request It via Console Script
```javascript
const selectParam = 'reports/rs_YSL_CMS_Annual_Budget_60612';
const formData = new URLSearchParams();
formData.append('BPOSTED', '-1');
formData.append('ReportMonitor', '../pages/SysConductorReportMonitor.aspx');
formData.append('Records', '');
formData.append('RptOutput', 'Filexlsx');
formData.append('__VIEWSTATE', document.querySelector('[name="__VIEWSTATE"]').value);
// ... add other required hidden fields ...

const url = `https://www.yardiasp13.com/03578cms/pages/SysSqlScript.aspx?action=Filter&select=${selectParam}`;
// ... complete fetch ...
```

### Parsing Tips
- See `YSL-Report-Structure.md` for detailed parsing instructions
- Always filter GL codes by category (4xxx, 5xxx, 6xxx for operating budgets)
- Skip GL code 0000-0000 (rollup/summary)
- Skip GL codes starting with 7, 1, 2, 3 (capital and balance sheet)
- Watch for duplicate GL codes in the "Adjustments" section near the end — keep the first occurrence only

---

## Insurance Schedule GL Codes

The Insurance Schedule is a subset of the General & Admin section dedicated to insurance expenses. It pulls from GL codes in the **6105-6195 range**.

### Common Insurance GL Codes

| GL Code | Type | Description |
|---------|------|-------------|
| `6105-0000` | General Liability | Standard coverage for premises liability |
| `6110-0000` | Property Insurance | Building structure and contents coverage |
| `6115-0000` | Directors & Officers | D&O liability for board members |
| `6120-0000` | Workers' Compensation | Injury/illness coverage for staff |
| `6125-0000` | Cyber Liability | Data breach and cyber coverage |
| `6130-0000` | Umbrella/Excess | Coverage above primary policies |
| `6140-0000` | Fiduciary Liability | Protection for benefit plan administration |
| `6150-0000` | Professional Liability | Coverage for consultants and professionals |
| `6160-0000` | Boiler & Machinery | Equipment breakdown coverage |
| `6170-0000` | Management Liability | Coverage for employment practices, etc. |
| `6180-0000` | Crime & Fidelity | Employee dishonesty and theft coverage |
| `6190-0000` | Other Insurance | Additional coverages not listed above |
| `6195-0000` | Insurance Brokerage Fees | Commissions and fees to insurance broker |

### How to Extract Insurance Data

When building an insurance schedule:
1. Run the YSL Annual Budget report
2. Filter GL codes to include only 6105-0000 through 6195-0000
3. Each GL code represents one line item (insurance policy or category)
4. Sum the "Period 5" (full-year budget) column to get total insurance expense
5. Individual GL codes can be tracked separately if needed for insurance procurement

---

## Building / Entity Code Mapping

### Known Buildings
The following entity codes are confirmed to be in use:

| Entity Code | Status |
|-------------|--------|
| 148 | Active |
| 204 | Active |
| 206 | Active |
| 805 | Active |
| (others) | Approximately 140 total |

To find a complete list of all entity codes and their corresponding building names:
1. Log into Yardi
2. Navigate to Setup > Properties
3. View or export the property list
4. Look for the entity code column

---

## Common Workflows

### Annual Budget Preparation
1. Retrieve prior year actual via YSL report (Period 2)
2. Set targets for the new budget year (Period 5)
3. Review and adjust by GL code and section
4. Approve budget in Yardi
5. Distribute approved budget to building managers

### Mid-Year Variance Analysis
1. Run YSL report for current year YTD
2. Compare Period 3 (YTD actual) vs Period 4 (YTD budget)
3. Identify overages and savings by GL code
4. Forecast full-year results
5. Recommend adjustments to reduce overages

### Quarterly Reporting
1. Generate YSL reports by building
2. Consolidate into company-wide summary
3. Present to stakeholders with variance analysis
4. Track trends over multiple quarters

---

## Access and Permissions

### Required Access
To run YSL reports and access building data:
- Valid Century Management Yardi account
- Permission to view the specific entity (building)
- Permission to run reports

### Session Management
- Sessions expire after a period of inactivity (typically 30 minutes)
- If a console script fails with authentication error, log back into Yardi and retry
- Console scripts only work while logged in — they don't handle re-authentication

---

## Troubleshooting

### "Report not found" or 404 when running report

**Cause:** The report ID is incorrect or not available in this Yardi instance.

**Solution:**
1. Log into Yardi manually
2. Navigate to the Reports section
3. Search for "YSL Annual Budget" or similar
4. Copy the correct report ID from the URL
5. Update the `selectParam` in your console script

### Entity code returns no data

**Cause:** The entity code doesn't exist, or you don't have permission to view it.

**Solution:**
1. Verify the entity code is correct (should be numeric, 1-3 digits)
2. Log into Yardi and manually check the Properties list
3. Ensure you're logged in with an account that has permission for that building

### Report times out or hangs

**Cause:** The report is computationally expensive or the server is busy.

**Solution:**
1. Wait a few minutes and retry
2. Try running for a smaller set of buildings (single building instead of all)
3. Check Yardi server status (contact Yardi support if persistent)

---

## Best Practices for Console Scripts

### File Management
- Always save the downloaded file immediately (browsers auto-delete temporary downloads)
- Name files descriptively: `YSL_2027_Budget_Building148.xlsx`
- Store files in a consistent location (e.g., Downloads or a project folder)

### Error Handling
- Log all responses to the browser console for debugging
- Include try/catch blocks in fetch scripts
- If polling times out (3+ minutes), something is wrong — don't wait longer

### Session Management
- Run console scripts while actively using Yardi (within an active session)
- If you see a login redirect, the session expired — log in again and retry
- Don't close the Yardi tab while the script is running

### Data Validation
- After downloading a report, open it in Excel and spot-check a few GL codes
- Verify the property code and name match expectations
- Confirm the periods are what you expected
