# YSL Report Structure

## Overview

The YSL Annual Budget report (`rs_YSL_CMS_Annual_Budget_60612`) is Yardi's standard annual budget template. It exports as an Excel file with a consistent structure designed for budget planning and analysis.

---

## File-Level Structure

### Metadata Section (Rows 1–15)

The report header contains property and period information:

| Row | Content | Location |
|-----|---------|----------|
| 1 | Report title | A1:D1 (merged) |
| 2 | Date generated / metadata | A2:D2 |
| 3 | **Property code** | **B3** |
| 4-10 | Budget period metadata | A4:D10 |
| 11 | **Property name** | **C11** |
| 12-15 | Column headers: GL Code, Description, Period 1, Period 2, Period 3, Period 4, Period 5 | Row 15 |

**Key extraction points:**
- Property code: `workbook.worksheets[0].getCell('B3').value`
- Property name: `workbook.worksheets[0].getCell('C11').value`
- Column headers start at row 15

### Data Section (Rows 16+)

Data rows begin after row 15. Each row represents either a GL account or a subtotal.

---

## Column A Metadata Markers

Column A contains special markers that indicate the row type. These are used to parse the report correctly:

| Marker | Meaning | Example | Action |
|--------|---------|---------|--------|
| `$t=H` | **Header row** (section label) | INCOME | Marks the start of a section. Use the cell value as a section name. Include in output with bold/heading formatting. |
| `$t=R` | **Detail row** (GL account data) | 4010 | This row has actual GL data. Extract GL code, description, and amounts. |
| `$t=T` | **Total row** (subtotal or grand total) | TOTAL INCOME | Marks an aggregated total. Can be used for validation or summary output. |
| (empty) | **Blank or continuation** | (blank) | Skip these rows. |

**Parsing pattern:**
```javascript
const marker = row.getCell('A').value;
if (marker === '$t=H') {
  // Section header
} else if (marker === '$t=R') {
  // Detail row
} else if (marker === '$t=T') {
  // Total row
} else {
  // Skip
}
```

---

## Data Columns

Each row contains GL data in columns B through H:

| Column | Header | Meaning | Data Type | Example |
|--------|--------|---------|-----------|---------|
| **B** | GL Code | General Ledger account code | String | `4010-0000`, `5010-1000` |
| **C** | GL Description | Account name / description | String | `Rent Income`, `Payroll Expense` |
| **D** | Period 1 | Prior prior year actual (rarely used) | Number | 12500.00 |
| **E** | Period 2 | Prior year full-year actual | Number | 13200.50 |
| **F** | Period 3 | Current year YTD actual (through as-of date) | Number | 3400.00 |
| **G** | Period 4 | Current year YTD budget (through as-of date) | Number | 3500.00 |
| **H** | Period 5 | Current year full-year approved budget | Number | 14000.00 |

**Note on columns D and F:**
- Column D (Period 1) is rarely populated — most budgets use Period 2 onward
- Column F contains the YTD actual for the current year up to the as-of period specified when the report was run
- Column G contains the YTD budget — useful for variance analysis

---

## GL Code Format and Categories

### Format

GL codes follow the pattern: `XXXX-XXXX` (four digits, dash, four digits)

Examples:
- `4010-0000` — Apartment rent income
- `5010-1000` — Payroll expense (staff)
- `6105-0000` — General liability insurance

### Major Categories

GL codes are organized by prefix. Use these to categorize data:

| Prefix | Category | Include in Operating Budget? | Notes |
|--------|----------|------------------------------|-------|
| **4xxx** | **Income** | **Yes** | Rent, maintenance charges, late fees, parking, laundry, etc. |
| **5xxx** | **Operating Expenses** | **Yes** | Payroll, utilities, repairs, supplies, services |
| **6xxx** | **General & Admin** | **Yes** | Insurance, professional services, licenses, office expenses |
| **7xxx** | **Capital Expenditures** | **No** | Building improvements, equipment purchases — exclude from operating budgets |
| **1xxx-3xxx** | **Balance Sheet** | **No** | Assets, liabilities, equity — exclude from operating budgets |
| **0000-0000** | **Rollup/Summary** | **No** | This code aggregates other accounts; including it would double-count |

**Common GL codes by range:**
- `4000-4999`: Income (4010 rent, 4020 maint fees, 4030 late fees, etc.)
- `5000-5999`: Operating (5010 payroll, 5110 utilities, 5200 repairs, etc.)
- `6000-6999`: G&A (6105 insurance, 6200 legal, 6300 accounting, etc.)
- `7000-7999`: Capital (7100 building improvements, etc.)

---

## The Duplicate GL Code Problem

### What Happens

Near the bottom of the report, there's an "Adjustments" section that re-lists some GL codes with **negated values**. This is intentional in Yardi's design — the adjustments represent changes to the budget. However, **when parsing the file, you should skip duplicates**.

### Example

```
Row 50: $t=R | 4010-0000 | Apartment Rent | 120000 | 125000 | 32000 | 33000 | 130000
Row 51: $t=R | 4020-0000 | Maint Fees     | 15000  | 16000  | 4000  | 4100  | 17000
...
Row 120: $t=H | ADJUSTMENTS
Row 121: $t=R | 4010-0000 | Apartment Rent | 0 | 0 | 0 | 0 | -5000  # Adjustment, negated
Row 122: $t=R | 6105-0000 | Insurance | 0 | 0 | 0 | 0 | -2000  # Adjustment, negated
```

If you parse both rows 50 and 121, you'll include `4010-0000` twice — once with 130000, once with -5000. This will throw off your totals.

### Solution: Keep First Occurrence Only

```javascript
const glCodesSeenBefore = new Set();

for (let rowNum = 16; rowNum <= maxRow; rowNum++) {
  const row = worksheet.getRow(rowNum);
  const marker = row.getCell('A').value;

  if (marker === '$t=R') {
    const glCode = row.getCell('B').value;

    // Skip if we've already seen this GL code
    if (glCodesSeenBefore.has(glCode)) {
      console.log(`Skipping duplicate GL code: ${glCode}`);
      continue;
    }

    // Process this row
    glCodesSeenBefore.add(glCode);
    // ... extract and store data ...
  }
}
```

### Why It Matters

If you include both the original and the adjustment:
- Budget totals will be wrong
- Variance calculations will be off
- Duplicate GL codes in your output data structure will cause confusion

**Always** keep the first occurrence and skip duplicates that appear later in the file.

---

## Parsing Workflow

### Step 1: Extract Metadata

```javascript
const workbook = new ExcelJS.Workbook();
await workbook.xlsx.load(fileBuffer);
const ws = workbook.worksheets[0];

const propertyCode = ws.getCell('B3').value;
const propertyName = ws.getCell('C11').value;
```

### Step 2: Identify Data Range

```javascript
// Find the last row with data
let maxRow = 15;
for (let i = 16; i <= ws.rowCount; i++) {
  const cell = ws.getCell(`A${i}`).value;
  if (cell) {
    maxRow = i;
  }
}

console.log(`Data rows: 16 to ${maxRow}`);
```

### Step 3: Iterate and Filter

```javascript
const glData = [];
const glCodesSeenBefore = new Set();

for (let rowNum = 16; rowNum <= maxRow; rowNum++) {
  const row = ws.getRow(rowNum);
  const marker = row.getCell('A').value;

  if (marker === '$t=H') {
    // Section header
    glData.push({
      type: 'header',
      value: row.getCell('B').value,
    });
  } else if (marker === '$t=R') {
    // Detail row
    const glCode = row.getCell('B').value;

    // Skip duplicates
    if (glCodesSeenBefore.has(glCode)) {
      continue;
    }

    glCodesSeenBefore.add(glCode);

    glData.push({
      type: 'detail',
      glCode: glCode,
      description: row.getCell('C').value,
      period1: row.getCell('D').value || 0,
      period2: row.getCell('E').value || 0,
      period3: row.getCell('F').value || 0,
      period4: row.getCell('G').value || 0,
      period5: row.getCell('H').value || 0,
    });
  } else if (marker === '$t=T') {
    // Total row
    glData.push({
      type: 'total',
      value: row.getCell('B').value,
      period5: row.getCell('H').value || 0,
    });
  }
}
```

### Step 4: Filter by GL Category

After extracting all GL data, filter to include only the categories you need:

```javascript
const isOperatingBudget = (glCode) => {
  const prefix = parseInt(glCode.substring(0, 1));
  return prefix === 4 || prefix === 5 || prefix === 6;  // Income, Operating, G&A
};

const operatingGLData = glData.filter(item => {
  if (item.type !== 'detail') return true;  // Keep headers and totals
  return isOperatingBudget(item.glCode);
});
```

---

## Period Definitions

When the YSL report is run, it includes data for five periods:

| Period | Meaning | Usage |
|--------|---------|-------|
| **1** | Prior prior year actual | Rarely used; often empty |
| **2** | Prior year full-year actual | Baseline for comparison; used for trend analysis |
| **3** | Current year YTD actual (as-of date) | Real spend/revenue through the as-of date |
| **4** | Current year YTD budget (as-of date) | Budget allocated through the as-of date; useful for variance |
| **5** | Current year full-year approved budget | The target annual budget; used for trend analysis and planning |

**Typical use case:**
- Use Period 2 (prior year) and Period 5 (current approved budget) for annual comparison
- Use Period 3 (YTD actual) and Period 4 (YTD budget) for mid-year variance analysis

---

## Multiple Properties / Sheets

The YSL report can be generated for a **single property** or a **range of properties**.

### Single Property Report
- One worksheet with one property's data
- Extract metadata (code, name) from rows 3 and 11
- Data rows start at 16

### Multi-Property Report
- Multiple worksheets (one per property)
- Each worksheet has the same structure
- Iterate through `workbook.worksheets` and process each one

**Parsing multiple sheets:**
```javascript
const allPropertiesData = [];

for (const worksheet of workbook.worksheets) {
  const propertyCode = worksheet.getCell('B3').value;
  const propertyName = worksheet.getCell('C11').value;

  const glData = [];
  // ... extract GL data as above ...

  allPropertiesData.push({
    code: propertyCode,
    name: propertyName,
    glData: glData,
  });
}
```

---

## Common Parsing Pitfalls

### 1. Including Rows Without Markers

**Pitfall:** You include blank rows or rows with empty marker columns, cluttering your output.

**Fix:** Check `if (marker === '$t=H' || marker === '$t=R' || marker === '$t=T')` before processing.

### 2. Not Handling Null/Undefined Column Values

**Pitfall:** JavaScript throws `TypeError: Cannot read property 'value' of null` when accessing a cell.

**Fix:** Use the `||` operator: `row.getCell('D').value || 0`

### 3. Forgetting to Decode GL Code Prefix

**Pitfall:** You store GL code as a string and try to filter by prefix — string comparison doesn't work as expected.

**Fix:** Extract the first character: `const prefix = parseInt(glCode[0])`, then compare: `if (prefix >= 4 && prefix <= 6)`

### 4. Including Duplicate GL Codes

**Pitfall:** Your budget totals are wrong because you counted some GL codes twice.

**Fix:** Use a Set to track GL codes you've already processed. Skip any duplicates.

### 5. Confusing Period Columns

**Pitfall:** You use Period 3 (YTD actual) when you meant to use Period 5 (full-year budget).

**Fix:** Always label your data clearly: `{ ytdActual: period3, fullYearBudget: period5 }` instead of generic `{ period3, period5 }`.

### 6. Not Handling Multi-Sheet Files

**Pitfall:** You parse only the first sheet and miss data for other properties.

**Fix:** Loop through all worksheets: `for (const ws of workbook.worksheets) { ... }`

---

## Example: Complete Parse Function

```javascript
async function parseYSLReport(fileBuffer) {
  const ExcelJS = window.ExcelJS; // Assumes ExcelJS is loaded
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(fileBuffer);

  const results = [];

  for (const worksheet of workbook.worksheets) {
    const propertyCode = worksheet.getCell('B3').value;
    const propertyName = worksheet.getCell('C11').value;

    const glData = [];
    const glCodesSeenBefore = new Set();

    // Find last row with data
    let maxRow = 15;
    for (let i = 16; i <= worksheet.rowCount; i++) {
      if (worksheet.getCell(`A${i}`).value) {
        maxRow = i;
      }
    }

    // Parse GL data
    for (let rowNum = 16; rowNum <= maxRow; rowNum++) {
      const row = worksheet.getRow(rowNum);
      const marker = row.getCell('A').value;
      const glCode = row.getCell('B').value;

      if (marker === '$t=R') {
        // Skip duplicate GL codes
        if (glCodesSeenBefore.has(glCode)) {
          continue;
        }
        glCodesSeenBefore.add(glCode);

        // Filter by GL category (Income, Operating, G&A only)
        const prefix = parseInt(glCode[0]);
        if (prefix >= 4 && prefix <= 6) {
          glData.push({
            glCode: glCode,
            description: row.getCell('C').value,
            ytdActual: row.getCell('F').value || 0,
            fullYearBudget: row.getCell('H').value || 0,
          });
        }
      }
    }

    results.push({
      propertyCode: propertyCode,
      propertyName: propertyName,
      glData: glData,
    });
  }

  return results;
}
```

---

## Validation Checklist

Before using parsed data from a YSL report:

- [ ] Property code extracted from B3
- [ ] Property name extracted from C11
- [ ] Data rows start at row 16 (no earlier)
- [ ] All rows checked for marker ($t=H, $t=R, $t=T)
- [ ] Duplicate GL codes skipped
- [ ] GL codes filtered by prefix (4xxx, 5xxx, 6xxx only for operating budget)
- [ ] 0000-0000 rollup codes excluded
- [ ] 7xxx capital codes excluded
- [ ] 1xxx-3xxx balance sheet codes excluded
- [ ] Period columns correctly assigned (Period 3 = YTD actual, Period 5 = full-year budget)
- [ ] Null/undefined values handled (use `|| 0`)
- [ ] Multi-sheet files looped through (if applicable)
