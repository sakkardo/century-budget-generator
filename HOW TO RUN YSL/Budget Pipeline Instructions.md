# 2027 Budget Pipeline — Working Instructions

**Last updated:** March 21, 2026
**Owner:** Jacob Sirotkin (JSirotkin@Centuryny.com)
**Status:** Pilot (4 buildings) — expanding to full portfolio

---

## Overview

This system automates 2027 budget creation for Century Management buildings. It downloads YSL Annual Budget reports from Yardi Voyager, parses the GL-level financial data, and populates standardized Excel budget templates — one per building.

### What You Need

- A computer with **Google Chrome** and access to **Yardi Voyager**
- Your **Century email address** and **Yardi login credentials**
- The **entity codes** for the buildings you want to budget (ask Jacob if unsure)
- **Python 3.10+** installed (only needed for Step 2 — the budget generation step)

### End-to-End Process

| Step | What | Who | Time |
|------|------|-----|------|
| 1 | Download YSL reports from Yardi | Anyone with Yardi access | ~2 min per building |
| 2 | Run the budget pipeline | Jacob or designated operator | ~30 seconds total |
| 3 | Review output budgets | Property manager / accountant | Varies |

---

## Step 1: Download YSL Reports from Yardi

This step uses a JavaScript script you paste into the Chrome browser console. It batch-downloads YSL Annual Budget reports as Excel files.

### 1.1 — Log into Yardi

Open Chrome and go to:
**https://www.yardiasp13.com/03578cms/pages/LoginAdvanced.aspx**

Log in with your Yardi credentials. You can be on any page after login — the script handles navigation.

### 1.2 — Open the Script File

Open **`YSL Budget Script.js`** in any text editor (TextEdit, Notepad, VS Code — anything works).

The file is located in: `budget_system/YSL Budget Script.js`

### 1.3 — Edit Your Settings

Near the top of the file, you'll see three settings to change:

```
const AS_OF_PERIOD = '02/2026';
const ENTITIES = [148, 204, 206, 805];
const EMAIL = 'JSirotkin@Centuryny.com';
```

- **AS_OF_PERIOD** — The budget period in MM/YYYY format (e.g., `'02/2026'`)
- **ENTITIES** — The entity codes for the buildings you want, separated by commas
- **EMAIL** — Your Century email address

### 1.4 — Copy the Entire Script

Select all text in the file (Cmd+A on Mac, Ctrl+A on Windows), then copy (Cmd+C / Ctrl+C).

### 1.5 — Open the Chrome Console

On the Yardi page in Chrome, press:
- **Mac:** Cmd + Option + J
- **Windows:** Ctrl + Shift + J

A panel will open at the bottom or side of the screen.

### 1.6 — Paste and Run

Click inside the Console panel, paste (Cmd+V / Ctrl+V), and press **Enter**.

### 1.7 — Wait for Downloads

The script logs progress for each building. Files download automatically to your **Downloads** folder, named like `YSL_Annual_Budget_148.xlsx`.

### Troubleshooting

| Problem | Fix |
|---------|-----|
| Nothing happens after pasting | Make sure you pressed Enter after pasting |
| "FAILED: No Records" | Your Yardi session may have expired — log in again and retry |
| Script seems stuck (60+ seconds) | Type `location.reload()` in Console and press Enter, then start over |
| File is very small (< 1 KB) | The entity code may be wrong — check it in Yardi |
| Console shows red errors | Refresh the Yardi page, log in fresh, and try again |

---

## Step 2: Run the Budget Pipeline

This step parses the downloaded YSL files and populates budget templates. **Requires Python.**

### 2.1 — Move YSL Files

Copy all downloaded `YSL_Annual_Budget_XXX.xlsx` files into:
`budget_system/ysl_downloads/`

### 2.2 — Verify buildings.csv

Open `budget_system/buildings.csv` and make sure it lists every entity you want to process. Format:

```
entity_code,building_name,address,city,zip,type,units
148,130 E. 18 Owners Corp.,130 East 18th St,New York,10003,Coop,280
```

If you're adding new buildings, add rows following this format. The entity_code must match the YSL filename number.

### 2.3 — Run the Pipeline

Open a terminal and run:

```bash
cd budget_system
python run_budgets.py
```

Or, if using the batch runner:

```bash
python run_batch.py
```

The script will:
1. Find all `YSL_Annual_Budget_*.xlsx` files in the folder
2. Parse each one for GL-level data
3. Map 200 GL codes to the correct template sheets and rows
4. Populate the budget template with Prior Year Actual, YTD Actual, YTD Budget, and Current Year Budget
5. Populate the Insurance Schedule with premium and budget data
6. Save each budget to `budgets/{entity} - {name}/`

### 2.4 — Check the Output

Each building gets its own folder under `budgets/` containing:
- `{entity}_{name}_2027_Budget.xlsx` — the populated budget workbook

Look for the SUCCESS/FAILED summary at the end of the script output.

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `YSL Budget Script.js` | Chrome Console script for downloading YSL reports |
| `Budget_Final_Template_v2.xlsx` | Master budget template (200 GL codes, do not edit unless adding GLs) |
| `buildings.csv` | Entity list — add buildings here to include them in batch runs |
| `run_budgets.py` | Main pipeline script |
| `ysl_parser.py` | Parses YSL Excel files into GL data |
| `gl_mapper.py` | Scans template for GL code locations |
| `template_populator.py` | Fills template with parsed YSL data |
| `config.py` | Configuration and column mappings |

---

## Column Mapping Reference

How YSL report columns map to budget template columns:

| YSL Column | YSL Content | Budget Template Column | Template Header |
|------------|-------------|----------------------|-----------------|
| E (period_2) | 2025 Full Year Actual | D | Prior Year Actual |
| F (period_3) | YTD 02/2026 Actual | E | YTD Actual |
| G (period_4) | YTD 02/2026 Budget | H | YTD Budget |
| H (period_5) | 2026 Approved Budget | K | Current Year Budget |

---

## Adding New Buildings

1. Get the entity code from Yardi
2. Add a row to `buildings.csv`
3. Add the entity code to the `ENTITIES` array in `YSL Budget Script.js`
4. Run the YSL download (Step 1)
5. Run the pipeline (Step 2)

---

## Notes and Known Limitations

- The template contains 200 GL codes covering all operational expense categories. If a building uses a GL code not in the template, the pipeline will skip it and log an "Unmatched" count. Tell Jacob if you see high unmatched counts — it may mean the template needs a new GL line.
- YSL files include balance sheet (1xxx-3xxx) and capital (7xxx) codes that are intentionally excluded from operating budgets. The "Unmatched" count in the pipeline output includes these — that's normal.
- The Insurance Schedule is auto-populated with Current Annual Premium (prior year actual) and Current Year Budget from the YSL data.
- Budget Summary totals pull from the detail sheets. If you manually edit GL values after generation, the summary should update via Excel formulas.
