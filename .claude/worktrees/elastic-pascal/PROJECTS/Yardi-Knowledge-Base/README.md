# Yardi Knowledge Base

This folder contains reference documentation for automating Yardi Voyager (Century Management's property management system) and understanding its data structures.

## When to Read This

- **Before automating any Yardi task:** Start with `Yardi-Automation-Patterns.md` to understand the console fetch script pattern
- **When parsing Yardi reports:** Read `YSL-Report-Structure.md` to understand the Excel file layout and GL code categorization
- **For context about Century Management:** Read `Century-Management-Context.md` for company-specific details like entity codes, the Yardi instance URL, and budget structure

## Key Files

| File | Purpose |
|------|---------|
| `Yardi-Automation-Patterns.md` | Technical patterns for browser Console fetch scripts, form field requirements, async polling, and common pitfalls |
| `YSL-Report-Structure.md` | Excel file layout, column meanings, GL code categories, and how to identify duplicate GL codes |
| `Century-Management-Context.md` | Company context: entity codes, URL, budget cycle, GL code ranges, and current year info |

## Quick Start for New Sessions

1. Read this README
2. If automating reports or forms: go to `Yardi-Automation-Patterns.md`
3. If parsing YSL reports: go to `YSL-Report-Structure.md`
4. If unsure about Century Management context: check `Century-Management-Context.md`
