"""
Batch Import — Extract Col 1 (Prior Actual) + Col 6 (Approved Budget) + Row Framework
from all approved budget Excel files.

For the 2027 budget cycle:
  Col 1 = 2024 Actual* (last audited_actual column in yrlycomp)
  Col 6 = 2026 Budget  (last budget column in yrlycomp, which = approved budget)

Everything else (cols 2-5, 7-8) is left blank for the app to compute.
"""

import os
import json
import glob
from budget_summary_parser import parse_yrlycomp


def extract_importable_data(parsed):
    """
    From a parsed yrlycomp result, extract only:
      - Row framework (labels, sections, row types)
      - Col 1: last audited_actual column values (2024 Actual)
      - Col 6: last budget column values (approved budget for current cycle)

    Returns dict ready for DB storage or batch report.
    """
    if "error" in parsed:
        return {"error": parsed["error"]}

    columns = parsed.get("columns", [])
    rows = parsed.get("rows", [])

    # Find the last audited_actual column → Col 1 (2024 Actual)
    col1_source = None
    for col in reversed(columns):
        if col["type"] == "audited_actual":
            col1_source = col
            break

    # Find the last budget column → Col 6 (2026 Approved Budget)
    # The last "budget" type column is the new year's approved budget
    budget_cols = [c for c in columns if c["type"] == "budget"]
    col6_source = budget_cols[-1] if budget_cols else None

    # Also grab the second-to-last budget col if it exists (current year budget)
    # This would be the "2025 Budget" equivalent — not imported but useful for validation
    prev_budget_source = budget_cols[-2] if len(budget_cols) >= 2 else None

    imported_rows = []
    for row in rows:
        # Extract Col 1 value
        col1_val = None
        if col1_source:
            raw = row["values"].get(str(col1_source["col_num"])) or row["values"].get(col1_source["col_num"])
            if raw is not None and not isinstance(raw, str):
                col1_val = round(float(raw), 2)

        # Extract Col 6 value (approved budget)
        col6_val = None
        if col6_source:
            raw = row["values"].get(str(col6_source["col_num"])) or row["values"].get(col6_source["col_num"])
            if raw is not None and not isinstance(raw, str):
                col6_val = round(float(raw), 2)

        imported_rows.append({
            "label": row["label"],
            "section": row.get("section"),
            "row_type": row["row_type"],
            "display_order": row.get("display_order"),
            "footnote_marker": row.get("footnote_marker"),
            "col1_prior_actual": col1_val,
            "col6_approved_budget": col6_val,
        })

    return {
        "entity_code": parsed.get("entity_code"),
        "building_name": parsed.get("building_name"),
        "budget_year": parsed.get("budget_year"),
        "source_file": parsed.get("source_file"),
        "col1_label": col1_source["display"] if col1_source else None,
        "col6_label": col6_source["display"] if col6_source else None,
        "rows": imported_rows,
        "stats": {
            "total_rows": len(imported_rows),
            "data_rows": len([r for r in imported_rows if r["row_type"] == "data"]),
            "subtotal_rows": len([r for r in imported_rows if r["row_type"] == "subtotal"]),
            "rows_with_col1": len([r for r in imported_rows if r["col1_prior_actual"] is not None]),
            "rows_with_col6": len([r for r in imported_rows if r["col6_approved_budget"] is not None]),
        }
    }


def enrich_with_gl_map(import_data):
    """
    Enrich extract_importable_data() output with GL prefix + source_tab info
    from GL_TO_SUMMARY_MAP.  Must be called before POSTing to /api/summary/import.

    Adds to each row:
      - gl_prefixes: list of GL prefix strings (for cols 3-5 aggregation)
      - source_tab: which app tab the data comes from (for display)
    """
    from GL_TO_SUMMARY_MAP import SUMMARY_ROW_MAP, LABEL_ALIASES, _CONDO_ROWS

    for row in import_data.get("rows", []):
        label = row["label"]
        if row["row_type"] != "data":
            row["gl_prefixes"] = []
            row["source_tab"] = None
            continue

        # Direct match
        cfg = SUMMARY_ROW_MAP.get(label)
        if not cfg:
            canonical = LABEL_ALIASES.get(label)
            if canonical:
                cfg = SUMMARY_ROW_MAP.get(canonical)
        if not cfg:
            cfg = _CONDO_ROWS.get(label)

        if cfg:
            row["gl_prefixes"] = cfg.get("gl_prefix", [])
            row["source_tab"] = cfg.get("sheet")
        else:
            row["gl_prefixes"] = []
            row["source_tab"] = None

    return import_data


def batch_scan(folder_path):
    """
    Scan all approved budget Excel files in a folder.
    Returns a report of what parsed vs what failed.
    """
    pattern = os.path.join(folder_path, "*.xlsx")
    files = sorted(glob.glob(pattern))

    results = {
        "total_files": len(files),
        "success": [],
        "failed": [],
        "buildings": [],
    }

    for filepath in files:
        filename = os.path.basename(filepath)
        try:
            parsed = parse_yrlycomp(filepath)

            if "error" in parsed:
                results["failed"].append({
                    "file": filename,
                    "error": parsed["error"],
                })
                continue

            imported = extract_importable_data(parsed)

            if "error" in imported:
                results["failed"].append({
                    "file": filename,
                    "error": imported["error"],
                })
                continue

            results["success"].append({
                "file": filename,
                "entity_code": imported["entity_code"],
                "building_name": imported["building_name"],
                "budget_year": imported["budget_year"],
                "col1_label": imported["col1_label"],
                "col6_label": imported["col6_label"],
                "stats": imported["stats"],
            })

            results["buildings"].append(imported)

        except Exception as e:
            results["failed"].append({
                "file": filename,
                "error": str(e),
            })

    results["success_count"] = len(results["success"])
    results["fail_count"] = len(results["failed"])
    results["success_rate"] = f"{len(results['success'])/len(files)*100:.1f}%" if files else "0%"

    return results


if __name__ == "__main__":
    folder = "/sessions/ecstatic-sleepy-thompson/mnt/Budgets/2025 budget approved budgets only"

    print(f"Scanning: {folder}")
    print(f"{'='*80}\n")

    report = batch_scan(folder)

    print(f"BATCH IMPORT SCAN RESULTS")
    print(f"{'─'*80}")
    print(f"Total files:  {report['total_files']}")
    print(f"Parsed OK:    {report['success_count']}")
    print(f"Failed:       {report['fail_count']}")
    print(f"Success rate: {report['success_rate']}")
    print(f"{'─'*80}\n")

    # Show successes
    print(f"✅ SUCCESSFUL ({report['success_count']}):\n")
    print(f"{'Entity':<8} {'Building':<45} {'Year':<6} {'Rows':<6} {'Col1':<6} {'Col6':<6} {'Col1 Label':<20} {'Col6 Label'}")
    print(f"{'─'*8} {'─'*45} {'─'*6} {'─'*6} {'─'*6} {'─'*6} {'─'*20} {'─'*15}")

    for s in sorted(report["success"], key=lambda x: x["entity_code"]):
        stats = s["stats"]
        print(f"{s['entity_code']:<8} {(s['building_name'] or '?')[:44]:<45} {s['budget_year'] or '?':<6} "
              f"{stats['total_rows']:<6} {stats['rows_with_col1']:<6} {stats['rows_with_col6']:<6} "
              f"{(s['col1_label'] or '?')[:19]:<20} {s['col6_label'] or '?'}")

    # Show failures
    if report["failed"]:
        print(f"\n\n❌ FAILED ({report['fail_count']}):\n")
        for f in report["failed"]:
            print(f"  {f['file'][:60]}")
            print(f"    Error: {f['error'][:100]}")

    # Save full report
    output_path = "/sessions/ecstatic-sleepy-thompson/mnt/Budgets/budget_summary/batch_scan_report.json"
    # Save without full building data to keep it readable
    save_report = {k: v for k, v in report.items() if k != "buildings"}
    with open(output_path, "w") as f:
        json.dump(save_report, f, indent=2, default=str)
    print(f"\n\nReport saved to: batch_scan_report.json")

    # Save full building data separately
    buildings_path = "/sessions/ecstatic-sleepy-thompson/mnt/Budgets/budget_summary/batch_imported_buildings.json"
    with open(buildings_path, "w") as f:
        json.dump(report["buildings"], f, indent=2, default=str)
    print(f"Building data saved to: batch_imported_buildings.json")
