#!/usr/bin/env python3
"""
Bulk Onboard — Seed all approved budget buildings into the Century Budget App.

Steps:
  1. batch_scan() parses all Excel files → extracts entity codes, summary rows, Col 1 + Col 6
  2. enrich_with_gl_map() adds GL prefix mappings for live aggregation
  3. POST to /api/summary/import/<entity_code> to upsert summary rows
  4. Ensure a Budget record exists for each entity (create if missing)

Usage:
  python bulk_onboard.py                          # Dry run (parse only, no DB writes)
  python bulk_onboard.py --commit                 # Actually push to the live app
  python bulk_onboard.py --commit --url https://centurybudget.up.railway.app
"""

import os
import sys
import json
import argparse
import requests
from datetime import datetime

# Add parent paths for imports
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "budget_app"))

from batch_import import batch_scan, enrich_with_gl_map


# ── Config ──────────────────────────────────────────────────────────────────
DEFAULT_FOLDER = os.path.join(
    os.path.dirname(__file__), "..",
    "2025 budget approved budgets only"
)
DEFAULT_URL = "https://centurybudget.up.railway.app"
BUDGET_YEAR = 2027


def run_scan(folder_path):
    """Step 1: Parse all Excel files."""
    print(f"\n{'='*70}")
    print(f"  BULK ONBOARD — Century Budget App")
    print(f"  Scanning: {os.path.basename(folder_path)}")
    print(f"{'='*70}\n")

    report = batch_scan(folder_path)

    print(f"  Files found:    {report['total_files']}")
    print(f"  Parsed OK:      {report['success_count']}")
    print(f"  Failed:         {report['fail_count']}")
    print(f"  Success rate:   {report['success_rate']}")

    if report["failed"]:
        print(f"\n  ⚠ Failed files:")
        for f in report["failed"]:
            print(f"    • {f['file'][:55]}")
            print(f"      {f['error'][:80]}")

    return report


def enrich_buildings(report):
    """Step 2: Add GL prefix mappings to each building's rows."""
    enriched = 0
    for bldg in report["buildings"]:
        try:
            enrich_with_gl_map(bldg)
            enriched += 1
        except Exception as e:
            print(f"  ⚠ GL enrichment failed for {bldg.get('entity_code')}: {e}")
    print(f"\n  GL mappings enriched: {enriched}/{len(report['buildings'])}")
    return report


def push_to_app(report, base_url, dry_run=True):
    """Step 3 & 4: Create Budget records + import summary rows."""
    buildings = report["buildings"]

    # Deduplicate by entity_code (keep first occurrence)
    seen = {}
    for bldg in buildings:
        ec = bldg.get("entity_code")
        if ec and ec not in seen:
            seen[ec] = bldg
    buildings = list(seen.values())

    print(f"\n{'─'*70}")
    print(f"  {'DRY RUN' if dry_run else 'COMMITTING'}: {len(buildings)} unique entities")
    print(f"  Target: {base_url}")
    print(f"{'─'*70}\n")

    results = {"created": [], "updated": [], "skipped": [], "errors": []}

    for bldg in sorted(buildings, key=lambda b: b.get("entity_code", "")):
        ec = bldg["entity_code"]
        name = bldg.get("building_name", "Unknown")
        row_count = len(bldg.get("rows", []))
        col1_count = sum(1 for r in bldg.get("rows", []) if r.get("col1_prior_actual") is not None)
        col6_count = sum(1 for r in bldg.get("rows", []) if r.get("col6_approved_budget") is not None)

        status = "DRY" if dry_run else "..."
        print(f"  [{ec:>5}] {name[:42]:<44} {row_count} rows, C1={col1_count}, C6={col6_count}  ", end="")

        if dry_run:
            print("✓ ready")
            results["created"].append(ec)
            continue

        try:
            # Step 3a: Ensure Budget record exists
            # POST to a lightweight endpoint that creates Budget if missing
            budget_resp = requests.post(
                f"{base_url}/api/budget/ensure",
                json={
                    "entity_code": ec,
                    "building_name": name,
                    "year": BUDGET_YEAR,
                },
                timeout=15,
            )

            # Step 3b: Import summary rows
            import_resp = requests.post(
                f"{base_url}/api/summary/import/{ec}",
                json=bldg,
                timeout=30,
            )

            if import_resp.status_code == 200:
                data = import_resp.json()
                imported = data.get("imported", 0)
                updated = data.get("updated", 0)
                print(f"✓ {imported} new, {updated} updated")
                results["created" if imported > 0 else "updated"].append(ec)
            else:
                print(f"✗ HTTP {import_resp.status_code}")
                results["errors"].append({"entity": ec, "error": import_resp.text[:100]})

        except Exception as e:
            print(f"✗ {str(e)[:60]}")
            results["errors"].append({"entity": ec, "error": str(e)[:100]})

    # Summary
    print(f"\n{'='*70}")
    print(f"  RESULTS:")
    print(f"    Created:  {len(results['created'])}")
    print(f"    Updated:  {len(results['updated'])}")
    print(f"    Skipped:  {len(results['skipped'])}")
    print(f"    Errors:   {len(results['errors'])}")
    print(f"{'='*70}\n")

    return results


def main():
    parser = argparse.ArgumentParser(description="Bulk onboard buildings to Century Budget App")
    parser.add_argument("--folder", default=DEFAULT_FOLDER, help="Path to approved budget Excel files")
    parser.add_argument("--url", default=DEFAULT_URL, help="App base URL")
    parser.add_argument("--commit", action="store_true", help="Actually push to the app (default is dry run)")
    parser.add_argument("--entity", help="Process only this entity code (for testing)")
    parser.add_argument("--min-rows", type=int, default=10, help="Skip entities with fewer than N data rows (default 10)")
    args = parser.parse_args()

    folder = os.path.abspath(args.folder)
    if not os.path.isdir(folder):
        print(f"Error: folder not found: {folder}")
        sys.exit(1)

    # Step 1: Scan
    report = run_scan(folder)
    if not report["buildings"]:
        print("No buildings parsed. Exiting.")
        sys.exit(1)

    # Filter to single entity if requested
    if args.entity:
        report["buildings"] = [b for b in report["buildings"] if b.get("entity_code") == args.entity]
        if not report["buildings"]:
            print(f"Entity {args.entity} not found in scan results.")
            sys.exit(1)
        print(f"\n  Filtered to entity: {args.entity}")

    # Filter out sparse/empty entities
    if args.min_rows > 0:
        before = len(report["buildings"])
        skipped = [b for b in report["buildings"] if len(b.get("rows", [])) < args.min_rows]
        report["buildings"] = [b for b in report["buildings"] if len(b.get("rows", [])) >= args.min_rows]
        if skipped:
            print(f"\n  Skipped {len(skipped)} entities with < {args.min_rows} rows:")
            for s in skipped:
                print(f"    • [{s.get('entity_code', '?'):>5}] {s.get('building_name', '?')[:45]} ({len(s.get('rows', []))} rows)")
            print(f"  Remaining: {len(report['buildings'])} of {before} entities")

    # Step 2: Enrich with GL mappings
    report = enrich_buildings(report)

    # Step 3+4: Push (or dry run)
    results = push_to_app(report, args.url, dry_run=not args.commit)

    # Save results
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_path = os.path.join(os.path.dirname(__file__), f"onboard_results_{ts}.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"  Results saved to: {os.path.basename(results_path)}")


if __name__ == "__main__":
    main()
