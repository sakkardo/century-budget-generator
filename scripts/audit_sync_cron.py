#!/usr/bin/env python3
"""Daily cron: trigger audit-sync from master folder to entity folders.

Designed to be the start command of a Railway scheduled service. POSTs to
/api/admin/audit-sync/run, prints the response summary, exits with status 0
on success or non-zero on failure.

Configure in Railway:
  - Start command: python scripts/audit_sync_cron.py
  - Cron schedule: 0 8 * * *   (daily at 8am UTC ≈ 3am-4am ET)

Override the URL via AUDIT_SYNC_URL env var if needed.
"""
import json
import os
import sys
import urllib.request


URL = os.environ.get(
    "AUDIT_SYNC_URL",
    "https://century-budget-generator-production.up.railway.app/api/admin/audit-sync/run",
)


def main():
    print(f"audit-sync cron: POST {URL}", flush=True)
    req = urllib.request.Request(
        URL,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=b"{}",
    )
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            payload = resp.read().decode()
            data = json.loads(payload)
    except Exception as e:
        print(f"  HTTP error: {e}", file=sys.stderr)
        return 1

    sm = data.get("summary", {})
    print(
        f"  copied={sm.get('copied', 0)}  skipped={sm.get('skipped', 0)}  "
        f"replaced={sm.get('replaced', 0)}  unmatched={sm.get('unmatched', 0)}  "
        f"error={sm.get('error', 0)}",
        flush=True,
    )
    print(f"  run_id={data.get('run_id', '?')}", flush=True)

    if data.get("fatal_error"):
        print(f"  fatal_error={data['fatal_error']}", file=sys.stderr)
        return 2
    if (data.get("summary") or {}).get("error", 0) > 0:
        # Per-file errors are non-fatal but still surface a non-zero exit so
        # Railway logs the run as failed for visibility.
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
