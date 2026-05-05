#!/usr/bin/env python3
"""Daily cron: trigger audit-sync from master folder to entity folders.

Designed as start command of a Railway scheduled service. POSTs to the
audit-sync endpoint, prints the response summary, exits non-zero on failure.

URL resolution order:
  1. AUDIT_SYNC_URL env var (explicit override)
  2. http://${WEB_PRIVATE_DOMAIN}:${WEB_PRIVATE_PORT}/api/admin/audit-sync/run
     (Railway internal — uses the linked service's private hostname/port)
  3. Public production URL (fallback)
"""
import json
import os
import sys
import urllib.request


def resolve_url():
    explicit = os.environ.get("AUDIT_SYNC_URL")
    if explicit:
        return explicit
    private_host = os.environ.get("WEB_PRIVATE_DOMAIN")
    private_port = os.environ.get("WEB_PRIVATE_PORT", "8080")
    if private_host:
        return f"http://{private_host}:{private_port}/api/admin/audit-sync/run"
    return "https://century-budget-generator-production.up.railway.app/api/admin/audit-sync/run"


def main():
    url = resolve_url()
    print(f"audit-sync cron: POST {url}", flush=True)
    req = urllib.request.Request(
        url,
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
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
