#!/usr/bin/env python3
"""Hourly cron: run the Monthly Financial Snapshot timer (24h PM reminder, 48h overdue email, retry held saves).

Start command of a Railway scheduled service (same pattern as audit_sync_cron.py):
    python scripts/snapshot_cron.py        schedule: 0 * * * *

URL resolution order:
  1. SNAPSHOT_TICK_URL env var (explicit override)
  2. http://${WEB_PRIVATE_DOMAIN}:${WEB_PRIVATE_PORT}/api/snapshots/cron/tick  (Railway internal)
  3. Public production URL (fallback)
Auth: ADMIN_KEY env var is sent as X-Admin-Key. Reference the web service's ADMIN_KEY in this service.
The endpoint is safe to call any number of times: each reminder and escalation is sent once.
"""
import json
import os
import sys
import urllib.request


def resolve_url():
    explicit = os.environ.get("SNAPSHOT_TICK_URL")
    if explicit:
        return explicit
    host = os.environ.get("WEB_PRIVATE_DOMAIN")
    port = os.environ.get("WEB_PRIVATE_PORT", "8080")
    if host:
        return "http://%s:%s/api/snapshots/cron/tick" % (host, port)
    return "https://century-budget-generator-production.up.railway.app/api/snapshots/cron/tick"


def main():
    url = resolve_url()
    key = os.environ.get("ADMIN_KEY", "").strip()
    print("snapshot cron: POST %s (admin_key=%s)" % (url, "set" if key else "MISSING"), flush=True)
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-Admin-Key"] = key
    req = urllib.request.Request(url, method="POST", headers=headers, data=b"{}")
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print("  HTTP error: %s" % e, file=sys.stderr)
        return 1
    print("  reminded=%s escalated=%s saved=%s" % (data.get("reminded"), data.get("escalated"), data.get("saved")), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
