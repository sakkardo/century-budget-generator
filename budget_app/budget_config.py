"""Cycle / environment configuration (leaf module — no app, no db, no workflow deps).

Architecture Phase 3, step 4 (2026-06-08). Extracted verbatim from workflow.py.
This is the BOTTOM of the import graph: it imports only the stdlib, so any module
(workflow.py, models.py, app.py) can import from it with zero circular-import risk.
That property is the whole point — the SQLAlchemy models reference BUDGET_YEAR 13x,
and they can only be pulled out of workflow.py cleanly once BUDGET_YEAR lives below
them. Re-exported by workflow.py so every existing reference resolves unchanged.

Change BUDGET_YEAR (or set the BUDGET_YEAR env var) ONCE each cycle. All routes,
queries, and column headers derive their years from it. BY=2027 means:
    Col 1 = 2024 Actual   (BY-3)
    Col 2 = 2025 Actual   (BY-2)
    Col 3 = 2026 YTD      (BY-1)
    Col 4 = 2026 Est.     (BY-1)
    Col 5 = 2026 Forecast (BY-1)
    Col 6 = 2026 Budget   (BY-1)
    Col 7 = 2027 Budget   (BY)
"""
import os

BUDGET_YEAR = int(os.environ.get("BUDGET_YEAR", 2027))
