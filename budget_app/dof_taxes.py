"""
NYC Department of Finance — Real Estate Tax Data Module

Fetches assessed values, tax rates, and exemption data for co-op properties
from the NYC Open Data SODA API, with fallback to cached/known values.

Used by the budget app to auto-populate the RE Taxes tab for co-ops.
"""

import json
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ─── Property Configuration ───────────────────────────────────────────────
# BBL (Borough-Block-Lot) and known tax data for managed co-ops.
# This is the canonical source of property-level tax info.
# Add new properties here as they join the portfolio.

PROPERTY_TAX_CONFIG = {
    "204": {
        "building_name": "444 East 86th Street Owners Corp.",
        "address": "444 East 86th St, New York, NY 10028",
        "bbl": "1-01565-0029",
        "borough": "1",
        "block": "01565",
        "lot": "0029",
        "tax_class": "2",
        "property_type": "coop",
        "units": 315,
        # Known DOF values (updated 2026-03)
        # Transitional AV (from 2025-2026 Final Assessment Roll)
        # Market AV is 43,138,800 but Trans AV is what taxes are based on
        "assessed_value": 39979620,
        "annual_tax": 4155283,
        "tax_rate": 0.096324,
    },
    "148": {
        "building_name": "130 E. 18 Owners Corp.",
        "address": "130 East 18th St, New York, NY 10003",
        "bbl": "1-00878-0048",
        "borough": "1",
        "block": "00878",
        "lot": "0048",
        "tax_class": "2",
        "property_type": "coop",
        "units": 271,
        "assessed_value": 23912100,
        "annual_tax": 2226885,
        "tax_rate": 0.093128,
    },
    "206": {
        "building_name": "77 Bleecker Street Corp.",
        "address": "77 Bleecker St, New York, NY 10012",
        "bbl": "1-00532-0020",
        "borough": "1",
        "block": "00532",
        "lot": "0020",
        "tax_class": "2",
        "property_type": "coop",
        "units": 243,
        "assessed_value": 36019350,
        "annual_tax": 3400090,
        "tax_rate": 0.094396,
    },
    "106": {
        "building_name": "5 West 14th Owners Corp.",
        "address": "10 West 15th St, New York, NY 10011",
        "bbl": "1-00821-0021",
        "borough": "1",
        "block": "00821",
        "lot": "0021",
        "tax_class": "2",
        "property_type": "coop",
        "units": 429,
        "assessed_value": 0,
        "annual_tax": 0,
        "tax_rate": 0.0,
    },
}

# NYC Open Data SODA API endpoint for property valuation
SODA_API_URL = "https://data.cityofnewyork.us/resource/8y4t-faws.json"

# Path to cached DOF data (persists between app restarts)
CACHE_DIR = Path(__file__).parent / "data"
CACHE_FILE = CACHE_DIR / "dof_tax_cache.json"


def get_property_tax_config(entity_code: str) -> dict | None:
    """Get the tax configuration for a property by entity code.

    Returns None if the property is not a co-op or not in the config.
    """
    return PROPERTY_TAX_CONFIG.get(entity_code)


def is_coop(entity_code: str, buildings: list[dict] = None) -> bool:
    """Check if a building is a co-op (needs RE Taxes tab).

    Uses the buildings CSV 'type' field. Falls back to PROPERTY_TAX_CONFIG.
    """
    if buildings:
        for b in buildings:
            if b.get("entity_code") == entity_code:
                btype = (b.get("type") or "").lower()
                return btype in ("coop", "co-op")
    # Fallback to config
    cfg = PROPERTY_TAX_CONFIG.get(entity_code)
    if cfg:
        return cfg.get("property_type") == "coop"
    return False


def fetch_dof_data(entity_code: str) -> dict | None:
    """Fetch current tax assessment data from NYC DOF via SODA API.

    Returns dict with assessed_value, tax_rate, annual_tax or None on failure.
    Falls back to cached/hardcoded values if API is unreachable.
    """
    cfg = PROPERTY_TAX_CONFIG.get(entity_code)
    if not cfg:
        return None

    # Try SODA API first
    # Note: This dataset (8y4t-faws) uses separate boro/block/lot columns,
    # NOT a combined bble field. Column names:
    #   curacttot = current actual total assessed value
    #   curmkttot = current market value total
    #   curtxbtot = current taxable total
    #   curtrntot = current transitional total
    # Tax rate and tax amount are NOT in this dataset — we use the
    # hardcoded rate from PROPERTY_TAX_CONFIG as the base.
    try:
        import requests
        params = {
            "boro": cfg["borough"],
            "block": cfg["block"],
            "lot": cfg["lot"],
            "$limit": 1,
        }
        resp = requests.get(SODA_API_URL, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                record = data[0]
                # Use transitional AV (curtrntot) for budgeting — this is
                # what the actual tax bill is based on, not the full actual AV
                transitional_av = float(record.get("curtrntot", 0))
                actual_av = float(record.get("curacttot", 0))
                market_value = float(record.get("curmkttot", 0))
                # Prefer transitional; fall back to actual if transitional is 0
                assessed_value = transitional_av if transitional_av > 0 else actual_av
                # Tax rate not in this dataset — use config value
                tax_rate = cfg.get("tax_rate", 0)
                annual_tax = assessed_value * tax_rate if tax_rate else cfg.get("annual_tax", 0)
                result = {
                    "entity_code": entity_code,
                    "bbl": cfg["bbl"],
                    "assessed_value": assessed_value,
                    "actual_av": actual_av,
                    "transitional_av": transitional_av,
                    "tax_rate": tax_rate,
                    "annual_tax": round(annual_tax, 2),
                    "market_value": market_value,
                    "source": "nyc_open_data_api",
                    "tax_class": cfg.get("tax_class", record.get("curtaxclass", "2")),
                }
                # Cache the result
                _save_cache(entity_code, result)
                logger.info(f"DOF data fetched from API for {entity_code}: AV=${result['assessed_value']:,.0f}")
                return result
    except Exception as e:
        logger.warning(f"DOF API fetch failed for {entity_code}: {e}")

    # Try cache
    cached = _load_cache(entity_code)
    if cached:
        logger.info(f"Using cached DOF data for {entity_code}")
        return cached

    # Fall back to hardcoded config values
    if cfg.get("assessed_value", 0) > 0:
        logger.info(f"Using hardcoded DOF data for {entity_code}")
        return {
            "entity_code": entity_code,
            "bbl": cfg["bbl"],
            "assessed_value": cfg["assessed_value"],
            "tax_rate": cfg["tax_rate"],
            "annual_tax": cfg["annual_tax"],
            "market_value": 0,
            "source": "hardcoded_config",
            "tax_class": cfg["tax_class"],
        }

    return None


def compute_re_taxes(entity_code: str, overrides: dict = None) -> dict:
    """Compute the full RE Taxes calculation for a co-op property.

    Returns a dict matching the RE Taxes tab structure:
    - 1st half: actual AV × actual rate / 2
    - 2nd half: estimated AV × estimated rate / 2
    - Gross = 1st + 2nd
    - Exemptions (veteran, SCHE, STAR, co-op abatement)
    - Net = Gross - Total Exemptions

    `overrides` can supply: transitional_av_increase, est_tax_rate,
    and exemption amounts.
    """
    overrides = overrides or {}
    dof = fetch_dof_data(entity_code)
    cfg = PROPERTY_TAX_CONFIG.get(entity_code, {})

    if not dof:
        # Return empty structure
        return _empty_re_taxes(entity_code)

    av = overrides.get("assessed_value", dof["assessed_value"])
    rate = overrides.get("tax_rate", dof["tax_rate"])
    trans_increase = overrides.get("transitional_av_increase", 0.0)
    est_rate = overrides.get("est_tax_rate", rate)

    # 1st Half (Jul-Dec): actual
    first_half_tax = av * rate / 2

    # 2nd Half (Jan-Jun): estimated
    est_av = av * (1 + trans_increase)
    second_half_tax = est_av * est_rate / 2

    gross_tax = first_half_tax + second_half_tax

    # Exemptions
    exemptions = {
        "veteran": {
            "gl_code": "6315-0025",
            "growth_pct": overrides.get("veteran_growth", 0.0),
            "current_year": overrides.get("veteran_current", 0.0),
            "budget_year": 0.0,  # computed below
        },
        "sche": {
            "gl_code": "6315-0035",
            "growth_pct": overrides.get("sche_growth", 0.0),
            "current_year": overrides.get("sche_current", 0.0),
            "budget_year": 0.0,
        },
        "star": {
            "gl_code": "6315-0020",
            "growth_pct": overrides.get("star_growth", 0.0),
            "current_year": overrides.get("star_current", 0.0),
            "budget_year": 0.0,
        },
        "coop_abatement": {
            "gl_code": "6315-0010",
            "growth_pct": overrides.get("abatement_growth", 0.0),
            "current_year": overrides.get("abatement_current", 0.0),
            "budget_year": 0.0,
        },
    }

    total_exemptions_current = 0
    total_exemptions_budget = 0
    for key, ex in exemptions.items():
        ex["budget_year"] = ex["current_year"] * (1 + ex["growth_pct"])
        total_exemptions_current += ex["current_year"]
        total_exemptions_budget += ex["budget_year"]

    net_tax = gross_tax - total_exemptions_budget

    return {
        "entity_code": entity_code,
        "bbl": cfg.get("bbl", dof.get("bbl", "")),
        "tax_class": cfg.get("tax_class", "2"),
        "address": cfg.get("address", ""),
        "source": dof.get("source", "unknown"),
        # 1st half
        "assessed_value": av,
        "tax_rate": rate,
        "first_half_tax": round(first_half_tax, 2),
        # 2nd half
        "transitional_av_increase": trans_increase,
        "est_assessed_value": round(est_av, 2),
        "est_tax_rate": est_rate,
        "second_half_tax": round(second_half_tax, 2),
        # Totals
        "gross_tax": round(gross_tax, 2),
        "exemptions": exemptions,
        "total_exemptions_current": round(total_exemptions_current, 2),
        "total_exemptions_budget": round(total_exemptions_budget, 2),
        "net_tax": round(net_tax, 2),
    }


def _empty_re_taxes(entity_code: str) -> dict:
    """Return an empty RE Taxes structure for properties without DOF data."""
    cfg = PROPERTY_TAX_CONFIG.get(entity_code, {})
    return {
        "entity_code": entity_code,
        "bbl": cfg.get("bbl", ""),
        "tax_class": cfg.get("tax_class", ""),
        "address": cfg.get("address", ""),
        "source": "none",
        "assessed_value": 0,
        "tax_rate": 0,
        "first_half_tax": 0,
        "transitional_av_increase": 0,
        "est_assessed_value": 0,
        "est_tax_rate": 0,
        "second_half_tax": 0,
        "gross_tax": 0,
        "exemptions": {
            "veteran": {"gl_code": "6315-0025", "growth_pct": 0, "current_year": 0, "budget_year": 0},
            "sche": {"gl_code": "6315-0035", "growth_pct": 0, "current_year": 0, "budget_year": 0},
            "star": {"gl_code": "6315-0020", "growth_pct": 0, "current_year": 0, "budget_year": 0},
            "coop_abatement": {"gl_code": "6315-0010", "growth_pct": 0, "current_year": 0, "budget_year": 0},
        },
        "total_exemptions_current": 0,
        "total_exemptions_budget": 0,
        "net_tax": 0,
    }


def _save_cache(entity_code: str, data: dict):
    """Save DOF data to local cache file."""
    try:
        cache = {}
        if CACHE_FILE.exists():
            with open(CACHE_FILE) as f:
                cache = json.load(f)
        cache[entity_code] = data
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save DOF cache: {e}")


def _load_cache(entity_code: str) -> dict | None:
    """Load DOF data from local cache file."""
    try:
        if CACHE_FILE.exists():
            with open(CACHE_FILE) as f:
                cache = json.load(f)
            return cache.get(entity_code)
    except Exception as e:
        logger.warning(f"Failed to load DOF cache: {e}")
    return None
