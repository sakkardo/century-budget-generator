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
        # Known DOF values (updated 2026-04, from SODA API year=2027)
        "assessed_value": 41182920,       # Current Transitional AV (curtrntot)
        "actual_av": 43114500,            # Current Actual AV (curacttot)
        "prior_trans_av": 39979620,       # Prior Year Transitional AV (pytrntot)
        "prior_actual_av": 43138800,      # Prior Year Actual AV (pyacttot)
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
    "212": {
        "building_name": "221 East 36th Owners Corp.",
        "address": "225 East 36th St, New York, NY 10016",
        "bbl": "1-00917-0017",
        "borough": "1",
        "block": "00917",
        "lot": "0017",
        "tax_class": "2",
        "property_type": "coop",
        "units": 285,
        "assessed_value": 0,
        "annual_tax": 0,
        "tax_rate": 0.0,
    },
}

# NYC Open Data SODA API endpoint for property valuation
SODA_API_URL = "https://data.cityofnewyork.us/resource/8y4t-faws.json"

# NYC Planning Labs free GeoSearch API — turns an address into a BBL.
# Returns BBL inside addendum.pad.bbl. No API key needed.
NYC_GEOSEARCH_URL = "https://geosearch.planninglabs.nyc/v2/search"

# FY2025-26 NYC class-2 final tax rate (cooperatives + 3+ unit rentals).
# Updated annually around June. Used as a fallback for coops not in
# PROPERTY_TAX_CONFIG. The FA can override per-building via the RE Tax
# tab UI.
DEFAULT_TAX_RATE_CLASS_2 = 0.12502

# Path to cached DOF data (persists between app restarts)
CACHE_DIR = Path(__file__).parent / "data"
CACHE_FILE = CACHE_DIR / "dof_tax_cache.json"
# Resolved-BBL cache: address → BBL. Saves repeat GeoSearch calls.
BBL_CACHE_FILE = CACHE_DIR / "bbl_cache.json"


def _load_bbl_cache() -> dict:
    """Load resolved BBL cache (address-keyed)."""
    try:
        if BBL_CACHE_FILE.exists():
            with open(BBL_CACHE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_bbl_cache(cache: dict) -> None:
    """Persist BBL cache."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(BBL_CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception as e:
        logger.warning(f"BBL cache save failed: {e}")


def _get_address_from_csv(entity_code: str) -> tuple[str, str] | tuple[None, None]:
    """Read (address, zip) from buildings.csv for a given entity_code.
    Returns (None, None) if not found or CSV missing."""
    try:
        import csv as _csv
        # buildings.csv lives one level up at budget_system/
        csv_path = Path(__file__).parent.parent / "budget_system" / "buildings.csv"
        if not csv_path.exists():
            return (None, None)
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                if str(row.get("entity_code", "")).strip() == str(entity_code).strip():
                    addr = (row.get("address") or "").strip()
                    zip_ = (row.get("zip") or "").strip()
                    return (addr, zip_) if addr else (None, None)
    except Exception as e:
        logger.warning(f"buildings.csv read failed for {entity_code}: {e}")
    return (None, None)


def _lookup_bbl_from_address(address: str, zip_code: str = "") -> str | None:
    """Look up a NYC BBL (10-digit parid) from a street address using
    the NYC Planning Labs GeoSearch API. Returns None on failure.

    BBL format: borough(1) + block(5) + lot(4) e.g. "1013410044"
    Used by fetch_dof_data() to support coops not in PROPERTY_TAX_CONFIG.
    """
    if not address:
        return None
    cache_key = f"{address.lower().strip()}|{(zip_code or '').strip()}"
    cache = _load_bbl_cache()
    if cache_key in cache:
        return cache[cache_key] or None  # cached negative = ""
    try:
        import requests
        text = f"{address} {zip_code} New York NY".strip()
        resp = requests.get(
            NYC_GEOSEARCH_URL,
            params={"text": text, "size": 1},
            timeout=8,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        feats = data.get("features") or []
        if not feats:
            cache[cache_key] = ""
            _save_bbl_cache(cache)
            return None
        props = feats[0].get("properties", {})
        bbl = ((props.get("addendum") or {}).get("pad") or {}).get("bbl")
        if bbl and len(str(bbl)) == 10:
            cache[cache_key] = str(bbl)
            _save_bbl_cache(cache)
            logger.info(f"BBL resolved for '{address}': {bbl}")
            return str(bbl)
    except Exception as e:
        logger.warning(f"GeoSearch BBL lookup failed for '{address}': {e}")
    return None


def _bbl_to_parid(bbl: str) -> str | None:
    """Normalize various BBL formats to a 10-char parid string.
    Accepts:
      "1013410044"   → "1013410044"
      "1-01341-0044" → "1013410044"
      "1-1341-44"    → pad block to 5, lot to 4 → "1013410044"
    Returns None if input is unparseable.
    """
    if not bbl:
        return None
    s = str(bbl).strip()
    if "-" in s:
        parts = s.split("-")
        if len(parts) == 3:
            boro = parts[0].strip()
            block = parts[1].strip().zfill(5)
            lot = parts[2].strip().zfill(4)
            s = boro + block + lot
    s = s.replace("-", "").strip()
    if len(s) == 10 and s.isdigit():
        return s
    return None


def get_property_tax_config(entity_code: str) -> dict | None:
    """Get the tax configuration for a property by entity code.

    Returns None if the property is not a co-op or not in the config.
    """
    return PROPERTY_TAX_CONFIG.get(entity_code)


def is_coop(entity_code: str, buildings: list[dict] = None) -> bool:
    """Check if a building is a co-op (needs RE Taxes tab).

    Resolution order (FA #2 fix, 2026-05-03):
      1. budgets.building_type column for the current BUDGET_YEAR (preferred —
         this is the per-entity per-year source of truth, already auto-backfilled
         from buildings.csv at startup; users can override via the Building Info tab).
      2. CSV `type` field (when explicitly passed in by caller)
      3. PROPERTY_TAX_CONFIG fallback (legacy; only ~5 buildings populated)

    Returns False only if NONE of those say it's a coop. Previously, a
    missing PROPERTY_TAX_CONFIG entry silently flipped real coops to False
    and hid their RE Tax tab.
    """
    # 1. Budget.building_type column (preferred — auto-backfilled from CSV).
    try:
        from flask import current_app
        from sqlalchemy import text
        if current_app and getattr(current_app, "extensions", {}).get("sqlalchemy"):
            db = current_app.extensions["sqlalchemy"]
            row = db.session.execute(
                text("SELECT building_type FROM budgets "
                     "WHERE entity_code = :ec ORDER BY year DESC LIMIT 1"),
                {"ec": entity_code},
            ).fetchone()
            if row and row[0]:
                return str(row[0]).lower() in ("coop", "co-op")
    except Exception:
        # Outside app context (script, test) — fall through to other sources.
        pass

    # 2. CSV data passed by caller
    if buildings:
        for b in buildings:
            if b.get("entity_code") == entity_code:
                btype = (b.get("type") or "").lower()
                return btype in ("coop", "co-op")

    # 3. Legacy fallback: PROPERTY_TAX_CONFIG
    cfg = PROPERTY_TAX_CONFIG.get(entity_code)
    if cfg:
        return cfg.get("property_type") == "coop"

    return False


def fetch_dof_data(entity_code: str) -> dict | None:
    """Fetch current tax assessment data from NYC DOF via SODA API.

    Resolution order (rewritten 2026-05-03 to fix two bugs):
      1. Resolve a 10-digit parid (BBL):
         - From PROPERTY_TAX_CONFIG[bbl] when present (legacy hand-keyed).
         - Else from NYC GeoSearch using the address in buildings.csv.
      2. Query SODA dataset 8y4t-faws by parid (no leading-zero issues
         that broke the prior boro/block/lot query for buildings like 148).
      3. Fill tax_rate from PROPERTY_TAX_CONFIG when present, else use
         DEFAULT_TAX_RATE_CLASS_2 (FA can override).
      4. Fall back to cached/hardcoded values if the API is unreachable.

    Returns None only if we can't resolve a BBL at all (truly unknown property).
    """
    cfg = PROPERTY_TAX_CONFIG.get(entity_code) or {}

    # Step 1: resolve BBL → parid. Prefer GeoSearch (live, address-based)
    # over PROPERTY_TAX_CONFIG[bbl] — at least one of the legacy hand-keyed
    # entries (148) has a wrong BBL, and GeoSearch self-heals that.
    parid = None
    address, zip_ = _get_address_from_csv(entity_code)
    if address:
        bbl = _lookup_bbl_from_address(address, zip_)
        if bbl:
            parid = _bbl_to_parid(bbl)
    if not parid and cfg.get("bbl"):
        # Fall back to config BBL only when address lookup fails.
        parid = _bbl_to_parid(cfg["bbl"])
    if not parid:
        # No BBL anywhere — can't query DOF
        return None

    # Step 2: query SODA by parid (most reliable identifier; avoids
    # leading-zero / bad-config mismatches in the boro/block/lot query)
    tax_rate = cfg.get("tax_rate") or DEFAULT_TAX_RATE_CLASS_2
    try:
        import requests
        params = {
            "$where": f"parid='{parid}'",
            "$limit": 1,
            "$order": "year DESC",
        }
        resp = requests.get(SODA_API_URL, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                record = data[0]
                # cur* = current roll year, py* = prior year, ten* = tentative.
                # Some records have empty cur* (e.g., year=2027 not yet certified);
                # fall back to ten* (tentative) when cur* is missing.
                def _f(*keys):
                    for k in keys:
                        v = record.get(k)
                        if v not in (None, "", "0"):
                            try:
                                return float(v)
                            except (TypeError, ValueError):
                                pass
                    return 0.0
                result = {
                    "entity_code": entity_code,
                    "bbl": parid,
                    "assessed_value": _f("curtrntot", "tentrntot"),
                    "actual_av": _f("curacttot", "tenacttot"),
                    "prior_trans_av": _f("pytrntot"),
                    "prior_actual_av": _f("pyacttot"),
                    "market_value": _f("curmkttot", "tenmkttot"),
                    "taxable_value": _f("curtxbtot", "tentxbtot"),
                    "tax_rate": tax_rate,
                    "source": "nyc_open_data_api",
                    "tax_class": cfg.get("tax_class") or record.get("curtaxclass") or "2",
                    "year": record.get("year", ""),
                }
                _save_cache(entity_code, result)
                logger.info(
                    f"DOF data fetched for {entity_code} (parid={parid}): "
                    f"TransAV=${result['assessed_value']:,.0f}, "
                    f"ActualAV=${result['actual_av']:,.0f}"
                )
                return result
            else:
                logger.warning(f"DOF SODA query returned no records for {entity_code} (parid={parid})")
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
            "assessed_value": cfg["assessed_value"],          # Transitional AV
            "actual_av": cfg.get("actual_av", 0),
            "prior_trans_av": cfg.get("prior_trans_av", 0),
            "prior_actual_av": cfg.get("prior_actual_av", 0),
            "tax_rate": cfg["tax_rate"],
            "market_value": 0,
            "taxable_value": cfg["assessed_value"],
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

    # Transitional AV values from DOF
    current_trans_av = dof.get("assessed_value", 0)       # curtrntot
    prior_trans_av = dof.get("prior_trans_av", 0)         # pytrntot
    actual_av = dof.get("actual_av", 0)                   # curacttot

    # Allow user overrides
    rate = overrides.get("tax_rate", dof.get("tax_rate", 0))
    est_rate = overrides.get("est_tax_rate", rate)

    # Auto-calculate transitional AV increase from DOF data
    if prior_trans_av > 0 and current_trans_av > 0:
        auto_trans_increase = (current_trans_av / prior_trans_av) - 1
    else:
        auto_trans_increase = 0.0
    # User can override the increase %
    trans_increase = overrides.get("transitional_av_increase", auto_trans_increase)

    # 1st Half (Jul-Dec): prior year transitional AV × rate
    first_half_av = overrides.get("first_half_av", prior_trans_av or current_trans_av)
    first_half_tax = first_half_av * rate / 2

    # 2nd Half (Jan-Jun): current transitional AV × estimated rate
    second_half_av = overrides.get("second_half_av", current_trans_av)
    second_half_tax = second_half_av * est_rate / 2

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
        "year": dof.get("year", ""),
        # AV values from DOF
        "current_trans_av": current_trans_av,
        "prior_trans_av": prior_trans_av,
        "actual_av": actual_av,
        # 1st half (Jul-Dec): prior year transitional AV
        "assessed_value": first_half_av,
        "tax_rate": rate,
        "first_half_tax": round(first_half_tax, 2),
        # 2nd half (Jan-Jun): current transitional AV
        "transitional_av_increase": round(trans_increase, 6),
        "est_assessed_value": round(second_half_av, 2),
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
