# services/parser.py — Excel MIS file parser
# Identifies module from filename, reads correct sheet,
# normalises column names, returns clean records.

import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger("bfam.parser")

# ── Module detection patterns (ordered: specific before generic) ──
MODULE_PATTERNS = [
    ("daily_whatsapp",  "wa_daily"),
    ("daily_savings",   "savings_daily"),
    ("whatsapp_ytd",    "wa"),
    ("whatsapp",        "wa"),
    ("savings",         "savings"),
    ("power_of_3",      "po3"),
    ("power of 3",      "po3"),
    ("wealth_sip",      "wsip"),
    ("wealth sip",      "wsip"),
    ("sip_mis",         "sip"),
    ("overall_sip",     "sip"),
]

# ── Sheet config per module ───────────────────────────
SHEET_CONFIG = {
    "wa": {
        "source_sheet": "BIC-DATA",
        "fallback_sheets": ["BIC-Data", "Sheet1"],
        "level": "bic",
        "columns": {
            "BIC":              "bic_name",
            "EMPLOYEE CODE":    "emp_code",
            "ARN_ACTIVATED":    "activation",
            "TXN_COUNT":        "txn_count",
            "INFLOWS":          "inflows",
            "CLUSTER":          "cluster_name",
            "CLUSTER MANAGER":  "manager_name",
            "REGION":           "region_name",
            "INFLOWS TARGETS":  "inflow_target",
            "ACTIVATION TARGETS": "activation_target",
        }
    },
    "wa_daily": {
        "source_sheet": "BIC-DATA",
        "fallback_sheets": ["BIC-Data", "Sheet1"],
        "level": "bic_daily",
        "columns": {
            "BIC":              "bic_name",
            "EMPLOYEE CODE":    "emp_code",
            "ARN_ACTIVATED":    "activation",
            "INFLOWS TXN_COUNT":"txn_count",
            "INFLOWS":          "inflows",
            "CLUSTER":          "cluster_name",
            "CLUSTER MANAGER":  "manager_name",
            "REGION":           "region_name",
        }
    },
    "savings": {
        "source_sheet": "BIC-DATA",
        "fallback_sheets": ["BIC-Data", "Sheet1"],
        "level": "bic",
        "columns": {
            "BIC":                "bic_name",
            "EMPLOYEE CODE":      "emp_code",       # present in daily savings, absent in YTD
            "TXN_COUNT":          "txn_count",
            "INFLOWS TXN_COUNT":  "txn_count",       # daily savings uses this name
            "INFLOWS":            "inflows",
            "ARN_ACTIVATED":      "activation",
            "CLUSTER":            "cluster_name",
            "CLUSTER MANAGER":    "manager_name",
            "REGION":             "region_name",
        }
    },
    "savings_daily": {
        "source_sheet": "BIC-DATA",
        "fallback_sheets": ["BIC-Data", "Sheet1"],
        "level": "bic_daily",
        "columns": {
            "BIC":                "bic_name",
            "EMPLOYEE CODE":      "emp_code",
            "ARN_ACTIVATED":      "activation",
            "INFLOWS TXN_COUNT":  "txn_count",
            "INFLOWS":            "inflows",
            "CLUSTER":            "cluster_name",
            "CLUSTER MANAGER":    "manager_name",
            "REGION":             "region_name",
        }
    },
    "po3": {
        "source_sheet": "Power of 3_BIC-DATA",
        "fallback_sheets": ["Power of 3_BIC-Data", "BIC-DATA", "Sheet1"],
        "level": "bic",
        "columns": {
            "BIC":              "bic_name",
            "EMPLOYEE CODE":    "emp_code",
            "ARN_ACTIVATED":    "activation",
            "INFLOWS TXN_COUNT":"txn_count",
            "INFLOWS":          "inflows",
            "CLUSTER":          "cluster_name",
            "CLUSTER MANAGER":  "manager_name",
            "REGION":           "region_name",
        }
    },
    "wsip": {
        "source_sheet": "Wealth SIP_BIC-DATA",
        "fallback_sheets": ["Wealth SIP_BIC-Data", "BIC-DATA", "Sheet1"],
        "level": "bic",
        "columns": {
            "BIC":              "bic_name",
            "EMPLOYEE CODE":    "emp_code",
            "ARN_ACTIVATED":    "activation",
            "INFLOWS TXN_COUNT":"txn_count",
            "INFLOWS":          "inflows",
            "CLUSTER":          "cluster_name",
            "CLUSTER MANAGER":  "manager_name",
            "REGION":           "region_name",
        }
    },
    "sip": {
        "source_sheet": "SIP-DATA",
        "fallback_sheets": ["BIC-DATA", "Sheet1"],
        "level": "bic",
        "columns": {
            "BIC":              "bic_name",
            "EMPLOYEE CODE":    "emp_code",
            "ARN_ACTIVATED":    "activation",
            "TXN_COUNT":        "txn_count",
            "INFLOWS":          "inflows",
            "CLUSTER":          "cluster_name",
            "CLUSTER MANAGER":  "manager_name",
            "REGION":           "region_name",
        }
    },
}

# Real regions — used to filter out system/unmapped entries
REAL_REGIONS = {
    'Andhra Pradesh, Telangana',
    'Bihar, Jharkhand, Orissa, Chattisgarh',
    'Delhi, NCR',
    'Gujarat',
    'Karnataka',
    'Kerala',
    'Kolkata',
    'Madhya Pradesh',
    'Mumbai',
    'Punjab, Haryana, Himachal Pradesh, Jammu Kashmir',
    'Rajasthan',
    'Rest of Bengal, North East',
    'Rest of Maharashtra, Goa',
    'Tamil Nadu',
    'Uttar Pradesh, Uttarakhand',
}


def identify_module(filename: str) -> str:
    """Identify which module a file belongs to from its filename."""
    normalised = filename.lower().replace(" ", "_").replace("-", "_")
    for pattern, module in MODULE_PATTERNS:
        if pattern.replace(" ", "_") in normalised:
            return module
    return "unknown"


def _open_sheet(filepath: Path, module: str) -> pd.DataFrame:
    cfg = SHEET_CONFIG.get(module)
    if not cfg:
        raise ValueError(f"No config for module: {module}")
    wb = pd.ExcelFile(filepath, engine="openpyxl")
    available = wb.sheet_names
    for sheet in [cfg["source_sheet"]] + cfg.get("fallback_sheets", []):
        if sheet in available:
            logger.info(f"[Parser] Sheet '{sheet}' for {module}")
            return pd.read_excel(filepath, sheet_name=sheet, engine="openpyxl")
    raise ValueError(f"No matching sheet in {filepath.name}. Available: {available}")


def _clean(df: pd.DataFrame, col_map: Dict) -> pd.DataFrame:
    # Normalise header: strip, upper
    df.columns = [str(c).strip().upper() for c in df.columns]
    present = [c for c in col_map if c in df.columns]
    df = df[present].copy()
    df = df.rename(columns={c: col_map[c] for c in present})

    # Drop rows with no meaningful identifier
    for id_col in ["emp_code", "bic_name"]:
        if id_col in df.columns:
            df = df[df[id_col].notna() & (df[id_col].astype(str).str.strip().str.upper() != 'NONE') & (df[id_col].astype(str).str.strip() != '')]

    # Numeric columns
    numeric = ["inflows","net_sales","txn_count","activation","avg_ticket",
               "sip_count","sip_fresh","sip_live","pad3","inflow_target","activation_target"]
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # String columns
    for col in ["emp_code","bic_name","cluster_name","manager_name","region_name"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({'None': '', 'nan': '', 'NAN': ''})

    # Normalise emp_code: remove decimals (75102.0 → 75102)
    if "emp_code" in df.columns:
        def clean_emp(v):
            try:
                return str(int(float(str(v)))) if v and str(v).strip() not in ('', 'None', 'nan') else ''
            except:
                return str(v).strip()
        df["emp_code"] = df["emp_code"].apply(clean_emp)
        df = df[df["emp_code"] != '']

    # Compute avg_ticket if missing
    if "avg_ticket" not in df.columns and "inflows" in df.columns and "txn_count" in df.columns:
        df["avg_ticket"] = df.apply(
            lambda r: round(r["inflows"] / r["txn_count"], 2) if r["txn_count"] > 0 else 0, axis=1)

    # Filter to real regions only (skip Unmapped, Group, Virtual)
    if "region_name" in df.columns:
        df = df[df["region_name"].isin(REAL_REGIONS) | (df["region_name"] == '')]

    return df.reset_index(drop=True)


def parse_file(filepath: Path, module: str, date: str = None) -> Dict:
    """
    Main entry point. Returns dict with records ready for DB insert.
    Always succeeds — errors are returned in the 'errors' list.
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    errors: List[str] = []
    records: List[Dict] = []

    try:
        cfg = SHEET_CONFIG.get(module, {})
        col_map = cfg.get("columns", {})
        level = cfg.get("level", "bic")

        df = _open_sheet(filepath, module)
        df = _clean(df, col_map)
        records = df.to_dict(orient="records")
        logger.info(f"[Parser] {filepath.name} → {len(records)} rows")

    except ValueError as e:
        errors.append(str(e))
        logger.error(f"[Parser] {e}")
    except Exception as e:
        errors.append(f"Unexpected error: {e}")
        logger.exception(f"[Parser] {e}")

    return {
        "module":    module,
        "date":      date,
        "level":     level,
        "records":   records,
        "row_count": len(records),
        "errors":    errors,
    }
