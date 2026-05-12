# services/parser.py — Excel MIS file parser
# Handles both BIC-DATA (aggregated) and DATA (raw transactions) sheets.
# DATA sheets use TRDATE for date-wise breakdown.
#
# CHANGES vs previous version:
#   - parse_data_sheet (WA path): builds 3-tier bic_map + extracts sip_arns_by_date
#     from SIP-DATA sheet. No longer returns a flat arn_data_arns list.
#   - New helpers: _extract_bic_map_from_df, _extract_sip_arns_by_date
#   - Non-WA modules: dead ARN-DATA reading code removed.
#   - Return dict always includes bic_map and sip_arns_by_date keys (empty for non-WA).

import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Set
from datetime import datetime

logger = logging.getLogger("bfam.parser")

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

SHEET_CONFIG = {
    "wa": {
        "source_sheet": "BIC-DATA",
        "fallback_sheets": ["BIC-Data", "Sheet1"],
        "level": "bic",
        "columns": {
            "BIC": "bic_name", "EMPLOYEE CODE": "emp_code",
            "ARN_ACTIVATED": "activation", "TXN_COUNT": "txn_count",
            "INFLOWS": "inflows", "CLUSTER": "cluster_name",
            "CLUSTER MANAGER": "manager_name", "REGION": "region_name",
            "INFLOWS TARGETS": "inflow_target", "ACTIVATION TARGETS": "activation_target",
        }
    },
    "wa_daily": {
        "source_sheet": "BIC-DATA", "fallback_sheets": ["BIC-Data", "Sheet1"],
        "level": "bic_daily",
        "columns": {
            "BIC": "bic_name", "EMPLOYEE CODE": "emp_code",
            "ARN_ACTIVATED": "activation", "INFLOWS TXN_COUNT": "txn_count",
            "INFLOWS": "inflows", "CLUSTER": "cluster_name",
            "CLUSTER MANAGER": "manager_name", "REGION": "region_name",
        }
    },
    "savings": {
        "source_sheet": "BIC-DATA", "fallback_sheets": ["BIC-Data", "Sheet1"],
        "level": "bic",
        "columns": {
            "BIC": "bic_name", "EMPLOYEE CODE": "emp_code",
            "TXN_COUNT": "txn_count", "INFLOWS TXN_COUNT": "txn_count",
            "INFLOWS": "inflows", "ARN_ACTIVATED": "activation",
            "CLUSTER": "cluster_name", "CLUSTER MANAGER": "manager_name",
            "REGION": "region_name",
        }
    },
    "savings_daily": {
        "source_sheet": "BIC-DATA", "fallback_sheets": ["BIC-Data", "Sheet1"],
        "level": "bic_daily",
        "columns": {
            "BIC": "bic_name", "EMPLOYEE CODE": "emp_code",
            "ARN_ACTIVATED": "activation", "INFLOWS TXN_COUNT": "txn_count",
            "INFLOWS": "inflows", "CLUSTER": "cluster_name",
            "CLUSTER MANAGER": "manager_name", "REGION": "region_name",
        }
    },
    "po3": {
        "source_sheet": "Power of 3_BIC-DATA",
        "fallback_sheets": ["Power of 3_BIC-Data", "BIC-DATA", "Sheet1"],
        "level": "bic",
        "columns": {
            "BIC": "bic_name", "EMPLOYEE CODE": "emp_code",
            "ARN_ACTIVATED": "activation", "INFLOWS TXN_COUNT": "txn_count",
            "INFLOWS": "inflows", "CLUSTER": "cluster_name",
            "CLUSTER MANAGER": "manager_name", "REGION": "region_name",
        }
    },
    "wsip": {
        "source_sheet": "Wealth SIP_BIC-DATA",
        "fallback_sheets": ["Wealth SIP_BIC-Data", "BIC-DATA", "Sheet1"],
        "level": "bic",
        "columns": {
            "BIC": "bic_name", "EMPLOYEE CODE": "emp_code",
            "ARN_ACTIVATED": "activation", "INFLOWS TXN_COUNT": "txn_count",
            "INFLOWS": "inflows", "CLUSTER": "cluster_name",
            "CLUSTER MANAGER": "manager_name", "REGION": "region_name",
        }
    },
    "sip": {
        "source_sheet": "SIP-DATA", "fallback_sheets": ["SIP DATA"],
        "level": "sip_raw",
        "columns": {
            "BIC_OWNER": "bic_name", "EMPLOYEE CODE": "emp_code",
            "SIP AMOUNT": "inflows", "AMOUNT": "inflows",
            "CLUSTER": "cluster_name", "CLUSTER MANAGER": "manager_name",
            "REGION": "region_name",
        }
    },
}

DATA_SHEET_COLUMNS = {
    "TRDATE": "trdate", "AMOUNT": "inflows", "BIC_OWNER": "bic_name",
    "EMPLOYEE CODE": "emp_code", "SUB-TRTYPE": "sub_trtype",
    "TRTYPE": "trtype", "CLUSTER": "cluster_name",
    "CLUSTER MANAGER": "manager_name", "REGION": "region_name",
    "TRS_AGENT": "trs_agent",
}

SIP_DATA_COLUMNS = {
    "SIP START DATE": "trdate", "SIP REGISTRATION DATE": "reg_date",
    "SIP AMOUNT": "inflows", "BIC_OWNER": "bic_name",
    "EMPLOYEE CODE": "emp_code", "SUB TR TYPE": "sub_trtype",
    "CLUSTER": "cluster_name", "CLUSTER MANAGER": "manager_name",
    "REGION": "region_name",
}

SIPDATA_COLUMNS = {
    "START_DATE": "trdate", "TRI_REGISTRATION_DATE": "reg_date",
    "AMOUNT": "inflows", "BIC_OWNER": "bic_name",
    "CLUSTER": "cluster_name", "CLUSTER MANAGER": "manager_name",
    "REGION": "region_name",
}

REAL_REGIONS = {
    'Andhra Pradesh, Telangana', 'Bihar, Jharkhand, Orissa, Chattisgarh',
    'Delhi, NCR', 'Gujarat', 'Karnataka', 'Kerala', 'Kolkata',
    'Madhya Pradesh', 'Mumbai',
    'Punjab, Haryana, Himachal Pradesh, Jammu Kashmir', 'Rajasthan',
    'Rest of Bengal, North East', 'Rest of Maharashtra, Goa',
    'Tamil Nadu', 'Uttar Pradesh, Uttarakhand',
}

SKIP_NAMES = {'bfam integration user'}


def _convert_am_code(code: str) -> str:
    """AM000116 → 00116"""
    code = str(code).strip()
    if code.upper().startswith('AM') and len(code) > 2:
        return code[2:].zfill(5)
    return code


def _parse_date(val) -> Optional[str]:
    """Convert DD-MM-YYYY or other formats to YYYY-MM-DD."""
    if not val:
        return None
    s = str(val).strip()
    for fmt in ('%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%b-%Y'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except:
            pass
    return None


def _clean_emp(v) -> str:
    v = str(v).strip()
    if not v or v in ('', 'None', 'nan'): return ''
    if v.upper().startswith('AM'):
        return _convert_am_code(v)
    try:
        return str(int(float(v)))
    except:
        return v


def identify_module(filename: str) -> str:
    normalised = filename.lower().replace(" ", "_").replace("-", "_")
    for pattern, module in MODULE_PATTERNS:
        if pattern.replace(" ", "_") in normalised:
            return module
    return "unknown"


def get_all_modules(filepath: Path) -> List[str]:
    try:
        wb = pd.ExcelFile(filepath, engine="openpyxl")
        sheets = set(wb.sheet_names)
        sheet_to_module = {
            "Power of 3_BIC-DATA": "po3",
            "Wealth SIP_BIC-DATA":  "wsip",
            "BIC-DATA":             "wa",
            "SIP-DATA":             "sip",
            "SIP DATA":             "sip",
        }
        modules, seen = [], set()
        for sheet, mod in sheet_to_module.items():
            if sheet in sheets and mod not in seen:
                modules.append(mod)
                seen.add(mod)
        return modules if modules else [identify_module(filepath.name)]
    except Exception as e:
        logger.warning(f"[Parser] get_all_modules failed: {e}")
        return [identify_module(filepath.name)]


def _open_sheet(filepath: Path, module: str) -> pd.DataFrame:
    cfg = SHEET_CONFIG.get(module)
    if not cfg:
        raise ValueError(f"No config for module: {module}")
    wb = pd.ExcelFile(filepath, engine="openpyxl")
    available = wb.sheet_names
    for sheet in [cfg["source_sheet"]] + cfg.get("fallback_sheets", []):
        if sheet in available:
            return pd.read_excel(filepath, sheet_name=sheet, engine="openpyxl")
    raise ValueError(f"No matching sheet in {filepath.name}. Available: {available}")


def _clean(df: pd.DataFrame, col_map: Dict) -> pd.DataFrame:
    df.columns = [str(c).strip().upper() for c in df.columns]
    present = [c for c in col_map if c in df.columns]
    df = df[present].copy()
    df = df.rename(columns={c: col_map[c] for c in present})

    for id_col in ["emp_code", "bic_name"]:
        if id_col in df.columns:
            df = df[df[id_col].notna()
                    & (df[id_col].astype(str).str.strip().str.upper() != 'NONE')
                    & (df[id_col].astype(str).str.strip() != '')]

    numeric = ["inflows","net_sales","txn_count","activation","avg_ticket",
               "sip_count","inflow_target","activation_target"]
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in ["emp_code","bic_name","cluster_name","manager_name","region_name"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({'None': '', 'nan': '', 'NAN': ''})

    if "emp_code" in df.columns:
        df["emp_code"] = df["emp_code"].apply(_clean_emp)
        df = df[df["emp_code"] != '']

    if "avg_ticket" not in df.columns and "inflows" in df.columns and "txn_count" in df.columns:
        df["avg_ticket"] = df.apply(
            lambda r: round(r["inflows"] / r["txn_count"], 2) if r["txn_count"] > 0 else 0, axis=1)

    if "region_name" in df.columns:
        df = df[df["region_name"].isin(REAL_REGIONS) | (df["region_name"] == '')]

    return df.reset_index(drop=True)


def _process_data_df(df: pd.DataFrame, col_map: Dict, module: str) -> pd.DataFrame:
    """Common cleanup for raw DATA sheet dataframes."""
    df.columns = [str(c).strip().upper() for c in df.columns]
    present = [c for c in col_map if c in df.columns]
    df = df[present].copy()
    df = df.rename(columns={c: col_map[c] for c in present})

    if 'bic_name' in df.columns:
        df = df[~df['bic_name'].astype(str).str.strip().str.lower().isin(SKIP_NAMES)]
        df = df[df['bic_name'].notna()
                & (df['bic_name'].astype(str).str.strip() != '')
                & (df['bic_name'].astype(str).str.strip().str.upper() != 'NONE')]

    if 'trdate' in df.columns:
        df['trdate'] = df['trdate'].apply(_parse_date)
        df = df[df['trdate'].notna()]

    if 'emp_code' in df.columns:
        df['emp_code'] = df['emp_code'].apply(_clean_emp)

    if 'inflows' in df.columns:
        df['inflows'] = pd.to_numeric(df['inflows'], errors='coerce').fillna(0)

    for col in ['bic_name','cluster_name','manager_name','region_name','sub_trtype','trtype']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({'None': '', 'nan': '', 'NAN': ''})

    if module == 'po3' and 'sub_trtype' in df.columns:
        df = df[df['sub_trtype'].str.upper().str.contains('POWER', na=False)]
    elif module == 'wsip' and 'sub_trtype' in df.columns:
        df = df[df['sub_trtype'].str.upper().str.contains('WEALTH', na=False)]

    if 'region_name' in df.columns:
        df = df[df['region_name'].isin(REAL_REGIONS) | (df['region_name'] == '')]

    return df.reset_index(drop=True)


# ── NEW: WA-specific bic_map / sip_arns_by_date helpers ──────────────────────

def _extract_bic_map_from_df(df: pd.DataFrame, arn_col: str) -> Dict:
    """
    Build ARN → bic_info dict from a raw (un-renamed) dataframe.
    Used to construct the 3-tier bic_map for the WA module.

    Args:
        df:      Raw dataframe (columns not yet renamed).
        arn_col: Name of the column holding the ARN (TRS_AGENT / AGENT / BROKER).

    Returns:
        {arn: {bic_name, emp_code, cluster, manager, region}}
    """
    result: Dict = {}
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    cols = set(df.columns)

    if arn_col not in cols:
        logger.warning(f"[Parser] _extract_bic_map_from_df: column '{arn_col}' not found")
        return result

    field_map = {
        'BIC_OWNER':      'bic_name',
        'EMPLOYEE CODE':  'emp_code',
        'CLUSTER':        'cluster',
        'CLUSTER MANAGER':'manager',
        'REGION':         'region',
    }

    for _, row in df.iterrows():
        arn = str(row.get(arn_col, '')).strip()
        if not arn or arn.lower() in ('nan', 'none', ''):
            continue
        entry: Dict = {}
        for src, dst in field_map.items():
            if src in cols:
                v = str(row.get(src, '')).strip()
                if v and v.lower() not in ('nan', 'none'):
                    entry[dst] = _clean_emp(v) if dst == 'emp_code' else v
        if entry:
            result[arn] = entry

    return result


def _extract_sip_arns_by_date(df: pd.DataFrame) -> Dict[str, Set[str]]:
    """
    Extract {date_str: set(arns)} from the WA SIP-DATA sheet,
    keyed by TRI_REGISTRATION_DATE with ARNs from the AGENT column.
    """
    sip_arns: Dict[str, Set[str]] = {}
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    cols = set(df.columns)

    date_col = 'TRI_REGISTRATION_DATE' if 'TRI_REGISTRATION_DATE' in cols else None
    arn_col  = 'AGENT' if 'AGENT' in cols else None

    if not date_col or not arn_col:
        logger.warning("[Parser] SIP-DATA missing TRI_REGISTRATION_DATE or AGENT column — "
                       "no SIP ARNs extracted")
        return sip_arns

    for _, row in df.iterrows():
        arn = str(row.get(arn_col, '')).strip()
        if not arn or arn.lower() in ('nan', 'none', ''):
            continue
        date = _parse_date(row.get(date_col))
        if not date:
            continue
        sip_arns.setdefault(date, set()).add(arn)

    logger.info(f"[Parser] SIP-DATA: {sum(len(v) for v in sip_arns.values())} ARNs "
                f"across {len(sip_arns)} dates")
    return sip_arns


def _extract_data_activation_arns_by_date(df: pd.DataFrame) -> Dict[str, Set[str]]:
    """
    Collect activation ARNs from the raw (unfiltered) DATA sheet grouped by date.

    Must be called BEFORE _process_data_df so that rows with empty BIC_OWNER
    are not lost — per the WA pseudocode, activation ARN collection happens on
    ALL rows regardless of BIC_OWNER. Those ARNs are later attributed to BICs
    via bic_map lookup in the processor.

    Activation types: Purchase + Switch-In (SIP is excluded, same as pseudocode).
    """
    result: Dict[str, Set[str]] = {}
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    cols = set(df.columns)

    required = {'TRTYPE', 'TRDATE', 'TRS_AGENT'}
    if not required.issubset(cols):
        missing = required - cols
        logger.warning(f"[Parser] _extract_data_activation_arns_by_date: "
                       f"missing columns {missing} — activation ARNs not extracted from DATA")
        return result

    # Match the same set used in processor ACTIVATION_TRTYPES
    _ACTIVATION_RAW = {'PURCHASE', 'SWITCH-IN', 'SWITCHIN', 'SWITCH IN'}

    for _, row in df.iterrows():
        trtype = str(row.get('TRTYPE', '') or '').strip().upper()
        if not any(a in trtype for a in _ACTIVATION_RAW):
            continue
        arn = str(row.get('TRS_AGENT', '') or '').strip()
        if not arn or arn.lower() in ('nan', 'none', ''):
            continue
        date = _parse_date(row.get('TRDATE'))
        if not date:
            continue
        result.setdefault(date, set()).add(arn)

    total_arns = sum(len(v) for v in result.values())
    logger.info(f"[Parser] DATA activation ARNs (raw): {total_arns} across {len(result)} dates")
    return result


# ── MAIN PARSE FUNCTIONS ──────────────────────────────────────────────────────

def parse_data_sheet(filepath: Path, module: str) -> Dict:
    """
    Parse raw DATA / SIP DATA / SIP-DATA sheets.
    Returns transaction-level records tagged with trdate.
    These are aggregated by date+BIC in the processor.

    For the WA module, also returns:
      bic_map          — 3-tier ARN→BIC lookup (DATA < SIP-DATA < ARN-DATA)
      sip_arns_by_date — {date: set(arns)} from SIP-DATA's TRI_REGISTRATION_DATE
    Both are empty dicts for all other modules.
    """
    errors: List[str] = []
    records: List[Dict] = []
    bic_map: Dict = {}
    sip_arns_by_date: Dict[str, Set[str]] = {}
    data_activation_arns_by_date: Dict[str, Set[str]] = {}  # WA only

    try:
        wb = pd.ExcelFile(filepath, engine="openpyxl")
        available = set(wb.sheet_names)

        # ── SIP module ────────────────────────────────────────────────────────
        if module == 'sip':
            for sheet_name, col_map in [
                ('SIP DATA', SIP_DATA_COLUMNS),
                ('SIP-DATA', SIPDATA_COLUMNS),
            ]:
                if sheet_name not in available:
                    continue
                df = pd.read_excel(filepath, sheet_name=sheet_name, engine="openpyxl")
                df = _process_data_df(df, col_map, module)
                records.extend(df.to_dict(orient='records'))
                logger.info(f"[Parser] '{sheet_name}' sip → {len(df)} rows")

        # ── All other modules (wa, savings, po3, wsip) ────────────────────────
        else:
            if 'DATA' not in available:
                logger.info(f"[Parser] No DATA sheet in {filepath.name}")
                return {
                    "module": module, "level": "data_raw",
                    "records": [], "row_count": 0, "errors": [],
                    "bic_map": {}, "sip_arns_by_date": {},
                }

            # Read the raw DATA sheet once (used both for records and bic_map tier 3)
            data_df_raw = pd.read_excel(filepath, sheet_name='DATA', engine="openpyxl")

            # ── WA: build 3-tier bic_map + extract sip_arns_by_date ──────────
            if module == 'wa':
                # Tier 3 — lowest priority: DATA sheet, TRS_AGENT column
                bic_map.update(_extract_bic_map_from_df(data_df_raw, 'TRS_AGENT'))
                logger.info(f"[Parser] bic_map tier3 (DATA/TRS_AGENT): {len(bic_map)} ARNs")

                # Extract activation ARNs from ALL DATA rows BEFORE bic_name filtering.
                # Rows with empty BIC_OWNER but valid TRS_AGENT + activation TRTYPE
                # must still contribute their ARN to the date pool (pseudocode Step 3).
                data_activation_arns_by_date = _extract_data_activation_arns_by_date(data_df_raw)

                # Tier 2: SIP-DATA sheet, AGENT column
                #         Also extract sip_arns_by_date keyed by TRI_REGISTRATION_DATE
                if 'SIP-DATA' in available:
                    try:
                        sip_df_raw = pd.read_excel(
                            filepath, sheet_name='SIP-DATA', engine="openpyxl")
                        tier2 = _extract_bic_map_from_df(sip_df_raw, 'AGENT')
                        bic_map.update(tier2)
                        sip_arns_by_date = _extract_sip_arns_by_date(sip_df_raw)
                        logger.info(f"[Parser] bic_map tier2 (SIP-DATA/AGENT): "
                                    f"+{len(tier2)} ARNs, total {len(bic_map)}")
                    except Exception as e:
                        errors.append(f"SIP-DATA sheet error: {e}")
                        logger.warning(f"[Parser] SIP-DATA read error: {e}")
                else:
                    logger.warning(f"[Parser] No SIP-DATA sheet in {filepath.name} — "
                                   "SIP ARNs will not be included in WA activation")

                # Tier 1 — highest priority: ARN-DATA sheet, BROKER column (overwrites all)
                if 'ARN-DATA' in available:
                    try:
                        arn_df_raw = pd.read_excel(
                            filepath, sheet_name='ARN-DATA', engine="openpyxl")
                        tier1 = _extract_bic_map_from_df(arn_df_raw, 'BROKER')
                        bic_map.update(tier1)
                        logger.info(f"[Parser] bic_map tier1 (ARN-DATA/BROKER): "
                                    f"+{len(tier1)} ARNs, total {len(bic_map)}")
                    except Exception as e:
                        errors.append(f"ARN-DATA sheet error: {e}")
                        logger.warning(f"[Parser] ARN-DATA read error: {e}")
                else:
                    logger.warning(f"[Parser] No ARN-DATA sheet in {filepath.name}")

            # Process DATA rows (all modules)
            df = _process_data_df(data_df_raw, DATA_SHEET_COLUMNS, module)
            records.extend(df.to_dict(orient='records'))
            logger.info(f"[Parser] 'DATA' {module} → {len(df)} rows")

    except Exception as e:
        errors.append(f"DATA sheet error: {e}")
        logger.exception(f"[Parser] DATA sheet error for {module}: {e}")

    return {
        "module":                       module,
        "level":                        "data_raw",
        "records":                      records,
        "row_count":                    len(records),
        "errors":                       errors,
        "bic_map":                      bic_map,                      # populated for WA only
        "sip_arns_by_date":             sip_arns_by_date,             # populated for WA only
        "data_activation_arns_by_date": data_activation_arns_by_date, # populated for WA only
    }


def parse_file(filepath: Path, module: str, date: str = None) -> Dict:
    """Parse BIC-DATA sheet — aggregated totals tagged to upload date."""
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
        logger.info(f"[Parser] {filepath.name} BIC-DATA → {len(records)} rows")
    except ValueError as e:
        errors.append(str(e))
        logger.error(f"[Parser] {e}")
    except Exception as e:
        errors.append(f"Unexpected error: {e}")
        logger.exception(f"[Parser] {e}")

    return {
        "module": module, "date": date, "level": level,
        "records": records, "row_count": len(records), "errors": errors,
        # bic_map / sip_arns_by_date / data_activation_arns_by_date not applicable
        # for BIC-DATA aggregated uploads
        "bic_map": {}, "sip_arns_by_date": {}, "data_activation_arns_by_date": {},
    }
