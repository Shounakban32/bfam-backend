# services/processor.py — Scoring engine + DB writes
# Handles both BIC-DATA aggregated records and raw DATA transaction records.
#
# CHANGES vs previous version:
#   - _aggregate_wa_data (NEW): correct WA activation — collects ARNs at DATE level,
#     merges with sip_arns_by_date, then attributes each ARN to the right BIC via bic_map.
#     Replaces the old wrong approach of merging all ARN-DATA ARNs into every BIC.
#   - _aggregate_standard_data (renamed from aggregate_raw_data): clean version for
#     savings/po3/wsip — BIC activation = unique ARNs from that BIC's own rows.
#     The incorrect arn_base_set merging is removed.
#   - _rollup_clusters / _rollup_regions: now module-aware.
#     WA    → cluster/region activation = sum of BIC activations (roll up).
#     Others → cluster activation = txn_count (CBH rule).
#              region  activation = txn_count (RBH rule).
#   - process_and_write: routes WA vs standard aggregation; passes module to rollup.

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Set
from collections import defaultdict
from sqlalchemy.orm import Session
from database.db import (BICData, ClusterData, RegionData,
                         DailySnapshot, GamificationConfig, User)

logger = logging.getLogger("bfam.processor")

OUTFLOW_TYPES = {
    "REDEMPTION", "SWITCH OUT", "SWITCH-OUT", "SWP",
    "SYSTEMATIC WITHDRAWAL", "SYSTEMATIC WITHDRAWAL PLAN"
}

INFLOW_TRTYPES     = {"PURCHASE", "SIP", "SWITCH-IN", "SWITCHIN", "SWITCH IN"}
ACTIVATION_TRTYPES = {"PURCHASE", "SWITCH-IN", "SWITCHIN", "SWITCH IN"}


def _is_outflow(sub_trtype: str) -> bool:
    return any(o in sub_trtype.upper() for o in OUTFLOW_TYPES)

def _is_inflow_trtype(trtype: str) -> bool:
    t = trtype.upper().strip()
    return any(it in t for it in INFLOW_TRTYPES)

def _is_activation_trtype(trtype: str) -> bool:
    t = trtype.upper().strip()
    return any(at in t for at in ACTIVATION_TRTYPES)


def _get_config(db: Session) -> Dict:
    cfg = db.query(GamificationConfig).first()
    if cfg:
        return {
            "pts_per_txn":             cfg.pts_per_txn or 3,
            "pts_per_activation":      cfg.pts_per_activation or 15,
            "pts_per_50k_inflow":      cfg.pts_per_50k_inflow or 1,
            "streak_multiplier_days":  cfg.streak_multiplier_days or 7,
            "streak_multiplier_value": cfg.streak_multiplier_value or 1.5,
            "module_bonus":            cfg.module_bonus or {},
        }
    return {
        "pts_per_txn": 3, "pts_per_activation": 15, "pts_per_50k_inflow": 1,
        "streak_multiplier_days": 7, "streak_multiplier_value": 1.5,
        "module_bonus": {"po3": 5, "wsip": 12, "savings": 10, "wa": 0, "sip": 0},
    }


def _batch_streaks(emp_codes: List[str], db: Session, today: str) -> Dict[str, int]:
    cutoff = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")
    rows = db.query(DailySnapshot.emp_code, DailySnapshot.date).filter(
        DailySnapshot.emp_code.in_(emp_codes),
        DailySnapshot.date >= cutoff,
        DailySnapshot.date < today,
        DailySnapshot.sip_count > 0
    ).all()
    active: Dict[str, set] = {}
    for ec, d in rows:
        active.setdefault(ec, set()).add(d)
    today_dt = datetime.strptime(today, "%Y-%m-%d").date()
    streaks: Dict[str, int] = {}
    for ec in emp_codes:
        streak, dates = 0, active.get(ec, set())
        for i in range(1, 61):
            if (today_dt - timedelta(days=i)).strftime("%Y-%m-%d") in dates:
                streak += 1
            else:
                break
        streaks[ec] = streak
    return streaks


def compute_points(r: Dict, module: str, streak: int, cfg: Dict) -> float:
    txn  = r.get("txn_count", 0) or 0
    act  = r.get("activation", 0) or 0
    infl = r.get("inflows", 0) or 0
    base  = (txn  * cfg["pts_per_txn"]
             + act * cfg["pts_per_activation"]
             + int(infl / 50000) * cfg["pts_per_50k_inflow"])
    bonus = cfg["module_bonus"].get(module, 0)
    total = base + txn * bonus
    if streak >= cfg["streak_multiplier_days"]:
        total *= cfg["streak_multiplier_value"]
    return round(total, 2)


def _name_to_empcode_map(db: Session) -> Dict[str, str]:
    """Build name→emp_code lookup from User table for BICs without emp codes in file."""
    rows = db.query(User.emp_code, User.name).filter(User.role == "BIC").all()
    return {name.strip(): ec for ec, name in rows if name}


def _new_group() -> Dict:
    return {
        "txn_count": 0, "inflows": 0.0, "gross_sales": 0.0, "outflows": 0.0,
        "bic_name": "", "emp_code": "",
        "cluster_name": "", "manager_name": "", "region_name": "",
        "activation_arns": set(),
    }


# ── AGGREGATION FUNCTIONS ─────────────────────────────────────────────────────

def _aggregate_wa_data(
    records: List[Dict],
    bic_map: Dict,
    data_activation_arns_by_date: Dict[str, Set[str]],
    sip_arns_by_date: Dict[str, Set[str]],
) -> List[Dict]:
    """
    WA-specific aggregation, implementing the pseudocode exactly.

    Inflows/txn:
      - Grouped by (trdate, bic_name).
      - Only TRTYPE ∈ {Purchase, SIP, Switch-In} counts.

    Activation:
      Step 1 — data_activation_arns_by_date (from parser): all activation ARNs
               (Purchase + Switch-In) from the UNFILTERED DATA sheet, per date.
               Using the raw sheet ensures rows with empty BIC_OWNER are not
               missed — per the pseudocode, ARN collection is independent of BIC_OWNER.
      Step 2 — Merge each date's pool with sip_arns_by_date[date] from SIP-DATA.
      Step 3 — For each ARN in the merged pool, look up bic_map → find which BIC
               owns that ARN, and credit it to that BIC for that date.
      Result — BIC activation = count of unique ARNs attributed to it per date.
    """
    # ── Pass: group inflows/txn by (date, bic_name) ──────────────────────────
    groups: Dict = defaultdict(lambda: {
        "txn_count": 0, "inflows": 0.0, "gross_sales": 0.0, "outflows": 0.0,
        "bic_name": "", "emp_code": "",
        "cluster_name": "", "manager_name": "", "region_name": "",
    })

    for r in records:
        bic_name  = str(r.get("bic_name", "")).strip()
        trdate    = str(r.get("trdate", "")).strip()
        trtype    = str(r.get("trtype", "") or "").strip()
        amount    = float(r.get("inflows", 0) or 0)
        sub_tr    = str(r.get("sub_trtype", "") or trtype or "").strip()

        if not trdate or not bic_name:
            continue

        if _is_inflow_trtype(trtype):
            key = (trdate, bic_name)
            g = groups[key]
            g["txn_count"] += 1
            g["inflows"]   += amount
            if _is_outflow(sub_tr):
                g["outflows"] += amount
            else:
                g["gross_sales"] += amount
            g["bic_name"] = g["bic_name"] or bic_name
            for f in ["emp_code", "cluster_name", "manager_name", "region_name"]:
                if not g[f] and r.get(f):
                    g[f] = str(r[f]).strip()

    # ── Step 2: merge raw DATA activation ARNs + SIP-DATA ARNs per date ──────
    # data_activation_arns_by_date comes from the unfiltered DATA sheet (parser).
    # sip_arns_by_date comes from SIP-DATA (parser).
    all_dates = set(data_activation_arns_by_date.keys()) | set(sip_arns_by_date.keys())
    final_arns_by_date: Dict[str, Set[str]] = {
        d: data_activation_arns_by_date.get(d, set()) | sip_arns_by_date.get(d, set())
        for d in all_dates
    }

    # ── Step 3: attribute each ARN to its BIC via bic_map ────────────────────
    # bic_activation[(date, bic_name)] = set of ARNs credited to this BIC on this date
    bic_activation: Dict[tuple, Set[str]] = defaultdict(set)
    for date, arns in final_arns_by_date.items():
        for arn in arns:
            info = bic_map.get(arn)
            if info:
                bic_name = info.get("bic_name", "")
                if bic_name:
                    bic_activation[(date, bic_name)].add(arn)

    # ── Build result records ──────────────────────────────────────────────────
    result = []
    for (trdate, bic_name), g in groups.items():
        if not bic_name:
            continue
        activation = len(bic_activation.get((trdate, bic_name), set()))
        gross = round(g["gross_sales"], 2)
        net   = round(gross - g["outflows"], 2)
        txn   = g["txn_count"]
        result.append({
            "trdate":       trdate,
            "date":         trdate,
            "emp_code":     g["emp_code"],
            "bic_name":     bic_name,
            "cluster_name": g["cluster_name"],
            "manager_name": g["manager_name"],
            "region_name":  g["region_name"],
            "txn_count":    txn,
            "inflows":      round(g["inflows"], 2),
            "gross_sales":  gross,
            "net_sales":    net,
            "activation":   activation,
            "sip_count":    txn,
            "avg_ticket":   round(g["inflows"] / txn, 2) if txn else 0,
        })

    logger.info(f"[Processor] WA: {len(records)} raw rows → {len(result)} BIC-date records "
                f"({len(final_arns_by_date)} active dates in activation pool)")
    return result


def _aggregate_standard_data(records: List[Dict], db: Session) -> List[Dict]:
    """
    Standard aggregation for savings, po3, wsip modules.

    BIC activation = count of unique TRS_AGENT ARNs under that BIC for that date.
    (Same as pseudocode: distinct ARN count per BIC.)

    No merging with external ARN lists — activation is derived purely from
    the BIC's own transaction rows.
    """
    name_map = _name_to_empcode_map(db)
    groups: Dict = defaultdict(_new_group)

    for r in records:
        ec        = str(r.get("emp_code", "")).strip()
        bic_name  = str(r.get("bic_name", "")).strip()
        trdate    = str(r.get("trdate", "")).strip()
        trtype    = str(r.get("trtype", "") or "").strip()

        if not trdate:
            continue

        if not ec and bic_name:
            ec = name_map.get(bic_name, "")

        key = (trdate, ec if ec else bic_name)
        g = groups[key]
        amount = float(r.get("inflows", 0) or 0)
        sub_tr = str(r.get("sub_trtype", "") or trtype or "").strip()

        if _is_inflow_trtype(trtype):
            g["txn_count"] += 1
            g["inflows"]   += amount
            if _is_outflow(sub_tr):
                g["outflows"] += amount
            else:
                g["gross_sales"] += amount

        # BIC activation: unique ARNs from this BIC's own rows
        if _is_activation_trtype(trtype):
            trs_agent = str(r.get("trs_agent", "") or "").strip()
            if trs_agent and trs_agent.lower() not in ("nan", "none", ""):
                g["activation_arns"].add(trs_agent)

        g["bic_name"] = g["bic_name"] or bic_name
        g["emp_code"] = g["emp_code"] or ec
        for f in ["cluster_name", "manager_name", "region_name"]:
            if not g[f] and r.get(f):
                g[f] = str(r[f]).strip()

    result = []
    for (trdate, _key), g in groups.items():
        if not g["emp_code"] and not g["bic_name"]:
            continue
        gross = round(g["gross_sales"], 2)
        net   = round(gross - g["outflows"], 2)
        txn   = g["txn_count"]
        result.append({
            "trdate":       trdate,
            "date":         trdate,
            "emp_code":     g["emp_code"],
            "bic_name":     g["bic_name"],
            "cluster_name": g["cluster_name"],
            "manager_name": g["manager_name"],
            "region_name":  g["region_name"],
            "txn_count":    txn,
            "inflows":      round(g["inflows"], 2),
            "gross_sales":  gross,
            "net_sales":    net,
            "activation":   len(g["activation_arns"]),
            "sip_count":    txn,
            "avg_ticket":   round(g["inflows"] / txn, 2) if txn else 0,
        })

    logger.info(f"[Processor] Standard: {len(records)} raw rows → {len(result)} BIC-date records")
    return result


# ── ROLLUP FUNCTIONS ──────────────────────────────────────────────────────────

def _rollup_clusters(records: List[Dict], module: str,
                     force_txn_activation: bool = False) -> List[Dict]:
    """
    Roll BIC-date records up to cluster level.

    Activation rules:
      force_txn_activation=True  (data_raw, non-WA):
        cluster activation = txn_count  (CBH rule from pseudocode).
      force_txn_activation=False (WA data_raw, or any BIC-DATA level):
        cluster activation = sum of BIC activations.
    """
    cls: Dict = {}
    for r in records:
        key = (r.get("cluster_name", ""), r.get("region_name", ""))
        if key not in cls:
            cls[key] = {
                "cluster_name": r.get("cluster_name", ""),
                "manager_name": r.get("manager_name", ""),
                "region_name":  r.get("region_name", ""),
                "inflows": 0.0, "gross_sales": 0.0, "net_sales": 0.0,
                "txn_count": 0, "activation": 0, "sip_count": 0,
            }
        c = cls[key]
        for f in ["inflows", "gross_sales", "net_sales", "txn_count", "sip_count"]:
            c[f] += r.get(f, 0) or 0
        # Always sum BIC activations; override below if CBH txn rule applies
        c["activation"] += r.get("activation", 0) or 0

    result = list(cls.values())
    for c in result:
        c["avg_ticket"] = round(c["inflows"] / c["txn_count"], 2) if c["txn_count"] else 0
        if force_txn_activation:
            c["activation"] = c["txn_count"]   # CBH rule (raw data, non-WA only)

    return result


def _rollup_regions(clusters: List[Dict], module: str,
                    force_txn_activation: bool = False) -> List[Dict]:
    """
    Roll cluster-level records up to region level.

    Activation rules:
      force_txn_activation=True  (data_raw, non-WA):
        region activation = txn_count  (RBH rule from pseudocode).
      force_txn_activation=False (WA data_raw, or any BIC-DATA level):
        region activation = sum of cluster activations.
    """
    rgn: Dict = {}
    for c in clusters:
        key = c.get("region_name", "")
        if key not in rgn:
            rgn[key] = {
                "region_name": key,
                "inflows": 0.0, "gross_sales": 0.0, "net_sales": 0.0,
                "txn_count": 0, "activation": 0, "sip_count": 0,
            }
        r = rgn[key]
        for f in ["inflows", "gross_sales", "net_sales", "txn_count", "sip_count"]:
            r[f] += c.get(f, 0) or 0
        r["activation"] += c.get("activation", 0) or 0

    result = list(rgn.values())
    for r in result:
        r["avg_ticket"] = round(r["inflows"] / r["txn_count"], 2) if r["txn_count"] else 0
        if force_txn_activation:
            r["activation"] = r["txn_count"]   # RBH rule (raw data, non-WA only)

    return result


# ── MAIN ENTRY POINT ──────────────────────────────────────────────────────────

def process_and_write(parsed: Dict, db: Session, season_id: int = None) -> Dict:
    module  = parsed["module"]
    records = parsed["records"]
    level   = parsed["level"]
    errors  = list(parsed["errors"])

    if not records:
        return {"rows_written": 0, "errors": errors}

    # ── Aggregate raw transaction records by date+BIC ─────────────────────────
    if level == "data_raw":
        if module == "wa":
            # WA: use bic_map + pre-extracted activation ARNs from raw sheet
            records = _aggregate_wa_data(
                records,
                bic_map                      = parsed.get("bic_map", {}),
                data_activation_arns_by_date = parsed.get("data_activation_arns_by_date", {}),
                sip_arns_by_date             = parsed.get("sip_arns_by_date", {}),
            )
        else:
            # savings / po3 / wsip: standard unique-ARN-per-BIC activation
            records = _aggregate_standard_data(records, db)

        if not records:
            return {
                "rows_written": 0,
                "errors": errors + ["No valid records after aggregation"],
            }

    # For data_raw non-WA: cluster/region activation = txn_count (CBH/RBH rule).
    # For WA data_raw or any BIC-DATA level: sum activations upward.
    force_txn_activation = (level == "data_raw" and module != "wa")

    default_date = parsed.get("date") or datetime.now().strftime("%Y-%m-%d")
    cfg          = _get_config(db)
    rows_written = 0
    bic_rows: List = []

    dates_present = set()
    for r in records:
        d = r.get("date") or r.get("trdate") or default_date
        dates_present.add(d)

    emp_codes = [str(r.get("emp_code", "")).strip() for r in records if r.get("emp_code")]
    if not emp_codes:
        emp_codes = ["__none__"]
    streaks = _batch_streaks(emp_codes, db, min(dates_present))

    for r in records:
        ec       = str(r.get("emp_code", "")).strip()
        date_str = r.get("date") or r.get("trdate") or default_date
        streak   = streaks.get(ec, 0)
        points   = compute_points(r, module, streak, cfg)

        bic_rows.append(BICData(
            season_id    = season_id,
            date         = date_str,
            module       = module,
            emp_code     = ec,
            bic_name     = r.get("bic_name", ""),
            cluster_name = r.get("cluster_name", ""),
            manager_name = r.get("manager_name", ""),
            region_name  = r.get("region_name", ""),
            inflows      = r.get("inflows", 0),
            gross_sales  = r.get("gross_sales", r.get("inflows", 0)),
            net_sales    = r.get("net_sales", 0),
            txn_count    = int(r.get("txn_count", 0)),
            activation   = int(r.get("activation", 0)),
            avg_ticket   = r.get("avg_ticket", 0),
            sip_count    = int(r.get("sip_count", 0) or r.get("txn_count", 0)),
            streak_days  = streak,
            points_ytd   = points,
        ))

    try:
        for d in dates_present:
            db.query(BICData).filter(
                BICData.date == d, BICData.module == module
            ).delete()
        db.flush()
        db.bulk_save_objects(bic_rows)
        rows_written += len(bic_rows)

        for d in dates_present:
            date_records = [
                r for r in records
                if (r.get("date") or r.get("trdate") or default_date) == d
            ]
            if not date_records:
                continue

            # Rollup with module-aware activation rules
            cluster_agg = _rollup_clusters(date_records, module,
                                           force_txn_activation=force_txn_activation)
            db.query(ClusterData).filter(
                ClusterData.date == d, ClusterData.module == module
            ).delete()
            db.flush()
            for c in cluster_agg:
                db.add(ClusterData(
                    season_id    = season_id,
                    date         = d,
                    module       = module,
                    cluster_name = c["cluster_name"],
                    manager_name = c["manager_name"],
                    region_name  = c["region_name"],
                    inflows      = c["inflows"],
                    gross_sales  = c.get("gross_sales", c["inflows"]),
                    net_sales    = c.get("net_sales", 0),
                    txn_count    = c["txn_count"],
                    activation   = c["activation"],
                    avg_ticket   = c["avg_ticket"],
                ))
            rows_written += len(cluster_agg)

            region_agg = _rollup_regions(cluster_agg, module,
                                         force_txn_activation=force_txn_activation)
            db.query(RegionData).filter(
                RegionData.date == d, RegionData.module == module
            ).delete()
            db.flush()
            for rg in region_agg:
                db.add(RegionData(
                    season_id    = season_id,
                    date         = d,
                    module       = module,
                    region_name  = rg["region_name"],
                    inflows      = rg["inflows"],
                    gross_sales  = rg.get("gross_sales", rg["inflows"]),
                    net_sales    = rg.get("net_sales", 0),
                    txn_count    = rg["txn_count"],
                    activation   = rg["activation"],
                    avg_ticket   = rg["avg_ticket"],
                ))
            rows_written += len(region_agg)

        db.commit()
        logger.info(f"[Processor] {module}: {rows_written} rows written "
                    f"across {len(dates_present)} dates")

    except Exception as e:
        db.rollback()
        errors.append(f"DB write failed: {e}")
        logger.error(f"[Processor] DB write failed: {e}")

    return {"rows_written": rows_written, "errors": errors, "dates_written": sorted(dates_present)}
