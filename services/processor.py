# services/processor.py — Scoring engine + DB writes
# Handles both BIC-DATA aggregated records and raw DATA transaction records.

import logging
from datetime import datetime, timedelta
from typing import List, Dict
from collections import defaultdict
from sqlalchemy.orm import Session
from database.db import (BICData, ClusterData, RegionData,
                         DailySnapshot, GamificationConfig, User)

logger = logging.getLogger("bfam.processor")


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
    base  = txn * cfg["pts_per_txn"] + act * cfg["pts_per_activation"] + int(infl / 50000) * cfg["pts_per_50k_inflow"]
    bonus = cfg["module_bonus"].get(module, 0)
    total = base + txn * bonus
    if streak >= cfg["streak_multiplier_days"]:
        total *= cfg["streak_multiplier_value"]
    return round(total, 2)


def _rollup_clusters(records: List[Dict]) -> List[Dict]:
    cls: Dict = {}
    for r in records:
        key = (r.get("cluster_name", ""), r.get("region_name", ""))
        if key not in cls:
            cls[key] = {"cluster_name": r.get("cluster_name", ""),
                        "manager_name": r.get("manager_name", ""),
                        "region_name":  r.get("region_name", ""),
                        "inflows": 0, "net_sales": 0, "txn_count": 0,
                        "activation": 0, "sip_count": 0}
        for f in ["inflows", "net_sales", "txn_count", "activation", "sip_count"]:
            cls[key][f] += r.get(f, 0) or 0
    result = list(cls.values())
    for c in result:
        c["avg_ticket"] = round(c["inflows"] / c["txn_count"], 2) if c["txn_count"] else 0
    return result


def _rollup_regions(clusters: List[Dict]) -> List[Dict]:
    rgn: Dict = {}
    for c in clusters:
        key = c.get("region_name", "")
        if key not in rgn:
            rgn[key] = {"region_name": key, "inflows": 0, "net_sales": 0,
                        "txn_count": 0, "activation": 0, "sip_count": 0}
        for f in ["inflows", "net_sales", "txn_count", "activation", "sip_count"]:
            rgn[key][f] += c.get(f, 0) or 0
    result = list(rgn.values())
    for r in result:
        r["avg_ticket"] = round(r["inflows"] / r["txn_count"], 2) if r["txn_count"] else 0
    return result


def _name_to_empcode_map(db: Session) -> Dict[str, str]:
    """Build name→emp_code lookup from User table for BICs without emp codes in file."""
    rows = db.query(User.emp_code, User.name).filter(User.role == "BIC").all()
    return {name.strip(): ec for ec, name in rows if name}


def aggregate_raw_data(records: List[Dict], db: Session) -> List[Dict]:
    """
    Aggregate raw transaction records (from DATA sheets) by trdate + BIC.
    Returns one record per BIC per date with txn_count and inflows summed.
    Looks up emp_code by name for records missing it.
    """
    name_map = _name_to_empcode_map(db)

    # Group by (trdate, emp_code or bic_name)
    groups: Dict = defaultdict(lambda: {
        "txn_count": 0, "inflows": 0.0,
        "bic_name": "", "emp_code": "",
        "cluster_name": "", "manager_name": "", "region_name": ""
    })

    for r in records:
        ec        = str(r.get("emp_code", "")).strip()
        bic_name  = str(r.get("bic_name", "")).strip()
        trdate    = str(r.get("trdate", "")).strip()

        if not trdate:
            continue

        # Look up emp_code by name if missing
        if not ec and bic_name:
            ec = name_map.get(bic_name, "")

        # Use emp_code as key if available, else bic_name
        key = (trdate, ec if ec else bic_name)

        g = groups[key]
        g["txn_count"]   += 1
        g["inflows"]     += float(r.get("inflows", 0) or 0)
        g["bic_name"]    = g["bic_name"] or bic_name
        g["emp_code"]    = g["emp_code"] or ec
        # Use first non-empty cluster/region/manager found for this BIC
        for f in ["cluster_name", "manager_name", "region_name"]:
            if not g[f] and r.get(f):
                g[f] = str(r[f]).strip()

    # Build result records
    result = []
    for (trdate, _key), g in groups.items():
        if not g["emp_code"] and not g["bic_name"]:
            continue
        result.append({
            "trdate":       trdate,
            "date":         trdate,
            "emp_code":     g["emp_code"],
            "bic_name":     g["bic_name"],
            "cluster_name": g["cluster_name"],
            "manager_name": g["manager_name"],
            "region_name":  g["region_name"],
            "txn_count":    g["txn_count"],
            "inflows":      round(g["inflows"], 2),
            "activation":   0,
            "sip_count":    g["txn_count"],
            "avg_ticket":   round(g["inflows"] / g["txn_count"], 2) if g["txn_count"] else 0,
        })

    logger.info(f"[Processor] Aggregated {len(records)} raw rows → {len(result)} BIC-date records")
    return result


def process_and_write(parsed: Dict, db: Session, season_id: int = None) -> Dict:
    module  = parsed["module"]
    records = parsed["records"]
    level   = parsed["level"]
    errors  = list(parsed["errors"])

    if not records:
        return {"rows_written": 0, "errors": errors}

    # For raw DATA records: aggregate by date+BIC first
    if level == "data_raw":
        records = aggregate_raw_data(records, db)
        if not records:
            return {"rows_written": 0, "errors": errors + ["No valid records after aggregation"]}

    # Use the date from parsed dict for BIC-DATA records,
    # or from the record itself for DATA records
    default_date = parsed.get("date") or datetime.now().strftime("%Y-%m-%d")

    cfg = _get_config(db)
    rows_written = 0
    bic_rows: List = []

    # Get all dates present in records
    dates_present = set()
    for r in records:
        d = r.get("date") or r.get("trdate") or default_date
        dates_present.add(d)

    # Batch streaks for all emp_codes
    emp_codes = [str(r.get("emp_code", "")).strip() for r in records if r.get("emp_code")]
    if not emp_codes:
        emp_codes = ["__none__"]
    # Use the earliest date for streak calculation
    earliest = min(dates_present)
    streaks = _batch_streaks(emp_codes, db, earliest)

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
            net_sales    = r.get("net_sales", 0),
            txn_count    = int(r.get("txn_count", 0)),
            activation   = int(r.get("activation", 0)),
            avg_ticket   = r.get("avg_ticket", 0),
            sip_count    = int(r.get("sip_count", 0) or r.get("txn_count", 0)),
            streak_days  = streak,
            points_ytd   = points,
        ))

    try:
        # Delete existing records for each date+module combination
        for d in dates_present:
            db.query(BICData).filter(
                BICData.date == d, BICData.module == module
            ).delete()
        db.flush()
        db.bulk_save_objects(bic_rows)
        rows_written += len(bic_rows)

        # Rollup to cluster and region for each date
        for d in dates_present:
            date_records = [r for r in records
                           if (r.get("date") or r.get("trdate") or default_date) == d]
            if not date_records:
                continue

            cluster_agg = _rollup_clusters(date_records)
            db.query(ClusterData).filter(
                ClusterData.date == d, ClusterData.module == module
            ).delete()
            db.flush()
            for c in cluster_agg:
                db.add(ClusterData(
                    season_id=season_id, date=d, module=module,
                    cluster_name=c["cluster_name"], manager_name=c["manager_name"],
                    region_name=c["region_name"], inflows=c["inflows"],
                    txn_count=c["txn_count"], activation=c["activation"],
                    avg_ticket=c["avg_ticket"], net_sales=c.get("net_sales", 0)
                ))
            rows_written += len(cluster_agg)

            region_agg = _rollup_regions(cluster_agg)
            db.query(RegionData).filter(
                RegionData.date == d, RegionData.module == module
            ).delete()
            db.flush()
            for rg in region_agg:
                db.add(RegionData(
                    season_id=season_id, date=d, module=module,
                    region_name=rg["region_name"], inflows=rg["inflows"],
                    txn_count=rg["txn_count"], activation=rg["activation"],
                    avg_ticket=rg["avg_ticket"], net_sales=rg.get("net_sales", 0)
                ))
            rows_written += len(region_agg)

        db.commit()
        logger.info(f"[Processor] {module}: {rows_written} rows written across {len(dates_present)} dates")

    except Exception as e:
        db.rollback()
        errors.append(f"DB write failed: {e}")
        logger.error(f"[Processor] DB write failed: {e}")

    return {"rows_written": rows_written, "errors": errors}
