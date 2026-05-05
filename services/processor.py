# services/processor.py — Scoring engine + DB writes

import logging
from datetime import datetime, timedelta
from typing import List, Dict
from sqlalchemy.orm import Session
from database.db import (BICData, ClusterData, RegionData,
                         DailySnapshot, GamificationConfig)

logger = logging.getLogger("bfam.processor")


def _get_config(db: Session) -> Dict:
    cfg = db.query(GamificationConfig).first()
    if cfg:
        return {
            "pts_per_txn"            : cfg.pts_per_txn or 3,
            "pts_per_activation"     : cfg.pts_per_activation or 15,
            "pts_per_50k_inflow"     : cfg.pts_per_50k_inflow or 1,
            "streak_multiplier_days" : cfg.streak_multiplier_days or 7,
            "streak_multiplier_value": cfg.streak_multiplier_value or 1.5,
            "module_bonus"           : cfg.module_bonus or {},
        }
    return {
        "pts_per_txn": 3, "pts_per_activation": 15,
        "pts_per_50k_inflow": 1, "streak_multiplier_days": 7,
        "streak_multiplier_value": 1.5,
        "module_bonus": {"po3":5,"wsip":12,"savings":10,"wa":0,"sip":0},
    }


def _batch_streaks(emp_codes: List[str], db: Session, today: str) -> Dict[str, int]:
    """Single query for all streaks — avoids N×60 queries."""
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
    txn = r.get("txn_count", 0) or 0
    act = r.get("activation", 0) or 0
    infl = r.get("inflows", 0) or 0
    base = txn * cfg["pts_per_txn"] + act * cfg["pts_per_activation"] + int(infl/50000) * cfg["pts_per_50k_inflow"]
    bonus = cfg["module_bonus"].get(module, 0)
    total = base + txn * bonus
    if streak >= cfg["streak_multiplier_days"]:
        total *= cfg["streak_multiplier_value"]
    return round(total, 2)


def _rollup_clusters(records: List[Dict]) -> List[Dict]:
    cls: Dict = {}
    for r in records:
        key = (r.get("cluster_name",""), r.get("region_name",""))
        if key not in cls:
            cls[key] = {"cluster_name": r.get("cluster_name",""),
                        "manager_name": r.get("manager_name",""),
                        "region_name":  r.get("region_name",""),
                        "inflows":0,"net_sales":0,"txn_count":0,"activation":0,"sip_count":0}
        for f in ["inflows","net_sales","txn_count","activation","sip_count"]:
            cls[key][f] += r.get(f, 0) or 0
    result = list(cls.values())
    for c in result:
        c["avg_ticket"] = round(c["inflows"]/c["txn_count"],2) if c["txn_count"] else 0
    return result


def _rollup_regions(clusters: List[Dict]) -> List[Dict]:
    rgn: Dict = {}
    for c in clusters:
        key = c.get("region_name","")
        if key not in rgn:
            rgn[key] = {"region_name":key,"inflows":0,"net_sales":0,
                        "txn_count":0,"activation":0,"sip_count":0}
        for f in ["inflows","net_sales","txn_count","activation","sip_count"]:
            rgn[key][f] += c.get(f,0) or 0
    result = list(rgn.values())
    for r in result:
        r["avg_ticket"] = round(r["inflows"]/r["txn_count"],2) if r["txn_count"] else 0
    return result


def process_and_write(parsed: Dict, db: Session, season_id: int = None) -> Dict:
    module   = parsed["module"]
    date_str = parsed["date"]
    records  = parsed["records"]
    level    = parsed["level"]
    errors   = list(parsed["errors"])

    if not records:
        return {"rows_written": 0, "errors": errors}

    cfg = _get_config(db)
    emp_codes = [str(r.get("emp_code","")).strip() for r in records if r.get("emp_code")]
    streaks   = _batch_streaks(emp_codes, db, date_str)
    rows_written = 0

    bic_rows: List = []
    daily_rows: List = []

    # Build name→emp_code lookup from User table for rows missing emp_code (e.g. Savings YTD)
    from database.db import User as _User
    name_to_ec = {}
    missing_ec_names = [str(r.get("bic_name","")).strip() for r in records
                        if not str(r.get("emp_code","")).strip()]
    if missing_ec_names:
        user_rows = db.query(_User.emp_code, _User.name).filter(
            _User.name.in_(missing_ec_names), _User.role == "BIC"
        ).all()
        name_to_ec = {name: ec for ec, name in user_rows}

    for r in records:
        ec = str(r.get("emp_code","")).strip()
        # Fallback: look up emp_code from User table by name (needed for Savings YTD)
        if not ec:
            bic_name = str(r.get("bic_name","")).strip()
            ec = name_to_ec.get(bic_name, "")
        if not ec:
            continue
        streak = streaks.get(ec, 0)
        points = compute_points(r, module, streak, cfg)

        bic_rows.append(BICData(
            season_id    = season_id,
            date         = date_str,
            module       = module,
            emp_code     = ec,
            bic_name     = r.get("bic_name",""),
            cluster_name = r.get("cluster_name",""),
            manager_name = r.get("manager_name",""),
            region_name  = r.get("region_name",""),
            inflows      = r.get("inflows",0),
            net_sales    = r.get("net_sales",0),
            txn_count    = int(r.get("txn_count",0)),
            activation   = int(r.get("activation",0)),
            avg_ticket   = r.get("avg_ticket",0),
            sip_count    = int(r.get("sip_count",0) or r.get("txn_count",0)),
            streak_days  = streak,
            points_ytd   = points,
        ))

        if "daily" in level:
            daily_rows.append(DailySnapshot(
                date=date_str, emp_code=ec,
                bic_name=r.get("bic_name",""),
                cluster_name=r.get("cluster_name",""),
                sip_count=int(r.get("sip_count",0) or r.get("txn_count",0)),
                inflows=r.get("inflows",0),
                activation=int(r.get("activation",0)),
                daily_points=points,
            ))

    try:
        db.query(BICData).filter(BICData.date==date_str, BICData.module==module).delete()
        db.flush()
        db.bulk_save_objects(bic_rows)
        rows_written += len(bic_rows)

        if daily_rows:
            db.query(DailySnapshot).filter(DailySnapshot.date==date_str).delete()
            db.flush()
            db.bulk_save_objects(daily_rows)
            rows_written += len(daily_rows)

        cluster_agg = _rollup_clusters(records)
        db.query(ClusterData).filter(ClusterData.date==date_str, ClusterData.module==module).delete()
        db.flush()
        for c in cluster_agg:
            db.add(ClusterData(season_id=season_id, date=date_str, module=module,
                               cluster_name=c["cluster_name"], manager_name=c["manager_name"],
                               region_name=c["region_name"], inflows=c["inflows"],
                               txn_count=c["txn_count"], activation=c["activation"],
                               avg_ticket=c["avg_ticket"], net_sales=c.get("net_sales",0)))
        rows_written += len(cluster_agg)

        region_agg = _rollup_regions(cluster_agg)
        db.query(RegionData).filter(RegionData.date==date_str, RegionData.module==module).delete()
        db.flush()
        for rg in region_agg:
            db.add(RegionData(season_id=season_id, date=date_str, module=module,
                              region_name=rg["region_name"], inflows=rg["inflows"],
                              txn_count=rg["txn_count"], activation=rg["activation"],
                              avg_ticket=rg["avg_ticket"], net_sales=rg.get("net_sales",0)))
        rows_written += len(region_agg)

        db.commit()
        logger.info(f"[Processor] {module}/{date_str}: {rows_written} rows written")

    except Exception as e:
        db.rollback()
        errors.append(f"DB write failed: {e}")
        logger.error(f"[Processor] DB write failed: {e}")

    return {"rows_written": rows_written, "errors": errors}
