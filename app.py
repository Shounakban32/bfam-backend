# app.py — BFAM Sales Intelligence Platform
# FastAPI backend — manual upload, full RBAC, audit log, user management

import os, io, time, json, logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
import tempfile

from fastapi import (FastAPI, Depends, HTTPException, UploadFile, File, Form)
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from database.db import (get_db, init_db, SessionLocal,
    User, Season, BICData, ClusterData, RegionData,
    DailySnapshot, TargetData, GamificationConfig,
    UploadLog, AuditLog)
from services.parser import parse_file, parse_data_sheet, identify_module, get_all_modules
from services.processor import process_and_write

load_dotenv()
Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/app.log")]
)
logger = logging.getLogger("bfam.api")

# ── Config ────────────────────────────────────────────
JWT_SECRET    = os.getenv("JWT_SECRET_KEY", "CHANGE_IN_PRODUCTION_32CHARS_MIN")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE    = int(os.getenv("JWT_EXPIRE_MINUTES", 480))
CORS_ORIGINS  = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")]

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2  = OAuth2PasswordBearer(tokenUrl="/auth/login")

# ── Lifespan ──────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    _seed_config()
    _seed_season()
    logger.info("[App] BFAM API started")
    yield
    logger.info("[App] BFAM API shutdown")

app = FastAPI(title="BFAM Sales Intelligence API", version="2.0.0", lifespan=lifespan)

# ── CORS ──────────────────────────────────────────────
# Custom middleware that reflects the request origin back.
# This handles ALL cases: file://, http://localhost:PORT, any domain.
# For production, replace with strict origin whitelist.
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

class PermissiveCORSMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        origin = request.headers.get("origin") or "*"
        # Handle preflight
        if request.method == "OPTIONS":
            resp = StarletteResponse(status_code=200)
            resp.headers["Access-Control-Allow-Origin"]      = origin
            resp.headers["Access-Control-Allow-Credentials"] = "true"
            resp.headers["Access-Control-Allow-Methods"]     = "GET,POST,PUT,PATCH,DELETE,OPTIONS"
            resp.headers["Access-Control-Allow-Headers"]     = "Authorization,Content-Type,Accept"
            resp.headers["Access-Control-Max-Age"]           = "86400"
            return resp
        response = await call_next(request)
        response.headers["Access-Control-Allow-Origin"]      = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"]     = "GET,POST,PUT,PATCH,DELETE,OPTIONS"
        response.headers["Access-Control-Allow-Headers"]     = "Authorization,Content-Type,Accept"
        return response

app.add_middleware(PermissiveCORSMiddleware)



def _seed_config():
    db = SessionLocal()
    try:
        if not db.query(GamificationConfig).first():
            db.add(GamificationConfig(
                pts_per_txn=3, pts_per_activation=15, pts_per_50k_inflow=1,
                streak_multiplier_days=7, streak_multiplier_value=1.5,
                module_bonus={"po3":5,"wsip":12,"savings":10,"wa":0,"sip":0},
                challenges=[
                    {"id":"sip5","name":"Daily SIP Sprint","desc":"Register 5 SIPs today",
                     "target":5,"metric":"sip_count","bonus":10,"color":"#1565C0","active":True},
                    {"id":"act5","name":"Activation Ace","desc":"Activate 5 partners today",
                     "target":5,"metric":"activation","bonus":20,"color":"#6B3FA0","active":True},
                    {"id":"lakh","name":"Lakh Club","desc":"₹1L+ inflows today",
                     "target":100000,"metric":"inflows","bonus":15,"color":"#0B9F6C","active":True},
                    {"id":"p3","name":"Power of 3 Pro","desc":"3 P3 transactions today",
                     "target":3,"metric":"pad3","bonus":25,"color":"#E6A817","active":True},
                    {"id":"wsip1","name":"Wealth SIP Star","desc":"1 Wealth SIP today",
                     "target":1,"metric":"wsip","bonus":30,"color":"#D62B2B","active":True},
                ],
                announcements=[]
            ))
            db.commit()
    finally:
        db.close()


def _seed_season():
    db = SessionLocal()
    try:
        if not db.query(Season).first():
            db.add(Season(name="FY 2025-26", start_date="2025-04-01", is_active=True))
            db.commit()
    finally:
        db.close()


def _audit(db: Session, actor: str, action: str, target_type: str = None,
           target_id: str = None, old_val=None, new_val=None, notes: str = None):
    try:
        db.add(AuditLog(actor=actor, action=action, target_type=target_type,
                        target_id=target_id,
                        old_value=json.dumps(old_val, default=str) if old_val is not None else None,
                        new_value=json.dumps(new_val, default=str) if new_val is not None else None,
                        notes=notes))
        db.commit()
    except Exception as e:
        logger.error(f"[Audit] Failed to write: {e}")


# ══ AUTH ══════════════════════════════════════════════
def verify_pw(plain, hashed): return pwd_ctx.verify(plain, hashed)
def hash_pw(plain): return pwd_ctx.hash(plain)

def create_token(data: dict) -> str:
    p = data.copy()
    p["exp"] = datetime.utcnow() + timedelta(minutes=JWT_EXPIRE)
    return jwt.encode(p, JWT_SECRET, algorithm=JWT_ALGORITHM)

def get_current_user(token: str = Depends(oauth2), db: Session = Depends(get_db)) -> User:
    err = HTTPException(401, "Invalid or expired token", headers={"WWW-Authenticate":"Bearer"})
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        ec = payload.get("sub")
        if not ec: raise err
    except JWTError:
        raise err
    u = db.query(User).filter(User.emp_code==ec, User.is_active==True).first()
    if not u: raise err
    return u

def require_coe(u: User = Depends(get_current_user)) -> User:
    if u.role != "COE":
        raise HTTPException(403, "Access restricted to Partners COE team")
    return u

def active_season_id(db: Session) -> Optional[int]:
    s = db.query(Season).filter(Season.is_active==True).first()
    return s.id if s else None

def latest_date(db, module, model=None) -> str:
    M = model or RegionData
    r = db.query(func.max(M.date)).filter(M.module==module).scalar()
    return r or datetime.now().strftime("%Y-%m-%d")


# ══ SCHEMAS ═══════════════════════════════════════════
class TokenOut(BaseModel):
    access_token: str; token_type: str; role: str; name: str; emp_code: str
    must_change_pw: bool; has_bic_data: bool

class UserCreate(BaseModel):
    emp_code: str; name: str; role: str; password: str
    region: Optional[str]=None; cluster: Optional[str]=None
    has_bic_data: bool=False; bic_emp_code: Optional[str]=None

class UserUpdate(BaseModel):
    name: Optional[str]=None; role: Optional[str]=None
    region: Optional[str]=None; cluster: Optional[str]=None
    is_active: Optional[bool]=None; has_bic_data: Optional[bool]=None
    bic_emp_code: Optional[str]=None; must_change_pw: Optional[bool]=None

class PwChange(BaseModel):
    old_password: str; new_password: str

class ConfigUpdate(BaseModel):
    pts_per_txn: Optional[float]=None; pts_per_activation: Optional[float]=None
    pts_per_50k_inflow: Optional[float]=None; streak_multiplier_days: Optional[int]=None
    streak_multiplier_value: Optional[float]=None; module_bonus: Optional[dict]=None
    challenges: Optional[list]=None; announcements: Optional[list]=None

class TargetUpdate(BaseModel):
    fiscal_year: str; module: str; region_name: str
    activation_target: int=0; inflow_target: float=0

class SeasonCreate(BaseModel):
    name: str; start_date: str


# ══ AUTH ROUTES ═══════════════════════════════════════
@app.post("/auth/login", response_model=TokenOut, tags=["Auth"])
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    u = db.query(User).filter(User.emp_code==form.username, User.is_active==True).first()
    if not u or not verify_pw(form.password, u.hashed_password):
        raise HTTPException(401, "Incorrect employee code or password")
    u.last_login = datetime.utcnow()
    db.commit()
    token = create_token({"sub": u.emp_code, "role": u.role})
    return {"access_token": token, "token_type": "bearer",
            "role": u.role, "name": u.name, "emp_code": u.emp_code,
            "must_change_pw": u.must_change_pw, "has_bic_data": u.has_bic_data or False}


@app.post("/auth/change-password", tags=["Auth"])
def change_password(body: PwChange, u: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not verify_pw(body.old_password, u.hashed_password):
        raise HTTPException(400, "Current password is incorrect")
    if len(body.new_password) < 8:
        raise HTTPException(400, "New password must be at least 8 characters")
    u.hashed_password = hash_pw(body.new_password)
    u.must_change_pw = False
    db.commit()
    return {"message": "Password changed successfully"}


# ══ DASHBOARD / KPI ════════════════════════════════════
@app.get("/kpi", tags=["Dashboard"])
def get_kpi(module: str="wa", date: Optional[str]=None,
            u: User=Depends(get_current_user), db: Session=Depends(get_db)):
    date = date or latest_date(db, module)
    rows = db.query(RegionData).filter(RegionData.module==module, RegionData.date==date).all()
    if not rows:
        return {"module":module,"date":date,"data":None,"message":"No data for this module/date"}
    ti = sum(r.inflows or 0 for r in rows)
    tt = sum(r.txn_count or 0 for r in rows)
    ta = sum(r.activation or 0 for r in rows)
    tgs = sum(r.gross_sales or 0 for r in rows)
    tns = sum(r.net_sales or 0 for r in rows)
    return {"module":module,"date":date,"total_inflows":ti,"total_txn":tt,
            "total_activation":ta,"avg_ticket":round(ti/tt,2) if tt else 0,
            "total_gross_sales":tgs,"total_net_sales":tns,
            "top_region":max(rows,key=lambda r:r.inflows or 0).region_name,
            "regions_active":len(rows)}


# ══ OVERALL / CROSS-MODULE KPI ════════════════════════
@app.get("/kpi/overall", tags=["Dashboard"])
def get_kpi_overall(
    date: Optional[str] = None,
    u: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    rq = db.query(RegionData)
    if date:
        rq = rq.filter(RegionData.date <= date)
    region_rows = rq.all()

    if not region_rows:
        return {"date": date or "YTD", "data": None,
                "message": "No data available yet"}

    ti  = sum(r.inflows     or 0 for r in region_rows)
    tt  = sum(r.txn_count   or 0 for r in region_rows)
    ta  = sum(r.activation  or 0 for r in region_rows)
    tgs = sum(r.gross_sales or 0 for r in region_rows)
    tns = sum(r.net_sales   or 0 for r in region_rows)
    active_regions = len(set(r.region_name for r in region_rows))

    bq = db.query(BICData)
    if date:
        bq = bq.filter(BICData.date <= date)
    bic_rows = bq.all()
    active_bics     = len(set(r.emp_code     for r in bic_rows))
    active_clusters = len(set(r.cluster_name for r in bic_rows))

    return {
        "date":               date or "YTD",
        "total_inflows":      ti,
        "total_txn":          tt,
        "total_activation":   ta,
        "avg_ticket":         round(ti / tt, 2) if tt else 0,
        "total_gross_sales":  tgs,
        "total_net_sales":    tns,
        "active_regions":     active_regions,
        "active_bics":        active_bics,
        "active_clusters":    active_clusters,
    }

# ══ RANKINGS ══════════════════════════════════════════
@app.get("/rankings/regions", tags=["Rankings"])
def region_rankings(module:str="wa", date:Optional[str]=None, sort_by:str="inflows",
                    u:User=Depends(get_current_user), db:Session=Depends(get_db)):
    date = date or latest_date(db, module)
    rows = db.query(RegionData).filter(RegionData.module==module, RegionData.date==date).all()
    data = [{"region_name":r.region_name,"inflows":r.inflows or 0,"txn_count":r.txn_count or 0,
             "activation":r.activation or 0,"avg_ticket":r.avg_ticket or 0,
             "gross_sales":r.gross_sales or 0,"net_sales":r.net_sales or 0} for r in rows]
    data.sort(key=lambda x: x.get(sort_by) or 0, reverse=True)
    for i,d in enumerate(data): d["rank"]=i+1
    return {"module":module,"date":date,"count":len(data),"rankings":data}


@app.get("/rankings/clusters", tags=["Rankings"])
def cluster_rankings(module:str="wa", date:Optional[str]=None, sort_by:str="inflows",
                     u:User=Depends(get_current_user), db:Session=Depends(get_db)):
    date = date or latest_date(db, module, ClusterData)
    rows = db.query(ClusterData).filter(ClusterData.module==module, ClusterData.date==date).all()
    data = [{"cluster_name":r.cluster_name,"manager_name":r.manager_name,"region_name":r.region_name,
             "inflows":r.inflows or 0,"txn_count":r.txn_count or 0,"activation":r.activation or 0,
             "avg_ticket":r.avg_ticket or 0,
             "gross_sales":r.gross_sales or 0,"net_sales":r.net_sales or 0} for r in rows]
    data.sort(key=lambda x: x.get(sort_by) or 0, reverse=True)
    for i,d in enumerate(data): d["rank"]=i+1
    return {"module":module,"date":date,"count":len(data),"rankings":data}


@app.get("/rankings/bics", tags=["Rankings"])
def bic_rankings(module:str="wa", date:Optional[str]=None, sort_by:str="inflows",
                 u:User=Depends(get_current_user), db:Session=Depends(get_db)):
    date = date or latest_date(db, module, BICData)
    rows = db.query(BICData).filter(BICData.module==module, BICData.date==date).all()
    data = [{"emp_code":r.emp_code,"bic_name":r.bic_name,"cluster_name":r.cluster_name,
             "manager_name":r.manager_name,"region_name":r.region_name,
             "inflows":r.inflows or 0,"txn_count":r.txn_count or 0,
             "activation":r.activation or 0,"avg_ticket":r.avg_ticket or 0,
             "gross_sales":r.gross_sales or 0,"net_sales":r.net_sales or 0,
             "sip_count":r.sip_count or 0,"streak_days":r.streak_days or 0,
             "points_ytd":r.points_ytd or 0,"badge":_badge(r.points_ytd)} for r in rows]
    data.sort(key=lambda x: x.get(sort_by) or 0, reverse=True)
    for i,d in enumerate(data): d["rank"]=i+1
    return {"module":module,"date":date,"count":len(data),"rankings":data}


# ══ DAILY CHALLENGE ════════════════════════════════════
@app.get("/daily/leaderboard", tags=["Daily Challenge"])
def daily_lb(date:Optional[str]=None, u:User=Depends(get_current_user), db:Session=Depends(get_db)):
    date = date or datetime.now().strftime("%Y-%m-%d")
    rows = db.query(DailySnapshot).filter(DailySnapshot.date==date)\
             .order_by(DailySnapshot.daily_points.desc()).limit(15).all()
    return {"date":date,"resets":"midnight IST",
            "top15":[{"rank":i+1,"emp_code":r.emp_code,"bic_name":r.bic_name,
                      "cluster_name":r.cluster_name,"sip_count":r.sip_count or 0,
                      "inflows":r.inflows or 0,"activation":r.activation or 0,
                      "daily_points":r.daily_points or 0} for i,r in enumerate(rows)]}


# ══ GAMIFICATION ══════════════════════════════════════
@app.get("/gamification/leaderboard", tags=["Gamification"])
def gami_lb(module:str="wa", date:Optional[str]=None,
            u:User=Depends(get_current_user), db:Session=Depends(get_db)):
    date = date or latest_date(db, module, BICData)
    rows = db.query(BICData).filter(BICData.module==module, BICData.date==date)\
             .order_by(BICData.points_ytd.desc()).all()
    return {"date":date,"module":module,
            "board":[{"rank":i+1,"emp_code":r.emp_code,"name":r.bic_name,
                      "cluster":r.cluster_name,"points":r.points_ytd or 0,
                      "streak":r.streak_days or 0,"badge":_badge(r.points_ytd)}
                     for i,r in enumerate(rows)]}


@app.get("/gamification/config", tags=["Gamification"])
def get_gami_config(u:User=Depends(get_current_user), db:Session=Depends(get_db)):
    cfg = db.query(GamificationConfig).first()
    if not cfg: raise HTTPException(404, "Config not found")
    return {"pts_per_txn":cfg.pts_per_txn,"pts_per_activation":cfg.pts_per_activation,
            "pts_per_50k_inflow":cfg.pts_per_50k_inflow,
            "streak_multiplier_days":cfg.streak_multiplier_days,
            "streak_multiplier_value":cfg.streak_multiplier_value,
            "module_bonus":cfg.module_bonus,"challenges":cfg.challenges,
            "announcements":cfg.announcements,"updated_at":cfg.updated_at}


# ══ TARGETS ═══════════════════════════════════════════
@app.get("/targets", tags=["Targets"])
def get_targets(module:str="wa", fiscal_year:str="2025-26",
                u:User=Depends(get_current_user), db:Session=Depends(get_db)):
    rows = db.query(TargetData).filter(TargetData.module==module,
                                       TargetData.fiscal_year==fiscal_year).all()
    return {"module":module,"fiscal_year":fiscal_year,
            "targets":[{"region":r.region_name,"activation_target":r.activation_target,
                        "inflow_target":r.inflow_target} for r in rows]}


@app.post("/targets", tags=["Targets"])
def upsert_target(body: TargetUpdate, admin:User=Depends(require_coe),
                  db:Session=Depends(get_db)):
    existing = db.query(TargetData).filter(
        TargetData.module==body.module, TargetData.fiscal_year==body.fiscal_year,
        TargetData.region_name==body.region_name).first()
    if existing:
        old = {"act":existing.activation_target,"infl":existing.inflow_target}
        existing.activation_target = body.activation_target
        existing.inflow_target = body.inflow_target
        existing.updated_by = admin.emp_code
    else:
        old = None
        db.add(TargetData(fiscal_year=body.fiscal_year, module=body.module,
                          region_name=body.region_name,
                          activation_target=body.activation_target,
                          inflow_target=body.inflow_target,
                          updated_by=admin.emp_code))
    db.commit()
    _audit(db, admin.emp_code, "UPDATE_TARGET", "target", body.region_name,
           old, {"act":body.activation_target,"infl":body.inflow_target})
    return {"message": "Target saved"}


# ══ DATA UPLOAD (COE only) ════════════════════════════
@app.post("/data/upload", tags=["Data Upload"])
async def upload_file(
    file: UploadFile = File(...),
    date_override: Optional[str] = Form(None),
    admin: User = Depends(require_coe),
    db: Session = Depends(get_db)
):
    start = time.time()
    filename = file.filename or "upload.xlsx"

    if not filename.lower().endswith(".xlsx"):
        raise HTTPException(400, f"Only .xlsx files are supported. Got: {filename}")

    module = identify_module(filename)
    if module == "unknown":
        raise HTTPException(400,
            f"Cannot identify module from filename '{filename}'. "
            f"Name must contain: WhatsApp, Savings, Power_of_3, Wealth_SIP or SIP_MIS.")

    date_tag = date_override or datetime.now().strftime("%Y-%m-%d")
    sid = active_season_id(db)
    tmp_path = None

    rows_parsed = rows_written = 0
    errors: List[str] = []
    upload_status = "success"

    try:
        contents = await file.read()
        if not contents:
            raise HTTPException(400, "Uploaded file is empty.")

        # Write to temp file
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(contents)
            tmp_path = Path(tmp.name)

        # Permanent archive copy
        raw_dest = Path("data/raw") / f"{date_tag}_{filename}"
        raw_dest.parent.mkdir(parents=True, exist_ok=True)
        raw_dest.write_bytes(contents)

        modules_in_file = get_all_modules(tmp_path)

        for mod in modules_in_file:
            if mod == "unknown":
                continue
            mod_parsed = parse_file(tmp_path, mod, date_tag)
            rows_parsed += mod_parsed["row_count"]
            errors.extend(mod_parsed["errors"])
            if mod_parsed["row_count"] > 0:
                result = process_and_write(mod_parsed, db, season_id=sid)
                rows_written += result["rows_written"]
                errors.extend(result["errors"])

        # ── Pass 2: DATA sheets (raw transactions by TRDATE) ─
        for mod in modules_in_file:
            if mod == "unknown":
                continue
            data_parsed = parse_data_sheet(tmp_path, mod)
            if data_parsed["row_count"] > 0:
                rows_parsed += data_parsed["row_count"]
                result = process_and_write(data_parsed, db, season_id=sid)
                rows_written += result["rows_written"]
                errors.extend(result["errors"])

        if rows_parsed == 0:
            upload_status = "failed"
            errors.append(
                f"Zero rows parsed from '{filename}'. "
                f"Check that the file has a recognised sheet."
            )
        elif errors and rows_written == 0:
            upload_status = "failed"
        elif errors:
            upload_status = "partial"

    except HTTPException:
        raise
    except Exception as e:
        upload_status = "failed"
        errors.append(f"Processing error: {str(e)}")
        logger.exception(f"[Upload] Failed for {filename}: {e}")
    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()

    duration = round(time.time() - start, 2)

    try:
        db.add(UploadLog(
            uploaded_by=admin.emp_code, filename=filename, module=module,
            date_tag=date_tag, rows_parsed=rows_parsed, rows_written=rows_written,
            status=upload_status, error_detail="\n".join(errors) if errors else None,
            duration_sec=duration
        ))
        db.commit()
        _audit(db, admin.emp_code, "UPLOAD_FILE", "data", filename,
               None, {"module": module, "rows": rows_written, "status": upload_status})
    except Exception as e:
        logger.error(f"[Upload] Failed to log: {e}")

    logger.info(f"[Upload] {filename} → {module} | {rows_parsed} parsed | {rows_written} written | {duration}s | {upload_status}")

    return {
        "filename":     filename,
        "module":       module,
        "date":         date_tag,
        "rows_parsed":  rows_parsed,
        "rows_written": rows_written,
        "status":       upload_status,
        "errors":       errors,
        "duration_sec": duration,
        "message": (
            f"✓ {rows_written} rows written to database"
            if upload_status == "success"
            else f"⚠ {rows_written} rows written · {len(errors)} issue(s)"
            if upload_status == "partial"
            else f"✗ Upload failed — {errors[0] if errors else 'unknown error'}"
        )
    }


@app.get("/data/upload-history", tags=["Data Upload"])
def upload_history(limit:int=20, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    logs = db.query(UploadLog).order_by(UploadLog.uploaded_at.desc()).limit(limit).all()
    return [{"id":l.id,"uploaded_by":l.uploaded_by,"filename":l.filename,"module":l.module,
             "date_tag":l.date_tag,"rows_parsed":l.rows_parsed,"rows_written":l.rows_written,
             "status":l.status,"error_detail":l.error_detail,"duration_sec":l.duration_sec,
             "uploaded_at":l.uploaded_at} for l in logs]


# ══ EXPORT ════════════════════════════════════════════
@app.get("/export/rankings", tags=["Export"])
def export_rankings(module:str="wa", view:str="bic", date:Optional[str]=None,
                    u:User=Depends(get_current_user), db:Session=Depends(get_db)):
    """Download any rankings view as Excel."""
    import xlsxwriter
    date = date or latest_date(db, module, BICData)

    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory":True})
    ws = wb.add_worksheet("Rankings")

    hdr_fmt = wb.add_format({"bold":True,"bg_color":"#1565C0","font_color":"#FFFFFF","border":1})
    row_fmt  = wb.add_format({"border":1})
    num_fmt  = wb.add_format({"border":1,"num_format":"#,##0.00"})

    if view == "bic":
        rows = db.query(BICData).filter(BICData.module==module, BICData.date==date)\
                 .order_by(BICData.points_ytd.desc()).all()
        headers = ["Rank","Emp Code","BIC Name","Cluster","Region","Inflows","Gross Sales","Net Sales","TXN","Activation","Avg Ticket","Points"]
        for c,h in enumerate(headers): ws.write(0,c,h,hdr_fmt)
        for i,r in enumerate(rows):
            ws.write(i+1,0,i+1,row_fmt); ws.write(i+1,1,r.emp_code,row_fmt)
            ws.write(i+1,2,r.bic_name,row_fmt); ws.write(i+1,3,r.cluster_name,row_fmt)
            ws.write(i+1,4,r.region_name,row_fmt); ws.write(i+1,5,r.inflows or 0,num_fmt)
            ws.write(i+1,6,r.gross_sales or 0,num_fmt); ws.write(i+1,7,r.net_sales or 0,num_fmt)
            ws.write(i+1,8,r.txn_count or 0,row_fmt); ws.write(i+1,9,r.activation or 0,row_fmt)
            ws.write(i+1,10,r.avg_ticket or 0,num_fmt); ws.write(i+1,11,r.points_ytd or 0,num_fmt)
    else:
        rows = db.query(RegionData).filter(RegionData.module==module, RegionData.date==date)\
                 .order_by(RegionData.inflows.desc()).all()
        headers = ["Rank","Region","Inflows","Gross Sales","Net Sales","TXN","Activation","Avg Ticket"]
        for c,h in enumerate(headers): ws.write(0,c,h,hdr_fmt)
        for i,r in enumerate(rows):
            ws.write(i+1,0,i+1,row_fmt); ws.write(i+1,1,r.region_name,row_fmt)
            ws.write(i+1,2,r.inflows or 0,num_fmt); ws.write(i+1,3,r.gross_sales or 0,num_fmt)
            ws.write(i+1,4,r.net_sales or 0,num_fmt); ws.write(i+1,5,r.txn_count or 0,row_fmt)
            ws.write(i+1,6,r.activation or 0,row_fmt); ws.write(i+1,7,r.avg_ticket or 0,num_fmt)

    ws.set_column(0,11,15)
    wb.close()
    output.seek(0)

    fname = f"BFAM_{module}_{view}_rankings_{date}.xlsx"
    return StreamingResponse(output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={fname}"})


# ══ HEALTH (public) ════════════════════════════════════
@app.get("/health", tags=["System"])
def health(db: Session = Depends(get_db)):
    last = db.query(UploadLog).order_by(UploadLog.uploaded_at.desc()).first()
    season = db.query(Season).filter(Season.is_active==True).first()
    return {"status":"online","timestamp":datetime.utcnow().isoformat(),
            "active_season":season.name if season else None,
            "last_upload":{
                "uploaded_by":last.uploaded_by if last else None,
                "filename":last.filename if last else None,
                "status":last.status if last else "never",
                "uploaded_at":last.uploaded_at.isoformat() if last else None,
                "rows_written":last.rows_written if last else 0,
            }}


# ══ ADMIN — CONFIG ════════════════════════════════════
@app.patch("/admin/config", tags=["Admin"])
def update_config(body: ConfigUpdate, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    cfg = db.query(GamificationConfig).first()
    if not cfg: raise HTTPException(404, "Config not found")
    updates = body.dict(exclude_none=True)
    old = {k: getattr(cfg,k) for k in updates}
    for k,v in updates.items(): setattr(cfg,k,v)
    cfg.updated_by = admin.emp_code
    cfg.updated_at = datetime.utcnow()
    db.commit()
    _audit(db, admin.emp_code, "UPDATE_CONFIG", "config", None, old, updates)
    return {"message":"Config updated","updated_fields":list(updates.keys())}


# ══ ADMIN — USERS ════════════════════════════════════
@app.get("/admin/users", tags=["Admin"])
def list_users(role:Optional[str]=None, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    q = db.query(User)
    if role: q = q.filter(User.role==role)
    users = q.order_by(User.role, User.name).all()
    return [_user_dict(u) for u in users]


@app.post("/admin/users", tags=["Admin"])
def create_user(body:UserCreate, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    if db.query(User).filter(User.emp_code==body.emp_code).first():
        raise HTTPException(400, f"Employee code {body.emp_code} already exists")
    u = User(emp_code=body.emp_code, name=body.name, role=body.role,
             region=body.region, cluster=body.cluster,
             has_bic_data=body.has_bic_data, bic_emp_code=body.bic_emp_code,
             hashed_password=hash_pw(body.password), is_active=True,
             must_change_pw=True, created_by=admin.emp_code)
    db.add(u); db.commit()
    _audit(db, admin.emp_code, "CREATE_USER", "user", body.emp_code,
           None, {"name":body.name,"role":body.role})
    return {"message": f"User {body.name} created", "emp_code": body.emp_code}


@app.post("/admin/users/bulk-import", tags=["Admin"])
async def bulk_import(file: UploadFile = File(...),
                      admin: User = Depends(require_coe), db: Session = Depends(get_db)):
    """
    Upload a CSV with columns: emp_code, name, role, region, cluster, password
    Creates all accounts at once. Skips existing emp codes.
    """
    contents = await file.read()
    import csv, io as _io
    reader = csv.DictReader(_io.StringIO(contents.decode("utf-8-sig")))
    created, skipped, errors = 0, 0, []
    for row in reader:
        try:
            ec = row.get("emp_code","").strip()
            if not ec: continue
            if db.query(User).filter(User.emp_code==ec).first():
                skipped += 1; continue
            db.add(User(
                emp_code=ec, name=row.get("name","").strip(),
                role=row.get("role","BIC").strip().upper(),
                region=row.get("region","").strip() or None,
                cluster=row.get("cluster","").strip() or None,
                hashed_password=hash_pw(row.get("password","Welcome@123").strip()),
                is_active=True, must_change_pw=True, created_by=admin.emp_code
            ))
            created += 1
        except Exception as e:
            errors.append(f"Row {ec}: {e}")
    db.commit()
    _audit(db, admin.emp_code, "BULK_IMPORT", "user", None,
           None, {"created":created,"skipped":skipped})
    return {"created":created,"skipped":skipped,"errors":errors,
            "message":f"{created} accounts created, {skipped} skipped"}


@app.patch("/admin/users/{emp_code}", tags=["Admin"])
def update_user(emp_code:str, body:UserUpdate, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    u = db.query(User).filter(User.emp_code==emp_code).first()
    if not u: raise HTTPException(404, "User not found")
    updates = body.dict(exclude_none=True)
    old = {k: getattr(u,k) for k in updates}
    for k,v in updates.items(): setattr(u,k,v)
    db.commit()
    _audit(db, admin.emp_code, "UPDATE_USER", "user", emp_code, old, updates)
    return {"message": f"User {emp_code} updated", "updated_fields": list(updates.keys())}


@app.delete("/admin/users/{emp_code}", tags=["Admin"])
def deactivate_user(emp_code:str, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    u = db.query(User).filter(User.emp_code==emp_code).first()
    if not u: raise HTTPException(404, "User not found")
    if u.role == "COE" and u.emp_code != admin.emp_code:
        active_coe = db.query(User).filter(User.role=="COE", User.is_active==True).count()
        if active_coe <= 1:
            raise HTTPException(400, "Cannot deactivate the last COE admin account")
    u.is_active = False
    db.commit()
    _audit(db, admin.emp_code, "DEACTIVATE_USER", "user", emp_code, {"is_active":True}, {"is_active":False})
    return {"message": f"User {emp_code} deactivated"}


@app.post("/admin/users/{emp_code}/reset-password", tags=["Admin"])
def reset_password(emp_code:str, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    u = db.query(User).filter(User.emp_code==emp_code).first()
    if not u: raise HTTPException(404, "User not found")
    temp_pw = "Welcome@123"
    u.hashed_password = hash_pw(temp_pw)
    u.must_change_pw = True
    db.commit()
    _audit(db, admin.emp_code, "RESET_PASSWORD", "user", emp_code)
    return {"message": f"Password reset for {emp_code}", "temp_password": temp_pw}



# ══ ADMIN — SEASONS ═══════════════════════════════════
@app.get("/admin/seasons", tags=["Admin"])
def list_seasons(admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    rows = db.query(Season).order_by(Season.id.desc()).all()
    return [{"id":s.id,"name":s.name,"start_date":s.start_date,"end_date":s.end_date,
             "is_active":s.is_active,"closed_by":s.closed_by,
             "closed_at":s.closed_at.isoformat() if s.closed_at else None,
             "created_at":s.created_at.isoformat() if s.created_at else None} for s in rows]


@app.post("/admin/seasons", tags=["Admin"])
def create_season(body:SeasonCreate, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    db.add(Season(name=body.name, start_date=body.start_date, is_active=False))
    db.commit()
    _audit(db, admin.emp_code, "CREATE_SEASON", "season", body.name)
    return {"message": f"Season '{body.name}' created"}


@app.post("/admin/seasons/{season_id}/activate", tags=["Admin"])
def activate_season(season_id:int, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    db.query(Season).update({"is_active": False}, synchronize_session=False)
    s = db.query(Season).filter(Season.id==season_id).first()
    if not s: raise HTTPException(404, "Season not found")
    s.is_active = True
    db.commit()
    _audit(db, admin.emp_code, "ACTIVATE_SEASON", "season", s.name)
    return {"message": f"Season '{s.name}' is now active"}


# ══ ADMIN — AUDIT LOG ════════════════════════════════
@app.get("/admin/audit-log", tags=["Admin"])
def audit_log(limit:int=50, admin:User=Depends(require_coe), db:Session=Depends(get_db)):
    logs = db.query(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit).all()
    return [{"id":l.id,"actor":l.actor,"action":l.action,"target_type":l.target_type,
             "target_id":l.target_id,"old_value":l.old_value,"new_value":l.new_value,
             "notes":l.notes,"created_at":l.created_at} for l in logs]


# ══ HELPERS ═══════════════════════════════════════════
BADGES = [(2500,"Legend","👑"),(1000,"Champion","🏆"),
          (500,"Warrior","⚔️"),(100,"Hustler","⚡"),(0,"Rookie","🌱")]

def _badge(pts) -> dict:
    pts = pts or 0
    for t,n,i in BADGES:
        if pts >= t: return {"name":n,"icon":i}
    return {"name":"Rookie","icon":"🌱"}

def _user_dict(u: User) -> dict:
    return {"emp_code":u.emp_code,"name":u.name,"role":u.role,
            "region":u.region,"cluster":u.cluster,"is_active":u.is_active,
            "has_bic_data":u.has_bic_data or False,"bic_emp_code":u.bic_emp_code,
            "must_change_pw":u.must_change_pw,"last_login":u.last_login,
            "created_at":u.created_at,"created_by":u.created_by}

# ══ TRENDS & ACTIVITY ════════════════════════════════
@app.get("/analytics/dates", tags=["Analytics"])
def get_available_dates(module: str = "wa",
                        u: User = Depends(get_current_user),
                        db: Session = Depends(get_db)):
    """Returns all dates that have data for a given module."""
    rows = db.query(RegionData.date).filter(
        RegionData.module == module
    ).distinct().order_by(RegionData.date.asc()).all()
    return {"module": module, "dates": [r[0] for r in rows]}


@app.get("/analytics/trends", tags=["Analytics"])
def get_trends(module: str = "wa",
               metric: str = "inflows",
               u: User = Depends(get_current_user),
               db: Session = Depends(get_db)):
    """
    Returns metric values grouped by date for trend charts.
    metric: inflows | txn_count | activation
    """
    rows = db.query(RegionData.date,
                    func.sum(RegionData.inflows).label("inflows"),
                    func.sum(RegionData.txn_count).label("txn_count"),
                    func.sum(RegionData.activation).label("activation"),
                    func.sum(RegionData.gross_sales).label("gross_sales"),
                    func.sum(RegionData.net_sales).label("net_sales")
                    ).filter(RegionData.module == module
                    ).group_by(RegionData.date
                    ).order_by(RegionData.date.asc()).all()
    return {
        "module": module,
        "metric": metric,
        "data": [{"date": r[0], "value": getattr(r, metric, 0) or 0} for r in rows]
    }


@app.get("/analytics/activity", tags=["Analytics"])
def get_daily_activity(module: str = "wa",
                       u: User = Depends(get_current_user),
                       db: Session = Depends(get_db)):
    """
    Returns per-date summary: total inflows, txn, activation,
    top BIC of the day, and upload info.
    """
    # Get all dates with data
    date_rows = db.query(
        RegionData.date,
        func.sum(RegionData.inflows).label("inflows"),
        func.sum(RegionData.txn_count).label("txn_count"),
        func.sum(RegionData.activation).label("activation")
    ).filter(RegionData.module == module
    ).group_by(RegionData.date
    ).order_by(RegionData.date.desc()).all()

    result = []
    for row in date_rows:
        # Top BIC for that date
        top_bic = db.query(BICData).filter(
            BICData.module == module,
            BICData.date == row[0]
        ).order_by(BICData.inflows.desc()).first()

        # Upload log for that date
        upload = db.query(UploadLog).filter(
            UploadLog.date_tag == row[0],
            UploadLog.module == module
        ).first()

        result.append({
            "date":         row[0],
            "inflows":      row[1] or 0,
            "txn_count":    row[2] or 0,
            "activation":   row[3] or 0,
            "top_bic":      top_bic.bic_name if top_bic else None,
            "top_bic_inflows": top_bic.inflows if top_bic else 0,
            "uploaded_by":  upload.uploaded_by if upload else None,
            "uploaded_at":  upload.uploaded_at.isoformat() if upload else None,
            "rows_written": upload.rows_written if upload else 0,
        })
    return {"module": module, "activity": result}

@app.get("/my/performance", tags=["Analytics"])
def get_my_performance(
    u: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Returns the logged-in BIC's own performance across all modules and all dates.
    Uses the JWT token to identify the BIC via bic_emp_code or emp_code.
    """
    # Resolve which emp_code to look up in BICData
    bic_code = u.bic_emp_code if u.bic_emp_code else u.emp_code

    rows = db.query(BICData).filter(
        BICData.emp_code == bic_code
    ).order_by(BICData.date.asc(), BICData.module.asc()).all()

    if not rows:
        return {
            "emp_code": bic_code,
            "name": u.name,
            "history": [],
            "totals": {}
        }

@app.get("/my/rankings", tags=["Analytics"])
def get_my_rankings(
    module: str = "wa",
    u: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    bic_code = u.bic_emp_code if u.bic_emp_code else u.emp_code

    # ── Region Rankings ──────────────────────────────────
    region_rows = db.query(
        RegionData.region_name,
        func.sum(RegionData.inflows).label("inflows"),
        func.sum(RegionData.txn_count).label("txn_count"),
        func.sum(RegionData.activation).label("activation")
    ).filter(RegionData.module == module
    ).group_by(RegionData.region_name
    ).order_by(func.sum(RegionData.inflows).desc()).all()

    all_regions = [
        {"rank": i+1, "name": r[0], "inflows": r[1] or 0,
         "txn": r[2] or 0, "activation": r[3] or 0}
        for i, r in enumerate(region_rows)
    ]
    top3_regions = all_regions[:3]
    own_region_entry = next((r for r in all_regions if r["name"] == u.region), None)
    own_region_in_top3 = own_region_entry and own_region_entry["rank"] <= 3

    # ── Cluster Rankings ─────────────────────────────────
    cluster_rows = db.query(
        ClusterData.cluster_name,
        func.sum(ClusterData.inflows).label("inflows"),
        func.sum(ClusterData.txn_count).label("txn_count"),
        func.sum(ClusterData.activation).label("activation")
    ).filter(ClusterData.module == module
    ).group_by(ClusterData.cluster_name
    ).order_by(func.sum(ClusterData.inflows).desc()).all()

    all_clusters = [
        {"rank": i+1, "name": r[0], "inflows": r[1] or 0,
         "txn": r[2] or 0, "activation": r[3] or 0}
        for i, r in enumerate(cluster_rows)
    ]
    top3_clusters = all_clusters[:3]
    own_cluster_entry = next((c for c in all_clusters if c["name"] == u.cluster), None)
    own_cluster_in_top3 = own_cluster_entry and own_cluster_entry["rank"] <= 3

    # ── BIC Rankings — own cluster only ──────────────────
    cluster_bic_rows = db.query(
        BICData.emp_code,
        BICData.bic_name,
        func.sum(BICData.inflows).label("inflows"),
        func.sum(BICData.txn_count).label("txn_count"),
        func.sum(BICData.activation).label("activation"),
        func.sum(BICData.points_ytd).label("points")
    ).filter(
        BICData.module == module,
        BICData.cluster_name == u.cluster
    ).group_by(BICData.emp_code, BICData.bic_name
    ).order_by(func.sum(BICData.inflows).desc()).all()

    cluster_bics = [
        {"cluster_rank": i+1, "emp_code": r[0], "name": r[1],
         "inflows": r[2] or 0, "txn": r[3] or 0,
         "activation": r[4] or 0, "points": r[5] or 0,
         "is_me": r[0] == bic_code}
        for i, r in enumerate(cluster_bic_rows)
    ]

    # ── Personal Ranks ────────────────────────────────────
    all_bic_rows = db.query(
        BICData.emp_code,
        BICData.cluster_name,
        BICData.region_name,
        func.sum(BICData.inflows).label("inflows")
    ).filter(BICData.module == module
    ).group_by(BICData.emp_code, BICData.cluster_name, BICData.region_name
    ).order_by(func.sum(BICData.inflows).desc()).all()

    overall_rank = next(
        (i+1 for i, r in enumerate(all_bic_rows) if r[0] == bic_code), None)

    region_bics = [r for r in all_bic_rows if r[2] == u.region]
    region_rank = next(
        (i+1 for i, r in enumerate(region_bics) if r[0] == bic_code), None)

    cluster_rank = next(
        (b["cluster_rank"] for b in cluster_bics if b["is_me"]), None)

    return {
        "module": module,
        "regions": {
            "top3": top3_regions,
            "own": own_region_entry,
            "own_in_top3": own_region_in_top3
        },
        "clusters": {
            "top3": top3_clusters,
            "own": own_cluster_entry,
            "own_in_top3": own_cluster_in_top3
        },
        "bic_cluster_rankings": cluster_bics,
        "personal_ranks": {
            "overall": overall_rank,
            "region": region_rank,
            "cluster": cluster_rank,
            "total_bics_org": len(all_bic_rows),
            "total_bics_region": len(region_bics),
            "total_bics_cluster": len(cluster_bics)
        }
    }

    # Group by date
    date_map = {}
    for r in rows:
        if r.date not in date_map:
            date_map[r.date] = {}
        date_map[r.date][r.module] = {
            "txn":         r.txn_count   or 0,
            "inflows":     r.inflows     or 0,
            "activation":  r.activation  or 0,
            "sip_count":   r.sip_count   or 0,
            "points_ytd":  r.points_ytd  or 0,
            "streak":      r.streak_days or 0,
            "gross_sales": r.gross_sales or 0,
            "net_sales":   r.net_sales   or 0,
        }

    # Build history list with cumulative points
    history = []
    cumulative_pts = 0
    for date in sorted(date_map.keys()):
        day_pts = sum(
            date_map[date][m].get("points_ytd", 0)
            for m in date_map[date]
        )
        cumulative_pts += day_pts
        history.append({
            "date":          date,
            "modules":       date_map[date],
            "day_points":    day_pts,
            "cumul_points":  cumulative_pts,
        })

    # Per-module totals across all dates
    totals = {}
    for mod in ["wa", "savings", "po3", "wsip", "sip"]:
        totals[mod] = {
            "txn":         sum(date_map[d].get(mod, {}).get("txn", 0)          for d in date_map),
            "inflows":     sum(date_map[d].get(mod, {}).get("inflows", 0)      for d in date_map),
            "points":      sum(date_map[d].get(mod, {}).get("points_ytd", 0)   for d in date_map),
            "gross_sales": sum(date_map[d].get(mod, {}).get("gross_sales", 0)  for d in date_map),
            "net_sales":   sum(date_map[d].get(mod, {}).get("net_sales", 0)    for d in date_map),
        }

    # Latest streak (from the most recent row)
    latest_row = rows[-1]

    return {
        "emp_code":   bic_code,
        "name":       u.name,
        "streak":     latest_row.streak_days or 0,
        "badge":      _badge(cumulative_pts),
        "history":    history,
        "totals":     totals,
        "total_pts":  cumulative_pts,
    }

@app.delete("/data/delete", tags=["Data"])
def delete_data_by_date(
    module: str,
    date: str,
    u: User = Depends(require_coe),
    db: Session = Depends(get_db)
):
    deleted_counts = {}

    r = db.query(RegionData).filter(RegionData.module == module, RegionData.date == date)
    deleted_counts["region_rows"] = r.count()
    r.delete(synchronize_session=False)

    b = db.query(BICData).filter(BICData.module == module, BICData.date == date)
    deleted_counts["bic_rows"] = b.count()
    b.delete(synchronize_session=False)

    c = db.query(ClusterData).filter(ClusterData.module == module, ClusterData.date == date)
    deleted_counts["cluster_rows"] = c.count()
    c.delete(synchronize_session=False)

    ds = db.query(DailySnapshot).filter(DailySnapshot.date == date)
    deleted_counts["daily_snapshots"] = ds.count()
    ds.delete(synchronize_session=False)

    ul = db.query(UploadLog).filter(UploadLog.module == module, UploadLog.date_tag == date)
    deleted_counts["upload_logs"] = ul.count()
    ul.delete(synchronize_session=False)

    db.commit()

    _audit(db, u.emp_code, "DELETE_DATA", "module_date", f"{module}/{date}",
           old_val=None, new_val=deleted_counts,
           notes=f"Deleted all data for module={module} date={date}")

    logger.info(f"[Delete] {u.emp_code} deleted {module}/{date}: {deleted_counts}")
    return {"message": f"Data deleted for {module} on {date}", "deleted": deleted_counts}


@app.get("/setup/create-first-admin", tags=["Setup"])
def create_first_admin(db: Session = Depends(get_db)):
    if db.query(User).filter(User.role == "COE").first():
        raise HTTPException(400, "Admin already exists")
    u = User(
        emp_code="ADMIN001",
        name="Admin",
        role="COE",
        hashed_password=hash_pw("Admin123"),
        is_active=True,
        must_change_pw=False,
        created_by="system"
    )
    db.add(u)
    db.commit()
    return {"message": "Admin created", "emp_code": "ADMIN001", "password": "Admin@1234"}
