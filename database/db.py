# database/db.py — BFAM Sales Intelligence Platform
# All SQLAlchemy models + connection setup
# SQLite (dev) → MySQL (production): change DB_HOST in .env

import os
from datetime import datetime
from sqlalchemy import (create_engine, Column, Integer, String, Float,
                        DateTime, Boolean, Text, JSON, ForeignKey)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from dotenv import load_dotenv

load_dotenv()

# ── Connection ────────────────────────────────────────
_host = os.getenv("DB_HOST")
_host = None  # Force SQLite for testing
if _host:
    _u = os.getenv("DB_USER")
    _p = os.getenv("DB_PASSWORD")
    _port = os.getenv("DB_PORT", "3306")
    _name = os.getenv("DB_NAME", "bfam_sales")
    DATABASE_URL = f"mysql+pymysql://{_u}:{_p}@{_host}:{_port}/{_name}"
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600, echo=False)
else:
    DATABASE_URL = "sqlite:///./bfam_dev.db"
    print("[DB] No MySQL config — using SQLite")
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False}, echo=False)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ══ MODELS ═══════════════════════════════════════════

class User(Base):
    """
    All platform users. Role determines what they see.
    Roles: COE (admin), RBH, CBH, BIC
    """
    __tablename__ = "users"

    id               = Column(Integer, primary_key=True, index=True)
    emp_code         = Column(String(20), unique=True, index=True, nullable=False)
    name             = Column(String(100), nullable=False)
    role             = Column(String(10), nullable=False, default="BIC")
    region           = Column(String(150), nullable=True)
    cluster          = Column(String(100), nullable=True)
    # CBH-specific: does this CBH also have personal BIC transaction data?
    has_bic_data     = Column(Boolean, default=False)
    # CBH-specific: their actual BIC emp code if different from login code
    bic_emp_code     = Column(String(20), nullable=True)
    hashed_password  = Column(String(200), nullable=False)
    is_active        = Column(Boolean, default=True)
    must_change_pw   = Column(Boolean, default=True)   # force pw change on first login
    created_at       = Column(DateTime, default=datetime.utcnow)
    last_login       = Column(DateTime, nullable=True)
    created_by       = Column(String(20), nullable=True)


class Season(Base):
    """
    Financial year seasons. COE can close a season and start fresh.
    All data is tagged to a season so history is preserved.
    """
    __tablename__ = "seasons"

    id           = Column(Integer, primary_key=True)
    name         = Column(String(20), nullable=False)   # e.g. "FY 2025-26"
    start_date   = Column(String(12), nullable=False)
    end_date     = Column(String(12), nullable=True)
    is_active    = Column(Boolean, default=True)
    closed_by    = Column(String(20), nullable=True)
    closed_at    = Column(DateTime, nullable=True)
    created_at   = Column(DateTime, default=datetime.utcnow)


class RegionData(Base):
    __tablename__ = "region_data"

    id          = Column(Integer, primary_key=True, index=True)
    season_id   = Column(Integer, nullable=True)
    date        = Column(String(12), index=True)
    module      = Column(String(20), index=True)
    region_name = Column(String(150))
    inflows     = Column(Float, default=0)
    txn_count   = Column(Integer, default=0)
    activation  = Column(Integer, default=0)
    avg_ticket  = Column(Float, default=0)
    net_sales   = Column(Float, default=0)
    sip_count   = Column(Integer, default=0)
    created_at  = Column(DateTime, default=datetime.utcnow)


class ClusterData(Base):
    __tablename__ = "cluster_data"

    id           = Column(Integer, primary_key=True, index=True)
    season_id    = Column(Integer, nullable=True)
    date         = Column(String(12), index=True)
    module       = Column(String(20), index=True)
    cluster_name = Column(String(100))
    manager_name = Column(String(100), nullable=True)
    region_name  = Column(String(150), nullable=True)
    inflows      = Column(Float, default=0)
    txn_count    = Column(Integer, default=0)
    activation   = Column(Integer, default=0)
    avg_ticket   = Column(Float, default=0)
    net_sales    = Column(Float, default=0)
    created_at   = Column(DateTime, default=datetime.utcnow)


class BICData(Base):
    __tablename__ = "bic_data"

    id           = Column(Integer, primary_key=True, index=True)
    season_id    = Column(Integer, nullable=True)
    date         = Column(String(12), index=True)
    module       = Column(String(20), index=True)
    emp_code     = Column(String(20), index=True)
    bic_name     = Column(String(100))
    cluster_name = Column(String(100), nullable=True)
    manager_name = Column(String(100), nullable=True)
    region_name  = Column(String(150), nullable=True)
    inflows      = Column(Float, default=0)
    txn_count    = Column(Integer, default=0)
    activation   = Column(Integer, default=0)
    avg_ticket   = Column(Float, default=0)
    net_sales    = Column(Float, default=0)
    sip_count    = Column(Integer, default=0)
    sip_live     = Column(Integer, default=0)
    sip_fresh    = Column(Integer, default=0)
    pad3         = Column(Integer, default=0)
    streak_days  = Column(Integer, default=0)
    points_ytd   = Column(Float, default=0)
    created_at   = Column(DateTime, default=datetime.utcnow)


class DailySnapshot(Base):
    __tablename__ = "daily_snapshot"

    id           = Column(Integer, primary_key=True, index=True)
    date         = Column(String(12), index=True)
    emp_code     = Column(String(20), index=True)
    bic_name     = Column(String(100))
    cluster_name = Column(String(100), nullable=True)
    sip_count    = Column(Integer, default=0)
    inflows      = Column(Float, default=0)
    activation   = Column(Integer, default=0)
    daily_points = Column(Float, default=0)
    created_at   = Column(DateTime, default=datetime.utcnow)


class TargetData(Base):
    __tablename__ = "target_data"

    id                = Column(Integer, primary_key=True, index=True)
    season_id         = Column(Integer, nullable=True)
    fiscal_year       = Column(String(10))
    module            = Column(String(20), index=True)
    region_name       = Column(String(150))
    activation_target = Column(Integer, default=0)
    inflow_target     = Column(Float, default=0)
    updated_by        = Column(String(20), nullable=True)
    updated_at        = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class GamificationConfig(Base):
    __tablename__ = "gamification_config"

    id                      = Column(Integer, primary_key=True)
    pts_per_txn             = Column(Float, default=3)
    pts_per_activation      = Column(Float, default=15)
    pts_per_50k_inflow      = Column(Float, default=1)
    streak_multiplier_days  = Column(Integer, default=7)
    streak_multiplier_value = Column(Float, default=1.5)
    module_bonus            = Column(JSON, default={"po3":5,"wsip":12,"savings":10,"wa":0,"sip":0})
    challenges              = Column(JSON, default=[])
    announcements           = Column(JSON, default=[])
    updated_by              = Column(String(50), nullable=True)
    updated_at              = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class UploadLog(Base):
    """
    Every file upload is recorded. COE can see full history of
    who uploaded what, when, and how many rows it produced.
    """
    __tablename__ = "upload_log"

    id           = Column(Integer, primary_key=True, index=True)
    uploaded_by  = Column(String(20), nullable=False)
    filename     = Column(String(200), nullable=False)
    module       = Column(String(20), nullable=True)
    date_tag     = Column(String(12), nullable=True)
    rows_parsed  = Column(Integer, default=0)
    rows_written = Column(Integer, default=0)
    status       = Column(String(20), default="success")   # success|partial|failed
    error_detail = Column(Text, nullable=True)
    duration_sec = Column(Float, nullable=True)
    uploaded_at  = Column(DateTime, default=datetime.utcnow)


class AuditLog(Base):
    """
    Every admin action is permanently recorded.
    Immutable — rows are never updated or deleted.
    """
    __tablename__ = "audit_log"

    id          = Column(Integer, primary_key=True, index=True)
    actor       = Column(String(20), nullable=False)     # emp_code of who did it
    action      = Column(String(50), nullable=False)     # e.g. "UPDATE_USER", "CHANGE_CONFIG"
    target_type = Column(String(30), nullable=True)      # e.g. "user", "config", "challenge"
    target_id   = Column(String(50), nullable=True)      # e.g. emp_code or config field
    old_value   = Column(Text, nullable=True)
    new_value   = Column(Text, nullable=True)
    notes       = Column(String(200), nullable=True)
    created_at  = Column(DateTime, default=datetime.utcnow)


def init_db():
    Base.metadata.create_all(bind=engine)
    print("[DB] All tables created/verified")
