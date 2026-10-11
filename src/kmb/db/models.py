"""SQLAlchemy 2 models for the shared Postgres database on node001 (portable types: tests run on SQLite).

Schema management: ``init_db`` runs ``create_all`` and records ``SCHEMA_VERSION`` in ``schema_meta``. That is
deliberately simpler than Alembic for v1; bump the version and add an explicit migration step if a column
changes on a live database.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SCHEMA_VERSION = 1

Money = Numeric(18, 6)
Count = Numeric(18, 2)
Price = Numeric(10, 6)


def utcnow() -> datetime:
    return datetime.now(UTC)


def TS() -> Any:
    return DateTime(timezone=True)


class Base(DeclarativeBase):
    type_annotation_map = {dict[str, Any]: JSON, list[Any]: JSON}


class SchemaMeta(Base):
    __tablename__ = "schema_meta"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(String(256))


# ---------------------------------------------------------------------------------------------- users


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True)
    password_hash: Mapped[str] = mapped_column(String(256))
    role: Mapped[str] = mapped_column(String(16), default="user")  # admin | user
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    live_enabled: Mapped[bool] = mapped_column(Boolean, default=False)  # admin must grant live trading
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    last_login_at: Mapped[datetime | None] = mapped_column(TS(), nullable=True)


class UserSession(Base):
    __tablename__ = "sessions"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)  # sha256 of the cookie token
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(TS())
    user_agent: Mapped[str | None] = mapped_column(String(256), nullable=True)


class ResearchToken(Base):
    __tablename__ = "research_tokens"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    label: Mapped[str] = mapped_column(String(64))
    expires_at: Mapped[datetime] = mapped_column(TS())
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)


class ResearchExperiment(Base):
    __tablename__ = "research_experiments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(128))
    hypothesis: Mapped[str] = mapped_column(Text)
    success_criteria: Mapped[str] = mapped_column(Text)
    plan: Mapped[dict[str, Any]] = mapped_column(JSON)
    notes: Mapped[list[Any]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)


class ResearchTrial(Base):
    __tablename__ = "research_trials"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_id: Mapped[int] = mapped_column(
        ForeignKey("research_experiments.id", ondelete="CASCADE"), index=True
    )
    candidate: Mapped[int] = mapped_column(Integer)
    split: Mapped[str] = mapped_column(String(16))
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), unique=True)
    __table_args__ = (UniqueConstraint("experiment_id", "candidate", "split"),)


class ResearchPaperProposal(Base):
    __tablename__ = "research_paper_proposals"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_id: Mapped[int] = mapped_column(
        ForeignKey("research_experiments.id", ondelete="CASCADE"), index=True
    )
    candidate: Mapped[int] = mapped_column(Integer)
    rationale: Mapped[str] = mapped_column(Text)
    spec: Mapped[dict[str, Any]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    job_id: Mapped[int | None] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    decided_at: Mapped[datetime | None] = mapped_column(TS(), nullable=True)


class KalshiKey(Base):
    __tablename__ = "kalshi_keys"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    label: Mapped[str] = mapped_column(String(64))
    environment: Mapped[str] = mapped_column(String(16), default="production")  # production | demo
    api_key_id: Mapped[str] = mapped_column(String(128))
    private_key_enc: Mapped[bytes] = mapped_column(LargeBinary)  # AES-GCM(nonce||ciphertext) of the PEM
    purpose: Mapped[str] = mapped_column(String(16), default="trading")  # trading | collector
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    last_used_at: Mapped[datetime | None] = mapped_column(TS(), nullable=True)


class KeyLease(Base):
    """At most one live/paper job may hold a key (Kalshi rate limits and positions are per account)."""

    __tablename__ = "key_leases"
    key_id: Mapped[int] = mapped_column(ForeignKey("kalshi_keys.id", ondelete="CASCADE"), primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"))
    worker_id: Mapped[str] = mapped_column(String(64))
    acquired_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(TS())


# ---------------------------------------------------------------------------------------------- bots & jobs


class Bot(Base):
    __tablename__ = "bots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(64))
    strategy: Mapped[str] = mapped_column(String(64))
    params: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    series: Mapped[list[Any]] = mapped_column(JSON, default=list)  # e.g. ["KXBTC15M"]
    key_id: Mapped[int | None] = mapped_column(
        ForeignKey("kalshi_keys.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(TS(), default=utcnow, onupdate=utcnow)
    __table_args__ = (UniqueConstraint("user_id", "name"),)


class Job(Base):
    """Work queue. Workers claim with ``SELECT ... FOR UPDATE SKIP LOCKED`` (Postgres)."""

    __tablename__ = "jobs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    bot_id: Mapped[int | None] = mapped_column(ForeignKey("bots.id", ondelete="SET NULL"), nullable=True)
    kind: Mapped[str] = mapped_column(String(16))  # sim | paper | live | calibration
    status: Mapped[str] = mapped_column(String(16), default="queued", index=True)
    # queued | running | succeeded | failed | canceled
    spec: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    worker_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(TS(), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(TS(), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(TS(), nullable=True)


class WorkerStatus(Base):
    __tablename__ = "workers"
    worker_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    host: Mapped[str] = mapped_column(String(64))
    capabilities: Mapped[list[Any]] = mapped_column(JSON, default=list)
    current_job_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    heartbeat_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)


class Run(Base):
    __tablename__ = "runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int | None] = mapped_column(
        ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    bot_id: Mapped[int | None] = mapped_column(ForeignKey("bots.id", ondelete="SET NULL"), nullable=True)
    mode: Mapped[str] = mapped_column(String(8))  # sim | paper | live
    strategy: Mapped[str] = mapped_column(String(64))
    params: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    sim_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    windows: Mapped[list[Any]] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String(16), default="running")
    started_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(TS(), nullable=True)
    summary: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class RunMetric(Base):
    __tablename__ = "run_metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    window_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    name: Mapped[str] = mapped_column(String(64))
    value: Mapped[Decimal] = mapped_column(Numeric(24, 8))


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"), nullable=True, index=True
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    mode: Mapped[str] = mapped_column(String(8))
    client_order_id: Mapped[str] = mapped_column(String(64), unique=True)
    order_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    ticker: Mapped[str] = mapped_column(String(64), index=True)
    side: Mapped[str] = mapped_column(String(4))
    price: Mapped[Decimal] = mapped_column(Price)
    initial_count: Mapped[Decimal] = mapped_column(Count)
    fill_count: Mapped[Decimal] = mapped_column(Count, default=Decimal(0))
    remaining_count: Mapped[Decimal] = mapped_column(Count, default=Decimal(0))
    time_in_force: Mapped[str] = mapped_column(String(24))
    post_only: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str] = mapped_column(String(16), default="intent")
    # intent (written before sending) | pending | resting | executed | canceled | rejected | unknown
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    tag: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(TS(), default=utcnow, onupdate=utcnow)
    sim_ts_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class OrderEvent(Base):
    __tablename__ = "order_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_pk: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), index=True)
    event: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    ts: Mapped[datetime] = mapped_column(TS(), default=utcnow)
    sim_ts_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class FillRow(Base):
    __tablename__ = "fills"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"), nullable=True, index=True
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    mode: Mapped[str] = mapped_column(String(8))
    trade_id: Mapped[str] = mapped_column(String(64))
    order_id: Mapped[str] = mapped_column(String(64))
    client_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ticker: Mapped[str] = mapped_column(String(64), index=True)
    side: Mapped[str] = mapped_column(String(4))
    price: Mapped[Decimal] = mapped_column(Price)
    count: Mapped[Decimal] = mapped_column(Count)
    fee: Mapped[Decimal] = mapped_column(Money)
    is_taker: Mapped[bool] = mapped_column(Boolean)
    post_position: Mapped[Decimal] = mapped_column(Count)
    ts_ms: Mapped[int] = mapped_column(BigInteger)
    __table_args__ = (UniqueConstraint("mode", "run_id", "trade_id", "order_id"),)


class PositionRow(Base):
    __tablename__ = "positions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"), nullable=True, index=True
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    mode: Mapped[str] = mapped_column(String(8))
    ticker: Mapped[str] = mapped_column(String(64))
    position: Mapped[Decimal] = mapped_column(Count)
    cost: Mapped[Decimal] = mapped_column(Money)
    realized_pnl: Mapped[Decimal] = mapped_column(Money)
    fees_paid: Mapped[Decimal] = mapped_column(Money)
    updated_at: Mapped[datetime] = mapped_column(TS(), default=utcnow, onupdate=utcnow)
    __table_args__ = (UniqueConstraint("run_id", "ticker"),)


class Settlement(Base):
    __tablename__ = "settlements"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"), nullable=True, index=True
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    mode: Mapped[str] = mapped_column(String(8))
    ticker: Mapped[str] = mapped_column(String(64))
    result: Mapped[str] = mapped_column(String(8))
    position: Mapped[Decimal] = mapped_column(Count)
    payout: Mapped[Decimal] = mapped_column(Money)
    pnl: Mapped[Decimal] = mapped_column(Money)
    settled_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)


class BotEvent(Base):
    """Errors, warnings and notable actions from bots/workers (shown in the UI)."""

    __tablename__ = "bot_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True
    )
    job_id: Mapped[int | None] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), nullable=True, index=True
    )
    run_id: Mapped[int | None] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), nullable=True)
    level: Mapped[str] = mapped_column(String(8))  # info | warn | error
    code: Mapped[str] = mapped_column(String(64))
    message: Mapped[str] = mapped_column(Text)
    detail: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    ts: Mapped[datetime] = mapped_column(TS(), default=utcnow, index=True)


class RiskLimit(Base):
    """Per-user live/paper limits (user_id NULL = global default), enforced before every order."""

    __tablename__ = "risk_limits"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=True, unique=True
    )
    max_order_count: Mapped[Decimal] = mapped_column(Count, default=Decimal(10))
    max_position_per_market: Mapped[Decimal] = mapped_column(Count, default=Decimal(20))
    max_open_orders: Mapped[int] = mapped_column(Integer, default=10)
    max_daily_loss: Mapped[Decimal] = mapped_column(Money, default=Decimal(25))
    max_notional_per_market: Mapped[Decimal] = mapped_column(Money, default=Decimal(20))
    max_orders_per_minute: Mapped[int] = mapped_column(Integer, default=30)
    updated_at: Mapped[datetime] = mapped_column(TS(), default=utcnow, onupdate=utcnow)


class KillSwitchLog(Base):
    __tablename__ = "kill_switch_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope: Mapped[str] = mapped_column(String(16))  # global | user
    user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    engaged: Mapped[bool] = mapped_column(Boolean)
    actor_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reason: Mapped[str] = mapped_column(Text, default="")
    ts: Mapped[datetime] = mapped_column(TS(), default=utcnow)


class AppSetting(Base):
    __tablename__ = "app_settings"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(TS(), default=utcnow, onupdate=utcnow)


# ------------------------------------------------------------------------------ market data catalog


class SeriesRow(Base):
    __tablename__ = "series"
    ticker: Mapped[str] = mapped_column(String(64), primary_key=True)
    title: Mapped[str | None] = mapped_column(String(256), nullable=True)
    raw: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(TS(), default=utcnow, onupdate=utcnow)


class MarketRow(Base):
    __tablename__ = "markets"
    ticker: Mapped[str] = mapped_column(String(64), primary_key=True)
    series_ticker: Mapped[str] = mapped_column(String(64), index=True)
    event_ticker: Mapped[str | None] = mapped_column(String(64), nullable=True)
    open_time: Mapped[datetime | None] = mapped_column(TS(), nullable=True, index=True)
    close_time: Mapped[datetime | None] = mapped_column(TS(), nullable=True, index=True)
    strike: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    price_level_structure: Mapped[str | None] = mapped_column(String(32), nullable=True)
    raw: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(TS(), default=utcnow, onupdate=utcnow)


class MarketResult(Base):
    __tablename__ = "market_results"
    ticker: Mapped[str] = mapped_column(String(64), primary_key=True)
    series_ticker: Mapped[str] = mapped_column(String(64), index=True)
    result: Mapped[str] = mapped_column(String(8))  # yes | no | void
    expiration_value: Mapped[str | None] = mapped_column(String(64), nullable=True)
    strike: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    close_time: Mapped[datetime | None] = mapped_column(TS(), nullable=True)
    settled_time: Mapped[datetime | None] = mapped_column(TS(), nullable=True)
    raw: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class CaptureWindow(Base):
    """One replayable 15-minute window built by the normalizer (mirrors the sidecar window.json)."""

    __tablename__ = "capture_windows"
    window_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    market_ticker: Mapped[str] = mapped_column(String(64), index=True)
    series_ticker: Mapped[str] = mapped_column(String(64), index=True)
    open_time: Mapped[datetime] = mapped_column(TS(), index=True)
    close_time: Mapped[datetime] = mapped_column(TS())
    result: Mapped[str | None] = mapped_column(String(8), nullable=True)
    strike: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    event_count: Mapped[int] = mapped_column(Integer, default=0)
    gap_count: Mapped[int] = mapped_column(Integer, default=0)
    quality: Mapped[str] = mapped_column(String(16), default="unknown")  # ok | degraded | bad | unknown
    path: Mapped[str] = mapped_column(String(512))
    sha256: Mapped[str] = mapped_column(String(64))
    size_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    built_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)


class CollectorStatus(Base):
    __tablename__ = "collector_status"
    collector_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    capture_id: Mapped[str] = mapped_column(String(64))
    status: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(TS(), default=utcnow, onupdate=utcnow)


class IngestStatus(Base):
    __tablename__ = "ingest_status"
    node: Mapped[str] = mapped_column(String(64), primary_key=True)
    status: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(TS(), default=utcnow, onupdate=utcnow)


class FidelityReport(Base):
    __tablename__ = "fidelity_reports"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    window_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    gate: Mapped[str] = mapped_column(String(8))  # G1..G5
    passed: Mapped[bool] = mapped_column(Boolean)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(TS(), default=utcnow)


Index("ix_jobs_queue", Job.status, Job.created_at)
