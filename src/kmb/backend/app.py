"""FastAPI backend: auth, users, keys, bots, jobs, runs, replay catalog + WS, operations, static UI."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Any, Literal

import httpx
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response, WebSocket
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from starlette.websockets import WebSocketDisconnect

from kmb.backend import serializers as S
from kmb.backend.security import (
    LoginLimiter,
    Vault,
    check_password_policy,
    csrf_for,
    csrf_ok,
    hash_password,
    key_aad,
    new_session_token,
    session_id_for,
    verify_password,
)
from kmb.backend.settings import BackendSettings
from kmb.db import init_db, make_engine, ops, session_factory
from kmb.db.models import (
    Bot,
    BotEvent,
    CollectorStatus,
    FidelityReport,
    FillRow,
    IngestStatus,
    Job,
    KalshiKey,
    KeyLease,
    Order,
    PositionRow,
    RiskLimit,
    Run,
    Settlement,
    User,
    UserSession,
    WorkerStatus,
    utcnow,
)
from kmb.replay.cursor import ReplayCursor
from kmb.replay.source import WindowNotFound, WindowSource, source_from_env
from kmb.replay.view import build_frame, build_overview, window_times
from kmb.strategy.registry import STRATEGIES, describe, validate_params

log = logging.getLogger("kmb.backend")

COOKIE = "kmb_session"
CSRF_HEADER = "X-KMB-CSRF"
SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
ACTIVE = ("queued", "running")
FRAME_INTERVAL_S = 0.1


def _aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


@dataclass
class AppCtx:
    settings: BackendSettings
    sessions: sessionmaker[Session]
    source: WindowSource
    vault: Vault | None
    limiter: LoginLimiter
    overview_cache: OrderedDict[str, dict[str, Any]] = field(default_factory=OrderedDict)


def ctx_of(request: Request) -> AppCtx:
    return request.app.state.kmb


def get_db(request: Request):
    db = ctx_of(request).sessions()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


DB = Annotated[Session, Depends(get_db)]


def _user_for_token(db: Session, token: str | None) -> User | None:
    if not token:
        return None
    sess = db.get(UserSession, session_id_for(token))
    if sess is None or _aware(sess.expires_at) <= utcnow():  # type: ignore[operator]
        return None
    user = db.get(User, sess.user_id)
    return user if user is not None and user.is_active else None


def current_user(request: Request, db: DB) -> User:
    token = request.cookies.get(COOKIE)
    user = _user_for_token(db, token)
    if user is None:
        raise HTTPException(401, "not authenticated")
    if request.method not in SAFE_METHODS and not csrf_ok(token or "", request.headers.get(CSRF_HEADER)):
        raise HTTPException(403, "missing or invalid CSRF token")
    return user


CurrentUser = Annotated[User, Depends(current_user)]


def admin_user(user: CurrentUser) -> User:
    if user.role != "admin":
        raise HTTPException(403, "admin only")
    return user


AdminUser = Annotated[User, Depends(admin_user)]


# ---------------------------------------------------------------------------------------------- bodies


class LoginBody(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class PasswordBody(BaseModel):
    current_password: str
    new_password: str = Field(max_length=256)


class NewUserBody(BaseModel):
    username: str = Field(min_length=2, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    password: str = Field(max_length=256)
    role: Literal["admin", "user"] = "user"


class PatchUserBody(BaseModel):
    role: Literal["admin", "user"] | None = None
    is_active: bool | None = None
    live_enabled: bool | None = None
    password: str | None = Field(default=None, max_length=256)


class NewKeyBody(BaseModel):
    label: str = Field(min_length=1, max_length=64)
    environment: Literal["production", "demo"] = "production"
    api_key_id: str = Field(min_length=8, max_length=128)
    private_key_pem: str = Field(min_length=100, max_length=10000)


class BotBody(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    strategy: str
    params: dict[str, Any] = Field(default_factory=dict)
    series: list[str] = Field(default_factory=lambda: ["KXBTC15M"])
    key_id: int | None = None


class PatchBotBody(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=64)
    strategy: str | None = None
    params: dict[str, Any] | None = None
    series: list[str] | None = None
    key_id: int | None = None


class SimOptions(BaseModel):
    latency_ms: int | None = Field(default=None, ge=0, le=10000)
    queue_model: Literal["front", "pro_rata", "back"] | None = None
    fee_multiplier: float | None = Field(default=None, ge=0, le=10)
    starting_balance: float | None = Field(default=None, gt=0, le=1_000_000)


class NewJobBody(BaseModel):
    kind: Literal["sim", "paper", "live"]
    bot_id: int
    windows: list[str] | None = Field(default=None, max_length=2000)
    sim: SimOptions | None = None


class LimitsBody(BaseModel):
    max_order_count: str | float | None = None
    max_position_per_market: str | float | None = None
    max_open_orders: int | None = Field(default=None, ge=0, le=1000)
    max_daily_loss: str | float | None = None
    max_notional_per_market: str | float | None = None
    max_orders_per_minute: int | None = Field(default=None, ge=0, le=6000)


class KillBody(BaseModel):
    scope: Literal["global", "user"]
    engaged: bool
    reason: str = Field(default="", max_length=500)


class LiveTradingBody(BaseModel):
    enabled: bool


ALLOWED_SERIES = {"KXBTC15M", "KXETH15M"}


# ---------------------------------------------------------------------------------------------- helpers


def _visible(user: User, owner_id: int) -> bool:
    return user.role == "admin" or owner_id == user.id


def _run_ids_for_jobs(db: Session, job_ids: list[int]) -> dict[int, int]:
    if not job_ids:
        return {}
    rows = db.execute(
        select(Run.job_id, func.max(Run.id)).where(Run.job_id.in_(job_ids)).group_by(Run.job_id)
    )
    return {jid: rid for jid, rid in rows if jid is not None}


def _job_out(db: Session, job: Job) -> dict[str, Any]:
    return S.job_json(job, _run_ids_for_jobs(db, [job.id]).get(job.id))


def _active_job(db: Session, bot_id: int) -> Job | None:
    return db.scalar(
        select(Job).where(Job.bot_id == bot_id, Job.status.in_(ACTIVE)).order_by(Job.id.desc()).limit(1)
    )


def _bot_out(db: Session, bot: Bot) -> dict[str, Any]:
    job = _active_job(db, bot.id)
    return S.bot_json(bot, _job_out(db, job) if job else None)


def _check_series(series: list[str]) -> list[str]:
    bad = [s for s in series if s not in ALLOWED_SERIES]
    if bad or not series:
        raise HTTPException(422, f"series must be a non-empty subset of {sorted(ALLOWED_SERIES)}")
    return list(dict.fromkeys(series))


def _check_params(strategy: str, params: dict[str, Any]) -> dict[str, Any]:
    if strategy not in STRATEGIES:
        raise HTTPException(422, f"unknown strategy {strategy!r}")
    try:
        return validate_params(strategy, params)
    except ValueError as e:
        raise HTTPException(422, str(e)) from None


def _own_key(db: Session, user: User, key_id: int | None) -> KalshiKey | None:
    if key_id is None:
        return None
    key = db.get(KalshiKey, key_id)
    if key is None or key.user_id != user.id:
        raise HTTPException(404, "key not found")
    return key


def _positive_dec(name: str, v: Any) -> Decimal:
    try:
        d = Decimal(str(v))
    except InvalidOperation:
        raise HTTPException(422, f"{name} must be a number") from None
    if not d.is_finite() or d < 0 or d > Decimal(1_000_000):
        raise HTTPException(422, f"{name} out of range")
    return d


def _set_session_cookie(resp: Response, token: str, settings: BackendSettings) -> None:
    resp.set_cookie(
        COOKIE,
        token,
        max_age=settings.session_ttl_hours * 3600,
        httponly=True,
        samesite="lax",
        secure=settings.cookie_secure,
        path="/",
    )


# ---------------------------------------------------------------------------------------------- auth


auth = APIRouter(prefix="/api/auth")


@auth.post("/login")
def login(body: LoginBody, request: Request, response: Response, db: DB) -> dict[str, Any]:
    ctx = ctx_of(request)
    ip = request.client.host if request.client else "?"
    keys = (f"u:{body.username.lower()}", f"ip:{ip}")
    if ctx.limiter.blocked(*keys):
        raise HTTPException(429, "too many failed logins; try again later")
    user = db.scalar(select(User).where(User.username == body.username))
    if user is None or not user.is_active or not verify_password(user.password_hash, body.password):
        ctx.limiter.fail(*keys)
        raise HTTPException(401, "invalid username or password")
    ctx.limiter.reset(keys[0])
    db.execute(delete(UserSession).where(UserSession.expires_at < utcnow()))
    token = new_session_token()
    db.add(
        UserSession(
            id=session_id_for(token),
            user_id=user.id,
            expires_at=utcnow() + timedelta(hours=ctx.settings.session_ttl_hours),
            user_agent=(request.headers.get("user-agent") or "")[:256],
        )
    )
    user.last_login_at = utcnow()
    _set_session_cookie(response, token, ctx.settings)
    return {"user": S.user_json(user), "csrf": csrf_for(token)}


@auth.post("/logout", status_code=204)
def logout(request: Request, db: DB, user: CurrentUser) -> Response:
    token = request.cookies.get(COOKIE) or ""
    db.execute(delete(UserSession).where(UserSession.id == session_id_for(token)))
    resp = Response(status_code=204)
    resp.delete_cookie(COOKIE, path="/")
    return resp


@auth.get("/me")
def me(request: Request, user: CurrentUser) -> dict[str, Any]:
    return {"user": S.user_json(user), "csrf": csrf_for(request.cookies.get(COOKIE) or "")}


@auth.post("/password", status_code=204)
def change_password(body: PasswordBody, request: Request, db: DB, user: CurrentUser) -> Response:
    if not verify_password(user.password_hash, body.current_password):
        raise HTTPException(400, "current password is wrong")
    if err := check_password_policy(body.new_password):
        raise HTTPException(422, err)
    user.password_hash = hash_password(body.new_password)
    keep = session_id_for(request.cookies.get(COOKIE) or "")
    db.execute(delete(UserSession).where(UserSession.user_id == user.id, UserSession.id != keep))
    return Response(status_code=204)


# ---------------------------------------------------------------------------------------------- users


users = APIRouter(prefix="/api/users")


@users.get("")
def list_users(db: DB, admin: AdminUser) -> list[dict[str, Any]]:
    return [S.user_json(u) for u in db.scalars(select(User).order_by(User.id))]


@users.post("", status_code=201)
def create_user(body: NewUserBody, db: DB, admin: AdminUser) -> dict[str, Any]:
    if err := check_password_policy(body.password):
        raise HTTPException(422, err)
    if db.scalar(select(User).where(User.username == body.username)):
        raise HTTPException(409, "username already exists")
    u = User(username=body.username, password_hash=hash_password(body.password), role=body.role)
    db.add(u)
    db.flush()
    return S.user_json(u)


@users.patch("/{user_id}")
def patch_user(user_id: int, body: PatchUserBody, db: DB, admin: AdminUser) -> dict[str, Any]:
    u = db.get(User, user_id)
    if u is None:
        raise HTTPException(404, "user not found")
    if u.id == admin.id and (body.role == "user" or body.is_active is False):
        raise HTTPException(400, "you cannot demote or deactivate yourself")
    if body.role is not None:
        u.role = body.role
    if body.is_active is not None:
        u.is_active = body.is_active
        if not body.is_active:
            db.execute(delete(UserSession).where(UserSession.user_id == u.id))
    if body.live_enabled is not None:
        u.live_enabled = body.live_enabled
    if body.password is not None:
        if err := check_password_policy(body.password):
            raise HTTPException(422, err)
        u.password_hash = hash_password(body.password)
        db.execute(delete(UserSession).where(UserSession.user_id == u.id))
    return S.user_json(u)


# ---------------------------------------------------------------------------------------------- keys


keys = APIRouter(prefix="/api/keys")


def _lease_map(db: Session, key_ids: list[int]) -> dict[int, int]:
    if not key_ids:
        return {}
    now = utcnow()
    out = {}
    for lease in db.scalars(select(KeyLease).where(KeyLease.key_id.in_(key_ids))):
        if _aware(lease.expires_at) > now:  # type: ignore[operator]
            out[lease.key_id] = lease.job_id
    return out


@keys.get("")
def list_keys(db: DB, user: CurrentUser) -> list[dict[str, Any]]:
    rows = list(db.scalars(select(KalshiKey).where(KalshiKey.user_id == user.id).order_by(KalshiKey.id)))
    leases = _lease_map(db, [k.id for k in rows])
    return [S.key_json(k, leases.get(k.id)) for k in rows]


@keys.post("", status_code=201)
def create_key(body: NewKeyBody, request: Request, db: DB, user: CurrentUser) -> dict[str, Any]:
    vault = ctx_of(request).vault
    if vault is None:
        raise HTTPException(503, "key vault is not configured (KMB_VAULT_KEY_FILE)")
    from kmb.kalshi.auth import load_private_key_pem

    pem = body.private_key_pem.strip().encode()
    try:
        load_private_key_pem(pem)
    except Exception:
        raise HTTPException(422, "private_key_pem is not a valid RSA private key (PEM)") from None
    k = KalshiKey(
        user_id=user.id,
        label=body.label,
        environment=body.environment,
        api_key_id=body.api_key_id.strip(),
        private_key_enc=vault.encrypt(pem, key_aad(user.id, body.api_key_id.strip())),
    )
    db.add(k)
    db.flush()
    return S.key_json(k, None)


@keys.delete("/{key_id}", status_code=204)
def delete_key(key_id: int, db: DB, user: CurrentUser) -> Response:
    k = _own_key(db, user, key_id)
    if _lease_map(db, [key_id]):
        raise HTTPException(409, "key is in use by a running job")
    db.delete(k)
    return Response(status_code=204)


@keys.post("/{key_id}/test")
async def test_key(key_id: int, request: Request, user: CurrentUser) -> dict[str, Any]:
    ctx = ctx_of(request)
    with ctx.sessions() as db:
        k = _own_key(db, user, key_id)
        assert k is not None
        api_key_id, env, blob, uid = k.api_key_id, k.environment, k.private_key_enc, k.user_id
    if ctx.vault is None:
        raise HTTPException(503, "key vault is not configured")
    from kmb.kalshi.auth import KalshiAuth
    from kmb.kalshi.endpoints import KalshiEnvironment
    from kmb.kalshi.rest import KalshiRestClient

    try:
        pem = ctx.vault.decrypt(blob, key_aad(uid, api_key_id))
        client = KalshiRestClient(
            KalshiEnvironment.from_name(env).rest_base_url,
            KalshiAuth.from_pem(api_key_id, pem),
            max_retries=0,
            timeout=10.0,
        )
        async with client:
            bal = await client.get_balance()
    except Exception as e:  # report, never raise: this is a diagnostic endpoint
        return {"ok": False, "error": str(e)[:300]}
    with ctx.sessions() as db:
        row = db.get(KalshiKey, key_id)
        if row is not None:
            row.last_used_at = utcnow()
            db.commit()
    if "balance_dollars" in bal:
        balance = str(bal["balance_dollars"])
    else:
        balance = format(Decimal(int(bal.get("balance", 0))) / 100, "f")
    return {"ok": True, "balance": balance}


# ------------------------------------------------------------ strategies & bots


bots = APIRouter(prefix="/api")


@bots.get("/strategies")
def strategies(user: CurrentUser) -> list[dict[str, Any]]:
    return describe()


@bots.get("/bots")
def list_bots(db: DB, user: CurrentUser) -> list[dict[str, Any]]:
    rows = db.scalars(select(Bot).where(Bot.user_id == user.id).order_by(Bot.id))
    return [_bot_out(db, b) for b in rows]


@bots.post("/bots", status_code=201)
def create_bot(body: BotBody, db: DB, user: CurrentUser) -> dict[str, Any]:
    params = _check_params(body.strategy, body.params)
    _own_key(db, user, body.key_id)
    b = Bot(
        user_id=user.id,
        name=body.name,
        strategy=body.strategy,
        params=params,
        series=_check_series(body.series),
        key_id=body.key_id,
    )
    db.add(b)
    try:
        db.flush()
    except IntegrityError:
        raise HTTPException(409, "a bot with that name already exists") from None
    return _bot_out(db, b)


def _own_bot(db: Session, user: User, bot_id: int) -> Bot:
    b = db.get(Bot, bot_id)
    if b is None or b.user_id != user.id:
        raise HTTPException(404, "bot not found")
    return b


@bots.patch("/bots/{bot_id}")
def patch_bot(bot_id: int, body: PatchBotBody, db: DB, user: CurrentUser) -> dict[str, Any]:
    b = _own_bot(db, user, bot_id)
    fields = body.model_fields_set
    strategy = body.strategy if "strategy" in fields and body.strategy else b.strategy
    if "params" in fields or "strategy" in fields:
        b.params = _check_params(strategy, body.params if body.params is not None else b.params)
        b.strategy = strategy
    if "name" in fields and body.name:
        b.name = body.name
    if "series" in fields and body.series is not None:
        b.series = _check_series(body.series)
    if "key_id" in fields:
        _own_key(db, user, body.key_id)
        b.key_id = body.key_id
    try:
        db.flush()
    except IntegrityError:
        raise HTTPException(409, "a bot with that name already exists") from None
    return _bot_out(db, b)


@bots.delete("/bots/{bot_id}", status_code=204)
def delete_bot(bot_id: int, db: DB, user: CurrentUser) -> Response:
    b = _own_bot(db, user, bot_id)
    if _active_job(db, b.id):
        raise HTTPException(409, "stop the bot's active job first")
    db.delete(b)
    return Response(status_code=204)


# ---------------------------------------------------------------------------------------------- jobs


jobs = APIRouter(prefix="/api/jobs")


@jobs.get("")
def list_jobs(
    db: DB,
    user: CurrentUser,
    status: str | None = None,
    kind: str | None = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[dict[str, Any]]:
    q = select(Job).order_by(Job.id.desc()).limit(limit)
    if user.role != "admin":
        q = q.where(Job.user_id == user.id)
    if status:
        q = q.where(Job.status == status)
    if kind:
        q = q.where(Job.kind == kind)
    rows = list(db.scalars(q))
    runs = _run_ids_for_jobs(db, [j.id for j in rows])
    return [S.job_json(j, runs.get(j.id)) for j in rows]


@jobs.post("", status_code=201)
def create_job(body: NewJobBody, db: DB, user: CurrentUser) -> dict[str, Any]:
    bot = _own_bot(db, user, body.bot_id)
    spec: dict[str, Any] = {
        "bot": {
            "id": bot.id,
            "name": bot.name,
            "strategy": bot.strategy,
            "params": dict(bot.params or {}),
            "series": list(bot.series or []),
            "key_id": bot.key_id,
        }
    }
    if body.kind == "sim":
        if not body.windows:
            raise HTTPException(422, "sim jobs need at least one window")
        spec["windows"] = list(dict.fromkeys(body.windows))
        spec["sim"] = (body.sim or SimOptions()).model_dump(exclude_none=True)
    else:
        if db.scalar(
            select(Job.id).where(
                Job.bot_id == bot.id, Job.status.in_(ACTIVE), Job.kind.in_(("paper", "live"))
            )
        ):
            raise HTTPException(409, "this bot already has an active paper/live job")
        kill = ops.kill_switch_state(db, user.id)
        if body.kind == "live":
            if not user.live_enabled:
                raise HTTPException(403, "live trading is not enabled for your account")
            if not ops.live_trading_enabled(db):
                raise HTTPException(403, "live trading is disabled globally")
            if bot.key_id is None:
                raise HTTPException(403, "the bot has no Kalshi key")
            if kill["global"] or kill["user"]:
                raise HTTPException(403, "a kill switch is engaged")
        elif kill["global"] or kill["user"]:
            raise HTTPException(403, "a kill switch is engaged")
        if body.sim:
            spec["sim"] = body.sim.model_dump(exclude_none=True)
    job = Job(user_id=user.id, bot_id=bot.id, kind=body.kind, spec=spec)
    db.add(job)
    db.flush()
    ops.log_event(db, "info", "job_created", f"{body.kind} job created", user_id=user.id, job_id=job.id)
    return S.job_json(job, None)


def _own_job(db: Session, user: User, job_id: int) -> Job:
    j = db.get(Job, job_id)
    if j is None or not _visible(user, j.user_id):
        raise HTTPException(404, "job not found")
    return j


@jobs.get("/{job_id}")
def get_job(job_id: int, db: DB, user: CurrentUser) -> dict[str, Any]:
    return _job_out(db, _own_job(db, user, job_id))


@jobs.post("/{job_id}/stop")
def stop_job(job_id: int, db: DB, user: CurrentUser) -> dict[str, Any]:
    j = _own_job(db, user, job_id)
    if j.status == "queued":
        j.status = "canceled"
        j.finished_at = utcnow()
        j.cancel_requested = True
    elif j.status == "running":
        j.cancel_requested = True
    return _job_out(db, j)


@jobs.get("/{job_id}/events")
def job_events(
    job_id: int, db: DB, user: CurrentUser, limit: int = Query(200, ge=1, le=2000)
) -> list[dict[str, Any]]:
    _own_job(db, user, job_id)
    rows = db.scalars(
        select(BotEvent).where(BotEvent.job_id == job_id).order_by(BotEvent.id.desc()).limit(limit)
    )
    return [S.event_json(e) for e in rows]


# ---------------------------------------------------------------------------------------------- runs


runs = APIRouter(prefix="/api/runs")


@runs.get("")
def list_runs(
    db: DB, user: CurrentUser, mode: str | None = None, limit: int = Query(50, ge=1, le=500)
) -> list[dict[str, Any]]:
    q = select(Run).order_by(Run.id.desc()).limit(limit)
    if user.role != "admin":
        q = q.where(Run.user_id == user.id)
    if mode:
        q = q.where(Run.mode == mode)
    return [S.run_json(r) for r in db.scalars(q)]


@runs.get("/{run_id}")
def get_run(run_id: int, db: DB, user: CurrentUser) -> dict[str, Any]:
    r = db.get(Run, run_id)
    if r is None or not _visible(user, r.user_id):
        raise HTTPException(404, "run not found")
    out = S.run_json(r)
    out["orders"] = [
        S.order_json(o) for o in db.scalars(select(Order).where(Order.run_id == r.id).order_by(Order.id))
    ]
    out["fills"] = [
        S.fill_json(f) for f in db.scalars(select(FillRow).where(FillRow.run_id == r.id).order_by(FillRow.id))
    ]
    out["positions"] = [
        S.position_json(p) for p in db.scalars(select(PositionRow).where(PositionRow.run_id == r.id))
    ]
    out["settlements"] = [
        S.settlement_json(s) for s in db.scalars(select(Settlement).where(Settlement.run_id == r.id))
    ]
    out["equity"] = (r.summary or {}).get("equity_curve", [])
    return out


class CalibrateBody(BaseModel):
    sim: SimOptions | None = None


@runs.post("/{run_id}/calibrate", status_code=201)
def calibrate_run(
    run_id: int, db: DB, user: CurrentUser, body: CalibrateBody | None = None
) -> dict[str, Any]:
    """Queue a G4 calibration job: replay the live run's orders on the recorded windows and compare fills."""
    r = db.get(Run, run_id)
    if r is None or not _visible(user, r.user_id):
        raise HTTPException(404, "run not found")
    if r.mode != "live":
        raise HTTPException(422, "only live runs can be calibrated")
    if r.job_id is not None and db.scalar(select(Job.id).where(Job.id == r.job_id, Job.status.in_(ACTIVE))):
        raise HTTPException(409, "the live run is still running")
    spec: dict[str, Any] = {"live_run_id": r.id}
    if body is not None and body.sim is not None:
        spec["sim"] = body.sim.model_dump(exclude_none=True)
    # The worker requires the job to belong to the run's owner (admins act on the owner's behalf).
    job = Job(user_id=r.user_id, bot_id=r.bot_id, kind="calibration", spec=spec)
    db.add(job)
    db.flush()
    ops.log_event(
        db, "info", "job_created", f"calibration job created for run {r.id}", user_id=r.user_id, job_id=job.id
    )
    return S.job_json(job, None)


# ------------------------------------------------------------ windows & replay


windows = APIRouter(prefix="/api/windows")


def _source_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except WindowNotFound:
        raise HTTPException(404, "window not found") from None
    except httpx.HTTPError as e:
        raise HTTPException(503, f"data API unavailable: {e}") from None


@windows.get("")
def list_windows(
    request: Request,
    user: CurrentUser,
    series: str | None = None,
    from_: Annotated[str | None, Query(alias="from")] = None,
    to: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> list[dict[str, Any]]:
    src = ctx_of(request).source
    rows = _source_call(src.list_windows, series=series, start=from_, end=to, limit=limit)
    return [S.window_json(w) for w in rows]


@windows.get("/{window_id}")
def get_window(window_id: str, request: Request, user: CurrentUser) -> dict[str, Any]:
    return S.window_json(_source_call(ctx_of(request).source.get_window, window_id))


@windows.get("/{window_id}/overview")
def get_overview(window_id: str, request: Request, user: CurrentUser) -> dict[str, Any]:
    ctx = ctx_of(request)
    cached = ctx.overview_cache.get(window_id)
    if cached is not None:
        ctx.overview_cache.move_to_end(window_id)
        return cached
    window = _source_call(ctx.source.get_window, window_id)
    events = _source_call(ctx.source.load_events, window_id)
    out = build_overview(window, events)
    out["window"] = S.window_json(window)
    ctx.overview_cache[window_id] = out
    while len(ctx.overview_cache) > 16:
        ctx.overview_cache.popitem(last=False)
    return out


def _origin_ok(ws: WebSocket, settings: BackendSettings) -> bool:
    origin = ws.headers.get("origin")
    if not origin:
        return True
    host = ws.headers.get("host", "")
    allowed = {f"http://{host}", f"https://{host}", *settings.allowed_origins}
    return origin in allowed


async def replay_ws(ws: WebSocket, window_id: str) -> None:
    ctx: AppCtx = ws.app.state.kmb
    with ctx.sessions() as db:
        user = _user_for_token(db, ws.cookies.get(COOKIE))
    if user is None or not _origin_ok(ws, ctx.settings):
        await ws.close(code=4401)
        return
    await ws.accept()
    try:
        window = await asyncio.to_thread(ctx.source.get_window, window_id)
        events = await asyncio.to_thread(ctx.source.load_events, window_id)
    except WindowNotFound:
        await ws.send_json({"type": "error", "detail": "window not found"})
        await ws.close()
        return
    except Exception as e:
        await ws.send_json({"type": "error", "detail": f"could not load window: {e}"})
        await ws.close()
        return
    ticker = window.get("market_ticker") or window_id
    times = window_times(window, events)
    cursor = ReplayCursor(events, start_ms=times["start_ms"])
    end_ms = times["end_ms"]
    await ws.send_json({"type": "meta", "window": S.window_json(window), **times})

    playing = False
    speed = 1.0

    async def send_frame(applied, notes) -> None:
        await ws.send_json(
            build_frame(
                cursor.state, ticker, applied, notes, ts_ms=cursor.now_ms, playing=playing, speed=speed
            )
        )

    applied, notes = cursor.advance_to(cursor.start_ms)
    await send_frame(applied, notes)
    last_wall = time.monotonic()
    try:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_json(), timeout=FRAME_INTERVAL_S if playing else None)
            except TimeoutError:
                msg = None
            now_wall = time.monotonic()
            if msg is not None:
                op = msg.get("op") if isinstance(msg, dict) else None
                if op == "play":
                    playing = cursor.now_ms < end_ms
                    last_wall = now_wall
                    await send_frame([], [])
                elif op == "pause":
                    playing = False
                    await send_frame([], [])
                elif op == "speed":
                    try:
                        speed = min(60.0, max(0.25, float(msg.get("speed", 1))))
                    except (TypeError, ValueError):
                        await ws.send_json({"type": "error", "detail": "bad speed"})
                    await send_frame([], [])
                elif op == "seek":
                    try:
                        cursor.seek(int(msg["ts_ms"]))
                    except (KeyError, TypeError, ValueError):
                        await ws.send_json({"type": "error", "detail": "bad seek"})
                        continue
                    last_wall = now_wall
                    await send_frame([], [])
                elif op == "step":
                    try:
                        step = max(1, min(int(msg.get("ms", 1000)), 15 * 60_000))
                    except (TypeError, ValueError):
                        step = 1000
                    applied, notes = cursor.advance_to(min(cursor.now_ms + step, end_ms))
                    await send_frame(applied, notes)
                else:
                    await ws.send_json({"type": "error", "detail": f"unknown op {op!r}"})
            if playing and now_wall - last_wall >= FRAME_INTERVAL_S * 0.9:
                target = min(end_ms, cursor.now_ms + int((now_wall - last_wall) * 1000 * speed))
                last_wall = now_wall
                applied, notes = cursor.advance_to(target)
                if cursor.now_ms >= end_ms:
                    playing = False
                await send_frame(applied, notes)
                if not playing:
                    await ws.send_json({"type": "end"})
    except WebSocketDisconnect:
        return


# ---------------------------------------------------------------------------------------------- operations


opsr = APIRouter(prefix="/api")


async def _fetch_json(url: str | None) -> dict[str, Any] | None:
    if not url:
        return None
    try:
        async with httpx.AsyncClient(timeout=1.5) as c:
            r = await c.get(url)
            if r.status_code < 500:
                return r.json()
    except (httpx.HTTPError, ValueError):
        return None
    return None


async def _status(request: Request, user: User) -> dict[str, Any]:
    ctx = ctx_of(request)
    collector, ingest, data_api = await asyncio.gather(
        _fetch_json(ctx.settings.collector_status_url),
        _fetch_json(ctx.settings.ingest_status_url),
        asyncio.to_thread(ctx.source.health),
    )
    with ctx.sessions() as db:
        if collector is None:
            row = db.scalar(select(CollectorStatus).order_by(CollectorStatus.updated_at.desc()).limit(1))
            collector = S.jsonable(row.status) if row else None
        if ingest is None:
            row2 = db.scalar(select(IngestStatus).order_by(IngestStatus.updated_at.desc()).limit(1))
            ingest = S.jsonable(row2.status) if row2 else None
        cutoff = utcnow() - timedelta(minutes=10)
        workers = [
            S.worker_json(w)
            for w in db.scalars(select(WorkerStatus).order_by(WorkerStatus.worker_id))
            if _aware(w.heartbeat_at) >= cutoff  # type: ignore[operator]
        ]
        return {
            "collector": collector,
            "ingest": ingest,
            "data_api": S.jsonable(data_api),
            "workers": workers,
            "live_trading_enabled": ops.live_trading_enabled(db),
            "kill_switch": ops.kill_switch_state(db, user.id),
        }


@opsr.get("/status")
async def status(request: Request, user: CurrentUser) -> dict[str, Any]:
    return await _status(request, user)


@opsr.get("/live-markets")
async def live_markets(request: Request, user: CurrentUser) -> dict[str, Any]:
    url = ctx_of(request).settings.collector_status_url
    if not url:
        raise HTTPException(503, "Live market recording is not configured")
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            response = await client.get(url.rsplit("/", 1)[0] + "/live-markets")
            response.raise_for_status()
            data = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("Live market view unavailable: %s", exc)
        raise HTTPException(503, "Cannot reach live markets. Retrying shortly.") from exc
    if not isinstance(data, dict) or not isinstance(data.get("markets"), list):
        log.error("Collector returned an invalid live market view")
        raise HTTPException(502, "Live market data is not available")
    return data


@opsr.get("/fidelity")
def fidelity(db: DB, user: CurrentUser, limit: int = Query(50, ge=1, le=500)) -> list[dict[str, Any]]:
    rows = db.scalars(select(FidelityReport).order_by(FidelityReport.id.desc()).limit(limit))
    return [S.fidelity_json(f) for f in rows]


@opsr.get("/risk-limits")
def get_limits(db: DB, user: CurrentUser) -> dict[str, Any]:
    g = ops.risk_limits_for(db, None)
    own = db.scalar(select(RiskLimit).where(RiskLimit.user_id == user.id))
    return {"global": S.limits_json(g), "user": S.limits_json(own) if own else None}


@opsr.put("/risk-limits/{target}")
def put_limits(target: str, body: LimitsBody, db: DB, admin: AdminUser) -> dict[str, Any]:
    if target == "global":
        row = ops.risk_limits_for(db, None)
    else:
        try:
            uid = int(target)
        except ValueError:
            raise HTTPException(404, "unknown target") from None
        if db.get(User, uid) is None:
            raise HTTPException(404, "user not found")
        row = db.scalar(select(RiskLimit).where(RiskLimit.user_id == uid))
        if row is None:
            g = ops.risk_limits_for(db, None)
            row = RiskLimit(
                user_id=uid,
                max_order_count=g.max_order_count,
                max_position_per_market=g.max_position_per_market,
                max_open_orders=g.max_open_orders,
                max_daily_loss=g.max_daily_loss,
                max_notional_per_market=g.max_notional_per_market,
                max_orders_per_minute=g.max_orders_per_minute,
            )
            db.add(row)
    for name in ("max_order_count", "max_position_per_market", "max_daily_loss", "max_notional_per_market"):
        v = getattr(body, name)
        if v is not None:
            setattr(row, name, _positive_dec(name, v))
    if body.max_open_orders is not None:
        row.max_open_orders = body.max_open_orders
    if body.max_orders_per_minute is not None:
        row.max_orders_per_minute = body.max_orders_per_minute
    db.flush()
    return S.limits_json(row)


@opsr.post("/kill-switch")
async def kill_switch(body: KillBody, request: Request, user: CurrentUser) -> dict[str, Any]:
    if body.scope == "global" and user.role != "admin":
        raise HTTPException(403, "only admins can use the global kill switch")
    with ctx_of(request).sessions() as db:
        ops.set_kill_switch(
            db,
            body.scope,
            body.engaged,
            user.id,
            body.reason,
            user_id=user.id if body.scope == "user" else None,
        )
        ops.log_event(
            db,
            "warn" if body.engaged else "info",
            "kill_switch",
            f"{body.scope} kill switch {'engaged' if body.engaged else 'released'} by {user.username}",
            user_id=user.id,
            detail={"reason": body.reason},
        )
        db.commit()
    return await _status(request, user)


@opsr.put("/settings/live-trading")
async def live_trading(body: LiveTradingBody, request: Request, admin: AdminUser) -> dict[str, Any]:
    with ctx_of(request).sessions() as db:
        ops.set_setting(db, ops.LIVE_TRADING, {"enabled": body.enabled, "by": admin.id})
        db.commit()
    return await _status(request, admin)


# ---------------------------------------------------------------------------------------------- app


def _security_headers(resp: Response) -> None:
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "same-origin")
    resp.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; connect-src 'self' ws: wss:; img-src 'self' data:; "
        "style-src 'self' 'unsafe-inline'",
    )


def create_app(
    settings: BackendSettings | None = None,
    sessions: sessionmaker[Session] | None = None,
    source: WindowSource | None = None,
) -> FastAPI:
    settings = settings or BackendSettings.from_env()
    if sessions is None:
        engine = make_engine(settings.database_url)
        init_db(engine)
        sessions = session_factory(engine)
    vault = Vault(settings.vault_key_hex) if settings.vault_key_hex else None
    if vault is None:
        log.warning("no vault key configured: Kalshi keys cannot be stored")
    app = FastAPI(title="kmb backend", version="1", docs_url="/api/docs", openapi_url="/api/openapi.json")
    app.state.kmb = AppCtx(
        settings=settings,
        sessions=sessions,
        source=source or source_from_env(),
        vault=vault,
        limiter=LoginLimiter(settings.login_max_failures, settings.login_window_s),
    )

    @app.middleware("http")
    async def headers_mw(request: Request, call_next):
        resp = await call_next(request)
        _security_headers(resp)
        return resp

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True}

    for r in (auth, users, keys, bots, jobs, runs, windows, opsr):
        app.include_router(r)
    from kmb.backend.research import router as research_router

    app.include_router(research_router)
    app.add_api_websocket_route("/ws/replay/{window_id}", replay_ws)
    from kmb.backend.live_stream import live_markets_ws

    app.add_api_websocket_route("/ws/live-markets", live_markets_ws)

    static = Path(settings.static_dir).resolve() if settings.static_dir else None
    if static and (static / "index.html").exists():

        @app.get("/{path:path}", include_in_schema=False)
        def spa(path: str) -> Response:
            if path.startswith(("api/", "ws/")):
                return JSONResponse({"detail": "not found"}, status_code=404)
            candidate = (static / path).resolve()
            if path and candidate.is_file() and candidate.is_relative_to(static):
                return FileResponse(candidate)
            return FileResponse(static / "index.html", headers={"Cache-Control": "no-cache"})

    else:
        log.info("frontend build not found at %s; serving API only", static)
    return app
