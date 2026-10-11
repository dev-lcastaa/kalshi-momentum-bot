"""DB operations shared by the backend and workers.

Settings, kill switches, risk limits, job queue, events.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from kmb.db.models import AppSetting, BotEvent, Job, KeyLease, KillSwitchLog, RiskLimit, WorkerStatus, utcnow

LIVE_TRADING = "live_trading"
KILL_GLOBAL = "kill_switch:global"


def kill_user_key(user_id: int) -> str:
    return f"kill_switch:user:{user_id}"


def get_setting(s: Session, key: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
    row = s.get(AppSetting, key)
    return dict(row.value) if row else dict(default or {})


def set_setting(s: Session, key: str, value: dict[str, Any]) -> None:
    row = s.get(AppSetting, key)
    if row is None:
        s.add(AppSetting(key=key, value=value))
    else:
        row.value = value
        row.updated_at = utcnow()


def live_trading_enabled(s: Session) -> bool:
    return bool(get_setting(s, LIVE_TRADING, {"enabled": False}).get("enabled"))


def kill_switch_state(s: Session, user_id: int | None) -> dict[str, bool]:
    g = bool(get_setting(s, KILL_GLOBAL).get("engaged"))
    u = bool(get_setting(s, kill_user_key(user_id)).get("engaged")) if user_id is not None else False
    return {"global": g, "user": u}


def set_kill_switch(
    s: Session, scope: str, engaged: bool, actor_user_id: int | None, reason: str, user_id: int | None = None
) -> None:
    key = KILL_GLOBAL if scope == "global" else kill_user_key(int(user_id))  # type: ignore[arg-type]
    set_setting(
        s, key, {"engaged": engaged, "reason": reason, "actor": actor_user_id, "at": utcnow().isoformat()}
    )
    s.add(
        KillSwitchLog(
            scope=scope,
            user_id=user_id if scope == "user" else None,
            engaged=engaged,
            actor_user_id=actor_user_id,
            reason=reason,
        )
    )
    if engaged:
        q = update(Job).where(Job.status.in_(("queued", "running")), Job.kind.in_(("paper", "live")))
        if scope == "user":
            q = q.where(Job.user_id == user_id)
        s.execute(q.values(cancel_requested=True))


def risk_limits_for(s: Session, user_id: int | None) -> RiskLimit:
    if user_id is not None:
        row = s.scalar(select(RiskLimit).where(RiskLimit.user_id == user_id))
        if row is not None:
            return row
    row = s.scalar(select(RiskLimit).where(RiskLimit.user_id.is_(None)))
    if row is None:
        row = RiskLimit(user_id=None)
        s.add(row)
        s.flush()
    return row


def log_event(
    s: Session,
    level: str,
    code: str,
    message: str,
    *,
    user_id: int | None = None,
    job_id: int | None = None,
    run_id: int | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    s.add(
        BotEvent(
            user_id=user_id,
            job_id=job_id,
            run_id=run_id,
            level=level,
            code=code,
            message=message[:4000],
            detail=detail or {},
        )
    )


# ---------------------------------------------------------------------------------------------- job queue


def claim_job(s: Session, worker_id: str, kinds: tuple[str, ...]) -> Job | None:
    """Atomically claim the oldest queued job.

    Uses SKIP LOCKED on Postgres; plain select elsewhere (tests).
    """
    q = (
        select(Job)
        .where(Job.status == "queued", Job.kind.in_(kinds))
        .order_by(Job.created_at, Job.id)
        .limit(1)
    )
    if s.bind is not None and s.bind.dialect.name == "postgresql":
        q = q.with_for_update(skip_locked=True)
    job = s.scalar(q)
    if job is None:
        return None
    if job.cancel_requested:
        job.status = "canceled"
        job.finished_at = utcnow()
        return None
    job.status = "running"
    job.worker_id = worker_id
    job.started_at = utcnow()
    job.heartbeat_at = utcnow()
    return job


def requeue_stale_jobs(s: Session, stale_after: timedelta) -> int:
    """Running jobs whose worker stopped heartbeating are failed.

    Never silently re-run: live jobs place orders.
    """
    cutoff = utcnow() - stale_after
    rows = s.scalars(select(Job).where(Job.status == "running", Job.heartbeat_at < cutoff)).all()
    for job in rows:
        job.status = "failed"
        job.error = f"worker {job.worker_id} stopped heartbeating"
        job.finished_at = utcnow()
        s.execute(KeyLease.__table__.delete().where(KeyLease.job_id == job.id))
    return len(rows)


def heartbeat_worker(
    s: Session, worker_id: str, host: str, capabilities: list[str], job_id: int | None
) -> None:
    row = s.get(WorkerStatus, worker_id)
    now = utcnow()
    if row is None:
        s.add(WorkerStatus(worker_id=worker_id, host=host, capabilities=capabilities, current_job_id=job_id))
    else:
        row.host = host
        row.capabilities = capabilities
        row.current_job_id = job_id
        row.heartbeat_at = now


def acquire_key_lease(s: Session, key_id: int, job_id: int, worker_id: str, ttl: timedelta) -> bool:
    now = utcnow()
    lease = s.get(KeyLease, key_id)
    if lease is not None and lease.job_id != job_id and _aware(lease.expires_at) > now:
        return False
    if lease is None:
        s.add(KeyLease(key_id=key_id, job_id=job_id, worker_id=worker_id, expires_at=now + ttl))
    else:
        lease.job_id = job_id
        lease.worker_id = worker_id
        lease.acquired_at = now
        lease.expires_at = now + ttl
    return True


def release_key_lease(s: Session, key_id: int, job_id: int) -> None:
    lease = s.get(KeyLease, key_id)
    if lease is not None and lease.job_id == job_id:
        s.delete(lease)


def _aware(dt: datetime) -> datetime:
    from datetime import UTC

    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
