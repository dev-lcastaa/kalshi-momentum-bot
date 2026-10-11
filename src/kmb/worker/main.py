"""Worker process: claim jobs from the shared database and run them, one at a time.

Scale out by running more worker containers (each with its own ``KMB_WORKER_ID``). Live jobs additionally
take a per-key lease, so two workers can never trade the same Kalshi account at once.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import time
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from kmb.db import make_engine, ops, session_factory
from kmb.db.models import Job, utcnow
from kmb.worker import jobs
from kmb.worker.jobs import JobCanceled, JobFailed, WorkerContext, ops_session

log = logging.getLogger(__name__)

ALL_KINDS = ("sim", "paper", "live", "calibration")


def _read_file(path: str | None) -> str | None:
    if path and os.path.exists(path):
        return Path(path).read_text(encoding="utf-8").strip()
    return None


@dataclass
class WorkerSettings:
    worker_id: str = field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    kinds: tuple[str, ...] = ALL_KINDS
    poll_s: float = 2.0
    stale_s: int = 120
    nats_url: str | None = None
    vault_key_hex: str | None = None
    database_url: str | None = None

    @classmethod
    def from_env(cls) -> WorkerSettings:
        env = os.environ
        kinds = tuple(
            k.strip() for k in env.get("KMB_WORKER_KINDS", ",".join(ALL_KINDS)).split(",") if k.strip()
        )
        bad = [k for k in kinds if k not in ALL_KINDS]
        if bad:
            raise ValueError(f"unknown KMB_WORKER_KINDS: {bad}")
        return cls(
            worker_id=env.get("KMB_WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}",
            kinds=kinds,
            poll_s=float(env.get("KMB_WORKER_POLL_S", "2")),
            stale_s=int(env.get("KMB_WORKER_STALE_S", "120")),
            nats_url=env.get("KMB_NATS_URL") or None,
            vault_key_hex=env.get("KMB_VAULT_KEY")
            or _read_file(env.get("KMB_VAULT_KEY_FILE", "/run/secrets/vault_key")),
            database_url=env.get("KMB_DATABASE_URL"),
        )


def build_context(settings: WorkerSettings, factory: sessionmaker[Session]) -> WorkerContext:
    from kmb.replay.source import source_from_env

    vault = None
    if settings.vault_key_hex:
        from kmb.backend.security import Vault

        vault = Vault(settings.vault_key_hex)
    feed_factory = None
    if settings.nats_url:
        from kmb.worker.feed import NatsFeed

        url = settings.nats_url

        def feed_factory() -> NatsFeed:
            return NatsFeed(url)

    return WorkerContext(
        factory=factory,
        worker_id=settings.worker_id,
        source_factory=source_from_env,
        feed_factory=feed_factory,
        vault=vault,
        capabilities=list(settings.kinds),
    )


async def run_job(wctx: WorkerContext, job: Job) -> dict[str, Any]:
    if job.kind == "sim":
        return await asyncio.to_thread(jobs.run_sim, wctx, job)
    if job.kind == "calibration":
        return await asyncio.to_thread(jobs.run_calibration, wctx, job)
    if job.kind == "paper":
        return await jobs.run_paper(wctx, job)
    if job.kind == "live":
        return await jobs.run_live(wctx, job)
    raise JobFailed(f"unknown job kind {job.kind!r}")


def _finish(
    factory: sessionmaker[Session], job_id: int, status: str, error: str | None, result: Any = None
) -> None:
    with ops_session(factory) as s:
        job = s.get(Job, job_id)
        if job is None:
            return
        job.status = status
        job.error = error
        job.finished_at = utcnow()
        if result is not None:
            job.spec = {**(job.spec or {}), "result": result}
        level = "error" if status == "failed" else "info"
        ops.log_event(
            s,
            level,
            f"job_{status}",
            f"{job.kind} job {job_id} {status}" + (f": {error}" if error else ""),
            user_id=job.user_id,
            job_id=job_id,
        )


async def process_one(wctx: WorkerContext, kinds: tuple[str, ...]) -> bool:
    """Claim and run one job. Returns False when the queue was empty."""
    with ops_session(wctx.factory) as s:
        job = ops.claim_job(s, wctx.worker_id, kinds)
        if job is not None:
            ops.heartbeat_worker(s, wctx.worker_id, wctx.host, wctx.capabilities, job.id)
            s.flush()
            s.expunge(job)
    if job is None:
        return False
    log.info("claimed %s job %s", job.kind, job.id)
    try:
        result = await run_job(wctx, job)
    except JobCanceled as exc:
        _finish(wctx.factory, job.id, "canceled", str(exc) or None)
    except JobFailed as exc:
        log.warning("job %s failed: %s", job.id, exc)
        _finish(wctx.factory, job.id, "failed", str(exc))
    except Exception as exc:
        log.exception("job %s crashed", job.id)
        _finish(wctx.factory, job.id, "failed", repr(exc)[:2000])
    else:
        _finish(wctx.factory, job.id, "succeeded", None, result)
    finally:
        with ops_session(wctx.factory) as s:
            ops.heartbeat_worker(s, wctx.worker_id, wctx.host, wctx.capabilities, None)
    return True


async def worker_loop(
    settings: WorkerSettings, factory: sessionmaker[Session], *, once: bool = False
) -> None:
    wctx = build_context(settings, factory)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # Windows
            pass
    log.info("worker %s started (kinds=%s)", settings.worker_id, ",".join(settings.kinds))
    last_reap = 0.0
    while not stop.is_set():
        if time.monotonic() - last_reap > 30:
            last_reap = time.monotonic()
            with ops_session(factory) as s:
                n = ops.requeue_stale_jobs(s, timedelta(seconds=settings.stale_s))
                ops.heartbeat_worker(s, wctx.worker_id, wctx.host, wctx.capabilities, None)
            if n:
                log.warning("failed %d stale jobs", n)
        did = await process_one(wctx, settings.kinds)
        if once:
            return
        if not did:
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.poll_s)
            except TimeoutError:
                pass
    log.info("worker %s stopped", settings.worker_id)


def main(once: bool = False) -> None:
    settings = WorkerSettings.from_env()
    engine = make_engine(settings.database_url)
    asyncio.run(worker_loop(settings, session_factory(engine), once=once))
