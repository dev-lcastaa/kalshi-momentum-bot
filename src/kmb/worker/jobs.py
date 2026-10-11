"""Job implementations: sim (recorded windows), paper (live bus + simulator), live (Kalshi), calibration (G4).

Every job runs one ``SimRunner`` loop; only the exchange and the event source differ. A ``JobControl``
polls the database (throttled) to heartbeat and pick up ``cancel_requested``, kill switches and risk limits.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from kmb.db import ops
from kmb.db.models import (
    FidelityReport,
    FillRow,
    Job,
    KalshiKey,
    Order,
    OrderEvent,
    Run,
    Settlement,
    User,
    utcnow,
)
from kmb.exchange import OrderRequest
from kmb.kalshi.auth import KalshiAuth
from kmb.kalshi.endpoints import KalshiEnvironment
from kmb.kalshi.messages import ZERO, D
from kmb.kalshi.rest import KalshiRestClient
from kmb.replay.runner import SimRunner
from kmb.replay.source import WindowSource
from kmb.replay.state import MarketState
from kmb.sim import SimConfig, SimExchange
from kmb.strategy.base import Cancel, Context, Event, Intent, Place, Strategy
from kmb.strategy.registry import create_strategy
from kmb.worker.feed import EventFeed
from kmb.worker.guard import Limits, RiskGuard
from kmb.worker.live import LiveExchange
from kmb.worker.recorder import DbRecorder

log = logging.getLogger(__name__)

MAX_EQUITY_POINTS = 2000
KEY_LEASE_TTL = timedelta(seconds=90)


class JobCanceled(Exception):
    pass


class JobFailed(Exception):
    pass


# ---------------------------------------------------------------------------------------------- context


@dataclass
class WorkerContext:
    factory: sessionmaker[Session]
    worker_id: str
    source_factory: Callable[[], WindowSource]
    feed_factory: Callable[[], EventFeed] | None = None
    vault: Any = None  # kmb.backend.security.Vault
    rest_factory: Callable[[KalshiEnvironment, KalshiAuth], KalshiRestClient] | None = None
    private_ws_factory: (
        Callable[[KalshiEnvironment, KalshiAuth, Callable[[dict[str, Any]], None]], Any] | None
    ) = None
    capabilities: list[str] = field(default_factory=lambda: ["sim", "paper", "live", "calibration"])
    control_every_s: float = 2.0
    summary_every_s: float = 15.0
    reconcile_every_s: float = 10.0
    balance_every_s: float = 60.0
    host: str = field(default_factory=socket.gethostname)
    _source: WindowSource | None = None

    @property
    def source(self) -> WindowSource:
        if self._source is None:
            self._source = self.source_factory()
        return self._source


@dataclass
class ControlState:
    cancel: bool = False
    kill: str | None = None
    limits: Limits = field(default_factory=Limits)


class JobControl:
    def __init__(self, wctx: WorkerContext, job: Job, *, lease_key_id: int | None = None):
        self.wctx = wctx
        self.job_id = job.id
        self.user_id = job.user_id
        self.lease_key_id = lease_key_id
        self.state = ControlState()
        self._last = 0.0

    def poll(self, force: bool = False) -> ControlState:
        now = time.monotonic()
        if not force and now - self._last < self.wctx.control_every_s:
            return self.state
        self._last = now
        with ops_session(self.wctx.factory) as s:
            job = s.get(Job, self.job_id)
            if job is not None:
                job.heartbeat_at = utcnow()
                self.state.cancel = bool(job.cancel_requested)
            ops.heartbeat_worker(s, self.wctx.worker_id, self.wctx.host, self.wctx.capabilities, self.job_id)
            kill = ops.kill_switch_state(s, self.user_id)
            self.state.kill = "global" if kill["global"] else ("user" if kill["user"] else None)
            self.state.limits = Limits.from_row(ops.risk_limits_for(s, self.user_id))
            if self.lease_key_id is not None and not ops.acquire_key_lease(
                s, self.lease_key_id, self.job_id, self.wctx.worker_id, KEY_LEASE_TTL
            ):
                self.state.kill = "key lease lost"
        return self.state


class ops_session:
    """Short-lived committed session (context manager)."""

    def __init__(self, factory: sessionmaker[Session]):
        self.s = factory()

    def __enter__(self) -> Session:
        return self.s

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self.s.commit()
            else:
                self.s.rollback()
        finally:
            self.s.close()


# ---------------------------------------------------------------------------------------------- helpers


def sim_config_from_spec(spec_sim: dict[str, Any] | None, seed: int) -> SimConfig:
    d = dict(spec_sim or {})
    fee: dict[str, Any] = {}
    if d.get("fee_multiplier") is not None:
        fee["multiplier"] = str(d.pop("fee_multiplier"))
    if isinstance(d.get("fee"), dict):
        fee.update(d.pop("fee"))
    if d.get("starting_balance") is not None:
        d["starting_balance"] = D(str(d["starting_balance"]))
    if fee:
        d["fee"] = fee
    d.setdefault("seed", seed)
    return SimConfig.from_dict(d)


def first_rest_market(events: list[Event], ticker: str) -> dict[str, Any] | None:
    for ev in events:
        if ev.kind == "rest" and ev.msg_type == "rest.market" and isinstance(ev.data, dict):
            body = ev.data.get("body")
            mk = body.get("market") if isinstance(body, dict) else None
            if isinstance(mk, dict) and mk.get("ticker") == ticker:
                return mk
    return None


def downsample(points: list[tuple[int, Decimal]], n: int = MAX_EQUITY_POINTS) -> list[list[Any]]:
    if len(points) > n:
        step = len(points) / n
        picked = [points[int(i * step)] for i in range(n - 1)] + [points[-1]]
    else:
        picked = points
    return [[t, str(v)] for t, v in picked]


def max_drawdown(points: list[tuple[int, Decimal]]) -> Decimal:
    peak, mdd = Decimal(0), Decimal(0)
    for _, eq in points:
        peak = max(peak, eq)
        mdd = max(mdd, peak - eq)
    return mdd


def new_run(s: Session, job: Job, mode: str, sim_config: dict[str, Any] | None = None) -> Run:
    bot = (job.spec or {}).get("bot") or {}
    run = Run(
        job_id=job.id,
        user_id=job.user_id,
        bot_id=job.bot_id,
        mode=mode,
        strategy=str(bot.get("strategy") or ""),
        params=dict(bot.get("params") or {}),
        sim_config=sim_config or {},
        windows=list((job.spec or {}).get("windows") or []),
        status="running",
    )
    s.add(run)
    s.flush()
    return run


def make_strategy(job: Job) -> Strategy:
    bot = (job.spec or {}).get("bot") or {}
    return create_strategy(str(bot.get("strategy") or "noop"), dict(bot.get("params") or {}))


def bot_series(job: Job) -> list[str]:
    return list(((job.spec or {}).get("bot") or {}).get("series") or [])


def prune_state(state: MarketState, exchange: Any, now_ms: int, keep_ms: int = 3_600_000) -> int:
    """Forget markets closed over an hour ago with no position or open order (long paper/live jobs)."""
    held = {t for t, p in exchange.positions().items() if p.position != ZERO}
    held |= {o.ticker for o in exchange.open_orders()}
    gone = [
        t for t, m in state.markets.items() if m.close_ms and m.close_ms < now_ms - keep_ms and t not in held
    ]
    for t in gone:
        state.markets.pop(t, None)
        state.tickers.pop(t, None)
        state.last_trade.pop(t, None)
        state.books.books.pop(t, None)
        getattr(exchange, "markets", {}).pop(t, None)
    return len(gone)


def wall_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------------------------- sim


def run_sim(wctx: WorkerContext, job: Job) -> dict[str, Any]:
    spec = job.spec or {}
    window_ids: list[str] = list(spec.get("windows") or [])
    if not window_ids:
        raise JobFailed("sim job has no windows")
    cfg = sim_config_from_spec(spec.get("sim"), seed=job.id)
    control = JobControl(wctx, job)
    with ops_session(wctx.factory) as s:
        run_id = new_run(s, job, "sim", cfg.to_dict()).id
    rec = DbRecorder(wctx.factory, run_id, job.user_id, job.id, "sim")
    total_pnl, total_fees, n_orders, n_fills = ZERO, ZERO, 0, 0
    curve: list[tuple[int, Decimal]] = []
    per_window: list[dict[str, Any]] = []
    status, error = "succeeded", None
    try:
        for i, wid in enumerate(window_ids):
            if control.poll(force=i == 0).cancel:
                raise JobCanceled()
            try:
                window = wctx.source.get_window(wid)
                events = wctx.source.load_events(wid)
            except Exception as exc:
                rec.on_log("error", "window_unavailable", f"{wid}: {exc}", 0)
                per_window.append({"window_id": wid, "error": str(exc)[:300]})
                continue
            ticker = window.get("market_ticker")
            state = MarketState()
            ex = SimExchange(cfg, state.books)
            runner = SimRunner(
                make_strategy(job), ex, state, recorder=rec, tickers=[ticker] if ticker else None
            )
            mk = first_rest_market(events, ticker) if ticker else None
            if mk:
                runner.register_market(mk)
            for n, ev in enumerate(events):
                runner.step(ev)
                if n % 2000 == 0 and control.poll().cancel:
                    raise JobCanceled()
            end_ms = int(window.get("end_ms") or (events[-1].ts_ms if events else 0))
            if not runner.stopped:
                runner.stop(end_ms)
            summary = runner.finish(end_ms)
            pnl = D(summary.get("equity_end", "0"))
            offset = total_pnl
            curve.extend((t, offset + v) for t, v in runner.equity)
            total_pnl += pnl
            total_fees += D(summary.get("fees", "0"))
            n_orders += int(summary.get("n_orders", 0))
            n_fills += int(summary.get("n_fills", 0))
            rec.write_positions(ex.positions())
            rec.write_metrics(
                [
                    (wid, "pnl", pnl),
                    (wid, "fees", D(summary.get("fees", "0"))),
                    (wid, "n_fills", Decimal(int(summary.get("n_fills", 0)))),
                    (wid, "max_drawdown", D(summary.get("max_drawdown", "0"))),
                ]
            )
            per_window.append(
                {
                    "window_id": wid,
                    "ticker": ticker,
                    "result": window.get("result"),
                    **summary,
                    "pnl": str(pnl),
                }
            )
            rec.flush()
    except JobCanceled:
        status = "canceled"
    except Exception as exc:
        status, error = "failed", repr(exc)[:2000]
        raise
    finally:
        out = {
            "pnl": str(total_pnl),
            "fees": str(total_fees),
            "n_orders": n_orders,
            "n_fills": n_fills,
            "n_windows": len(per_window),
            "max_drawdown": str(max_drawdown(curve)),
            "windows": per_window[:500],
            "equity_curve": downsample(curve),
        }
        if error:
            out["error"] = error
        rec.update_run(status=status, finished_at=utcnow(), summary=out)
        rec.close()
    if status == "canceled":
        raise JobCanceled()
    return {"run_id": run_id, **{k: out[k] for k in ("pnl", "n_fills", "n_windows")}}


# ---------------------------------------------------------------------------------------- paper / live loop


@dataclass
class LoopTimers:
    summary: float = 0.0
    reconcile: float = 0.0
    balance: float = 0.0
    prune: float = 0.0


async def _drive(
    wctx: WorkerContext,
    runner: SimRunner,
    feed: EventFeed,
    control: JobControl,
    guard: RiskGuard,
    rec: DbRecorder,
    *,
    live: LiveExchange | None = None,
    max_events: int | None = None,
) -> str:
    """Main loop for paper/live. Returns the stop reason."""
    timers = LoopTimers()
    seen = 0
    reason = ""
    while True:
        st = control.poll()
        guard.limits = st.limits
        guard.killed = st.kill
        if st.cancel:
            reason = "stop requested"
            break
        if st.kill:
            reason = f"kill switch ({st.kill})"
            break
        ev = await feed.get(0.25)
        if ev is not None:
            runner.step(ev)
            seen += 1
        else:
            runner.tick(wall_ms())
        if live is not None:
            await live.pump()
            runner.dispatch(live.flush(runner.now_ms))
        now = time.monotonic()
        if live is not None and now - timers.reconcile >= wctx.reconcile_every_s:
            timers.reconcile = now
            await live.reconcile()
            runner.dispatch(live.flush(runner.now_ms))
        if live is not None and now - timers.balance >= wctx.balance_every_s:
            timers.balance = now
            await live.refresh_balance()
        if now - timers.summary >= wctx.summary_every_s:
            timers.summary = now
            _write_progress(runner, rec, feed)
        if now - timers.prune >= 600:
            timers.prune = now
            prune_state(runner.state, runner.exchange, runner.now_ms)
            if len(runner.equity) > 4 * MAX_EQUITY_POINTS:
                runner.equity[:] = [tuple(p) for p in _thin(runner.equity)]
        if max_events is not None and seen >= max_events and ev is None:
            reason = "feed exhausted"
            break
    return reason


def _thin(points: list[tuple[int, Decimal]]) -> list[tuple[int, Decimal]]:
    return [(t, D(v)) for t, v in downsample(points, 2 * MAX_EQUITY_POINTS)]


def _write_progress(runner: SimRunner, rec: DbRecorder, feed: EventFeed) -> None:
    summary = runner.summary()
    summary["equity_curve"] = downsample(runner.equity)
    summary["feed_dropped"] = getattr(feed, "dropped", 0)
    summary["updated_ms"] = wall_ms()
    rec.write_positions(runner.exchange.positions())
    rec.update_run(summary=summary)
    rec.flush()


async def _shutdown(runner: SimRunner, rec: DbRecorder, live: LiveExchange | None, reason: str) -> None:
    rec.on_log("info", "stopping", reason, runner.now_ms)
    runner.stop(wall_ms() if live is not None else None)
    if live is not None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            await live.pump()
            runner.dispatch(live.flush(runner.now_ms))
            if not live.open_orders() and not live.has_work():
                break
            await asyncio.sleep(0.5)
            await live.reconcile()
        left = live.open_orders()
        if left:
            rec.on_log(
                "error",
                "orders_left_open",
                f"{len(left)} orders still open after stop: "
                + ", ".join(o.order_id or o.client_order_id for o in left),
                runner.now_ms,
            )


def _finalize(
    runner: SimRunner, rec: DbRecorder, status: str, reason: str, extra: dict[str, Any] | None = None
) -> None:
    summary = runner.finish(runner.now_ms)
    summary["equity_curve"] = downsample(runner.equity)
    summary["stop_reason"] = reason
    summary.update(extra or {})
    rec.write_positions(runner.exchange.positions())
    rec.update_run(status=status, finished_at=utcnow(), summary=summary)
    rec.close()


async def run_paper(wctx: WorkerContext, job: Job, *, max_events: int | None = None) -> dict[str, Any]:
    if wctx.feed_factory is None:
        raise JobFailed("this worker has no market-data feed configured (KMB_NATS_URL)")
    cfg = sim_config_from_spec((job.spec or {}).get("sim"), seed=job.id)
    control = JobControl(wctx, job)
    st = control.poll(force=True)
    with ops_session(wctx.factory) as s:
        run_id = new_run(s, job, "paper", cfg.to_dict()).id
    rec = DbRecorder(wctx.factory, run_id, job.user_id, job.id, "paper", batch=200, sim_clock=False)
    guard = RiskGuard(st.limits)
    state = MarketState()
    ex = SimExchange(cfg, state.books)
    runner = SimRunner(
        make_strategy(job), ex, state, recorder=rec, guard=guard, series=bot_series(job) or None
    )
    feed = wctx.feed_factory()
    await feed.start()
    status, reason = "succeeded", ""
    try:
        reason = await _drive(wctx, runner, feed, control, guard, rec, max_events=max_events)
        status = "canceled" if reason == "stop requested" or reason.startswith("kill") else "succeeded"
        await _shutdown(runner, rec, None, reason)
    except Exception as exc:
        status, reason = "failed", repr(exc)[:500]
        raise
    finally:
        await feed.close()
        _finalize(runner, rec, status, reason)
    if status == "canceled":
        raise JobCanceled(reason)
    return {"run_id": run_id, "stop_reason": reason}


# ---------------------------------------------------------------------------------------------- live


def _today_baseline(s: Session, user_id: int) -> Decimal:
    """Realised live P&L already booked today (UTC): settlements minus fees. Approximate by design."""
    start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    pnl = s.scalar(
        select(func.coalesce(func.sum(Settlement.pnl), 0)).where(
            Settlement.user_id == user_id, Settlement.mode == "live", Settlement.settled_at >= start
        )
    )
    fees = s.scalar(
        select(func.coalesce(func.sum(FillRow.fee), 0)).where(
            FillRow.user_id == user_id, FillRow.mode == "live", FillRow.ts_ms >= int(start.timestamp() * 1000)
        )
    )
    return D(str(pnl or 0)) - D(str(fees or 0))


def _load_key(s: Session, wctx: WorkerContext, job: Job) -> tuple[KalshiKey, KalshiAuth]:
    from kmb.backend.security import key_aad

    key_id = ((job.spec or {}).get("bot") or {}).get("key_id")
    key = s.get(KalshiKey, key_id) if key_id else None
    if key is None or key.user_id != job.user_id or not key.is_active:
        raise JobFailed("bot has no active Kalshi key")
    if wctx.vault is None:
        raise JobFailed("worker has no vault key (KMB_VAULT_KEY_FILE)")
    pem = wctx.vault.decrypt(key.private_key_enc, key_aad(key.user_id, key.api_key_id))
    return key, KalshiAuth.from_pem(key.api_key_id, pem)


def _check_live_gates(s: Session, job: Job) -> None:
    user = s.get(User, job.user_id)
    if user is None or not user.is_active or not user.live_enabled:
        raise JobFailed("live trading is not enabled for this user")
    if not ops.live_trading_enabled(s):
        raise JobFailed("live trading is globally disabled")
    kill = ops.kill_switch_state(s, job.user_id)
    if kill["global"] or kill["user"]:
        raise JobFailed("a kill switch is engaged")


async def _reconcile_previous(s: Session, live: LiveExchange, rec: DbRecorder, user_id: int) -> int:
    """Orders left open by earlier live jobs (crash, restart) are canceled; unknown intents are marked."""
    rows = s.scalars(
        select(Order).where(
            Order.user_id == user_id, Order.mode == "live", Order.status.in_(("intent", "pending", "resting"))
        )
    ).all()
    if not rows:
        return 0
    by_coid = {r.client_order_id: r for r in rows}
    resting: dict[str, dict[str, Any]] = {}
    body = await live.rest.get_orders(status="resting")
    for o in body.get("orders") or []:
        if o.get("client_order_id") in by_coid:
            resting[o["client_order_id"]] = o
    n = 0
    for coid, row in by_coid.items():
        raw = resting.get(coid)
        if raw is not None:
            resp = await live.rest.cancel_order(
                str(raw.get("order_id")), str(raw.get("ticker") or row.ticker)
            )
            row.status = "canceled" if resp.ok or resp.status == 404 else "unknown"
            row.order_id = row.order_id or str(raw.get("order_id"))
        else:
            row.status = "unknown" if row.status in ("intent", "pending") else "canceled"
        row.error = (row.error or "") + " [reconciled at live job start]"
        s.add(OrderEvent(order_pk=row.id, event="reconciled", payload={"status": row.status}))
        n += 1
    return n


def _default_rest(env: KalshiEnvironment, auth: KalshiAuth) -> KalshiRestClient:
    return KalshiRestClient(env.rest_base_url, auth, timeout=5.0, max_retries=3)


def _default_private_ws(
    env: KalshiEnvironment, auth: KalshiAuth, handler: Callable[[dict[str, Any]], None]
) -> Any:
    from kmb.kalshi.ws import KalshiWsClient, Subscription

    async def on_message(data: dict[str, Any], _frame: Any) -> None:
        handler(data)

    client = KalshiWsClient(env.ws_url, auth, on_message=on_message)
    asyncio.get_running_loop().create_task(client.subscribe(Subscription("private", ["user_orders", "fill"])))
    return client


async def run_live(wctx: WorkerContext, job: Job, *, max_events: int | None = None) -> dict[str, Any]:
    if wctx.feed_factory is None:
        raise JobFailed("this worker has no market-data feed configured (KMB_NATS_URL)")
    with ops_session(wctx.factory) as s:
        _check_live_gates(s, job)
        key, auth = _load_key(s, wctx, job)
        if not ops.acquire_key_lease(s, key.id, job.id, wctx.worker_id, KEY_LEASE_TTL):
            raise JobFailed("the Kalshi key is in use by another job")
        key.last_used_at = utcnow()
        env = KalshiEnvironment.from_name(key.environment)
        key_pk = key.id
        baseline = _today_baseline(s, job.user_id)
        run_id = new_run(s, job, "live").id
    control = JobControl(wctx, job, lease_key_id=key_pk)
    rest = (wctx.rest_factory or _default_rest)(env, auth)
    rec = DbRecorder(wctx.factory, run_id, job.user_id, job.id, "live", batch=1, sim_clock=False)
    state = MarketState()
    live = LiveExchange(rest, state.books, before_send=rec.flush)
    guard = RiskGuard(control.poll(force=True).limits, baseline_pnl=baseline)
    runner: SimRunner | None = None
    ws = None
    ws_task = None
    feed = None
    status, reason = "succeeded", ""
    try:
        bal = await live.refresh_balance()
        if bal is None:
            raise JobFailed("could not read the Kalshi balance (check the API key)")
        live.portfolio.cash = bal
        with ops_session(wctx.factory) as s:
            n = await _reconcile_previous(s, live, rec, job.user_id)
            if n:
                ops.log_event(
                    s,
                    "warn",
                    "reconciled",
                    f"{n} orders from earlier live jobs reconciled",
                    user_id=job.user_id,
                    job_id=job.id,
                    run_id=run_id,
                )
        ws = (wctx.private_ws_factory or _default_private_ws)(env, auth, live.on_ws_message)
        ws_task = asyncio.get_running_loop().create_task(ws.run_forever())
        runner = SimRunner(
            make_strategy(job), live, state, recorder=rec, guard=guard, series=bot_series(job) or None
        )
        rec.on_log("info", "live_start", f"live job started; balance {bal}", wall_ms())
        feed = wctx.feed_factory()
        await feed.start()
        reason = await _drive(wctx, runner, feed, control, guard, rec, live=live, max_events=max_events)
        status = "canceled" if reason == "stop requested" or reason.startswith("kill") else "succeeded"
        await _shutdown(runner, rec, live, reason)
    except Exception as exc:
        status, reason = "failed", repr(exc)[:500]
        if runner is not None:
            try:
                await _shutdown(runner, rec, live, f"error: {reason}")
            except Exception:
                log.exception("emergency cancel failed")
        raise
    finally:
        if feed is not None:
            await feed.close()
        if ws is not None:
            ws.stop()
        if ws_task is not None:
            ws_task.cancel()
            try:
                await ws_task
            except (asyncio.CancelledError, Exception):
                pass
        if runner is not None:
            _finalize(runner, rec, status, reason, {"exchange_balance": str(live.exchange_balance)})
        else:
            rec.update_run(status=status, finished_at=utcnow(), summary={"stop_reason": reason})
            rec.close()
        with ops_session(wctx.factory) as s:
            ops.release_key_lease(s, key_pk, job.id)
        await rest.aclose()
    if status == "canceled":
        raise JobCanceled(reason)
    return {"run_id": run_id, "stop_reason": reason}


# ---------------------------------------------------------------------------------------------- calibration


class ScriptedStrategy(Strategy):
    """Re-issues a recorded order sequence (same client_order_ids, prices, counts, times) for calibration."""

    name = "scripted"

    def __init__(self, places: list[tuple[int, OrderRequest]], cancels: list[tuple[int, str]]):
        super().__init__({})
        self.places = sorted(places, key=lambda x: x[0])
        self.cancels = sorted(cancels, key=lambda x: x[0])

    def on_event(self, ctx: Context, ev: Event) -> list[Intent]:
        out: list[Intent] = []
        while self.places and self.places[0][0] <= ctx.now_ms:
            out.append(Place(self.places.pop(0)[1], tag="calibration"))
        if self.cancels and self.cancels[0][0] <= ctx.now_ms:
            open_by_coid = {o.client_order_id: o for o in ctx.open_orders}
            while self.cancels and self.cancels[0][0] <= ctx.now_ms:
                coid = self.cancels.pop(0)[1]
                o = open_by_coid.get(coid)
                if o is not None:
                    out.append(Cancel(o.order_id, o.ticker))
        return out


def run_calibration(wctx: WorkerContext, job: Job) -> dict[str, Any]:
    from kmb.fidelity.g4_calibration import compare_fills

    spec = job.spec or {}
    live_run_id = spec.get("live_run_id")
    with ops_session(wctx.factory) as s:
        run = s.get(Run, live_run_id) if live_run_id else None
        if run is None or run.mode != "live" or run.user_id != job.user_id:
            raise JobFailed("calibration needs a live run owned by the job's user")
        orders = s.scalars(select(Order).where(Order.run_id == run.id, Order.sim_ts_ms.is_not(None))).all()
        real_fills = [
            {
                "client_order_id": f.client_order_id,
                "price": str(f.price),
                "count": str(f.count),
                "fee": str(f.fee),
                "is_taker": f.is_taker,
                "ts_ms": f.ts_ms,
            }
            for f in s.scalars(select(FillRow).where(FillRow.run_id == run.id))
        ]
        cancel_rows = s.execute(
            select(Order.client_order_id, OrderEvent.sim_ts_ms)
            .join(OrderEvent, OrderEvent.order_pk == Order.id)
            .where(Order.run_id == run.id, OrderEvent.event == "canceled", OrderEvent.sim_ts_ms.is_not(None))
        ).all()
    by_ticker: dict[str, list[Order]] = {}
    for o in orders:
        if o.status != "rejected":
            by_ticker.setdefault(o.ticker, []).append(o)
    cfg = sim_config_from_spec(spec.get("sim"), seed=job.id)
    sim_fills: list[dict[str, Any]] = []
    windows_used, missing = [], []
    for ticker, rows in by_ticker.items():
        wid = _find_window(wctx.source, ticker)
        if wid is None:
            missing.append(ticker)
            continue
        windows_used.append(wid)
        events = wctx.source.load_events(wid)
        places = [
            (
                int(o.sim_ts_ms or 0),
                OrderRequest(
                    o.ticker,
                    o.side,
                    D(str(o.price)),
                    D(str(o.initial_count)),  # type: ignore[arg-type]
                    time_in_force=o.time_in_force,
                    post_only=o.post_only,  # type: ignore[arg-type]
                    client_order_id=o.client_order_id,
                ),
            )
            for o in rows
        ]
        coids = {o.client_order_id for o in rows}
        cancels = [(int(ts), coid) for coid, ts in cancel_rows if coid in coids]
        state = MarketState()
        ex = SimExchange(cfg, state.books)
        runner = SimRunner(ScriptedStrategy(places, cancels), ex, state, tickers=[ticker])
        mk = first_rest_market(events, ticker)
        if mk:
            runner.register_market(mk)
        runner.run(events)
        sim_fills.extend(f.to_dict() for f in ex.fills())
    report = compare_fills(real_fills, sim_fills)
    report.update(
        {
            "live_run_id": live_run_id,
            "windows": windows_used,
            "missing_windows": missing,
            "sim_config": cfg.to_dict(),
        }
    )
    with ops_session(wctx.factory) as s:
        s.add(
            FidelityReport(
                window_id=None, gate="G4", passed=bool(report["passed"]) and not missing, metrics=report
            )
        )
        ops.log_event(
            s,
            "info",
            "calibration",
            f"G4 calibration of run {live_run_id}: "
            f"{report['n_match']} match, {report['n_mismatch']} mismatch",
            user_id=job.user_id,
            job_id=job.id,
            detail={k: report[k] for k in ("n_real", "n_simulated")},
        )
    return {
        "live_run_id": live_run_id,
        "passed": report["passed"],
        "n_match": report["n_match"],
        "n_mismatch": report["n_mismatch"],
        "missing_windows": missing,
    }


def _find_window(source: WindowSource, ticker: str) -> str | None:
    series = ticker.split("-", 1)[0]
    for w in source.list_windows(series=series, limit=500):
        if w.get("market_ticker") == ticker:
            return str(w["window_id"])
    return None
