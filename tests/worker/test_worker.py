from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from sqlalchemy import select

from kmb.backend.security import Vault, key_aad
from kmb.book import BookManager
from kmb.db import init_db, make_engine, ops, session_factory
from kmb.db.models import (
    BotEvent,
    FidelityReport,
    FillRow,
    Job,
    KalshiKey,
    KeyLease,
    Order,
    RiskLimit,
    Run,
    RunMetric,
    Settlement,
    User,
)
from kmb.exchange import OrderRequest, Position
from kmb.kalshi.messages import D
from kmb.kalshi.rest import RestResponse
from kmb.replay.events import event_from_record
from kmb.replay.source import LocalWindowSource
from kmb.strategy.base import Context
from kmb.worker.feed import ListFeed
from kmb.worker.guard import Limits, RiskGuard
from kmb.worker.jobs import WorkerContext, downsample
from kmb.worker.live import LiveExchange
from kmb.worker.main import process_one
from tests import synth

MOMENTUM = {"hold_to_settle": True, "take_profit": "0.50", "stop_loss": "0.50", "max_hold_s": 100000}


# ---------------------------------------------------------------------------------------------- fixtures


@pytest.fixture
def env(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path / 'db.sqlite'}")
    init_db(engine)
    factory = session_factory(engine)
    with factory() as s:
        s.add(User(username="bob", password_hash="x", role="user", live_enabled=True))
        s.commit()
    windows = tmp_path / "derived"
    synth.write_window(windows)
    return factory, windows


def _wctx(factory, windows, **kw) -> WorkerContext:
    kw.setdefault("source_factory", lambda: LocalWindowSource(windows))
    return WorkerContext(factory=factory, worker_id="w-test", control_every_s=0.05, summary_every_s=1.0, **kw)


def _job(factory, kind: str, spec: dict[str, Any], **kw) -> int:
    with factory() as s:
        job = Job(user_id=1, kind=kind, spec=spec, **kw)
        s.add(job)
        s.commit()
        return job.id


def _bot(**kw) -> dict[str, Any]:
    return {"id": None, "name": "b", "strategy": "momentum", "params": MOMENTUM, "series": ["KXBTC15M"], **kw}


def _events() -> list:
    return [event_from_record(r) for r in synth.make_rows()]


# ---------------------------------------------------------------------------------------------- sim


async def test_sim_job_end_to_end(env):
    factory, windows = env
    job_id = _job(
        factory,
        "sim",
        {
            "bot": _bot(),
            "windows": [synth.TICKER, "missing-window"],
            "sim": {"latency_ms": 50, "fee_multiplier": 1.0, "starting_balance": 500},
        },
    )
    assert await process_one(_wctx(factory, windows), ("sim",))
    with factory() as s:
        job = s.get(Job, job_id)
        assert job.status == "succeeded", job.error
        assert job.spec["result"]["n_fills"] >= 1
        run = s.scalar(select(Run).where(Run.job_id == job_id))
        assert run.mode == "sim" and run.status == "succeeded"
        assert run.sim_config["starting_balance"] == "500" and run.sim_config["latency_ms"] == 50
        assert D(run.summary["pnl"]) > 0
        assert run.summary["equity_curve"] and run.summary["equity_curve"][-1][1] == run.summary["pnl"]
        assert [w.get("error") is not None for w in run.summary["windows"]] == [False, True]
        orders = s.scalars(select(Order).where(Order.run_id == run.id)).all()
        assert (
            orders
            and orders[0].sim_ts_ms is not None
            and orders[0].status in ("executed", "canceled", "resting")
        )
        assert s.scalars(select(FillRow).where(FillRow.run_id == run.id)).all()
        assert s.scalars(select(Settlement).where(Settlement.run_id == run.id)).one().result == "yes"
        names = {m.name for m in s.scalars(select(RunMetric).where(RunMetric.run_id == run.id))}
        assert names == {"pnl", "fees", "n_fills", "max_drawdown"}
        assert s.scalar(select(BotEvent).where(BotEvent.code == "window_unavailable"))


async def test_sim_never_materializes_recording(env, monkeypatch):
    factory, windows = env

    def reject_loading(*args):
        raise AssertionError("simulation must stream events instead of loading the whole recording")

    monkeypatch.setattr(LocalWindowSource, "load_events", reject_loading)
    job_id = _job(factory, "sim", {"bot": _bot(), "windows": [synth.TICKER]})
    assert await process_one(_wctx(factory, windows), ("sim",))
    with factory() as session:
        job = session.get(Job, job_id)
        assert job.status == "succeeded", job.error
        run = session.scalar(select(Run).where(Run.job_id == job_id))
        assert D(run.summary["pnl"]) > 0


async def test_sim_stream_failure_is_not_reported_as_success(env, monkeypatch):
    factory, windows = env
    original = LocalWindowSource.iter_events

    def failing_events(self, window_id):
        yield from original(self, window_id)
        raise ValueError("invalid replay order")

    monkeypatch.setattr(LocalWindowSource, "iter_events", failing_events)
    job_id = _job(factory, "sim", {"bot": _bot(), "windows": [synth.TICKER]})
    assert await process_one(_wctx(factory, windows), ("sim",))
    with factory() as session:
        job = session.get(Job, job_id)
        assert job.status == "failed"
        assert "invalid replay order" in job.error
        assert session.scalar(select(Run).where(Run.job_id == job_id)).status == "failed"


async def test_sim_job_cancel_requested(env):
    factory, windows = env
    job_id = _job(factory, "sim", {"bot": _bot(), "windows": [synth.TICKER]}, cancel_requested=True)
    # a queued job with cancel_requested is canceled at claim time
    assert not await process_one(_wctx(factory, windows), ("sim",))
    with factory() as s:
        assert s.get(Job, job_id).status == "canceled"


async def test_sim_job_canceled_while_running(env, monkeypatch):
    factory, windows = env
    job_id = _job(factory, "sim", {"bot": _bot(), "windows": [synth.TICKER, synth.TICKER]})
    from kmb.worker import jobs

    real_poll = jobs.JobControl.poll

    def poll(self, force=False):
        with factory() as s:
            s.get(Job, job_id).cancel_requested = True
            s.commit()
        return real_poll(self, force)

    monkeypatch.setattr(jobs.JobControl, "poll", poll)
    await process_one(_wctx(factory, windows), ("sim",))
    with factory() as s:
        assert s.get(Job, job_id).status == "canceled"
        assert s.scalar(select(Run).where(Run.job_id == job_id)).status == "canceled"


async def test_sim_job_without_windows_fails(env):
    factory, windows = env
    job_id = _job(factory, "sim", {"bot": _bot(), "windows": []})
    await process_one(_wctx(factory, windows), ("sim",))
    with factory() as s:
        job = s.get(Job, job_id)
        assert job.status == "failed" and "no windows" in job.error


def test_downsample():
    pts = [(i, Decimal(i)) for i in range(10_000)]
    out = downsample(pts, 100)
    assert len(out) == 100 and out[0] == [0, "0"] and out[-1] == [9999, "9999"]


# ---------------------------------------------------------------------------------------------- paper


async def test_paper_job_on_feed(env):
    factory, windows = env
    events = _events()
    job_id = _job(factory, "paper", {"bot": _bot(), "sim": {"latency_ms": 50}})
    from kmb.worker import jobs

    wctx = _wctx(factory, windows, feed_factory=lambda: ListFeed(events))
    with factory() as s:
        job = s.get(Job, job_id)
        s.expunge(job)
    out = await jobs.run_paper(wctx, job, max_events=len(events))
    with factory() as s:
        run = s.get(Run, out["run_id"])
        assert run.mode == "paper" and run.status == "succeeded"
        assert run.summary["stop_reason"] == "feed exhausted"
        assert int(run.summary["n_fills"]) >= 1 and run.summary["equity_curve"]
        assert s.scalars(select(FillRow).where(FillRow.run_id == run.id, FillRow.mode == "paper")).all()


async def test_paper_job_kill_switch_stops(env):
    factory, windows = env
    job_id = _job(factory, "paper", {"bot": _bot()})
    with factory() as s:
        ops.set_kill_switch(s, "user", True, None, "test", user_id=1)
        s.commit()
    wctx = _wctx(factory, windows, feed_factory=lambda: ListFeed(_events()))
    await process_one(wctx, ("paper",))
    with factory() as s:
        job = s.get(Job, job_id)
        # set_kill_switch flags running/queued paper jobs for cancellation
        assert job.status == "canceled"


async def test_paper_without_feed_fails(env):
    factory, windows = env
    job_id = _job(factory, "paper", {"bot": _bot()})
    await process_one(_wctx(factory, windows), ("paper",))
    with factory() as s:
        assert "feed" in s.get(Job, job_id).error


# ---------------------------------------------------------------------------------------------- guard


def _ctx(positions=None, open_orders=None, now_ms=0) -> Context:
    return Context(
        now_ms=now_ms, books=BookManager(), positions=positions or {}, open_orders=open_orders or []
    )


def test_risk_guard_limits():
    g = RiskGuard(
        Limits(
            max_order_count=Decimal(5),
            max_position_per_market=Decimal(6),
            max_notional_per_market=Decimal(3),
            max_orders_per_minute=3,
            max_daily_loss=Decimal(10),
        )
    )
    t = synth.TICKER
    assert g(OrderRequest(t, "bid", D("0.5"), D("6")), _ctx()).startswith("count")
    assert g(OrderRequest(t, "bid", D("0.5"), D("0")), _ctx()) == "count must be positive"
    assert g(OrderRequest(t, "bid", D("0.9"), D("4")), _ctx()).startswith("notional")
    pos = {t: Position(t, position=D("4"), cost=D("1"))}
    assert g(OrderRequest(t, "bid", D("0.1"), D("3")), _ctx(pos)).startswith("projected position")
    # reducing exposure is always allowed, even past limits
    assert g(OrderRequest(t, "ask", D("0.5"), D("4")), _ctx(pos)) is None
    assert g(OrderRequest(t, "bid", D("0.1"), D("1")), _ctx()) is None
    assert g(OrderRequest(t, "bid", D("0.1"), D("1")), _ctx()) is None
    assert g(OrderRequest(t, "bid", D("0.1"), D("1")), _ctx()).startswith("order rate")
    assert g(OrderRequest(t, "bid", D("0.1"), D("1")), _ctx(now_ms=61_000)) is None
    loss = {"X": Position("X", realized_pnl=D("-10"))}
    assert g(OrderRequest(t, "bid", D("0.1"), D("1")), _ctx(loss, now_ms=200_000)).startswith("daily loss")
    g.killed = "user"
    assert g(OrderRequest(t, "ask", D("0.5"), D("1")), _ctx(pos)).startswith("kill switch")


def test_limits_from_row():
    lim = Limits.from_row(
        RiskLimit(
            max_order_count=Decimal(3),
            max_position_per_market=Decimal(4),
            max_open_orders=2,
            max_daily_loss=Decimal(5),
            max_notional_per_market=Decimal(6),
            max_orders_per_minute=7,
        )
    )
    assert lim.max_open_orders == 2 and lim.to_dict()["max_daily_loss"] == "5"


# ---------------------------------------------------------------------------------------------- live


def _resp(status: int, body: Any, method: str = "POST", path: str = "/") -> RestResponse:
    text = json.dumps(body) if body is not None else ""
    return RestResponse(method, path, {}, None, status, text, {}, 0, 0, 0, 0, 1)


class FakeKalshi:
    """Minimal Kalshi: takers fill immediately at the limit price (fills pushed via the 'private WS')."""

    def __init__(self, *, fill: bool = True, fail_create: bool = False):
        self.fill = fill
        self.fail_create = fail_create
        self.handler = None
        self.created: list[dict[str, Any]] = []
        self.canceled: list[str] = []
        self.resting: dict[str, dict[str, Any]] = {}
        self.extra_resting: list[dict[str, Any]] = []
        self.fills: list[dict[str, Any]] = []
        self.n = 0
        self.closed = False

    # REST
    async def create_order(self, payload):
        if self.fail_create:
            raise TimeoutError("create timed out")
        self.n += 1
        oid = f"oid-{self.n}"
        self.created.append(payload)
        count = payload["count"]
        if self.fill:
            fill = {
                "trade_id": f"t-{self.n}",
                "order_id": oid,
                "client_order_id": payload["client_order_id"],
                "market_ticker": payload["ticker"],
                "is_taker": True,
                "side": "yes",
                "action": "buy",
                "book_side": payload["side"],
                "yes_price_dollars": payload["price"],
                "count_fp": count,
                "fee_cost": "0.02",
                "ts_ms": int(time.time() * 1000),
            }
            self.fills.append(fill)
            if self.handler:
                self.handler(
                    {
                        "type": "user_order",
                        "msg": {
                            "order_id": oid,
                            "client_order_id": payload["client_order_id"],
                            "ticker": payload["ticker"],
                            "status": "executed",
                            "fill_count_fp": count,
                            "remaining_count_fp": "0.00",
                        },
                    }
                )
                self.handler({"type": "fill", "msg": fill})
            return _resp(
                201,
                {
                    "order_id": oid,
                    "client_order_id": payload["client_order_id"],
                    "fill_count": count,
                    "remaining_count": "0.00",
                    "ts_ms": 1,
                },
            )
        self.resting[oid] = {
            "order_id": oid,
            "client_order_id": payload["client_order_id"],
            "ticker": payload["ticker"],
            "status": "resting",
            "remaining_count_fp": count,
        }
        return _resp(
            201,
            {
                "order_id": oid,
                "client_order_id": payload["client_order_id"],
                "fill_count": "0.00",
                "remaining_count": count,
                "ts_ms": 1,
            },
        )

    async def cancel_order(self, order_id, market_ticker):
        self.canceled.append(order_id)
        if self.resting.pop(order_id, None) is None and not any(
            o["order_id"] == order_id for o in self.extra_resting
        ):
            return _resp(404, {"error": {"code": "not_found", "message": "gone"}}, "DELETE")
        self.extra_resting = [o for o in self.extra_resting if o["order_id"] != order_id]
        return _resp(200, {"order": {"order_id": order_id}}, "DELETE")

    async def get_orders(self, ticker=None, status=None, cursor=None, limit=None):
        return {"orders": list(self.resting.values()) + list(self.extra_resting)}

    async def get_fills(self, ticker=None, order_id=None, min_ts=None, cursor=None, limit=200):
        return {"fills": list(reversed(self.fills))}

    async def get_balance(self):
        return {"balance": 12345}

    async def get_positions(self, ticker=None, cursor=None):
        return {"market_positions": []}

    async def decrease_order(self, order_id, ticker, reduce_to):
        return _resp(200, {})

    async def aclose(self):
        self.closed = True


class FakeWs:
    def __init__(self, fake: FakeKalshi, handler):
        fake.handler = handler
        self._stop = asyncio.Event()

    async def run_forever(self):
        await self._stop.wait()

    def stop(self):
        self._stop.set()


def _pem() -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


def _live_setup(factory) -> tuple[Vault, int]:
    vault = Vault(Vault.generate_key_hex())
    with factory() as s:
        key = KalshiKey(
            user_id=1,
            label="k",
            environment="demo",
            api_key_id="api-1",
            private_key_enc=vault.encrypt(_pem(), key_aad(1, "api-1")),
        )
        s.add(key)
        ops.set_setting(s, ops.LIVE_TRADING, {"enabled": True})
        s.commit()
        return vault, key.id


async def test_live_job_end_to_end_and_calibration(env):
    factory, windows = env
    vault, key_id = _live_setup(factory)
    fake = FakeKalshi()
    # an order left resting by a crashed earlier job must be canceled at start
    with factory() as s:
        s.add(
            Order(
                user_id=1,
                mode="live",
                client_order_id="old-1",
                order_id="oid-old",
                ticker=synth.TICKER,
                side="bid",
                price=D("0.40"),
                initial_count=D("1"),
                time_in_force="good_till_canceled",
                status="resting",
            )
        )
        s.commit()
    fake.extra_resting.append(
        {"order_id": "oid-old", "client_order_id": "old-1", "ticker": synth.TICKER, "status": "resting"}
    )
    events = _events()
    job_id = _job(factory, "live", {"bot": _bot(key_id=key_id)})
    wctx = _wctx(
        factory,
        windows,
        vault=vault,
        feed_factory=lambda: ListFeed(events),
        rest_factory=lambda env_, auth: fake,
        private_ws_factory=lambda env_, auth, h: FakeWs(fake, h),
    )
    from kmb.worker import jobs

    with factory() as s:
        job = s.get(Job, job_id)
        s.expunge(job)
    out = await jobs.run_live(wctx, job, max_events=len(events))
    assert fake.closed and "oid-old" in fake.canceled and fake.created
    with factory() as s:
        run = s.get(Run, out["run_id"])
        assert run.mode == "live" and run.status == "succeeded", run.summary
        assert run.summary["exchange_balance"] == "123.45"
        orders = s.scalars(select(Order).where(Order.run_id == run.id)).all()
        assert orders and all(o.status == "executed" and o.order_id for o in orders)
        fills = s.scalars(select(FillRow).where(FillRow.run_id == run.id)).all()
        assert len(fills) == len(fake.created) and fills[0].mode == "live"
        assert s.scalars(select(Settlement).where(Settlement.run_id == run.id)).one().result == "yes"
        old = s.scalar(select(Order).where(Order.client_order_id == "old-1"))
        assert old.status == "canceled" and "reconciled" in old.error
        assert s.get(KeyLease, key_id) is None  # lease released
        live_run_id = run.id

    cal_id = _job(factory, "calibration", {"live_run_id": live_run_id, "sim": {"latency_ms": 0}})
    assert await process_one(wctx, ("calibration",))
    with factory() as s:
        job = s.get(Job, cal_id)
        assert job.status == "succeeded", job.error
        rep = s.scalar(select(FidelityReport).where(FidelityReport.gate == "G4"))
        assert rep.metrics["live_run_id"] == live_run_id and rep.metrics["missing_windows"] == []
        assert (
            rep.metrics["n_real"] == len(fake.created) and rep.metrics["n_simulated"] == rep.metrics["n_real"]
        )


async def test_live_job_refused_without_gates(env):
    factory, windows = env
    vault, key_id = _live_setup(factory)
    with factory() as s:
        ops.set_setting(s, ops.LIVE_TRADING, {"enabled": False})
        s.commit()
    job_id = _job(factory, "live", {"bot": _bot(key_id=key_id)})
    wctx = _wctx(factory, windows, vault=vault, feed_factory=lambda: ListFeed([]))
    await process_one(wctx, ("live",))
    with factory() as s:
        job = s.get(Job, job_id)
        assert job.status == "failed" and "globally disabled" in job.error


async def test_live_job_refused_when_key_leased(env):
    from datetime import timedelta

    factory, windows = env
    vault, key_id = _live_setup(factory)
    other = _job(factory, "live", {"bot": _bot(key_id=key_id)}, status="running")
    with factory() as s:
        assert ops.acquire_key_lease(s, key_id, other, "w-other", timedelta(minutes=5))
        s.commit()
    job_id = _job(factory, "live", {"bot": _bot(key_id=key_id)})
    await process_one(_wctx(factory, windows, vault=vault, feed_factory=lambda: ListFeed([])), ("live",))
    with factory() as s:
        assert "in use" in s.get(Job, job_id).error


async def test_live_exchange_unknown_create_and_rejects():
    fake = FakeKalshi(fail_create=True)
    clock = [1_000_000]
    books = BookManager()
    live = LiveExchange(fake, books, clock=lambda: clock[0])
    sent = []
    live.before_send = lambda: sent.append(1)
    req = OrderRequest(synth.TICKER, "bid", D("0.47"), D("1"))
    live.submit(req, 0)
    await live.pump()
    assert sent == [1] and live.open_orders()[0].status == "pending" and live.errors == 1
    clock[0] += 31_000
    await live.reconcile()
    o = live.orders()[0]
    assert o.status == "canceled" and o.reject_code == "UNKNOWN"

    # rejected by the exchange
    fake2 = FakeKalshi()

    async def bad_create(payload):
        return _resp(400, {"error": {"code": "invalid_order", "message": "bad price"}})

    fake2.create_order = bad_create
    live2 = LiveExchange(fake2, books)
    live2.submit(OrderRequest(synth.TICKER, "bid", D("0.47"), D("1")), 0)
    await live2.pump()
    o = live2.orders()[0]
    assert o.status == "rejected" and o.reject_code == "invalid_order"

    # cancel before send never reaches the exchange
    fake3 = FakeKalshi()
    live3 = LiveExchange(fake3, books)
    st = live3.submit(OrderRequest(synth.TICKER, "bid", D("0.47"), D("1")), 0)
    live3.request_cancel(st.client_order_id, 0)
    await live3.pump()
    assert not fake3.created and live3.orders()[0].status == "canceled"


async def test_live_exchange_resting_cancel_and_fill_backfill():
    fake = FakeKalshi(fill=False)
    live = LiveExchange(fake, BookManager())
    st = live.submit(OrderRequest(synth.TICKER, "bid", D("0.40"), D("2")), 0)
    await live.pump()
    o = live.open_orders()[0]
    assert o.status == "resting" and o.order_id == "oid-1"
    # a fill the private WS missed is recovered by reconcile (REST /portfolio/fills), only once
    fake.fills.append(
        {
            "trade_id": "t-x",
            "order_id": "oid-1",
            "client_order_id": st.client_order_id,
            "ticker": synth.TICKER,
            "is_taker": False,
            "side": "yes",
            "action": "buy",
            "yes_price_dollars": "0.40",
            "count_fp": "1.00",
            "fee_cost": "0.00",
            "ts_ms": 5,
        }
    )
    fake.fills.append(
        {
            "trade_id": "t-y",
            "order_id": "someone-else",
            "ticker": synth.TICKER,
            "yes_price_dollars": "0.40",
            "count_fp": "1.00",
            "ts_ms": 6,
        }
    )
    await live.reconcile()
    await live.reconcile()
    assert len(live.fills()) == 1 and live.positions()[synth.TICKER].position == D("1")
    live.request_cancel_all(None, 0)
    await live.pump()
    assert fake.canceled == ["oid-1"] and not live.open_orders()
    notes = live.flush(0)
    assert any(getattr(n, "status", None) == "canceled" for n in notes)
