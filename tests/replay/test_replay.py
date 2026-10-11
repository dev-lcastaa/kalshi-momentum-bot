from __future__ import annotations

import io
import json
from decimal import Decimal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from kmb.exchange import OrderRequest
from kmb.replay.cursor import ReplayCursor
from kmb.replay.events import event_from_record, iter_parquet_events, read_parquet_events
from kmb.replay.runner import SimRunner
from kmb.replay.source import DataApiWindowSource, LocalWindowSource, WindowNotFound
from kmb.replay.state import MarketState
from kmb.replay.view import build_frame, build_overview, window_times
from kmb.sim import SimConfig, SimExchange
from kmb.strategy.registry import create_strategy, describe, validate_params
from tests import synth


@pytest.fixture
def events():
    return [event_from_record(r) for r in synth.make_rows()]


def test_event_from_record_kinds():
    rows = synth.make_rows()
    evs = [event_from_record(r) for r in rows]
    assert evs[0].kind == "transport" and evs[0].data["event"] == "connected"
    rest = next(e for e in evs if e.kind == "rest")
    assert rest.data["body"]["market"]["ticker"] == synth.TICKER
    assert rest.data["meta"]["endpoint"] == "market"
    snap = next(e for e in evs if e.msg_type == "orderbook_snapshot")
    assert snap.data["msg"]["market_ticker"] == synth.TICKER
    assert [e.local_seq for e in evs] == sorted(e.local_seq for e in evs)


def test_market_state_builds_book_refs_and_result(events):
    st = MarketState()
    notes = []
    for ev in events:
        notes.extend(st.apply(ev))
    book = st.books.book(synth.TICKER)
    assert book.valid
    assert book.best_yes_bid()[0] == Decimal("0.45")
    assert book.best_yes_ask()[0] == Decimal("0.47")
    assert book.size_at("yes", Decimal("0.44")) > Decimal("100")  # deltas applied
    m = st.markets[synth.TICKER]
    assert m.open_ms == synth.OPEN_MS and m.close_ms == synth.CLOSE_MS
    assert m.strike == Decimal("97000") and m.result == "yes"
    assert st.reference["BTC-USD"] > Decimal("97000")
    assert st.reference["BRTI"] > Decimal("97000")
    assert st.last_trade[synth.TICKER].price == Decimal("0.47")
    assert st.tickers[synth.TICKER].volume is not None
    kinds = {n.kind for n in notes}
    assert {"transport", "lifecycle"} <= kinds


def test_transport_gap_invalidates_books():
    rows = synth.make_rows(gap_at_ms=synth.OPEN_MS)
    st = MarketState()
    for r in rows:
        ev = event_from_record(r)
        st.apply(ev)
        if ev.kind == "transport" and ev.data.get("event") == "gap":
            assert not st.books.book(synth.TICKER).valid
            break
    else:
        pytest.fail("gap event not found")
    assert st.gaps == 1


def test_cursor_seek_matches_linear_replay(events):
    cur = ReplayCursor(events, checkpoint_every_ms=5000)
    mid = synth.OPEN_MS + 300_000
    cur.advance_to(mid + 200_000)
    cur.seek(mid)  # backwards: restores a checkpoint and re-applies
    fresh = ReplayCursor(events)
    fresh.advance_to(mid)
    assert cur.now_ms == mid
    assert cur.state.books.book(synth.TICKER).to_dict() == fresh.state.books.book(synth.TICKER).to_dict()
    assert cur.state.reference == fresh.state.reference
    cur.seek(10**15)
    assert cur.now_ms == cur.end_ms and cur.done
    cur.seek(0)
    assert cur.now_ms == cur.start_ms


def test_frame_and_overview(events):
    window = {"market_ticker": synth.TICKER, "open_ms": synth.OPEN_MS, "close_ms": synth.CLOSE_MS}
    cur = ReplayCursor(events)
    applied, notes = cur.advance_to(synth.OPEN_MS + 60_000)
    f = build_frame(cur.state, synth.TICKER, applied, notes, ts_ms=cur.now_ms, playing=True, speed=2)
    assert f["type"] == "frame" and f["book"]["valid"]
    assert [Decimal(x) for x in f["book"]["yes_bids"][0]] == [Decimal("0.45"), Decimal("50")]
    assert Decimal(f["book"]["yes_asks"][0][0]) == Decimal("0.47")
    assert f["ref"] and f["coinbase"]
    assert f["trades"] and f["trades"][0]["taker_side"] == "yes"
    ov = build_overview(window, events)
    assert ov["open_ms"] == synth.OPEN_MS and ov["close_ms"] == synth.CLOSE_MS
    assert len(ov["series"]) > 900
    assert ov["trades"] and ov["depth"] and ov["lifecycle"]
    pt = ov["series"][-1]
    assert Decimal(pt["yes_bid"]) == Decimal("0.45") and pt["coinbase"] is not None
    times = window_times(window, events)
    assert times["start_ms"] == events[0].ts_ms


def test_local_window_source(tmp_path):
    meta = synth.write_window(tmp_path)
    src = LocalWindowSource(tmp_path)
    assert [w["window_id"] for w in src.list_windows()] == [synth.TICKER]
    assert src.list_windows(series="KXETH15M") == []
    assert src.list_windows(start="2027-01-01T00:00:00Z") == []
    assert src.get_window(synth.TICKER)["parquet"] == meta["parquet"]
    evs = src.load_events(synth.TICKER)
    assert list(src.iter_events(synth.TICKER)) == evs
    assert len(evs) == meta["event_count"]
    assert src.load_events(synth.TICKER) is evs  # cached
    assert read_parquet_events(str(tmp_path / meta["parquet"]))[0].kind == "transport"
    with pytest.raises(WindowNotFound):
        src.get_window("nope")


def test_http_streamed_events_match_local_recording(tmp_path):
    meta = synth.write_window(tmp_path)
    data = (tmp_path / meta["parquet"]).read_bytes()
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=data))
    source = DataApiWindowSource("http://data-api", "test", transport=transport)
    assert list(source.iter_events(synth.TICKER)) == read_parquet_events(str(tmp_path / meta["parquet"]))
    missing = DataApiWindowSource(
        "http://data-api", "test", transport=httpx.MockTransport(lambda request: httpx.Response(404))
    )
    with pytest.raises(WindowNotFound):
        list(missing.iter_events("missing"))


def test_parquet_reader_is_lazy_and_rejects_out_of_order_events():
    buffer = io.BytesIO()
    rows = [
        {"ord": i, "kind": "control", "recv_wall_ms": i, "data": json.dumps({"event": "clock"})}
        for i in range(10001)
    ]
    rows[-1]["ord"] = 0
    pq.write_table(pa.Table.from_pylist(rows), buffer, row_group_size=4096)
    buffer.seek(0)
    events = iter_parquet_events(buffer)
    assert next(events).local_seq == 0
    for sequence in range(1, 10000):
        assert next(events).local_seq == sequence
    with pytest.raises(ValueError, match="strictly increasing"):
        next(events)


def _run(events, strategy, **cfg):
    state = MarketState()
    ex = SimExchange(SimConfig(**cfg), state.books)
    runner = SimRunner(strategy, ex, state, tickers=[synth.TICKER])
    summary = runner.run(events, end_ms=events[-1].ts_ms)
    return runner, ex, summary


def test_momentum_trades_and_settles(events):
    strat = create_strategy(
        "momentum", {"hold_to_settle": True, "take_profit": "0.50", "stop_loss": "0.50", "max_hold_s": 100000}
    )
    runner, ex, summary = _run(events, strat, latency_ms=50)
    fills = ex.fills()
    assert fills, "rising reference should trigger a YES entry"
    assert fills[0].side == "bid" and fills[0].price == Decimal("0.47")
    assert ex.settlements() and ex.settlements()[0].result == "yes"
    assert Decimal(summary["equity_end"]) > 0  # bought at 0.47, settled at 1
    assert runner.equity and runner.equity[-1][1] == Decimal(summary["equity_end"])


def test_momentum_falling_buys_no():
    evs = [event_from_record(r) for r in synth.make_rows(rising=False, result="no")]
    strat = create_strategy(
        "momentum", {"hold_to_settle": True, "take_profit": "0.50", "stop_loss": "0.50", "max_hold_s": 100000}
    )
    _, ex, summary = _run(evs, strat, latency_ms=50)
    assert ex.fills() and ex.fills()[0].side == "ask"
    assert Decimal(summary["equity_end"]) > 0


def test_noop_and_guard(events):
    _, ex, summary = _run(events, create_strategy("noop"))
    assert not ex.orders() and Decimal(summary["equity_end"]) == 0

    state = MarketState()
    ex2 = SimExchange(SimConfig(), state.books)
    rejected = []

    class Rec:
        def on_submit(self, order, tag, now_ms):
            rejected.append(order)

        def on_notification(self, note, now_ms):
            pass

        def on_log(self, level, code, message, now_ms):
            pass

    runner = SimRunner(
        create_strategy("momentum"), ex2, state, recorder=Rec(), guard=lambda req, ctx: "blocked"
    )
    runner.run(events)
    assert rejected and all(o.status == "rejected" and o.reject_code == "RISK_LIMIT" for o in rejected)
    assert not ex2.orders()


def test_runner_requires_shared_books():
    with pytest.raises(ValueError):
        SimRunner(create_strategy("noop"), SimExchange(SimConfig(), MarketState().books), MarketState())


def test_registry_validation():
    names = {s["name"] for s in describe()}
    assert {"momentum", "noop"} <= names
    out = validate_params("momentum", {"lookback_s": "5", "size": 2, "hold_to_settle": True})
    assert out == {"lookback_s": 5.0, "size": "2", "hold_to_settle": True}
    for bad in (
        {"nope": 1},
        {"lookback_s": 0},
        {"lookback_s": "x"},
        {"hold_to_settle": "yes"},
        {"size": "NaN"},
    ):
        with pytest.raises(ValueError):
            validate_params("momentum", bad)
    with pytest.raises(ValueError):
        validate_params("missing", {})
    assert OrderRequest  # imported for type parity with strategies
