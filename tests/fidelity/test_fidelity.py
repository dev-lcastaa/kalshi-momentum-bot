from kmb.fidelity.events import event_from_row
from kmb.fidelity.g1_book_parity import run_g1
from kmb.fidelity.g2_tape import run_g2
from kmb.strategy.base import Event

T = "TEST"


def ev(ts, msg_type, msg, stream="kalshi_ws", kind="frame", data=None):
    return Event(ts, stream, kind, msg_type, T, data or {"type": msg_type, "msg": msg})


def test_g1_book_snapshot_ticker_and_deliberate_mismatch():
    events = [
        ev(
            1,
            "orderbook_snapshot",
            {"market_ticker": T, "yes_dollars_fp": [["0.40", "2.00"]], "no_dollars_fp": [["0.50", "3.00"]]},
            data={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": T,
                    "yes_dollars_fp": [["0.40", "2.00"]],
                    "no_dollars_fp": [["0.50", "3.00"]],
                },
            },
        ),
        ev(2, "ticker", {"market_ticker": T, "yes_bid_dollars": "0.40", "yes_ask_dollars": "0.50"}),
        ev(3, "ticker", {"market_ticker": T, "yes_bid_dollars": "0.41", "yes_ask_dollars": "0.50"}),
    ]
    report = run_g1(events)
    assert report["n_ticker"] == 2
    assert report["n_ticker_match"] == 1
    assert report["n_ticker"] - report["n_ticker_match"] == 1


def test_g2_tape_decrease_and_missing_delta():
    trade = {
        "market_ticker": T,
        "trade_id": "1",
        "taker_side": "no",
        "yes_price_dollars": "0.40",
        "no_price_dollars": "0.60",
        "count_fp": "2.00",
    }
    delta = {"market_ticker": T, "side": "yes", "price_dollars": "0.40", "delta_fp": "-2.00"}
    report = run_g2([ev(1, "trade", trade), ev(2, "orderbook_delta", delta)])
    assert report["passed"]
    report = run_g2([ev(1, "trade", trade)])
    assert not report["passed"]


def test_rest_row_conversion_keeps_snapshot_and_request_timing():
    event = event_from_row(
        {
            "stream": "kalshi_rest",
            "kind": "rest",
            "msg_type": "rest.orderbook",
            "market_ticker": T,
            "recv_wall_ms": 9,
            "local_event_seq": 4,
            "raw": '{"orderbook_fp":{"yes_dollars":[["0.4","1"]],"no_dollars":[]}}',
            "data": '{"sent_wall_ms":5,"recv_wall_ms":9}',
        }
    )
    assert event.data["orderbook_fp"]["yes_dollars"] == [["0.4", "1"]]
    assert event.envelope_data["sent_wall_ms"] == 5
    assert event.ts_ms == 9


def test_g1_rest_depth_matches_timing_interval_and_reports_mismatch():
    snapshot = {
        "type": "orderbook_snapshot",
        "sid": 7,
        "seq": 1,
        "msg": {
            "market_ticker": T,
            "yes_dollars_fp": [["0.40", "2.00"]],
            "no_dollars_fp": [["0.50", "3.00"]],
        },
    }
    good_rest = {
        "response": {"orderbook_fp": {"yes_dollars": [["0.40", "2.00"]], "no_dollars": [["0.50", "3.00"]]}},
        "_envelope_data": {"sent_wall_ms": 5, "recv_wall_ms": 15},
    }
    bad_rest = {
        "response": {"orderbook_fp": {"yes_dollars": [["0.40", "9.00"]], "no_dollars": []}},
        "_envelope_data": {"sent_wall_ms": 18, "recv_wall_ms": 25},
    }
    report = run_g1(
        [
            Event(10, "kalshi_ws", "frame", "orderbook_snapshot", T, snapshot),
            Event(15, "kalshi_rest", "rest", "rest.orderbook", T, good_rest),
            Event(25, "kalshi_rest", "rest", "rest.orderbook", T, bad_rest),
        ]
    )
    assert report["n_snapshots"] == 2
    assert report["n_exact"] == 1
    assert report["n_mismatch"] == 1
    assert not report["passed"]


def test_g4_accepts_fill_aliases_and_rejects_maker_timing_drift():
    from kmb.fidelity.g4_calibration import compare_fills

    real = [
        {
            "client_order_id": "client",
            "yes_price_dollars": "0.40",
            "count_fp": "1.00",
            "fee_cost": "0.01",
            "is_taker": False,
            "ts_ms": 100,
        }
    ]
    modeled = [
        {
            "client_order_id": "client",
            "price": "0.40",
            "count": "1.00",
            "fee": "0.01",
            "is_taker": False,
            "ts_ms": 200,
        }
    ]
    assert compare_fills(real, modeled, maker_tolerance_ms=100)["passed"]
    modeled[0]["ts_ms"] = 1000
    assert not compare_fills(real, modeled, maker_tolerance_ms=100)["passed"]


def test_g2_does_not_reuse_one_delta_for_multiple_trades():
    trade = {
        "market_ticker": T,
        "taker_side": "no",
        "yes_price_dollars": "0.40",
        "no_price_dollars": "0.60",
        "count_fp": "2.00",
    }
    delta = {"market_ticker": T, "side": "yes", "price_dollars": "0.40", "delta_fp": "-2.00"}
    report = run_g2([ev(1, "trade", trade), ev(2, "trade", trade), ev(3, "orderbook_delta", delta)])
    assert report["n_trades"] == 2
    assert report["n_matched"] == 1
    assert report["n_mismatch"] == 1
