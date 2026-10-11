import json
from decimal import Decimal

import pytest

from kmb.book import BookManager, OrderBook
from kmb.envelope import Sequencer, extract_fields, make_envelope, subject_for
from kmb.exchange import OrderRequest
from kmb.kalshi.messages import D, PriceGrid, fmt_count, fmt_price

T = "KXBTC15M-26JAN011200-00"


def snap(seq=1, sid=1, yes=(("0.4000", "10.00"),), no=(("0.5500", "5.00"),)):
    return {
        "type": "orderbook_snapshot",
        "sid": sid,
        "seq": seq,
        "msg": {
            "market_ticker": T,
            "yes_dollars_fp": [list(x) for x in yes],
            "no_dollars_fp": [list(x) for x in no],
        },
    }


def delta(seq, price, d, side="yes", sid=1):
    return {
        "type": "orderbook_delta",
        "sid": sid,
        "seq": seq,
        "msg": {"market_ticker": T, "price_dollars": price, "delta_fp": d, "side": side},
    }


def test_book_snapshot_and_queries():
    b = OrderBook(T)
    b.apply_snapshot(snap()["msg"])
    assert b.best_yes_bid() == (Decimal("0.4000"), Decimal("10.00"))
    assert b.best_yes_ask() == (Decimal("0.4500"), Decimal("5.00"))
    assert b.spread() == Decimal("0.05")
    assert b.mid() == Decimal("0.425")
    assert b.yes_asks() == [(Decimal("0.4500"), Decimal("5.00"))]


def test_book_missing_side_is_empty():
    b = OrderBook(T)
    b.apply_snapshot({"market_ticker": T, "yes_dollars_fp": [["0.10", "1"]]})
    assert b.best_yes_ask() is None
    assert b.mid() is None


def test_manager_delta_and_removal():
    m = BookManager()
    m.on_message(snap())
    assert m.on_message(delta(2, "0.4100", "3.00")) == T
    assert m.book(T).best_yes_bid() == (Decimal("0.4100"), Decimal("3.00"))
    m.on_message(delta(3, "0.4100", "-3.00"))
    assert m.book(T).best_yes_bid()[0] == Decimal("0.4000")


def test_manager_gap_invalidates_until_snapshot():
    m = BookManager()
    m.on_message(snap())
    assert m.on_message(delta(5, "0.4100", "3.00")) is None
    assert not m.book(T).valid
    assert m.gaps == 1
    m.on_message(snap(seq=6))
    assert m.book(T).valid
    assert m.on_message(delta(7, "0.4100", "1.00")) == T


def test_negative_size_invalidates():
    b = OrderBook(T)
    b.apply_snapshot(snap()["msg"])
    b.apply_delta({"price_dollars": "0.4000", "delta_fp": "-11.00", "side": "yes"})
    assert not b.valid


def test_decimal_rejects_float():
    with pytest.raises(TypeError):
        D(0.1)
    assert fmt_price(Decimal("0.5")) == "0.5000"
    assert fmt_count(Decimal("3")) == "3.00"


def test_price_grid_tapered():
    g = PriceGrid.from_structure("tapered_deci_cent")
    assert g.is_valid(Decimal("0.005"))
    assert g.is_valid(Decimal("0.95"))
    assert g.is_valid(Decimal("0.955"))
    assert g.is_valid(Decimal("0.50"))
    assert not g.is_valid(Decimal("0.505"))
    assert not g.is_valid(Decimal("0"))
    assert not g.is_valid(Decimal("1"))
    assert g.snap(Decimal("0.507"), "down") == Decimal("0.50")
    assert g.snap(Decimal("0.507"), "up") == Decimal("0.51")


def test_price_grid_from_ranges():
    g = PriceGrid.from_market({"price_ranges": [{"start": "0.00", "end": "1.00", "step": "0.01"}]})
    assert g.is_valid(Decimal("0.37"))
    assert not g.is_valid(Decimal("0.375"))


def test_order_request_payload():
    r = OrderRequest(ticker=T, side="bid", price=Decimal("0.42"), count=Decimal("2"), client_order_id="c1")
    p = r.to_payload()
    assert p["price"] == "0.4200"
    assert p["count"] == "2.00"
    assert p["side"] == "bid"
    assert "expiration_time" not in p


def test_envelope_and_subjects():
    seq = Sequencer()
    raw = json.dumps(snap())
    env = make_envelope(seq, capture_id="c", stream="kalshi_ws", kind="frame", source="kalshi", raw=raw)
    assert env["market_ticker"] == T
    assert env["msg_type"] == "orderbook_snapshot"
    assert env["sid"] == 1 and env["seq"] == 1
    assert env["stream_seq"] == 0 and env["local_event_seq"] == 0
    assert subject_for(env) == f"md.kalshi.{T}"
    cfb = make_envelope(
        seq,
        capture_id="c",
        stream="kalshi_ws",
        kind="frame",
        source="kalshi",
        raw=json.dumps({"type": "cfbenchmarks_value", "msg": {"index_id": "BRTI"}}),
    )
    assert subject_for(cfb) == "md.cfb.BRTI"
    cb = make_envelope(
        seq,
        capture_id="c",
        stream="coinbase_ws",
        kind="frame",
        source="coinbase",
        raw=json.dumps({"type": "match", "product_id": "BTC-USD"}),
    )
    assert cb["stream_seq"] == 0 and cb["local_event_seq"] == 2
    assert subject_for(cb) == "md.coinbase.BTC-USD"
    tr = make_envelope(
        seq, capture_id="c", stream="control", kind="transport", source="collector", data={"x": 1}
    )
    assert subject_for(tr) == "md.control"
    assert extract_fields("not json")["msg_type"] is None
