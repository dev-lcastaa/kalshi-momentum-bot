from decimal import Decimal

import pytest

from kmb.book import BookManager
from kmb.exchange import OrderRequest
from kmb.kalshi.messages import PriceGrid
from kmb.sim import FeeModel, SimConfig, SimExchange
from kmb.sim.fees import FeeAccumulator
from kmb.sim.portfolio import Portfolio
from kmb.strategy.base import Event

D = Decimal
T = "TEST"


def make_exchange(*, latency=0, balance="1000", queue="front", fee=None):
    books = BookManager()
    books.on_message(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": T,
                "yes_dollars_fp": [["0.40", "2.00"]],
                "no_dollars_fp": [["0.50", "2.00"]],
            },
        },
        0,
    )
    sim = SimExchange(
        SimConfig(latency_ms=latency, queue_model=queue, starting_balance=D(balance), fee=fee or FeeModel()),
        books,
    )
    sim.set_market(T, grid=PriceGrid.from_structure("deci_cent"))
    return sim, books


def req(side="bid", price="0.60", count="1", tif="immediate_or_cancel", **kwargs):
    return OrderRequest(T, side, D(price), D(count), time_in_force=tif, **kwargs)


def event(kind, msg_type, msg, ts=1):
    return Event(ts, "kalshi_ws", "frame", msg_type, T, {"type": msg_type, "msg": msg})


def test_fee_worked_rounding_and_maker_fee_types():
    fee_model = FeeModel(precision=D("0.01"))
    trade_fee = fee_model.taker_fee(D("0.055"), D("1"))
    assert trade_fee == D("0.003639")
    net, remainder = fee_model.apply_rounding(D("-0.055"), trade_fee)
    assert remainder == D("0.001361")
    assert net == D("0.005000")
    assert FeeModel(fee_type="quadratic").maker_fee(D("0.5"), D("1")) == 0
    assert FeeModel(fee_type="quadratic_with_maker_fees").maker_fee(D("0.5"), D("1")) == D("0.004375")


def test_accumulator_rebate_and_config_roundtrip():
    acc = FeeAccumulator()
    model = FeeModel(precision=D("0.01"))
    assert acc.charge(model, D("-0.055"), D("0.003639")) == D("0.005000")
    assert acc.accumulated_rounding == D("0.001361")
    cfg = SimConfig.from_dict({"starting_balance": "20.10", "unused": 1, "fee": {"multiplier": "2"}})
    assert cfg.starting_balance == D("20.10")
    assert SimConfig.from_dict(cfg.to_dict()).to_dict() == cfg.to_dict()


def test_tick_and_count_validation_and_insufficient_balance():
    sim, _ = make_exchange(balance="0.10")
    assert sim.submit(req(price="0.6001"), 0).reject_code == "INVALID_PRICE"
    assert sim.submit(req(price="0.60", count="0.001"), 0).reject_code == "INVALID_COUNT"
    assert sim.submit(req(price="0.60", count="1"), 0).reject_code == "INSUFFICIENT_BALANCE"


def test_ioc_partial_and_fok_atomicity():
    sim, _ = make_exchange()
    partial = sim.submit(req(count="3"), 0)
    notifications = sim.flush(0)
    assert partial.status == "pending"
    assert sim.orders()[0].fill_count == D("2.00")
    assert sim.orders()[0].status == "canceled"
    assert len([x for x in notifications if hasattr(x, "trade_id")]) == 1
    sim2, _ = make_exchange()
    sim2.submit(req(count="3", tif="fill_or_kill"), 0)
    sim2.flush(0)
    assert not sim2.fills()
    assert sim2.orders()[0].status == "canceled"


def test_gtc_rests_and_marketable_gtc_takes():
    sim, _ = make_exchange()
    sim.submit(req(price="0.45", tif="good_till_canceled"), 0)
    sim.flush(0)
    assert sim.orders()[0].status == "resting"
    sim2, _ = make_exchange()
    sim2.submit(req(price="0.60", count="3", tif="good_till_canceled"), 0)
    sim2.flush(0)
    assert sim2.orders()[0].fill_count == D("2.00")
    assert sim2.orders()[0].status == "resting"


def test_post_only_and_latency_arrival_uses_current_book():
    sim, books = make_exchange(latency=5)
    rejected = sim.submit(req(price="0.60", tif="good_till_canceled", post_only=True), 0)
    assert sim.flush(4) == []
    assert rejected.status == "pending"
    books.on_message(
        {
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 2,
            "msg": {"market_ticker": T, "side": "no", "price_dollars": "0.50", "delta_fp": "-2.00"},
        },
        4,
    )
    sim.before_event(Event(5, "control", "control", None, T, {}))
    assert sim.orders()[0].status == "resting"
    sim2, _ = make_exchange()
    sim2.submit(req(price="0.60", tif="good_till_canceled", post_only=True), 0)
    sim2.flush(0)
    assert sim2.orders()[0].reject_code == "POST_ONLY_CROSS"


def test_footprint_overlay_prevents_double_dip():
    sim, _ = make_exchange()
    sim.submit(req(count="1", client_order_id="same"), 0)
    sim.flush(0)
    sim.submit(req(count="2"), 0)
    sim.flush(0)
    assert sum((f.count for f in sim.fills()), D(0)) == D("2.00")


def test_queue_trade_and_lifecycle_pause_settlement():
    sim, _ = make_exchange()
    sim.submit(req(side="ask", price="0.45", tif="good_till_canceled", cancel_order_on_pause=True), 0)
    sim.flush(0)
    order = sim.orders()[0]
    assert order.status == "resting"
    sim.after_event(event("frame", "market_lifecycle_v2", {"event_type": "deactivated"}))
    assert sim.orders()[0].status == "canceled"
    paused = sim.submit(req(), 2)
    assert paused.reject_code == "MARKET_PAUSED"
    sim.after_event(event("frame", "market_lifecycle_v2", {"event_type": "activated"}, 3))
    sim.after_event(event("frame", "market_lifecycle_v2", {"event_type": "settled", "result": "yes"}, 4))
    assert sim.settlements()[-1].result == "yes"


def test_reduce_only_validation_and_close_cutoff():
    sim, _ = make_exchange()
    assert sim.submit(req(reduce_only=True), 0).reject_code == "REDUCE_ONLY_NO_POSITION"
    sim.set_market(T, close_ms=2)
    assert sim.submit(req(), 2).reject_code == "MARKET_INACTIVE"


def test_signed_position_closes_opposite_contract_and_settles():
    portfolio = Portfolio(D("100"))
    portfolio.apply_fill(T, "ask", D("0.40"), D("2"), D("0"))
    portfolio.apply_fill(T, "bid", D("0.30"), D("1"), D("0"))
    position = portfolio.position(T)
    assert position.position == D("-1")
    assert position.cost == D("0.60")
    assert position.realized_pnl == D("0.10")
    assert portfolio.cash == D("99.50")
    signed, payout, pnl = portfolio.settle(T, "no")
    assert (signed, payout, pnl) == (D("-1"), D("1"), D("0.40"))


def test_deterministic_seeded_notifications():
    def execute():
        sim, _ = make_exchange(latency=5)
        sim.submit(req(count="1", client_order_id="same"), 0)
        sim.flush(10)
        return [x.to_dict() for x in sim._notifications]

    assert execute() == execute()


@pytest.mark.parametrize(
    ("model", "expected_filled"), [("front", D("2")), ("pro_rata", D("1")), ("back", D("0"))]
)
def test_queue_models_vary_fill_progress(model, expected_filled):
    sim, books = make_exchange(queue=model)
    sim.submit(req(price="0.40", count="2", tif="good_till_canceled"), 0)
    sim.flush(0)
    for seq, delta in ((2, "2.00"), (3, "-2.00")):
        message = {
            "type": "orderbook_delta",
            "sid": 1,
            "seq": seq,
            "msg": {"market_ticker": T, "side": "yes", "price_dollars": "0.40", "delta_fp": delta},
        }
        books.on_message(message, seq)
        sim.after_event(Event(seq, "kalshi_ws", "frame", "orderbook_delta", T, message))
    trade = {
        "type": "trade",
        "msg": {
            "market_ticker": T,
            "taker_side": "no",
            "yes_price_dollars": "0.40",
            "no_price_dollars": "0.60",
            "count_fp": "2.00",
        },
    }
    sim.after_event(Event(4, "kalshi_ws", "frame", "trade", T, trade))
    assert sum((f.count for f in sim.fills()), D(0)) == expected_filled


def test_trade_and_delta_queue_attribution_consumes_volume_once():
    sim, books = make_exchange(queue="front")
    sim.submit(req(price="0.40", count="1", tif="good_till_canceled"), 0)
    sim.flush(0)
    trade = {
        "type": "trade",
        "msg": {
            "market_ticker": T,
            "taker_side": "no",
            "yes_price_dollars": "0.40",
            "no_price_dollars": "0.60",
            "count_fp": "1.00",
        },
    }
    sim.after_event(Event(1, "kalshi_ws", "frame", "trade", T, trade))
    delta = {
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 2,
        "msg": {"market_ticker": T, "side": "yes", "price_dollars": "0.40", "delta_fp": "-1.00"},
    }
    books.on_message(delta, 2)
    sim.after_event(Event(2, "kalshi_ws", "frame", "orderbook_delta", T, delta))
    assert not sim.fills()
    sim.after_event(Event(3, "kalshi_ws", "frame", "trade", T, trade))
    assert not sim.fills()
    sim.after_event(Event(4, "kalshi_ws", "frame", "trade", T, trade))
    assert len(sim.fills()) == 1


def test_stp_modes_gtt_and_reactivation_cancel():
    taker_sim, _ = make_exchange()
    taker_sim.submit(req(side="ask", price="0.50", tif="good_till_canceled"), 0)
    taker_sim.flush(0)
    taker_sim.submit(req(price="0.60", self_trade_prevention_type="taker_at_cross"), 1)
    taker_sim.flush(1)
    assert taker_sim.orders()[1].status == "canceled"
    assert taker_sim.open_orders(T)
    maker_sim, _ = make_exchange()
    maker_sim.submit(req(side="ask", price="0.50", tif="good_till_canceled"), 0)
    maker_sim.flush(0)
    maker_sim.submit(req(price="0.60", self_trade_prevention_type="maker"), 1)
    maker_sim.flush(1)
    assert maker_sim.orders()[0].status == "canceled"
    assert maker_sim.orders()[1].fill_count == D("1")

    gtt_sim, _ = make_exchange()
    gtt_sim.submit(req(price="0.30", tif="good_till_canceled", expiration_time=1), 0)
    gtt_sim.flush(0)
    assert gtt_sim.orders()[0].status == "resting"
    gtt_sim.flush(1000)
    assert gtt_sim.orders()[0].status == "canceled"
    reactivated = gtt_sim.submit(req(price="0.30", tif="good_till_canceled"), 1001)
    gtt_sim.flush(1001)
    gtt_sim.after_event(event("frame", "market_lifecycle_v2", {"event_type": "deactivated"}, 1002))
    gtt_sim.after_event(event("frame", "market_lifecycle_v2", {"event_type": "activated"}, 1003))
    assert next(o for o in gtt_sim.orders() if o.order_id == reactivated.order_id).status == "canceled"


def test_reduce_only_cannot_flip_and_market_settlement_payouts():
    sim, _ = make_exchange()
    sim.portfolio.apply_fill(T, "ask", D("0.40"), D("1"), D(0))
    invalid = sim.submit(req(side="bid", price="0.60", count="1.01", reduce_only=True), 0)
    assert invalid.reject_code == "REDUCE_ONLY_EXCEEDS_POSITION"
    sim.submit(req(side="bid", price="0.60", count="1", reduce_only=True), 0)
    sim.flush(0)
    assert sim.orders()[-1].status == "executed"
    assert sim.positions()[T].position == 0

    settlement, _ = make_exchange()
    settlement.portfolio.apply_fill(T, "bid", D("0.30"), D("2"), D(0))
    settlement._settle(T, "yes", 5)
    assert settlement.settlements()[0].payout == D("2")
    assert settlement.settlements()[0].pnl == D("1.40")


def test_fee_accumulator_rebates_precision_increment_without_negative_fee():
    accumulator = FeeAccumulator()
    model = FeeModel(precision=D("0.01"))
    charges = [accumulator.charge(model, D("-0.055"), D("0.003639")) for _ in range(12)]
    assert all(charge >= D(0) for charge in charges)
    assert any(charge == D(0) for charge in charges)
    assert accumulator.accumulated_rounding >= D(0)


def test_same_level_orders_obey_simulated_time_priority():
    sim, _ = make_exchange(queue="front")
    sim.submit(req(price="0.40", count="1", tif="good_till_canceled"), 0)
    sim.flush(0)
    second = sim.submit(req(price="0.40", count="1", tif="good_till_canceled"), 1)
    sim.flush(1)
    assert sim._queues[second.order_id][0] == D("3.00")
    trade = {
        "type": "trade",
        "msg": {
            "market_ticker": T,
            "taker_side": "no",
            "yes_price_dollars": "0.40",
            "no_price_dollars": "0.60",
            "count_fp": "3.00",
        },
    }
    sim.after_event(Event(2, "kalshi_ws", "frame", "trade", T, trade))
    assert sim.orders()[0].status == "executed"
    assert sim.orders()[1].fill_count == 0
    trade["msg"]["count_fp"] = "1.00"
    sim.after_event(Event(3, "kalshi_ws", "frame", "trade", T, trade))
    assert sim.orders()[1].status == "executed"


def test_reduce_only_requires_ioc_and_position_close_cashflow():
    sim, _ = make_exchange()
    sim.portfolio.apply_fill(T, "ask", D("0.40"), D("2"), D(0))
    sim.portfolio.apply_fill(T, "bid", D("0.30"), D("1"), D(0))
    assert sim.balance() == D("999.50")
    assert (
        sim.submit(
            req(side="bid", price="0.60", count="1", tif="good_till_canceled", reduce_only=True), 0
        ).reject_code
        == "REDUCE_ONLY_REQUIRES_IOC"
    )
