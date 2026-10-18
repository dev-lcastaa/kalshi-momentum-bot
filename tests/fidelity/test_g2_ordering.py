from kmb.fidelity.g2_tape import run_g2
from kmb.strategy.base import Event

T = "KXBTC15M-26JAN011200-00"


def delta(ts: int, side: str, price: str, size: str) -> Event:
    msg = {"market_ticker": T, "side": side, "price_dollars": price, "delta_fp": size}
    return Event(ts, "kalshi_ws", "frame", "orderbook_delta", T, {"type": "orderbook_delta", "msg": msg})


def trade(ts: int, taker: str, yes: str, no: str, count: str) -> Event:
    msg = {
        "market_ticker": T,
        "taker_side": taker,
        "yes_price_dollars": yes,
        "no_price_dollars": no,
        "count_fp": count,
        "trade_id": f"t{ts}",
    }
    return Event(ts, "kalshi_ws", "frame", "trade", T, {"type": "trade", "msg": msg})


def test_g2_matches_a_decrease_published_before_the_trade() -> None:
    # Kalshi sends the resting-order decrease first, then the trade it caused.
    report = run_g2([delta(1, "yes", "0.4800", "-1.00"), trade(2, "no", "0.4800", "0.5200", "1.00")])
    assert report["passed"] and report["n_matched"] == 1


def test_g2_still_flags_a_trade_with_no_matching_decrease() -> None:
    report = run_g2([delta(1, "yes", "0.4900", "-1.00"), trade(2, "no", "0.4800", "0.5200", "1.00")])
    assert not report["passed"]


def test_g2_matches_a_sweep_with_many_unrelated_rows_between() -> None:
    rows = [delta(1, "no", "0.3900", "-64.00")]
    rows += [delta(2 + i, "yes", "0.1000", "1.00") for i in range(40)]
    rows += [trade(60, "yes", "0.6100", "0.3900", "64.00")]
    assert run_g2(rows)["passed"]
