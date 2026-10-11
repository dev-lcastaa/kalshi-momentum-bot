"""Window overview (1-second buckets) and replay frames, in the shapes defined by docs/api.md."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from kmb.envelope.windows import coin_for_ticker
from kmb.replay.state import MarketState, Note
from kmb.strategy.base import Event

BOOK_DEPTH = 25
HEATMAP_DEPTH = 10


def _s(v: Decimal | None) -> str | None:
    return None if v is None else str(v)


def window_times(window: dict[str, Any], events: Sequence[Event]) -> dict[str, int]:
    first = events[0].ts_ms if events else 0
    last = events[-1].ts_ms if events else 0
    start = int(window.get("start_ms") or first)
    end = int(window.get("end_ms") or last)
    return {
        "start_ms": start,
        "end_ms": max(end, start),
        "open_ms": int(window.get("open_ms") or start),
        "close_ms": int(window.get("close_ms") or end),
    }


def refs_for(ticker: str) -> tuple[str | None, str | None]:
    coin = coin_for_ticker(ticker)
    return (coin.cf_index, coin.coinbase_pair) if coin else (None, None)


def book_view(state: MarketState, ticker: str, depth: int = BOOK_DEPTH) -> dict[str, Any]:
    b = state.books.book(ticker)
    return {
        "ticker": ticker,
        "valid": b.valid,
        "yes_bids": [[str(p), str(s)] for p, s in b.yes_bids(depth)],
        "yes_asks": [[str(p), str(s)] for p, s in b.yes_asks(depth)],
        "mid": _s(b.mid()),
        "spread": _s(b.spread()),
    }


def build_frame(
    state: MarketState,
    ticker: str,
    applied: Sequence[Event],
    notes: Sequence[Note],
    *,
    ts_ms: int,
    playing: bool,
    speed: float,
) -> dict[str, Any]:
    ref_key, cb_key = refs_for(ticker)
    trades = []
    for ev in applied:
        if ev.msg_type == "trade" and ev.market_ticker == ticker and isinstance(ev.data, dict):
            msg = ev.data.get("msg") or {}
            trades.append(
                {
                    "ts_ms": ev.ts_ms,
                    "price": msg.get("yes_price_dollars"),
                    "count": msg.get("count_fp"),
                    "taker_side": msg.get("taker_side"),
                }
            )
    tk = state.tickers.get(ticker)
    return {
        "type": "frame",
        "ts_ms": ts_ms,
        "playing": playing,
        "speed": speed,
        "book": book_view(state, ticker),
        "trades": trades[-200:],
        "ticker": tk.to_dict() if tk else None,
        "ref": _s(state.reference.get(ref_key)) if ref_key else None,
        "coinbase": _s(state.reference.get(cb_key)) if cb_key else None,
        "events": [n.to_dict() for n in notes if n.ticker in (None, ticker)][-50:],
    }


def build_overview(window: dict[str, Any], events: Sequence[Event], bucket_ms: int = 1000) -> dict[str, Any]:
    ticker = window["market_ticker"]
    times = window_times(window, events)
    ref_key, cb_key = refs_for(ticker)
    state = MarketState()
    series: list[dict[str, Any]] = []
    depth: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    lifecycle: list[dict[str, Any]] = []
    bucket_volume = Decimal(0)
    next_t = times["start_ms"] + bucket_ms

    def emit(t: int) -> None:
        nonlocal bucket_volume
        b = state.books.book(ticker)
        bid, ask = (b.best_yes_bid(), b.best_yes_ask()) if b.valid else (None, None)
        last = state.last_trade.get(ticker)
        series.append(
            {
                "ts_ms": t,
                "yes_bid": _s(bid[0]) if bid else None,
                "yes_ask": _s(ask[0]) if ask else None,
                "mid": _s(b.mid()) if b.valid else None,
                "last": _s(last.price) if last else None,
                "volume": str(bucket_volume),
                "ref": _s(state.reference.get(ref_key)) if ref_key else None,
                "coinbase": _s(state.reference.get(cb_key)) if cb_key else None,
            }
        )
        if b.valid:
            depth.append(
                {
                    "ts_ms": t,
                    "bids": [[str(p), str(s)] for p, s in b.yes_bids(HEATMAP_DEPTH)],
                    "asks": [[str(p), str(s)] for p, s in b.yes_asks(HEATMAP_DEPTH)],
                }
            )
        bucket_volume = Decimal(0)

    for ev in events:
        while ev.ts_ms >= next_t and next_t <= times["end_ms"]:
            emit(next_t)
            next_t += bucket_ms
        notes = state.apply(ev)
        if ev.msg_type == "trade" and ev.market_ticker == ticker:
            tp = state.last_trade.get(ticker)
            if tp is not None and tp.ts_ms == ev.ts_ms:
                trades.append(tp.to_dict())
                bucket_volume += tp.count
        for n in notes:
            if n.kind == "lifecycle" and n.ticker == ticker:
                lifecycle.append({"ts_ms": n.ts_ms, "event_type": n.text.split(" ")[0], "text": n.text})
            elif n.kind in ("gap", "transport"):
                lifecycle.append({"ts_ms": n.ts_ms, "event_type": n.kind, "text": n.text})
    while next_t <= times["end_ms"]:
        emit(next_t)
        next_t += bucket_ms
    return {
        "window": window,
        **times,
        "series": series,
        "trades": trades,
        "depth": depth,
        "lifecycle": lifecycle,
    }
