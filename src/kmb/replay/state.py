"""Market state rebuilt from events: books, market info, last ticker, trades, reference prices.

The same ``MarketState`` feeds replay frames for the UI, strategy contexts in sim/paper/live, and the window
overview, so everything sees the market identically.
"""

from __future__ import annotations

import copy
import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from kmb.book import BookManager
from kmb.strategy.base import Event, MarketInfo


def _dec(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _iso_ms(value: Any) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


@dataclass
class TradePrint:
    ts_ms: int
    ticker: str
    price: Decimal  # YES price
    count: Decimal
    taker_side: str  # yes | no

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "price": str(self.price),
            "count": str(self.count),
            "taker_side": self.taker_side,
        }


@dataclass
class TickerInfo:
    ts_ms: int
    price: Decimal | None
    yes_bid: Decimal | None
    yes_ask: Decimal | None
    volume: Decimal | None
    open_interest: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        def s(v: Decimal | None) -> str | None:
            return None if v is None else str(v)

        return {
            "price": s(self.price),
            "yes_bid": s(self.yes_bid),
            "yes_ask": s(self.yes_ask),
            "volume": s(self.volume),
            "open_interest": s(self.open_interest),
        }


@dataclass
class Note:
    ts_ms: int
    kind: str  # lifecycle | transport | gap
    text: str
    ticker: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"ts_ms": self.ts_ms, "kind": self.kind, "text": self.text}


@dataclass
class MarketState:
    books: BookManager = field(default_factory=BookManager)
    markets: dict[str, MarketInfo] = field(default_factory=dict)
    tickers: dict[str, TickerInfo] = field(default_factory=dict)
    last_trade: dict[str, TradePrint] = field(default_factory=dict)
    trades: deque[TradePrint] = field(default_factory=lambda: deque(maxlen=500))
    reference: dict[str, Decimal] = field(default_factory=dict)
    reference_ts_ms: dict[str, int] = field(default_factory=dict)
    # CF 60 s windowed average (final minute)
    settlement_avg: dict[str, Decimal] = field(default_factory=dict)
    now_ms: int = 0
    gaps: int = 0
    reconnects: int = 0

    def market(self, ticker: str) -> MarketInfo:
        m = self.markets.get(ticker)
        if m is None:
            m = self.markets[ticker] = MarketInfo(ticker=ticker, series=ticker.split("-", 1)[0])
        return m

    def snapshot(self) -> MarketState:
        return copy.deepcopy(self)

    # ------------------------------------------------------------------------------------------- apply

    def apply(self, ev: Event) -> list[Note]:
        """Apply one event; returns notes worth showing (lifecycle, transport, gaps)."""
        self.now_ms = max(self.now_ms, ev.ts_ms)
        data = ev.data if isinstance(ev.data, dict) else {}
        if ev.kind == "transport":
            return self._transport(ev, data)
        if ev.kind == "rest":
            self._rest(ev, data)
            return []
        if ev.kind != "frame":
            return []
        if ev.stream == "coinbase_ws":
            self._coinbase(ev, data)
            return []
        mt = ev.msg_type or data.get("type")
        msg = data.get("msg") if isinstance(data.get("msg"), dict) else {}
        if mt in ("orderbook_snapshot", "orderbook_delta"):
            gaps_before = self.books.gaps
            self.books.on_message(data, ev.ts_ms)
            if self.books.gaps > gaps_before:
                self.gaps += 1
                text = f"orderbook seq gap on sid {data.get('sid')}"
                return [Note(ev.ts_ms, "gap", text, msg.get("market_ticker"))]
            return []
        if mt == "ticker":
            t = msg.get("market_ticker")
            if t:
                self.tickers[t] = TickerInfo(
                    ev.ts_ms,
                    _dec(msg.get("price_dollars")),
                    _dec(msg.get("yes_bid_dollars")),
                    _dec(msg.get("yes_ask_dollars")),
                    _dec(msg.get("volume_fp")),
                    _dec(msg.get("open_interest_fp")),
                )
            return []
        if mt == "trade":
            t = msg.get("market_ticker")
            price, count = _dec(msg.get("yes_price_dollars")), _dec(msg.get("count_fp"))
            if t and price is not None and count is not None:
                tp = TradePrint(ev.ts_ms, t, price, count, str(msg.get("taker_side") or ""))
                self.trades.append(tp)
                self.last_trade[t] = tp
            return []
        if mt in ("market_lifecycle_v2", "market_lifecycle"):
            return self._lifecycle(ev, msg)
        if mt in ("cfbenchmarks_value_5hz", "cfbenchmarks_value"):
            self._cfb(ev, mt, msg)
            return []
        return []

    def _transport(self, ev: Event, data: dict[str, Any]) -> list[Note]:
        what = str(data.get("event") or "")
        if ev.stream == "kalshi_ws" and what == "connected":
            self.reconnects += 1
            self.books.reset_connection()
        if ev.stream == "kalshi_ws" and what in ("gap", "resync"):
            self.gaps += 1
            for b in self.books.books.values():
                b.invalidate()
        detail = {k: v for k, v in data.items() if k not in ("event",)}
        return [Note(ev.ts_ms, "transport", f"{ev.stream} {what} {json.dumps(detail, default=str)[:160]}")]

    def _rest(self, ev: Event, data: dict[str, Any]) -> None:
        body = data.get("body")
        if ev.msg_type == "rest.market" and isinstance(body, dict) and isinstance(body.get("market"), dict):
            self.update_market_from_rest(body["market"])

    def update_market_from_rest(self, mk: dict[str, Any]) -> None:
        t = mk.get("ticker")
        if not t:
            return
        m = self.market(t)
        m.series = mk.get("series_ticker") or m.series
        m.open_ms = _iso_ms(mk.get("open_time")) or m.open_ms
        m.close_ms = _iso_ms(mk.get("close_time")) or m.close_ms
        strike = _dec(mk.get("floor_strike"))
        m.strike = strike if strike is not None else m.strike
        m.status = mk.get("status") or m.status
        if mk.get("result") in ("yes", "no", "void"):
            m.result = mk["result"]

    def _lifecycle(self, ev: Event, msg: dict[str, Any]) -> list[Note]:
        t = msg.get("market_ticker")
        if not t:
            return []
        m = self.market(t)
        et = str(msg.get("event_type") or "")
        if msg.get("open_ts"):
            m.open_ms = int(msg["open_ts"]) * 1000
        if msg.get("close_ts"):
            m.close_ms = int(msg["close_ts"]) * 1000
        meta = msg.get("additional_metadata") if isinstance(msg.get("additional_metadata"), dict) else {}
        strike = _dec(meta.get("floor_strike"))
        if strike is not None:
            m.strike = strike
        if et == "deactivated":
            m.status = "paused"
        elif et == "activated":
            m.status = "active"
        elif et in ("determined", "settled"):
            m.status = et
            if msg.get("result") in ("yes", "no", "void"):
                m.result = msg["result"]
        text = et + (f" result={msg['result']}" if msg.get("result") else "")
        return [Note(ev.ts_ms, "lifecycle", text, t)]

    def _cfb(self, ev: Event, mt: str, msg: dict[str, Any]) -> None:
        index = msg.get("index_id")
        if not index:
            return
        value = _dec(msg.get("value_usd"))
        if value is None:
            raw = msg.get("data")
            inner = None
            if isinstance(raw, str):
                try:
                    inner = json.loads(raw)
                except ValueError:
                    inner = None
            elif isinstance(raw, dict):
                inner = raw
            if isinstance(inner, dict):
                value = _dec(inner.get("value"))
        if value is not None:
            self.reference[index] = value
            self.reference_ts_ms[index] = ev.ts_ms
        avg = msg.get("last_60s_windowed_average_15min")
        if isinstance(avg, dict) and _dec(avg.get("value")) is not None:
            self.settlement_avg[index] = _dec(avg.get("value"))  # type: ignore[assignment]

    def _coinbase(self, ev: Event, data: dict[str, Any]) -> None:
        pair = data.get("product_id")
        if not pair:
            return
        if data.get("type") in ("match", "last_match", "ticker"):
            price = _dec(data.get("price"))
            if price is not None:
                self.reference[pair] = price
                self.reference_ts_ms[pair] = ev.ts_ms
