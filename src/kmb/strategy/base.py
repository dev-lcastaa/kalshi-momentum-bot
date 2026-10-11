"""Strategy contract. Strategies are synchronous and deterministic: same events in, same intents out.

The runner (replay, sim, paper or live) owns the clock, the books and the exchange. For every market-data
event it updates state, then calls ``on_event``; the strategy returns intents which the runner turns into
exchange calls. Order and fill updates come back through ``on_order_update`` / ``on_fill``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from kmb.book import BookManager, OrderBook
from kmb.exchange import Fill, OrderRequest, OrderState, Position


@dataclass(frozen=True)
class Event:
    """One market-data event as seen by a strategy (built from a capture envelope)."""

    ts_ms: int  # local receive wall time (replay clock)
    stream: str  # kalshi_ws | kalshi_rest | coinbase_ws | control
    kind: str  # frame | rest | transport | control
    msg_type: str | None
    market_ticker: str | None
    data: Any  # parsed JSON payload
    local_seq: int = 0


@dataclass(frozen=True)
class Place:
    order: OrderRequest
    tag: str = ""


@dataclass(frozen=True)
class Cancel:
    order_id: str
    ticker: str


@dataclass(frozen=True)
class CancelAll:
    ticker: str | None = None


Intent = Place | Cancel | CancelAll


@dataclass
class MarketInfo:
    ticker: str
    series: str | None = None
    open_ms: int | None = None
    close_ms: int | None = None
    strike: Decimal | None = None
    status: str | None = None
    result: str | None = None  # "yes" | "no" once determined


@dataclass
class Context:
    now_ms: int
    books: BookManager
    markets: dict[str, MarketInfo] = field(default_factory=dict)
    positions: dict[str, Position] = field(default_factory=dict)
    open_orders: list[OrderState] = field(default_factory=list)
    reference: dict[str, Decimal] = field(default_factory=dict)  # e.g. BRTI, ETHUSD_RTI, BTC-USD, ETH-USD
    reference_ts_ms: dict[str, int] = field(default_factory=dict)
    balance: Decimal = Decimal(0)
    params: dict[str, Any] = field(default_factory=dict)
    logs: list[str] = field(default_factory=list)

    def book(self, ticker: str) -> OrderBook:
        return self.books.book(ticker)

    def position(self, ticker: str) -> Decimal:
        p = self.positions.get(ticker)
        return p.position if p else Decimal(0)

    def orders_for(self, ticker: str) -> list[OrderState]:
        return [o for o in self.open_orders if o.ticker == ticker]

    def log(self, msg: str) -> None:
        self.logs.append(f"{self.now_ms} {msg}")


class Strategy:
    """Base class. Override what you need; every hook returns a list of intents."""

    name = "base"
    default_params: dict[str, Any] = {}

    def __init__(self, params: dict[str, Any] | None = None):
        self.params = {**self.default_params, **(params or {})}

    def on_start(self, ctx: Context) -> list[Intent]:
        return []

    def on_event(self, ctx: Context, ev: Event) -> list[Intent]:
        return []

    def on_order_update(self, ctx: Context, order: OrderState) -> list[Intent]:
        return []

    def on_fill(self, ctx: Context, fill: Fill) -> list[Intent]:
        return []

    def on_stop(self, ctx: Context) -> list[Intent]:
        return []
