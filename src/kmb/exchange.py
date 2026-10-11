"""Exchange-facing types shared by SimExchange, LiveExchange, paper trading, replay and strategies.

Shapes follow Kalshi's V2 order API: ``side`` is the YES book side (``bid`` = buy YES, ``ask`` = sell YES,
which is economically buying NO at 1 - price). Prices are YES dollar prices, counts are contracts. A strategy
written against :class:`Exchange` runs unchanged on the simulator and on the real exchange.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Literal, Protocol

from kmb.kalshi.messages import ZERO, fmt_count, fmt_price

Side = Literal["bid", "ask"]
TimeInForce = Literal["fill_or_kill", "good_till_canceled", "immediate_or_cancel"]
STP = Literal["taker_at_cross", "maker"]
OrderStatus = Literal["pending", "resting", "canceled", "executed", "rejected"]
Mode = Literal["sim", "paper", "live"]


def new_client_order_id() -> str:
    return str(uuid.uuid4())


@dataclass(frozen=True)
class OrderRequest:
    ticker: str
    side: Side
    price: Decimal
    count: Decimal
    time_in_force: TimeInForce = "good_till_canceled"
    post_only: bool = False
    reduce_only: bool = False
    self_trade_prevention_type: STP = "taker_at_cross"
    expiration_time: int | None = None  # unix seconds; only with good_till_canceled
    cancel_order_on_pause: bool = False
    client_order_id: str = field(default_factory=new_client_order_id)

    def to_payload(self) -> dict[str, Any]:
        """Exact JSON body for POST /portfolio/events/orders."""
        body: dict[str, Any] = {
            "ticker": self.ticker,
            "client_order_id": self.client_order_id,
            "side": self.side,
            "count": fmt_count(self.count),
            "price": fmt_price(self.price),
            "time_in_force": self.time_in_force,
            "self_trade_prevention_type": self.self_trade_prevention_type,
            "post_only": self.post_only,
            "reduce_only": self.reduce_only,
            "cancel_order_on_pause": self.cancel_order_on_pause,
        }
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        return body


@dataclass
class OrderState:
    order_id: str
    client_order_id: str
    ticker: str
    side: Side
    price: Decimal
    initial_count: Decimal
    fill_count: Decimal = ZERO
    remaining_count: Decimal = ZERO
    status: OrderStatus = "pending"
    time_in_force: TimeInForce = "good_till_canceled"
    post_only: bool = False
    taker_fill_cost: Decimal = ZERO
    maker_fill_cost: Decimal = ZERO
    taker_fees: Decimal = ZERO
    maker_fees: Decimal = ZERO
    created_ts_ms: int = 0
    last_update_ts_ms: int = 0
    expiration_ts_ms: int | None = None
    reject_code: str | None = None
    reject_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in d.items()}


@dataclass(frozen=True)
class Fill:
    trade_id: str
    order_id: str
    client_order_id: str
    ticker: str
    side: Side  # book side of OUR order
    price: Decimal  # YES price
    count: Decimal
    fee: Decimal  # net fee charged on this fill (trade + rounding - rebate)
    is_taker: bool
    ts_ms: int
    post_position: Decimal  # signed YES position after this fill (negative = NO)

    def to_dict(self) -> dict[str, Any]:
        return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in asdict(self).items()}


@dataclass(frozen=True)
class PlaceResult:
    ok: bool
    order_id: str | None
    client_order_id: str
    fill_count: Decimal = ZERO
    remaining_count: Decimal = ZERO
    average_fill_price: Decimal | None = None
    average_fee_paid: Decimal | None = None
    ts_ms: int = 0
    error_code: str | None = None
    error_message: str | None = None


@dataclass
class Position:
    ticker: str
    position: Decimal = ZERO  # signed: + YES contracts, - NO contracts
    cost: Decimal = ZERO  # dollars paid for the open position (YES or NO), excluding fees
    realized_pnl: Decimal = ZERO
    fees_paid: Decimal = ZERO
    volume: Decimal = ZERO

    def to_dict(self) -> dict[str, Any]:
        return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in asdict(self).items()}


@dataclass(frozen=True)
class SettlementNote:
    ticker: str
    result: str  # yes | no | void
    position: Decimal  # signed YES position at settlement
    payout: Decimal  # dollars credited
    pnl: Decimal  # payout - cost of the settled position (fees already in realized pnl)
    ts_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in asdict(self).items()}


Notification = OrderState | Fill | SettlementNote


class Exchange(Protocol):
    """Order API used by strategies through the runner. Implemented by SimExchange and LiveExchange."""

    mode: Mode

    async def place_order(self, req: OrderRequest) -> PlaceResult: ...
    async def cancel_order(self, order_id: str, ticker: str) -> OrderState | None: ...
    async def decrease_order(self, order_id: str, ticker: str, reduce_to: Decimal) -> OrderState | None: ...
    def open_orders(self, ticker: str | None = None) -> list[OrderState]: ...
    def positions(self) -> dict[str, Position]: ...
    def balance(self) -> Decimal: ...
