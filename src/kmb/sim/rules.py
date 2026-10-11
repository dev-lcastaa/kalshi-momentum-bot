from __future__ import annotations

from decimal import Decimal

from kmb.exchange import OrderRequest
from kmb.kalshi.messages import COUNT_STEP, ZERO


def validate_request(req: OrderRequest, grid, paused: bool, closed: bool, position: Decimal) -> str | None:
    if req.side not in ("bid", "ask"):
        return "INVALID_SIDE"
    if not grid.is_valid(req.price):
        return "INVALID_PRICE"
    if req.count < COUNT_STEP or req.count % COUNT_STEP:
        return "INVALID_COUNT"
    if req.time_in_force not in ("fill_or_kill", "immediate_or_cancel", "good_till_canceled"):
        return "INVALID_TIF"
    if req.reduce_only and req.time_in_force != "immediate_or_cancel":
        return "REDUCE_ONLY_REQUIRES_IOC"
    if req.expiration_time is not None and req.time_in_force != "good_till_canceled":
        return "INVALID_EXPIRATION"
    if req.self_trade_prevention_type not in ("taker_at_cross", "maker"):
        return "INVALID_STP"
    if closed:
        return "MARKET_INACTIVE"
    if paused:
        return "MARKET_PAUSED"
    if req.reduce_only and (position == ZERO or (position > ZERO) != (req.side == "ask")):
        return "REDUCE_ONLY_NO_POSITION"
    if req.reduce_only and req.count > abs(position):
        return "REDUCE_ONLY_EXCEEDS_POSITION"
    return None


def market_closed(close_ms: int | None, now_ms: int) -> bool:
    return close_ms is not None and now_ms >= close_ms
