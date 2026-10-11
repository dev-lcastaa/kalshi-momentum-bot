"""Pre-trade risk checks for paper and live jobs (RiskLimit row + kill switch), applied before every order."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal

from kmb.db.models import RiskLimit
from kmb.exchange import OrderRequest
from kmb.kalshi.messages import ONE, ZERO
from kmb.strategy.base import Context


@dataclass(frozen=True)
class Limits:
    max_order_count: Decimal = Decimal(10)
    max_position_per_market: Decimal = Decimal(20)
    max_open_orders: int = 10
    max_daily_loss: Decimal = Decimal(25)
    max_notional_per_market: Decimal = Decimal(20)
    max_orders_per_minute: int = 30

    @classmethod
    def from_row(cls, row: RiskLimit) -> Limits:
        return cls(
            Decimal(row.max_order_count),
            Decimal(row.max_position_per_market),
            int(row.max_open_orders),
            Decimal(row.max_daily_loss),
            Decimal(row.max_notional_per_market),
            int(row.max_orders_per_minute),
        )

    def to_dict(self) -> dict[str, str]:
        return {k: str(v) for k, v in self.__dict__.items()}


class RiskGuard:
    """Callable guard for ``SimRunner``: returns a reject reason or None.

    ``killed`` and ``limits`` are refreshed by the job's control loop (DB poll), so a kill switch or a limit
    change takes effect on the next order without restarting the job. ``baseline_pnl`` is the realised P&L
    already booked today before this job started (live), so the daily-loss limit spans restarts.
    """

    def __init__(self, limits: Limits, baseline_pnl: Decimal = ZERO):
        self.limits = limits
        self.killed: str | None = None
        self.baseline_pnl = baseline_pnl
        self._sent: deque[int] = deque()

    def __call__(self, req: OrderRequest, ctx: Context) -> str | None:
        lim = self.limits
        if self.killed:
            return f"kill switch: {self.killed}"
        if req.count <= ZERO:
            return "count must be positive"
        if req.count > lim.max_order_count:
            return f"count {req.count} > max_order_count {lim.max_order_count}"
        if len(ctx.open_orders) >= lim.max_open_orders:
            return f"open orders {len(ctx.open_orders)} >= max_open_orders {lim.max_open_orders}"
        while self._sent and ctx.now_ms - self._sent[0] >= 60_000:
            self._sent.popleft()
        if len(self._sent) >= lim.max_orders_per_minute:
            return f"order rate >= {lim.max_orders_per_minute}/min"

        signed = req.count if req.side == "bid" else -req.count
        pos = ctx.position(req.ticker)
        resting = [o for o in ctx.open_orders if o.ticker == req.ticker]
        same_dir = sum((o.remaining_count for o in resting if o.side == req.side), ZERO)
        if not req.reduce_only and abs(pos + signed) > abs(pos):
            # worst case: every resting order on this side fills too
            projected = abs(pos + signed + (same_dir if signed > 0 else -same_dir))
            if projected > lim.max_position_per_market:
                return (
                    f"projected position {projected} > max_position_per_market {lim.max_position_per_market}"
                )
            p = ctx.positions.get(req.ticker)
            unit = req.price if req.side == "bid" else ONE - req.price
            committed = (p.cost if p else ZERO) + sum(
                ((o.price if o.side == "bid" else ONE - o.price) * o.remaining_count for o in resting), ZERO
            )
            if committed + unit * req.count > lim.max_notional_per_market:
                total = committed + unit * req.count
                return f"notional {total} > max_notional_per_market {lim.max_notional_per_market}"
            # past the daily loss limit only exposure-reducing orders are allowed
            realized = sum((p.realized_pnl for p in ctx.positions.values()), ZERO) + self.baseline_pnl
            if realized <= -lim.max_daily_loss:
                return f"daily loss {-realized} >= max_daily_loss {lim.max_daily_loss}"
        self._sent.append(ctx.now_ms)
        return None
