from __future__ import annotations

from decimal import Decimal

from kmb.exchange import Position
from kmb.kalshi.messages import ZERO


class Portfolio:
    def __init__(self, starting_balance: Decimal):
        self.cash = starting_balance
        self.positions: dict[str, Position] = {}

    def position(self, ticker: str) -> Position:
        return self.positions.setdefault(ticker, Position(ticker))

    def apply_fill(self, ticker: str, side: str, price: Decimal, count: Decimal, fee: Decimal) -> Decimal:
        """Track signed YES exposure; opposite purchases redeem paired contracts at $1."""
        pos = self.position(ticker)
        signed = count if side == "bid" else -count
        unit_cost = price if side == "bid" else Decimal(1) - price
        prior, change = pos.position, signed
        realized = ZERO
        if prior and prior * change < ZERO:
            closed = min(abs(prior), abs(change))
            avg_cost = pos.cost / abs(prior)
            close_value = (Decimal(1) - unit_cost) * closed
            realized = close_value - avg_cost * closed
            self.cash += closed
            pos.cost -= avg_cost * closed
            if abs(change) > abs(prior):
                pos.cost += unit_cost * (abs(change) - abs(prior))
        else:
            pos.cost += unit_cost * abs(change)
        pos.position += change
        if not pos.position:
            pos.cost = ZERO
        self.cash -= unit_cost * count + fee
        pos.realized_pnl += realized - fee
        pos.fees_paid += fee
        pos.volume += count
        return realized

    def reserve(self, orders) -> Decimal:
        total = ZERO
        for order in orders:
            if order.status not in ("pending", "resting"):
                continue
            unit = order.price if order.side == "bid" else Decimal(1) - order.price
            total += unit * order.remaining_count
        return total

    def available(self, orders) -> Decimal:
        return self.cash - self.reserve(orders)

    def settle(self, ticker: str, result: str) -> tuple[Decimal, Decimal, Decimal]:
        pos = self.position(ticker)
        cost, position = pos.cost, pos.position
        if result == "void":
            payout = cost
        elif result == "yes":
            payout = max(position, ZERO)
        elif result == "no":
            payout = max(-position, ZERO)
        else:
            return ZERO, ZERO, ZERO
        pnl = payout - cost
        self.cash += payout
        pos.realized_pnl += pnl
        pos.position = ZERO
        pos.cost = ZERO
        return position, payout, pnl
