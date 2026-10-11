from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from kmb.book import OrderBook
from kmb.kalshi.messages import ONE, ZERO


@dataclass(frozen=True)
class Level:
    book_side: str
    book_price: Decimal
    yes_price: Decimal
    size: Decimal


def crossing_levels(book: OrderBook, side: str, limit: Decimal, overlays: dict) -> list[Level]:
    if not book.valid:
        return []
    if side == "bid":
        raw = (("no", p, ONE - p, s) for p, s in book.no.items() if p >= ONE - limit)
        levels = sorted(raw, key=lambda x: x[1], reverse=True)
    else:
        raw = (("yes", p, p, s) for p, s in book.yes.items() if p >= limit)
        levels = sorted(raw, key=lambda x: x[1], reverse=True)
    return [
        Level(bs, p, yp, max(ZERO, size - overlays.get((book.ticker, bs, p), ZERO)))
        for bs, p, yp, size in levels
    ]
