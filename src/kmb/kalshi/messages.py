"""Exact Decimal handling for Kalshi fixed-point fields and per-market price grids.

Prices are dollar strings (up to 4 dp in requests, up to 6 dp in responses); counts are ``_fp`` strings
with 2 dp (fractional contracts allowed, min 0.01). Never use floats for either.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

ZERO = Decimal(0)
ONE = Decimal(1)
CENT = Decimal("0.01")
COUNT_STEP = Decimal("0.01")
PRICE_Q = Decimal("0.0001")
FEE_Q = Decimal("0.000001")


def D(value: Any) -> Decimal:
    """Decimal from str/int/Decimal. Floats are rejected so precision is never silently lost."""
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("bool is not a number")
    if isinstance(value, int | str):
        return Decimal(value)
    raise TypeError(f"refusing to convert {type(value).__name__} to Decimal: {value!r}")


def fmt_price(p: Decimal) -> str:
    return str(p.quantize(PRICE_Q))


def fmt_count(c: Decimal) -> str:
    return str(c.quantize(COUNT_STEP))


def fmt_dollars6(v: Decimal) -> str:
    return str(v.quantize(FEE_Q))


def ceil_to(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def floor_to(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


@dataclass(frozen=True)
class PriceBand:
    start: Decimal
    end: Decimal
    step: Decimal


# Documented structures (docs: Fixed-Point Representation). `price_ranges` from the market wins when present.
_STRUCTURES: dict[str, list[tuple[str, str, str]]] = {
    "linear_cent": [("0", "1", "0.01")],
    "deci_cent": [("0", "1", "0.001")],
    "tapered_deci_cent": [("0", "0.10", "0.001"), ("0.10", "0.90", "0.01"), ("0.90", "1", "0.001")],
}


class PriceGrid:
    """Valid order prices for one market. Any on-grid price strictly between 0 and 1 is valid."""

    def __init__(self, bands: Iterable[PriceBand]):
        self.bands = sorted(bands, key=lambda b: b.start)
        if not self.bands:
            raise ValueError("price grid needs at least one band")

    @classmethod
    def from_market(cls, market: dict[str, Any]) -> PriceGrid:
        ranges = market.get("price_ranges")
        if ranges:
            return cls(PriceBand(D(r["start"]), D(r["end"]), D(r["step"])) for r in ranges)
        structure = market.get("price_level_structure") or "linear_cent"
        return cls.from_structure(structure)

    @classmethod
    def from_structure(cls, structure: str) -> PriceGrid:
        if structure not in _STRUCTURES:
            raise ValueError(f"unknown price_level_structure {structure!r}; pass price_ranges")
        return cls(PriceBand(D(a), D(b), D(s)) for a, b, s in _STRUCTURES[structure])

    def is_valid(self, price: Decimal) -> bool:
        if price <= ZERO or price >= ONE:
            return False
        if price % CENT == ZERO:
            return True  # whole cents are valid in every structure
        for band in self.bands:
            if band.start <= price <= band.end and (price - band.start) % band.step == ZERO:
                return True
        return False

    def tick_at(self, price: Decimal) -> Decimal:
        for band in self.bands:
            if band.start <= price < band.end:
                return band.step
        return self.bands[-1].step

    def snap(self, price: Decimal, direction: str = "down") -> Decimal:
        """Snap onto the grid (``down`` for bids/passive buys, ``up`` for asks)."""
        step = self.tick_at(price)
        band = next((b for b in self.bands if b.start <= price < b.end), self.bands[-1])
        offset = price - band.start
        snapped = band.start + (floor_to(offset, step) if direction == "down" else ceil_to(offset, step))
        return min(max(snapped, step), ONE - step)
