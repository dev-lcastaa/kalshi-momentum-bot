"""Exact Decimal order book for Kalshi binary markets, rebuilt from WS snapshots and deltas.

Kalshi books hold YES bids and NO bids. A YES ask at p is the NO bid at 1 - p (and vice versa), so
buying YES at p crosses NO bids priced >= 1 - p.

``BookManager`` consumes parsed WS messages (dicts) for many markets and tracks per-sid ``seq``.
A gap invalidates the books on that sid until the next snapshot arrives, so nothing ever trades or
reports on a book that is known to be wrong.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from kmb.kalshi.messages import ONE, ZERO, D


def _levels(raw: Any) -> dict[Decimal, Decimal]:
    out: dict[Decimal, Decimal] = {}
    for item in raw or []:
        price, count = D(str(item[0])), D(str(item[1]))
        if count > ZERO:
            out[price] = out.get(price, ZERO) + count
    return out


@dataclass
class OrderBook:
    ticker: str
    yes: dict[Decimal, Decimal] = field(default_factory=dict)  # YES bids: price -> size
    no: dict[Decimal, Decimal] = field(default_factory=dict)  # NO bids: price -> size
    valid: bool = False
    last_seq: int | None = None
    last_update_ms: int | None = None

    # --- mutation -------------------------------------------------------------------------------

    def apply_snapshot(self, msg: dict[str, Any], seq: int | None = None, ts_ms: int | None = None) -> None:
        self.yes = _levels(msg.get("yes_dollars_fp") or msg.get("yes_dollars"))
        self.no = _levels(msg.get("no_dollars_fp") or msg.get("no_dollars"))
        self.valid = True
        self.last_seq = seq
        self.last_update_ms = ts_ms

    def apply_delta(self, msg: dict[str, Any], seq: int | None = None, ts_ms: int | None = None) -> None:
        side = msg.get("side")
        price = D(str(msg.get("price_dollars")))
        delta = D(str(msg.get("delta_fp") if msg.get("delta_fp") is not None else msg.get("delta")))
        book = self.yes if side == "yes" else self.no if side == "no" else None
        if book is None:
            raise ValueError(f"unknown side {side!r}")
        new = book.get(price, ZERO) + delta
        if new > ZERO:
            book[price] = new
        else:
            if new < ZERO:
                self.valid = False  # negative size means we missed something
            book.pop(price, None)
        self.last_seq = seq
        self.last_update_ms = ts_ms

    def invalidate(self) -> None:
        self.valid = False

    # --- queries --------------------------------------------------------------------------------

    def best_yes_bid(self) -> tuple[Decimal, Decimal] | None:
        if not self.yes:
            return None
        p = max(self.yes)
        return p, self.yes[p]

    def best_no_bid(self) -> tuple[Decimal, Decimal] | None:
        if not self.no:
            return None
        p = max(self.no)
        return p, self.no[p]

    def best_yes_ask(self) -> tuple[Decimal, Decimal] | None:
        nb = self.best_no_bid()
        return None if nb is None else (ONE - nb[0], nb[1])

    def best_no_ask(self) -> tuple[Decimal, Decimal] | None:
        yb = self.best_yes_bid()
        return None if yb is None else (ONE - yb[0], yb[1])

    def mid(self) -> Decimal | None:
        b, a = self.best_yes_bid(), self.best_yes_ask()
        if b is None or a is None:
            return None
        return (b[0] + a[0]) / 2

    def spread(self) -> Decimal | None:
        b, a = self.best_yes_bid(), self.best_yes_ask()
        if b is None or a is None:
            return None
        return a[0] - b[0]

    def yes_bids(self, depth: int | None = None) -> list[tuple[Decimal, Decimal]]:
        out = sorted(self.yes.items(), key=lambda kv: kv[0], reverse=True)
        return out[:depth] if depth else out

    def yes_asks(self, depth: int | None = None) -> list[tuple[Decimal, Decimal]]:
        """YES asks derived from NO bids, best (lowest) first."""
        out = sorted(((ONE - p, s) for p, s in self.no.items()), key=lambda kv: kv[0])
        return out[:depth] if depth else out

    def size_at(self, side: str, price: Decimal) -> Decimal:
        return (self.yes if side == "yes" else self.no).get(price, ZERO)

    def imbalance(self, depth: int = 5) -> Decimal | None:
        bid = sum((s for _, s in self.yes_bids(depth)), ZERO)
        ask = sum((s for _, s in self.yes_asks(depth)), ZERO)
        total = bid + ask
        return None if total == ZERO else (bid - ask) / total

    def copy(self) -> OrderBook:
        return OrderBook(
            self.ticker, dict(self.yes), dict(self.no), self.valid, self.last_seq, self.last_update_ms
        )

    def to_dict(self, depth: int | None = None) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "valid": self.valid,
            "yes_bids": [[str(p), str(s)] for p, s in self.yes_bids(depth)],
            "yes_asks": [[str(p), str(s)] for p, s in self.yes_asks(depth)],
        }


class BookManager:
    """Books for many markets from one Kalshi WS connection's message stream.

    ``strict_seq`` tracks per-sid ``seq`` continuity (live use). Recorded windows only hold one market's
    frames, so the other markets' messages that share the sid look like gaps; replays turn it off because the
    normalizer already certifies continuity on the complete stream and reconnects arrive as transport events.
    """

    def __init__(self, strict_seq: bool = True) -> None:
        self.strict_seq = strict_seq
        self.books: dict[str, OrderBook] = {}
        self._sid_seq: dict[int, int] = {}
        self._sid_tickers: dict[int, set[str]] = {}
        self.gaps = 0

    def book(self, ticker: str) -> OrderBook:
        b = self.books.get(ticker)
        if b is None:
            b = self.books[ticker] = OrderBook(ticker)
        return b

    def reset_connection(self) -> None:
        """A new WS connection: sids/seqs restart and every book needs a fresh snapshot."""
        self._sid_seq.clear()
        self._sid_tickers.clear()
        for b in self.books.values():
            b.invalidate()

    def on_message(self, data: dict[str, Any], ts_ms: int | None = None) -> str | None:
        """Apply one parsed WS message. Returns the ticker whose book changed, if any."""
        msg_type = data.get("type")
        if msg_type not in ("orderbook_snapshot", "orderbook_delta"):
            return None
        msg = data.get("msg") or {}
        ticker = msg.get("market_ticker")
        if not ticker:
            return None
        sid, seq = data.get("sid"), data.get("seq")
        book = self.book(ticker)
        if isinstance(sid, int):
            self._sid_tickers.setdefault(sid, set()).add(ticker)
        if msg_type == "orderbook_snapshot":
            if isinstance(sid, int) and isinstance(seq, int):
                self._sid_seq[sid] = seq
            book.apply_snapshot(msg, seq, ts_ms)
            return ticker
        if self.strict_seq and isinstance(sid, int) and isinstance(seq, int):
            last = self._sid_seq.get(sid)
            self._sid_seq[sid] = seq
            if last is not None and seq != last + 1:
                self.gaps += 1
                for t in self._sid_tickers.get(sid, ()):
                    self.book(t).invalidate()
        if not book.valid:
            return None
        book.apply_delta(msg, seq, ts_ms)
        return ticker
