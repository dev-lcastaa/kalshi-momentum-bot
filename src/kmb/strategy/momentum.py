"""Momentum scalper for the 15-minute crypto markets.

Signal: the reference price (Coinbase last trade by default, or the CF index) moved more than
``min_move_bps`` over ``lookback_s``. Up-move → buy YES at the ask; down-move → buy NO (V2 ``ask`` at the
YES bid). Positions are closed on take-profit / stop-loss / max hold time / before close, with IOC orders.
Taker-only by design, so its simulated fills are reproducible from public data (fidelity G4 taker case).
"""

from __future__ import annotations

from collections import deque
from decimal import Decimal
from typing import Any

from kmb.envelope.windows import coin_for_ticker
from kmb.exchange import Fill, OrderRequest, OrderState
from kmb.kalshi.messages import ONE
from kmb.strategy.base import Context, Event, Intent, Place, Strategy

TERMINAL = ("executed", "canceled", "rejected")


class MomentumStrategy(Strategy):
    name = "momentum"
    description = "Taker momentum scalper: follow sharp reference-price moves, exit on TP/SL/time."
    param_schema: list[dict[str, Any]] = [
        {"name": "ref_source", "type": "str", "default": "coinbase", "help": "coinbase | cf"},
        {
            "name": "lookback_s",
            "type": "float",
            "default": 10,
            "min": 1,
            "max": 120,
            "help": "momentum window",
        },
        {
            "name": "min_move_bps",
            "type": "float",
            "default": 3,
            "min": 0.1,
            "max": 100,
            "help": "trigger size",
        },
        {"name": "size", "type": "decimal", "default": "1", "min": 0.01, "help": "contracts per entry"},
        {"name": "max_position", "type": "decimal", "default": "3", "min": 0.01, "help": "abs contracts"},
        {"name": "min_entry_price", "type": "decimal", "default": "0.15", "help": "min price paid"},
        {"name": "max_entry_price", "type": "decimal", "default": "0.85", "help": "max price paid"},
        {"name": "max_spread", "type": "decimal", "default": "0.04", "help": "skip wider books"},
        {"name": "take_profit", "type": "decimal", "default": "0.04", "help": "per-contract gain to exit"},
        {"name": "stop_loss", "type": "decimal", "default": "0.05", "help": "per-contract loss to exit"},
        {"name": "max_hold_s", "type": "float", "default": 120, "help": "exit after this long"},
        {"name": "no_entry_last_s", "type": "float", "default": 90, "help": "no entries this close to close"},
        {
            "name": "exit_before_close_s",
            "type": "float",
            "default": 30,
            "help": "flatten this close to close",
        },
        {"name": "hold_to_settle", "type": "bool", "default": False, "help": "skip pre-close flattening"},
        {"name": "cooldown_s", "type": "float", "default": 15, "help": "pause after each entry"},
    ]
    default_params = {p["name"]: p["default"] for p in param_schema}

    def __init__(self, params: dict[str, Any] | None = None):
        super().__init__(params)
        p = self.params
        self.lookback_ms = int(float(p["lookback_s"]) * 1000)
        self.min_move = Decimal(str(p["min_move_bps"])) / Decimal(10000)
        self.size = Decimal(str(p["size"]))
        self.max_pos = Decimal(str(p["max_position"]))
        self.min_px = Decimal(str(p["min_entry_price"]))
        self.max_px = Decimal(str(p["max_entry_price"]))
        self.max_spread = Decimal(str(p["max_spread"]))
        self.tp = Decimal(str(p["take_profit"]))
        self.sl = Decimal(str(p["stop_loss"]))
        self.max_hold_ms = int(float(p["max_hold_s"]) * 1000)
        self.no_entry_ms = int(float(p["no_entry_last_s"]) * 1000)
        self.exit_before_ms = int(float(p["exit_before_close_s"]) * 1000)
        self.hold_to_settle = bool(p["hold_to_settle"])
        self.cooldown_ms = int(float(p["cooldown_s"]) * 1000)
        self._hist: dict[str, deque[tuple[int, Decimal]]] = {}
        self._inflight: dict[str, str] = {}  # ticker -> client_order_id
        self._entry_ts: dict[str, int] = {}
        self._last_entry: dict[str, int] = {}

    def _ref_key(self, ticker: str) -> str | None:
        coin = coin_for_ticker(ticker)
        if coin is None:
            return None
        return coin.cf_index if self.params["ref_source"] == "cf" else coin.coinbase_pair

    def _momentum(self, ctx: Context, key: str) -> Decimal | None:
        price = ctx.reference.get(key)
        if price is None:
            return None
        h = self._hist.setdefault(key, deque())
        if not h or h[-1][1] != price or ctx.now_ms - h[-1][0] > 1000:
            h.append((ctx.now_ms, price))
        while len(h) > 1 and ctx.now_ms - h[1][0] >= self.lookback_ms:
            h.popleft()
        old_ts, old = h[0]
        if ctx.now_ms - old_ts < self.lookback_ms * 0.8 or old == 0:
            return None
        return (price - old) / old

    def on_event(self, ctx: Context, ev: Event) -> list[Intent]:
        intents: list[Intent] = []
        for ticker, info in ctx.markets.items():
            if (
                info.open_ms is None
                or info.close_ms is None
                or not (info.open_ms <= ctx.now_ms < info.close_ms)
            ):
                continue
            if info.status in ("paused", "determined", "settled") or ticker in self._inflight:
                continue
            book = ctx.book(ticker)
            if not book.valid:
                continue
            key = self._ref_key(ticker)
            mom = self._momentum(ctx, key) if key else None
            pos = ctx.position(ticker)
            to_close = info.close_ms - ctx.now_ms
            if pos != 0:
                intent = self._exit(ctx, ticker, pos, to_close)
                if intent:
                    intents.append(intent)
                continue
            if mom is None or to_close < self.no_entry_ms:
                continue
            if ctx.now_ms - self._last_entry.get(ticker, -(10**15)) < self.cooldown_ms:
                continue
            spread = book.spread()
            if spread is None or spread > self.max_spread:
                continue
            if mom >= self.min_move:
                ask = book.best_yes_ask()
                if ask and self.min_px <= ask[0] <= self.max_px:
                    intents.append(
                        self._place(ctx, ticker, "bid", ask[0], min(self.size, self.max_pos), "entry_yes")
                    )
            elif mom <= -self.min_move:
                bid = book.best_yes_bid()
                if bid and self.min_px <= ONE - bid[0] <= self.max_px:
                    intents.append(
                        self._place(ctx, ticker, "ask", bid[0], min(self.size, self.max_pos), "entry_no")
                    )
        return intents

    def _exit(self, ctx: Context, ticker: str, pos: Decimal, to_close: int) -> Intent | None:
        book = ctx.book(ticker)
        p = ctx.positions.get(ticker)
        if p is None or pos == 0:
            return None
        entry = p.cost / abs(pos) if pos != 0 else Decimal(0)
        held = ctx.now_ms - self._entry_ts.get(ticker, ctx.now_ms)
        time_exit = held >= self.max_hold_ms or (not self.hold_to_settle and to_close <= self.exit_before_ms)
        if pos > 0:
            bid = book.best_yes_bid()
            if bid is None:
                return None
            pnl = bid[0] - entry
            if pnl >= self.tp or pnl <= -self.sl or time_exit:
                return self._place(ctx, ticker, "ask", bid[0], pos, "exit_yes", reduce_only=True)
        else:
            ask = book.best_yes_ask()
            if ask is None:
                return None
            pnl = (ONE - ask[0]) - entry  # NO value = 1 - YES ask
            if pnl >= self.tp or pnl <= -self.sl or time_exit:
                return self._place(ctx, ticker, "bid", ask[0], -pos, "exit_no", reduce_only=True)
        return None

    def _place(
        self,
        ctx: Context,
        ticker: str,
        side: str,
        price: Decimal,
        count: Decimal,
        tag: str,
        reduce_only: bool = False,
    ) -> Place:
        req = OrderRequest(
            ticker=ticker,
            side=side,  # type: ignore[arg-type]
            price=price,
            count=count,
            time_in_force="immediate_or_cancel",
            reduce_only=reduce_only,
        )
        self._inflight[ticker] = req.client_order_id
        if tag.startswith("entry"):
            self._last_entry[ticker] = ctx.now_ms
        ctx.log(f"{tag} {ticker} {side} {count}@{price}")
        return Place(req, tag)

    def on_order_update(self, ctx: Context, order: OrderState) -> list[Intent]:
        if order.status in TERMINAL and self._inflight.get(order.ticker) == order.client_order_id:
            del self._inflight[order.ticker]
        return []

    def on_fill(self, ctx: Context, fill: Fill) -> list[Intent]:
        if fill.post_position == 0:
            self._entry_ts.pop(fill.ticker, None)
        elif fill.ticker not in self._entry_ts:
            self._entry_ts[fill.ticker] = fill.ts_ms
        return []
