"""Deterministic strategy runner on top of ``SimExchange`` (sim jobs on recorded windows, paper/live jobs
on the live bus).

Per event: (1) the sim processes order arrivals/cancels due by ``ev.ts_ms`` against the pre-event book,
(2) the event updates market state, (3) the sim reacts to it (trades vs resting orders, lifecycle,
settlement), (4) the strategy sees it. Every notification is routed to the strategy hooks and to the recorder.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from decimal import Decimal
from typing import Any, Protocol

from kmb.exchange import Fill, Notification, OrderRequest, OrderState, SettlementNote
from kmb.kalshi.messages import ONE, PriceGrid
from kmb.replay.state import MarketState
from kmb.strategy.base import Cancel, CancelAll, Context, Event, Intent, Place, Strategy

log = logging.getLogger(__name__)

Guard = Callable[[OrderRequest, Context], str | None]  # returns a reject reason or None


class Recorder(Protocol):
    def on_submit(self, order: OrderState, tag: str, now_ms: int) -> None: ...
    def on_notification(self, note: Notification, now_ms: int) -> None: ...
    def on_log(self, level: str, code: str, message: str, now_ms: int) -> None: ...


class NullRecorder:
    def on_submit(self, order: OrderState, tag: str, now_ms: int) -> None:
        pass

    def on_notification(self, note: Notification, now_ms: int) -> None:
        pass

    def on_log(self, level: str, code: str, message: str, now_ms: int) -> None:
        pass


class SimRunner:
    def __init__(
        self,
        strategy: Strategy,
        exchange: Any,  # kmb.sim.SimExchange
        state: MarketState | None = None,
        recorder: Recorder | None = None,
        guard: Guard | None = None,
        tickers: Iterable[str] | None = None,
        equity_every_ms: int = 1000,
        series: Iterable[str] | None = None,
    ):
        self.strategy = strategy
        self.exchange = exchange
        self.state = state or MarketState()
        if getattr(exchange, "books", None) is not self.state.books:
            raise ValueError("exchange must share the runner's BookManager (state.books)")
        self.recorder = recorder or NullRecorder()
        self.guard = guard
        self.tickers = set(tickers) if tickers else None
        self.series = set(series) if series else None
        self.equity_every_ms = equity_every_ms
        self.equity: list[tuple[int, Decimal]] = []
        self.start_balance = exchange.balance()
        self._registered: dict[str, tuple[int | None, str | None]] = {}
        self._tags: dict[str, str] = {}
        self._last_equity_ms: int | None = None
        self._started = False
        self.now_ms = 0
        self.stopped = False

    # ------------------------------------------------------------------------------------------- context

    def _wanted(self, ticker: str, series: str | None) -> bool:
        if self.tickers is not None and ticker not in self.tickers:
            return False
        if self.series is not None and (series or ticker.split("-", 1)[0]) not in self.series:
            return False
        return True

    def context(self) -> Context:
        markets = self.state.markets
        if self.tickers is not None or self.series is not None:
            markets = {t: m for t, m in markets.items() if self._wanted(t, m.series)}
        ctx = Context(
            now_ms=self.now_ms,
            books=self.state.books,
            markets=markets,
            positions=self.exchange.positions(),
            open_orders=self.exchange.open_orders(),
            reference=self.state.reference,
            reference_ts_ms=self.state.reference_ts_ms,
            balance=self.exchange.balance(),
            params=self.strategy.params,
        )
        return ctx

    def _flush_logs(self, ctx: Context) -> None:
        for line in ctx.logs:
            self.recorder.on_log("info", "strategy", line, self.now_ms)
        ctx.logs.clear()

    # ------------------------------------------------------------------------------------------- execution

    def execute(self, intents: Iterable[Intent], ctx: Context) -> None:
        for intent in intents:
            if isinstance(intent, Place):
                self._place(intent, ctx)
            elif isinstance(intent, Cancel):
                self.exchange.request_cancel(intent.order_id, self.now_ms)
            elif isinstance(intent, CancelAll):
                self.exchange.request_cancel_all(intent.ticker, self.now_ms)
        self._flush_logs(ctx)

    def _place(self, intent: Place, ctx: Context) -> None:
        req = intent.order
        if self.stopped:
            return
        reason = self.guard(req, ctx) if self.guard else None
        if reason:
            rejected = OrderState(
                order_id="",
                client_order_id=req.client_order_id,
                ticker=req.ticker,
                side=req.side,
                price=req.price,
                initial_count=req.count,
                status="rejected",
                time_in_force=req.time_in_force,
                created_ts_ms=self.now_ms,
                last_update_ts_ms=self.now_ms,
                reject_code="RISK_LIMIT",
                reject_message=reason,
            )
            self.recorder.on_submit(rejected, intent.tag, self.now_ms)
            self.recorder.on_log(
                "warn",
                "risk_reject",
                f"{req.ticker} {req.side} {req.count}@{req.price}: {reason}",
                self.now_ms,
            )
            self.execute(self.strategy.on_order_update(ctx, rejected), ctx)
            return
        order = self.exchange.submit(req, self.now_ms)
        self._tags[order.client_order_id] = intent.tag
        self.recorder.on_submit(order, intent.tag, self.now_ms)

    def dispatch(self, notes: Iterable[Notification]) -> None:
        for note in notes:
            self.recorder.on_notification(note, self.now_ms)
            ctx = self.context()
            if isinstance(note, OrderState):
                self.execute(self.strategy.on_order_update(ctx, note), ctx)
            elif isinstance(note, Fill):
                self.execute(self.strategy.on_fill(ctx, note), ctx)
            elif isinstance(note, SettlementNote):
                self.recorder.on_log(
                    "info", "settlement", f"{note.ticker} {note.result} pnl={note.pnl}", self.now_ms
                )

    # ------------------------------------------------------------------------------------------- driving

    def _sync_markets(self) -> None:
        for t, m in self.state.markets.items():
            if not self._wanted(t, m.series):
                continue
            key = (m.close_ms, m.status)
            if self._registered.get(t) == key:
                continue
            self._registered[t] = key
            known = getattr(self.exchange, "markets", None)
            if isinstance(known, dict) and t in known:
                known[t]["close_ms"] = m.close_ms  # keep the grid / paused / result the sim already has
            else:
                self.exchange.set_market(t, close_ms=m.close_ms)

    def register_market(self, market: dict[str, Any]) -> None:
        """Register REST market metadata (grid, fees) with the sim before replay starts."""
        self.state.update_market_from_rest(market)
        t = market.get("ticker")
        if not t:
            return
        grid = None
        try:
            grid = PriceGrid.from_market(market)
        except (ValueError, KeyError, TypeError):
            grid = None
        m = self.state.market(t)
        self.exchange.set_market(t, grid=grid, close_ms=m.close_ms)
        self._registered[t] = (m.close_ms, m.status)

    def start(self, now_ms: int) -> None:
        self.now_ms = now_ms
        self._started = True
        ctx = self.context()
        self.execute(self.strategy.on_start(ctx), ctx)

    def step(self, ev: Event) -> None:
        if not self._started:
            self.start(ev.ts_ms)
        self.now_ms = max(self.now_ms, ev.ts_ms)
        self.dispatch(self.exchange.before_event(ev))
        self.state.apply(ev)
        self._sync_markets()
        self.dispatch(self.exchange.after_event(ev))
        if not self.stopped:
            ctx = self.context()
            self.execute(self.strategy.on_event(ctx, ev), ctx)
        self._sample_equity()

    def tick(self, now_ms: int) -> None:
        """Advance the clock without a market event (live/paper idle): deliver pending notifications."""
        if not self._started:
            self.start(now_ms)
        self.now_ms = max(self.now_ms, now_ms)
        self.dispatch(self.exchange.flush(self.now_ms))
        self._sample_equity()

    def stop(self, now_ms: int | None = None, cancel_all: bool = True) -> None:
        if now_ms is not None:
            self.now_ms = max(self.now_ms, now_ms)
        ctx = self.context()
        self.execute(self.strategy.on_stop(ctx), ctx)
        self.stopped = True
        if cancel_all:
            self.exchange.request_cancel_all(None, self.now_ms)

    def finish(self, end_ms: int) -> dict[str, Any]:
        self.now_ms = max(self.now_ms, end_ms)
        self.dispatch(self.exchange.flush(self.now_ms))
        self._sample_equity(force=True)
        return self.summary()

    def run(self, events: Iterable[Event], end_ms: int | None = None) -> dict[str, Any]:
        last = 0
        for ev in events:
            self.step(ev)
            last = ev.ts_ms
        if not self.stopped:
            self.stop(end_ms or last)
        return self.finish(end_ms or last)

    # ------------------------------------------------------------------------------------------- results

    def mark_to_market(self) -> Decimal:
        value = Decimal(0)
        for t, p in self.exchange.positions().items():
            if p.position == 0:
                continue
            m = self.state.markets.get(t)
            if m is not None and m.result in ("yes", "no"):
                won = (m.result == "yes") == (p.position > 0)
                value += abs(p.position) if won else Decimal(0)
                continue
            book = self.state.books.book(t)
            if p.position > 0:
                bid = book.best_yes_bid()
                mark = bid[0] if bid else (book.mid() or Decimal(0))
                value += p.position * mark
            else:
                ask = book.best_yes_ask()
                mark = (ONE - ask[0]) if ask else (ONE - (book.mid() or ONE))
                value += -p.position * mark
        return value

    def equity_now(self) -> Decimal:
        return self.exchange.balance() + self.mark_to_market() - self.start_balance

    def _sample_equity(self, force: bool = False) -> None:
        if (
            force
            or self._last_equity_ms is None
            or self.now_ms - self._last_equity_ms >= self.equity_every_ms
        ):
            eq = self.equity_now()
            if force or not self.equity or self.equity[-1][1] != eq:
                self.equity.append((self.now_ms, eq))
            self._last_equity_ms = self.now_ms

    def summary(self) -> dict[str, Any]:
        base = dict(self.exchange.summary())
        base["equity_end"] = str(self.equity_now())
        peak = Decimal(0)
        mdd = Decimal(0)
        for _, eq in self.equity:
            peak = max(peak, eq)
            mdd = max(mdd, peak - eq)
        base["max_drawdown"] = str(mdd)
        return base
