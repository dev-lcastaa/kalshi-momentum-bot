"""LiveExchange: the SimExchange-shaped adapter that lets ``SimRunner`` drive real Kalshi orders.

The runner is synchronous; Kalshi is not. ``submit`` / ``request_cancel`` only queue work and return the local
order state, ``pump()`` (awaited by the live job between events) performs the REST calls, and private-WS
messages (``user_orders`` + ``fill``) are turned into the same ``OrderState`` / ``Fill`` notifications the
simulator produces. Before every create-order call ``before_send`` runs so the job can make the ``intent``
row durable first; create-order is never retried automatically (a timeout becomes ``unknown`` and is
resolved from the private WS or the next REST reconciliation by ``client_order_id``).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal
from typing import Any

from kmb.book import BookManager
from kmb.exchange import Fill, Notification, OrderRequest, OrderState, PlaceResult, SettlementNote
from kmb.kalshi.messages import ZERO, D, PriceGrid
from kmb.kalshi.rest import KalshiRestClient
from kmb.sim.portfolio import Portfolio
from kmb.strategy.base import Event

log = logging.getLogger(__name__)

OPEN = ("pending", "resting")
FINAL = ("canceled", "executed", "rejected")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _dec(v: Any, default: Decimal = ZERO) -> Decimal:
    if v is None or v == "":
        return default
    try:
        return D(str(v))
    except Exception:
        return default


class LiveExchange:
    mode = "live"

    def __init__(
        self,
        rest: KalshiRestClient,
        books: BookManager,
        *,
        starting_balance: Decimal = ZERO,
        before_send: Callable[[], None] | None = None,
        clock: Callable[[], int] = _now_ms,
    ):
        self.rest = rest
        self.books = books
        self.before_send = before_send
        self.clock = clock
        self.portfolio = Portfolio(starting_balance)
        self.markets: dict[str, dict[str, Any]] = {}
        self._orders: dict[str, OrderState] = {}  # client_order_id -> state
        self._by_oid: dict[str, str] = {}  # order_id -> client_order_id
        self._requests: dict[str, OrderRequest] = {}
        self._to_send: list[str] = []
        self._to_cancel: list[str] = []  # client_order_ids
        self._notes: list[Notification] = []
        self._fills: list[Fill] = []
        self._fill_keys: set[tuple[str, str]] = set()
        self._settlements: list[SettlementNote] = []
        self._settled: set[str] = set()
        self._unknown_since: dict[str, int] = {}
        self.exchange_balance: Decimal | None = None
        self.errors = 0
        self.started_ms = clock()
        self._last_fill_ms = 0

    # ------------------------------------------------------------------------------ runner-facing (sync)

    def set_market(
        self, ticker: str, *, grid: PriceGrid | None = None, close_ms: int | None = None, **_: Any
    ) -> None:
        self.markets[ticker] = {"grid": grid, "close_ms": close_ms, "result": None}

    def submit(self, req: OrderRequest, now_ms: int) -> OrderState:
        order = OrderState(
            order_id="",
            client_order_id=req.client_order_id,
            ticker=req.ticker,
            side=req.side,
            price=req.price,
            initial_count=req.count,
            remaining_count=req.count,
            status="pending",
            time_in_force=req.time_in_force,
            post_only=req.post_only,
            created_ts_ms=now_ms,
            last_update_ts_ms=now_ms,
        )
        grid = (self.markets.get(req.ticker) or {}).get("grid")
        if grid is not None and not grid.is_valid(req.price):
            order.status = "rejected"
            order.reject_code = "INVALID_PRICE"
            order.reject_message = f"price {req.price} not on the market's tick grid"
        self._orders[req.client_order_id] = order
        self._requests[req.client_order_id] = req
        if order.status == "pending":
            self._to_send.append(req.client_order_id)
        return replace(order)

    def request_cancel(self, order_id: str, now_ms: int) -> None:
        coid = self._by_oid.get(order_id) or (order_id if order_id in self._orders else None)
        if coid and coid not in self._to_cancel:
            self._to_cancel.append(coid)

    def request_cancel_all(self, ticker: str | None, now_ms: int) -> None:
        for o in self._orders.values():
            if o.status in OPEN and (ticker is None or o.ticker == ticker):
                self.request_cancel(o.order_id or o.client_order_id, now_ms)

    def _drain(self) -> list[Notification]:
        out, self._notes = self._notes, []
        return out

    def before_event(self, ev: Event) -> list[Notification]:
        return self._drain()

    def after_event(self, ev: Event) -> list[Notification]:
        data = ev.data if isinstance(ev.data, dict) else {}
        msg = data.get("msg") if isinstance(data.get("msg"), dict) else {}
        if ev.kind == "frame" and ev.msg_type in ("market_lifecycle_v2", "market_lifecycle"):
            t = msg.get("market_ticker") or ev.market_ticker
            if (
                t
                and msg.get("event_type") in ("determined", "settled")
                and msg.get("result") in ("yes", "no", "void")
            ):
                self._settle(t, msg["result"], ev.ts_ms)
        return self._drain()

    def flush(self, now_ms: int) -> list[Notification]:
        return self._drain()

    def open_orders(self, ticker: str | None = None) -> list[OrderState]:
        return [
            replace(o)
            for o in self._orders.values()
            if o.status in OPEN and (ticker is None or o.ticker == ticker)
        ]

    def positions(self):
        return {k: replace(v) for k, v in self.portfolio.positions.items()}

    def balance(self) -> Decimal:
        return self.portfolio.cash

    def orders(self) -> list[OrderState]:
        return [replace(o) for o in self._orders.values()]

    def fills(self) -> list[Fill]:
        return list(self._fills)

    def settlements(self) -> list[SettlementNote]:
        return list(self._settlements)

    def has_work(self) -> bool:
        return bool(self._to_send or self._to_cancel)

    def summary(self) -> dict[str, Any]:
        realized = sum((p.realized_pnl for p in self.portfolio.positions.values()), ZERO)
        fees = sum((p.fees_paid for p in self.portfolio.positions.values()), ZERO)
        volume = sum((p.volume for p in self.portfolio.positions.values()), ZERO)
        return {
            "pnl": str(realized),
            "realized": str(realized),
            "fees": str(fees),
            "volume": str(volume),
            "n_orders": len(self._orders),
            "n_fills": len(self._fills),
            "exchange_balance": str(self.exchange_balance) if self.exchange_balance is not None else None,
            "errors": self.errors,
        }

    # ------------------------------------------------------------------------------ async side

    async def pump(self) -> None:
        """Send queued creates, then queued cancels. Called by the live job between events."""
        while self._to_send:
            coid = self._to_send.pop(0)
            order = self._orders[coid]
            if order.status != "pending" or coid in self._to_cancel:
                if coid in self._to_cancel and not order.order_id:
                    self._to_cancel.remove(coid)
                    self._update(
                        order, status="canceled", remaining_count=ZERO, reject_code="CANCELED_BEFORE_SEND"
                    )
                continue
            if self.before_send is not None:
                self.before_send()
            await self._create(order, self._requests[coid])
        pending = list(self._to_cancel)
        self._to_cancel.clear()
        for coid in pending:
            order = self._orders.get(coid)
            if order is None or order.status not in OPEN:
                continue
            if not order.order_id:
                self._to_cancel.append(coid)  # still unknown; retry after reconciliation
                continue
            await self._cancel(order)

    async def _create(self, order: OrderState, req: OrderRequest) -> None:
        try:
            resp = await self.rest.create_order(req.to_payload())
        except Exception as exc:  # network / timeout: the order may or may not exist
            self.errors += 1
            log.warning("create_order %s failed: %r", order.client_order_id, exc)
            self._unknown_since[order.client_order_id] = self.clock()
            self._notes.append(replace(order))
            return
        if not resp.ok:
            self.errors += 1
            code, message = _error(resp.text)
            self._update(
                order,
                status="rejected",
                remaining_count=ZERO,
                reject_code=code or str(resp.status),
                reject_message=message or resp.text[:300],
            )
            return
        body = resp.json() if resp.text else {}
        oid = str(body.get("order_id") or "")
        fill = _dec(body.get("fill_count"))
        remaining = _dec(body.get("remaining_count"), order.initial_count - fill)
        if oid:
            self._by_oid[oid] = order.client_order_id
        if order.status in FINAL:  # the private WS already delivered the final state
            if oid and not order.order_id:
                order.order_id = oid
            return
        status = "resting" if remaining > ZERO else ("executed" if fill > ZERO else "canceled")
        self._update(
            order,
            order_id=oid,
            fill_count=max(order.fill_count, fill),
            remaining_count=remaining,
            status=status,
            ts_ms=int(body.get("ts_ms") or self.clock()),
        )

    async def _cancel(self, order: OrderState) -> None:
        try:
            resp = await self.rest.cancel_order(order.order_id, order.ticker)
        except Exception as exc:
            self.errors += 1
            log.warning("cancel_order %s failed: %r", order.order_id, exc)
            self._to_cancel.append(order.client_order_id)
            return
        if resp.ok:
            self._update(order, status="canceled", remaining_count=ZERO)
        elif resp.status == 404:
            # already gone (filled or canceled); the private WS / reconciliation has the final state
            self._update(order, status="canceled", remaining_count=ZERO, reject_code="NOT_FOUND")
        else:
            self.errors += 1
            self._to_cancel.append(order.client_order_id)

    async def refresh_balance(self) -> Decimal | None:
        try:
            body = await self.rest.get_balance()
        except Exception as exc:
            log.warning("get_balance failed: %r", exc)
            return None
        bal = body.get("balance_dollars")
        self.exchange_balance = _dec(bal) if bal is not None else _dec(body.get("balance")) / 100
        return self.exchange_balance

    async def reconcile(self, tickers: list[str] | None = None) -> int:
        """Resolve orders whose create call timed out and resync resting orders from REST."""
        changed = 0
        try:
            body = await self.rest.get_orders(status="resting")
        except Exception as exc:
            log.warning("reconcile get_orders failed: %r", exc)
            return 0
        seen: set[str] = set()
        for raw in body.get("orders") or []:
            coid = raw.get("client_order_id")
            if coid in self._orders:
                seen.add(coid)
                self.on_user_order(raw)
                changed += 1
        now = self.clock()
        try:
            min_ts = (self._last_fill_ms // 1000 - 5) if self._last_fill_ms else (self.started_ms // 1000 - 5)
            fills = await self.rest.get_fills(min_ts=min_ts)
            for raw in reversed(fills.get("fills") or []):
                if (str(raw.get("trade_id") or ""), str(raw.get("order_id") or "")) in self._fill_keys:
                    continue
                if raw.get("client_order_id") not in self._orders and raw.get("order_id") not in self._by_oid:
                    continue  # manual trade on the account; not this job's
                self.on_fill({**raw, "market_ticker": raw.get("market_ticker") or raw.get("ticker")})
                changed += 1
        except Exception as exc:
            log.warning("reconcile get_fills failed: %r", exc)
        for coid, since in list(self._unknown_since.items()):
            order = self._orders[coid]
            if coid in seen or order.order_id:
                self._unknown_since.pop(coid, None)
            elif now - since > 30_000 and order.status == "pending":
                # not resting and never acknowledged: treat as not placed (fills, if any, still arrive via WS)
                self._update(
                    order,
                    status="canceled",
                    remaining_count=ZERO,
                    reject_code="UNKNOWN",
                    reject_message="create_order outcome unknown; not found resting after 30s",
                )
                self._unknown_since.pop(coid, None)
                changed += 1
        return changed

    # ------------------------------------------------------------------------------ private WS

    def on_ws_message(self, data: dict[str, Any]) -> None:
        mt = data.get("type")
        msg = data.get("msg") if isinstance(data.get("msg"), dict) else {}
        if mt == "user_order":
            self.on_user_order(msg)
        elif mt == "fill":
            self.on_fill(msg)

    def on_user_order(self, msg: dict[str, Any]) -> None:
        coid = msg.get("client_order_id") or self._by_oid.get(str(msg.get("order_id") or ""))
        order = self._orders.get(coid or "")
        if order is None:
            return  # not ours (manual order on the same account)
        oid = str(msg.get("order_id") or order.order_id)
        if oid:
            self._by_oid[oid] = order.client_order_id
        raw_status = msg.get("status")
        status = raw_status if raw_status in ("resting", "canceled", "executed") else order.status
        fill = _dec(msg.get("fill_count_fp", msg.get("fill_count")), order.fill_count)
        remaining = _dec(msg.get("remaining_count_fp", msg.get("remaining_count")), order.remaining_count)
        if order.status in FINAL and status not in FINAL:
            return  # stale update after a final state
        self._update(
            order,
            order_id=oid,
            status=status,
            fill_count=max(order.fill_count, fill),
            remaining_count=remaining,
            taker_fees=_dec(msg.get("taker_fees_dollars"), order.taker_fees),
            maker_fees=_dec(msg.get("maker_fees_dollars"), order.maker_fees),
            taker_fill_cost=_dec(msg.get("taker_fill_cost_dollars"), order.taker_fill_cost),
            maker_fill_cost=_dec(msg.get("maker_fill_cost_dollars"), order.maker_fill_cost),
            ts_ms=int(msg.get("last_updated_ts_ms") or self.clock()),
        )
        self._unknown_since.pop(order.client_order_id, None)

    def on_fill(self, msg: dict[str, Any]) -> None:
        trade_id, oid = str(msg.get("trade_id") or ""), str(msg.get("order_id") or "")
        if not trade_id or (trade_id, oid) in self._fill_keys:
            return
        self._fill_keys.add((trade_id, oid))
        coid = msg.get("client_order_id") or self._by_oid.get(oid) or ""
        side = msg.get("book_side")
        if side not in ("bid", "ask"):
            buy = msg.get("action") in ("buy", "")
            side = "bid" if (msg.get("side") == "yes") == buy else "ask"
        price = _dec(msg.get("yes_price_dollars"))
        count = _dec(msg.get("count_fp", msg.get("count")))
        fee = _dec(msg.get("fee_cost"))
        ticker = msg.get("market_ticker") or ""
        self.portfolio.apply_fill(ticker, side, price, count, fee)
        post = msg.get("post_position_fp")
        fill = Fill(
            trade_id=trade_id,
            order_id=oid,
            client_order_id=coid,
            ticker=ticker,
            side=side,
            price=price,
            count=count,
            fee=fee,
            is_taker=bool(msg.get("is_taker")),
            ts_ms=int(msg.get("ts_ms") or (int(msg.get("ts") or 0) * 1000) or self.clock()),
            post_position=_dec(post) if post is not None else self.portfolio.position(ticker).position,
        )
        self._fills.append(fill)
        self._notes.append(fill)
        self._last_fill_ms = max(self._last_fill_ms, fill.ts_ms)

    # ------------------------------------------------------------------------------ helpers

    def _update(self, order: OrderState, *, ts_ms: int | None = None, **fields: Any) -> None:
        for k, v in fields.items():
            setattr(order, k, v)
        order.last_update_ts_ms = ts_ms or self.clock()
        self._notes.append(replace(order))

    def _settle(self, ticker: str, result: str, ts_ms: int) -> None:
        if ticker in self._settled:
            return
        self._settled.add(ticker)
        if ticker in self.markets:
            self.markets[ticker]["result"] = result
        pos = self.portfolio.positions.get(ticker)
        if pos is None or (pos.position == ZERO and pos.cost == ZERO):
            return
        position, payout, pnl = self.portfolio.settle(ticker, result)
        note = SettlementNote(ticker, result, position, payout, pnl, ts_ms)
        self._settlements.append(note)
        self._notes.append(note)

    async def place_order(self, req: OrderRequest) -> PlaceResult:
        self.submit(req, self.clock())
        await self.pump()
        o = self._orders[req.client_order_id]
        return PlaceResult(
            o.status != "rejected",
            o.order_id or None,
            req.client_order_id,
            o.fill_count,
            o.remaining_count,
            ts_ms=o.last_update_ts_ms,
            error_code=o.reject_code,
            error_message=o.reject_message,
        )

    async def cancel_order(self, order_id: str, ticker: str) -> OrderState | None:
        self.request_cancel(order_id, self.clock())
        await self.pump()
        coid = self._by_oid.get(order_id)
        return replace(self._orders[coid]) if coid else None

    async def decrease_order(self, order_id: str, ticker: str, reduce_to: Decimal) -> OrderState | None:
        resp = await self.rest.decrease_order(order_id, ticker, reduce_to=str(reduce_to))
        coid = self._by_oid.get(order_id)
        if not resp.ok or not coid:
            return None
        order = self._orders[coid]
        self._update(order, remaining_count=min(order.remaining_count, reduce_to))
        return replace(order)


def _error(text: str) -> tuple[str | None, str | None]:
    import json

    try:
        body = json.loads(text)
    except ValueError:
        return None, None
    err = body.get("error") if isinstance(body, dict) else None
    if isinstance(err, dict):
        return err.get("code"), err.get("message") or err.get("details")
    return None, None


__all__ = ["LiveExchange"]
