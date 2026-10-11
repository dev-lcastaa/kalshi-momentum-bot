from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal
from typing import Any

from kmb.book import BookManager
from kmb.exchange import (
    Fill,
    Notification,
    OrderRequest,
    OrderState,
    PlaceResult,
    SettlementNote,
)
from kmb.kalshi.messages import ONE, ZERO, D, PriceGrid
from kmb.sim.fees import FeeAccumulator, FeeModel, fee_model_from_dict
from kmb.sim.latency import LatencyModel
from kmb.sim.matching import Level, crossing_levels
from kmb.sim.portfolio import Portfolio
from kmb.sim.queue import QUEUE_MODELS, queue_decrease
from kmb.sim.rules import market_closed, validate_request
from kmb.strategy.base import Event


@dataclass
class SimConfig:
    latency_ms: int = 150
    latency_jitter_ms: int = 0
    queue_model: str = "pro_rata"
    fee: FeeModel = field(default_factory=FeeModel)
    starting_balance: Decimal = Decimal("1000")
    seed: int = 0
    reject_after_close: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> SimConfig:
        values = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        if "starting_balance" in values:
            values["starting_balance"] = D(values["starting_balance"])
        if isinstance(values.get("fee"), dict):
            values["fee"] = fee_model_from_dict(values["fee"])
        return cls(**values)

    def to_dict(self) -> dict:
        return {
            "latency_ms": self.latency_ms,
            "latency_jitter_ms": self.latency_jitter_ms,
            "queue_model": self.queue_model,
            "fee": {k: str(v) if isinstance(v, Decimal) else v for k, v in asdict(self.fee).items()},
            "starting_balance": str(self.starting_balance),
            "seed": self.seed,
            "reject_after_close": self.reject_after_close,
        }


class SimExchange:
    mode = "sim"

    def __init__(self, config: SimConfig, books: BookManager):
        if config.queue_model not in QUEUE_MODELS:
            raise ValueError(f"unknown queue model {config.queue_model!r}")
        self.config = config
        self.books = books
        self.latency = LatencyModel(config.latency_ms, config.latency_jitter_ms, config.seed)
        self.portfolio = Portfolio(config.starting_balance)
        self.markets: dict[str, dict[str, Any]] = {}
        self._orders: dict[str, OrderState] = {}
        self._requests: dict[str, OrderRequest] = {}
        self._notifications: list[Notification] = []
        self._fills: list[Fill] = []
        self._settlements: list[SettlementNote] = []
        self._order_fees: dict[str, FeeAccumulator] = {}
        self._seq = 0
        self._trade_seq = 0
        self._scheduled: list[tuple[int, int, str, str]] = []
        self._overlays: dict[tuple[str, str, Decimal], Decimal] = {}
        self._queues: dict[str, tuple[Decimal, Decimal]] = {}
        self._pending_trades: dict[tuple[str, str, Decimal], Decimal] = {}
        self._pending_trade_ts: dict[tuple[str, str, Decimal], int] = {}
        self._now_ms = 0

    def set_market(
        self,
        ticker: str,
        *,
        grid: PriceGrid | None = None,
        close_ms: int | None = None,
        fee_type: str | None = None,
        fee_multiplier: Decimal | None = None,
    ) -> None:
        self.markets[ticker] = {
            "grid": grid or PriceGrid.from_structure("linear_cent"),
            "close_ms": close_ms,
            "fee_type": fee_type,
            "fee_multiplier": fee_multiplier,
            "paused": False,
            "result": None,
        }

    def _market(self, ticker: str) -> dict[str, Any]:
        if ticker not in self.markets:
            self.set_market(ticker)
        return self.markets[ticker]

    def _new_id(self) -> str:
        self._seq += 1
        return f"sim-{self._seq:08d}"

    def _copy(self, order: OrderState) -> OrderState:
        return replace(order)

    def _emit_order(self, order: OrderState) -> OrderState:
        self._notifications.append(self._copy(order))
        return self._copy(order)

    def submit(self, req: OrderRequest, now_ms: int) -> OrderState:
        self._now_ms = now_ms
        oid = self._new_id()
        mkt = self._market(req.ticker)
        closed = self.config.reject_after_close and market_closed(mkt["close_ms"], now_ms)
        code = validate_request(
            req, mkt["grid"], mkt["paused"], closed, self.portfolio.position(req.ticker).position
        )
        count = req.count
        if code is None:
            required = self._unit_cost(req.side, req.price) * count
            if required > self.portfolio.available(self._orders.values()):
                code = "INSUFFICIENT_BALANCE"
        expiration = req.expiration_time * 1000 if req.expiration_time is not None else None
        state = OrderState(
            order_id=oid,
            client_order_id=req.client_order_id,
            ticker=req.ticker,
            side=req.side,
            price=req.price,
            initial_count=count,
            remaining_count=count,
            status="pending" if code is None else "rejected",
            time_in_force=req.time_in_force,
            post_only=req.post_only,
            created_ts_ms=now_ms,
            last_update_ts_ms=now_ms,
            expiration_ts_ms=expiration,
            reject_code=code,
            reject_message=code,
        )
        self._orders[oid] = state
        self._requests[oid] = req
        self._order_fees[oid] = FeeAccumulator()
        if code is None:
            self._schedule(now_ms + self.latency.sample(), "arrival", oid)
        return self._emit_order(state)

    def _schedule(self, due: int, action: str, oid: str) -> None:
        self._scheduled.append((due, self._seq + len(self._scheduled), action, oid))
        self._scheduled.sort(key=lambda x: (x[0], x[1]))

    def request_cancel(self, order_id: str, now_ms: int) -> None:
        if order_id in self._orders:
            self._schedule(now_ms + self.latency.sample(), "cancel", order_id)

    def request_cancel_all(self, ticker: str | None, now_ms: int) -> None:
        for order in self.open_orders(ticker):
            self.request_cancel(order.order_id, now_ms)

    def before_event(self, ev: Event) -> list[Notification]:
        self._now_ms = ev.ts_ms
        self._notifications = []
        while self._scheduled and self._scheduled[0][0] <= ev.ts_ms:
            due, _, action, oid = self._scheduled.pop(0)
            self._now_ms = due
            if action == "arrival":
                self._arrive(oid, due)
            else:
                self._cancel(oid, due)
        self._now_ms = ev.ts_ms
        return list(self._notifications)

    def after_event(self, ev: Event) -> list[Notification]:
        self._now_ms = ev.ts_ms
        self._notifications = []
        data = ev.data if isinstance(ev.data, dict) else {}
        msg = data.get("msg") or {}
        ticker = msg.get("market_ticker") or ev.market_ticker
        if ev.stream == "kalshi_ws" and ev.kind == "frame":
            if ev.msg_type == "trade":
                self._on_trade(ticker, msg, ev.ts_ms)
            elif ev.msg_type == "orderbook_delta":
                self._on_delta(ticker, msg)
            elif ev.msg_type in ("market_lifecycle_v2", "market_lifecycle"):
                self._on_lifecycle(ticker, msg, ev.ts_ms)
            if ticker:
                self._cross_resting(ticker, ev.ts_ms)
        self._expire(ev.ts_ms)
        return list(self._notifications)

    def flush(self, now_ms: int) -> list[Notification]:
        self._notifications = []
        self._now_ms = now_ms
        while self._scheduled and self._scheduled[0][0] <= now_ms:
            due, _, action, oid = self._scheduled.pop(0)
            self._now_ms = due
            self._arrive(oid, due) if action == "arrival" else self._cancel(oid, due)
        self._now_ms = now_ms
        self._expire(now_ms)
        return list(self._notifications)

    def _unit_cost(self, side: str, price: Decimal) -> Decimal:
        return price if side == "bid" else ONE - price

    def _fee_model(self, ticker: str) -> FeeModel:
        mkt = self._market(ticker)
        return replace(
            self.config.fee,
            fee_type=mkt["fee_type"] or self.config.fee.fee_type,
            multiplier=mkt["fee_multiplier"]
            if mkt["fee_multiplier"] is not None
            else self.config.fee.multiplier,
        )

    def _arrive(self, oid: str, now_ms: int) -> None:
        order = self._orders[oid]
        if order.status != "pending":
            return
        req = self._requests[oid]
        mkt = self._market(order.ticker)
        code = validate_request(
            req,
            mkt["grid"],
            mkt["paused"],
            self.config.reject_after_close and market_closed(mkt["close_ms"], now_ms),
            self.portfolio.position(order.ticker).position,
        )
        if code:
            self._set_status(order, "rejected", now_ms, code)
            return
        levels = crossing_levels(self.books.book(order.ticker), order.side, order.price, self._overlays)
        own_crosses = []
        for resting in self.open_orders(order.ticker):
            if resting.status != "resting" or resting.side == order.side:
                continue
            book_side, book_price = self._resting_level(resting)
            is_cross = resting.price <= order.price if order.side == "bid" else resting.price >= order.price
            if is_cross:
                own_crosses.append(resting)
                if not any((lv.book_side, lv.book_price) == (book_side, book_price) for lv in levels):
                    yes_price = ONE - book_price if book_side == "no" else book_price
                    levels.append(Level(book_side, book_price, yes_price, ZERO))
        levels.sort(key=lambda level: level.book_price, reverse=True)
        crosses = any(level.size > ZERO for level in levels) or bool(own_crosses)
        if req.post_only and crosses:
            self._set_status(order, "rejected", now_ms, "POST_ONLY_CROSS")
            return
        if req.time_in_force == "fill_or_kill" and (
            sum((level.size for level in levels), ZERO) < order.remaining_count
            or (own_crosses and req.self_trade_prevention_type == "taker_at_cross")
        ):
            self._set_status(order, "canceled", now_ms)
            return
        if levels and order.remaining_count:
            self._match_taker(order, levels, now_ms)
            if order.status == "canceled":
                return
            if req.time_in_force != "good_till_canceled":
                if order.remaining_count:
                    self._set_status(order, "canceled", now_ms)
                elif order.status != "executed":
                    self._set_status(order, "executed", now_ms)
                return
        if req.time_in_force != "good_till_canceled":
            self._set_status(order, "canceled", now_ms)
            return
        if order.remaining_count:
            book = self.books.book(order.ticker)
            book_side = "yes" if order.side == "bid" else "no"
            book_price = order.price if order.side == "bid" else ONE - order.price
            level = (book_side, book_price)
            displayed = book.size_at(book_side, book_price) if book.valid else ZERO
            displayed = max(ZERO, displayed - self._overlays.get((order.ticker, book_side, book_price), ZERO))
            earlier = [
                o
                for o in self.open_orders(order.ticker)
                if o.order_id != oid
                and o.status == "resting"
                and o.side == order.side
                and self._resting_level(o) == level
            ]
            self._queues[oid] = (displayed + sum((o.remaining_count for o in earlier), ZERO), ZERO)
            order.status = "resting"
            order.last_update_ts_ms = now_ms
            self._emit_order(order)

    def _match_taker(self, order: OrderState, levels, now_ms: int) -> None:
        for level in levels:
            if order.remaining_count <= ZERO:
                break
            own = [
                o
                for o in self.open_orders(order.ticker)
                if o.order_id != order.order_id
                and o.status == "resting"
                and o.side != order.side
                and self._resting_level(o) == (level.book_side, level.book_price)
            ]
            if own:
                stp = self._requests[order.order_id].self_trade_prevention_type
                if stp == "taker_at_cross":
                    self._set_status(order, "canceled", now_ms)
                    return
                for resting in own:
                    self._cancel(resting.order_id, now_ms)
            available = max(ZERO, level.size)
            count = min(order.remaining_count, available)
            if count:
                self._overlays[(order.ticker, level.book_side, level.book_price)] = (
                    self._overlays.get((order.ticker, level.book_side, level.book_price), ZERO) + count
                )
                self._apply_fill(order, level.yes_price, count, True, now_ms)

    def _resting_level(self, order: OrderState) -> tuple[str, Decimal]:
        return ("yes", order.price) if order.side == "bid" else ("no", ONE - order.price)

    def _apply_fill(
        self, order: OrderState, price: Decimal, count: Decimal, taker: bool, now_ms: int
    ) -> None:
        model = self._fee_model(order.ticker)
        fee_price = price if order.side == "bid" else ONE - price
        base = model.taker_fee(fee_price, count) if taker else model.maker_fee(fee_price, count)
        revenue = -self._unit_cost(order.side, price) * count
        fee = self._order_fees[order.order_id].charge(model, revenue, base)
        order.fill_count += count
        order.remaining_count -= count
        unit_cost = self._unit_cost(order.side, price)
        if taker:
            order.taker_fill_cost += unit_cost * count
            order.taker_fees += fee
        else:
            order.maker_fill_cost += unit_cost * count
            order.maker_fees += fee
        position = self.portfolio.position(order.ticker).position + (count if order.side == "bid" else -count)
        self.portfolio.apply_fill(order.ticker, order.side, price, count, fee)
        self._trade_seq += 1
        fill = Fill(
            f"sim-trade-{self._trade_seq:08d}",
            order.order_id,
            order.client_order_id,
            order.ticker,
            order.side,
            price,
            count,
            fee,
            taker,
            now_ms,
            position,
        )
        self._fills.append(fill)
        order.last_update_ts_ms = now_ms
        if order.remaining_count == ZERO:
            order.status = "executed"
        self._emit_order(order)
        self._notifications.append(fill)

    def _set_status(self, order: OrderState, status: str, now_ms: int, code: str | None = None) -> None:
        order.status = status
        order.last_update_ts_ms = now_ms
        if code:
            order.reject_code = code
            order.reject_message = code
        self._queues.pop(order.order_id, None)
        self._emit_order(order)

    def _release_queue_priority(self, order: OrderState, count: Decimal) -> None:
        level = self._resting_level(order)
        for follower in self.open_orders(order.ticker):
            if follower.order_id == order.order_id or follower.status != "resting":
                continue
            if follower.side != order.side or self._resting_level(follower) != level:
                continue
            ahead, behind = self._queues.get(follower.order_id, (ZERO, ZERO))
            self._queues[follower.order_id] = (max(ZERO, ahead - count), behind)

    def _cancel(self, oid: str, now_ms: int) -> None:
        order = self._orders.get(oid)
        if order and order.status in ("pending", "resting"):
            if order.status == "resting":
                self._release_queue_priority(order, order.remaining_count)
            self._set_status(order, "canceled", now_ms)

    def _on_trade(self, ticker: str | None, msg: dict, ts_ms: int) -> None:
        if not ticker:
            return
        count = D(str(msg.get("count_fp", msg.get("count", "0"))))
        taker_side = msg.get("taker_side")
        if taker_side == "yes":
            book_side, price, hit_side = "no", D(str(msg.get("no_price_dollars"))), "ask"
        elif taker_side == "no":
            book_side, price, hit_side = "yes", D(str(msg.get("yes_price_dollars"))), "bid"
        else:
            return
        key = (ticker, book_side, price)
        previous_ts = self._pending_trade_ts.get(key)
        if previous_ts is not None and ts_ms - previous_ts > 1000:
            self._pending_trades.pop(key, None)
        self._pending_trades[key] = self._pending_trades.get(key, ZERO) + count
        self._pending_trade_ts[key] = ts_ms
        for order in self._orders.values():
            if order.ticker != ticker or order.status != "resting" or order.side != hit_side:
                continue
            if self._resting_level(order) != (book_side, price):
                continue
            ahead, behind = self._queues.get(order.order_id, (ZERO, ZERO))
            used = min(ahead, count)
            ahead -= used
            available = count - used
            self._queues[order.order_id] = (ahead, behind)
            if available > ZERO:
                self._apply_fill(order, order.price, min(order.remaining_count, available), False, ts_ms)

    def _on_delta(self, ticker: str | None, msg: dict) -> None:
        if not ticker:
            return
        side = msg.get("side")
        price = D(str(msg.get("price_dollars")))
        delta = D(str(msg.get("delta_fp", msg.get("delta", "0"))))
        book = self.books.book(ticker)
        key = (ticker, side, price)
        side_for_orders = "bid" if side == "yes" else "ask"
        level = (side, price)
        if delta < ZERO:
            reduced = -delta
            pending_ts = self._pending_trade_ts.get(key)
            if pending_ts is not None and self._now_ms - pending_ts > 1000:
                self._pending_trades.pop(key, None)
                self._pending_trade_ts.pop(key, None)
            attributed = min(reduced, self._pending_trades.get(key, ZERO))
            pending = self._pending_trades.get(key, ZERO) - attributed
            if pending:
                self._pending_trades[key] = pending
            else:
                self._pending_trades.pop(key, None)
                self._pending_trade_ts.pop(key, None)
            nontrade = reduced - attributed
            for order in self.open_orders(ticker):
                if order.status != "resting" or order.side != side_for_orders:
                    continue
                if self._resting_level(order) != level:
                    continue
                ahead, behind = self._queues.get(order.order_id, (ZERO, ZERO))
                next_ahead = queue_decrease(ahead, behind, nontrade, self.config.queue_model)
                removed_ahead = ahead - next_ahead
                next_behind = max(ZERO, behind - max(ZERO, nontrade - removed_ahead))
                self._queues[order.order_id] = (next_ahead, next_behind)
        elif delta > ZERO:
            for order in self.open_orders(ticker):
                if order.status != "resting" or order.side != side_for_orders:
                    continue
                if self._resting_level(order) != level:
                    continue
                ahead, behind = self._queues.get(order.order_id, (ZERO, ZERO))
                self._queues[order.order_id] = (ahead, behind + delta)
        overlay = self._overlays.get(key, ZERO)
        current = book.size_at(side, price)
        if overlay > current:
            if current > ZERO:
                self._overlays[key] = current
            else:
                self._overlays.pop(key, None)

    def _cross_resting(self, ticker: str, ts_ms: int) -> None:
        book = self.books.book(ticker)
        if not book.valid:
            return
        for order in self._orders.values():
            if order.ticker != ticker or order.status != "resting":
                continue
            levels = crossing_levels(book, order.side, order.price, self._overlays)
            for level in levels:
                count = min(order.remaining_count, level.size)
                if count <= ZERO:
                    continue
                self._release_queue_priority(order, count)
                self._apply_fill(order, order.price, count, False, ts_ms)
                break

    def _on_lifecycle(self, ticker: str | None, msg: dict, ts_ms: int) -> None:
        if not ticker:
            return
        market = self._market(ticker)
        event_type = msg.get("event_type")
        if event_type == "deactivated":
            market["paused"] = True
            for order in self.open_orders(ticker):
                if self._requests[order.order_id].cancel_order_on_pause:
                    self._cancel(order.order_id, ts_ms)
        elif event_type == "activated":
            market["paused"] = False
            for order in self.open_orders(ticker):
                if order.status == "resting":
                    self._cancel(order.order_id, ts_ms)
        elif event_type in ("determined", "settled"):
            result = msg.get("result")
            if result in ("yes", "no", "void"):
                market["result"] = result
                self._settle(ticker, result, ts_ms)

    def _settle(self, ticker: str, result: str, ts_ms: int) -> None:
        position, payout, pnl = self.portfolio.settle(ticker, result)
        note = SettlementNote(ticker, result, position, payout, pnl, ts_ms)
        self._settlements.append(note)
        self._notifications.append(note)
        for order in self.open_orders(ticker):
            self._cancel(order.order_id, ts_ms)

    def _expire(self, now_ms: int) -> None:
        for order in list(self._orders.values()):
            if (
                order.status in ("pending", "resting")
                and order.expiration_ts_ms is not None
                and order.expiration_ts_ms <= now_ms
            ):
                self._cancel(order.order_id, now_ms)

    def open_orders(self, ticker: str | None = None) -> list[OrderState]:
        return [
            self._copy(o)
            for o in self._orders.values()
            if o.status in ("pending", "resting") and (ticker is None or o.ticker == ticker)
        ]

    def positions(self):
        return {k: replace(v) for k, v in self.portfolio.positions.items()}

    def balance(self) -> Decimal:
        return self.portfolio.cash

    def orders(self) -> list[OrderState]:
        return [self._copy(o) for o in self._orders.values()]

    def fills(self) -> list[Fill]:
        return list(self._fills)

    def settlements(self) -> list[SettlementNote]:
        return list(self._settlements)

    def summary(self) -> dict:
        realized = sum((p.realized_pnl for p in self.portfolio.positions.values()), ZERO)
        fees = sum((p.fees_paid for p in self.portfolio.positions.values()), ZERO)
        volume = sum((p.volume for p in self.portfolio.positions.values()), ZERO)
        unrealized = ZERO
        for ticker, pos in self.portfolio.positions.items():
            mid = self.books.book(ticker).mid()
            if mid is not None:
                market_value = pos.position * mid if pos.position > ZERO else abs(pos.position) * (ONE - mid)
                unrealized += market_value - pos.cost
        return {
            "pnl": str(realized + unrealized),
            "fees": str(fees),
            "n_orders": len(self._orders),
            "n_fills": len(self._fills),
            "volume": str(volume),
            "realized": str(realized),
            "unrealized_at_mid": str(unrealized),
        }

    async def place_order(self, req: OrderRequest) -> PlaceResult:
        order = self.submit(req, self._now_ms)
        return PlaceResult(
            order.status != "rejected",
            order.order_id,
            req.client_order_id,
            order.fill_count,
            order.remaining_count,
            ts_ms=self._now_ms,
            error_code=order.reject_code,
        )

    async def cancel_order(self, order_id: str, ticker: str):
        self.request_cancel(order_id, self._now_ms)
        return next((o for o in self.open_orders(ticker) if o.order_id == order_id), None)

    async def decrease_order(self, order_id: str, ticker: str, reduce_to: Decimal):
        order = self._orders.get(order_id)
        if order is None or order.ticker != ticker or order.status != "resting":
            return None
        if reduce_to >= order.remaining_count or reduce_to < ZERO:
            return self._copy(order)
        order.remaining_count = reduce_to
        order.initial_count = order.fill_count + reduce_to
        order.last_update_ts_ms = self._now_ms
        return self._emit_order(order)
