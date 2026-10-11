"""Persist runner output (orders, order events, fills, settlements, logs, positions) to the shared database.

Writes are buffered and committed in batches so long sims stay fast. ``flush()`` is called by live jobs before
an order is sent, so the ``intent`` row is durable before Kalshi ever sees the request.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from kmb.db.models import (
    BotEvent,
    FillRow,
    Order,
    OrderEvent,
    PositionRow,
    Run,
    RunMetric,
    Settlement,
    utcnow,
)
from kmb.exchange import Fill, Notification, OrderState, Position, SettlementNote

log = logging.getLogger(__name__)

MAX_EVENTS_PER_RUN = 20_000  # strategy log lines beyond this are counted, not stored


def ms_to_dt(ms: int | None) -> datetime | None:
    return datetime.fromtimestamp(ms / 1000, tz=UTC) if ms else None


class DbRecorder:
    def __init__(
        self,
        factory: sessionmaker[Session],
        run_id: int,
        user_id: int,
        job_id: int | None,
        mode: str,
        *,
        batch: int = 500,
        sim_clock: bool = True,
    ):
        self.factory = factory
        self.run_id = run_id
        self.user_id = user_id
        self.job_id = job_id
        self.mode = mode
        self.batch = batch
        self.sim_clock = sim_clock
        self._s: Session = factory()
        self._orders: dict[str, Order] = {}
        self._pending_events: list[tuple[Order, str, dict[str, Any], int]] = []
        self._fill_keys: set[tuple[str, str]] = set()
        self._dirty = 0
        self.n_logs = 0
        self.dropped_logs = 0

    # ------------------------------------------------------------------------------------------- Recorder API

    def on_submit(self, order: OrderState, tag: str, now_ms: int) -> None:
        row = self._orders.get(order.client_order_id)
        if row is None:
            row = Order(
                run_id=self.run_id,
                user_id=self.user_id,
                mode=self.mode,
                client_order_id=order.client_order_id,
                order_id=order.order_id or None,
                ticker=order.ticker,
                side=order.side,
                price=order.price,
                initial_count=order.initial_count,
                fill_count=order.fill_count,
                remaining_count=order.remaining_count,
                time_in_force=order.time_in_force,
                post_only=order.post_only,
                status="rejected" if order.status == "rejected" else "intent",
                error=order.reject_message,
                tag=(tag or None) and tag[:64],
                sim_ts_ms=now_ms,
            )
            self._s.add(row)
            self._orders[order.client_order_id] = row
        self._event(row, "submit" if order.status != "rejected" else "rejected", order.to_dict(), now_ms)
        self._tick()

    def on_notification(self, note: Notification, now_ms: int) -> None:
        if isinstance(note, OrderState):
            self._order_update(note, now_ms)
        elif isinstance(note, Fill):
            self._fill(note)
        elif isinstance(note, SettlementNote):
            self._s.add(
                Settlement(
                    run_id=self.run_id,
                    user_id=self.user_id,
                    mode=self.mode,
                    ticker=note.ticker,
                    result=note.result,
                    position=note.position,
                    payout=note.payout,
                    pnl=note.pnl,
                    settled_at=(ms_to_dt(note.ts_ms) if self.sim_clock else None) or utcnow(),
                )
            )
        self._tick()

    def on_log(self, level: str, code: str, message: str, now_ms: int) -> None:
        self.n_logs += 1
        if self.n_logs > MAX_EVENTS_PER_RUN and level == "info":
            self.dropped_logs += 1
            return
        self._s.add(
            BotEvent(
                user_id=self.user_id,
                job_id=self.job_id,
                run_id=self.run_id,
                level=level,
                code=code[:64],
                message=message[:4000],
                detail={"ts_ms": now_ms},
                ts=(ms_to_dt(now_ms) if self.sim_clock else None) or utcnow(),
            )
        )
        self._tick()

    # ------------------------------------------------------------------------------------------- helpers

    def known(self, client_order_id: str) -> bool:
        return client_order_id in self._orders

    def adopt(self, row: Order) -> None:
        """Track an order row created by an earlier job (live reconciliation)."""
        row = self._s.merge(row)
        self._orders[row.client_order_id] = row

    def _event(self, row: Order, event: str, payload: dict[str, Any], now_ms: int) -> None:
        self._pending_events.append((row, event, payload, now_ms))

    def _order_update(self, o: OrderState, now_ms: int) -> None:
        row = self._orders.get(o.client_order_id)
        if row is None:
            row = self._s.scalar(select(Order).where(Order.client_order_id == o.client_order_id))
            if row is None:
                log.warning("order update for unknown client_order_id %s", o.client_order_id)
                return
            self._orders[o.client_order_id] = row
        if o.order_id:
            row.order_id = o.order_id
        row.fill_count = o.fill_count
        row.remaining_count = o.remaining_count
        row.initial_count = o.initial_count
        row.status = o.status
        if o.reject_message or o.reject_code:
            row.error = f"{o.reject_code or ''} {o.reject_message or ''}".strip()
        row.updated_at = utcnow()
        self._event(row, o.status, o.to_dict(), now_ms)

    def _fill(self, f: Fill) -> None:
        key = (f.trade_id, f.order_id)
        if key in self._fill_keys:
            return
        self._fill_keys.add(key)
        self._s.add(
            FillRow(
                run_id=self.run_id,
                user_id=self.user_id,
                mode=self.mode,
                trade_id=f.trade_id,
                order_id=f.order_id,
                client_order_id=f.client_order_id or None,
                ticker=f.ticker,
                side=f.side,
                price=f.price,
                count=f.count,
                fee=f.fee,
                is_taker=f.is_taker,
                post_position=f.post_position,
                ts_ms=f.ts_ms,
            )
        )

    def _tick(self) -> None:
        self._dirty += 1
        if self._dirty >= self.batch:
            self.flush()

    def flush(self) -> None:
        if self._pending_events:
            self._s.flush()  # assigns order primary keys
            for row, event, payload, ts in self._pending_events:
                self._s.add(OrderEvent(order_pk=row.id, event=event[:32], payload=payload, sim_ts_ms=ts))
            self._pending_events.clear()
        self._s.commit()
        self._dirty = 0

    # ------------------------------------------------------------------------------------------- run-level

    def write_positions(self, positions: dict[str, Position]) -> None:
        existing = {
            p.ticker: p for p in self._s.scalars(select(PositionRow).where(PositionRow.run_id == self.run_id))
        }
        for t, p in positions.items():
            row = existing.get(t)
            if row is None:
                row = PositionRow(run_id=self.run_id, user_id=self.user_id, mode=self.mode, ticker=t)
                self._s.add(row)
            row.position = p.position
            row.cost = p.cost
            row.realized_pnl = p.realized_pnl
            row.fees_paid = p.fees_paid
            row.updated_at = utcnow()

    def write_metrics(self, metrics: Iterable[tuple[str | None, str, Decimal]]) -> None:
        for window_id, name, value in metrics:
            self._s.add(RunMetric(run_id=self.run_id, window_id=window_id, name=name[:64], value=value))

    def update_run(self, **fields: Any) -> None:
        run = self._s.get(Run, self.run_id)
        if run is None:
            return
        for k, v in fields.items():
            setattr(run, k, v)

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._s.close()
