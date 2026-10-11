"""JSON serializers matching docs/api.md (decimals as strings, datetimes ISO-8601 UTC)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from kmb.db.models import (
    Bot,
    BotEvent,
    FidelityReport,
    FillRow,
    Job,
    KalshiKey,
    Order,
    PositionRow,
    RiskLimit,
    Run,
    Settlement,
    User,
    WorkerStatus,
)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def dec(v: Decimal | None) -> str | None:
    """Trailing zeros trimmed, but at least 2 decimals (``0.410000`` -> ``0.41``, ``120`` -> ``120.00``)."""
    if v is None:
        return None
    n = v.normalize()
    if n.as_tuple().exponent > -2:  # type: ignore[operator]
        n = n.quantize(Decimal("0.01"))
    return format(n, "f")


def jsonable(v: Any) -> Any:
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, datetime):
        return iso(v)
    if isinstance(v, dict):
        return {k: jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    return v


def user_json(u: User) -> dict[str, Any]:
    return {
        "id": u.id,
        "username": u.username,
        "role": u.role,
        "is_active": u.is_active,
        "live_enabled": u.live_enabled,
        "created_at": iso(u.created_at),
    }


def key_json(k: KalshiKey, leased_by_job_id: int | None) -> dict[str, Any]:
    return {
        "id": k.id,
        "label": k.label,
        "environment": k.environment,
        "api_key_id": k.api_key_id,
        "is_active": k.is_active,
        "created_at": iso(k.created_at),
        "last_used_at": iso(k.last_used_at),
        "leased_by_job_id": leased_by_job_id,
    }


def job_json(j: Job, run_id: int | None) -> dict[str, Any]:
    return {
        "id": j.id,
        "kind": j.kind,
        "status": j.status,
        "bot_id": j.bot_id,
        "spec": jsonable(j.spec),
        "worker_id": j.worker_id,
        "cancel_requested": j.cancel_requested,
        "error": j.error,
        "created_at": iso(j.created_at),
        "started_at": iso(j.started_at),
        "finished_at": iso(j.finished_at),
        "heartbeat_at": iso(j.heartbeat_at),
        "run_id": run_id,
    }


def bot_json(b: Bot, active_job: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "id": b.id,
        "name": b.name,
        "strategy": b.strategy,
        "params": jsonable(b.params),
        "series": list(b.series or []),
        "key_id": b.key_id,
        "created_at": iso(b.created_at),
        "updated_at": iso(b.updated_at),
        "active_job": active_job,
    }


def event_json(e: BotEvent) -> dict[str, Any]:
    return {
        "id": e.id,
        "level": e.level,
        "code": e.code,
        "message": e.message,
        "detail": jsonable(e.detail),
        "ts": iso(e.ts),
    }


def run_json(r: Run) -> dict[str, Any]:
    return {
        "id": r.id,
        "job_id": r.job_id,
        "bot_id": r.bot_id,
        "mode": r.mode,
        "strategy": r.strategy,
        "params": jsonable(r.params),
        "sim_config": jsonable(r.sim_config),
        "windows": list(r.windows or []),
        "status": r.status,
        "started_at": iso(r.started_at),
        "finished_at": iso(r.finished_at),
        "summary": jsonable(r.summary),
    }


def order_json(o: Order) -> dict[str, Any]:
    return {
        "client_order_id": o.client_order_id,
        "order_id": o.order_id,
        "ticker": o.ticker,
        "side": o.side,
        "price": dec(o.price),
        "initial_count": dec(o.initial_count),
        "fill_count": dec(o.fill_count),
        "remaining_count": dec(o.remaining_count),
        "time_in_force": o.time_in_force,
        "status": o.status,
        "error": o.error,
        "tag": o.tag,
        "created_at": iso(o.created_at),
        "sim_ts_ms": o.sim_ts_ms,
    }


def fill_json(f: FillRow) -> dict[str, Any]:
    return {
        "trade_id": f.trade_id,
        "order_id": f.order_id,
        "ticker": f.ticker,
        "side": f.side,
        "price": dec(f.price),
        "count": dec(f.count),
        "fee": dec(f.fee),
        "is_taker": f.is_taker,
        "post_position": dec(f.post_position),
        "ts_ms": f.ts_ms,
    }


def position_json(p: PositionRow) -> dict[str, Any]:
    return {
        "ticker": p.ticker,
        "position": dec(p.position),
        "cost": dec(p.cost),
        "realized_pnl": dec(p.realized_pnl),
        "fees_paid": dec(p.fees_paid),
    }


def settlement_json(s: Settlement) -> dict[str, Any]:
    return {
        "ticker": s.ticker,
        "result": s.result,
        "position": dec(s.position),
        "payout": dec(s.payout),
        "pnl": dec(s.pnl),
    }


def limits_json(r: RiskLimit) -> dict[str, Any]:
    return {
        "max_order_count": dec(r.max_order_count),
        "max_position_per_market": dec(r.max_position_per_market),
        "max_open_orders": r.max_open_orders,
        "max_daily_loss": dec(r.max_daily_loss),
        "max_notional_per_market": dec(r.max_notional_per_market),
        "max_orders_per_minute": r.max_orders_per_minute,
    }


def worker_json(w: WorkerStatus) -> dict[str, Any]:
    return {
        "worker_id": w.worker_id,
        "host": w.host,
        "current_job_id": w.current_job_id,
        "heartbeat_at": iso(w.heartbeat_at),
        "capabilities": list(w.capabilities or []),
    }


def fidelity_json(f: FidelityReport) -> dict[str, Any]:
    return {
        "id": f.id,
        "window_id": f.window_id,
        "gate": f.gate,
        "passed": f.passed,
        "metrics": jsonable(f.metrics),
        "created_at": iso(f.created_at),
    }


def window_json(w: dict[str, Any]) -> dict[str, Any]:
    """Normalize a data-API / window.json record to the UI ``window`` shape."""
    return {
        "window_id": w.get("window_id") or w.get("market_ticker"),
        "market_ticker": w.get("market_ticker"),
        "series_ticker": w.get("series_ticker"),
        "open_time": w.get("open_time"),
        "close_time": w.get("close_time"),
        "result": w.get("result"),
        "strike": None if w.get("strike") is None else str(w.get("strike")),
        "event_count": int(w.get("event_count") or 0),
        "gap_count": int(w.get("gap_count") or 0),
        "quality": w.get("quality") or "unknown",
        "size_bytes": int(w.get("size_bytes") or 0),
    }
