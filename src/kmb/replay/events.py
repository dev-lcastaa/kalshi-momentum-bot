"""Turn capture records (Parquet rows or live bus envelopes) into strategy ``Event``s."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any

from kmb.strategy.base import Event


def _loads(value: Any) -> Any:
    if value is None or isinstance(value, dict | list):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def event_from_record(rec: dict[str, Any]) -> Event:
    """Build an Event from an envelope dict or a window Parquet row (same field names).

    ``data`` is the parsed frame for ``frame`` events, ``{"body": <parsed body>, "meta": <request info>}`` for
    ``rest`` events, and the event dict for ``transport`` / ``control`` events.
    """
    kind = rec.get("kind") or "frame"
    if kind in ("frame", "rest"):
        payload = _loads(rec.get("raw"))
        if kind == "rest":
            payload = {"body": payload, "meta": _loads(rec.get("data")) or {}}
    else:
        payload = _loads(rec.get("data")) or {}
    seq = rec.get("ord")
    if seq is None:
        seq = rec.get("local_event_seq") or 0
    return Event(
        ts_ms=int(rec.get("recv_wall_ms") or 0),
        stream=rec.get("stream") or "",
        kind=kind,
        msg_type=rec.get("msg_type"),
        market_ticker=rec.get("market_ticker"),
        data=payload,
        local_seq=int(seq),
    )


def events_from_records(records: Iterable[dict[str, Any]]) -> Iterator[Event]:
    for rec in records:
        yield event_from_record(rec)


def read_parquet_events(path: str) -> list[Event]:
    import pyarrow.parquet as pq

    rows = pq.read_table(path).to_pylist()
    rows.sort(key=lambda r: r.get("ord") or 0)
    return [event_from_record(r) for r in rows]
