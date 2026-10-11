from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from kmb.strategy.base import Event


@dataclass(frozen=True)
class _ParquetRestEvent(Event):
    envelope_data: dict | None = None


def _json(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def event_from_row(row: dict) -> Event:
    stream, kind = row.get("stream", ""), row.get("kind", "")
    envelope_data = _json(row.get("data"))
    raw_data = _json(row.get("raw")) if kind in ("frame", "rest") else envelope_data
    kwargs = {
        "ts_ms": int(row.get("recv_wall_ms") or 0),
        "stream": stream,
        "kind": kind,
        "msg_type": row.get("msg_type"),
        "market_ticker": row.get("market_ticker"),
        "data": raw_data,
        "local_seq": int(row.get("local_event_seq") or 0),
    }
    if stream == "kalshi_rest" and kind == "rest":
        return _ParquetRestEvent(**kwargs, envelope_data=envelope_data)
    return Event(**kwargs)


def events_from_parquet(path: str | Path) -> Iterator[Event]:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches():
        for row in batch.to_pylist():
            yield event_from_row(row)
