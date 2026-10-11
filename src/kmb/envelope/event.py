"""Capture envelope: one JSON object per recorded event (collector, archive, normalizer, replay).

Ordering keys:
- ``local_event_seq`` is one counter per capture across *all* streams. It is the replay order (receive order).
- ``stream_seq`` is contiguous from 0 per (capture, stream) so a missing upload batch shows up as a hole.

``raw`` is the exact text received (WebSocket frame or REST body), never re-serialized. Convenience fields
(``market_ticker``, ``msg_type``, ``sid``, ``seq``) are extracted from it without changing it.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime
from typing import Any

from kmb.envelope.batch import SCHEMA_VERSION

STREAM_KALSHI_WS = "kalshi_ws"
STREAM_KALSHI_REST = "kalshi_rest"
STREAM_COINBASE_WS = "coinbase_ws"
STREAM_CONTROL = "control"
STREAMS = (STREAM_KALSHI_WS, STREAM_KALSHI_REST, STREAM_COINBASE_WS, STREAM_CONTROL)

KIND_FRAME = "frame"  # raw WebSocket frame text in `raw`
KIND_REST = "rest"  # REST response body in `raw`; request info in `data`
KIND_TRANSPORT = "transport"  # connect/disconnect/subscribe/gap/resync events in `data`
KIND_CONTROL = "control"  # session_start/session_end/clock/discovery notes in `data`
KINDS = (KIND_FRAME, KIND_REST, KIND_TRANSPORT, KIND_CONTROL)

SOURCE_KALSHI = "kalshi"
SOURCE_COINBASE = "coinbase"
SOURCE_COLLECTOR = "collector"


def ms_to_utc_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


class Sequencer:
    """Thread-safe allocator for (stream_seq, local_event_seq)."""

    def __init__(self, next_local: int = 0, next_by_stream: dict[str, int] | None = None):
        self._lock = threading.Lock()
        self._next_local = next_local
        self._next_by_stream = dict(next_by_stream or {})

    def next(self, stream: str) -> tuple[int, int]:
        with self._lock:
            stream_seq = self._next_by_stream.get(stream, 0)
            self._next_by_stream[stream] = stream_seq + 1
            local = self._next_local
            self._next_local += 1
            return stream_seq, local

    def state(self) -> dict[str, Any]:
        with self._lock:
            return {"next_local": self._next_local, "next_by_stream": dict(self._next_by_stream)}


def extract_fields(raw: str | None) -> dict[str, Any]:
    """Pull routing fields out of a Kalshi or Coinbase JSON frame without modifying it."""
    out: dict[str, Any] = {"market_ticker": None, "msg_type": None, "sid": None, "seq": None}
    if not raw:
        return out
    try:
        data = json.loads(raw)
    except ValueError:
        return out
    if not isinstance(data, dict):
        return out
    msg_type = data.get("type") or data.get("channel")
    out["msg_type"] = msg_type if isinstance(msg_type, str) else None
    if isinstance(data.get("sid"), int):
        out["sid"] = data["sid"]
    if isinstance(data.get("seq"), int):
        out["seq"] = data["seq"]
    msg = data.get("msg")
    if isinstance(msg, dict):
        ticker = msg.get("market_ticker") or msg.get("ticker") or msg.get("index_id")
        if isinstance(ticker, str):
            out["market_ticker"] = ticker
    elif isinstance(data.get("market"), dict) and isinstance(data["market"].get("ticker"), str):
        out["market_ticker"] = data["market"]["ticker"]
    elif isinstance(data.get("product_id"), str):  # Coinbase Exchange feed (match / ticker)
        out["market_ticker"] = data["product_id"]
    return out


def make_envelope(
    sequencer: Sequencer,
    *,
    capture_id: str,
    stream: str,
    kind: str,
    source: str,
    raw: str | None = None,
    data: dict[str, Any] | None = None,
    conn_id: str | None = None,
    recv_wall_ms: int | None = None,
    recv_mono_ns: int | None = None,
    market_ticker: str | None = None,
    msg_type: str | None = None,
) -> dict[str, Any]:
    if stream not in STREAMS:
        raise ValueError(f"unknown stream {stream!r}")
    if kind not in KINDS:
        raise ValueError(f"unknown kind {kind!r}")
    wall = recv_wall_ms if recv_wall_ms is not None else int(time.time() * 1000)
    mono = recv_mono_ns if recv_mono_ns is not None else time.monotonic_ns()
    stream_seq, local_seq = sequencer.next(stream)
    fields = extract_fields(raw) if kind in (KIND_FRAME, KIND_REST) else {}
    return {
        "schema_version": SCHEMA_VERSION,
        "capture_id": capture_id,
        "stream": stream,
        "stream_seq": stream_seq,
        "local_event_seq": local_seq,
        "kind": kind,
        "received_at_utc": ms_to_utc_iso(wall),
        "recv_wall_ms": wall,
        "recv_mono_ns": mono,
        "source": source,
        "conn_id": conn_id,
        "market_ticker": market_ticker or fields.get("market_ticker"),
        "msg_type": msg_type or fields.get("msg_type"),
        "sid": fields.get("sid"),
        "seq": fields.get("seq"),
        "raw": raw,
        "data": data,
    }


def dumps(envelope: dict[str, Any]) -> str:
    return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)
