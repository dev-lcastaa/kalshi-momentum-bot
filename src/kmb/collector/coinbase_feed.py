from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
import time
import uuid
from collections.abc import Callable
from typing import Any

from kmb.envelope import (
    KIND_FRAME,
    KIND_TRANSPORT,
    SOURCE_COINBASE,
    SOURCE_COLLECTOR,
    Sequencer,
    make_envelope,
)

logger = logging.getLogger(__name__)
URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCTS = ("BTC-USD", "ETH-USD")


class CoinbaseFeed:
    def __init__(
        self,
        emit: Callable[[dict[str, Any]], None],
        sequencer: Sequencer,
        capture_id: str,
        *,
        connector: Callable[[str], Any] | None = None,
        max_backoff: float = 30,
    ) -> None:
        self.emit = emit
        self.sequencer = sequencer
        self.capture_id = capture_id
        self.connector = connector or _connect
        self.max_backoff = max_backoff
        self.connected = False
        self.reconnects = 0
        self.last_event_mono: float | None = None
        self._stop = asyncio.Event()

    async def run(self) -> None:
        delay = 1.0
        while not self._stop.is_set():
            conn_id = uuid.uuid4().hex[:12]
            ws = None
            connected_at = time.monotonic()
            try:
                connection = self.connector(URL)
                ws = await connection if inspect.isawaitable(connection) else connection
                await ws.send(
                    json.dumps(
                        {
                            "type": "subscribe",
                            "product_ids": list(PRODUCTS),
                            "channels": ["matches", "ticker", "heartbeat"],
                        },
                        separators=(",", ":"),
                    )
                )
                self.connected = True
                connected_at = time.monotonic()
                self._transport("connected", conn_id)
                while not self._stop.is_set():
                    raw = await ws.recv()
                    text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                    try:
                        payload = json.loads(text)
                    except ValueError:
                        payload = {}
                    frame_type = payload.get("type") if isinstance(payload, dict) else None
                    if frame_type == "heartbeat":
                        continue
                    if frame_type in {"match", "last_match", "ticker", "subscriptions", "error"}:
                        wall, mono = int(time.time() * 1000), time.monotonic_ns()
                        self.last_event_mono = time.monotonic()
                        self.emit(
                            make_envelope(
                                self.sequencer,
                                capture_id=self.capture_id,
                                stream="coinbase_ws",
                                kind=KIND_FRAME,
                                source=SOURCE_COINBASE,
                                raw=text,
                                conn_id=conn_id,
                                recv_wall_ms=wall,
                                recv_mono_ns=mono,
                            )
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.reconnects += 1
                logger.warning("Coinbase WS disconnected: %s", exc)
                self._transport("disconnected", conn_id, reason=repr(exc))
                if time.monotonic() - connected_at > 60:
                    delay = 1.0
                else:
                    delay = min(delay * random.uniform(1.0, 1.3), self.max_backoff)
            finally:
                self.connected = False
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass
            if not self._stop.is_set():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except TimeoutError:
                    pass

    async def stop(self) -> None:
        self._stop.set()

    def _transport(self, event: str, conn_id: str, **extra: Any) -> None:
        wall, mono = int(time.time() * 1000), time.monotonic_ns()
        self.emit(
            make_envelope(
                self.sequencer,
                capture_id=self.capture_id,
                stream="coinbase_ws",
                kind=KIND_TRANSPORT,
                source=SOURCE_COLLECTOR,
                conn_id=conn_id,
                recv_wall_ms=wall,
                recv_mono_ns=mono,
                data={"event": event, "conn_id": conn_id, "wall_ms": wall, "mono_ns": mono, **extra},
            )
        )


async def _connect(url: str):
    import websockets

    return await websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None)
