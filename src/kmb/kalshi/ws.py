"""Async WebSocket client for Kalshi's multiplexed v2 connection.

Built for exact capture:
- every inbound frame is handed to `on_frame` (raw text + receive timestamps + conn_id) before parsing;
- connects, disconnects, outbound commands and sequence gaps are reported to `on_transport`;
- subscriptions are declarative: they are re-sent on every reconnect and their server `sid`s are tracked so
  `add_markets`/`remove_markets` can use `update_subscription`;
- per-sid `seq` gaps trigger a reconnect by default, which forces fresh snapshots.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import logging
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import websockets

from .auth import KalshiAuth
from .endpoints import WS_PATH

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WsFrame:
    conn_id: str
    text: str
    recv_wall_ms: int
    recv_mono_ns: int


@dataclass
class Subscription:
    """A desired subscription. `sids` holds the server ids once acknowledged (one per channel)."""

    key: str
    channels: list[str]
    market_tickers: list[str] | None = None
    index_ids: list[str] | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    sids: dict[str, int] = field(default_factory=dict)

    def params(self) -> dict[str, Any]:
        params: dict[str, Any] = {"channels": list(self.channels), **self.extra}
        if self.market_tickers:
            params["market_tickers"] = list(self.market_tickers)
        if self.index_ids:
            params["index_ids"] = list(self.index_ids)
        return params


class WsConnection(Protocol):
    async def send(self, message: str) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def close(self) -> None: ...


Connector = Callable[[str, dict[str, str]], Awaitable[WsConnection]]
FrameObserver = Callable[[WsFrame], None]
TransportObserver = Callable[[dict[str, Any]], None]
MessageHandler = Callable[[dict[str, Any], WsFrame], Awaitable[None]]
GapHandler = Callable[[int, int, int], bool]


async def _default_connector(url: str, headers: dict[str, str]) -> WsConnection:
    return await websockets.connect(
        url, additional_headers=headers, ping_interval=10, ping_timeout=10, max_size=None, close_timeout=5
    )


class GapDetected(Exception):
    pass


class KalshiWsClient:
    def __init__(
        self,
        ws_url: str,
        auth: KalshiAuth | None,
        *,
        on_message: MessageHandler | None = None,
        on_frame: FrameObserver | None = None,
        on_transport: TransportObserver | None = None,
        on_gap: GapHandler | None = None,
        idle_timeout: float | None = None,
        max_backoff: float = 30.0,
        connector: Connector = _default_connector,
    ):
        self._ws_url = ws_url
        self._auth = auth
        self._on_message = on_message
        self._on_frame = on_frame
        self._on_transport = on_transport
        self._on_gap = on_gap
        self._idle_timeout = idle_timeout
        self._max_backoff = max_backoff
        self._connector = connector
        self._ids = itertools.count(1)
        self._subs: dict[str, Subscription] = {}
        self._pending_cmds: dict[int, tuple[str, str | None]] = {}
        self._seq_by_sid: dict[int, int] = {}
        self._ws: WsConnection | None = None
        self._conn_id: str | None = None
        self._stop = asyncio.Event()
        self._connected = asyncio.Event()

    # --- public API -----------------------------------------------------------------------------

    @property
    def conn_id(self) -> str | None:
        return self._conn_id

    @property
    def connected(self) -> asyncio.Event:
        return self._connected

    @property
    def subscriptions(self) -> dict[str, Subscription]:
        return self._subs

    async def subscribe(self, sub: Subscription) -> None:
        """Register a subscription; it is sent now if connected, and on every reconnect."""
        if sub.key in self._subs:
            raise ValueError(f"Subscription {sub.key!r} already exists")
        self._subs[sub.key] = sub
        if self._ws is not None:
            await self._send_subscribe(sub)

    async def add_markets(self, key: str, tickers: list[str]) -> None:
        await self._update_markets(key, tickers, "add_markets")

    async def remove_markets(self, key: str, tickers: list[str]) -> None:
        await self._update_markets(key, tickers, "delete_markets")

    async def send_command(self, cmd: str, params: dict[str, Any], *, sub_key: str | None = None) -> int:
        if self._ws is None:
            raise ConnectionError("Kalshi WS is not connected")
        cmd_id = next(self._ids)
        text = json.dumps({"id": cmd_id, "cmd": cmd, "params": params}, separators=(",", ":"))
        self._pending_cmds[cmd_id] = (cmd, sub_key)
        self._emit({"event": "sent", "cmd_id": cmd_id, "cmd": cmd, "text": text})
        await self._ws.send(text)
        return cmd_id

    def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._run_once()
                backoff = 1.0
            except GapDetected:
                backoff = 1.0
            except (websockets.ConnectionClosed, OSError, TimeoutError) as exc:
                logger.warning("Kalshi WS disconnected (%s)", exc)
            except Exception as exc:
                logger.exception("Kalshi WS crashed")
                self._emit({"event": "error", "error": repr(exc)})
            if self._stop.is_set():
                return
            delay = min(backoff, self._max_backoff) * random.uniform(0.8, 1.2)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            backoff = min(backoff * 2, self._max_backoff)

    # --- internals ------------------------------------------------------------------------------

    def _emit(self, event: dict[str, Any]) -> None:
        if self._on_transport is None:
            return
        event = {
            "conn_id": self._conn_id,
            "wall_ms": int(time.time() * 1000),
            "mono_ns": time.monotonic_ns(),
            **event,
        }
        try:
            self._on_transport(event)
        except Exception:
            logger.exception("on_transport observer failed")

    async def _update_markets(self, key: str, tickers: list[str], action: str) -> None:
        sub = self._subs[key]
        current = list(sub.market_tickers or [])
        if action == "add_markets":
            changed = [t for t in tickers if t not in current]
            sub.market_tickers = current + changed
        else:
            changed = [t for t in tickers if t in current]
            sub.market_tickers = [t for t in current if t not in changed]
        if not changed or self._ws is None or not sub.sids:
            return
        params = {"sids": sorted(sub.sids.values()), "market_tickers": changed, "action": action}
        await self.send_command("update_subscription", params, sub_key=key)

    async def _send_subscribe(self, sub: Subscription) -> None:
        sub.sids.clear()
        await self.send_command("subscribe", sub.params(), sub_key=sub.key)

    async def _recv(self, ws: WsConnection) -> str | bytes:
        if self._idle_timeout is None:
            return await ws.recv()
        return await asyncio.wait_for(ws.recv(), timeout=self._idle_timeout)

    async def _run_once(self) -> None:
        self._conn_id = uuid.uuid4().hex[:12]
        self._seq_by_sid.clear()
        self._pending_cmds.clear()
        headers = self._auth.headers("GET", WS_PATH) if self._auth is not None else {}
        self._emit({"event": "connecting", "url": self._ws_url})
        ws = await self._connector(self._ws_url, headers)
        self._ws = ws
        self._connected.set()
        self._emit({"event": "connected", "url": self._ws_url})
        reason = "closed"
        stop_task = asyncio.ensure_future(self._stop.wait())
        try:
            for sub in self._subs.values():
                await self._send_subscribe(sub)
            while True:
                recv_task = asyncio.ensure_future(self._recv(ws))
                done, _ = await asyncio.wait({recv_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
                if stop_task in done:
                    recv_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await recv_task
                    reason = "stopped"
                    return
                await self._handle_raw(recv_task.result())
        except GapDetected:
            reason = "seq_gap"
            raise
        except BaseException as exc:
            reason = repr(exc)
            raise
        finally:
            stop_task.cancel()
            self._ws = None
            self._connected.clear()
            for sub in self._subs.values():
                sub.sids.clear()
            with contextlib.suppress(Exception):
                await ws.close()
            self._emit({"event": "disconnected", "reason": reason})

    async def _handle_raw(self, raw: str | bytes) -> None:
        text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
        frame = WsFrame(self._conn_id or "", text, int(time.time() * 1000), time.monotonic_ns())
        if self._on_frame is not None:
            try:
                self._on_frame(frame)
            except Exception:
                logger.exception("on_frame observer failed")
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("Non-JSON WS frame: %r", text[:200])
            return
        if not isinstance(data, dict):
            return
        self._track_control(data)
        self._check_sequence(data)
        if self._on_message is not None:
            await self._on_message(data, frame)

    def _track_control(self, data: dict[str, Any]) -> None:
        msg_type = data.get("type")
        cmd_id = data.get("id")
        pending = self._pending_cmds.get(cmd_id) if isinstance(cmd_id, int) else None
        if msg_type == "subscribed" and pending is not None:
            msg = data.get("msg") or {}
            sub = self._subs.get(pending[1] or "")
            if sub is not None and "sid" in msg:
                sub.sids[str(msg.get("channel"))] = int(msg["sid"])
        elif msg_type == "error":
            self._emit(
                {
                    "event": "command_error",
                    "cmd_id": cmd_id,
                    "cmd": pending[0] if pending else None,
                    "msg": data.get("msg"),
                }
            )
        if msg_type in {"ok", "error", "unsubscribed"} and isinstance(cmd_id, int):
            self._pending_cmds.pop(cmd_id, None)

    def _check_sequence(self, data: dict[str, Any]) -> None:
        sid, seq = data.get("sid"), data.get("seq")
        if not isinstance(sid, int) or not isinstance(seq, int):
            return
        if data.get("type") == "orderbook_snapshot":
            self._seq_by_sid[sid] = seq
            return
        last = self._seq_by_sid.get(sid)
        self._seq_by_sid[sid] = seq
        if last is None or seq == last + 1:
            return
        expected = last + 1
        logger.warning("Sequence gap on sid=%s: expected %s, got %s", sid, expected, seq)
        self._emit({"event": "seq_gap", "sid": sid, "expected": expected, "got": seq})
        reconnect = True if self._on_gap is None else self._on_gap(sid, expected, seq)
        if reconnect:
            raise GapDetected(f"sid={sid} expected={expected} got={seq}")
