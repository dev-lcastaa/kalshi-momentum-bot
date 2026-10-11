from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from typing import Any

import uvicorn

from kmb.envelope import (
    KIND_CONTROL,
    SOURCE_COLLECTOR,
    STREAM_CONTROL,
    Sequencer,
    make_envelope,
)
from kmb.kalshi.auth import KalshiAuth
from kmb.kalshi.rest import KalshiRestClient
from kmb.kalshi.ws import KalshiWsClient

from .bus import NatsPublisher
from .coinbase_feed import CoinbaseFeed
from .config import CollectorConfig
from .db_sink import DatabaseSink
from .kalshi_feed import KalshiFeed
from .live import LiveMarkets
from .spool import Spool
from .status import create_status_app
from .uploader import BatchUploader

logger = logging.getLogger(__name__)


class CollectorApp:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self.capture_id = uuid.uuid4().hex
        self.started = time.monotonic()
        self.sequencer = Sequencer()
        self.stream_counts = {stream: 0 for stream in ("kalshi_ws", "kalshi_rest", "coinbase_ws", "control")}
        self.stream_last_wall: dict[str, int] = {}
        self.spool = Spool(
            config.spool_dir,
            self.capture_id,
            cap_bytes=config.spool_cap_bytes,
            max_batch_bytes=config.batch_max_bytes,
            max_age_seconds=config.batch_max_age_seconds,
        )
        self.uploader = BatchUploader(self.spool, config.ingest_url, config.ingest_token or "")
        self.bus = NatsPublisher(config.nats_url)
        self.db = DatabaseSink(config.db_url) if config.db_url else None
        self.rest: KalshiRestClient | None = None
        self.kalshi: KalshiFeed | None = None
        self.coinbase = CoinbaseFeed(self.emit, self.sequencer, self.capture_id)
        self._stop = asyncio.Event()
        self._server: uvicorn.Server | None = None
        self._pending_drop_count = 0
        self.live = LiveMarkets()

    def emit(self, envelope: dict[str, Any]) -> None:
        stored = self.spool.append(envelope)
        self.stream_counts[envelope["stream"]] = self.stream_counts.get(envelope["stream"], 0) + 1
        self.stream_last_wall[envelope["stream"]] = envelope["recv_wall_ms"]
        self.bus.publish(envelope)
        self.live.apply(envelope)
        if not stored:
            logger.error("spool at capacity; capture event dropped (%s)", envelope["stream"])
            self._pending_drop_count += 1
            return
        if self._pending_drop_count:
            count = self._pending_drop_count
            self._pending_drop_count = 0
            note = make_envelope(
                self.sequencer,
                capture_id=self.capture_id,
                stream=STREAM_CONTROL,
                kind=KIND_CONTROL,
                source=SOURCE_COLLECTOR,
                data={"event": "spool_drop", "count": count},
            )
            if self.spool.append(note):
                self.stream_counts[STREAM_CONTROL] += 1
                self.stream_last_wall[STREAM_CONTROL] = note["recv_wall_ms"]
                self.bus.publish(note)
            else:
                self._pending_drop_count += count

    def on_rest_response(self, response) -> None:
        endpoint, ticker = _rest_route(response.path)
        data = {
            "endpoint": endpoint,
            "method": response.method,
            "path": response.path,
            "params": response.params,
            "status": response.status,
            "sent_wall_ms": response.sent_wall_ms,
            "recv_wall_ms": response.recv_wall_ms,
        }
        envelope = make_envelope(
            self.sequencer,
            capture_id=self.capture_id,
            stream="kalshi_rest",
            kind="rest",
            source="kalshi",
            raw=response.text,
            data=data,
            market_ticker=ticker,
            msg_type=f"rest.{endpoint}",
            recv_wall_ms=response.recv_wall_ms,
            recv_mono_ns=response.recv_mono_ns,
        )
        self.emit(envelope)
        if self.db is not None:
            asyncio.get_running_loop().create_task(self._write_market_response(response))

    async def _write_market_response(self, response) -> None:
        try:
            await asyncio.to_thread(self.db.market_response, response)
            self.db.ok = True
        except Exception:
            self.db.errors += 1
            self.db.ok = False
            logger.exception("market metadata database write failed")

    def emit_control(self, event: str, **fields: Any) -> None:
        self.emit(
            make_envelope(
                self.sequencer,
                capture_id=self.capture_id,
                stream=STREAM_CONTROL,
                kind=KIND_CONTROL,
                source=SOURCE_COLLECTOR,
                data={"event": event, **fields},
            )
        )

    def status(self) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        spool = self.spool.stats()
        return {
            "capture_id": self.capture_id,
            "collector_id": self.config.collector_id,
            "uptime_seconds": round(time.monotonic() - self.started, 2),
            "subscribed_markets": sorted(self.kalshi.subscribed) if self.kalshi else [],
            "streams": {
                key: {
                    "count": count,
                    "last_event_age_ms": max(0, now_ms - self.stream_last_wall[key])
                    if key in self.stream_last_wall
                    else None,
                }
                for key, count in self.stream_counts.items()
            },
            "ws": {
                "connected": bool(self.kalshi and self.kalshi.ws.connected.is_set()),
                "reconnects": self.kalshi.ws_reconnects if self.kalshi else 0,
                "gaps": self.kalshi.ws_gaps if self.kalshi else 0,
                "coinbase_connected": self.coinbase.connected,
                "coinbase_reconnects": self.coinbase.reconnects,
            },
            "spool": {**spool, "drops": self.spool.drop_count},
            "uploader": {
                "backlog": spool["pending_batches"],
                "last_success": self.uploader.stats.last_success,
                "errors": self.uploader.stats.errors,
                "attempts": self.uploader.stats.attempts,
            },
            "nats": {
                "connected": self.bus.connected,
                "published": self.bus.published,
                "dropped": self.bus.dropped,
            },
            "db": {
                "enabled": self.db is not None,
                "ok": self.db.ok if self.db else None,
                "errors": self.db.errors if self.db else 0,
            },
        }

    def ready(self) -> bool:
        age = None if self.kalshi is None else self.kalshi.last_ws_event_mono
        recent = bool(self.kalshi and self.kalshi.ws.connected.is_set()) and (
            age is None or time.monotonic() - age < 120
        )
        return recent and self.spool.writable()

    def live_markets(self) -> dict[str, Any]:
        return self.live.snapshot(
            self.kalshi.markets if self.kalshi else {},
            self.config.series,
            now_ms=int(time.time() * 1000),
            kalshi_connected=bool(self.kalshi and self.kalshi.ws.connected.is_set()),
            coinbase_connected=self.coinbase.connected,
        )

    async def run(self) -> None:
        self.config.check()
        if self.db is not None:
            try:
                await asyncio.to_thread(self.db.initialize)
            except Exception:
                self.db.errors += 1
                self.db.ok = False
                logger.exception("database initialization failed; capture continues")
        env = self.config.kalshi_environment
        auth = None
        if self.config.kalshi_key_id and self.config.kalshi_private_key_path:
            auth = KalshiAuth.from_file(self.config.kalshi_key_id, str(self.config.kalshi_private_key_path))
        self.rest = KalshiRestClient(env.rest_base_url, auth, on_response=self.on_rest_response)
        self.kalshi = KalshiFeed(
            self.rest,
            KalshiWsClient(env.ws_url, auth),
            self.emit,
            series=self.config.series,
            discovery_interval=self.config.discovery_interval,
            snapshot_interval=self.config.rest_snapshot_interval,
        )
        self.kalshi.configure_capture(self.sequencer, self.capture_id)
        self.kalshi.ws._on_frame = self.kalshi.on_frame
        self.kalshi.ws._on_transport = self.kalshi.on_transport
        self.emit_control("session_start", version="0.1.0", collector_id=self.config.collector_id)
        tasks = [
            asyncio.create_task(self.kalshi.run(), name="kalshi-feed"),
            asyncio.create_task(self.coinbase.run(), name="coinbase-feed"),
            asyncio.create_task(self.uploader.run(), name="uploader"),
            asyncio.create_task(self.bus.run(), name="nats"),
            asyncio.create_task(self._clock_loop(), name="clock"),
            asyncio.create_task(self._status_db_loop(), name="db-status"),
            asyncio.create_task(self._serve_status(), name="status-api"),
        ]
        self._install_signal_handlers()
        try:
            await self._stop.wait()
        finally:
            self.emit_control("session_end", version="0.1.0", collector_id=self.config.collector_id)
            try:
                self.spool.seal_all()
            except Exception:
                logger.exception("failed sealing collector spool during shutdown")
            await self.kalshi.stop()
            await self.coinbase.stop()
            await self.bus.stop()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.rest is not None:
                await self.rest.aclose()
            # Give the oldest batches a short best-effort drain after sealing.
            deadline = asyncio.get_running_loop().time() + 5
            while asyncio.get_running_loop().time() < deadline:
                pending = self.spool.pending_batches()
                if not pending:
                    break
                try:
                    outcome = await asyncio.wait_for(
                        self.uploader.upload_one(pending[0]),
                        timeout=deadline - asyncio.get_running_loop().time(),
                    )
                except Exception:
                    logger.exception("final collector upload attempt failed")
                    break
                if outcome == "retry":
                    break
            await self.uploader.close()
            if self.db is not None:
                await asyncio.to_thread(self.db.close)

    async def _clock_loop(self) -> None:
        while True:
            self.emit_control("clock", wall_ms=int(time.time() * 1000), mono_ns=time.monotonic_ns())
            await asyncio.sleep(60)

    async def _status_db_loop(self) -> None:
        while True:
            if self.db is not None:
                try:
                    await asyncio.to_thread(
                        self.db.write_status, self.config.collector_id, self.capture_id, self.status()
                    )
                    self.db.ok = True
                except Exception:
                    self.db.errors += 1
                    self.db.ok = False
                    logger.exception("collector status write failed")
            await asyncio.sleep(30)

    async def _serve_status(self) -> None:
        app = create_status_app(self.status, self.ready, self.live_markets)
        config = uvicorn.Config(app, host="0.0.0.0", port=self.config.status_port, log_level="warning")
        self._server = uvicorn.Server(config)
        await self._server.serve()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except (NotImplementedError, RuntimeError):
                pass


def _rest_route(path: str) -> tuple[str, str | None]:
    parts = path.strip("/").split("/")
    if len(parts) == 1 and parts[0] == "series":
        return "series", None
    if len(parts) == 2 and parts[0] == "series":
        return "series", parts[1]
    if parts[0] == "markets" and len(parts) == 1:
        return "markets", None
    if parts[0] == "markets" and len(parts) >= 3 and parts[2] == "orderbook":
        return "orderbook", parts[1]
    if parts[0] == "markets" and len(parts) == 2:
        return "market", parts[1]
    return parts[0] if parts else "unknown", None


async def run(config: CollectorConfig | None = None) -> None:
    await CollectorApp(config or CollectorConfig.from_env()).run()
