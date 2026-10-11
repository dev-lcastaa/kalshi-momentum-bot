from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from kmb.envelope import dumps, subject_for

logger = logging.getLogger(__name__)
Connector = Callable[[str], Awaitable[Any]]


class NatsPublisher:
    """Bounded best-effort publisher: capture never waits for NATS acknowledgements."""

    def __init__(
        self,
        url: str,
        *,
        queue_size: int = 2048,
        connector: Connector | None = None,
    ) -> None:
        self.url = url
        self.connector = connector
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_size)
        self.published = 0
        self.dropped = 0
        self.connected = False
        self._nc = None
        self._js = None
        self._js_ok = False
        self._stop = asyncio.Event()

    def publish(self, envelope: dict[str, Any]) -> None:
        if not self.url:
            return
        if not self.connected:
            self.dropped += 1
            return
        try:
            self.queue.put_nowait(envelope)
        except asyncio.QueueFull:
            self.dropped += 1

    async def run(self) -> None:
        if not self.url:
            return
        delay = 1.0
        while not self._stop.is_set():
            try:
                from nats.js.api import StorageType, StreamConfig

                if self.connector is None:
                    import nats

                    self._nc = await nats.connect(self.url, connect_timeout=3, max_reconnect_attempts=0)
                else:
                    self._nc = await self.connector(self.url)
                self._js = self._nc.jetstream()
                config = StreamConfig(
                    name="MD",
                    subjects=["md.>"],
                    storage=StorageType.MEMORY,
                    max_age=3600,
                    max_bytes=256 * 1024**2,
                )
                try:
                    await self._js.add_stream(config=config)
                except Exception:
                    try:
                        await self._js.update_stream(config=config)
                    except Exception:
                        logger.warning("JetStream MD setup unavailable; using core NATS", exc_info=True)
                        self._js = None
                self._js_ok = self._js is not None
                self.connected = True
                delay = 1.0
                while not self._stop.is_set() and self._nc.is_connected:
                    try:
                        envelope = await asyncio.wait_for(self.queue.get(), timeout=0.5)
                    except TimeoutError:
                        continue
                    try:
                        payload = dumps(envelope).encode()
                        subject = subject_for(envelope)
                        if self._js_ok:
                            await self._js.publish(subject, payload)
                        else:
                            await self._nc.publish(subject, payload)
                        self.published += 1
                    except Exception:
                        self.dropped += 1
                        logger.warning("NATS publish failed; event dropped", exc_info=True)
                    finally:
                        self.queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("NATS connection unavailable", exc_info=True)
            finally:
                self.connected = False
                self._js = None
                self._js_ok = False
                if self._nc is not None:
                    try:
                        await self._nc.close()
                    except Exception:
                        pass
                    self._nc = None
            if not self._stop.is_set():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
                delay = min(delay * 2, 30)

    async def stop(self) -> None:
        self._stop.set()
        if self._nc is not None:
            try:
                await self._nc.close()
            except Exception:
                pass
