"""Live market data for paper/live jobs: the collector's NATS bus (``md.>``) as an async stream of Events."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Protocol

from kmb.replay.events import event_from_record
from kmb.strategy.base import Event

log = logging.getLogger(__name__)


class EventFeed(Protocol):
    dropped: int

    async def start(self) -> None: ...
    async def get(self, wait_s: float) -> Event | None: ...
    async def close(self) -> None: ...


class NatsFeed:
    """Core-NATS subscription (no JetStream replay: a restarted job starts from the live edge).

    The queue is bounded; on overflow the oldest events are dropped and counted. Dropped order-book deltas
    surface as seq gaps, which invalidate the affected books until the collector's next snapshot.
    """

    def __init__(self, url: str, subject: str = "md.>", maxsize: int = 20_000):
        self.url = url
        self.subject = subject
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0
        self.received = 0
        self._nc: Any = None

    async def start(self) -> None:
        import nats

        self._nc = await nats.connect(
            self.url, connect_timeout=5, max_reconnect_attempts=-1, reconnect_time_wait=2
        )
        await self._nc.subscribe(self.subject, cb=self._on_msg)

    async def _on_msg(self, msg: Any) -> None:
        try:
            rec = json.loads(msg.data)
            ev = event_from_record(rec)
        except Exception:
            log.warning("undecodable bus message on %s", getattr(msg, "subject", "?"))
            return
        self.put(ev)

    def put(self, ev: Event) -> None:
        self.received += 1
        if self.queue.full():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self.dropped += 1
        self.queue.put_nowait(ev)

    async def get(self, wait_s: float) -> Event | None:
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=wait_s)
        except TimeoutError:
            return None

    async def close(self) -> None:
        if self._nc is not None:
            try:
                await self._nc.drain()
            except Exception:
                pass
            self._nc = None


class ListFeed:
    """In-memory feed for tests: yields the given events, then idles."""

    def __init__(self, events: list[Event]):
        self.events = list(events)
        self.dropped = 0

    async def start(self) -> None:
        pass

    async def get(self, wait_s: float) -> Event | None:
        if self.events:
            return self.events.pop(0)
        await asyncio.sleep(min(wait_s, 0.01))
        return None

    async def close(self) -> None:
        pass
