from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from anyio import CancelScope
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect


def create_status_app(
    status: Callable[[], dict[str, Any]],
    ready: Callable[[], bool],
    live_markets: Callable[[], dict[str, Any]] | None = None,
    subscribers: set[asyncio.Event] | None = None,
) -> FastAPI:
    app = FastAPI(title="kmb-collector", docs_url=None, redoc_url=None, openapi_url=None)
    started = time.monotonic()

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "uptime_seconds": time.monotonic() - started}

    @app.get("/ready")
    async def readiness() -> dict[str, Any]:
        is_ready = ready()
        return {"ready": is_ready, "spool_writable_and_ws_recent": is_ready}

    @app.get("/status")
    async def current_status() -> dict[str, Any]:
        return status()

    @app.get("/live-markets")
    async def current_markets() -> dict[str, Any]:
        if live_markets is None:
            raise HTTPException(503, "Live markets are not configured")
        return live_markets()

    @app.websocket("/ws/live-markets")
    async def stream_markets(ws: WebSocket) -> None:
        if live_markets is None or subscribers is None:
            await ws.close(code=1013)
            return
        await ws.accept()
        changed = asyncio.Event()
        subscribers.add(changed)

        async def send_updates() -> None:
            while True:
                changed.clear()
                await ws.send_json(live_markets())
                try:
                    await asyncio.wait_for(changed.wait(), timeout=5)
                    # Coalesce bursts, retaining only the current state for slow viewers.
                    await asyncio.sleep(0.05)
                except TimeoutError:
                    pass  # Heartbeat also refreshes connectivity and market boundaries.

        async def receive_until_closed() -> None:
            while True:
                await ws.receive_text()

        tasks = [asyncio.create_task(send_updates()), asyncio.create_task(receive_until_closed())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        finally:
            subscribers.discard(changed)
            for task in tasks:
                task.cancel()
            with CancelScope(shield=True):
                await asyncio.gather(*tasks, return_exceptions=True)

    return app
