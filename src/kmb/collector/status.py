from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from fastapi import FastAPI, HTTPException


def create_status_app(
    status: Callable[[], dict[str, Any]],
    ready: Callable[[], bool],
    live_markets: Callable[[], dict[str, Any]] | None = None,
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

    return app
