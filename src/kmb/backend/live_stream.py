"""Authenticated socket relay from the collector; no HTTP polling."""

import asyncio
import json
import logging
from urllib.parse import urlsplit, urlunsplit

from anyio import CancelScope
from fastapi import WebSocket, WebSocketDisconnect
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from kmb.backend.app import COOKIE, AppCtx, _origin_ok, _user_for_token

log = logging.getLogger(__name__)


def collector_socket_url(status_url: str) -> str:
    url = urlsplit(status_url)
    if url.scheme not in ("http", "https") or not url.netloc:
        raise ValueError("collector status URL must use HTTP(S)")
    path = url.path.rsplit("/", 1)[0] + "/ws/live-markets"
    return urlunsplit(("wss" if url.scheme == "https" else "ws", url.netloc, path, "", ""))


async def live_markets_ws(ws: WebSocket) -> None:
    ctx: AppCtx = ws.app.state.kmb
    token = ws.cookies.get(COOKIE)
    with ctx.sessions() as db:
        user = _user_for_token(db, token)
    if user is None or not _origin_ok(ws, ctx.settings):
        await ws.close(code=4401)
        return
    await ws.accept()
    url = ctx.settings.collector_status_url
    if not url:
        await ws.send_json({"type": "error", "detail": "Live market recording is not configured"})
        await ws.close(code=1013)
        return

    async def relay() -> None:
        async with connect(collector_socket_url(url), open_timeout=5, max_size=1_000_000) as upstream:
            async for raw in upstream:
                with ctx.sessions() as db:
                    if _user_for_token(db, token) is None:
                        await ws.close(code=4401)
                        return
                payload = json.loads(raw)
                if not isinstance(payload, dict) or not isinstance(payload.get("markets"), list):
                    raise ValueError("invalid collector live snapshot")
                await ws.send_json(payload)
            raise OSError("Collector closed the live stream")

    async def receive_until_closed() -> None:
        while True:
            await ws.receive_text()

    tasks = [asyncio.create_task(relay()), asyncio.create_task(receive_until_closed())]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except (OSError, TimeoutError, WebSocketException, ValueError) as exc:
        log.warning("Live market socket unavailable: %s", exc)
        await ws.send_json({"type": "error", "detail": "Live feed disconnected. Reconnecting."})
        await ws.close(code=1013)
    finally:
        for task in tasks:
            task.cancel()
        with CancelScope(shield=True):
            await asyncio.gather(*tasks, return_exceptions=True)
