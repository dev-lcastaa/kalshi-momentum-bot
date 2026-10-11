"""Deployed Streamable HTTP transport.

By default every request must carry a research bearer token. When a server-held
``service_token`` is configured, clients connect without credentials and all
backend calls use that single research token instead.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from urllib.parse import urlsplit

import httpx
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from kmb.mcp.server import Settings, create_server

log = logging.getLogger("kmb.mcp")
_token: ContextVar[str] = ContextVar("research_token")


class ResearchAuth:
    def __init__(self, app: ASGIApp, backend_url: str):
        self.app = app
        self.backend_url = backend_url

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] == "/health":
            await self.app(scope, receive, send)
            return
        authorization = Request(scope).headers.get("authorization", "")
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token or token != token.strip():
            await JSONResponse(
                {"detail": "research bearer token required"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )(scope, receive, send)
            return
        try:
            async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
                response = await client.get(
                    f"{self.backend_url}/api/research/capabilities",
                    headers={"Authorization": f"Bearer {token}"},
                )
        except httpx.HTTPError as exc:
            log.warning("Research authentication backend unavailable: %s", type(exc).__name__)
            await JSONResponse({"detail": "research backend unavailable"}, status_code=503)(
                scope, receive, send
            )
            return
        if not response.is_success:
            status = response.status_code if response.status_code in (401, 403) else 503
            if status == 503:
                log.warning("Research authentication backend returned %s", response.status_code)
            await JSONResponse(
                {"detail": "research token denied" if status != 503 else "research backend unavailable"},
                status_code=status,
                headers={"WWW-Authenticate": "Bearer"} if status == 401 else {},
            )(scope, receive, send)
            return
        context_token = _token.set(token)
        try:
            await self.app(scope, receive, send)
        finally:
            _token.reset(context_token)


def create_http_app(backend_url: str, public_url: str, service_token: str = "") -> Starlette:
    backend = urlsplit(backend_url)
    public = urlsplit(public_url)
    for parsed in (backend, public):
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
        ):
            raise ValueError("MCP backend and public URLs must be HTTP(S) origins without credentials")
    if public.scheme != "https":
        raise ValueError("deployed MCP public URL must use HTTPS")
    service_token = service_token.strip()
    server = create_server(
        Settings(backend_url.rstrip("/"), ""),
        token_provider=(lambda: service_token) if service_token else _token.get,
        transport_security=TransportSecuritySettings(
            allowed_hosts=[public.netloc],
            allowed_origins=[public_url.rstrip("/")],
        ),
    )

    @server.custom_route("/health", methods=["GET"])
    async def health(request: Request) -> JSONResponse:
        return JSONResponse({"ok": True})

    app = server.streamable_http_app()
    if not service_token:
        app.add_middleware(ResearchAuth, backend_url=backend_url.rstrip("/"))
    return app
