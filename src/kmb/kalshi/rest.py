"""Async REST client for Kalshi market data, portfolio and V2 event-market orders.

Every response is surfaced as a `RestResponse` that keeps the exact body text and timing so the
collector can archive it byte-for-byte. Only idempotent GETs are retried; order writes never are.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx

from .auth import KalshiAuth

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class KalshiApiError(Exception):
    def __init__(self, response: RestResponse):
        self.response = response
        super().__init__(f"{response.method} {response.path} -> {response.status}: {response.text[:300]}")

    @property
    def status(self) -> int:
        return self.response.status


@dataclass(frozen=True)
class RestResponse:
    method: str
    path: str
    params: dict[str, Any]
    request_body: str | None
    status: int
    text: str
    headers: dict[str, str] = field(repr=False)
    sent_wall_ms: int
    recv_wall_ms: int
    sent_mono_ns: int
    recv_mono_ns: int
    attempt: int

    def json(self) -> Any:
        return json.loads(self.text)

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


ResponseObserver = Callable[[RestResponse], None]


class KalshiRestClient:
    def __init__(
        self,
        base_url: str,
        auth: KalshiAuth | None = None,
        *,
        timeout: float = 10.0,
        max_retries: int = 5,
        initial_backoff: float = 0.5,
        on_response: ResponseObserver | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._sign_prefix = urlsplit(self._base_url).path
        self._auth = auth
        self._max_retries = max_retries
        self._initial_backoff = initial_backoff
        self._on_response = on_response
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=timeout, transport=transport)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> KalshiRestClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # --- transport ---------------------------------------------------------------------------

    def _headers(self, method: str, path: str, *, auth_required: bool) -> dict[str, str]:
        if self._auth is None:
            if auth_required:
                raise ValueError(f"{method} {path} requires authentication")
            return {}
        return self._auth.headers(method, self._sign_prefix + path)

    async def _send_once(
        self,
        method: str,
        path: str,
        params: dict[str, Any],
        body: str | None,
        attempt: int,
        auth_required: bool,
    ) -> RestResponse:
        headers = self._headers(method, path, auth_required=auth_required)
        if body is not None:
            headers["Content-Type"] = "application/json"
        sent_wall_ms = int(time.time() * 1000)
        sent_mono_ns = time.monotonic_ns()
        response = await self._client.request(
            method, path, params=params or None, content=body, headers=headers
        )
        result = RestResponse(
            method=method,
            path=path,
            params=params,
            request_body=body,
            status=response.status_code,
            text=response.text,
            headers=dict(response.headers),
            sent_wall_ms=sent_wall_ms,
            recv_wall_ms=int(time.time() * 1000),
            sent_mono_ns=sent_mono_ns,
            recv_mono_ns=time.monotonic_ns(),
            attempt=attempt,
        )
        if self._on_response is not None:
            try:
                self._on_response(result)
            except Exception:
                logger.exception("on_response observer failed")
        return result

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        auth_required: bool = False,
    ) -> RestResponse:
        method = method.upper()
        clean_params = {k: v for k, v in (params or {}).items() if v is not None}
        body = None if json_body is None else json.dumps(json_body, separators=(",", ":"))
        retries = self._max_retries if method == "GET" else 0
        backoff = self._initial_backoff
        for attempt in range(retries + 1):
            result = await self._send_once(method, path, clean_params, body, attempt, auth_required)
            if result.ok:
                return result
            if result.status not in RETRYABLE_STATUS or attempt == retries:
                raise KalshiApiError(result)
            logger.warning("%s %s -> %s; retrying in %.1fs", method, path, result.status, backoff)
            await asyncio.sleep(backoff)
            backoff *= 2
        raise AssertionError("unreachable")

    async def get(
        self, path: str, params: dict[str, Any] | None = None, *, auth_required: bool = False
    ) -> Any:
        return (await self.request("GET", path, params=params, auth_required=auth_required)).json()

    # --- exchange / series / events / markets --------------------------------------------------

    async def get_exchange_status(self) -> dict[str, Any]:
        return await self.get("/exchange/status")

    async def get_series_list(self, category: str | None = None, tags: str | None = None) -> dict[str, Any]:
        return await self.get("/series", {"category": category, "tags": tags})

    async def get_series(self, series_ticker: str) -> dict[str, Any]:
        return await self.get(f"/series/{series_ticker}")

    async def get_event(self, event_ticker: str) -> dict[str, Any]:
        return await self.get(f"/events/{event_ticker}")

    async def get_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        status: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return await self.get(
            "/markets",
            {
                "series_ticker": series_ticker,
                "event_ticker": event_ticker,
                "status": status,
                "limit": limit,
                "cursor": cursor or None,
            },
        )

    async def get_market(self, ticker: str) -> dict[str, Any]:
        return await self.get(f"/markets/{ticker}")

    async def get_market_orderbook(self, ticker: str, depth: int | None = None) -> dict[str, Any]:
        return await self.get(f"/markets/{ticker}/orderbook", {"depth": depth})

    async def get_trades(
        self,
        ticker: str | None = None,
        min_ts: int | None = None,
        limit: int = 1000,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return await self.get(
            "/markets/trades", {"ticker": ticker, "min_ts": min_ts, "limit": limit, "cursor": cursor or None}
        )

    async def get_market_candlesticks(
        self, series_ticker: str, ticker: str, start_ts: int, end_ts: int, period_interval: int = 1
    ) -> dict[str, Any]:
        return await self.get(
            f"/series/{series_ticker}/markets/{ticker}/candlesticks",
            {"start_ts": start_ts, "end_ts": end_ts, "period_interval": period_interval},
        )

    # --- CF Benchmarks passthrough (settlement source) ----------------------------------------

    async def cfbenchmarks_values(self, index_id: str) -> dict[str, Any]:
        return await self.get("/cfbenchmarks/values", {"id": index_id}, auth_required=True)

    async def cfbenchmarks_history(self, index_id: str, timespan: str, timestamp: str) -> dict[str, Any]:
        return await self.get(
            "/cfbenchmarks/history/values",
            {"id": index_id, "timespan": timespan, "timestamp": timestamp},
            auth_required=True,
        )

    # --- portfolio ----------------------------------------------------------------------------

    async def get_balance(self) -> dict[str, Any]:
        return await self.get("/portfolio/balance", auth_required=True)

    async def get_orders(
        self,
        ticker: str | None = None,
        status: str | None = None,
        cursor: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        return await self.get(
            "/portfolio/orders",
            {"ticker": ticker, "status": status, "cursor": cursor or None, "limit": limit},
            auth_required=True,
        )

    async def get_order(self, order_id: str) -> dict[str, Any]:
        return await self.get(f"/portfolio/orders/{order_id}", auth_required=True)

    async def get_fills(
        self,
        ticker: str | None = None,
        order_id: str | None = None,
        min_ts: int | None = None,
        cursor: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        return await self.get(
            "/portfolio/fills",
            {
                "ticker": ticker,
                "order_id": order_id,
                "min_ts": min_ts,
                "cursor": cursor or None,
                "limit": limit,
            },
            auth_required=True,
        )

    async def get_positions(self, ticker: str | None = None, cursor: str | None = None) -> dict[str, Any]:
        return await self.get(
            "/portfolio/positions",
            {"ticker": ticker, "cursor": cursor or None, "limit": 200},
            auth_required=True,
        )

    # --- V2 event-market orders (never retried automatically) --------------------------------

    async def create_order(self, payload: dict[str, Any]) -> RestResponse:
        """POST /portfolio/events/orders. `payload` uses the V2 shape (side bid/ask, dollar price strings)."""
        return await self.request("POST", "/portfolio/events/orders", json_body=payload, auth_required=True)

    async def cancel_order(self, order_id: str, market_ticker: str) -> RestResponse:
        return await self.request(
            "DELETE",
            f"/portfolio/events/orders/{order_id}",
            params={"market_ticker": market_ticker},
            auth_required=True,
        )

    async def amend_order(self, order_id: str, payload: dict[str, Any]) -> RestResponse:
        return await self.request(
            "POST", f"/portfolio/events/orders/{order_id}/amend", json_body=payload, auth_required=True
        )

    async def decrease_order(
        self,
        order_id: str,
        market_ticker: str,
        *,
        reduce_by: str | None = None,
        reduce_to: str | None = None,
    ) -> RestResponse:
        if (reduce_by is None) == (reduce_to is None):
            raise ValueError("Exactly one of reduce_by or reduce_to must be provided")
        body: dict[str, Any] = {"market_ticker": market_ticker}
        if reduce_by is not None:
            body["reduce_by"] = reduce_by
        else:
            body["reduce_to"] = reduce_to
        return await self.request(
            "POST", f"/portfolio/events/orders/{order_id}/decrease", json_body=body, auth_required=True
        )
