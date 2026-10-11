import json

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from kmb.kalshi.auth import KalshiAuth
from kmb.kalshi.endpoints import KalshiEnvironment
from kmb.kalshi.rest import KalshiApiError, KalshiRestClient

BASE = "https://external-api.kalshi.com/trade-api/v2"


@pytest.fixture(scope="module")
def auth() -> KalshiAuth:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return KalshiAuth(key_id="kid", private_key=key, clock_ms=lambda: 1)


def make_client(handler, auth=None, **kwargs) -> KalshiRestClient:
    return KalshiRestClient(BASE, auth, transport=httpx.MockTransport(handler), initial_backoff=0, **kwargs)


async def test_get_preserves_exact_body_text_and_drops_none_params():
    body = '{"markets": [{"ticker": "KXBTC15M-X", "yes_bid_dollars": "0.4100"}],  "cursor": ""}'
    seen: list[httpx.Request] = []
    observed = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, text=body)

    async with make_client(handler, on_response=observed.append) as client:
        data = await client.get_markets(series_ticker="KXBTC15M", status="open")

    assert data["markets"][0]["yes_bid_dollars"] == "0.4100"
    assert seen[0].url.path == "/trade-api/v2/markets"
    assert dict(seen[0].url.params) == {"series_ticker": "KXBTC15M", "status": "open", "limit": "200"}
    assert observed[0].text == body
    assert observed[0].recv_mono_ns >= observed[0].sent_mono_ns


async def test_signature_covers_full_api_path(auth):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"balance": 100})

    signed: list[tuple[str, str]] = []
    original = auth.headers

    def spy(method, path):
        signed.append((method, path))
        return original(method, path)

    object.__setattr__(auth, "headers", spy)
    try:
        async with make_client(handler, auth) as client:
            await client.get_balance()
    finally:
        object.__delattr__(auth, "headers")

    assert signed == [("GET", "/trade-api/v2/portfolio/balance")]
    assert seen[0].headers["KALSHI-ACCESS-KEY"] == "kid"


async def test_get_retries_retryable_status_then_succeeds():
    statuses = iter([429, 503, 200])

    def handler(request):
        status = next(statuses)
        return httpx.Response(status, json={"ok": status == 200})

    observed = []
    async with make_client(handler, on_response=observed.append) as client:
        assert await client.get_exchange_status() == {"ok": True}
    assert [r.attempt for r in observed] == [0, 1, 2]


async def test_get_raises_after_retries_exhausted():
    def handler(request):
        return httpx.Response(429, text="slow down")

    async with make_client(handler, max_retries=2) as client:
        with pytest.raises(KalshiApiError) as exc_info:
            await client.get_exchange_status()
    assert exc_info.value.status == 429
    assert exc_info.value.response.attempt == 2


async def test_order_writes_are_never_retried(auth):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503, text="unavailable")

    async with make_client(handler, auth) as client:
        with pytest.raises(KalshiApiError):
            await client.create_order({"ticker": "T", "side": "bid", "price": "0.5000", "count": "1.00"})
    assert len(calls) == 1


async def test_v2_order_endpoints_shape(auth):
    seen: list[httpx.Request] = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={})

    payload = {
        "ticker": "T",
        "side": "bid",
        "price": "0.5000",
        "count": "1.00",
        "time_in_force": "immediate_or_cancel",
    }
    async with make_client(handler, auth) as client:
        await client.create_order(payload)
        await client.cancel_order("oid", "T")
        await client.amend_order("oid", {**payload, "price": "0.5100"})
        await client.decrease_order("oid", "T", reduce_by="1.00")

    assert [(r.method, r.url.path) for r in seen] == [
        ("POST", "/trade-api/v2/portfolio/events/orders"),
        ("DELETE", "/trade-api/v2/portfolio/events/orders/oid"),
        ("POST", "/trade-api/v2/portfolio/events/orders/oid/amend"),
        ("POST", "/trade-api/v2/portfolio/events/orders/oid/decrease"),
    ]
    assert json.loads(seen[0].content) == payload
    assert seen[1].url.params["market_ticker"] == "T"
    assert json.loads(seen[3].content) == {"market_ticker": "T", "reduce_by": "1.00"}


async def test_decrease_requires_exactly_one_amount(auth):
    async with make_client(lambda r: httpx.Response(200, json={}), auth) as client:
        with pytest.raises(ValueError):
            await client.decrease_order("oid", "T")
        with pytest.raises(ValueError):
            await client.decrease_order("oid", "T", reduce_by="1", reduce_to="0")


async def test_portfolio_calls_require_auth():
    async with make_client(lambda r: httpx.Response(200, json={})) as client:
        with pytest.raises(ValueError, match="requires authentication"):
            await client.get_positions()


def test_environments():
    prod = KalshiEnvironment.from_name("prod")
    assert prod.rest_base_url.endswith("/trade-api/v2")
    assert prod.ws_url.endswith("/trade-api/ws/v2")
    assert KalshiEnvironment.from_name("demo").name == "demo"
    with pytest.raises(ValueError):
        KalshiEnvironment.from_name("staging")
