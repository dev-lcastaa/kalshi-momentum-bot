from unittest.mock import patch

import httpx
import pytest
from starlette.testclient import TestClient

from kmb.mcp.http import create_http_app

URL = "https://node003:8801"
SERVICE = "service-research-token"
HEADERS = {"Accept": "application/json, text/event-stream"}
seen: list[str | None] = []


def backend_response(request):
    auth = request.headers.get("authorization")
    seen.append(auth)
    if auth != f"Bearer {SERVICE}":
        return httpx.Response(401, json={"detail": "invalid token"})
    if request.url.path == "/api/research/capabilities":
        return httpx.Response(200, json={"owner": "service"})
    return httpx.Response(404)


@pytest.fixture
def client():
    original = httpx.AsyncClient
    seen.clear()

    def mock_client(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(backend_response)
        return original(*args, **kwargs)

    with patch("httpx.AsyncClient", side_effect=mock_client):
        with TestClient(create_http_app("http://backend:8800", URL, SERVICE), base_url=URL) as c:
            yield c


def call_capabilities(client, headers):
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "research_capabilities", "arguments": {}},
    }
    return client.post("/mcp", headers=headers, json=body)


def test_no_client_credentials_required(client):
    response = call_capabilities(client, HEADERS)
    assert response.status_code == 200
    assert not response.json()["result"].get("isError")
    assert seen == [f"Bearer {SERVICE}"]


def test_client_supplied_token_is_ignored(client):
    response = call_capabilities(client, {**HEADERS, "Authorization": "Bearer other"})
    assert response.status_code == 200
    assert not response.json()["result"].get("isError")
    assert seen == [f"Bearer {SERVICE}"]


def test_rebinding_protection_kept(client):
    assert client.post("/mcp", json={}, headers={"Host": "evil.example"}).status_code == 421
    assert client.post("/mcp", json={}, headers={"Origin": "https://evil.example"}).status_code == 403
