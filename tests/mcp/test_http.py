from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import httpx
import pytest
from starlette.testclient import TestClient

from kmb.mcp.http import create_http_app

URL = "https://node003:8801"


def backend_response(request):
    token = request.headers.get("authorization")
    if token not in ("Bearer alice", "Bearer bob"):
        return httpx.Response(401, json={"detail": "invalid token"})
    if request.url.path == "/api/research/capabilities":
        return httpx.Response(200, json={"owner": token})
    if request.url.path == "/api/research/windows":
        return httpx.Response(200, json=[{"owner": token}])
    return httpx.Response(404)


@pytest.fixture
def client():
    original = httpx.AsyncClient

    def mock_client(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(backend_response)
        return original(*args, **kwargs)

    with patch("httpx.AsyncClient", side_effect=mock_client):
        with TestClient(create_http_app("http://backend:8800", URL), base_url=URL) as client:
            yield client


def rpc(client, token, method, params=None):
    return client.post(
        "/mcp",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
        },
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}},
    )


def test_health_and_authentication(client):
    assert client.get("/health").json() == {"ok": True}
    assert client.post("/mcp", json={}).status_code == 401
    assert rpc(client, "invalid", "tools/list").status_code == 401
    assert rpc(client, "alice", "tools/list").status_code == 200


def test_protocol_and_tools(client):
    response = rpc(
        client,
        "alice",
        "initialize",
        {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}},
    )
    assert response.status_code == 200
    assert response.json()["result"]["serverInfo"]["name"] == "Kalshi scalping research"
    tools = rpc(client, "alice", "tools/list").json()["result"]["tools"]
    assert len(tools) == 14
    assert "list_recordings" in {tool["name"] for tool in tools}
    response = rpc(client, "alice", "tools/call", {"name": "list_recordings", "arguments": {}})
    assert response.status_code == 200
    assert "Bearer alice" in response.text
    assert not response.json()["result"].get("isError")


def test_concurrent_tokens_are_isolated(client):
    def call(token):
        result = rpc(client, token, "tools/call", {"name": "research_capabilities", "arguments": {}})
        assert result.status_code == 200
        assert f"Bearer {token}" in result.text
        assert f"Bearer {'bob' if token == 'alice' else 'alice'}" not in result.text

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(call, ["alice", "bob"] * 4))


def test_revocation_checked_on_every_request(client):
    assert rpc(client, "alice", "tools/list").status_code == 200
    with patch("tests.mcp.test_http.backend_response", return_value=httpx.Response(401)):
        assert rpc(client, "alice", "tools/list").status_code == 401


def test_backend_outage_fails_closed(client):
    def unavailable(request):
        raise httpx.ConnectError("unavailable")

    with patch("tests.mcp.test_http.backend_response", side_effect=unavailable):
        assert rpc(client, "alice", "tools/list").status_code == 503


def test_rebinding_and_origin_rejected(client):
    assert client.post(
        "/mcp", json={}, headers={"Host": "evil.example", "Authorization": "Bearer alice"}
    ).status_code == 421
    assert client.post(
        "/mcp", json={}, headers={"Origin": "https://evil.example", "Authorization": "Bearer alice"}
    ).status_code == 403


def test_https_required():
    with pytest.raises(ValueError, match="HTTPS"):
        create_http_app("http://backend:8800", "http://node003:8801")
