from __future__ import annotations

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from sqlalchemy import select

from kmb.backend.app import create_app
from kmb.backend.security import Vault, hash_password
from kmb.backend.settings import BackendSettings
from kmb.db import init_db, make_engine, session_factory
from kmb.db.models import Job, KalshiKey, Run, User
from kmb.replay.source import LocalWindowSource
from tests import synth

ADMIN_PW = "admin-password-1"
USER_PW = "user-password-1"


@pytest.fixture
def env(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path / 'db.sqlite'}")
    init_db(engine)
    sessions = session_factory(engine)
    with sessions() as db:
        db.add(User(username="admin", password_hash=hash_password(ADMIN_PW), role="admin"))
        db.add(User(username="bob", password_hash=hash_password(USER_PW), role="user"))
        db.commit()
    windows = tmp_path / "derived"
    synth.write_window(windows)
    settings = BackendSettings(
        vault_key_hex=Vault.generate_key_hex(),
        static_dir=None,
        collector_status_url=None,
        ingest_status_url=None,
        login_max_failures=3,
    )
    app = create_app(settings, sessions=sessions, source=LocalWindowSource(windows))
    return app, sessions


def login(app, username, password):
    c = TestClient(app)
    r = c.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    c.headers["X-KMB-CSRF"] = r.json()["csrf"]
    return c


def test_auth_flow_and_csrf(env):
    app, _ = env
    c = TestClient(app)
    assert c.get("/api/auth/me").status_code == 401
    assert c.post("/api/auth/login", json={"username": "bob", "password": "wrong"}).status_code == 401
    r = c.post("/api/auth/login", json={"username": "bob", "password": USER_PW})
    assert r.status_code == 200
    assert "kmb_session" in r.cookies or c.cookies.get("kmb_session")
    csrf = r.json()["csrf"]
    me = c.get("/api/auth/me").json()
    assert me["user"]["username"] == "bob" and me["csrf"] == csrf
    # mutation without CSRF header is refused
    assert c.post("/api/bots", json={"name": "x", "strategy": "noop"}).status_code == 403
    assert (
        c.post("/api/bots", json={"name": "x", "strategy": "noop"}, headers={"X-KMB-CSRF": csrf}).status_code
        == 201
    )
    r = c.post(
        "/api/auth/password",
        json={"current_password": USER_PW, "new_password": "short"},
        headers={"X-KMB-CSRF": csrf},
    )
    assert r.status_code == 422
    assert c.post("/api/auth/logout", headers={"X-KMB-CSRF": csrf}).status_code == 204
    assert c.get("/api/auth/me").status_code == 401
    assert r.headers["x-frame-options"] == "DENY"


def test_live_markets_requires_login_and_proxies_both_coins(env, monkeypatch):
    app, _ = env
    assert TestClient(app).get("/api/live-markets").status_code == 401
    client = login(app, "bob", USER_PW)
    assert client.get("/api/live-markets").status_code == 503
    app.state.kmb.settings.collector_status_url = "http://collector:8710/status"
    payload = {"ts_ms": 123, "kalshi_connected": True, "coinbase_connected": True,
               "markets": [{"series_ticker": "KXBTC15M"}, {"series_ticker": "KXETH15M"}]}
    original = httpx.AsyncClient

    def handler(request):
        assert str(request.url) == "http://collector:8710/live-markets"
        return httpx.Response(200, json=payload)

    monkeypatch.setattr(httpx, "AsyncClient",
                        lambda **kwargs: original(transport=httpx.MockTransport(handler), **kwargs))
    assert client.get("/api/live-markets").json() == payload


def test_live_markets_reports_collector_failure(env, monkeypatch):
    app, _ = env
    app.state.kmb.settings.collector_status_url = "http://collector:8710/status"
    original = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original(
        transport=httpx.MockTransport(lambda request: httpx.Response(500)), **kwargs))
    assert login(app, "bob", USER_PW).get("/api/live-markets").status_code == 503


def test_login_rate_limit(env):
    app, _ = env
    c = TestClient(app)
    for _ in range(3):
        c.post("/api/auth/login", json={"username": "bob", "password": "nope"})
    assert c.post("/api/auth/login", json={"username": "bob", "password": USER_PW}).status_code == 429


def test_users_admin_only(env):
    app, _ = env
    bob = login(app, "bob", USER_PW)
    assert bob.get("/api/users").status_code == 403
    admin = login(app, "admin", ADMIN_PW)
    assert len(admin.get("/api/users").json()) == 2
    r = admin.post("/api/users", json={"username": "carol", "password": "carol-password", "role": "user"})
    assert r.status_code == 201
    assert (
        admin.post("/api/users", json={"username": "carol", "password": "carol-password"}).status_code == 409
    )
    uid = r.json()["id"]
    assert admin.patch(f"/api/users/{uid}", json={"live_enabled": True}).json()["live_enabled"] is True
    me = admin.get("/api/auth/me").json()["user"]["id"]
    assert admin.patch(f"/api/users/{me}", json={"role": "user"}).status_code == 400
    assert admin.patch(f"/api/users/{uid}", json={"is_active": False}).status_code == 200
    TestClient(app).post("/api/auth/login", json={"username": "carol", "password": "carol-password"})
    assert (
        TestClient(app)
        .post("/api/auth/login", json={"username": "carol", "password": "carol-password"})
        .status_code
        == 401
    )


def _pem() -> str:
    k = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return k.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()


def test_keys_are_encrypted_and_write_only(env):
    app, sessions = env
    bob = login(app, "bob", USER_PW)
    pem = _pem()
    r = bob.post("/api/keys", json={"label": "main", "api_key_id": "abcdef12-3456", "private_key_pem": pem})
    assert r.status_code == 201, r.text
    body = r.json()
    assert "private_key_pem" not in body and body["leased_by_job_id"] is None
    with sessions() as db:
        row = db.scalar(select(KalshiKey))
        assert pem.encode() not in row.private_key_enc
    bad = bob.post(
        "/api/keys", json={"label": "x", "api_key_id": "abcdef12-3456", "private_key_pem": "x" * 200}
    )
    assert bad.status_code == 422
    admin = login(app, "admin", ADMIN_PW)
    assert admin.get("/api/keys").json() == []  # own keys only
    assert admin.delete(f"/api/keys/{body['id']}").status_code == 404
    assert bob.delete(f"/api/keys/{body['id']}").status_code == 204


def test_bots_jobs_and_live_gates(env):
    app, sessions = env
    bob = login(app, "bob", USER_PW)
    strategies = bob.get("/api/strategies").json()
    assert any(s["name"] == "momentum" for s in strategies)
    assert (
        bob.post("/api/bots", json={"name": "m", "strategy": "momentum", "params": {"bogus": 1}}).status_code
        == 422
    )
    assert (
        bob.post("/api/bots", json={"name": "m", "strategy": "momentum", "series": ["KXSOL15M"]}).status_code
        == 422
    )
    r = bob.post("/api/bots", json={"name": "m", "strategy": "momentum", "params": {"lookback_s": 5}})
    assert r.status_code == 201
    bot = r.json()
    assert bob.post("/api/bots", json={"name": "m", "strategy": "noop"}).status_code == 409
    assert bob.patch(f"/api/bots/{bot['id']}", json={"params": {"size": "2"}}).json()["params"] == {
        "size": "2"
    }

    assert bob.post("/api/jobs", json={"kind": "sim", "bot_id": bot["id"]}).status_code == 422
    r = bob.post(
        "/api/jobs",
        json={
            "kind": "sim",
            "bot_id": bot["id"],
            "windows": [synth.TICKER],
            "sim": {"latency_ms": 100, "queue_model": "back"},
        },
    )
    assert r.status_code == 201
    job = r.json()
    assert job["status"] == "queued" and job["spec"]["bot"]["strategy"] == "momentum"
    assert job["spec"]["sim"] == {"latency_ms": 100, "queue_model": "back"}
    assert bob.get("/api/bots").json()[0]["active_job"]["id"] == job["id"]
    assert bob.delete(f"/api/bots/{bot['id']}").status_code == 409
    stopped = bob.post(f"/api/jobs/{job['id']}/stop").json()
    assert stopped["status"] == "canceled"
    events = bob.get(f"/api/jobs/{job['id']}/events").json()
    assert events and events[0]["code"] == "job_created"

    # live gates
    assert bob.post("/api/jobs", json={"kind": "live", "bot_id": bot["id"]}).status_code == 403
    admin = login(app, "admin", ADMIN_PW)
    with sessions() as db:
        bob_id = db.scalar(select(User.id).where(User.username == "bob"))
    admin.patch(f"/api/users/{bob_id}", json={"live_enabled": True})
    r = bob.post("/api/jobs", json={"kind": "live", "bot_id": bot["id"]})
    assert r.status_code == 403 and "globally" in r.json()["detail"]
    assert bob.put("/api/settings/live-trading", json={"enabled": True}).status_code == 403
    assert (
        admin.put("/api/settings/live-trading", json={"enabled": True}).json()["live_trading_enabled"] is True
    )
    r = bob.post("/api/jobs", json={"kind": "live", "bot_id": bot["id"]})
    assert r.status_code == 403 and "key" in r.json()["detail"]

    # paper: allowed, one per bot, blocked by kill switch
    assert bob.post("/api/jobs", json={"kind": "paper", "bot_id": bot["id"]}).status_code == 201
    assert bob.post("/api/jobs", json={"kind": "paper", "bot_id": bot["id"]}).status_code == 409
    assert bob.post("/api/kill-switch", json={"scope": "global", "engaged": True}).status_code == 403
    st = bob.post("/api/kill-switch", json={"scope": "user", "engaged": True, "reason": "test"}).json()
    assert st["kill_switch"] == {"global": False, "user": True}
    with sessions() as db:
        assert db.scalar(select(Job).where(Job.kind == "paper")).cancel_requested is True
    bot2 = bob.post("/api/bots", json={"name": "n", "strategy": "noop"}).json()
    assert bob.post("/api/jobs", json={"kind": "paper", "bot_id": bot2["id"]}).status_code == 403

    # other users can't see bob's jobs; admin can
    assert login(app, "admin", ADMIN_PW).get(f"/api/jobs/{job['id']}").status_code == 200
    with sessions() as db:
        db.add(User(username="eve", password_hash=hash_password("eve-password1")))
        db.commit()
    assert login(app, "eve", "eve-password1").get(f"/api/jobs/{job['id']}").status_code == 404


def test_risk_limits(env):
    app, _ = env
    bob = login(app, "bob", USER_PW)
    lim = bob.get("/api/risk-limits").json()
    assert lim["global"]["max_open_orders"] == 10 and lim["user"] is None
    assert bob.put("/api/risk-limits/global", json={"max_open_orders": 5}).status_code == 403
    admin = login(app, "admin", ADMIN_PW)
    assert admin.put("/api/risk-limits/global", json={"max_daily_loss": "-1"}).status_code == 422
    out = admin.put("/api/risk-limits/global", json={"max_open_orders": 5, "max_daily_loss": "12.5"}).json()
    assert out["max_open_orders"] == 5 and out["max_daily_loss"] == "12.50"
    bob_id = bob.get("/api/auth/me").json()["user"]["id"]
    assert admin.put(f"/api/risk-limits/{bob_id}", json={"max_position_per_market": 3}).status_code == 200
    assert bob.get("/api/risk-limits").json()["user"]["max_position_per_market"] == "3.00"


def test_windows_overview_and_status(env):
    app, _ = env
    bob = login(app, "bob", USER_PW)
    ws = bob.get("/api/windows").json()
    assert ws[0]["window_id"] == synth.TICKER and ws[0]["quality"] == "ok"
    assert bob.get("/api/windows?series=KXETH15M").json() == []
    assert bob.get("/api/windows/nope").status_code == 404
    ov = bob.get(f"/api/windows/{synth.TICKER}/overview").json()
    assert ov["window"]["window_id"] == synth.TICKER and len(ov["series"]) > 900
    st = bob.get("/api/status").json()
    assert st["data_api"]["ok"] is True and st["live_trading_enabled"] is False
    assert st["collector"] is None and st["workers"] == []
    assert bob.get("/api/fidelity").json() == []
    assert bob.get("/api/runs").json() == []


def test_replay_websocket(env):
    app, _ = env
    bob = login(app, "bob", USER_PW)
    with bob.websocket_connect(f"/ws/replay/{synth.TICKER}") as ws:
        meta = ws.receive_json()
        assert meta["type"] == "meta" and meta["open_ms"] == synth.OPEN_MS
        first = ws.receive_json()
        assert first["type"] == "frame" and first["ts_ms"] == meta["start_ms"]
        ws.send_json({"op": "seek", "ts_ms": synth.OPEN_MS + 120_000})
        f = ws.receive_json()
        assert f["ts_ms"] == synth.OPEN_MS + 120_000 and f["book"]["valid"] and f["coinbase"]
        ws.send_json({"op": "step", "ms": 5000})
        f2 = ws.receive_json()
        assert f2["ts_ms"] == f["ts_ms"] + 5000
        ws.send_json({"op": "speed", "speed": 60})
        assert ws.receive_json()["speed"] == 60
        ws.send_json({"op": "seek", "ts_ms": meta["end_ms"] - 50})
        ws.receive_json()
        ws.send_json({"op": "play"})
        kinds = []
        for _ in range(20):
            m = ws.receive_json()
            kinds.append(m["type"])
            if m["type"] == "end":
                break
        assert "end" in kinds
        ws.send_json({"op": "bogus"})
        assert ws.receive_json()["type"] == "error"


def test_replay_websocket_requires_auth(env):
    app, _ = env
    from starlette.websockets import WebSocketDisconnect

    with (
        pytest.raises(WebSocketDisconnect),
        TestClient(app).websocket_connect(f"/ws/replay/{synth.TICKER}") as ws,
    ):
        ws.receive_json()
    bob = login(app, "bob", USER_PW)
    with (
        pytest.raises(WebSocketDisconnect),
        bob.websocket_connect(f"/ws/replay/{synth.TICKER}", headers={"origin": "http://evil.example"}) as ws,
    ):
        ws.receive_json()


def test_calibrate_live_run(env):
    app, sessions = env
    with sessions() as db:
        bob_id = db.scalar(select(User.id).where(User.username == "bob"))
        live_job = Job(user_id=bob_id, kind="live", status="running", spec={})
        db.add(live_job)
        db.flush()
        live = Run(user_id=bob_id, job_id=live_job.id, mode="live", strategy="noop", status="running")
        sim = Run(user_id=bob_id, mode="sim", strategy="noop", status="succeeded")
        db.add_all([live, sim])
        db.commit()
        live_id, sim_id, live_job_id = live.id, sim.id, live_job.id
    bob = login(app, "bob", USER_PW)
    assert bob.post(f"/api/runs/{sim_id}/calibrate").status_code == 422
    assert bob.post(f"/api/runs/{live_id}/calibrate").status_code == 409
    assert bob.post("/api/runs/99999/calibrate").status_code == 404
    with sessions() as db:
        db.get(Job, live_job_id).status = "succeeded"
        db.commit()
    r = bob.post(f"/api/runs/{live_id}/calibrate", json={"sim": {"latency_ms": 50}})
    assert r.status_code == 201, r.text
    admin = login(app, "admin", ADMIN_PW)
    assert admin.post(f"/api/runs/{live_id}/calibrate").status_code == 201
    with sessions() as db:
        jobs = db.scalars(select(Job).where(Job.kind == "calibration").order_by(Job.id)).all()
        assert [j.user_id for j in jobs] == [bob_id, bob_id]
        assert jobs[0].spec["live_run_id"] == live_id
        assert jobs[0].spec["sim"]["latency_ms"] == 50
