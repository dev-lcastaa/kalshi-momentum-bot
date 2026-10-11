from __future__ import annotations

import json
import os
import socket
import sys
import threading
from datetime import timedelta
from decimal import Decimal

import anyio
import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.server.fastmcp.exceptions import ToolError
from sqlalchemy import select

from kmb.backend.app import create_app
from kmb.backend.security import hash_password
from kmb.backend.settings import BackendSettings
from kmb.db import init_db, make_engine, ops, session_factory
from kmb.db.models import Job, ResearchToken, RiskLimit, Run, User, utcnow
from kmb.mcp.server import ResearchClient, Settings, create_server
from kmb.replay.source import LocalWindowSource
from kmb.research import engine_fingerprint
from kmb.worker import jobs as worker_jobs
from kmb.worker.feed import ListFeed
from kmb.worker.jobs import JobControl, WorkerContext
from kmb.worker.main import process_one
from tests import synth

PASSWORD = "research-password"
TEST_TICKER = "KXBTC15M-26JAN011215-00"


@pytest.fixture
def env(tmp_path, monkeypatch):
    engine = make_engine(f"sqlite:///{tmp_path / 'research.sqlite'}")
    init_db(engine)
    factory = session_factory(engine)
    with factory() as db:
        db.add(User(username="researcher", password_hash=hash_password(PASSWORD)))
        db.add(User(username="outsider", password_hash=hash_password(PASSWORD)))
        db.commit()
    root = tmp_path / "derived"
    synth.write_window(root)
    with monkeypatch.context() as patch:
        patch.setattr(synth, "TICKER", TEST_TICKER)
        patch.setattr(synth, "OPEN_MS", synth.OPEN_MS + 900_000)
        patch.setattr(synth, "CLOSE_MS", synth.CLOSE_MS + 900_000)
        synth.write_window(root)
    source = LocalWindowSource(root)
    app = create_app(BackendSettings(static_dir=None, collector_status_url=None), factory, source)
    human = TestClient(app)
    response = human.post("/api/auth/login", json={"username": "researcher", "password": PASSWORD})
    human.headers["X-KMB-CSRF"] = response.json()["csrf"]
    token = human.post("/api/research/tokens", json={"label": "test agent"}).json()
    agent = TestClient(app, headers={"Authorization": f"Bearer {token['token']}"})
    worker = WorkerContext(
        factory=factory,
        worker_id="research-worker",
        source_factory=lambda: source,
        feed_factory=lambda: ListFeed([]),
        control_every_s=0.01,
    )
    return app, factory, human, agent, worker, token, root


def plan(**changes):
    return {
        "name": "Baseline experiment",
        "hypothesis": "Noop checks the research pipeline.",
        "success_criteria": "Both splits replay without gaps; no trading is expected.",
        "train_windows": [synth.TICKER],
        "test_windows": [TEST_TICKER],
        "candidates": [{"label": "baseline", "strategy": "noop"}],
    } | changes


def experiment(agent, **changes):
    response = agent.post("/api/research/experiments", json=plan(**changes))
    assert response.status_code == 201, response.text
    return response.json()


async def complete_split(agent, worker, experiment_id, split):
    response = agent.post(f"/api/research/experiments/{experiment_id}/batches", json={"split": split})
    assert response.status_code == 201, response.text
    for _ in response.json()["jobs"]:
        assert await process_one(worker, ("sim",))
    return response.json()["jobs"]


def test_scoped_credentials_and_revocation(env):
    _, factory, human, agent, _, token, _ = env
    assert agent.get("/api/research/capabilities").status_code == 200
    for path in ("/api/auth/me", "/api/keys", "/api/bots", "/api/jobs", "/api/users"):
        assert agent.get(path).status_code == 401
    assert agent.post("/api/jobs", json={"kind": "live", "bot_id": 1}).status_code == 401
    assert agent.post("/api/research/tokens", json={"label": "escalate"}).status_code == 401
    assert agent.get("/api/research/tokens").status_code == 401
    assert agent.post("/api/research/paper-proposals/1/approve").status_code == 401
    with factory() as db:
        assert db.get(ResearchToken, token["id"]).id != token["token"]
    assert human.delete(f"/api/research/tokens/{token['id']}").status_code == 204
    assert agent.get("/api/research/capabilities").status_code == 401


def test_expired_tokens_and_csrf(env):
    _, factory, human, agent, _, token, _ = env
    with factory() as db:
        db.get(ResearchToken, token["id"]).expires_at = utcnow() - timedelta(seconds=1)
        db.commit()
    assert agent.get("/api/research/capabilities").status_code == 401
    human.headers.pop("X-KMB-CSRF")
    assert human.post("/api/research/experiments", json=plan()).status_code == 403


@pytest.mark.parametrize(
    "changes",
    [
        {"test_windows": [synth.TICKER]},
        {"train_windows": [synth.TICKER, synth.TICKER]},
        {"train_windows": [TEST_TICKER], "test_windows": [synth.TICKER]},
        {"candidates": [{"label": "bad", "strategy": "momentum", "params": {"ref_source": "wrong"}}]},
        {"candidates": [{"label": "bad", "strategy": "momentum", "params": {"max_spread": "-1"}}]},
        {"candidates": [{"label": "bad", "strategy": "momentum", "params": {"max_hold_s": -1}}]},
        {"candidates": [{"label": "bad", "strategy": "momentum", "sim": {"fee_multiplier": 0}}]},
        {"candidates": [{"label": "bad", "strategy": "noop", "series": []}]},
        {"candidates": [{"label": "bad", "strategy": "noop", "params": {"unknown": 1}}]},
        {"candidates": [{"label": str(i), "strategy": "noop"} for i in range(13)]},
        {"train_windows": [str(i) for i in range(101)]},
        {"live": True},
    ],
)
def test_invalid_plans_rejected(env, changes):
    _, _, _, agent, _, _, _ = env
    assert agent.post("/api/research/experiments", json=plan(**changes)).status_code == 422


def test_window_quality_and_ownership(env):
    app, _, _, agent, _, _, root = env
    row = experiment(agent)
    foreign = TestClient(app)
    response = foreign.post("/api/auth/login", json={"username": "outsider", "password": PASSWORD})
    foreign.headers["X-KMB-CSRF"] = response.json()["csrf"]
    assert foreign.get(f"/api/research/experiments/{row['id']}").status_code == 404
    path = next(root.rglob("*.window.json"))
    metadata = json.loads(path.read_text())
    metadata["quality"] = "bad"
    path.write_text(json.dumps(metadata))
    assert agent.post("/api/research/experiments", json=plan()).status_code == 422
    overview = agent.get(f"/api/research/windows/{synth.TICKER}/overview?limit=2").json()
    assert len(overview["series"]) == 2
    assert overview["counts"]["series"] > 2
    assert agent.get("/api/research/windows?limit=501").status_code == 422


async def test_end_to_end_research_and_human_approval(env, monkeypatch):
    _, factory, human, agent, worker, _, _ = env
    row = experiment(agent, candidates=[{"label": "scalper", "strategy": "momentum"}])
    eid = row["id"]
    assert row["plan"]["candidates"][0]["params"]["take_profit"] == "0.04"
    assert row["plan"]["candidates"][0]["sim"]["fee_multiplier"] == 1
    assert row["plan"]["engine_fingerprint"] == engine_fingerprint()
    assert agent.post(f"/api/research/experiments/{eid}/batches", json={"split": "test"}).status_code == 409
    train = await complete_split(agent, worker, eid, "train")
    repeated = agent.post(f"/api/research/experiments/{eid}/batches", json={"split": "train"}).json()
    assert repeated["jobs"][0]["id"] == train[0]["id"]
    assert (
        agent.post(
            f"/api/research/experiments/{eid}/paper-proposals",
            json={"candidate": 0, "rationale": "too early"},
        ).status_code
        == 409
    )
    test = await complete_split(agent, worker, eid, "test")
    result = agent.get(f"/api/research/experiments/{eid}/results").json()
    assert all(trial["valid_for_comparison"] for trial in result["trials"])
    assert result["trials"][0]["summary"]["n_fills"] > 0
    assert result["trials"][0]["window_stats"]["traded_windows"] == 1
    run = agent.get(f"/api/research/jobs/{test[0]['id']}/run").json()
    assert run["sim_config"]["seed"] == 0
    assert "equity_curve" not in run["summary"]
    details = agent.get(f"/api/research/jobs/{test[0]['id']}/run?section=fills&limit=1").json()
    assert len(details["items"]) == 1
    agent.post(
        f"/api/research/experiments/{eid}/notes", json={"text": "Baseline completed; no profit claim."}
    )
    assert agent.get(f"/api/research/experiments/{eid}").json()["notes"][0]["text"].startswith("Baseline")
    proposal = agent.post(
        f"/api/research/experiments/{eid}/paper-proposals",
        json={
            "candidate": 0,
            "rationale": "Evaluate live-feed execution next.",
            "duration_minutes": 1,
        },
    ).json()
    assert proposal["status"] == "pending" and proposal["job_id"] is None
    assert proposal["spec"]["duration_seconds"] == 60
    assert agent.post(f"/api/research/paper-proposals/{proposal['id']}/approve").status_code == 401
    approved = human.post(f"/api/research/paper-proposals/{proposal['id']}/approve").json()
    assert approved["status"] == "approved"
    assert human.post(f"/api/research/paper-proposals/{proposal['id']}/approve").status_code == 409
    second = agent.post(
        f"/api/research/experiments/{eid}/paper-proposals",
        json={
            "candidate": 0,
            "rationale": "Separate launch needs its own approval.",
        },
    ).json()
    assert human.post(f"/api/research/paper-proposals/{second['id']}/approve").status_code == 409
    with factory() as db:
        job = db.get(Job, approved["job_id"])
        assert job.kind == "paper" and job.bot_id is None
        assert job.spec["bot"]["key_id"] is None
        assert job.spec["duration_seconds"] == 60

    def expired_control(*args, **kwargs):
        control = JobControl(*args, **kwargs)
        control.started_monotonic -= 61
        return control

    monkeypatch.setattr(worker_jobs, "JobControl", expired_control)
    assert await process_one(worker, ("paper",))
    with factory() as db:
        run = db.scalar(select(Run).where(Run.job_id == approved["job_id"]))
        assert run.summary["stop_reason"] == "approved duration reached"


async def test_budget_is_atomic_and_ordinary_jobs_are_hidden(env):
    _, factory, _, agent, _, _, _ = env
    candidates = [{"label": str(i), "strategy": "noop"} for i in range(12)]
    first, second, third = [experiment(agent, candidates=candidates) for _ in range(3)]
    for row in (first, second):
        assert (
            agent.post(f"/api/research/experiments/{row['id']}/batches", json={"split": "train"}).status_code
            == 201
        )
    assert (
        agent.post(f"/api/research/experiments/{third['id']}/batches", json={"split": "train"}).status_code
        == 409
    )
    with factory() as db:
        assert len(list(db.scalars(select(Job)))) == 24
        ordinary = Job(user_id=1, kind="live", spec={})
        db.add(ordinary)
        db.commit()
        jid = ordinary.id
    assert agent.get(f"/api/research/jobs/{jid}").status_code == 404
    assert agent.post(f"/api/research/jobs/{jid}/stop").status_code == 404
    jobs = agent.get(f"/api/research/experiments/{first['id']}/results").json()["trials"]
    assert agent.post(f"/api/research/jobs/{jobs[0]['job']['id']}/stop").json()["status"] == "canceled"


async def test_missing_or_changed_research_recording_fails(env):
    _, factory, _, agent, worker, _, root = env
    row = experiment(agent)
    response = agent.post(f"/api/research/experiments/{row['id']}/batches", json={"split": "train"}).json()
    path = next(path for path in root.rglob("*.window.json") if synth.TICKER in path.name)
    path.unlink()
    assert await process_one(worker, ("sim",))
    job = agent.get(f"/api/research/jobs/{response['jobs'][0]['id']}").json()
    assert job["status"] == "failed" and "research window unavailable" in job["error"]
    results = agent.get(f"/api/research/experiments/{row['id']}/results").json()
    assert results["trials"][0]["valid_for_comparison"] is False
    assert (
        agent.post(f"/api/research/experiments/{row['id']}/batches", json={"split": "test"}).status_code
        == 409
    )
    with factory() as db:
        assert db.scalar(select(Run)).status == "failed"


async def test_changed_manifest_and_worker_engine_are_rejected(env):
    _, factory, _, agent, worker, _, root = env
    eid = experiment(agent)["id"]
    response = agent.post(f"/api/research/experiments/{eid}/batches", json={"split": "train"}).json()
    path = next(path for path in root.rglob("*.window.json") if synth.TICKER in path.name)
    metadata = json.loads(path.read_text())
    metadata["sha256"] = "changed-content"
    path.write_text(json.dumps(metadata))
    assert await process_one(worker, ("sim",))
    assert agent.get(f"/api/research/jobs/{response['jobs'][0]['id']}").json()["status"] == "failed"
    next_eid = experiment(agent)["id"]
    response = agent.post(f"/api/research/experiments/{next_eid}/batches", json={"split": "train"}).json()
    jid = response["jobs"][0]["id"]
    with factory() as db:
        job = db.get(Job, jid)
        job.spec = job.spec | {"research": job.spec["research"] | {"engine_fingerprint": "different-build"}}
        db.commit()
    assert await process_one(worker, ("sim",))
    failed = agent.get(f"/api/research/jobs/{jid}").json()
    assert failed["status"] == "failed" and "fingerprint" in failed["error"]


async def test_notes_pagination_and_incomplete_metrics(env):
    _, factory, _, agent, worker, _, _ = env
    eid = experiment(agent)["id"]
    for index in range(7):
        assert (
            agent.post(
                f"/api/research/experiments/{eid}/notes", json={"text": f"finding-{index}"}
            ).status_code
            == 200
        )
    detail = agent.get(f"/api/research/experiments/{eid}").json()
    assert detail["notes_total"] == 7 and len(detail["notes"]) == 5
    assert detail["notes"][0]["text"] == "finding-6"
    older = agent.get(f"/api/research/experiments/{eid}?notes_offset=5&notes_limit=2").json()
    assert [note["text"] for note in older["notes"]] == ["finding-1", "finding-0"]
    manifest = agent.get(f"/api/research/experiments/{eid}?include_manifest=true").json()
    assert manifest["plan"]["train_windows"][0]["sha256"] == "x"
    assert agent.patch(f"/api/research/experiments/{eid}", json={"name": "changed"}).status_code == 405
    jobs = await complete_split(agent, worker, eid, "train")
    with factory() as db:
        run = db.scalar(select(Run).where(Run.job_id == jobs[0]["id"]))
        run.summary = run.summary | {"windows": []}
        db.commit()
    trial = agent.get(f"/api/research/experiments/{eid}/results").json()["trials"][0]
    assert trial["valid_for_comparison"] is False and trial["comparison_issue"]


async def test_pending_proposal_limit(env):
    _, _, _, agent, worker, _, _ = env
    eid = experiment(agent)["id"]
    await complete_split(agent, worker, eid, "train")
    await complete_split(agent, worker, eid, "test")
    for _ in range(10):
        assert (
            agent.post(
                f"/api/research/experiments/{eid}/paper-proposals",
                json={
                    "candidate": 0,
                    "rationale": "Approval budget check.",
                },
            ).status_code
            == 201
        )
    assert (
        agent.post(
            f"/api/research/experiments/{eid}/paper-proposals",
            json={
                "candidate": 0,
                "rationale": "Must exceed pending budget.",
            },
        ).status_code
        == 409
    )


def test_approved_risk_caps_cannot_be_loosened(env):
    _, factory, _, _, worker, _, _ = env
    with factory() as db:
        job = Job(
            user_id=1,
            kind="paper",
            spec={
                "risk_limits": {
                    "max_order_count": "1",
                    "max_position_per_market": "2",
                    "max_open_orders": "2",
                    "max_daily_loss": "3",
                    "max_notional_per_market": "2",
                    "max_orders_per_minute": "5",
                }
            },
        )
        db.add(job)
        db.commit()
        control = JobControl(worker, job)
    assert control.poll(force=True).limits.max_order_count == Decimal(1)
    with factory() as db:
        db.scalar(select(RiskLimit)).max_order_count = Decimal("0.5")
        db.commit()
    assert control.poll(force=True).limits.max_order_count == Decimal("0.5")


async def test_kill_switch_blocks_human_approval(env):
    _, factory, human, agent, worker, _, _ = env
    eid = experiment(agent)["id"]
    await complete_split(agent, worker, eid, "train")
    await complete_split(agent, worker, eid, "test")
    proposal = agent.post(
        f"/api/research/experiments/{eid}/paper-proposals",
        json={
            "candidate": 0,
            "rationale": "Check controls.",
        },
    ).json()
    with factory() as db:
        ops.set_kill_switch(db, "global", True, 1, "test")
        db.commit()
    assert human.post(f"/api/research/paper-proposals/{proposal['id']}/approve").status_code == 403
    assert human.post(f"/api/research/paper-proposals/{proposal['id']}/reject").json()["status"] == "rejected"


async def test_mcp_tool_schema_and_errors(env):
    app, _, _, _, _, token, _ = env
    server = create_server(Settings("http://localhost", token["token"]))
    tools = await server.list_tools()
    names = {tool.name for tool in tools}
    assert {
        "create_experiment",
        "submit_simulation_batch",
        "propose_paper_session",
        "compare_experiment",
    } <= names
    assert len(tools) == 14
    assert all("ctx" not in tool.inputSchema["properties"] for tool in tools)
    assert not any("approve" in name or "live" in name or "token" in name for name in names)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://localhost",
        headers={"Authorization": f"Bearer {token['token']}"},
    ) as http:
        client = ResearchClient(http)
        assert (await client.request("GET", "/capabilities"))["live_trading"] is False
        with pytest.raises(ToolError, match="returned 404"):
            await client.request("GET", "/jobs/999")


def test_mcp_transport_configuration(tmp_path, monkeypatch):
    token_path = tmp_path / "token"
    token_path.write_text("test-token")
    monkeypatch.setenv("KMB_MCP_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("KMB_MCP_BACKEND_URL", "http://127.0.0.1:8800")
    assert Settings.from_env().token == "test-token"
    for url in ("http://192.168.1.181:8800", "https://user:pass@example.com", "https://example.com/api"):
        monkeypatch.setenv("KMB_MCP_BACKEND_URL", url)
        with pytest.raises(ValueError):
            Settings.from_env()


async def test_stdio_protocol_end_to_end(env, tmp_path):
    app, _, _, _, _, token, _ = env
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    started = threading.Event()

    class TestServer(uvicorn.Server):
        async def startup(self, sockets=None):
            await super().startup(sockets)
            started.set()

    server = TestServer(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    token_path = tmp_path / "mcp-token"
    token_path.write_text(token["token"])
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "kmb.mcp"],
        env=os.environ
        | {"KMB_MCP_BACKEND_URL": f"http://127.0.0.1:{port}", "KMB_MCP_TOKEN_FILE": str(token_path)},
    )
    try:
        assert await anyio.to_thread.run_sync(started.wait, 5)
        async with httpx.AsyncClient() as http:
            assert (await http.get(f"http://127.0.0.1:{port}/health")).status_code == 200
        with anyio.fail_after(30):
            async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                assert len(tools.tools) == 14
                response = await session.call_tool("research_capabilities", {})
                assert not response.isError
                assert response.structuredContent["live_trading"] is False
                created = await session.call_tool("create_experiment", plan())
                assert not created.isError
                eid = created.structuredContent["id"]
                batch = await session.call_tool(
                    "submit_simulation_batch", {"experiment_id": eid, "split": "train"}
                )
                assert not batch.isError and batch.structuredContent["jobs"][0]["status"] == "queued"
                rejected = await session.call_tool(
                    "submit_simulation_batch", {"experiment_id": eid, "split": "test"}
                )
                assert rejected.isError
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        listener.close()
        assert not thread.is_alive()
