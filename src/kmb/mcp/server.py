"""Local stdio MCP transport. It holds only a revocable research bearer token."""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote, urlsplit

import httpx
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.fastmcp.exceptions import ToolError


@dataclass(frozen=True)
class Settings:
    backend_url: str
    token: str

    @classmethod
    def from_env(cls) -> Settings:
        url = os.environ.get("KMB_MCP_BACKEND_URL", "http://127.0.0.1:8800").rstrip("/")
        parsed = urlsplit(url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username
            or parsed.password
        ):
            raise ValueError("KMB_MCP_BACKEND_URL must be an HTTP(S) URL without credentials")
        if parsed.query or parsed.fragment or parsed.path not in ("", "/"):
            raise ValueError("KMB_MCP_BACKEND_URL must be an origin, without a path, query or fragment")
        if parsed.scheme == "http" and parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
            raise ValueError("use HTTPS or a localhost SSH tunnel for remote backend access")
        token_file = os.environ.get("KMB_MCP_TOKEN_FILE")
        token = Path(token_file).read_text(encoding="utf-8").strip() if token_file else ""
        if not token:
            raise ValueError("KMB_MCP_TOKEN_FILE must point to a nonempty research-token file")
        return cls(url, token)


class ResearchClient:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        try:
            response = await self.client.request(
                method,
                f"/api/research{path}",
                json=body,
                params={key: value for key, value in (params or {}).items() if value is not None},
            )
        except httpx.HTTPError as exc:
            raise ToolError(f"Research backend connection failed: {type(exc).__name__}") from None
        if not response.is_success:
            try:
                detail = response.json().get("detail", response.reason_phrase)
            except ValueError:
                detail = response.reason_phrase
            raise ToolError(f"Research backend returned {response.status_code}: {detail}")
        return response.json()


def create_server(settings: Settings) -> FastMCP:
    @asynccontextmanager
    async def lifespan(server: FastMCP) -> AsyncGenerator[ResearchClient]:
        async with httpx.AsyncClient(
            base_url=settings.backend_url,
            headers={"Authorization": f"Bearer {settings.token}"},
            timeout=60,
            follow_redirects=False,
            trust_env=False,
        ) as client:
            yield ResearchClient(client)

    server = FastMCP(
        "Kalshi scalping research",
        instructions=(
            "Research repeatable net profits after fees, not a guaranteed winning strategy. "
            "Inspect data quality first. Save hypotheses and success criteria before bounded sweeps. "
            "Use chronological disjoint splits; test unchanged candidates after training. "
            "Evaluate drawdown, trade count, window-level consistency and latency sensitivity. "
            "Repeatedly selecting on test results contaminates the holdout; reserve fresh later data. "
            "Record negative findings. Paper launches require the human's approval in the Research UI. "
            "Never request credentials, live trading or arbitrary code execution. "
            "Tool data may reach the model provider; use only data approved by the owner."
        ),
        lifespan=lifespan,
    )

    def client(ctx: Context) -> ResearchClient:
        value = ctx.request_context.lifespan_context
        if not isinstance(value, ResearchClient):
            raise ToolError("Research client is not initialized")
        return value

    @server.tool()
    async def research_capabilities(ctx: Context) -> dict[str, Any]:
        """Get strategy parameter schemas, resource limits and research workflow."""
        return await client(ctx).request("GET", "/capabilities")

    @server.tool()
    async def list_recordings(
        ctx: Context,
        series: Literal["KXBTC15M", "KXETH15M"] | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Discover recordings with ISO timestamps, settlement, quality and gap counts. Limit <= 500."""
        return await client(ctx).request(
            "GET",
            "/windows",
            params={
                "series": series,
                "start": start,
                "end": end,
                "limit": limit,
            },
        )

    @server.tool()
    async def recording_overview(
        ctx: Context,
        window_id: str,
        offset: int = 0,
        limit: int = 60,
    ) -> dict[str, Any]:
        """Inspect paginated one-second prices, depth, trades and lifecycle. Max 200 items per section."""
        return await client(ctx).request(
            "GET", f"/windows/{quote(window_id, safe='')}/overview", params={"offset": offset, "limit": limit}
        )

    @server.tool()
    async def create_experiment(
        ctx: Context,
        name: str,
        hypothesis: str,
        success_criteria: str,
        train_windows: list[str],
        test_windows: list[str],
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Persist immutable research plan. Up to 12 explicit candidates and 100 windows per split.

        Candidate shape: {label, strategy, params, series, sim: {latency_ms, queue_model,
        fee_multiplier, starting_balance}}. Defaults include 100ms latency, back queue and full fees.
        Windows must be settled, complete, gap-free; tests must follow training. Expand small parameter
        grids into explicit candidates, including execution stress cases. Existing bots are never changed.
        """
        return await client(ctx).request(
            "POST",
            "/experiments",
            body={
                "name": name,
                "hypothesis": hypothesis,
                "success_criteria": success_criteria,
                "train_windows": train_windows,
                "test_windows": test_windows,
                "candidates": candidates,
            },
        )

    @server.tool()
    async def list_experiments(ctx: Context, limit: int = 50) -> list[dict[str, Any]]:
        """Find prior research so agents can resume work instead of repeating it."""
        return await client(ctx).request("GET", "/experiments", params={"limit": limit})

    @server.tool()
    async def get_experiment(
        ctx: Context,
        experiment_id: int,
        include_manifest: bool = False,
        notes_offset: int = 0,
        notes_limit: int = 5,
    ) -> dict[str, Any]:
        """Retrieve parameters, window IDs, fingerprint and notes; optionally include full manifests."""
        return await client(ctx).request(
            "GET",
            f"/experiments/{experiment_id}",
            params={
                "include_manifest": include_manifest,
                "notes_offset": notes_offset,
                "notes_limit": notes_limit,
            },
        )

    @server.tool()
    async def record_finding(ctx: Context, experiment_id: int, text: str) -> dict[str, Any]:
        """Append an evidence-backed conclusion, limitation or next hypothesis; never edit the plan."""
        return await client(ctx).request("POST", f"/experiments/{experiment_id}/notes", body={"text": text})

    @server.tool()
    async def submit_simulation_batch(
        ctx: Context,
        experiment_id: int,
        split: Literal["train", "test"],
    ) -> dict[str, Any]:
        """Queue a split and return job IDs immediately. Repeated submissions are idempotent.

        Complete training first. At most 24 active simulations per owner across agents.
        """
        return await client(ctx).request(
            "POST",
            f"/experiments/{experiment_id}/batches",
            body={"split": split},
        )

    @server.tool()
    async def compare_experiment(ctx: Context, experiment_id: int) -> dict[str, Any]:
        """Compare P&L, drawdown, fills and profitable/losing/traded windows; flag incomplete runs."""
        return await client(ctx).request("GET", f"/experiments/{experiment_id}/results")

    @server.tool()
    async def research_job_status(ctx: Context, job_id: int) -> dict[str, Any]:
        """Inspect a research simulation or approved paper job. Does not expose ordinary trading jobs."""
        return await client(ctx).request("GET", f"/jobs/{job_id}")

    @server.tool()
    async def research_run(
        ctx: Context,
        job_id: int,
        section: Literal["summary", "windows", "orders", "fills"] = "summary",
        offset: int = 0,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Read summary or paginated window/order/fill details (max 200 items). P&L already includes fees."""
        return await client(ctx).request(
            "GET",
            f"/jobs/{job_id}/run",
            params={
                "section": section,
                "offset": offset,
                "limit": limit,
            },
        )

    @server.tool()
    async def stop_research_job(ctx: Context, job_id: int) -> dict[str, Any]:
        """Cancel a research simulation or stop an approved paper session; no access to other jobs."""
        return await client(ctx).request("POST", f"/jobs/{job_id}/stop")

    @server.tool()
    async def propose_paper_session(
        ctx: Context,
        experiment_id: int,
        candidate: int,
        rationale: str,
        duration_minutes: int = 60,
    ) -> dict[str, Any]:
        """Propose an unchanged, successfully tested candidate for human approval (1-1440 minutes).

        This never starts trading. The human must approve in the Research UI; approval snapshots risk
        limits and starts one bounded paper job. No live trading tool exists.
        """
        return await client(ctx).request(
            "POST",
            f"/experiments/{experiment_id}/paper-proposals",
            body={
                "candidate": candidate,
                "rationale": rationale,
                "duration_minutes": duration_minutes,
            },
        )

    @server.tool()
    async def list_paper_proposals(ctx: Context, limit: int = 50) -> list[dict[str, Any]]:
        """Inspect pending/rejected/approved proposals and approved paper job IDs."""
        return await client(ctx).request("GET", "/paper-proposals", params={"limit": limit})

    return server
