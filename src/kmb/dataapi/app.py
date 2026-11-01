"""Authenticated, read-only catalog of completed normalizer windows."""

from __future__ import annotations

import json
import os
import secrets
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from stat import S_ISREG

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from starlette.responses import FileResponse, JSONResponse, Response

from kmb.envelope.windows import window_relpath
from kmb.normalizer.index import WINDOW_ID, under


class Catalog:
    """Window catalog. ``ttl_s > 0`` serves a cached snapshot and refreshes it in the background."""

    def __init__(self, root: Path, ttl_s: float = 0.0):
        self.root = root.resolve()
        self.ttl_s = ttl_s
        self._cache: dict[Path, tuple[tuple, dict, Path]] = {}
        self._snapshot: list[dict] | None = None
        self._snapshot_at = 0.0
        self._by_id: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._state = threading.Lock()
        self._refreshing = False

    def _load(self, path: Path) -> tuple[tuple, dict, Path] | None:
        try:
            stat = path.stat()
            signature = (stat.st_mtime_ns, stat.st_size)
            cached = self._cache.get(path)
            if cached and cached[0] == signature:
                # Unchanged sidecar was fully validated before; only confirm the parquet is still intact.
                parquet_stat = cached[2].stat()
                if S_ISREG(parquet_stat.st_mode) and parquet_stat.st_size == cached[1]["size_bytes"]:
                    return cached
                return None
            if not path.resolve().is_relative_to(self.root):
                return None
            meta = json.loads(path.read_text(encoding="utf-8"))
            ticker = meta["window_id"]
            if not isinstance(ticker, str) or not WINDOW_ID.fullmatch(ticker):
                return None
            date = datetime.fromisoformat(meta["open_time"]).astimezone(UTC).date().isoformat()
            parquet_rel, json_rel = window_relpath(meta["series_ticker"], date, ticker)
            if meta["parquet"] != parquet_rel or path.resolve() != under(self.root, json_rel):
                return None
            parquet = under(self.root, meta["parquet"])
            if not parquet.is_file() or parquet.stat().st_size != meta["size_bytes"]:
                return None
            return signature, meta, parquet
        except (OSError, ValueError, TypeError, KeyError):
            return None

    def _scan(self) -> list[dict]:
        started = time.monotonic()
        current = {}
        for path in self.root.rglob("*.window.json"):
            loaded = self._load(path)
            if loaded is not None:
                current[path] = loaded
        self._cache = current
        snapshot = sorted(
            (item[1] for item in current.values()),
            key=lambda m: (m["open_ms"], m["window_id"]),
            reverse=True,
        )
        self._snapshot, self._snapshot_at = snapshot, started
        self._by_id = {m["window_id"]: m for m in snapshot}
        return snapshot

    def refresh(self) -> list[dict]:
        with self._lock:
            return self._scan()

    def _refresh_in_background(self) -> None:
        try:
            self.refresh()
        finally:
            with self._state:
                self._refreshing = False

    def windows(self) -> list[dict]:
        if self.ttl_s <= 0:
            return self.refresh()
        snapshot = self._snapshot
        if snapshot is None:
            with self._lock:
                return self._snapshot if self._snapshot is not None else self._scan()
        with self._state:
            stale = time.monotonic() - self._snapshot_at >= self.ttl_s
            start = stale and not self._refreshing
            self._refreshing = self._refreshing or start
        if start:
            threading.Thread(target=self._refresh_in_background, daemon=True).start()
        return snapshot

    def count(self) -> int:
        """Window count without ever waiting on a scan once caching is enabled."""
        if self.ttl_s > 0 and self._snapshot is None:
            return 0
        return len(self.windows())

    def get(self, window_id: str) -> dict:
        if not WINDOW_ID.fullmatch(window_id):
            raise HTTPException(400, "invalid window_id")
        if self.ttl_s > 0 and window_id in self._by_id:
            return self._by_id[window_id]
        # Not in the snapshot (or caching is off): read just this window's sidecar instead of rescanning.
        for path in sorted(self.root.glob(f"windows/*/*/{window_id}.window.json")):
            loaded = self._load(path)
            if loaded is not None and loaded[1]["window_id"] == window_id:
                return loaded[1]
        raise HTTPException(404, "window not found")


def create_app(
    root: Path | None = None, token: str | None = None, catalog_ttl_s: float | None = None
) -> FastAPI:
    root = root or Path(os.getenv("KMB_DATAAPI_DERIVED_ROOT", "/mnt/storage1/kalshi-derived"))
    if token is None:
        token = (
            Path(os.getenv("KMB_DATAAPI_TOKEN_FILE", "/run/secrets/data_api_token"))
            .read_text(encoding="utf-8")
            .strip()
        )
    if not token:
        raise ValueError("data API token cannot be empty")
    if catalog_ttl_s is None:
        catalog_ttl_s = float(os.getenv("KMB_DATAAPI_CATALOG_TTL_S", "30"))
    catalog = Catalog(root, catalog_ttl_s)
    if catalog_ttl_s > 0:
        threading.Thread(target=catalog.refresh, daemon=True).start()
    app = FastAPI(title="KMB replay data API", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.catalog = catalog

    def authorize(authorization: str | None = Header(default=None)):
        expected = "Bearer " + token
        if not authorization or not secrets.compare_digest(authorization.encode(), expected.encode()):
            raise HTTPException(401, "invalid bearer token", headers={"WWW-Authenticate": "Bearer"})

    @app.get("/health")
    def health():
        ok = root.is_dir()
        return JSONResponse({"ok": ok, "windows": catalog.count()}, status_code=200 if ok else 503)

    @app.get("/v1/windows", dependencies=[Depends(authorize)])
    def list_windows(
        series: str | None = None,
        from_: str | None = Query(None, alias="from"),
        to: str | None = None,
        limit: int = Query(100, ge=1, le=1000),
    ):
        def millis(value):
            if value is None:
                return None
            try:
                dt = datetime.fromisoformat(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return int(dt.timestamp() * 1000)
            except ValueError as exc:
                raise HTTPException(422, "from/to must be ISO timestamps") from exc

        lower, upper = millis(from_), millis(to)
        if lower is not None and upper is not None and lower > upper:
            raise HTTPException(422, "from is after to")
        return [
            m
            for m in catalog.windows()
            if (not series or m["series_ticker"] == series)
            and (lower is None or m["open_ms"] >= lower)
            and (upper is None or m["open_ms"] <= upper)
        ][:limit]

    @app.get("/v1/windows/{window_id}", dependencies=[Depends(authorize)])
    def get_window(window_id: str):
        return catalog.get(window_id)

    @app.get("/v1/windows/{window_id}/parquet", dependencies=[Depends(authorize)])
    def get_parquet(window_id: str, request: Request):
        meta = catalog.get(window_id)
        etag = f'"{meta["sha256"]}"'
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers={"ETag": etag})
        return FileResponse(
            under(root, meta["parquet"]),
            media_type="application/vnd.apache.parquet",
            filename=f"{window_id}.parquet",
            headers={"ETag": etag},
        )

    return app
