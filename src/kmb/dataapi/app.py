"""Authenticated, read-only catalog of completed normalizer windows."""

from __future__ import annotations

import json
import os
import secrets
import threading
from datetime import UTC, datetime
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from starlette.responses import FileResponse, JSONResponse, Response

from kmb.envelope.windows import window_relpath
from kmb.normalizer.index import WINDOW_ID, under


class Catalog:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self._cache: dict[Path, tuple[tuple, dict]] = {}
        self._lock = threading.Lock()

    def windows(self) -> list[dict]:
        with self._lock:
            current = {}
            for path in self.root.rglob("*.window.json"):
                try:
                    if not path.resolve().is_relative_to(self.root):
                        continue
                    stat = path.stat()
                    signature = (stat.st_mtime_ns, stat.st_size)
                    cached = self._cache.get(path)
                    meta = (
                        cached[1]
                        if cached and cached[0] == signature
                        else json.loads(path.read_text(encoding="utf-8"))
                    )
                    ticker = meta["window_id"]
                    if not isinstance(ticker, str) or not WINDOW_ID.fullmatch(ticker):
                        continue
                    date = datetime.fromisoformat(meta["open_time"]).astimezone(UTC).date().isoformat()
                    parquet_rel, json_rel = window_relpath(meta["series_ticker"], date, ticker)
                    if meta["parquet"] != parquet_rel or path.resolve() != under(self.root, json_rel):
                        continue
                    parquet = under(self.root, meta["parquet"])
                    if not parquet.is_file() or parquet.stat().st_size != meta["size_bytes"]:
                        continue
                    current[path] = (signature, meta)
                except (OSError, ValueError, TypeError, KeyError):
                    continue
            self._cache = current
            return sorted(
                (item[1] for item in current.values()),
                key=lambda m: (m["open_ms"], m["window_id"]),
                reverse=True,
            )

    def get(self, window_id: str) -> dict:
        if not WINDOW_ID.fullmatch(window_id):
            raise HTTPException(400, "invalid window_id")
        for meta in self.windows():
            if meta["window_id"] == window_id:
                return meta
        raise HTTPException(404, "window not found")


def create_app(root: Path | None = None, token: str | None = None) -> FastAPI:
    root = root or Path(os.getenv("KMB_DATAAPI_DERIVED_ROOT", "/mnt/storage1/kalshi-derived"))
    if token is None:
        token = (
            Path(os.getenv("KMB_DATAAPI_TOKEN_FILE", "/run/secrets/data_api_token"))
            .read_text(encoding="utf-8")
            .strip()
        )
    if not token:
        raise ValueError("data API token cannot be empty")
    catalog = Catalog(root)
    app = FastAPI(title="KMB replay data API", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.catalog = catalog

    def authorize(authorization: str | None = Header(default=None)):
        expected = "Bearer " + token
        if not authorization or not secrets.compare_digest(authorization.encode(), expected.encode()):
            raise HTTPException(401, "invalid bearer token", headers={"WWW-Authenticate": "Bearer"})

    @app.get("/health")
    def health():
        ok = root.is_dir()
        return JSONResponse({"ok": ok, "windows": len(catalog.windows())}, status_code=200 if ok else 503)

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
