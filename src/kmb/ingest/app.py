from __future__ import annotations

import hmac
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from kmb.envelope import BatchMeta, BatchValidationError
from kmb.ingest.archive import Archive, IngestError
from kmb.ingest.config import IngestConfig
from kmb.ingest.receipts import ReceiptStore
from kmb.ingest.storage import StorageGuard

log = logging.getLogger("kmb.ingest.api")

META_HEADER = "X-Batch-Meta"


def build_archive(cfg: IngestConfig, guard: StorageGuard | None = None) -> Archive:
    guard = guard or StorageGuard(
        cfg.archive_root,
        cfg.archive_id,
        min_free_bytes=cfg.min_free_bytes,
        expected_fs_uuid=cfg.expected_fs_uuid,
        expected_fstype=cfg.expected_fstype,
        by_uuid_dir=cfg.by_uuid_dir,
        mountinfo_path=cfg.mountinfo_path,
    )
    return Archive(
        cfg.archive_root,
        ReceiptStore(cfg.db_path),
        guard,
        max_uncompressed_bytes=cfg.max_uncompressed_bytes,
        verify_recent_hours=cfg.verify_recent_hours,
    )


def create_app(cfg: IngestConfig, archive: Archive | None = None) -> FastAPI:
    archive = archive or build_archive(cfg)
    expected = f"Bearer {cfg.token}".encode()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        status = archive.guard.check(probe_write=True)
        if status.ok:
            await run_in_threadpool(archive.reconcile)
        else:
            # Stay up and report not-ready; reconcile runs once storage is back (next write re-checks).
            log.error("archive not ready at startup", extra={"ctx": {"reason": status.reason}})
        yield
        archive.receipts.close()

    app = FastAPI(title="kmb-ingest", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.archive = archive

    def require_token(authorization: str | None = Header(default=None)) -> None:
        if authorization is None or not hmac.compare_digest(authorization.encode(), expected):
            raise IngestError("unauthorized", 401, False, "missing or invalid token")

    @app.exception_handler(IngestError)
    async def _ingest_error(_: Request, exc: IngestError) -> JSONResponse:
        return JSONResponse(exc.to_dict(), status_code=exc.status)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "service": "kmb-ingest"}

    @app.get("/ready")
    async def ready() -> JSONResponse:
        status = await run_in_threadpool(archive.guard.check, probe_write=True, use_cache=True)
        db_ok = await run_in_threadpool(archive.receipts.ping)
        ok = status.ok and db_ok
        body = {
            "ready": ok,
            "reason": status.reason if not status.ok else (None if db_ok else "receipt index unavailable"),
            "checks": {**status.checks, "receipt_db": db_ok},
            "free_bytes": status.free_bytes,
            "min_free_bytes": archive.guard.min_free_bytes,
            "last_durable_write_utc": archive.last_durable_write_utc,
            "recent_errors": list(archive.recent_errors),
            "receipts": await run_in_threadpool(archive.receipts.counts) if db_ok else None,
        }
        return JSONResponse(body, status_code=200 if ok else 503)

    @app.put("/v1/batches/{batch_id}", dependencies=[Depends(require_token)])
    async def put_batch(batch_id: str, request: Request) -> JSONResponse:
        raw_meta = request.headers.get(META_HEADER)
        if not raw_meta:
            raise IngestError("missing_metadata", 400, False, f"{META_HEADER} header required")
        try:
            meta = BatchMeta.from_json(raw_meta)
        except BatchValidationError as exc:
            raise IngestError("bad_metadata", 400, False, str(exc)) from exc
        if meta.batch_id != batch_id:
            raise IngestError("bad_metadata", 400, False, "path batch_id differs from metadata")

        declared = request.headers.get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > cfg.max_batch_bytes:
            raise IngestError("batch_too_large", 413, False, f"batch exceeds {cfg.max_batch_bytes} bytes")
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > cfg.max_batch_bytes:
                raise IngestError("batch_too_large", 413, False, f"batch exceeds {cfg.max_batch_bytes} bytes")

        result = await run_in_threadpool(archive.put, meta, bytes(body))
        return JSONResponse(
            {"status": "created" if result.created else "duplicate", "receipt": _public(result.receipt)},
            status_code=201 if result.created else 200,
        )

    @app.get("/v1/batches/{batch_id}", dependencies=[Depends(require_token)])
    async def get_batch(batch_id: str) -> JSONResponse:
        receipt = await run_in_threadpool(archive.receipts.get, batch_id)
        if receipt is None:
            raise IngestError("not_found", 404, False, "unknown batch_id")
        return JSONResponse({"receipt": _public(receipt)})

    @app.get("/v1/captures/{capture_id}/coverage", dependencies=[Depends(require_token)])
    async def coverage(capture_id: str) -> dict:
        return await run_in_threadpool(archive.receipts.coverage, capture_id)

    return app


def _public(receipt: dict) -> dict:
    return {k: v for k, v in receipt.items() if k != "meta_json"}
