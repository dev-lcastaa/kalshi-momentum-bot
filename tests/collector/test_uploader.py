from __future__ import annotations

import hashlib
import json
from pathlib import Path

import httpx

from kmb.collector.spool import Spool
from kmb.collector.uploader import BatchUploader
from kmb.envelope import KIND_FRAME, SOURCE_KALSHI, Sequencer, make_envelope


def make_spool(root: Path) -> Spool:
    spool = Spool(root, "upload-test")
    envelope = make_envelope(
        Sequencer(),
        capture_id="upload-test",
        stream="kalshi_ws",
        kind=KIND_FRAME,
        source=SOURCE_KALSHI,
        raw='{"type":"ticker"}',
    )
    assert spool.append(envelope)
    spool.seal_all()
    return spool


async def test_uploader_put_confirm_then_delete(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    batch = spool.pending_batches()[0]
    requests: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.method)
        if request.method == "PUT":
            metadata = json.loads(request.headers["X-Batch-Meta"])
            assert metadata["sha256"] == hashlib.sha256(request.content).hexdigest()
            assert request.headers["Authorization"] == "Bearer " + "x" * 32
            return httpx.Response(201, json={"status": "created"})
        return httpx.Response(200, json={"receipt": {"status": "committed", "sha256": batch.meta.sha256}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    uploader = BatchUploader(spool, "http://ingest", "x" * 32, client=client)
    assert await uploader.upload_one(batch) == "uploaded"
    assert requests == ["PUT", "GET"]
    assert spool.pending_batches() == []
    await client.aclose()


async def test_uploader_retryable_and_permanent_failures(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    batch = spool.pending_batches()[0]

    async def retry_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"retryable": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(retry_handler))
    uploader = BatchUploader(spool, "http://ingest", "x" * 32, client=client)
    assert await uploader.upload_one(batch) == "retry"
    await client.aclose()

    async def reject_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"retryable": False})

    client = httpx.AsyncClient(transport=httpx.MockTransport(reject_handler))
    uploader = BatchUploader(spool, "http://ingest", "x" * 32, client=client)
    assert await uploader.upload_one(batch) == "rejected"
    assert list((tmp_path / "rejected").glob("*.jsonl.gz"))
    await client.aclose()


async def test_uploader_matches_real_ingest_contract(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from kmb.ingest.app import create_app
    from kmb.ingest.archive import Archive
    from kmb.ingest.config import IngestConfig
    from kmb.ingest.receipts import ReceiptStore
    from kmb.ingest.storage import SENTINEL_NAME, StorageGuard

    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    archive_id = "collector-integration"
    (archive_root / SENTINEL_NAME).write_text(archive_id + "\n", encoding="utf-8")
    token = "integration-token" * 3
    config = IngestConfig(
        token=token,
        archive_root=archive_root,
        db_path=tmp_path / "state" / "ingest.sqlite",
        archive_id=archive_id,
        max_batch_bytes=1024 * 1024,
        max_uncompressed_bytes=4 * 1024 * 1024,
        min_free_bytes=0,
    )
    guard = StorageGuard(
        archive_root,
        archive_id,
        min_free_bytes=0,
        disk_usage=lambda _path: SimpleNamespace(free=100 * 1024**3),
    )
    archive = Archive(
        archive_root,
        ReceiptStore(config.db_path),
        guard,
        max_uncompressed_bytes=config.max_uncompressed_bytes,
    )
    app = create_app(config, archive=archive)
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://ingest",
    )
    spool = make_spool(tmp_path / "spool")
    uploader = BatchUploader(spool, "http://ingest", token, client=client)
    try:
        assert await uploader.upload_one(spool.pending_batches()[0]) == "uploaded"
        assert spool.pending_batches() == []
        assert archive.receipts.counts() == {"committed": 1}
    finally:
        await client.aclose()
        archive.receipts.close()
