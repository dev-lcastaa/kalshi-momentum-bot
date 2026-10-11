from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kmb.envelope import BatchMeta, make_batch_id
from kmb.ingest.app import create_app
from kmb.ingest.archive import Archive
from kmb.ingest.config import IngestConfig
from kmb.ingest.receipts import ReceiptStore
from kmb.ingest.storage import SENTINEL_NAME, StorageGuard

TOKEN = "t" * 40
ARCHIVE_ID = "kalshi-archive-test"


def make_events(
    capture: str, stream: str, first_stream_seq: int, n: int, first_local: int, local_step: int = 2
) -> list[dict]:
    return [
        {
            "schema_version": 1,
            "capture_id": capture,
            "stream": stream,
            "stream_seq": first_stream_seq + i,
            "local_event_seq": first_local + i * local_step,
            "kind": "frame",
            "received_at_utc": f"2026-10-10T15:00:{i:02d}.000000Z",
            "raw": json.dumps({"type": "orderbook_delta", "seq": i}),
        }
        for i in range(n)
    ]


def encode(events: list[dict]) -> bytes:
    data = b"".join(json.dumps(e, separators=(",", ":")).encode() + b"\n" for e in events)
    return gzip.compress(data, mtime=0)


def make_batch(
    capture: str = "cap-1",
    stream: str = "kalshi_ws",
    first_stream_seq: int = 0,
    n: int = 3,
    first_local: int = 0,
    events: list[dict] | None = None,
    body: bytes | None = None,
    **overrides,
) -> tuple[BatchMeta, bytes]:
    events = events if events is not None else make_events(capture, stream, first_stream_seq, n, first_local)
    body = body if body is not None else encode(events)
    fields = {
        "batch_id": make_batch_id(capture, stream, first_stream_seq),
        "capture_id": capture,
        "stream": stream,
        "first_stream_seq": first_stream_seq,
        "last_stream_seq": first_stream_seq + len(events) - 1,
        "first_local_seq": events[0]["local_event_seq"],
        "last_local_seq": events[-1]["local_event_seq"],
        "event_count": len(events),
        "compressed_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "first_recv_utc": events[0]["received_at_utc"],
        "last_recv_utc": events[-1]["received_at_utc"],
        "created_at_utc": "2026-10-10T15:00:10.000000Z",
    }
    fields.update(overrides)
    return BatchMeta(**fields), body


class FakeUsage:
    def __init__(self, free: int) -> None:
        self.free = free


@pytest.fixture
def archive_root(tmp_path: Path) -> Path:
    root = tmp_path / "archive"
    root.mkdir()
    (root / SENTINEL_NAME).write_text(ARCHIVE_ID + "\n", encoding="utf-8")
    return root


@pytest.fixture
def free_bytes() -> dict:
    return {"free": 100 * 1024**3}


@pytest.fixture
def cfg(tmp_path: Path, archive_root: Path) -> IngestConfig:
    return IngestConfig(
        token=TOKEN,
        archive_root=archive_root,
        db_path=tmp_path / "state" / "ingest.sqlite",
        archive_id=ARCHIVE_ID,
        max_batch_bytes=64 * 1024,
        max_uncompressed_bytes=1024 * 1024,
        min_free_bytes=1024**3,
    )


@pytest.fixture
def make_archive(cfg: IngestConfig, free_bytes: dict) -> Callable[[], Archive]:
    def factory() -> Archive:
        guard = StorageGuard(
            cfg.archive_root,
            cfg.archive_id,
            min_free_bytes=cfg.min_free_bytes,
            disk_usage=lambda _p: FakeUsage(free_bytes["free"]),
        )
        return Archive(
            cfg.archive_root,
            ReceiptStore(cfg.db_path),
            guard,
            max_uncompressed_bytes=cfg.max_uncompressed_bytes,
        )

    return factory


@pytest.fixture
def archive(make_archive) -> Archive:
    a = make_archive()
    yield a
    a.receipts.close()


@pytest.fixture
def client(cfg: IngestConfig, make_archive):
    app = create_app(cfg, archive=make_archive())
    with TestClient(app) as c:
        yield c


def put(
    client: TestClient, meta: BatchMeta, body: bytes, token: str | None = TOKEN, path_id: str | None = None
):
    headers = {"X-Batch-Meta": meta.to_json(), "Content-Type": "application/gzip"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return client.put(f"/v1/batches/{path_id or meta.batch_id}", content=body, headers=headers)
