from __future__ import annotations

import gzip
import json
import threading
from pathlib import Path

from kmb.ingest.archive import rel_path_for
from kmb.ingest.storage import SENTINEL_NAME
from tests.ingest.conftest import TOKEN, encode, make_batch, make_events, put


def _files(root: Path) -> list[Path]:
    return sorted(p for p in (root / "raw").rglob("*") if p.is_file()) if (root / "raw").exists() else []


def test_accepts_valid_batch_and_writes_readable_jsonl(client, archive_root):
    meta, body = make_batch()
    resp = put(client, meta, body)
    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "created"
    assert resp.json()["receipt"]["status"] == "committed"

    path = archive_root / rel_path_for(meta)
    assert path.as_posix().endswith(
        "raw/date=2026-10-10/capture_id=cap-1/stream=kalshi_ws/batch-000000000000.jsonl.gz"
    )
    lines = gzip.decompress(path.read_bytes()).decode().splitlines()
    assert [json.loads(line)["stream_seq"] for line in lines] == [0, 1, 2]
    assert _files(archive_root) == [path]


def test_duplicate_is_idempotent(client, archive_root):
    meta, body = make_batch()
    assert put(client, meta, body).status_code == 201
    resp = put(client, meta, body)
    assert resp.status_code == 200
    assert resp.json()["status"] == "duplicate"
    assert len(_files(archive_root)) == 1


def test_reused_id_with_different_content_is_rejected(client, archive_root):
    meta, body = make_batch()
    assert put(client, meta, body).status_code == 201
    events = make_events("cap-1", "kalshi_ws", 0, 3, 100)
    meta2, body2 = make_batch(events=events)
    resp = put(client, meta2, body2)
    assert resp.status_code == 409
    assert resp.json() == {"error": "batch_conflict", "message": resp.json()["message"], "retryable": False}
    assert gzip.decompress(_files(archive_root)[0].read_bytes()) == gzip.decompress(body)


def test_rejects_unauthorized(client):
    meta, body = make_batch()
    assert put(client, meta, body, token=None).status_code == 401
    assert put(client, meta, body, token="wrong" * 10).status_code == 401


def test_rejects_bad_checksum(client, archive_root):
    meta, body = make_batch()
    meta_bad, _ = make_batch(sha256="0" * 64)
    resp = put(client, meta_bad, body)
    assert resp.status_code == 400
    assert resp.json()["error"] == "checksum_mismatch"
    assert resp.json()["retryable"] is False
    assert _files(archive_root) == []


def test_rejects_malformed_gzip(client):
    meta, body = make_batch(body=b"not gzip at all")
    resp = put(client, meta, body)
    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_gzip"


def test_rejects_malformed_jsonl(client):
    body = gzip.compress(b'{"schema_version": 1}\nnot json\n{}\n')
    meta, _ = make_batch(body=body)
    resp = put(client, meta, body)
    assert resp.status_code == 400
    assert resp.json()["error"] in {"bad_envelope", "bad_jsonl"}


def test_rejects_oversized_upload(client):
    events = make_events("cap-1", "kalshi_ws", 0, 50, 0)
    for e in events:
        e["raw"] = __import__("os").urandom(2048).hex()
    meta, body = make_batch(events=events)
    assert len(body) > 64 * 1024
    resp = put(client, meta, body)
    assert resp.status_code == 413
    assert resp.json()["error"] == "batch_too_large"


def test_rejects_zip_bomb(client):
    events = make_events("cap-1", "kalshi_ws", 0, 3, 0)
    events[0]["raw"] = "A" * (2 * 1024 * 1024)
    meta, body = make_batch(events=events)
    resp = put(client, meta, body)
    assert resp.status_code == 413
    assert resp.json()["error"] == "uncompressed_too_large"


def test_rejects_bad_metadata(client):
    meta, body = make_batch()
    resp = client.put(
        f"/v1/batches/{meta.batch_id}",
        content=body,
        headers={"Authorization": f"Bearer {TOKEN}", "X-Batch-Meta": "{not json"},
    )
    assert resp.status_code == 400
    assert put(client, meta, body, path_id="cap-1:kalshi_ws:99").status_code == 400
    resp = client.put(
        f"/v1/batches/{meta.batch_id}", content=body, headers={"Authorization": f"Bearer {TOKEN}"}
    )
    assert resp.json()["error"] == "missing_metadata"


def test_rejects_path_traversal_ids(client):
    meta, body = make_batch()
    raw = meta.to_dict() | {"capture_id": "..", "batch_id": "..:kalshi_ws:0"}
    resp = client.put(
        "/v1/batches/..:kalshi_ws:0",
        content=body,
        headers={"Authorization": f"Bearer {TOKEN}", "X-Batch-Meta": json.dumps(raw)},
    )
    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_metadata"


def test_rejects_non_contiguous_stream_seq(client):
    events = make_events("cap-1", "kalshi_ws", 0, 3, 0)
    events[2]["stream_seq"] = 5
    meta, body = make_batch(events=events, last_stream_seq=2)
    resp = put(client, meta, body)
    assert resp.status_code == 400
    assert resp.json()["error"] == "stream_seq_not_contiguous"


def test_rejects_envelope_from_other_stream(client):
    events = make_events("cap-1", "kalshi_ws", 0, 3, 0)
    events[1]["stream"] = "coinbase_ws"
    meta, body = make_batch(events=events)
    resp = put(client, meta, body)
    assert resp.json()["error"] == "envelope_mismatch"


def test_missing_sentinel_fails_closed(client, archive_root):
    (archive_root / SENTINEL_NAME).unlink()
    meta, body = make_batch()
    resp = put(client, meta, body)
    assert resp.status_code == 503
    assert resp.json()["error"] == "storage_not_ready"
    assert resp.json()["retryable"] is True
    assert _files(archive_root) == []
    ready = client.get("/ready")
    assert ready.status_code == 503
    assert ready.json()["checks"]["sentinel"] is False


def test_wrong_sentinel_fails_closed(client, archive_root):
    (archive_root / SENTINEL_NAME).write_text("some-other-drive", encoding="utf-8")
    meta, body = make_batch()
    assert put(client, meta, body).status_code == 503


def test_low_disk_space_returns_507(client, free_bytes, archive_root):
    free_bytes["free"] = 10
    meta, body = make_batch()
    resp = put(client, meta, body)
    assert resp.status_code == 507
    assert resp.json()["retryable"] is True
    assert _files(archive_root) == []


def test_write_failure_is_not_acknowledged_and_retry_succeeds(client, archive_root, monkeypatch):
    import builtins

    real_open = builtins.open

    def failing_open(path, mode="r", *args, **kwargs):
        if "x" in mode:
            raise OSError(5, "Input/output error")
        return real_open(path, mode, *args, **kwargs)

    meta, body = make_batch()
    monkeypatch.setattr(builtins, "open", failing_open)
    resp = put(client, meta, body)
    monkeypatch.setattr(builtins, "open", real_open)
    assert resp.status_code == 500
    assert resp.json() == {"error": "write_failed", "message": resp.json()["message"], "retryable": True}
    receipt = client.get(f"/v1/batches/{meta.batch_id}", headers={"Authorization": f"Bearer {TOKEN}"}).json()
    assert receipt["receipt"]["status"] == "failed"

    resp = put(client, meta, body)
    assert resp.status_code == 201
    assert resp.json()["receipt"]["attempts"] == 2


def test_disk_full_error_maps_to_507(client, monkeypatch):
    import builtins
    import errno

    real_open = builtins.open

    def full_open(path, mode="r", *args, **kwargs):
        if "x" in mode:
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_open(path, mode, *args, **kwargs)

    meta, body = make_batch()
    monkeypatch.setattr(builtins, "open", full_open)
    resp = put(client, meta, body)
    monkeypatch.setattr(builtins, "open", real_open)
    assert resp.status_code == 507


def test_concurrent_uploads_do_not_overwrite(client, archive_root):
    batches = [make_batch(first_stream_seq=i * 3, first_local=i * 10) for i in range(12)]
    results: list[int] = []

    def worker(meta, body):
        results.append(put(client, meta, body).status_code)

    threads = [threading.Thread(target=worker, args=b) for b in batches + batches[:4]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results).count(201) == 12
    assert len(_files(archive_root)) == 12


def test_health_and_ready_do_not_leak_secrets(client):
    for path in ("/health", "/ready"):
        resp = client.get(path)
        assert resp.status_code == 200
        assert TOKEN not in resp.text
    body = client.get("/ready").json()
    assert body["ready"] is True
    assert body["checks"]["writable"] is True
    assert body["free_bytes"] > 0


def test_coverage_reports_holes_and_completeness(client):
    # stream A: seq 0-2 (local 0,2,4) and 3-5 (local 6,8,10); stream B: local 1,3,5,7,9,11
    a1 = make_batch(stream="kalshi_ws", first_stream_seq=0, n=3, first_local=0)
    a2 = make_batch(stream="kalshi_ws", first_stream_seq=3, n=3, first_local=6)
    b1 = make_batch(stream="coinbase_ws", first_stream_seq=0, n=6, first_local=1)
    for meta, body in (a1, b1):
        assert put(client, meta, body).status_code == 201
    headers = {"Authorization": f"Bearer {TOKEN}"}

    cov = client.get("/v1/captures/cap-1/coverage", headers=headers).json()
    assert cov["streams"]["kalshi_ws"]["next_stream_seq"] == 3
    assert cov["missing_local_seqs"] == 3  # local 6, 8, 10 still missing
    assert cov["complete_so_far"] is False

    assert put(client, *a2).status_code == 201
    cov = client.get("/v1/captures/cap-1/coverage", headers=headers).json()
    assert cov["total_events"] == 12
    assert cov["missing_local_seqs"] == 0
    assert cov["complete_so_far"] is True


def test_coverage_detects_missing_middle_batch(client):
    put(client, *make_batch(first_stream_seq=0, n=3, first_local=0, events=None))
    events = make_events("cap-1", "kalshi_ws", 6, 3, 6, local_step=1)
    put(client, *make_batch(first_stream_seq=6, events=events))
    cov = client.get("/v1/captures/cap-1/coverage", headers={"Authorization": f"Bearer {TOKEN}"}).json()
    assert cov["streams"]["kalshi_ws"]["holes"] == [[3, 5]]


def test_encode_helper_roundtrip():
    events = make_events("c", "s", 0, 2, 0)
    assert len(gzip.decompress(encode(events)).splitlines()) == 2
