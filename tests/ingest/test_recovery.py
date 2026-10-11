from __future__ import annotations

from kmb.ingest.archive import TMP_PREFIX, rel_path_for
from kmb.ingest.receipts import COMMITTED, CORRUPT, FAILED
from tests.ingest.conftest import make_batch


def test_crash_after_rename_before_commit_is_recovered(archive, archive_root):
    meta, body = make_batch()
    rel = rel_path_for(meta)
    archive.receipts.upsert_pending(meta, rel, "2026-10-10T15:00:11Z", 100)
    final = archive_root / rel
    final.parent.mkdir(parents=True)
    final.write_bytes(body)
    stray = final.parent / f"{TMP_PREFIX}{final.name}-deadbeef"
    stray.write_bytes(b"partial")

    summary = archive.reconcile()
    assert summary["pending_committed"] == 1
    assert summary["tmp_removed"] == 1
    assert archive.receipts.get(meta.batch_id)["status"] == COMMITTED
    assert not stray.exists()


def test_crash_before_rename_marks_failed_and_retry_works(archive, archive_root):
    meta, body = make_batch()
    rel = rel_path_for(meta)
    archive.receipts.upsert_pending(meta, rel, "2026-10-10T15:00:11Z", 100)
    final = archive_root / rel
    final.parent.mkdir(parents=True)
    (final.parent / f"{TMP_PREFIX}{final.name}-cafe").write_bytes(body[:10])

    summary = archive.reconcile()
    assert summary["pending_failed"] == 1
    assert archive.receipts.get(meta.batch_id)["status"] == FAILED

    result = archive.put(meta, body)
    assert result.created
    assert final.read_bytes() == body


def test_partial_final_file_is_quarantined_not_deleted(archive, archive_root):
    meta, body = make_batch()
    rel = rel_path_for(meta)
    archive.receipts.upsert_pending(meta, rel, "2026-10-10T15:00:11Z", 100)
    final = archive_root / rel
    final.parent.mkdir(parents=True)
    final.write_bytes(body[:5])

    archive.reconcile()
    quarantined = list((archive_root / "quarantine").rglob("*"))
    assert any(p.is_file() and p.read_bytes() == body[:5] for p in quarantined)
    assert archive.put(meta, body).created
    assert final.read_bytes() == body


def test_reconcile_flags_corrupt_recent_file_and_reupload_repairs(archive, archive_root):
    meta, body = make_batch()
    assert archive.put(meta, body).created
    final = archive_root / rel_path_for(meta)
    final.write_bytes(b"bitrot")

    summary = archive.reconcile()
    assert summary["recent_corrupt"] == 1
    assert archive.receipts.get(meta.batch_id)["status"] == CORRUPT

    assert archive.put(meta, body).created
    assert final.read_bytes() == body
    assert archive.receipts.get(meta.batch_id)["status"] == COMMITTED


def test_verify_all(archive, archive_root):
    for i in range(3):
        archive.put(*make_batch(first_stream_seq=i * 3, first_local=i * 10))
    (archive_root / rel_path_for(make_batch(first_stream_seq=3, first_local=10)[0])).write_bytes(b"x")
    result = archive.verify_all()
    assert result["verified"] == 2
    assert result["corrupt"] == ["cap-1:kalshi_ws:3"]
