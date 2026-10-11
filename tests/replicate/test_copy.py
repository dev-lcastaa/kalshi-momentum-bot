from __future__ import annotations

import pytest

from kmb.replicate.copy import Config, run
from tests.normalizer.test_windows import archive_fixture


def test_copy_verify_cache_no_overwrite(tmp_path):
    source = tmp_path / "storage1"
    source.mkdir()
    cfg, _ = archive_fixture(source)
    cfg.archive_root.rename(source / "kalshi-archive")
    derived = source / "kalshi-derived"
    derived.mkdir()
    (derived / "test.parquet").write_bytes(b"PAR1")
    (derived / "partial.tmp").write_bytes(b"unfinished")
    (derived / ".hidden").write_bytes(b"ignore")
    destination = tmp_path / "storage2"
    destination.mkdir()
    rcfg = Config(source, destination, cfg.receipts_path, tmp_path / "replicate.sqlite")
    checked = []

    def guard(path, uuid):
        checked.append((path, uuid))

    report = run(rcfg, mount_check=guard)
    assert report["copied"] == 5 and not report["errors"]
    assert checked == [(source, "C174-8D73"), (destination, "2E5C-6B4D")]
    assert not (destination / "kalshi-derived" / "partial.tmp").exists()
    assert run(rcfg, mount_check=guard)["cached"] == 5
    target = destination / "kalshi-derived" / "test.parquet"
    target.write_bytes(b"DIFFERENT")
    assert run(rcfg, mount_check=guard)["conflicts"] == ["kalshi-derived/test.parquet"]
    assert target.read_bytes() == b"DIFFERENT"
    target.write_bytes(b"PAR1")
    assert run(rcfg, mount_check=guard)["verified"] == 1


def test_mount_failure_no_state_or_writes(tmp_path):
    cfg = Config(tmp_path / "missing", tmp_path / "target", state_path=tmp_path / "state.sqlite")

    def fail(_path, _uuid):
        raise RuntimeError("wrong UUID")

    with pytest.raises(RuntimeError, match="wrong UUID"):
        run(cfg, mount_check=fail)
    assert not cfg.state_path.exists()
    assert not cfg.destination_mount.exists()


def test_unacknowledged_archive_is_skipped_and_completion_marker_withheld(tmp_path):
    source = tmp_path / "storage1"
    source.mkdir()
    cfg, _ = archive_fixture(source)
    cfg.archive_root.rename(source / "kalshi-archive")
    from kmb.ingest.receipts import ReceiptStore

    receipts = ReceiptStore(cfg.receipts_path)
    try:
        receipts._conn.execute("UPDATE batches SET status='pending' WHERE stream='control'")
    finally:
        receipts.close()
    derived = source / "kalshi-derived"
    derived.mkdir()
    (derived / "test.parquet").write_bytes(b"original")
    (derived / "test.window.json").write_text("{}")
    destination = tmp_path / "storage2"
    (destination / "kalshi-derived").mkdir(parents=True)
    (destination / "kalshi-derived" / "test.parquet").write_bytes(b"conflict")
    cfg = Config(source, destination, cfg.receipts_path, tmp_path / "replicate.sqlite")
    report = run(cfg, mount_check=lambda *_args: None)
    assert report["conflicts"] == ["kalshi-derived/test.parquet"]
    assert report["errors"][0]["path"] == "kalshi-derived/test.window.json"
    assert not (destination / "kalshi-derived" / "test.window.json").exists()
    assert not (destination / "kalshi-archive" / "batches" / "control.jsonl.gz").exists()
