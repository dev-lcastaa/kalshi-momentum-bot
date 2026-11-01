from __future__ import annotations

import json
import time
from pathlib import Path

from fastapi.testclient import TestClient

from kmb.dataapi.app import Catalog, create_app
from kmb.normalizer.windows import run_once
from tests.normalizer.test_windows import CLOSE, TICKER, archive_fixture


def test_api_auth_catalog_ranges_traversal_and_refresh(tmp_path):
    cfg, _ = archive_fixture(tmp_path)
    run_once(cfg, now_ms=CLOSE + 180001)
    client = TestClient(create_app(cfg.derived_root, "test-token", catalog_ttl_s=0))
    auth = {"Authorization": "Bearer test-token"}
    assert client.get("/health").json() == {"ok": True, "windows": 1}
    assert client.get("/v1/windows").status_code == 401
    assert client.get("/v1/windows", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert len(client.get("/v1/windows?series=KXBTC15M", headers=auth).json()) == 1
    assert client.get("/v1/windows?series=KXETH15M", headers=auth).json() == []
    assert client.get("/v1/windows?from=2027-01-01", headers=auth).json() == []
    assert client.get("/v1/windows?from=bad", headers=auth).status_code == 422
    meta = client.get(f"/v1/windows/{TICKER}", headers=auth).json()
    url = f"/v1/windows/{TICKER}/parquet"
    response = client.get(url, headers=auth)
    assert response.content[:4] == b"PAR1"
    assert response.headers["etag"] == f'"{meta["sha256"]}"'
    ranged = client.get(url, headers={**auth, "Range": "bytes=0-3"})
    assert ranged.status_code == 206 and ranged.content == b"PAR1"
    assert client.get(url, headers={**auth, "Range": "bytes=999999999-"}).status_code == 416
    assert client.get(url, headers={**auth, "If-None-Match": response.headers["etag"]}).status_code == 304
    assert client.get("/v1/windows/BAD%5Cpath", headers=auth).status_code == 400
    assert client.get("/v1/windows/UNKNOWN", headers=auth).status_code == 404
    sidecar = next(cfg.derived_root.rglob("*.window.json"))
    meta["quality"] = "degraded"
    sidecar.write_text(json.dumps(meta))
    assert client.get(f"/v1/windows/{TICKER}", headers=auth).json()["quality"] == "degraded"
    meta["parquet"] = "../private"
    sidecar.write_text(json.dumps(meta))
    assert client.get("/v1/windows", headers=auth).json() == []
    sidecar.unlink()
    assert client.get("/health").json()["windows"] == 0


def test_warm_catalog_scan_skips_revalidation_but_still_detects_changes(tmp_path, monkeypatch):
    cfg, _ = archive_fixture(tmp_path)
    run_once(cfg, now_ms=CLOSE + 180001)
    catalog = Catalog(cfg.derived_root)
    assert len(catalog.windows()) == 1

    def fail(*args, **kwargs):
        raise AssertionError("warm scan revalidated an unchanged window")

    monkeypatch.setattr(Path, "resolve", fail)
    monkeypatch.setattr(json, "loads", fail)
    assert len(catalog.windows()) == 1
    assert catalog.get(TICKER)["window_id"] == TICKER
    monkeypatch.undo()

    parquet = next(cfg.derived_root.rglob("*.parquet"))
    parquet.write_bytes(parquet.read_bytes()[:-1])
    assert catalog.windows() == []
    parquet.unlink()
    assert catalog.windows() == []


def test_cached_catalog_serves_snapshot_and_refreshes_in_background(tmp_path, monkeypatch):
    cfg, _ = archive_fixture(tmp_path)
    run_once(cfg, now_ms=CLOSE + 180001)
    catalog = Catalog(cfg.derived_root, ttl_s=60)

    def fail():
        raise AssertionError("scan ran before the snapshot was warm")

    monkeypatch.setattr(catalog, "_scan", fail)
    assert catalog.count() == 0
    monkeypatch.undo()

    assert len(catalog.windows()) == 1
    assert catalog.get(TICKER)["window_id"] == TICKER

    def fail(*args, **kwargs):
        raise AssertionError("snapshot lookup touched the filesystem")

    monkeypatch.setattr(Path, "glob", fail)
    monkeypatch.setattr(catalog, "_load", fail)
    assert catalog.get(TICKER)["window_id"] == TICKER
    monkeypatch.undo()

    next(cfg.derived_root.rglob("*.parquet")).unlink()
    assert len(catalog.windows()) == 1 and catalog.count() == 1
    assert catalog.refresh() == []

    stale = Catalog(cfg.derived_root, ttl_s=0.001)
    assert stale.windows() == []
    stale._snapshot = [{"window_id": "STALE"}]
    time.sleep(0.01)
    assert stale.windows() == [{"window_id": "STALE"}]
    deadline = time.monotonic() + 5
    while stale.windows() != [] and time.monotonic() < deadline:
        time.sleep(0.01)
    assert stale.windows() == []


def test_health_does_not_wait_for_catalog_scan(tmp_path):
    cfg, _ = archive_fixture(tmp_path)
    run_once(cfg, now_ms=CLOSE + 180001)
    client = TestClient(create_app(cfg.derived_root, "test-token", catalog_ttl_s=60))
    deadline = time.monotonic() + 5
    while client.get("/health").json()["windows"] != 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert client.get("/health").json() == {"ok": True, "windows": 1}


def test_single_window_lookup_does_not_rescan_catalog(tmp_path, monkeypatch):
    cfg, _ = archive_fixture(tmp_path)
    run_once(cfg, now_ms=CLOSE + 180001)
    app = create_app(cfg.derived_root, "test-token", catalog_ttl_s=0)
    client = TestClient(app)
    auth = {"Authorization": "Bearer test-token"}

    def fail():
        raise AssertionError("catalog was fully rescanned")

    monkeypatch.setattr(app.state.catalog, "windows", fail)
    assert client.get(f"/v1/windows/{TICKER}", headers=auth).json()["window_id"] == TICKER
    assert client.get("/v1/windows/UNKNOWN", headers=auth).status_code == 404
    assert client.get(f"/v1/windows/{TICKER}/parquet", headers=auth).content[:4] == b"PAR1"
