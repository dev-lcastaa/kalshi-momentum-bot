from __future__ import annotations

import json

from fastapi.testclient import TestClient

from kmb.dataapi.app import create_app
from kmb.normalizer.windows import run_once
from tests.normalizer.test_windows import CLOSE, TICKER, archive_fixture


def test_api_auth_catalog_ranges_traversal_and_refresh(tmp_path):
    cfg, _ = archive_fixture(tmp_path)
    run_once(cfg, now_ms=CLOSE + 180001)
    client = TestClient(create_app(cfg.derived_root, "test-token"))
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
