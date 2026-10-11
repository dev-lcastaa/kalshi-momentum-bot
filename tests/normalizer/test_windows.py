from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy.orm import Session

from kmb.db import init_db, make_engine
from kmb.db.models import CaptureWindow, MarketResult
from kmb.envelope import BatchMeta
from kmb.envelope.event import Sequencer, make_envelope
from kmb.envelope.windows import PARQUET_COLUMNS
from kmb.ingest.receipts import ReceiptStore
from kmb.ingest.storage import SENTINEL_NAME
from kmb.normalizer.config import Config
from kmb.normalizer.index import FileIndex
from kmb.normalizer.windows import run_once

TICKER = "KXBTC15M-26JAN011200-00"
OPEN = 1767285900000
CLOSE = OPEN + 900000


def archive_fixture(root: Path, *, gap=False, recovered=True):
    archive = root / "archive"
    archive.mkdir()
    (archive / SENTINEL_NAME).write_text("test", encoding="utf-8")
    receipts = ReceiptStore(root / "receipts.sqlite")
    sequencer = Sequencer()
    envelopes = []

    def emit(wall, raw=None, *, stream="kalshi_ws", kind="frame", data=None, ticker=None, msg_type=None):
        event = make_envelope(
            sequencer,
            capture_id="cap",
            stream=stream,
            kind=kind,
            source="collector" if kind in {"transport", "control"} else "kalshi",
            raw=raw,
            data=data,
            market_ticker=ticker,
            msg_type=msg_type,
            recv_wall_ms=wall,
        )
        envelopes.append(event)

    market = {
        "market": {
            "ticker": TICKER,
            "open_time": "2026-01-01T16:45:00Z",
            "close_time": "2026-01-01T17:00:00Z",
            "floor_strike": "97000.00",
            "result": "yes",
        }
    }
    emit(OPEN - 60000, json.dumps(market), stream="kalshi_rest", kind="rest", msg_type="rest.market")
    emit(
        OPEN, json.dumps({"type": "orderbook_snapshot", "sid": 1, "seq": 1, "msg": {"market_ticker": TICKER}})
    )
    emit(OPEN + 100, json.dumps({"type": "cfbenchmarks_value", "msg": {"index_id": "BRTI"}}))
    emit(OPEN + 200, json.dumps({"type": "match", "product_id": "BTC-USD"}), stream="coinbase_ws")
    emit(OPEN + 300, json.dumps({"type": "ticker", "msg": {"market_ticker": "OTHER"}}))
    emit(OPEN + 400, json.dumps({"type": "ok"}))
    emit(OPEN + 500, stream="control", kind="control", data={"event": "clock"})
    if gap:
        emit(OPEN + 600, kind="transport", data={"event": "gap"})
        emit(OPEN + 700, kind="transport", data={"event": "disconnected"})
        if recovered:
            emit(
                OPEN + 800,
                json.dumps(
                    {"type": "orderbook_snapshot", "sid": 2, "seq": 1, "msg": {"market_ticker": TICKER}}
                ),
            )
    emit(CLOSE + 180000, json.dumps({"type": "trade", "msg": {"market_ticker": TICKER}}))
    emit(CLOSE + 180001, json.dumps({"type": "trade", "msg": {"market_ticker": TICKER}}))
    for stream in sorted({e["stream"] for e in envelopes}):
        group = [e for e in envelopes if e["stream"] == stream]
        body = gzip.compress(b"".join(json.dumps(e).encode() + b"\n" for e in group), mtime=0)
        meta = BatchMeta(
            batch_id=f"cap:{stream}:0",
            capture_id="cap",
            stream=stream,
            first_stream_seq=0,
            last_stream_seq=len(group) - 1,
            first_local_seq=group[0]["local_event_seq"],
            last_local_seq=group[-1]["local_event_seq"],
            event_count=len(group),
            compressed_bytes=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
            first_recv_utc=group[0]["received_at_utc"],
            last_recv_utc=group[-1]["received_at_utc"],
            created_at_utc=group[-1]["received_at_utc"],
        )
        path = f"batches/{stream}.jsonl.gz"
        (archive / "batches").mkdir(exist_ok=True)
        (archive / path).write_bytes(body)
        receipts.upsert_pending(meta, path, meta.created_at_utc, 100)
        receipts.mark_committed(meta.batch_id, meta.created_at_utc)
    receipts.close()
    cfg = Config(archive, root / "derived", root / "receipts.sqlite", root / "state.sqlite", 0, "test")
    return cfg, envelopes


def test_window_contract_idempotency_and_db(tmp_path):
    cfg, envelopes = archive_fixture(tmp_path)
    engine = make_engine(f"sqlite:///{tmp_path / 'app.sqlite'}")
    init_db(engine)
    assert run_once(cfg, now_ms=CLOSE + 180000, engine=engine)["built"] == []
    assert run_once(cfg, now_ms=CLOSE + 180001, engine=engine)["built"] == [TICKER]
    sidecar = next(cfg.derived_root.rglob("*.window.json"))
    meta = json.loads(sidecar.read_text())
    table = pq.read_table(cfg.derived_root / meta["parquet"])
    assert table.column_names == list(PARQUET_COLUMNS)
    rows = table.to_pylist()
    assert [r["ord"] for r in rows] == list(range(len(rows)))
    assert [r["local_event_seq"] for r in rows] == sorted(r["local_event_seq"] for r in rows)
    assert min(r["recv_wall_ms"] for r in rows) == OPEN - 60000
    assert max(r["recv_wall_ms"] for r in rows) == CLOSE + 180000
    assert not any(r["market_ticker"] == "OTHER" for r in rows)
    assert {"BRTI", "BTC-USD", None} <= {r["market_ticker"] for r in rows}
    assert meta["quality"] == "ok"
    assert meta["event_count"] == len(envelopes) - 2
    assert run_once(cfg, now_ms=CLOSE + 999999, engine=engine)["built"] == []
    with Session(engine) as session:
        assert session.scalar(select(CaptureWindow)).quality == "ok"
        assert session.get(MarketResult, TICKER).result == "yes"
    engine.dispose()


def test_quality_recovery_and_missing_book(tmp_path):
    cfg, _ = archive_fixture(tmp_path, gap=True)
    run_once(cfg, now_ms=CLOSE + 180001)
    sidecar = next(cfg.derived_root.rglob("*.window.json"))
    meta = json.loads(sidecar.read_text())
    assert meta["quality"] == "degraded"
    assert meta["gap_count"] == 1 and meta["reconnect_count"] == 1
    receipts = ReceiptStore(cfg.receipts_path)
    try:
        receipts._conn.execute("UPDATE batches SET status='pending' WHERE stream='kalshi_ws'")
    finally:
        receipts.close()
    run_once(cfg, rebuild=TICKER)
    assert json.loads(sidecar.read_text())["quality"] == "bad"


def test_index_only_acknowledged_verified_and_sentinel(tmp_path):
    cfg, _ = archive_fixture(tmp_path)
    (cfg.archive_root / "batches" / "kalshi_ws.jsonl.gz").write_bytes(b"corrupt")
    index = FileIndex(cfg.state_path, cfg.archive_root, cfg.receipts_path)
    assert index.update()["invalid"] == 1
    assert any(not row["verified"] for row in index.files(OPEN, CLOSE))
    index.conn.commit()
    index.close()
    import pytest

    with pytest.raises(RuntimeError):
        run_once(replace(cfg, archive_id="wrong"))


def test_lifecycle_discovery():
    from kmb.normalizer.windows import market_metadata

    created = {
        "msg_type": "market_lifecycle_v2",
        "raw": json.dumps(
            {
                "msg": {
                    "market_ticker": TICKER,
                    "event_type": "created",
                    "open_ts": OPEN // 1000,
                    "close_ts": CLOSE // 1000,
                    "additional_metadata": {"floor_strike": "97000"},
                }
            }
        ),
    }
    assert market_metadata(created)["open_ms"] == OPEN
    assert market_metadata(created)["strike"] == "97000"


def test_stream_holes(tmp_path):
    from kmb.normalizer.windows import stream_holes

    cfg, _ = archive_fixture(tmp_path)
    index = FileIndex(cfg.state_path, cfg.archive_root, cfg.receipts_path)
    index.update()
    index.conn.execute("UPDATE files SET first_stream_seq=3 WHERE stream='kalshi_ws'")
    assert stream_holes(index, {"cap"}, OPEN, CLOSE)
    index.close()


def test_replay_capture_order_and_unrecovered_gap(tmp_path):
    cfg, _ = archive_fixture(tmp_path, gap=True, recovered=False)
    run_once(cfg, now_ms=CLOSE + 180001)
    meta = json.loads(next(cfg.derived_root.rglob("*.window.json")).read_text())
    assert meta["quality"] == "bad"


def test_pending_trailing_batch_is_bad(tmp_path):
    cfg, _ = archive_fixture(tmp_path)
    receipts = ReceiptStore(cfg.receipts_path)
    try:
        receipts._conn.execute("UPDATE batches SET status='pending' WHERE stream='control'")
    finally:
        receipts.close()
    run_once(cfg, now_ms=CLOSE + 180001)
    meta = json.loads(next(cfg.derived_root.rglob("*.window.json")).read_text())
    assert meta["quality"] == "bad"


def test_capture_order_uses_first_receive_time_across_archive(tmp_path):
    cfg, _ = archive_fixture(tmp_path)
    seq = Sequencer()
    group = [
        make_envelope(
            seq,
            capture_id="earlier",
            stream="control",
            kind="control",
            source="collector",
            data={"event": "clock"},
            recv_wall_ms=wall,
        )
        for wall in (OPEN - 120000, OPEN + 1000)
    ]
    body = gzip.compress(b"".join(json.dumps(e).encode() + b"\n" for e in group), mtime=0)
    meta = BatchMeta(
        batch_id="earlier:control:0",
        capture_id="earlier",
        stream="control",
        first_stream_seq=0,
        last_stream_seq=1,
        first_local_seq=0,
        last_local_seq=1,
        event_count=2,
        compressed_bytes=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
        first_recv_utc=group[0]["received_at_utc"],
        last_recv_utc=group[-1]["received_at_utc"],
        created_at_utc=group[-1]["received_at_utc"],
    )
    relative = "batches/earlier.jsonl.gz"
    (cfg.archive_root / relative).write_bytes(body)
    receipts = ReceiptStore(cfg.receipts_path)
    try:
        receipts.upsert_pending(meta, relative, meta.created_at_utc, 100)
        receipts.mark_committed(meta.batch_id, meta.created_at_utc)
    finally:
        receipts.close()
    run_once(cfg, now_ms=CLOSE + 180001)
    sidecar = json.loads(next(cfg.derived_root.rglob("*.window.json")).read_text())
    rows = pq.read_table(cfg.derived_root / sidecar["parquet"]).to_pylist()
    assert rows[0]["capture_id"] == "earlier"
    assert rows[0]["local_event_seq"] == 1
    assert rows[1]["capture_id"] == "cap"
    assert sidecar["captures"] == ["earlier", "cap"]
