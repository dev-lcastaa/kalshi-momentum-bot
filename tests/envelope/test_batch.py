from __future__ import annotations

import pytest

from kmb.envelope import BatchMeta, BatchValidationError

GOOD = {
    "batch_id": "cap-1:kalshi_ws:10",
    "capture_id": "cap-1",
    "stream": "kalshi_ws",
    "first_stream_seq": 10,
    "last_stream_seq": 12,
    "first_local_seq": 100,
    "last_local_seq": 110,
    "event_count": 3,
    "compressed_bytes": 50,
    "sha256": "a" * 64,
    "first_recv_utc": "2026-10-10T23:59:59.900000Z",
    "last_recv_utc": "2026-10-11T00:00:00.100000Z",
    "created_at_utc": "2026-10-11T00:00:01Z",
}


def test_roundtrip_and_date_partition_uses_first_event():
    meta = BatchMeta.from_dict(GOOD)
    assert BatchMeta.from_json(meta.to_json()) == meta
    assert meta.utc_date == "2026-10-10"


@pytest.mark.parametrize(
    "change",
    [
        {"batch_id": "cap-1:kalshi_ws:11"},
        {"capture_id": "cap:1", "batch_id": "cap:1:kalshi_ws:10"},
        {"capture_id": "../x", "batch_id": "../x:kalshi_ws:10"},
        {"stream": "Kalshi/WS", "batch_id": "cap-1:Kalshi/WS:10"},
        {"event_count": 4},
        {"last_local_seq": 101},
        {"sha256": "XYZ"},
        {"first_recv_utc": "2026-10-10T23:59:59"},
        {"last_recv_utc": "2026-10-10T00:00:00Z"},
        {"first_stream_seq": True, "batch_id": "cap-1:kalshi_ws:True"},
        {"schema_version": 2},
        {"format": "parquet"},
        {"extra": 1},
    ],
)
def test_rejects_invalid(change):
    with pytest.raises(BatchValidationError):
        BatchMeta.from_dict(GOOD | change)


def test_rejects_missing_field():
    data = dict(GOOD)
    del data["sha256"]
    with pytest.raises(BatchValidationError):
        BatchMeta.from_dict(data)
