from __future__ import annotations

import gzip
import json
from pathlib import Path

from kmb.collector.spool import Spool
from kmb.envelope import KIND_FRAME, SOURCE_KALSHI, Sequencer, make_envelope


def test_spool_renumbers_accepted_stream_events_after_a_drop(tmp_path: Path) -> None:
    spool = Spool(tmp_path, "capture", cap_bytes=10_000, max_batch_bytes=10_000)
    sequencer = Sequencer()

    def append_one() -> bool:
        return spool.append(
            make_envelope(
                sequencer,
                capture_id="capture",
                stream="kalshi_ws",
                kind=KIND_FRAME,
                source=SOURCE_KALSHI,
                raw='{"type":"ticker"}',
            )
        )

    assert append_one()
    spool.cap_bytes = spool.stats()["spool_bytes"]
    assert not append_one()
    spool.cap_bytes = 10_000
    assert append_one()
    spool.seal_all()
    saved = [
        json.loads(line)
        for batch in spool.pending_batches()
        for line in gzip.decompress(batch.path.read_bytes()).splitlines()
    ]
    assert [event["stream_seq"] for event in saved] == [0, 1]
    assert [event["local_event_seq"] for event in saved] == [0, 2]


def _envelope(sequencer: Sequencer, stream: str, raw: str = '{"type":"ticker"}') -> dict:
    return make_envelope(
        sequencer,
        capture_id="capture",
        stream=stream,
        kind=KIND_FRAME,
        source=SOURCE_KALSHI,
        raw=raw,
    )


def test_deferred_seal_rotates_segments_and_keeps_every_event(tmp_path: Path) -> None:
    spool = Spool(tmp_path, "capture", max_batch_bytes=2_000, defer_seal=True)
    sequencer = Sequencer()
    for _ in range(40):
        assert spool.append(_envelope(sequencer, "kalshi_ws", "x" * 100))
    # Appending never seals inline in deferred mode; segments wait for seal_ready().
    assert spool.pending_batches() == []
    assert spool.seal_ready() >= 1
    spool.seal_all()
    saved = [
        json.loads(line)
        for batch in spool.pending_batches()
        for line in gzip.decompress(batch.path.read_bytes()).splitlines()
    ]
    assert sorted(event["stream_seq"] for event in saved) == list(range(40))
    assert not list((tmp_path / "open").iterdir())


def test_deferred_segments_survive_a_crash_before_sealing(tmp_path: Path) -> None:
    spool = Spool(tmp_path, "capture", max_batch_bytes=2_000, defer_seal=True)
    sequencer = Sequencer()
    for _ in range(40):
        spool.append(_envelope(sequencer, "kalshi_ws", "x" * 100))
    reopened = Spool(tmp_path, "capture2", max_batch_bytes=2_000)
    recovered = sum(batch.meta.event_count for batch in reopened.pending_batches())
    assert recovered == 40


def test_shed_streams_are_refused_before_the_cap_but_kalshi_is_not(tmp_path: Path) -> None:
    spool = Spool(tmp_path, "capture", cap_bytes=10_000, shed_streams=frozenset({"coinbase_ws"}))
    sequencer = Sequencer()
    accepted_coinbase = 0
    while spool.append(_envelope(sequencer, "coinbase_ws", "c" * 100)):
        accepted_coinbase += 1
    assert 0 < accepted_coinbase
    assert spool.stats()["spool_bytes"] <= 8_000
    assert spool.append(_envelope(sequencer, "kalshi_ws", "k" * 100))
