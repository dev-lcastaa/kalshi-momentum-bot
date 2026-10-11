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
