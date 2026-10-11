"""SQLite receipt index. Lives on the ext4 root (not on exFAT) and holds one row per batch."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from kmb.envelope import BatchMeta

PENDING = "pending"
COMMITTED = "committed"
FAILED = "failed"
CORRUPT = "corrupt"

_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    capture_id TEXT NOT NULL,
    stream TEXT NOT NULL,
    first_stream_seq INTEGER NOT NULL,
    last_stream_seq INTEGER NOT NULL,
    first_local_seq INTEGER NOT NULL,
    last_local_seq INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    compressed_bytes INTEGER NOT NULL,
    uncompressed_bytes INTEGER,
    sha256 TEXT NOT NULL,
    first_recv_utc TEXT NOT NULL,
    last_recv_utc TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    meta_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('{PENDING}','{COMMITTED}','{FAILED}','{CORRUPT}')),
    attempts INTEGER NOT NULL DEFAULT 1,
    received_at_utc TEXT NOT NULL,
    committed_at_utc TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS ix_batches_capture ON batches (capture_id, stream, first_stream_seq);
CREATE INDEX IF NOT EXISTS ix_batches_status ON batches (status);
CREATE INDEX IF NOT EXISTS ix_batches_committed ON batches (committed_at_utc);
"""


class ReceiptStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def ping(self) -> bool:
        try:
            with self._lock:
                self._conn.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False

    def get(self, batch_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
        return dict(row) if row else None

    def upsert_pending(self, meta: BatchMeta, rel_path: str, now: str, uncompressed: int) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO batches (batch_id, capture_id, stream, first_stream_seq, last_stream_seq,
                    first_local_seq, last_local_seq, event_count, compressed_bytes, uncompressed_bytes,
                    sha256, first_recv_utc, last_recv_utc, rel_path, meta_json, status, received_at_utc)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (batch_id) DO UPDATE SET
                    status = excluded.status, attempts = batches.attempts + 1,
                    received_at_utc = excluded.received_at_utc, error = NULL
                WHERE batches.status != 'committed'
                """,
                (
                    meta.batch_id,
                    meta.capture_id,
                    meta.stream,
                    meta.first_stream_seq,
                    meta.last_stream_seq,
                    meta.first_local_seq,
                    meta.last_local_seq,
                    meta.event_count,
                    meta.compressed_bytes,
                    uncompressed,
                    meta.sha256,
                    meta.first_recv_utc,
                    meta.last_recv_utc,
                    rel_path,
                    meta.to_json(),
                    PENDING,
                    now,
                ),
            )

    def mark_committed(self, batch_id: str, now: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE batches SET status = ?, committed_at_utc = ?, error = NULL WHERE batch_id = ?",
                (COMMITTED, now, batch_id),
            )

    def mark(self, batch_id: str, status: str, error: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE batches SET status = ?, error = ? WHERE batch_id = ?", (status, error, batch_id)
            )

    def by_status(self, status: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM batches WHERE status = ?", (status,)).fetchall()
        return [dict(r) for r in rows]

    def committed_since(self, since_iso: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches WHERE status = ? AND committed_at_utc >= ?", (COMMITTED, since_iso)
            ).fetchall()
        return [dict(r) for r in rows]

    def all_committed(self) -> list[dict]:
        return self.by_status(COMMITTED)

    def counts(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT status, COUNT(*) AS n FROM batches GROUP BY status").fetchall()
        return {r["status"]: r["n"] for r in rows}

    def coverage(self, capture_id: str) -> dict:
        """Per-stream contiguity of committed batches plus a global local_event_seq completeness check.

        A trailing gap (batches not yet uploaded) cannot be seen here; the collector reports its own
        backlog.
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT stream, first_stream_seq, last_stream_seq, first_local_seq, last_local_seq,
                          event_count, status
                   FROM batches WHERE capture_id = ? ORDER BY stream, first_stream_seq""",
                (capture_id,),
            ).fetchall()
        streams: dict[str, dict] = {}
        total_events = 0
        min_local: int | None = None
        max_local: int | None = None
        for r in rows:
            s = streams.setdefault(
                r["stream"],
                {
                    "committed_batches": 0,
                    "events": 0,
                    "next_stream_seq": 0,
                    "holes": [],
                    "overlaps": [],
                    "not_committed": {},
                },
            )
            if r["status"] != COMMITTED:
                s["not_committed"][r["status"]] = s["not_committed"].get(r["status"], 0) + 1
                continue
            first, last = r["first_stream_seq"], r["last_stream_seq"]
            if first > s["next_stream_seq"]:
                s["holes"].append([s["next_stream_seq"], first - 1])
            elif first < s["next_stream_seq"]:
                s["overlaps"].append([first, last])
            s["next_stream_seq"] = max(s["next_stream_seq"], last + 1)
            s["committed_batches"] += 1
            s["events"] += r["event_count"]
            total_events += r["event_count"]
            min_local = r["first_local_seq"] if min_local is None else min(min_local, r["first_local_seq"])
            max_local = r["last_local_seq"] if max_local is None else max(max_local, r["last_local_seq"])
        missing_local = None if max_local is None else (max_local + 1) - total_events
        complete = (
            bool(streams)
            and missing_local == 0
            and all(not s["holes"] and not s["overlaps"] and not s["not_committed"] for s in streams.values())
        )
        return {
            "capture_id": capture_id,
            "streams": streams,
            "total_events": total_events,
            "min_local_seq": min_local,
            "max_local_seq": max_local,
            "missing_local_seqs": missing_local,
            "complete_so_far": complete,
        }
