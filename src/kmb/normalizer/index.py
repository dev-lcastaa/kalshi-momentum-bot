"""Receipt-authoritative, incremental index of immutable archived envelope batches."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import re
import sqlite3
from collections.abc import Iterator
from pathlib import Path

LOG = logging.getLogger(__name__)
WINDOW_ID = re.compile(r"^[A-Z0-9][A-Z0-9._-]*$")


def under(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or path == root.resolve():
        raise ValueError("path escapes storage root")
    return path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def events(path: Path) -> Iterator[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError("envelope must be an object")
            yield event


def acknowledged(receipts: Path) -> Iterator[dict]:
    # mode=ro, not immutable=1: ingest's live WAL must remain visible.
    with sqlite3.connect(receipts.resolve().as_uri() + "?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT * FROM batches WHERE status='committed' ORDER BY batch_id"):
            yield dict(row)


class FileIndex:
    def __init__(self, state: Path, archive: Path, receipts: Path):
        state.parent.mkdir(parents=True, exist_ok=True)
        self.archive, self.receipts = archive, receipts
        self.conn = sqlite3.connect(state, timeout=60)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY, capture_id TEXT, stream TEXT,
                first_stream_seq INTEGER, last_stream_seq INTEGER,
                min_ms INTEGER, max_ms INTEGER, tickers TEXT, sha256 TEXT,
                size INTEGER, mtime_ns INTEGER, verified INTEGER, active INTEGER
            );
            CREATE INDEX IF NOT EXISTS files_time ON files(min_ms,max_ms);
            CREATE TABLE IF NOT EXISTS markets (ticker TEXT PRIMARY KEY, metadata TEXT NOT NULL);
        """)

    def close(self):
        self.conn.close()

    def update(self) -> dict:
        from kmb.envelope.batch import parse_utc
        from kmb.normalizer.windows import market_metadata

        added, invalid = 0, 0
        self.conn.execute("UPDATE files SET active=0")
        for receipt in acknowledged(self.receipts):
            relative = receipt["rel_path"]
            old = self.conn.execute("SELECT * FROM files WHERE path=?", (relative,)).fetchone()
            try:
                path = under(self.archive, relative)
                stat = path.stat()
                if (
                    old
                    and old["verified"]
                    and old["sha256"] == receipt["sha256"]
                    and old["size"] == stat.st_size
                    and old["mtime_ns"] == stat.st_mtime_ns
                ):
                    self.conn.execute("UPDATE files SET active=1 WHERE path=?", (relative,))
                    continue
                if stat.st_size != receipt["compressed_bytes"] or sha256(path) != receipt["sha256"]:
                    raise ValueError("receipt checksum/size mismatch")
                lo, hi, tickers, metadata = None, None, set(), []
                count = 0
                for event in events(path):
                    if (
                        event["capture_id"] != receipt["capture_id"]
                        or event["stream"] != receipt["stream"]
                        or event["stream_seq"] != receipt["first_stream_seq"] + count
                    ):
                        raise ValueError("batch identity/stream sequence mismatch")
                    wall = int(event["recv_wall_ms"])
                    lo = wall if lo is None else min(lo, wall)
                    hi = wall if hi is None else max(hi, wall)
                    if event.get("market_ticker"):
                        tickers.add(event["market_ticker"])
                    meta = market_metadata(event)
                    if meta:
                        metadata.append((wall, meta))
                    count += 1
                if (
                    count != receipt["event_count"]
                    or count != receipt["last_stream_seq"] - receipt["first_stream_seq"] + 1
                ):
                    raise ValueError("batch event count mismatch")
                for wall, meta in metadata:
                    self._merge_market(wall, meta)
                values = (lo, hi, json.dumps(sorted(tickers)), stat.st_size, stat.st_mtime_ns, 1)
                added += 1
            except (OSError, ValueError, KeyError, TypeError, EOFError) as exc:
                LOG.warning("unverified batch %s: %s", relative, exc)
                lo = int(parse_utc(receipt["first_recv_utc"]).timestamp() * 1000)
                hi = int(parse_utc(receipt["last_recv_utc"]).timestamp() * 1000)
                values = (lo, hi, "[]", 0, 0, 0)
                invalid += 1
            self.conn.execute(
                """
                INSERT OR REPLACE INTO files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1)
            """,
                (
                    relative,
                    receipt["capture_id"],
                    receipt["stream"],
                    receipt["first_stream_seq"],
                    receipt["last_stream_seq"],
                    values[0],
                    values[1],
                    values[2],
                    receipt["sha256"],
                    values[3],
                    values[4],
                    values[5],
                ),
            )
        return {"indexed": added, "invalid": invalid}

    def _merge_market(self, wall: int, meta: dict):
        ticker = meta.pop("ticker")
        row = self.conn.execute("SELECT metadata FROM markets WHERE ticker=?", (ticker,)).fetchone()
        data = json.loads(row[0]) if row else {"ticker": ticker, "_versions": {}}
        for key, value in meta.items():
            if value is not None and wall >= data["_versions"].get(key, -1):
                data[key] = value
                data["_versions"][key] = wall
        self.conn.execute("INSERT OR REPLACE INTO markets VALUES (?,?)", (ticker, json.dumps(data)))

    def markets(self) -> Iterator[dict]:
        for row in self.conn.execute("SELECT metadata FROM markets ORDER BY ticker"):
            yield json.loads(row[0])

    def files(self, start: int, end: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM files WHERE active=1 AND min_ms<=? AND max_ms>=? "
            "ORDER BY capture_id,stream,first_stream_seq",
            (end, start),
        ).fetchall()

    def capture_starts(self) -> dict[str, int]:
        return dict(
            self.conn.execute(
                "SELECT capture_id,MIN(min_ms) FROM files WHERE active=1 AND verified=1 GROUP BY capture_id"
            ).fetchall()
        )
