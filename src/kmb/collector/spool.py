from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO

from kmb.envelope import BatchMeta, make_batch_id, utc_now_iso

logger = logging.getLogger(__name__)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        logger.debug("directory fsync unavailable for %s", path, exc_info=True)


def _fsync_file(path: Path) -> None:
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())


@dataclass
class Segment:
    path: Path
    events: list[dict[str, Any]] = field(default_factory=list)
    size: int = 0
    started: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class PendingBatch:
    meta: BatchMeta
    path: Path
    meta_path: Path


class Spool:
    """Durable per-stream JSONL segments, atomically sealed into ingest batches."""

    def __init__(
        self,
        root: Path,
        capture_id: str,
        *,
        cap_bytes: int = 5 * 1024**3,
        max_batch_bytes: int = 2 * 1024**2,
        max_age_seconds: int = 60,
        defer_seal: bool = False,
        shed_streams: frozenset[str] = frozenset(),
        shed_fraction: float = 0.8,
    ) -> None:
        if cap_bytes <= 0 or max_batch_bytes <= 0 or max_age_seconds <= 0:
            raise ValueError("spool cap, batch size, and batch age must be positive")
        # With defer_seal, full segments are only queued by append(); seal_ready() (run off the event loop)
        # does the compression and fsyncs so a busy feed never stalls its own WebSocket keepalives.
        self.defer_seal = defer_seal
        # Auxiliary streams stop being accepted at shed_fraction of the cap so they never crowd out Kalshi.
        self.shed_streams = shed_streams
        self.shed_fraction = shed_fraction
        self.root = Path(root)
        self.capture_id = capture_id
        self.cap_bytes = cap_bytes
        self.max_batch_bytes = max_batch_bytes
        self.max_age_seconds = max_age_seconds
        self.open_dir = self.root / "open"
        self.batch_dir = self.root / "batches"
        self.rejected_dir = self.root / "rejected"
        for directory in (self.open_dir, self.batch_dir, self.rejected_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._seal_lock = threading.Lock()
        self._segments: dict[str, Segment] = {}
        self._handles: dict[str, BinaryIO] = {}
        self._ready: list[Segment] = []
        self._stream_next: dict[str, int] = {}
        self._batches: dict[str, PendingBatch] = {}
        self._used_cache = 0
        self._cache_ready = False
        self.drop_count = 0
        self._drop_unreported = 0
        self._rescan_batches()
        self._recover_open_segments()
        self._used_cache = self._scan_used_bytes()
        self._cache_ready = True

    def append(self, envelope: dict[str, Any]) -> bool:
        stream = str(envelope["stream"])
        with self._lock:
            next_seq = self._stream_next.get(stream, 0)
            stored_event = {**envelope, "stream_seq": next_seq}
            line = json.dumps(stored_event, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
            target_bytes = self.max_batch_bytes - min(64 * 1024, self.max_batch_bytes // 16)
            segment = self._segments.get(stream)
            if segment and segment.size + len(line) > target_bytes:
                self._seal(stream)
                segment = None
            limit = self.cap_bytes * self.shed_fraction if stream in self.shed_streams else self.cap_bytes
            if len(line) > target_bytes or self._used_bytes() + len(line) > limit:
                self.drop_count += 1
                self._drop_unreported += 1
                return False
            if segment is None:
                segment = Segment(self.open_dir / f"{self.capture_id}.{stream}.{next_seq:012d}.jsonl")
                self._segments[stream] = segment
            handle = self._handles.get(stream)
            if handle is None:
                handle = self._handles[stream] = segment.path.open("ab")
            handle.write(line)
            handle.flush()
            envelope["stream_seq"] = next_seq
            self._stream_next[stream] = next_seq + 1
            segment.events.append(stored_event)
            segment.size += len(line)
            if self._cache_ready:
                self._used_cache += len(line)
            if self._drop_unreported:
                logger.warning("spool_drop count=%d; refusing new capture data at cap", self._drop_unreported)
                self._drop_unreported = 0
            if segment.size >= target_bytes:
                self._seal(stream)
            return True

    def seal_due(self, *, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        with self._lock:
            for stream, segment in list(self._segments.items()):
                if segment.events and now - segment.started >= self.max_age_seconds:
                    self._seal(stream)

    def seal_all(self) -> None:
        with self._lock:
            for stream in list(self._segments):
                self._seal(stream)
        self.seal_ready()

    def seal_ready(self) -> int:
        """Seal queued segments; safe to call from a worker thread. Returns the number sealed."""
        with self._seal_lock:
            return self._seal_ready_locked()

    def _seal_ready_locked(self) -> int:
        sealed = 0
        while True:
            with self._lock:
                if not self._ready:
                    return sealed
                segment = self._ready[0]
            # Compression and fsync run outside the lock so append() is never blocked behind them.
            self._seal_events(segment.events)
            with self._lock:
                self._ready.remove(segment)
                segment.path.unlink(missing_ok=True)
                if self._cache_ready:
                    self._used_cache = max(0, self._used_cache - segment.size)
                _fsync_dir(self.open_dir)
            sealed += 1

    def pending_batches(self) -> list[PendingBatch]:
        with self._lock:
            return sorted(
                self._batches.values(),
                key=lambda item: (item.meta.first_recv_utc, item.meta.created_at_utc),
            )

    def confirm_deleted(self, batch: PendingBatch) -> None:
        with self._lock:
            data_size = batch.path.stat().st_size if batch.path.exists() else 0
            meta_size = batch.meta_path.stat().st_size if batch.meta_path.exists() else 0
            batch.path.unlink(missing_ok=True)
            batch.meta_path.unlink(missing_ok=True)
            self._batches.pop(batch.meta.batch_id, None)
            if self._cache_ready:
                self._used_cache = max(0, self._used_cache - data_size - meta_size)
            _fsync_dir(self.batch_dir)

    def reject(self, batch: PendingBatch, reason: str) -> None:
        with self._lock:
            for path in (batch.path, batch.meta_path):
                if path.exists():
                    os.replace(path, self.rejected_dir / path.name)
            reason_path = self.rejected_dir / f"{batch.meta.batch_id.replace(':', '_')}.reason.txt"
            reason_path.write_text(reason, encoding="utf-8")
            self._batches.pop(batch.meta.batch_id, None)
            if self._cache_ready:
                self._used_cache += reason_path.stat().st_size
            _fsync_dir(self.rejected_dir)

    def stats(self) -> dict[str, int]:
        with self._lock:
            batches = self.pending_batches()
            return {
                "pending_batches": len(batches),
                "pending_bytes": sum(x.path.stat().st_size + x.meta_path.stat().st_size for x in batches),
                "spool_bytes": self._used_bytes(),
                "drops": self.drop_count,
            }

    def writable(self) -> bool:
        return self.root.is_dir() and os.access(self.root, os.W_OK)

    def _used_bytes(self) -> int:
        return self._used_cache if self._cache_ready else self._scan_used_bytes()

    def _scan_used_bytes(self) -> int:
        total = 0
        for directory in (self.open_dir, self.batch_dir, self.rejected_dir):
            for path in directory.iterdir():
                if path.is_file():
                    try:
                        total += path.stat().st_size
                    except OSError:
                        pass
        return total

    def _batch_stem(self, meta: BatchMeta) -> str:
        digest = hashlib.sha256(meta.batch_id.encode()).hexdigest()[:16]
        return f"{meta.capture_id}_{meta.stream}_{meta.first_stream_seq:012d}_{digest}"

    def _seal(self, stream: str) -> None:
        segment = self._segments.pop(stream, None)
        handle = self._handles.pop(stream, None)
        if handle is not None:
            handle.close()
        if segment is None or not segment.events:
            return
        if self.defer_seal:
            self._ready.append(segment)
            return
        try:
            self._seal_events(segment.events)
        except Exception:
            self._segments[stream] = segment
            raise
        segment.path.unlink(missing_ok=True)
        if self._cache_ready:
            self._used_cache = max(0, self._used_cache - segment.size)
        _fsync_dir(self.open_dir)

    def _seal_events(self, events: list[dict[str, Any]]) -> None:
        payload = b"".join(
            (json.dumps(event, separators=(",", ":"), ensure_ascii=False) + "\n").encode() for event in events
        )
        compressed = gzip.compress(payload, mtime=0)
        if len(compressed) > self.max_batch_bytes and len(events) > 1:
            midpoint = len(events) // 2
            self._seal_events(events[:midpoint])
            self._seal_events(events[midpoint:])
            return
        first, last = events[0], events[-1]
        meta = BatchMeta(
            batch_id=make_batch_id(first["capture_id"], first["stream"], first["stream_seq"]),
            capture_id=first["capture_id"],
            stream=first["stream"],
            first_stream_seq=first["stream_seq"],
            last_stream_seq=last["stream_seq"],
            first_local_seq=first["local_event_seq"],
            last_local_seq=last["local_event_seq"],
            event_count=len(events),
            compressed_bytes=len(compressed),
            sha256=hashlib.sha256(compressed).hexdigest(),
            first_recv_utc=first["received_at_utc"],
            last_recv_utc=last["received_at_utc"],
            created_at_utc=utc_now_iso(),
            uncompressed_bytes=len(payload),
        )
        stem = self._batch_stem(meta)
        data_path = self.batch_dir / f"{stem}.jsonl.gz"
        meta_path = self.batch_dir / f"{stem}.meta.json"
        data_tmp, meta_tmp = (
            data_path.with_suffix(data_path.suffix + ".tmp"),
            meta_path.with_suffix(".json.tmp"),
        )
        data_tmp.write_bytes(compressed)
        meta_tmp.write_text(meta.to_json(), encoding="utf-8")
        _fsync_file(data_tmp)
        _fsync_file(meta_tmp)
        os.replace(data_tmp, data_path)
        os.replace(meta_tmp, meta_path)
        _fsync_dir(self.batch_dir)
        batch = PendingBatch(meta, data_path, meta_path)
        with self._lock:
            self._batches[meta.batch_id] = batch
            if self._cache_ready:
                self._used_cache += data_path.stat().st_size + meta_path.stat().st_size

    def _rescan_batches(self) -> None:
        for data_path in self.batch_dir.glob("*.jsonl.gz"):
            meta_path = data_path.with_suffix("").with_suffix(".meta.json")
            if not meta_path.exists():
                self._recover_batch_meta(data_path, meta_path)
            try:
                meta = BatchMeta.from_json(meta_path.read_text(encoding="utf-8"))
                compressed = data_path.read_bytes()
                if (
                    len(compressed) != meta.compressed_bytes
                    or hashlib.sha256(compressed).hexdigest() != meta.sha256
                ):
                    logger.error("spool checksum mismatch: %s", data_path)
                    continue
                self._batches[meta.batch_id] = PendingBatch(meta, data_path, meta_path)
            except (OSError, ValueError) as exc:
                logger.error("invalid spool batch %s: %s", data_path, exc)

    def _recover_batch_meta(self, data_path: Path, meta_path: Path) -> None:
        try:
            compressed = data_path.read_bytes()
            payload = gzip.decompress(compressed)
            events = [json.loads(line) for line in payload.splitlines() if line]
            if not events:
                return
            first, last = events[0], events[-1]
            meta = BatchMeta(
                batch_id=make_batch_id(first["capture_id"], first["stream"], first["stream_seq"]),
                capture_id=first["capture_id"],
                stream=first["stream"],
                first_stream_seq=first["stream_seq"],
                last_stream_seq=last["stream_seq"],
                first_local_seq=first["local_event_seq"],
                last_local_seq=last["local_event_seq"],
                event_count=len(events),
                compressed_bytes=len(compressed),
                sha256=hashlib.sha256(compressed).hexdigest(),
                first_recv_utc=first["received_at_utc"],
                last_recv_utc=last["received_at_utc"],
                created_at_utc=utc_now_iso(),
                uncompressed_bytes=len(payload),
            )
            meta_tmp = meta_path.with_suffix(".json.tmp")
            meta_tmp.write_text(meta.to_json(), encoding="utf-8")
            _fsync_file(meta_tmp)
            os.replace(meta_tmp, meta_path)
            _fsync_dir(self.batch_dir)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            logger.error("unable to recover batch metadata %s: %s", data_path, exc)

    def _recover_open_segments(self) -> None:
        for path in sorted(self.open_dir.glob("*.jsonl")):
            try:
                raw_lines = path.read_bytes().splitlines(keepends=True)
                complete = [line for line in raw_lines if line.endswith(b"\n")]
                events = [json.loads(line) for line in complete]
                if events:
                    self._seal_events(events)
                path.unlink(missing_ok=True)
                _fsync_dir(self.open_dir)
            except (OSError, ValueError, KeyError, TypeError):
                logger.exception("unable to recover open spool segment %s", path)
