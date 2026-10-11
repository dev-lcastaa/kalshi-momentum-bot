"""Batch validation, write-once persistence and crash recovery for the raw archive.

Commit protocol (exFAT has no journal, so every step is verified):

1. Validate metadata and content (size, sha256, gzip, JSONL envelopes, seq contiguity).
2. Re-check storage (sentinel + mount identity + free space) under the write lock.
3. Record a ``pending`` receipt in SQLite (on ext4) before touching the archive.
4. Write ``.tmp-*`` in the destination dir, flush + fsync, rename to the final name, fsync the dir.
5. Read the final file back and compare its sha256.
6. Mark the receipt ``committed``; only then acknowledge.

On startup, ``reconcile`` resolves ``pending`` receipts left by a crash, removes stray temp files and
re-verifies recently committed files. Archive files are never deleted: conflicting or corrupt files
are moved to ``quarantine/``.
"""

from __future__ import annotations

import errno
import gzip
import hashlib
import io
import json
import logging
import os
import threading
import uuid
import zlib
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from kmb.envelope import ENVELOPE_REQUIRED_FIELDS, SCHEMA_VERSION, BatchMeta, utc_now_iso
from kmb.ingest.receipts import COMMITTED, CORRUPT, FAILED, PENDING, ReceiptStore
from kmb.ingest.storage import StorageGuard

log = logging.getLogger("kmb.ingest.archive")

TMP_PREFIX = ".tmp-"
QUARANTINE_DIR = "quarantine"


class IngestError(Exception):
    def __init__(self, code: str, status: int, retryable: bool, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.retryable = retryable
        self.message = message

    def to_dict(self) -> dict:
        return {"error": self.code, "message": self.message, "retryable": self.retryable}


def _invalid(code: str, message: str) -> IngestError:
    return IngestError(code, 400, False, message)


@dataclass(frozen=True)
class PutResult:
    created: bool
    receipt: dict


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fsync_dir(path: Path) -> None:
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    except OSError as exc:
        if exc.errno not in (errno.EINVAL, errno.ENOTSUP):
            raise
    finally:
        os.close(fd)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def validate_content(meta: BatchMeta, body: bytes, max_uncompressed: int) -> int:
    """Validate a compressed batch against its metadata. Returns the uncompressed size."""
    if len(body) != meta.compressed_bytes:
        raise _invalid("size_mismatch", f"body is {len(body)} bytes, metadata says {meta.compressed_bytes}")
    if hashlib.sha256(body).hexdigest() != meta.sha256:
        raise _invalid("checksum_mismatch", "sha256 of body does not match metadata")

    chunks: list[bytes] = []
    total = 0
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
            while chunk := gz.read(1 << 20):
                total += len(chunk)
                if total > max_uncompressed:
                    raise IngestError(
                        "uncompressed_too_large", 413, False, f"uncompressed size exceeds {max_uncompressed}"
                    )
                chunks.append(chunk)
    except (OSError, EOFError, zlib.error) as exc:
        raise _invalid("bad_gzip", f"body is not valid gzip: {exc}") from exc
    data = b"".join(chunks)

    if meta.uncompressed_bytes is not None and meta.uncompressed_bytes != total:
        raise _invalid("uncompressed_size_mismatch", "uncompressed size does not match metadata")
    if not data.endswith(b"\n"):
        raise _invalid("bad_jsonl", "JSONL must end with a newline")
    lines = data.split(b"\n")[:-1]
    if len(lines) != meta.event_count:
        raise _invalid("event_count_mismatch", f"{len(lines)} lines, metadata says {meta.event_count}")

    prev_local: int | None = None
    first_ev: dict | None = None
    ev: dict = {}
    for i, line in enumerate(lines):
        try:
            ev = json.loads(line)
        except ValueError as exc:
            raise _invalid("bad_jsonl", f"line {i} is not valid JSON") from exc
        if not isinstance(ev, dict):
            raise _invalid("bad_envelope", f"line {i} is not a JSON object")
        missing = [f for f in ENVELOPE_REQUIRED_FIELDS if f not in ev]
        if missing:
            raise _invalid("bad_envelope", f"line {i} missing fields {missing}")
        if ev["schema_version"] != SCHEMA_VERSION:
            raise _invalid("bad_envelope", f"line {i} has unsupported schema_version")
        if ev["capture_id"] != meta.capture_id or ev["stream"] != meta.stream:
            raise _invalid("envelope_mismatch", f"line {i} capture_id/stream differ from metadata")
        stream_seq, local_seq = ev["stream_seq"], ev["local_event_seq"]
        if not _is_int(stream_seq) or not _is_int(local_seq):
            raise _invalid("bad_envelope", f"line {i} has non-integer sequence numbers")
        if stream_seq != meta.first_stream_seq + i:
            raise _invalid("stream_seq_not_contiguous", f"line {i} stream_seq {stream_seq} out of order")
        if prev_local is not None and local_seq <= prev_local:
            raise _invalid("local_seq_not_increasing", f"line {i} local_event_seq not increasing")
        prev_local = local_seq
        if first_ev is None:
            first_ev = ev

    assert first_ev is not None
    if first_ev["local_event_seq"] != meta.first_local_seq or ev["local_event_seq"] != meta.last_local_seq:
        raise _invalid("local_seq_mismatch", "first/last local_event_seq differ from metadata")
    if first_ev["received_at_utc"] != meta.first_recv_utc or ev["received_at_utc"] != meta.last_recv_utc:
        raise _invalid("recv_time_mismatch", "first/last received_at_utc differ from metadata")
    return total


def rel_path_for(meta: BatchMeta) -> str:
    return (
        f"raw/date={meta.utc_date}/capture_id={meta.capture_id}/stream={meta.stream}/"
        f"batch-{meta.first_stream_seq:012d}.jsonl.gz"
    )


class Archive:
    def __init__(
        self,
        root: Path,
        receipts: ReceiptStore,
        guard: StorageGuard,
        *,
        max_uncompressed_bytes: int,
        verify_recent_hours: int = 24,
        now: Callable[[], str] = utc_now_iso,
    ) -> None:
        self.root = root
        self.receipts = receipts
        self.guard = guard
        self.max_uncompressed_bytes = max_uncompressed_bytes
        self.verify_recent_hours = verify_recent_hours
        self._now = now
        self._write_lock = threading.Lock()
        self.last_durable_write_utc: str | None = None
        self.recent_errors: deque[dict] = deque(maxlen=20)

    def _record_error(self, batch_id: str | None, code: str, message: str) -> None:
        self.recent_errors.append(
            {"at": self._now(), "batch_id": batch_id, "error": code, "message": message}
        )

    def put(self, meta: BatchMeta, body: bytes) -> PutResult:
        try:
            return self._put(meta, body)
        except IngestError as exc:
            self._record_error(meta.batch_id, exc.code, exc.message)
            log.warning(
                "batch rejected",
                extra={
                    "ctx": {
                        "batch_id": meta.batch_id,
                        "capture_id": meta.capture_id,
                        "stream": meta.stream,
                        "error": exc.code,
                        "detail": exc.message,
                    }
                },
            )
            raise

    def _check_existing(self, meta: BatchMeta, existing: dict | None) -> PutResult | None:
        if existing is None:
            return None
        if existing["sha256"] != meta.sha256:
            log.error(
                "batch id reused with different content",
                extra={"ctx": {"batch_id": meta.batch_id, "status": existing["status"]}},
            )
            raise IngestError("batch_conflict", 409, False, "batch_id already exists with a different sha256")
        if existing["status"] == COMMITTED:
            return PutResult(created=False, receipt=existing)
        return None

    def _put(self, meta: BatchMeta, body: bytes) -> PutResult:
        dup = self._check_existing(meta, self.receipts.get(meta.batch_id))
        if dup:
            return dup
        uncompressed = validate_content(meta, body, self.max_uncompressed_bytes)
        rel = rel_path_for(meta)

        with self._write_lock:
            dup = self._check_existing(meta, self.receipts.get(meta.batch_id))
            if dup:
                return dup
            status = self.guard.check()
            if not status.ok:
                code = (
                    "insufficient_storage"
                    if status.checks.get("free_space") is False
                    else "storage_not_ready"
                )
                raise IngestError(
                    code,
                    507 if code == "insufficient_storage" else 503,
                    True,
                    status.reason or "storage not ready",
                )
            if status.free_bytes is not None and status.free_bytes - len(body) < self.guard.min_free_bytes:
                raise IngestError(
                    "insufficient_storage", 507, True, "batch would cross the free-space threshold"
                )

            self.receipts.upsert_pending(meta, rel, self._now(), uncompressed)
            final = self.root / rel
            tmp: Path | None = None
            try:
                final.parent.mkdir(parents=True, exist_ok=True)
                if final.exists():
                    # Left over from a crash after rename but before the receipt commit.
                    if sha256_file(final) != meta.sha256:
                        self._quarantine(final, "pre-existing file with different content")
                if not final.exists():
                    tmp = final.parent / f"{TMP_PREFIX}{final.name}-{uuid.uuid4().hex[:8]}"
                    with open(tmp, "xb") as fh:
                        fh.write(body)
                        fh.flush()
                        os.fsync(fh.fileno())
                    os.rename(tmp, final)
                    tmp = None
                    _fsync_dir(final.parent)
                if sha256_file(final) != meta.sha256:
                    self._quarantine(final, "read-back checksum mismatch")
                    raise IngestError("write_verify_failed", 500, True, "read-back checksum mismatch")
            except IngestError as exc:
                self.receipts.mark(meta.batch_id, FAILED, exc.code)
                raise
            except OSError as exc:
                self.receipts.mark(meta.batch_id, FAILED, f"{exc.__class__.__name__}: {exc}")
                if exc.errno == errno.ENOSPC:
                    raise IngestError("insufficient_storage", 507, True, "disk full") from exc
                raise IngestError("write_failed", 500, True, f"write failed: {exc}") from exc
            finally:
                if tmp is not None:
                    try:
                        tmp.unlink(missing_ok=True)
                    except OSError:
                        log.exception("could not remove temp file", extra={"ctx": {"path": str(tmp)}})

            committed_at = self._now()
            self.receipts.mark_committed(meta.batch_id, committed_at)
            self.last_durable_write_utc = committed_at

        receipt = self.receipts.get(meta.batch_id)
        assert receipt is not None
        log.info(
            "batch committed",
            extra={
                "ctx": {
                    "batch_id": meta.batch_id,
                    "capture_id": meta.capture_id,
                    "stream": meta.stream,
                    "events": meta.event_count,
                    "bytes": meta.compressed_bytes,
                }
            },
        )
        return PutResult(created=True, receipt=receipt)

    def _quarantine(self, path: Path, reason: str) -> Path:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        rel = path.relative_to(self.root)
        dest = self.root / QUARANTINE_DIR / rel.parent / f"{rel.name}.{stamp}"
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.rename(path, dest)
        log.error("file quarantined", extra={"ctx": {"path": str(rel), "to": str(dest), "reason": reason}})
        return dest

    def reconcile(self) -> dict:
        """Crash recovery. Safe to run at every startup; cost is bounded by pending + recent batches."""
        summary = {
            "pending_committed": 0,
            "pending_failed": 0,
            "tmp_removed": 0,
            "recent_verified": 0,
            "recent_corrupt": 0,
        }
        dirs: set[Path] = set()

        for r in self.receipts.by_status(PENDING):
            final = self.root / r["rel_path"]
            dirs.add(final.parent)
            try:
                if final.exists() and sha256_file(final) == r["sha256"]:
                    self.receipts.mark_committed(r["batch_id"], self._now())
                    summary["pending_committed"] += 1
                    continue
                if final.exists():
                    self._quarantine(final, "pending batch with mismatching file")
            except OSError as exc:
                log.error(
                    "reconcile could not inspect file",
                    extra={"ctx": {"batch_id": r["batch_id"], "error": str(exc)}},
                )
            self.receipts.mark(r["batch_id"], FAILED, "interrupted before commit")
            summary["pending_failed"] += 1

        since = (
            (datetime.now(UTC) - timedelta(hours=self.verify_recent_hours)).isoformat().replace("+00:00", "Z")
        )
        for r in self.receipts.committed_since(since):
            final = self.root / r["rel_path"]
            dirs.add(final.parent)
            ok = False
            try:
                ok = final.stat().st_size == r["compressed_bytes"] and sha256_file(final) == r["sha256"]
            except OSError:
                ok = False
            if ok:
                summary["recent_verified"] += 1
            else:
                self.receipts.mark(r["batch_id"], CORRUPT, "missing or checksum mismatch at reconcile")
                self._record_error(r["batch_id"], "corrupt", "committed file missing or corrupt")
                log.critical("committed batch missing or corrupt", extra={"ctx": {"batch_id": r["batch_id"]}})
                summary["recent_corrupt"] += 1

        for d in dirs:
            if not d.is_dir():
                continue
            for tmp in d.glob(f"{TMP_PREFIX}*"):
                try:
                    tmp.unlink()
                    summary["tmp_removed"] += 1
                except OSError:
                    log.exception("could not remove temp file", extra={"ctx": {"path": str(tmp)}})
        log.info("reconcile finished", extra={"ctx": summary})
        return summary

    def verify_all(self) -> dict:
        """Full re-hash of every committed batch. Expensive; run manually or from a weekly job."""
        result = {"verified": 0, "corrupt": []}
        for r in self.receipts.all_committed():
            final = self.root / r["rel_path"]
            try:
                ok = sha256_file(final) == r["sha256"]
            except OSError:
                ok = False
            if ok:
                result["verified"] += 1
            else:
                self.receipts.mark(r["batch_id"], CORRUPT, "checksum mismatch in full verify")
                result["corrupt"].append(r["batch_id"])
        return result
