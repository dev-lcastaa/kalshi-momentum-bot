"""Upload batch metadata shared by the collector (producer) and the ingest service (consumer).

A batch is one gzip-compressed JSON Lines file holding consecutive envelopes of a single
(capture_id, stream). Within a stream, ``stream_seq`` is contiguous starting at 0, so a missing
batch shows up as a hole. ``local_event_seq`` is the global replay order across all streams of a
capture. The batch ID is deterministic, so a restarted collector can never mint a new ID for a
batch that already exists.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime

SCHEMA_VERSION = 1
BATCH_FORMAT = "jsonl.gz"

ENVELOPE_REQUIRED_FIELDS = (
    "schema_version",
    "capture_id",
    "stream",
    "stream_seq",
    "local_event_seq",
    "kind",
    "received_at_utc",
)

# exFAT forbids " * / : < > ? \ | in names, so IDs used as path components stay within this set.
_CAPTURE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
_STREAM_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,31}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class BatchValidationError(ValueError):
    pass


def make_batch_id(capture_id: str, stream: str, first_stream_seq: int) -> str:
    return f"{capture_id}:{stream}:{first_stream_seq}"


def parse_utc(value: str) -> datetime:
    if not isinstance(value, str) or not (value.endswith("Z") or value.endswith("+00:00")):
        raise BatchValidationError(f"timestamp must be UTC ISO-8601: {value!r}")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise BatchValidationError(f"invalid timestamp: {value!r}") from exc
    return dt.astimezone(UTC)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _require_int(name: str, value: object, minimum: int = 0) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise BatchValidationError(f"{name} must be an integer >= {minimum}")


@dataclass(frozen=True)
class BatchMeta:
    batch_id: str
    capture_id: str
    stream: str
    first_stream_seq: int
    last_stream_seq: int
    first_local_seq: int
    last_local_seq: int
    event_count: int
    compressed_bytes: int
    sha256: str
    first_recv_utc: str
    last_recv_utc: str
    created_at_utc: str
    uncompressed_bytes: int | None = None
    schema_version: int = SCHEMA_VERSION
    format: str = BATCH_FORMAT

    @classmethod
    def from_dict(cls, data: object) -> BatchMeta:
        if not isinstance(data, dict):
            raise BatchValidationError("batch metadata must be a JSON object")
        known = {f.name for f in fields(cls)}
        unknown = set(data) - known
        if unknown:
            raise BatchValidationError(f"unknown metadata fields: {sorted(unknown)}")
        try:
            meta = cls(**data)
        except TypeError as exc:
            raise BatchValidationError(f"missing metadata fields: {exc}") from exc
        meta.validate()
        return meta

    @classmethod
    def from_json(cls, text: str) -> BatchMeta:
        try:
            data = json.loads(text)
        except ValueError as exc:
            raise BatchValidationError("batch metadata is not valid JSON") from exc
        return cls.from_dict(data)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)

    @property
    def utc_date(self) -> str:
        return parse_utc(self.first_recv_utc).date().isoformat()

    def validate(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise BatchValidationError(f"unsupported schema_version {self.schema_version}")
        if self.format != BATCH_FORMAT:
            raise BatchValidationError(f"unsupported format {self.format!r}")
        if not isinstance(self.capture_id, str) or not _CAPTURE_RE.match(self.capture_id):
            raise BatchValidationError("invalid capture_id")
        if not isinstance(self.stream, str) or not _STREAM_RE.match(self.stream):
            raise BatchValidationError("invalid stream")
        for name in ("first_stream_seq", "last_stream_seq", "first_local_seq", "last_local_seq"):
            _require_int(name, getattr(self, name))
        _require_int("event_count", self.event_count, 1)
        _require_int("compressed_bytes", self.compressed_bytes, 1)
        if self.uncompressed_bytes is not None:
            _require_int("uncompressed_bytes", self.uncompressed_bytes, 1)
        if self.batch_id != make_batch_id(self.capture_id, self.stream, self.first_stream_seq):
            raise BatchValidationError("batch_id must equal capture_id:stream:first_stream_seq")
        if self.last_stream_seq - self.first_stream_seq + 1 != self.event_count:
            raise BatchValidationError("stream_seq range does not match event_count")
        if self.last_local_seq - self.first_local_seq + 1 < self.event_count:
            raise BatchValidationError("local_event_seq range smaller than event_count")
        if not isinstance(self.sha256, str) or not _SHA256_RE.match(self.sha256):
            raise BatchValidationError("sha256 must be 64 lowercase hex chars")
        if parse_utc(self.last_recv_utc) < parse_utc(self.first_recv_utc):
            raise BatchValidationError("last_recv_utc before first_recv_utc")
        parse_utc(self.created_at_utc)
