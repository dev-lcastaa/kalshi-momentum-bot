from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_MIB = 1024 * 1024
_GIB = 1024 * _MIB


class ConfigError(RuntimeError):
    pass


def _int(env: dict[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


@dataclass(frozen=True)
class IngestConfig:
    token: str = field(repr=False)
    archive_root: Path
    db_path: Path
    archive_id: str
    expected_fs_uuid: str | None = None
    expected_fstype: str | None = None
    by_uuid_dir: Path = Path("/dev/disk/by-uuid")
    mountinfo_path: Path = Path("/proc/self/mountinfo")
    max_batch_bytes: int = 8 * _MIB
    max_uncompressed_bytes: int = 128 * _MIB
    min_free_bytes: int = 5 * _GIB
    verify_recent_hours: int = 24
    host: str = "0.0.0.0"
    port: int = 8701
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> IngestConfig:
        env = dict(os.environ if env is None else env)
        token = env.get("INGEST_TOKEN", "")
        token_file = env.get("INGEST_TOKEN_FILE")
        if not token and token_file:
            token = Path(token_file).read_text(encoding="utf-8").strip()
        if len(token) < 32:
            raise ConfigError("INGEST_TOKEN or INGEST_TOKEN_FILE must provide a token of at least 32 chars")
        for name in ("INGEST_ARCHIVE_ROOT", "INGEST_DB_PATH", "INGEST_ARCHIVE_ID"):
            if not env.get(name):
                raise ConfigError(f"{name} is required")
        return cls(
            token=token,
            archive_root=Path(env["INGEST_ARCHIVE_ROOT"]),
            db_path=Path(env["INGEST_DB_PATH"]),
            archive_id=env["INGEST_ARCHIVE_ID"],
            expected_fs_uuid=env.get("INGEST_EXPECTED_FS_UUID") or None,
            expected_fstype=env.get("INGEST_EXPECTED_FSTYPE") or None,
            by_uuid_dir=Path(env.get("INGEST_BY_UUID_DIR", "/dev/disk/by-uuid")),
            mountinfo_path=Path(env.get("INGEST_MOUNTINFO", "/proc/self/mountinfo")),
            max_batch_bytes=_int(env, "INGEST_MAX_BATCH_BYTES", 8 * _MIB),
            max_uncompressed_bytes=_int(env, "INGEST_MAX_UNCOMPRESSED_BYTES", 128 * _MIB),
            min_free_bytes=_int(env, "INGEST_MIN_FREE_BYTES", 5 * _GIB),
            verify_recent_hours=_int(env, "INGEST_VERIFY_RECENT_HOURS", 24),
            host=env.get("INGEST_HOST", "0.0.0.0"),
            port=_int(env, "INGEST_PORT", 8701),
            log_level=env.get("INGEST_LOG_LEVEL", "INFO"),
        )
