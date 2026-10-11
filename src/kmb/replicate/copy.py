"""Verified, append-only storage1 -> storage2 replication (never overwrite or delete)."""

from __future__ import annotations

import ctypes
import errno
import os
import shutil
import sqlite3
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from kmb.ingest.storage import StorageGuard
from kmb.normalizer.index import acknowledged, sha256, under
from kmb.normalizer.windows import sync_directory


@dataclass(frozen=True)
class Config:
    source_mount: Path = Path("/mnt/storage1")
    destination_mount: Path = Path("/mnt/storage2")
    receipts_path: Path = Path("/var/lib/kmb-ingest/ingest.sqlite")
    state_path: Path = Path("/var/lib/kmb-normalizer/replicate.sqlite")
    source_uuid: str = "C174-8D73"
    destination_uuid: str = "2E5C-6B4D"

    @classmethod
    def from_env(cls):
        return cls(
            source_mount=Path(os.getenv("KMB_REPLICATE_SOURCE_MOUNT", str(cls.source_mount))),
            destination_mount=Path(os.getenv("KMB_REPLICATE_DESTINATION_MOUNT", str(cls.destination_mount))),
            receipts_path=Path(os.getenv("KMB_REPLICATE_RECEIPTS_PATH", str(cls.receipts_path))),
            state_path=Path(os.getenv("KMB_REPLICATE_STATE_PATH", str(cls.state_path))),
            source_uuid=os.getenv("KMB_REPLICATE_SOURCE_UUID", cls.source_uuid),
            destination_uuid=os.getenv("KMB_REPLICATE_DESTINATION_UUID", cls.destination_uuid),
        )


def check_mount(root: Path, expected_uuid: str):
    guard = StorageGuard(root, "", min_free_bytes=0, expected_fs_uuid=expected_uuid, expected_fstype="exfat")
    reason = guard.check_mount_identity()
    if reason:
        raise RuntimeError(reason)


def rename_no_replace(source: Path, destination: Path):
    # os.rename overwrites on Linux. renameat2 is atomic and supports exFAT without hardlinks.
    if os.name == "posix":
        libc = ctypes.CDLL(None, use_errno=True)
        rename = libc.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        if rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), str(destination))
    else:
        os.rename(source, destination)


def signature(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def candidates(cfg: Config) -> Iterator[tuple[str, str | None]]:
    for receipt in acknowledged(cfg.receipts_path):
        yield "kalshi-archive/" + receipt["rel_path"], receipt["sha256"]
    for name in ("kalshi-derived", "kalshi-db-backups"):
        root = cfg.source_mount / name
        # Sidecars are completion markers and must be copied after Parquet.
        for path in sorted(root.rglob("*"), key=lambda p: (p.name.endswith(".window.json"), str(p))):
            if path.is_file() and not any(
                part.startswith(".") or part.endswith(".tmp") for part in path.relative_to(root).parts
            ):
                yield path.relative_to(cfg.source_mount).as_posix(), None


def run(cfg: Config, *, mount_check: Callable[[Path, str], None] = check_mount) -> dict:
    mount_check(cfg.source_mount, cfg.source_uuid)
    mount_check(cfg.destination_mount, cfg.destination_uuid)
    if cfg.source_mount.resolve() == cfg.destination_mount.resolve():
        raise ValueError("source and destination mounts must differ")
    cfg.state_path.parent.mkdir(parents=True, exist_ok=True)
    report = {"copied": 0, "verified": 0, "cached": 0, "conflicts": [], "errors": []}
    with sqlite3.connect(cfg.state_path, timeout=60) as conn:
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("""CREATE TABLE IF NOT EXISTS copies (
            path TEXT PRIMARY KEY, source_size INTEGER, source_mtime INTEGER,
            destination_size INTEGER, destination_mtime INTEGER, sha256 TEXT
        )""")
        conn.execute("BEGIN IMMEDIATE")
        blocked = set()
        for relative, expected in candidates(cfg):
            temporary = None
            try:
                source, destination = (
                    under(cfg.source_mount, relative),
                    under(cfg.destination_mount, relative),
                )
                if source.is_symlink() or destination.is_symlink():
                    raise ValueError("symlinks are not replicated")
                source_sig = signature(source)
                old = conn.execute("SELECT * FROM copies WHERE path=?", (relative,)).fetchone()
                if relative.endswith(".window.json"):
                    parquet = relative.removesuffix(".window.json") + ".parquet"
                    if parquet in blocked:
                        raise ValueError("completion marker withheld: Parquet was not verified")
                if destination.exists():
                    destination_sig = signature(destination)
                    if (
                        old
                        and source_sig == tuple(old[1:3])
                        and destination_sig == tuple(old[3:5])
                        and (expected is None or expected == old[5])
                    ):
                        report["cached"] += 1
                        continue
                digest = sha256(source)
                if expected is not None and digest != expected:
                    raise ValueError("acknowledged archive checksum mismatch")
                if destination.exists():
                    if destination.stat().st_size != source_sig[0] or sha256(destination) != digest:
                        report["conflicts"].append(relative)
                        blocked.add(relative)
                        continue
                    report["verified"] += 1
                else:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    temporary = destination.with_name(destination.name + f".{uuid.uuid4().hex}.tmp")
                    with source.open("rb") as src, temporary.open("xb") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)
                        dst.flush()
                        os.fsync(dst.fileno())
                    if signature(source) != source_sig or sha256(temporary) != digest:
                        raise ValueError("source changed during copy or temporary checksum mismatch")
                    try:
                        rename_no_replace(temporary, destination)
                    except OSError as exc:
                        if exc.errno != errno.EEXIST:
                            raise
                        if destination.stat().st_size != source_sig[0] or sha256(destination) != digest:
                            report["conflicts"].append(relative)
                            blocked.add(relative)
                            continue
                    sync_directory(destination.parent)
                    if sha256(destination) != digest:
                        raise ValueError("destination checksum mismatch after rename")
                    report["copied"] += 1
                conn.execute(
                    "INSERT OR REPLACE INTO copies VALUES (?,?,?,?,?,?)",
                    (relative, *source_sig, *signature(destination), digest),
                )
            except (OSError, ValueError) as exc:
                blocked.add(relative)
                report["errors"].append({"path": relative, "error": str(exc)})
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
    return report
