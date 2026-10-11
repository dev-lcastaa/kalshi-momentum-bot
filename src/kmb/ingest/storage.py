"""Fail-closed archive storage checks.

A mount-point directory still exists when the external drive is missing (fstab uses ``nofail``),
so "the directory exists" proves nothing. Before accepting writes we require:

1. A sentinel file at the archive root whose content equals the configured archive ID.
2. Optionally, the archive root sits on a mount whose fstype matches and whose source device is
   the one ``/dev/disk/by-uuid/<UUID>`` points to; the root's st_dev must also match that mount.
3. Free space above the configured safety threshold.
4. Optionally, a small probe write succeeds.
"""

from __future__ import annotations

import os
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

SENTINEL_NAME = ".kmb-archive-id"


@dataclass
class StorageStatus:
    ok: bool
    checks: dict[str, bool | None] = field(default_factory=dict)
    reason: str | None = None
    free_bytes: int | None = None
    checked_at: float = 0.0


@dataclass(frozen=True)
class MountEntry:
    major_minor: tuple[int, int]
    mount_point: str
    fstype: str
    source: str


def _unescape(value: str) -> str:
    # mountinfo escapes space, tab, newline and backslash as octal sequences.
    out, i = [], 0
    while i < len(value):
        if value[i] == "\\" and value[i + 1 : i + 4].isdigit() and len(value[i + 1 : i + 4]) == 3:
            out.append(chr(int(value[i + 1 : i + 4], 8)))
            i += 4
        else:
            out.append(value[i])
            i += 1
    return "".join(out)


def parse_mountinfo(text: str) -> list[MountEntry]:
    entries = []
    for line in text.splitlines():
        parts = line.split()
        if "-" not in parts or len(parts) < 10:
            continue
        sep = parts.index("-")
        major, minor = parts[2].split(":")
        entries.append(
            MountEntry(
                major_minor=(int(major), int(minor)),
                mount_point=_unescape(parts[4]),
                fstype=parts[sep + 1],
                source=_unescape(parts[sep + 2]),
            )
        )
    return entries


def find_mount(entries: list[MountEntry], path: str) -> MountEntry | None:
    best: MountEntry | None = None
    for entry in entries:
        mp = entry.mount_point.rstrip("/") or "/"
        if path == mp or path.startswith(mp + "/") or mp == "/":
            if best is None or len(mp) >= len(best.mount_point.rstrip("/") or "/"):
                best = entry
    return best


def _default_dev_of(path: Path) -> tuple[int, int]:
    st = os.stat(path)
    return os.major(st.st_dev), os.minor(st.st_dev)


class StorageGuard:
    def __init__(
        self,
        root: Path,
        archive_id: str,
        *,
        min_free_bytes: int,
        expected_fs_uuid: str | None = None,
        expected_fstype: str | None = None,
        by_uuid_dir: Path = Path("/dev/disk/by-uuid"),
        mountinfo_path: Path = Path("/proc/self/mountinfo"),
        dev_of: Callable[[Path], tuple[int, int]] = _default_dev_of,
        readlink: Callable[[Path], str] = os.readlink,
        disk_usage: Callable[[Path], object] = shutil.disk_usage,
        cache_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.root = root
        self.archive_id = archive_id
        self.min_free_bytes = min_free_bytes
        self.expected_fs_uuid = expected_fs_uuid
        self.expected_fstype = expected_fstype
        self.by_uuid_dir = by_uuid_dir
        self.mountinfo_path = mountinfo_path
        self._dev_of = dev_of
        self._readlink = readlink
        self._disk_usage = disk_usage
        self._cache_seconds = cache_seconds
        self._clock = clock
        self._cached: StorageStatus | None = None

    def check(self, *, probe_write: bool = False, use_cache: bool = False) -> StorageStatus:
        now = self._clock()
        if use_cache and self._cached and now - self._cached.checked_at < self._cache_seconds:
            return self._cached
        status = self._check(probe_write)
        status.checked_at = now
        self._cached = status
        return status

    def _check(self, probe_write: bool) -> StorageStatus:
        checks: dict[str, bool | None] = {
            "root_present": None,
            "sentinel": None,
            "mount_identity": None,
            "free_space": None,
            "writable": None,
        }

        def fail(name: str, reason: str, free: int | None = None) -> StorageStatus:
            checks[name] = False
            return StorageStatus(ok=False, checks=checks, reason=reason, free_bytes=free)

        if not self.root.is_dir():
            return fail("root_present", f"archive root missing: {self.root}")
        checks["root_present"] = True

        try:
            content = (self.root / SENTINEL_NAME).read_text(encoding="utf-8").strip()
        except OSError:
            return fail("sentinel", "archive sentinel missing (drive not mounted or wrong drive?)")
        if content != self.archive_id:
            return fail("sentinel", "archive sentinel does not match configured archive ID")
        checks["sentinel"] = True

        if self.expected_fs_uuid or self.expected_fstype:
            reason = self.check_mount_identity()
            if reason:
                return fail("mount_identity", reason)
            checks["mount_identity"] = True

        try:
            free = int(self._disk_usage(self.root).free)
        except OSError as exc:
            return fail("free_space", f"cannot read free space: {exc}")
        if free < self.min_free_bytes:
            return fail("free_space", f"free space {free} below minimum {self.min_free_bytes}", free)
        checks["free_space"] = True

        if probe_write:
            probe = self.root / f".kmb-probe-{os.getpid()}"
            try:
                with open(probe, "wb") as fh:
                    fh.write(b"probe")
                    fh.flush()
                    os.fsync(fh.fileno())
                probe.unlink()
            except OSError as exc:
                return fail("writable", f"archive not writable: {exc}", free)
            checks["writable"] = True

        return StorageStatus(ok=True, checks=checks, free_bytes=free)

    def check_mount_identity(self) -> str | None:
        try:
            entries = parse_mountinfo(self.mountinfo_path.read_text(encoding="utf-8"))
        except OSError as exc:
            return f"cannot read mountinfo: {exc}"
        root = os.path.realpath(self.root).replace("\\", "/")
        entry = find_mount(entries, root)
        if entry is None or entry.mount_point == "/":
            return "archive root is not on a dedicated mount (would write to the root filesystem)"
        if self.expected_fstype and entry.fstype != self.expected_fstype:
            return f"archive fstype {entry.fstype!r} != expected {self.expected_fstype!r}"
        try:
            if self._dev_of(self.root) != entry.major_minor:
                return "archive root device does not match its mount entry"
        except OSError as exc:
            return f"cannot stat archive root: {exc}"
        if self.expected_fs_uuid:
            link = self.by_uuid_dir / self.expected_fs_uuid
            try:
                target = str(self._readlink(link))
            except OSError:
                return f"filesystem UUID {self.expected_fs_uuid} not present (drive missing?)"
            expected_dev = os.path.basename(target.replace("\\", "/"))
            actual_dev = os.path.basename(entry.source)
            if expected_dev != actual_dev:
                return f"archive is on {actual_dev}, but UUID {self.expected_fs_uuid} is {expected_dev}"
        return None
