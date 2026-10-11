from __future__ import annotations

from pathlib import Path

import pytest

from kmb.ingest.config import ConfigError, IngestConfig
from kmb.ingest.storage import SENTINEL_NAME, StorageGuard, find_mount, parse_mountinfo
from tests.ingest.conftest import ARCHIVE_ID, FakeUsage

# Shape of a bind-mounted exFAT folder as seen inside the ingest container.
MOUNTINFO = """\
600 500 0:52 / / rw,relatime - overlay overlay rw
610 600 179:2 /var/lib/kmb-ingest /state rw,noatime - ext4 /dev/mmcblk0p2 rw
620 600 8:1 /kalshi-archive {root} rw,relatime - exfat /dev/sda1 rw,uid=1000,gid=1000
630 600 8:1 /with\\040space /mnt/with\\040space rw - exfat /dev/sda1 rw
"""


def _guard(
    root: Path,
    mountinfo: str,
    *,
    uuid_target: str | None = "../../sda1",
    dev: tuple[int, int] = (8, 1),
    fstype: str | None = "exfat",
) -> StorageGuard:
    mi = root.parent / "mountinfo"
    mi.write_text(mountinfo.format(root=root.as_posix()), encoding="utf-8")

    def readlink(_p: Path) -> str:
        if uuid_target is None:
            raise FileNotFoundError("no such uuid")
        return uuid_target

    return StorageGuard(
        root,
        ARCHIVE_ID,
        min_free_bytes=1,
        expected_fs_uuid="C174-8D73",
        expected_fstype=fstype,
        mountinfo_path=mi,
        dev_of=lambda _p: dev,
        readlink=readlink,
        disk_usage=lambda _p: FakeUsage(10**12),
    )


@pytest.fixture
def root(tmp_path: Path, monkeypatch) -> Path:
    r = tmp_path / "data" / "kalshi"
    r.mkdir(parents=True)
    (r / SENTINEL_NAME).write_text(ARCHIVE_ID, encoding="utf-8")
    # realpath on Windows returns a drive path; keep the comparison posix-style.
    monkeypatch.setattr("kmb.ingest.storage.os.path.realpath", lambda p: Path(p).as_posix())
    return r


def test_parse_mountinfo_unescapes():
    entries = parse_mountinfo(MOUNTINFO.format(root="/data/kalshi"))
    assert entries[2].fstype == "exfat"
    assert entries[2].source == "/dev/sda1"
    assert entries[3].mount_point == "/mnt/with space"
    assert find_mount(entries, "/data/kalshi/raw").mount_point == "/data/kalshi"
    assert find_mount(entries, "/etc").mount_point == "/"


def test_mount_identity_ok(root):
    status = _guard(root, MOUNTINFO).check(probe_write=True)
    assert status.ok, status.reason
    assert status.checks["mount_identity"] is True


def test_fails_when_uuid_missing(root):
    status = _guard(root, MOUNTINFO, uuid_target=None).check()
    assert not status.ok
    assert "C174-8D73" in status.reason


def test_fails_when_uuid_points_to_other_device(root):
    status = _guard(root, MOUNTINFO, uuid_target="../../sdb1").check()
    assert not status.ok
    assert "sda1" in status.reason and "sdb1" in status.reason


def test_fails_when_archive_on_root_filesystem(root):
    only_root = "600 500 0:52 / / rw,relatime - overlay overlay rw\n"
    status = _guard(root, only_root).check()
    assert not status.ok
    assert "root filesystem" in status.reason


def test_fails_on_fstype_mismatch(root):
    status = _guard(root, MOUNTINFO.replace("exfat /dev/sda1 rw,uid", "ext4 /dev/sda1 rw,uid")).check()
    assert not status.ok
    assert "fstype" in status.reason


def test_fails_when_root_device_differs_from_mount(root):
    status = _guard(root, MOUNTINFO, dev=(179, 2)).check()
    assert not status.ok


def test_fails_when_root_missing(tmp_path):
    guard = StorageGuard(tmp_path / "nope", ARCHIVE_ID, min_free_bytes=1)
    status = guard.check()
    assert not status.ok
    assert status.checks["root_present"] is False


def test_config_requires_strong_token(tmp_path):
    base = {
        "INGEST_ARCHIVE_ROOT": "/data/kalshi",
        "INGEST_DB_PATH": "/state/x.sqlite",
        "INGEST_ARCHIVE_ID": "a",
    }
    with pytest.raises(ConfigError):
        IngestConfig.from_env(base | {"INGEST_TOKEN": "short"})
    token_file = tmp_path / "tok"
    token_file.write_text("s" * 48 + "\n", encoding="utf-8")
    cfg = IngestConfig.from_env(base | {"INGEST_TOKEN_FILE": str(token_file)})
    assert cfg.token == "s" * 48
    assert "s" * 48 not in repr(cfg)
    with pytest.raises(ConfigError):
        IngestConfig.from_env({"INGEST_TOKEN": "x" * 40})
