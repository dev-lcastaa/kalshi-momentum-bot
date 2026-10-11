"""Normalizer settings. State and receipt SQLite databases belong on ext4."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from kmb.ingest.storage import SENTINEL_NAME


@dataclass(frozen=True)
class Config:
    archive_root: Path = Path("/mnt/storage1/kalshi-archive")
    derived_root: Path = Path("/mnt/storage1/kalshi-derived")
    receipts_path: Path = Path("/var/lib/kmb-ingest/ingest.sqlite")
    state_path: Path = Path("/var/lib/kmb-normalizer/state.sqlite")
    settle_lag_ms: int = 20 * 60 * 1000
    archive_id: str = "kalshi-archive-storage1-C174-8D73"

    @classmethod
    def from_env(cls):
        return cls(
            archive_root=Path(os.getenv("KMB_NORMALIZER_ARCHIVE_ROOT", str(cls.archive_root))),
            derived_root=Path(os.getenv("KMB_NORMALIZER_DERIVED_ROOT", str(cls.derived_root))),
            receipts_path=Path(os.getenv("KMB_NORMALIZER_RECEIPTS_PATH", str(cls.receipts_path))),
            state_path=Path(os.getenv("KMB_NORMALIZER_STATE_PATH", str(cls.state_path))),
            settle_lag_ms=int(os.getenv("KMB_NORMALIZER_SETTLE_LAG_SECONDS", "1200")) * 1000,
            archive_id=os.getenv("KMB_NORMALIZER_ARCHIVE_ID", cls.archive_id),
        )

    def check_storage(self):
        if (self.archive_root / SENTINEL_NAME).read_text(encoding="utf-8").strip() != self.archive_id:
            raise RuntimeError("archive sentinel does not match")
        if self.derived_root.resolve().parent != self.archive_root.resolve().parent:
            raise RuntimeError("derived root must share the archive's sentinel-guarded parent mount")
        if self.settle_lag_ms < 0:
            raise ValueError("settle lag cannot be negative")
        self.derived_root.mkdir(parents=True, exist_ok=True)
