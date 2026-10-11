"""kmb-ingest CLI.

python -m kmb.ingest serve          # run the API (default)
python -m kmb.ingest init-archive   # write the sentinel; only after verifying the mount by hand
python -m kmb.ingest reconcile      # crash recovery without serving
python -m kmb.ingest verify-all     # re-hash every committed batch (slow)
"""

from __future__ import annotations

import argparse
import json
import sys

from kmb.ingest.app import build_archive, create_app
from kmb.ingest.config import IngestConfig
from kmb.ingest.storage import SENTINEL_NAME
from kmb.logs import setup_logging


def _init_archive(cfg: IngestConfig) -> int:
    root = cfg.archive_root
    if not root.is_dir():
        print(f"archive root {root} does not exist; create it on the mounted drive first", file=sys.stderr)
        return 2
    sentinel = root / SENTINEL_NAME
    if sentinel.exists():
        current = sentinel.read_text(encoding="utf-8").strip()
        if current != cfg.archive_id:
            print(f"sentinel exists with a different archive ID ({current!r}); refusing", file=sys.stderr)
            return 2
        print("sentinel already present")
        return 0
    archive = build_archive(cfg)
    # Check mount identity before writing the sentinel (sentinel check is expected to fail here).
    reason = archive.guard.check_mount_identity() if (cfg.expected_fs_uuid or cfg.expected_fstype) else None
    if reason:
        print(f"mount identity check failed: {reason}", file=sys.stderr)
        return 2
    sentinel.write_text(cfg.archive_id + "\n", encoding="utf-8")
    print(f"wrote {sentinel}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kmb-ingest")
    parser.add_argument(
        "command", nargs="?", default="serve", choices=["serve", "init-archive", "reconcile", "verify-all"]
    )
    args = parser.parse_args(argv)
    cfg = IngestConfig.from_env()
    setup_logging(cfg.log_level)

    if args.command == "init-archive":
        return _init_archive(cfg)
    if args.command in ("reconcile", "verify-all"):
        archive = build_archive(cfg)
        status = archive.guard.check(probe_write=True)
        if not status.ok:
            print(f"archive not ready: {status.reason}", file=sys.stderr)
            return 2
        result = archive.reconcile() if args.command == "reconcile" else archive.verify_all()
        print(json.dumps(result, indent=2))
        return 0

    import uvicorn

    uvicorn.run(create_app(cfg), host=cfg.host, port=cfg.port, log_config=None, access_log=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
