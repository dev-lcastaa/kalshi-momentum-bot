from __future__ import annotations

import argparse
import json
import logging
import os
import time

from kmb.normalizer.config import Config
from kmb.normalizer.index import FileIndex
from kmb.normalizer.windows import run_once


def main():
    parser = argparse.ArgumentParser(description="Receipt-verified raw archive to replay Parquet")
    sub = parser.add_subparsers(dest="command", required=True)
    once = sub.add_parser("run-once")
    once.add_argument("--rebuild", action="store_true", help="explicitly rebuild all ready windows")
    loop = sub.add_parser("loop")
    loop.add_argument("--interval", type=int, default=900)
    sub.add_parser("rebuild").add_argument("window_id")
    sub.add_parser("status")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    cfg = Config.from_env()
    if args.command == "status":
        index = FileIndex(cfg.state_path, cfg.archive_root, cfg.receipts_path)
        try:
            print(
                json.dumps(
                    {
                        "batches": index.conn.execute(
                            "SELECT COUNT(*) FROM files WHERE active=1 AND verified=1"
                        ).fetchone()[0],
                        "markets": index.conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0],
                        "complete_windows": sum(1 for _ in cfg.derived_root.rglob("*.window.json")),
                    }
                )
            )
        finally:
            index.close()
        return
    if args.command == "loop":
        if args.interval <= 0:
            parser.error("interval must be positive")
        if hasattr(os, "nice"):
            os.nice(10)
        while True:
            try:
                print(json.dumps(run_once(cfg)), flush=True)
            except Exception:
                logging.exception("normalizer cycle failed; retrying next interval")
            time.sleep(args.interval)
    elif args.command == "run-once" and args.rebuild:
        # Build discovery first, then invoke the same serialized writer for each ready window.
        run_once(cfg)
        index = FileIndex(cfg.state_path, cfg.archive_root, cfg.receipts_path)
        try:
            ready = [
                m["ticker"]
                for m in index.markets()
                if m.get("close_ms", float("inf")) + 180000 + cfg.settle_lag_ms < int(time.time() * 1000)
                and "open_ms" in m
            ]
        finally:
            index.close()
        for ticker in ready:
            print(json.dumps(run_once(cfg, rebuild=ticker)))
    else:
        print(json.dumps(run_once(cfg, rebuild=args.window_id if args.command == "rebuild" else None)))


if __name__ == "__main__":
    main()
