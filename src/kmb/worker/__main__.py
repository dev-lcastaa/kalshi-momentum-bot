"""``python -m kmb.worker run [--once]``"""

from __future__ import annotations

import argparse
import logging


def main() -> None:
    p = argparse.ArgumentParser(prog="kmb.worker")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="claim and run jobs until stopped")
    r.add_argument("--once", action="store_true", help="process at most one job, then exit")
    r.add_argument("--log-level", default="INFO")
    args = p.parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from kmb.worker.main import main as run

    run(once=args.once)


if __name__ == "__main__":
    main()
