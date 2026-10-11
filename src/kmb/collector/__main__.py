from __future__ import annotations

import argparse
import asyncio
import logging

from .app import run
from .config import CollectorConfig


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m kmb.collector")
    parser.add_argument("command", choices=("run", "check-config"), nargs="?", default="run")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = CollectorConfig.from_env()
    if args.command == "check-config":
        config.check()
        print("collector configuration is valid")
        return
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
