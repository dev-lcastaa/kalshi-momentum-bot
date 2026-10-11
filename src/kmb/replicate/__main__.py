from __future__ import annotations

import argparse
import json

from kmb.replicate.copy import Config, run


def main():
    parser = argparse.ArgumentParser(description="Verified append-only storage mirror")
    parser.add_argument("command", choices=["run"])
    parser.parse_args()
    report = run(Config.from_env())
    print(json.dumps(report))
    if report["conflicts"] or report["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
