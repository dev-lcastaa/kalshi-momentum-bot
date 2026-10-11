from __future__ import annotations

import argparse
import json

from kmb.fidelity.events import events_from_parquet
from kmb.fidelity.report import run_fidelity


def main() -> None:
    parser = argparse.ArgumentParser(description="Run market-data fidelity checks for a window Parquet file")
    parser.add_argument("window_parquet")
    args = parser.parse_args()
    print(json.dumps(run_fidelity(events_from_parquet(args.window_parquet)), indent=2))


if __name__ == "__main__":
    main()
