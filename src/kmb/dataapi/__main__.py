from __future__ import annotations

import argparse
import os

import uvicorn

from kmb.dataapi.app import create_app


def main():
    parser = argparse.ArgumentParser(description="Read-only replay data API")
    parser.add_argument("command", choices=["serve"])
    parser.parse_args()
    uvicorn.run(
        create_app(),
        host=os.getenv("KMB_DATAAPI_HOST", "0.0.0.0"),
        port=int(os.getenv("KMB_DATAAPI_PORT", "8702")),
    )


if __name__ == "__main__":
    main()
