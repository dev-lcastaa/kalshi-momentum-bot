"""Synthetic 15-minute capture windows for tests (envelope rows in the normalizer's Parquet layout)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kmb.envelope.windows import PARQUET_COLUMNS, window_relpath

TICKER = "KXBTC15M-26JAN011200-00"
SERIES = "KXBTC15M"
OPEN_MS = 1_767_285_900_000  # 2026-01-01T16:45:00Z
CLOSE_MS = OPEN_MS + 900_000


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat().replace("+00:00", "Z")


class _Rows:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.kseq: dict[int, int] = {}

    def add(
        self,
        ts: int,
        stream: str,
        kind: str,
        msg_type: str | None,
        ticker: str | None,
        raw: Any = None,
        data: Any = None,
        sid: int | None = None,
        seq: int | None = None,
    ) -> None:
        n = len(self.rows)
        self.rows.append(
            {
                "ord": n,
                "capture_id": "cap-test",
                "local_event_seq": n + 1,
                "recv_wall_ms": ts,
                "stream": stream,
                "kind": kind,
                "source": "test",
                "conn_id": "c1",
                "msg_type": msg_type,
                "market_ticker": ticker,
                "sid": sid,
                "seq": seq,
                "raw": None if raw is None else json.dumps(raw),
                "data": None if data is None else json.dumps(data),
            }
        )

    def kalshi(self, ts: int, mtype: str, msg: dict[str, Any], sid: int = 1) -> None:
        seq = self.kseq[sid] = self.kseq.get(sid, 0) + 1
        frame = {"type": mtype, "sid": sid, "seq": seq, "msg": msg}
        self.add(ts, "kalshi_ws", "frame", mtype, msg.get("market_ticker"), raw=frame, sid=sid, seq=seq)


def make_rows(
    *, rising: bool = True, result: str = "yes", gap_at_ms: int | None = None
) -> list[dict[str, Any]]:
    """Events from open-60s to close+180s. The Coinbase price trends (+/-1 bp per second)."""
    start, end = OPEN_MS - 60_000, CLOSE_MS + 180_000
    r = _Rows()
    r.add(start, "kalshi_ws", "transport", None, None, data={"event": "connected", "conn_id": "c1"})
    market = {
        "ticker": TICKER,
        "series_ticker": SERIES,
        "open_time": _iso(OPEN_MS),
        "close_time": _iso(CLOSE_MS),
        "floor_strike": 97000,
        "status": "active",
        "price_level_structure": "linear_cent",
    }
    r.add(
        start + 1,
        "kalshi_rest",
        "rest",
        "rest.market",
        TICKER,
        raw={"market": market},
        data={"endpoint": "market", "method": "GET", "status": 200},
    )
    r.kalshi(
        start + 2,
        "orderbook_snapshot",
        {
            "market_ticker": TICKER,
            "yes_dollars_fp": [["0.4400", "100.00"], ["0.4500", "50.00"]],
            "no_dollars_fp": [["0.5200", "80.00"], ["0.5300", "40.00"]],
        },
    )
    r.kalshi(
        start + 3,
        "market_lifecycle_v2",
        {
            "market_ticker": TICKER,
            "event_type": "activated",
            "open_ts": OPEN_MS // 1000,
            "close_ts": CLOSE_MS // 1000,
            "additional_metadata": {"floor_strike": 97000},
        },
        sid=4,
    )
    price = 97_000.0
    step = 1 if rising else -1
    for i, ts in enumerate(range(start + 1000, end, 1000)):
        price *= 1 + step * 0.0001
        r.add(
            ts,
            "coinbase_ws",
            "frame",
            "ticker",
            "BTC-USD",
            raw={"type": "ticker", "product_id": "BTC-USD", "price": f"{price:.2f}"},
        )
        r.kalshi(
            ts + 100, "cfbenchmarks_value_5hz", {"index_id": "BRTI", "value_usd": f"{price + 3:.2f}"}, sid=9
        )
        if i % 5 == 0:
            r.kalshi(
                ts + 200,
                "orderbook_delta",
                {"market_ticker": TICKER, "price_dollars": "0.4400", "delta_fp": "1.00", "side": "yes"},
            )
        if i % 7 == 0:
            r.kalshi(
                ts + 300,
                "trade",
                {
                    "market_ticker": TICKER,
                    "yes_price_dollars": "0.4700",
                    "no_price_dollars": "0.5300",
                    "count_fp": "2.00",
                    "taker_side": "yes",
                },
                sid=2,
            )
            r.kalshi(
                ts + 301,
                "ticker",
                {
                    "market_ticker": TICKER,
                    "price_dollars": "0.4700",
                    "yes_bid_dollars": "0.4500",
                    "yes_ask_dollars": "0.4700",
                    "volume_fp": f"{100 + i}.00",
                    "open_interest_fp": "50.00",
                },
                sid=3,
            )
        if gap_at_ms is not None and ts == gap_at_ms:
            r.add(ts + 400, "kalshi_ws", "transport", None, None, data={"event": "gap", "sid": 1})
    r.kalshi(
        CLOSE_MS + 10_000,
        "market_lifecycle_v2",
        {"market_ticker": TICKER, "event_type": "determined", "result": result},
        sid=4,
    )
    r.rows.sort(key=lambda x: (x["recv_wall_ms"], x["ord"]))
    for i, row in enumerate(r.rows):
        row["ord"] = i
    return r.rows


def write_window(root: Path, rows: list[dict[str, Any]] | None = None, result: str = "yes") -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = rows if rows is not None else make_rows(result=result)
    date = _iso(OPEN_MS)[:10]
    parquet_rel, json_rel = window_relpath(SERIES, date, TICKER)
    path = root / parquet_rel
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([{c: row.get(c) for c in PARQUET_COLUMNS} for row in rows])
    pq.write_table(table, path)
    meta = {
        "format_version": 1,
        "window_id": TICKER,
        "market_ticker": TICKER,
        "series_ticker": SERIES,
        "open_time": _iso(OPEN_MS),
        "close_time": _iso(CLOSE_MS),
        "open_ms": OPEN_MS,
        "close_ms": CLOSE_MS,
        "start_ms": OPEN_MS - 60_000,
        "end_ms": CLOSE_MS + 180_000,
        "strike": "97000",
        "result": result,
        "event_count": len(rows),
        "gap_count": 0,
        "reconnect_count": 0,
        "quality": "ok",
        "parquet": parquet_rel,
        "sha256": "x",
        "size_bytes": path.stat().st_size,
    }
    (root / json_rel).write_text(json.dumps(meta), encoding="utf-8")
    return meta
