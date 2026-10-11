"""Build the exact replay-window contract in docs/data-formats.md."""

from __future__ import annotations

import heapq
import json
import logging
import os
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from kmb.db import init_db, make_engine
from kmb.db.models import CaptureWindow, MarketResult
from kmb.envelope.event import ms_to_utc_iso
from kmb.envelope.windows import (
    PARQUET_COLUMNS,
    POST_CLOSE_MS,
    PRE_OPEN_MS,
    WINDOW_FORMAT_VERSION,
    coin_for_ticker,
    window_relpath,
)
from kmb.normalizer.config import Config
from kmb.normalizer.index import WINDOW_ID, FileIndex, events, sha256, under

LOG = logging.getLogger(__name__)
INT_COLUMNS = {"ord", "local_event_seq", "recv_wall_ms", "sid", "seq"}
SCHEMA = pa.schema(
    [
        pa.field(
            name,
            pa.int64() if name in INT_COLUMNS else pa.string(),
            nullable=name in {"conn_id", "msg_type", "market_ticker", "sid", "seq", "raw", "data"},
        )
        for name in PARQUET_COLUMNS
    ]
)


def timestamp(value) -> int:
    if isinstance(value, (int, float)):
        return int(value * 1000)
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError("market time must include timezone")
    return int(dt.timestamp() * 1000)


def market_metadata(event: dict) -> dict | None:
    msg_type = event.get("msg_type")
    if msg_type not in {"rest.market", "market_lifecycle_v2"}:
        return None
    body = json.loads(event["raw"])
    if not isinstance(body, dict):
        return None
    msg = body.get("market") if msg_type == "rest.market" else body.get("msg")
    if not isinstance(msg, dict):
        return None
    ticker = msg.get("ticker") or msg.get("market_ticker") or event.get("market_ticker")
    if not isinstance(ticker, str) or not WINDOW_ID.fullmatch(ticker) or not coin_for_ticker(ticker):
        return None
    metadata = {"ticker": ticker}
    for dest, names in {"open_ms": ("open_time", "open_ts"), "close_ms": ("close_time", "close_ts")}.items():
        for name in names:
            if msg.get(name) is not None:
                metadata[dest] = timestamp(msg[name])
                break
    additional = msg.get("additional_metadata") or {}
    strike = msg.get("floor_strike", additional.get("floor_strike"))
    if strike is not None:
        metadata["strike"] = str(strike)
    if msg.get("result") in {"yes", "no", "void"}:
        metadata["result"] = msg["result"]
    return metadata


def sync_directory(path: Path):
    if os.name == "posix":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def atomic_json(path: Path, data: dict):
    temporary = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as fh:
            json.dump(data, fh, separators=(",", ":"), sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def stream_holes(index: FileIndex, captures: set[str], open_ms: int, close_ms: int) -> bool:
    # Pending/failed receipts expose missing trailing uploads, which sequence adjacency alone cannot see.
    import sqlite3

    with sqlite3.connect(index.receipts.resolve().as_uri() + "?mode=ro", uri=True) as conn:
        for first, last in conn.execute(
            "SELECT first_recv_utc,last_recv_utc FROM batches WHERE status != 'committed'"
        ):
            if timestamp(first) <= close_ms and timestamp(last) >= open_ms:
                return True
    for capture in captures:
        previous = {}
        for row in index.conn.execute(
            "SELECT * FROM files WHERE active=1 AND capture_id=? ORDER BY stream,first_stream_seq", (capture,)
        ):
            prev = previous.get(row["stream"])
            if prev is None:
                if row["first_stream_seq"] > 0 and open_ms <= row["min_ms"] <= close_ms:
                    return True
            elif row["first_stream_seq"] > prev["last_stream_seq"] + 1:
                lo, hi = sorted((prev["max_ms"], row["min_ms"]))
                if lo <= close_ms and hi >= open_ms:
                    return True
            if not row["verified"] and row["min_ms"] <= close_ms and row["max_ms"] >= open_ms:
                return True
            previous[row["stream"]] = row
    return False


def quality(
    all_events: Iterable[dict], selected: Callable[[dict], bool], holes: bool, open_ms: int, close_ms: int
):
    gaps, disconnects = 0, 0
    last_seq, pending = {}, set()
    has_book = False
    for event in all_events:
        inside = open_ms <= event["recv_wall_ms"] <= close_ms
        key = (event["capture_id"], event["stream"])
        kind, msg = event.get("kind"), event.get("msg_type")
        if kind == "transport" and inside:
            action = (event.get("data") or {}).get("event")
            if action in {"gap", "disconnected"}:
                gaps += action == "gap"
                disconnects += action == "disconnected"
                pending.add(key)
        if event["stream"] == "kalshi_ws" and kind == "frame" and event.get("seq") is not None:
            seq_key = (event["capture_id"], event.get("conn_id"), event.get("sid"))
            prev = last_seq.get(seq_key)
            if inside and prev is not None and event["seq"] > prev + 1:
                gaps += 1
                pending.add(key)
            last_seq[seq_key] = event["seq"]
        if selected(event):
            if msg in {"orderbook_snapshot", "orderbook_delta", "rest.orderbook"}:
                has_book = True
            if msg in {"orderbook_snapshot", "rest.orderbook"}:
                pending.discard((event["capture_id"], "kalshi_ws"))
                pending.discard(key)
    rating = "bad" if holes or not has_book or pending else "degraded" if gaps or disconnects else "ok"
    return rating, gaps, disconnects


def window_events(index: FileIndex, rows, start: int, end: int) -> Iterator[dict]:
    """Merge stream sequences without loading a whole multi-coin window into RAM."""
    captures = index.capture_starts()
    grouped = {}
    for row in rows:
        if row["verified"]:
            grouped.setdefault(row["capture_id"], {}).setdefault(row["stream"], []).append(row)

    def stream_events(files):
        for row in sorted(files, key=lambda r: r["first_stream_seq"]):
            path = under(index.archive, row["path"])
            if sha256(path) != row["sha256"]:
                raise RuntimeError(f"archive changed after indexing: {row['path']}")
            yield from (event for event in events(path) if start <= event["recv_wall_ms"] <= end)

    for capture in sorted(grouped, key=lambda c: (captures[c], c)):
        yield from heapq.merge(
            *(stream_events(files) for files in grouped[capture].values()),
            key=lambda event: event["local_event_seq"],
        )


def upsert_window(meta: dict, engine):
    with Session(engine) as session:
        row = session.get(CaptureWindow, meta["window_id"])
        if row is None:
            row = CaptureWindow(window_id=meta["window_id"])
            session.add(row)
        for name in (
            "market_ticker",
            "series_ticker",
            "result",
            "event_count",
            "gap_count",
            "quality",
            "sha256",
            "size_bytes",
        ):
            setattr(row, name, meta.get(name))
        for name in ("open_time", "close_time", "built_at"):
            setattr(row, name, datetime.fromisoformat(meta[name]))
        row.strike = Decimal(meta["strike"]) if meta.get("strike") is not None else None
        row.path, row.meta = meta["parquet"], meta
        if meta.get("result"):
            result = session.get(MarketResult, meta["market_ticker"])
            if result is None:
                result = MarketResult(ticker=meta["market_ticker"])
                session.add(result)
            result.series_ticker, result.result = meta["series_ticker"], meta["result"]
            result.strike, result.close_time, result.raw = row.strike, row.close_time, meta
        session.commit()


def build_window(index: FileIndex, cfg: Config, market: dict, *, rebuild=False) -> dict | None:
    ticker = market["ticker"]
    coin = coin_for_ticker(ticker)
    if not WINDOW_ID.fullmatch(ticker) or coin is None:
        raise ValueError("invalid market ticker")
    open_ms, close_ms = market["open_ms"], market["close_ms"]
    if close_ms <= open_ms:
        raise ValueError("market closes before it opens")
    date = datetime.fromtimestamp(open_ms / 1000, UTC).date().isoformat()
    rel_parquet, rel_json = window_relpath(coin.series, date, ticker)
    parquet_path, sidecar = under(cfg.derived_root, rel_parquet), under(cfg.derived_root, rel_json)
    if sidecar.exists() and not rebuild:
        return None
    start, end = open_ms - PRE_OPEN_MS, close_ms + POST_CLOSE_MS
    rows = index.files(start, end)
    holes = any(
        not row["verified"] and row["min_ms"] <= close_ms and row["max_ms"] >= open_ms for row in rows
    )
    captures = index.capture_starts()

    def wanted(e):
        return (
            e.get("market_ticker") in {ticker, coin.cf_index, coin.coinbase_pair}
            or e.get("kind") in {"transport", "control"}
            or (e["stream"] == "kalshi_ws" and e.get("kind") == "frame" and not e.get("market_ticker"))
        )

    contributing = {row["capture_id"] for row in rows if row["verified"]}
    holes |= stream_holes(index, contributing, open_ms, close_ms)
    counts = Counter()
    selected_captures = set()
    event_count = 0
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = parquet_path.with_name(parquet_path.name + f".{uuid.uuid4().hex}.tmp")
    try:
        with pq.ParquetWriter(temporary, SCHEMA, compression="zstd") as writer:

            def scan():
                nonlocal event_count
                batch = []
                for event in window_events(index, rows, start, end):
                    if wanted(event):
                        row = {name: event.get(name) for name in PARQUET_COLUMNS}
                        row["ord"] = event_count
                        if row["data"] is not None:
                            row["data"] = json.dumps(row["data"], separators=(",", ":"), ensure_ascii=False)
                        batch.append(row)
                        event_count += 1
                        counts[event.get("msg_type") or event["kind"]] += 1
                        selected_captures.add(event["capture_id"])
                        if len(batch) >= 4096:
                            writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
                            batch.clear()
                    yield event
                if batch:
                    writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))

            rating, gaps, reconnects = quality(scan(), wanted, holes, open_ms, close_ms)
        with temporary.open("rb+") as fh:
            os.fsync(fh.fileno())
        # Explicit rebuild revokes the completion marker before replacing its data file.
        if rebuild and sidecar.exists():
            sidecar.unlink()
            sync_directory(sidecar.parent)
        os.replace(temporary, parquet_path)
        sync_directory(parquet_path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    meta = {
        "format_version": WINDOW_FORMAT_VERSION,
        "window_id": ticker,
        "market_ticker": ticker,
        "series_ticker": coin.series,
        "open_time": ms_to_utc_iso(open_ms),
        "close_time": ms_to_utc_iso(close_ms),
        "open_ms": open_ms,
        "close_ms": close_ms,
        "start_ms": start,
        "end_ms": end,
        "strike": market.get("strike"),
        "result": market.get("result"),
        "event_count": event_count,
        "gap_count": gaps,
        "reconnect_count": reconnects,
        "quality": rating,
        "counts": dict(counts),
        "captures": sorted(selected_captures, key=lambda c: (captures[c], c)),
        "parquet": rel_parquet,
        "sha256": sha256(parquet_path),
        "size_bytes": parquet_path.stat().st_size,
        "built_at": ms_to_utc_iso(int(time.time() * 1000)),
    }
    atomic_json(sidecar, meta)
    return meta


def run_once(cfg: Config, *, now_ms=None, rebuild: str | None = None, engine=None) -> dict:
    cfg.check_storage()
    index = FileIndex(cfg.state_path, cfg.archive_root, cfg.receipts_path)
    own_engine = False
    try:
        # Serializes loop/run-once/rebuild across processes; no concurrent completion-marker writers.
        index.conn.execute("BEGIN IMMEDIATE")
        report = index.update()
        now = int(time.time() * 1000) if now_ms is None else now_ms
        built = []
        if engine is None:
            try:
                engine = (
                    make_engine(connect_args={"connect_timeout": 3})
                    if not os.getenv("KMB_DATABASE_URL", "").startswith("sqlite")
                    else make_engine()
                )
                own_engine = True
                init_db(engine)
            except Exception as exc:
                LOG.info(
                    "database unavailable/unconfigured (%s); archive builds continue", type(exc).__name__
                )
                if engine is not None:
                    engine.dispose()
                engine = None
        found = False
        for market in index.markets():
            if rebuild and market["ticker"] != rebuild:
                continue
            found = True
            if "open_ms" not in market or "close_ms" not in market:
                continue
            if not rebuild and not window_ready(market, cfg, now):
                continue
            meta = build_window(index, cfg, market, rebuild=bool(rebuild))
            if meta:
                built.append(meta["window_id"])
            elif engine is not None:
                date = datetime.fromtimestamp(market["open_ms"] / 1000, UTC).date().isoformat()
                _, rel = window_relpath(coin_for_ticker(market["ticker"]).series, date, market["ticker"])
                meta = json.loads(under(cfg.derived_root, rel).read_text(encoding="utf-8"))
            if meta and engine is not None:
                try:
                    upsert_window(meta, engine)
                except Exception as exc:
                    LOG.warning("window DB upsert deferred (%s)", type(exc).__name__)
        if rebuild and not found:
            raise ValueError(f"unknown window: {rebuild}")
        index.conn.commit()
        return {**report, "built": built}
    finally:
        index.close()
        if own_engine and engine is not None:
            engine.dispose()


def window_ready(market: dict, cfg: Config, now_ms: int) -> bool:
    if "open_ms" not in market or "close_ms" not in market:
        return False
    lag = 0 if market.get("result") in ("yes", "no", "void") else cfg.settle_lag_ms
    return now_ms > market["close_ms"] + POST_CLOSE_MS + lag
