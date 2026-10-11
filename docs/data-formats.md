# Capture data formats

## Envelope (one recorded event)

Built by `kmb.envelope.make_envelope`; fields: `schema_version, capture_id, stream, stream_seq, local_event_seq,
kind, received_at_utc, recv_wall_ms, recv_mono_ns, source, conn_id, market_ticker, msg_type, sid, seq, raw, data`.

| stream | kind | source | raw | data | msg_type |
|---|---|---|---|---|---|
| `kalshi_ws` | `frame` | `kalshi` | exact WS text | null | WS `type` (`orderbook_snapshot`, `orderbook_delta`, `ticker`, `trade`, `market_lifecycle_v2`, `event_lifecycle`, `cfbenchmarks_value`, `cfbenchmarks_value_5hz`, `subscribed`, `ok`, `error`) |
| `kalshi_ws` | `transport` | `collector` | null | `{event: connected\|disconnected\|subscribed\|gap\|resync, conn_id, ...}` | null |
| `kalshi_rest` | `rest` | `kalshi` | exact response body | `{endpoint, method, path, params, status, sent_wall_ms, recv_wall_ms}` | `rest.<endpoint>` |
| `coinbase_ws` | `frame` | `coinbase` | exact WS text | null | Coinbase `type` (`match`, `last_match`, `ticker`, `subscriptions`, `error`) |
| `coinbase_ws` | `transport` | `collector` | null | as above | null |
| `control` | `control` | `collector` | null | `{event: session_start\|session_end\|clock\|discovery\|spool_drop, ...}` | null |

REST `endpoint` values: `orderbook` (GET /markets/{t}/orderbook; `market_ticker` set explicitly),
`market` (GET /markets/{t}), `markets` (list), `series`, `event`. `market_ticker` for CF frames is the index id
(`BRTI`, `ETHUSD_RTI`); for Coinbase frames the product id (`BTC-USD`, `ETH-USD`).

Coin mapping: `KXBTC15M` ↔ index `BRTI` ↔ Coinbase `BTC-USD`; `KXETH15M` ↔ `ETHUSD_RTI` ↔ `ETH-USD`
(`kmb.envelope.windows.COINS`).

## Window (replay unit)

`window_id` = the market ticker (each 15-minute market is one window). Built by the normalizer on node001:

```
/mnt/storage1/kalshi-derived/windows/<series>/<YYYY-MM-DD of open, UTC>/<ticker>.parquet
/mnt/storage1/kalshi-derived/windows/<series>/<YYYY-MM-DD>/<ticker>.window.json
```

Time range: `start_ms = open_ms - 60_000` to `end_ms = close_ms + 180_000`. Contents: every envelope in range
whose `market_ticker` is the window's ticker, the coin's CF index and Coinbase pair, plus every Kalshi WS frame
without a ticker, every transport event and every control event.

Parquet columns (rows sorted in replay order; `ord` = 0..n-1):

| column | type |
|---|---|
| `ord` | int64 |
| `capture_id` | string |
| `local_event_seq` | int64 |
| `recv_wall_ms` | int64 |
| `stream` | string |
| `kind` | string |
| `source` | string |
| `conn_id` | string (nullable) |
| `msg_type` | string (nullable) |
| `market_ticker` | string (nullable) |
| `sid` | int64 (nullable) |
| `seq` | int64 (nullable) |
| `raw` | string (nullable) |
| `data` | string (JSON text, nullable) |

Replay order: captures ordered by their first `recv_wall_ms`, then `local_event_seq` within a capture.

The normalizer merges archived streams and writes at most 4096 rows per batch.
It checks for ready recordings every 60 seconds. Confirmed outcomes are eligible
after the three-minute post-close tail; unconfirmed outcomes retain the additional
20-minute settlement wait. Backlog processing and uploads can add delay.

Simulation workers spool downloaded Parquet to a temporary file and replay it in
4096-row batches without retaining the full recording in their event cache.
Streaming requires strictly increasing `ord`; invalid ordering fails the run
rather than silently changing replay order. Interactive replay still uses an
in-memory event list for seeking, but decodes Parquet in batches.

`window.json` (write-once, rebuilt only by an explicit `--rebuild`):

```json
{
  "format_version": 1,
  "window_id": "KXBTC15M-26JAN011200-00",
  "market_ticker": "KXBTC15M-26JAN011200-00",
  "series_ticker": "KXBTC15M",
  "open_time": "2026-01-01T16:45:00Z", "close_time": "2026-01-01T17:00:00Z",
  "open_ms": 0, "close_ms": 0, "start_ms": 0, "end_ms": 0,
  "strike": "97000.00" , "result": "yes",
  "event_count": 0, "gap_count": 0, "reconnect_count": 0,
  "quality": "ok",
  "counts": {"orderbook_delta": 0, "trade": 0},
  "captures": ["..."],
  "parquet": "windows/KXBTC15M/2026-01-01/KXBTC15M-26JAN011200-00.parquet",
  "sha256": "...", "size_bytes": 0,
  "built_at": "..."
}
```

`quality`: `ok` (no gaps/holes/disconnects inside `[open_ms, close_ms]`), `degraded` (recovered gaps, i.e.
a snapshot followed), `bad` (missing upload batches / stream_seq holes in range, or no book data).

## Data API (node001, read-only, port 8702)

Bearer token from `/etc/kmb/secrets/data_api_token` (header `Authorization: Bearer <token>`); `/health` is open.

| Method | Path | Response |
|---|---|---|
| GET | `/health` | `{ok, windows}` |
| GET | `/v1/windows?series=&from=<iso>&to=<iso>&limit=100` | `[window.json]` newest first |
| GET | `/v1/windows/{window_id}` | `window.json` |
| GET | `/v1/windows/{window_id}/parquet` | file bytes, `ETag: "<sha256>"` |
