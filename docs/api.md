# Backend API (v1)

Served by `kmb.backend` on node003 (`http://192.168.1.181:8800`). The built UI is served from `/` by the same
process, so the UI uses relative URLs and same-origin cookies. All JSON. Decimal values (prices, counts,
dollars) are **strings**; timestamps are unix **milliseconds** (`*_ms`) or ISO-8601 UTC strings (`*_at`).

## Auth

Session cookie `kmb_session` (HttpOnly, SameSite=Lax). State-changing requests (POST/PUT/PATCH/DELETE other
than login) must also send header `X-KMB-CSRF: <csrf>` where `csrf` comes from `/api/auth/me` or the login
response.

| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/api/auth/login` | `{username, password}` | `200 {user, csrf}` / `401 {detail}` |
| POST | `/api/auth/logout` | – | `204` |
| GET | `/api/auth/me` | – | `200 {user, csrf}` / `401` |
| POST | `/api/auth/password` | `{current_password, new_password}` | `204` |

`user` = `{id, username, role: "admin"|"user", is_active, live_enabled, created_at}`.

Errors are `{detail: string}` with a suitable status (400/401/403/404/409/422/503).

## Live markets (read-only)

Authenticated `GET /api/live-markets` returns `{ts_ms, kalshi_connected, coinbase_connected, markets}`.
The backend proxies the collector's `/live-markets` endpoint directly, independent of NATS delivery.
Each configured coin has an entry, including when no current market is available:
`{series_ticker, market_ticker, open_ms, close_ms, strike, status, yes_bid, yes_ask, last,
quote_ms, price_ms, ref, ref_ms, coinbase, coinbase_ms}`.
Missing fields are null, never another coin's prices. Only the current window
(`open_ms <= ts_ms < close_ms`) is selected. Prices are decimal strings in dollars;
the UI derives the NO offer as `1 - yes_bid`. Timestamp fields indicate when each
source last updated; disconnected feeds do not imply current prices.
Collector failures return 503 rather than cached success. No trading key is needed by viewers.

## Users (admin)

| Method | Path | Body | Response |
|---|---|---|---|
| GET | `/api/users` | – | `[user]` |
| POST | `/api/users` | `{username, password, role}` | `201 user` |
| PATCH | `/api/users/{id}` | `{role?, is_active?, live_enabled?, password?}` | `user` |

## Kalshi API keys (own keys only; the private key is write-only)

| Method | Path | Body | Response |
|---|---|---|---|
| GET | `/api/keys` | – | `[key]` |
| POST | `/api/keys` | `{label, environment: "production"\|"demo", api_key_id, private_key_pem}` | `201 key` |
| DELETE | `/api/keys/{id}` | – | `204` |
| POST | `/api/keys/{id}/test` | – | `{ok, balance?: string, error?: string}` |

`key` = `{id, label, environment, api_key_id, is_active, created_at, last_used_at, leased_by_job_id}`.

## Strategies

`GET /api/strategies` → `[{name, description, params: [{name, type: "int"|"float"|"decimal"|"bool"|"str",
default, min?, max?, help}]}]`.

## Bots

| Method | Path | Body | Response |
|---|---|---|---|
| GET | `/api/bots` | – | `[bot]` |
| POST | `/api/bots` | `{name, strategy, params, series: ["KXBTC15M"], key_id?}` | `201 bot` |
| PATCH | `/api/bots/{id}` | any of the above | `bot` |
| DELETE | `/api/bots/{id}` | – | `204` |

`bot` = `{id, name, strategy, params, series, key_id, created_at, updated_at, active_job: job|null}`.

## Jobs

| Method | Path | Body | Response |
|---|---|---|---|
| GET | `/api/jobs?status=&kind=&limit=50` | – | `[job]` |
| POST | `/api/jobs` | see below | `201 job` |
| GET | `/api/jobs/{id}` | – | `job` |
| POST | `/api/jobs/{id}/stop` | – | `job` (sets `cancel_requested`) |
| GET | `/api/jobs/{id}/events?limit=200` | – | `[bot_event]` |

Create body: `{kind: "sim"|"paper"|"live", bot_id, windows?: [window_id], sim?: {latency_ms?, queue_model?:
"front"|"pro_rata"|"back", fee_multiplier?, starting_balance?}}`. `sim` requires `windows`; `paper`/`live` run
until stopped. `live` returns 403 unless the user has `live_enabled`, live trading is enabled globally, the bot
has a key, and no kill switch is engaged.

`job` = `{id, kind, status: "queued"|"running"|"succeeded"|"failed"|"canceled", bot_id, spec, worker_id,
cancel_requested, error, created_at, started_at, finished_at, heartbeat_at, run_id}`.

`bot_event` = `{id, level: "info"|"warn"|"error", code, message, detail, ts}`.

## Runs (results)

| Method | Path | Response |
|---|---|---|
| GET | `/api/runs?mode=&limit=50` | `[run]` |
| GET | `/api/runs/{id}` | `run & {orders, fills, positions, settlements, equity: [[ts_ms, pnl]]}` |
| POST | `/api/runs/{id}/calibrate` | `201 job` (body optional: `{sim?: {...}}`) |

`calibrate` queues a `calibration` job (fidelity gate G4) for a finished **live** run: the worker replays the
run's orders at their recorded `sim_ts_ms` on the captured windows and compares simulated vs real fills. A
summary (`{live_run_id, passed, n_match, n_mismatch, missing_windows}`) is stored in `job.spec.result`; the
full report (with `mismatches`, `windows`, `sim_config`) goes to a G4 `fidelity_reports` row. 422 for
non-live runs, 409 while the live job is still active.
The job belongs to the run's owner (admins may trigger it for any run).

`run` = `{id, job_id, bot_id, mode, strategy, params, sim_config, windows, status, started_at, finished_at,
summary: {pnl, fees, n_orders, n_fills, volume, ...}}`.
`order` = `{client_order_id, order_id, ticker, side, price, initial_count, fill_count, remaining_count,
time_in_force, status, error, tag, created_at, sim_ts_ms}`.
`fill` = `{trade_id, order_id, ticker, side, price, count, fee, is_taker, post_position, ts_ms}`.
`position` = `{ticker, position, cost, realized_pnl, fees_paid}`.
`settlement` = `{ticker, result, position, payout, pnl}`.

## Capture windows (replay catalog)

| Method | Path | Response |
|---|---|---|
| GET | `/api/windows?series=KXBTC15M&from=<iso>&to=<iso>&limit=100` | `[window]` (newest first) |
| GET | `/api/windows/{window_id}` | `window` |
| GET | `/api/windows/{window_id}/overview` | `overview` |

`window` = `{window_id, market_ticker, series_ticker, open_time, close_time, result: "yes"|"no"|null, strike,
event_count, gap_count, quality: "ok"|"degraded"|"bad"|"unknown", size_bytes}`.

`overview` (1-second buckets across the whole window, for the full-window chart):

```json
{
  "window": {},
  "start_ms": 0, "end_ms": 0, "open_ms": 0, "close_ms": 0,
  "series": [{"ts_ms": 0, "yes_bid": "0.41", "yes_ask": "0.43", "mid": "0.42", "last": "0.42",
              "volume": "12.00", "ref": "97123.45", "coinbase": "97120.10"}],
  "trades": [{"ts_ms": 0, "price": "0.42", "count": "3.00", "taker_side": "yes"}],
  "depth": [{"ts_ms": 0, "bids": [["0.41", "120.00"]], "asks": [["0.43", "80.00"]]}],
  "lifecycle": [{"ts_ms": 0, "event_type": "determined", "text": "result=yes"}]
}
```

Any series value may be `null`. `ref` is the CF Benchmarks index (BRTI / ETHUSD_RTI), `coinbase` the Coinbase
last trade. `depth` has up to 10 levels per side per bucket. `volume` is contracts traded within the bucket.

## Replay stream

`WS /ws/replay/{window_id}` (cookie auth). The server first sends:

```json
{"type": "meta", "window": {}, "start_ms": 0, "end_ms": 0, "open_ms": 0, "close_ms": 0}
```

Client → server: `{"op":"play"}`, `{"op":"pause"}`, `{"op":"speed","speed":N}` (0.25…60),
`{"op":"seek","ts_ms":N}`, `{"op":"step","ms":N}` (advance while paused).

Server → client (≈10 frames/s of wall time while playing, plus one after every seek/step; each frame covers all
events since the previous one):

```json
{"type": "frame", "ts_ms": 0, "playing": true, "speed": 1,
 "book": {"ticker": "...", "valid": true, "yes_bids": [["0.41","120.00"]], "yes_asks": [["0.43","80.00"]],
          "mid": "0.42", "spread": "0.02"},
 "trades": [{"ts_ms": 0, "price": "0.42", "count": "3.00", "taker_side": "yes"}],
 "ticker": {"price": "0.42", "volume": "1200.00", "open_interest": "800.00"},
 "ref": "97123.45", "coinbase": "97120.10",
 "events": [{"ts_ms": 0, "kind": "lifecycle", "text": "..."}]}
```

`events[].kind` is `lifecycle`, `transport` or `gap`. `ticker`, `ref`, `coinbase` may be `null`. Book levels
are best-first, up to 25 per side. `{"type":"end"}` is sent when the cursor reaches `end_ms`;
`{"type":"error","detail":"..."}` on failure.

## Operations

| Method | Path | Who | Response |
|---|---|---|---|
| GET | `/api/status` | any | `status` |
| GET | `/api/fidelity?limit=50` | any | `[{id, window_id, gate, passed, metrics, created_at}]` |
| GET | `/api/risk-limits` | any | `{global: limits, user: limits\|null}` |
| PUT | `/api/risk-limits/global` or `/api/risk-limits/{user_id}` | admin | `limits` |
| POST | `/api/kill-switch` | own scope: any; global: admin | body `{scope: "global"\|"user", engaged, reason}` → `status` |
| PUT | `/api/settings/live-trading` | admin | body `{enabled}` → `status` |

`status` = `{collector: object|null, ingest: object|null, data_api: {ok, detail?}, workers: [worker],
live_trading_enabled, kill_switch: {global: bool, user: bool}}`.
`worker` = `{worker_id, host, current_job_id, heartbeat_at, capabilities}`.
`limits` = `{max_order_count, max_position_per_market, max_open_orders, max_daily_loss,
max_notional_per_market, max_orders_per_minute}` (decimal strings, ints for the last and `max_open_orders`).

Engaging a kill switch makes workers cancel all resting orders of affected paper/live jobs and stop them.
