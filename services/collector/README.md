# Collector service

Run with `python -m kmb.collector run`; validate configuration using
`python -m kmb.collector check-config`. The service archives exact Kalshi REST/WS and Coinbase
Exchange frame text in the ingest-compatible durable spool, publishes best-effort to NATS, and
serves `/health`, `/ready`, and `/status` on port 8710.

Configuration uses `KMB_COLLECTOR_*`: `KALSHI_ENV` (`production` or `demo`),
`KALSHI_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`, `SERIES` (default `KXBTC15M,KXETH15M`),
`SPOOL_DIR`, `SPOOL_CAP_BYTES`, `BATCH_MAX_BYTES`, `BATCH_MAX_AGE_SECONDS`, `INGEST_URL`,
`INGEST_TOKEN_FILE` (or `INGEST_TOKEN`), `NATS_URL` (empty disables),
`REST_SNAPSHOT_INTERVAL`, `DISCOVERY_INTERVAL`, `STATUS_PORT`, and `COLLECTOR_ID`.
Postgres is optional through `KMB_DATABASE_URL` or `KMB_DB_PASSWORD_FILE` plus the shared `KMB_DB_*`
settings. Coinbase heartbeat frames are intentionally discarded; matches, ticker, subscriptions,
and error frames are retained.
