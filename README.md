# AQLab Kalshi Scalper Center

Momentum scalping research and trading for Kalshi 15-minute crypto markets (`KXBTC15M`, `KXETH15M`):
capture every market-data frame, replay recorded 15-minute windows through an exchange simulator, and run
the same strategy code in sim, paper and (gated) live mode.

## Layout

```
src/kmb/          shared core library used by every service
  kalshi/         auth (RSA-PSS signing), REST client, WebSocket client, market discovery
  envelope/       capture envelope, upload batches, window conventions
  collector/      node003 capture service: Kalshi REST/WS + Coinbase -> spool -> ingest, and -> NATS
  ingest/         node001 ingestion API (raw archive on /mnt/storage1)
  normalizer/     node001: raw archive -> per-window derived files + Postgres market results
  dataapi/        node001: read-only HTTP API over derived windows
  replicate/      node001: nightly copy storage1 -> storage2
  db/             Postgres schema (users, bots, jobs, runs, orders, fills, events, ...) and shared ops
  replay/         window sources, event stream, SimRunner
  sim/            exchange simulator (book, queue model, fees, settlement)
  fidelity/       simulator fidelity gates (G1-G4)
  strategy/       strategy interface, registry, momentum scalper
  backend/        FastAPI API + UI host (python -m kmb.backend)
  worker/         sim / paper / live / calibration job runner (python -m kmb.worker run)
services/         per-service Dockerfiles (app = backend + workers + built UI)
frontend/         React UI (replay charts, runs, bots/jobs, keys, admin)
deploy/           per-node compose files and setup guides
docs/             api.md (backend REST/WS), data-formats.md (archive + derived formats)
tests/
```

## Services and nodes

- **node001** (192.168.1.129): `ingest` :8701, Postgres :5432, `normalizer` (every 15 min),
  `data-api` :8702, nightly backup + replication. See [deploy/node001/README.md](deploy/node001/README.md).
- **node003** (192.168.1.181): `collector` :8710, NATS (memory only), `backend` + UI :8800,
  `worker-trade` (paper/live) and `worker-sim` (sim/calibration) pools. See
  [deploy/node003/README.md](deploy/node003/README.md).
- **Jenkins** (node002) runs the tests and deploys node001, then node003 ([Jenkinsfile](Jenkinsfile)).

Live trading is disabled by default and needs a global switch, a per-user flag, a bot key, no kill switch,
and passes per-order risk limits.

## Agent research

The **Agent research** page manages scoped MCP tokens, experiment evidence and
human approval of bounded paper sessions. Agents can run autonomous simulation
batches without accessing keys, trading bots, administration or live trading.
The local stdio server is `python -m kmb.mcp`; see
[docs/research-mcp.md](docs/research-mcp.md) for setup, permissions, tools and the
training/held-out validation workflow.

## Development

```powershell
uv sync
uv run pytest
uv run ruff check .
cd frontend; npm ci; npm run lint; npm test; npm run build   # UI dev: see frontend/README.md
```

Never commit Kalshi keys. Keys are loaded from paths or env vars at runtime (`KALSHI_KEY_ID`,
`KALSHI_PRIVATE_KEY_PATH`); users' keys are stored encrypted with the vault key. Captured data must stay
outside this repo (and outside OneDrive).
