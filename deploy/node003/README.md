# node003: collector, NATS, backend + UI, workers

node003 (192.168.1.181, Pi with 4 cores and 8 GB RAM) runs the live side:

| Service | What it does | Port |
|---|---|---|
| `nats` | Market-data bus. JetStream stream `MD` (`md.>`) is in memory only (1 h / 256 MB). | internal |
| `collector` | Kalshi REST/WS + Coinbase capture. Spools to `/var/lib/kmb-collector`, uploads to node001 ingest, publishes to NATS. | 8710 (`/health`, `/ready`, `/status`) |
| `backend` | FastAPI API + the built UI, served from the same origin. Creates/updates the Postgres schema on start. | 8800 |
| `worker-trade` | Paper and live jobs, which run until stopped. One job per container. Scale with `KMB_TRADE_WORKERS` (default 2). | – |
| `worker-sim` | Replays of recorded windows (sim) and G4 calibration jobs. Scale with `KMB_SIM_WORKERS` (default 1). | – |

Postgres, ingest and the data API live on node001, which must be deployed first
(see [../node001/README.md](../node001/README.md)). Day-to-day deploys run from Jenkins; the steps below are done once.

## One-time setup (on node003 as `lcastaa`)

1. Docker works for `lcastaa` and for the Jenkins agent user (same as node001 step 1).

   On both Pis, `docker --context default info --format '{{.MemoryLimit}}'` must
   print `true` before deployment; otherwise Docker ignores the RAM budgets.
   Enable `cgroup_enable=memory cgroup_memory=1` in the single-line
   `/boot/firmware/cmdline.txt` after backing it up, then reboot during an approved
   maintenance window. The initial preparation saved the original settings as
   `/boot/firmware/cmdline.txt.kmb-before-memory` on each Pi.

2. Collector spool directory on the SD card:

   ```bash
   sudo install -d -o 1000 -g 1000 -m 0750 /var/lib/kmb-collector
   ```

3. Secrets in `/etc/kmb/secrets` (owner 1000, readable only by uid 1000). The first three are
   **copied from node001** and must match exactly:

   ```bash
   sudo install -d -o 1000 -g 1000 -m 0750 /etc/kmb/secrets
   cd /etc/kmb/secrets
   # from node001: ingest_token, db_password, data_api_token
   scp lcastaa@192.168.1.129:/etc/kmb/secrets/{ingest_token,db_password,data_api_token} /tmp/
   sudo mv /tmp/ingest_token /tmp/db_password /tmp/data_api_token .
   # vault key: encrypts users' Kalshi API keys at rest. Back it up offline; losing it means re-entering keys.
   python3 -c 'import secrets; print(secrets.token_hex(32))' | sudo tee vault_key >/dev/null
   # your Kalshi API private key (used by the collector, read-only market data)
   sudo cp ~/kalshi-key.pem kalshi_private_key.pem
   sudo chown 1000:1000 * && sudo chmod 0400 *
   ```

4. Non-secret settings in `/etc/kmb/node003.env`:

   ```bash
   sudo tee /etc/kmb/node003.env >/dev/null <<'EOF'
   KMB_COLLECTOR_KALSHI_KEY_ID=<your Kalshi API key id>
   KMB_TRADE_WORKERS=0
   KMB_SIM_WORKERS=1
   EOF
   ```

   These worker counts start recording, the UI, and one replay worker without any
   trading workers. `deploy.sh` also reads this file for Compose substitutions.
   For manual Compose commands use `docker compose --env-file /etc/kmb/node003.env`.
   Increase `KMB_TRADE_WORKERS` when ready for paper-trading workers; real-money
   trading still requires the separate safety switches below.

   Optional overrides are environment variables for `docker compose` / Jenkins: `KMB_KALSHI_ENV`
   (`production`|`demo`), `KMB_TRADE_WORKERS`, `KMB_SIM_WORKERS`, `KMB_DB_HOST`, `KMB_DATA_API_URL`,
   `KMB_INGEST_URL`, `KMB_BACKEND_COOKIE_SECURE` (set `1` only behind HTTPS).

5. First deploy (same script Jenkins runs), then create the first admin account:

   ```bash
   cd ~/kalshi-momentum-bot/deploy/node003
   bash deploy.sh
   docker compose run --rm --no-deps backend -m kmb.backend create-admin lcastaa
   ```

   Open `http://192.168.1.181:8800`. Admins create other users from the Admin page; each user adds
   their own Kalshi key (encrypted with the vault key) and their own bots.

## Live trading safety

Live trading is **off** by default. All of these must hold before a live job starts:
global live switch on (Admin page), the user has `live_enabled`, the bot has a key, no kill switch is engaged,
and the key is not leased by another live job. Risk limits (order size, position and notional per market,
open orders, orders per minute, daily loss) are enforced by every worker before each order. Engaging a kill switch
stops running paper/live jobs.

## Agent research

The backend/UI also serves **Agent research** for scoped MCP tokens, experiment
results and human-only paper approvals. MCP runs on the agent workstation over
stdio and reaches this backend through HTTPS or a localhost SSH tunnel; it needs
no additional deployed service. See [../../docs/research-mcp.md](../../docs/research-mcp.md).
Deploy the updated backend and workers together so source fingerprints match.
Keep simulation workers enabled; approved paper jobs additionally need at least
one paper-capable trade worker. Live trading can remain disabled.

## Operations

```bash
docker compose ps
docker compose logs -f --tail 100 collector
docker compose logs -f --tail 100 worker-trade
curl -s http://192.168.1.181:8710/status | python3 -m json.tool       # capture status
docker compose up -d --no-build --scale worker-trade=3 worker-trade      # temporary scale
```

Rollback: `deploy.sh` tags the previous images `:previous` and restores them automatically on failure. Manual:

```bash
docker image tag kmb-app:previous kmb-app:latest && docker compose up -d --no-build
```

The collector keeps capturing while node001 is down; batches wait in the spool (5 GB cap) and upload later.
Workers fail jobs whose heartbeat stops for 120 s (they are never silently re-run, because live jobs place orders).
