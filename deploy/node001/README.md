# node001: ingest service

Receives batches from the node003 collector and writes them to the exFAT archive on
`/mnt/storage1` (UUID `C174-8D73`). The service fails closed: if the drive, its UUID or the
sentinel file is wrong, it refuses uploads with `503` and the collector keeps the data in its spool.

Day-to-day deploys run from Jenkins (node002), see [Deploying with Jenkins](#deploying-with-jenkins).
The steps below are done once by hand, because they touch the drive and secrets.

## One-time setup (on node001 as `lcastaa`)

1. Docker is installed. Make sure both `lcastaa` and the Jenkins agent's user can use it:

   ```bash
   export DOCKER_CONTEXT=default
   docker info >/dev/null && echo ok
   ps -o user= -p "$(pgrep -f agent.jar | head -1)"   # Jenkins agent user
   sudo usermod -aG docker <that user>                # if it is not already in the docker group; restart the agent
   ```

   AQLab uses the system Docker daemon (`default` context), not rootless Docker.
   A rootless daemon started before `/etc/kmb` was created may not see the secret
   directory in its mount namespace. Deployment, backup scripts and Jenkins select
   the system daemon without changing your saved Docker context or other workloads.
   Set `DOCKER_CONTEXT=default` for manual Compose and replication commands too.

2. Get the code onto node001 (only needed for steps 6–7; Jenkins uses its own checkout):

   ```bash
   git clone https://github.com/dev-lcastaa/kalshi-momentum-bot.git ~/kalshi-momentum-bot
   cd ~/kalshi-momentum-bot/deploy/node001
   ```

3. Confirm the archive drive is mounted and is the right one. Expect `/dev/sdX1 exfat C174-8D73`:

   ```bash
   findmnt -no SOURCE,FSTYPE,UUID /mnt/storage1
   id -u   # must print 1000 (matches uid=1000 in fstab and the container user)
   ```

4. Create the directories. The archive folder lives on the drive; the receipt DB lives on the SD card (ext4):

   ```bash
   mkdir /mnt/storage1/kalshi-archive
   sudo mkdir -p /var/lib/kmb-ingest && sudo chown 1000:1000 /var/lib/kmb-ingest
   ```

5. Create the upload token in `/etc/kmb/secrets` (outside any checkout, so Jenkins workspace
   cleanups can't delete it). The node003 collector needs the same value. Never commit it.

   ```bash
   sudo install -d -o 1000 -g 1000 -m 0755 /etc/kmb/secrets
   openssl rand -hex 32 | sudo tee /etc/kmb/secrets/ingest_token >/dev/null
   sudo chown 1000:1000 /etc/kmb/secrets/ingest_token && sudo chmod 0400 /etc/kmb/secrets/ingest_token
   ```

6. Write the archive sentinel. This checks the mount identity first and refuses if it is wrong:

   ```bash
   docker compose build
   docker compose run --rm ingest init-archive
   ```

7. Start it and check readiness (same script Jenkins runs):

   ```bash
   bash deploy.sh
   curl -s http://192.168.1.129:8701/ready | python3 -m json.tool
   ```

   `"ready": true` with every check `true` means it is safe to point the collector at it.

## Deploying with Jenkins

The [Jenkinsfile](../../Jenkinsfile) runs on Jenkins at `http://node002:8080/` using the agents labelled
`node001` and `node003`. Each image is built on the node that runs it (both are aarch64), so there is no registry.

1. **Test** (node001): `docker build --target test` runs pytest and ruff inside the image.
2. **Deploy node001** (`main` only): `deploy.sh` runs pre-flight checks (drive UUID, sentinel, state dir,
   token, Docker access). It then builds, restarts, and waits up to 180 s for `/ready`. If `/ready`
   doesn't pass, it prints the logs and switches back to `kmb-ingest:previous`.
3. **Deploy node003** (`main` only): runs `deploy/node003/deploy.sh` once it exists (the collector arrives in M1b).
   It only runs after node001 is ready, because the collector uploads to ingest.

Job setup: **New Item → Pipeline**. Definition: *Pipeline script from SCM*. Git URL
`https://github.com/dev-lcastaa/kalshi-momentum-bot.git`, branch `*/main`, script path `Jenkinsfile`.
Run it once by hand: that first run registers the 5-minute SCM poll and the `DEPLOY` parameter.
Uncheck `DEPLOY` to run tests only.

The pipeline never runs `init-archive` and never creates directories or tokens. If any of those are
missing, the deploy fails at pre-flight with a pointer to the step above.

## Operations

| Task | Command |
|---|---|
| Logs | `docker compose logs -f ingest` |
| Crash recovery (also runs at every start) | `docker compose run --rm ingest reconcile` |
| Full checksum audit (slow) | `docker compose run --rm ingest verify-all` |
| Upgrade | Push to `main` (Jenkins deploys), or by hand: `git pull && bash deploy.sh` |
| Manual rollback | `docker image tag kmb-ingest:previous kmb-ingest:latest && docker compose up -d --no-build` |

## API (LAN only, bearer token)

| Method | Path | Notes |
|---|---|---|
| `PUT` | `/v1/batches/{capture_id}:{stream}:{first_stream_seq}` | Body: gzip JSONL. Header `X-Batch-Meta`: batch metadata JSON. `201` created, `200` duplicate. |
| `GET` | `/v1/batches/{batch_id}` | Receipt (status `pending`/`committed`/`failed`/`corrupt`). |
| `GET` | `/v1/captures/{capture_id}/coverage` | Per-stream holes and the global sequence completeness check. |
| `GET` | `/health` | Process alive (no auth). |
| `GET` | `/ready` | Safe to accept writes (no auth, no secrets). |

Errors return `{"error", "message", "retryable"}`. The collector retries only `retryable: true`
(`500`, `503`, `507`) and keeps the batch in its spool. Other errors (`400`, `401`, `409`, `413`) are
permanent and raise an alert.

## Notes

- Archive files are write-once and never deleted. Corrupt or conflicting files go to `quarantine/`.
- Port 8701 is bound to the LAN IP only. Limiting it to node003 with a firewall rule is optional and
  needs your approval before any firewall change.
- storage1 is a Ventoy USB stick: don't reuse it as a boot drive while it holds the archive.

## M2: normalizer, data API, Postgres and storage2 mirror

Additional one-time setup (before running the extended `deploy.sh`):

```bash
findmnt -no SOURCE,FSTYPE,UUID /mnt/storage2 # exfat, UUID 2E5C-6B4D
mkdir -p /mnt/storage1/kalshi-derived /mnt/storage1/kalshi-db-backups/{daily,weekly}
sudo install -d -o 1000 -g 1000 -m 0750 /var/lib/kmb-normalizer
sudo install -d -o 999 -g 1000 -m 0700 /var/lib/kmb-postgres
openssl rand -hex 32 | sudo tee /etc/kmb/secrets/db_password >/dev/null
openssl rand -hex 32 | sudo tee /etc/kmb/secrets/data_api_token >/dev/null
sudo chown 1000:1000 /etc/kmb/secrets/{db_password,data_api_token}
sudo chmod 0440 /etc/kmb/secrets/db_password
sudo chmod 0400 /etc/kmb/secrets/data_api_token
bash deploy.sh
```

The existing upload token remains uid 1000, mode 0400. Compose file secrets are **bind mounts**:
`uid`/`gid`/`mode` overrides do not remap local files. We use `postgres:16-bookworm` (uid 999)
with primary group 1000 and its data directory owned by 999:1000; the shared password is 0440,
owner 1000:1000, so both Postgres and the uid-1000 normalizer can read it. Do not switch to the
Alpine uid-70 image without changing ownership/user. Other users cannot read any token.
The PostgreSQL port 5432 and data API port 8702 are bound to 192.168.1.129, alongside ingest
8701. Keep these ports LAN/VPN-only, never port-forward. Use distinct least-privilege DB roles
for future backend/workers; the initial `kmb` role bootstraps schema/catalog only.

Normalizer runs every 60 seconds with nice +10, 1 CPU / 1 GiB. Archive events are
merged by capture/sequence and written in 4096-row chunks, so full recordings do
not need to fit in RAM. Only receipt-committed batches
with verified checksums are indexed; archive and live WAL receipt directory are read-only.
A window with a confirmed outcome becomes eligible strictly after close + 180 seconds.
For an unconfirmed outcome, the additional 1200-second settlement lag remains.
Processing time and upload backlog can delay visibility beyond that eligibility time.
Window sidecars are completion markers. Normal runs never rebuild them; explicit rebuilds can
conflict with the append-only storage2 mirror and require manual reconciliation (never automatic overwrite).
Postgres writes are best-effort and retried for completed sidecars in later cycles.

Configuration: `KMB_NORMALIZER_ARCHIVE_ROOT`, `DERIVED_ROOT`, `RECEIPTS_PATH`, `STATE_PATH`,
`SETTLE_LAG_SECONDS`, `ARCHIVE_ID` (all prefixed `KMB_NORMALIZER_`); roots must share the sentinel
parent mount. DB uses `KMB_DB_PASSWORD_FILE` / `KMB_DB_HOST` or `KMB_DATABASE_URL`.
Data API uses `KMB_DATAAPI_DERIVED_ROOT`, `TOKEN_FILE`, `HOST`, `PORT`.
Replicator uses `KMB_REPLICATE_SOURCE_MOUNT`, `DESTINATION_MOUNT`, `SOURCE_UUID`,
`DESTINATION_UUID`, `RECEIPTS_PATH`, `STATE_PATH` (all prefixed `KMB_REPLICATE_`). UUID identity
is checked against `/proc/self/mountinfo` and `/dev/disk/by-uuid` on every run.

### Scheduled backups and replication

Use host cron as uid 1000 (`lcastaa`, Docker group), with a **stable checkout** not a Jenkins
workspace. Cron times below are host local time; backup filenames/Sunday selection use UTC.
Set PATH explicitly; send log failures to monitoring. Replication is one-shot, maintenance-profile
service, not a permanent sleep-loop container. `flock` prevents overlapping runs.

```cron
PATH=/usr/local/bin:/usr/bin:/bin
0 3 * * * /bin/bash /home/lcastaa/kalshi-momentum-bot/deploy/node001/backup/pg_backup.sh >> /var/lib/kmb-normalizer/backup.log 2>&1
30 3 * * * cd /home/lcastaa/kalshi-momentum-bot/deploy/node001 && flock -n /var/lib/kmb-normalizer/replicate.lock docker compose run --rm --no-deps replicate >> /var/lib/kmb-normalizer/replicate.log 2>&1
```

Backups are `pg_dump -Fc`, validated with `pg_restore --list`, fsynced and renamed; retain newest
14 daily and 8 Sunday weekly copies. Mount UUID and sentinel are checked first. Replication
copies acknowledged archive batches, derived files and backup dumps using verified atomic
no-replace rename, caches size/mtime/checksum in ext4 SQLite, and exits nonzero on conflicts.
No destination files are ever deleted or overwritten. Rotate host logs separately.

### Restore procedure

See [backup/restore.md](backup/restore.md). **Stop all DB writers first**, including node003
backend/workers/collector, then stop the normalizer. Verify the chosen dump before restoring.

### Data API usage

`/health` is open. All `/v1/windows` endpoints require the shared bearer token; the backend
must keep this token server-side. List newest first with optional series/from/to/limit filters:

```bash
TOKEN="$(cat /etc/kmb/secrets/data_api_token)"
curl -fsS -H "Authorization: Bearer $TOKEN" 'http://192.168.1.129:8702/v1/windows?series=KXBTC15M&limit=10'
curl -fsS -H "Authorization: Bearer $TOKEN" "http://192.168.1.129:8702/v1/windows/$WINDOW_ID"
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'Range: bytes=0-99' "http://192.168.1.129:8702/v1/windows/$WINDOW_ID/parquet"
unset TOKEN
```

Parquet includes a quoted SHA256 ETag, conditional GET (304) and byte ranges (206/416).
Catalog scans sidecars, caches JSON by mtime/size, removes deleted entries and rejects unsafe paths.

| Task | Command |
|---|---|
| Normalizer status | `docker compose run --rm --no-deps normalizer -m kmb.normalizer status` |
| Build now | `docker compose run --rm --no-deps normalizer -m kmb.normalizer run-once` |
| Explicit rebuild | `docker compose run --rm --no-deps normalizer -m kmb.normalizer rebuild "$WINDOW_ID"` |
| Catalog health | `curl -fsS http://192.168.1.129:8702/health` |
| Service logs | `docker compose logs -f normalizer data-api postgres` |
| Replicate now | `docker compose run --rm --no-deps replicate` |
| Backup now | `bash backup/pg_backup.sh` |
| DB health | `docker exec kmb-postgres pg_isready -U kmb -d kmb` |

Deploy now builds both application images, starts Postgres and waits healthy before the rest,
checks both HTTP services and Postgres, and restores each application's previous image on
failure. It never downgrades Postgres or rolls back DB data. Jenkins' ingest test target still
runs the full Python suite; the new shared image also has a standalone `test` target.
