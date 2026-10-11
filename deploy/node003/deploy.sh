#!/usr/bin/env bash
# Deploy node003 services (nats, collector, backend + UI, workers); keep previous images for rollback.
set -euo pipefail
cd "$(dirname "$0")"
export KMB_SECRETS_DIR="${KMB_SECRETS_DIR:-/etc/kmb/secrets}"
export KMB_ENV_FILE="${KMB_ENV_FILE:-/etc/kmb/node003.env}"
export COMPOSE_ENV_FILES="${COMPOSE_ENV_FILES:-$KMB_ENV_FILE}"
NODE001="${NODE001:-192.168.1.129}"
BACKEND_URL="${BACKEND_URL:-http://192.168.1.181:8800/health}"
COLLECTOR_URL="${COLLECTOR_URL:-http://192.168.1.181:8710/health}"
MCP_URL="${MCP_URL:-https://node003:8801/health}"
READY_TIMEOUT_S="${READY_TIMEOUT_S:-180}"
fail() { echo "DEPLOY FAILED: $*" >&2; exit 1; }

echo '== pre-flight'
command -v docker >/dev/null || fail 'docker not installed'
docker info >/dev/null 2>&1 || fail 'cannot talk to Docker'
[ "$(docker info --format '{{.MemoryLimit}}')" = true ] || fail 'Docker memory limits unavailable; enable the Pi memory controller'
for secret in ingest_token db_password data_api_token vault_key kalshi_private_key.pem mcp_tls_cert.pem mcp_tls_key.pem mcp_research_token; do
    [ -s "$KMB_SECRETS_DIR/$secret" ] || fail "$KMB_SECRETS_DIR/$secret missing/empty; see README.md"
done
[ -f "$KMB_ENV_FILE" ] || fail "$KMB_ENV_FILE missing; see README.md"
grep -q '^KMB_COLLECTOR_KALSHI_KEY_ID=.' "$KMB_ENV_FILE" || fail "KMB_COLLECTOR_KALSHI_KEY_ID not set in $KMB_ENV_FILE"
[ -d /var/lib/kmb-collector ] || fail '/var/lib/kmb-collector missing; see README.md'
# node001 must be up first: the collector uploads to ingest, everything else needs Postgres.
curl -fsS --max-time 5 "http://$NODE001:8701/ready" >/dev/null || fail "node001 ingest not ready"
timeout 5 bash -c "</dev/tcp/$NODE001/5432" 2>/dev/null || fail "node001 postgres unreachable"
docker compose config --quiet || fail 'compose config invalid'
trade_workers=$(docker compose config --environment | awk -F= '$1 == "KMB_TRADE_WORKERS" {print $2}')
sim_workers=$(docker compose config --environment | awk -F= '$1 == "KMB_SIM_WORKERS" {print $2}')
trade_workers="${trade_workers:-2}"
sim_workers="${sim_workers:-1}"
for count in "$trade_workers" "$sim_workers"; do
    case "$count" in ''|*[!0-9]*) fail 'worker counts must be nonnegative integers';; esac
done

declare -A previous
for image in kmb-collector kmb-app kmb-mcp; do
    previous[$image]=0
    if docker image inspect "$image:latest" >/dev/null 2>&1; then
        docker image tag "$image:latest" "$image:previous"
        previous[$image]=1
    fi
done
rollback() {
    local image
    docker compose logs --tail 100 collector backend mcp worker-trade worker-sim >&2 || true
    for image in kmb-collector kmb-app kmb-mcp; do
        if [ "${previous[$image]}" = 1 ]; then docker image tag "$image:previous" "$image:latest"; fi
    done
    docker compose up -d --no-build || true
}
trap 'code=$?; if [ "$code" != 0 ]; then rollback; fi' EXIT

wait_url() {
    local url="$1" deadline=$((SECONDS + READY_TIMEOUT_S))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if curl -fsS --max-time 5 "$url" >/dev/null 2>&1; then return 0; fi
        sleep 3
    done
    return 1
}

echo '== build'
docker compose build collector backend mcp
echo '== start nats + backend (backend creates/updates the schema)'
docker compose up -d --no-build --remove-orphans nats backend
wait_url "$BACKEND_URL" || fail 'backend did not become healthy'
echo '== start collector + workers'
# Workers finish their current loop iteration on SIGTERM; live jobs release their key lease.
docker compose up -d --no-build collector mcp worker-trade worker-sim
wait_url "$COLLECTOR_URL" || fail 'collector did not become healthy'
curl -fsS --cacert "$KMB_SECRETS_DIR/mcp_tls_cert.pem" --retry 20 --retry-connrefused --retry-delay 3 --max-time 5 "$MCP_URL" >/dev/null || fail 'MCP did not become healthy'
sleep 10
for pair in "worker-trade:$trade_workers" "worker-sim:$sim_workers"; do
    svc="${pair%%:*}" want="${pair#*:}"
    running=$(docker compose ps --status running -q "$svc" | wc -l)
    [ "$running" -eq "$want" ] || fail "$svc: $running/$want replicas running"
done
curl -fsS --max-time 5 "${COLLECTOR_URL%/health}/status" || true
echo
docker compose ps
trap - EXIT
echo '== deployed'
