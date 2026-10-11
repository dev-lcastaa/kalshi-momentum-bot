#!/usr/bin/env bash
# Deploy node001 services; preserve each service image for targeted rollback.
set -euo pipefail
export DOCKER_CONTEXT="${DOCKER_CONTEXT:-default}"
cd "$(dirname "$0")"
export KMB_SECRETS_DIR="${KMB_SECRETS_DIR:-/etc/kmb/secrets}"
READY_URL="${READY_URL:-http://192.168.1.129:8701/ready}"
DATA_READY_URL="${DATA_READY_URL:-http://192.168.1.129:8702/health}"
READY_TIMEOUT_S="${READY_TIMEOUT_S:-180}"
fail() { echo "DEPLOY FAILED: $*" >&2; exit 1; }
echo '== pre-flight'
command -v docker >/dev/null || fail 'docker not installed'
docker info >/dev/null 2>&1 || fail 'cannot talk to Docker'
[ "$(docker info --format '{{.MemoryLimit}}')" = true ] || fail 'Docker memory limits unavailable; enable the Pi memory controller'
[ "$(findmnt -no UUID /mnt/storage1 || true)" = C174-8D73 ] || fail 'storage1 UUID mismatch'
[ -f /mnt/storage1/kalshi-archive/.kmb-archive-id ] || fail 'archive sentinel missing; see README.md'
[ "$(cat /mnt/storage1/kalshi-archive/.kmb-archive-id)" = kalshi-archive-storage1-C174-8D73 ] \
    || fail 'archive sentinel content mismatch'
for dir in /var/lib/kmb-ingest /var/lib/kmb-normalizer /var/lib/kmb-postgres \
    /mnt/storage1/kalshi-derived /mnt/storage1/kalshi-db-backups; do
    [ -d "$dir" ] || fail "$dir missing; see README.md"
done
for secret in ingest_token db_password data_api_token; do
    [ -s "$KMB_SECRETS_DIR/$secret" ] || fail "$secret missing/empty; see README.md"
done
# Existing images are saved before either build. Both derived services share one image.
declare -A previous
for image in kmb-ingest kmb-normalizer; do
    previous[$image]=0
    if docker image inspect "$image:latest" >/dev/null 2>&1; then
        docker image tag "$image:latest" "$image:previous"
        previous[$image]=1
    fi
done
rollback() {
    local image
    docker compose logs --tail 100 ingest postgres normalizer data-api >&2 || true
    for image in kmb-ingest kmb-normalizer; do
        if [ "${previous[$image]}" = 1 ]; then docker image tag "$image:previous" "$image:latest"; fi
    done
    # Do not change/downgrade Postgres or touch its data as part of application rollback.
    docker compose up -d --no-build ingest normalizer data-api || true
}
trap 'code=$?; if [ "$code" != 0 ]; then rollback; fi' EXIT
echo '== build'
docker compose build ingest normalizer
echo '== start postgres'
docker compose up -d --no-build postgres
deadline=$((SECONDS + READY_TIMEOUT_S))
while [ "$(docker inspect --format '{{.State.Health.Status}}' kmb-postgres 2>/dev/null || true)" != healthy ]; do
    [ "$SECONDS" -lt "$deadline" ] || fail 'postgres did not become healthy'
    sleep 3
done
docker compose up -d --no-build --remove-orphans ingest normalizer data-api
wait_ready() {
    local url="$1" deadline=$((SECONDS + READY_TIMEOUT_S))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if curl -fsS --max-time 5 "$url" >/dev/null 2>&1; then return 0; fi
        sleep 3
    done
    return 1
}
wait_ready "$READY_URL" || fail 'ingest did not become ready'
wait_ready "$DATA_READY_URL" || fail 'data API did not become ready'
docker exec kmb-postgres pg_isready -U kmb -d kmb >/dev/null || fail 'postgres readiness failed'
curl -fsS --max-time 5 "$READY_URL"
echo
curl -fsS --max-time 5 "$DATA_READY_URL"
echo
trap - EXIT
echo '== deployed'
