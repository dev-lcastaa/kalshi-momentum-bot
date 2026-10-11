#!/usr/bin/env bash
# Run on node001 as uid 1000 with Docker access. No passwords in arguments or dump names.
set -euo pipefail
export DOCKER_CONTEXT="${DOCKER_CONTEXT:-default}"
ROOT="${KMB_BACKUP_ROOT:-/mnt/storage1/kalshi-db-backups}"
CONTAINER="${KMB_POSTGRES_CONTAINER:-kmb-postgres}"
[ "$(findmnt -no UUID /mnt/storage1)" = C174-8D73 ] || { echo 'storage1 UUID mismatch' >&2; exit 1; }
[ "$(cat /mnt/storage1/kalshi-archive/.kmb-archive-id)" = kalshi-archive-storage1-C174-8D73 ] \
    || { echo 'archive sentinel mismatch' >&2; exit 1; }
mkdir -p "$ROOT/daily" "$ROOT/weekly"
# Serialize host cron and manual backups on ext4, not the exFAT archive.
exec 9>"${KMB_BACKUP_LOCK:-/var/lib/kmb-normalizer/pg-backup.lock}"
flock -n 9 || { echo 'backup already running' >&2; exit 1; }
STAMP="$(date -u +%Y%m%d)"
DAILY="$ROOT/daily/kmb-$STAMP.dump"
TMP="$DAILY.$$.tmp"
WEEKLY_TMP=""
trap 'rm -f "$TMP"; [ -z "$WEEKLY_TMP" ] || rm -f "$WEEKLY_TMP"' EXIT
if [ ! -f "$DAILY" ]; then
    docker exec "$CONTAINER" pg_dump -U kmb -d kmb -Fc > "$TMP"
    docker exec -i "$CONTAINER" pg_restore --list < "$TMP" >/dev/null
    sync -f "$TMP"
    mv -n "$TMP" "$DAILY"
    sync -f "$ROOT/daily"
fi
# Verify even an existing same-day dump before rotation or making the weekly copy.
docker exec -i "$CONTAINER" pg_restore --list < "$DAILY" >/dev/null
if [ "$(date -u +%u)" = 7 ]; then
    WEEKLY="$ROOT/weekly/kmb-$STAMP.dump"
    if [ ! -f "$WEEKLY" ]; then
        WEEKLY_TMP="$WEEKLY.$$.tmp"
        cp "$DAILY" "$WEEKLY_TMP"
        docker exec -i "$CONTAINER" pg_restore --list < "$WEEKLY_TMP" >/dev/null
        sync -f "$WEEKLY_TMP"
        mv -n "$WEEKLY_TMP" "$WEEKLY"
        sync -f "$ROOT/weekly"
    fi
fi
# Keep the newest 14 daily and 8 weekly valid filenames; never rotate on backup failure.
rotate() {
    local dir="$1" keep="$2" n=0 path
    while IFS= read -r path; do
        n=$((n + 1))
        if [ "$n" -gt "$keep" ]; then rm -f "$dir/$path"; fi
    done < <(find "$dir" -maxdepth 1 -type f -regextype posix-extended \
        -regex '.*/kmb-[0-9]{8}\.dump' -printf '%f\n' | sort -r)
}
rotate "$ROOT/daily" 14
rotate "$ROOT/weekly" 8
echo "Verified backup: $DAILY"
