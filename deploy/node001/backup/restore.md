# Restore the node001 PostgreSQL database

Run from the stable `deploy/node001` checkout. Stop every node003 DB writer (backend,
workers, collector) before proceeding; this is a destructive replacement of the app database.
Raw archive and Parquet are not changed.

```bash
export DOCKER_CONTEXT=default
DUMP=/mnt/storage1/kalshi-db-backups/daily/kmb-YYYYMMDD.dump
docker exec -i kmb-postgres pg_restore --list < "$DUMP" >/dev/null
docker compose stop normalizer
# Save a fresh pre-restore backup elsewhere; a same-day scheduled dump is not overwritten.
docker exec kmb-postgres pg_dump -U kmb -d kmb -Fc > /mnt/storage1/kalshi-db-backups/pre-restore.dump
docker exec -i kmb-postgres pg_restore --list < /mnt/storage1/kalshi-db-backups/pre-restore.dump >/dev/null
docker exec kmb-postgres dropdb -U kmb --force kmb
docker exec kmb-postgres createdb -U kmb -O kmb kmb
docker exec -i kmb-postgres pg_restore -U kmb -d kmb --no-owner --exit-on-error < "$DUMP"
docker exec kmb-postgres psql -U kmb -d kmb -c 'SELECT count(*) FROM capture_windows;'
docker compose start normalizer
```

Restart node003 writers only after checking users/jobs/results and schema compatibility. On
failure leave writers stopped and restore the verified pre-restore dump using the same process.
If storage1 is lost, first verify storage2's UUID `2E5C-6B4D` and use its verified mirror dump.
`pg_restore --list` validates dump structure, not recoverability of all application invariants:
periodically test restores into an isolated database. Never delete raw captures to recover DB space.
