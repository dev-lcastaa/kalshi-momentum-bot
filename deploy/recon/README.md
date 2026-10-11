# Stage A reconnaissance (read-only)

Run on **node001** (192.168.1.129) and **node003** (192.168.1.181):

```bash
bash stage_a_recon.sh > "recon-$(hostname).txt" 2>&1
```

The script only reads system state. It never installs, mounts, formats, restarts or writes anything except
its own output file. Send the resulting `recon-*.txt` files back so the deployment can be sized.

What we check, and why:

| Check | Why it matters |
|---|---|
| 64-bit OS (`aarch64`) | Docker images for DuckDB, pyarrow, NATS and Postgres are arm64. |
| node003 disks (`TRAN`, `ROTA`) | The spool, NATS, Postgres and the Parquet mirror must be on an SSD, not the SD card. |
| `/mnt/storage1`, `/mnt/storage2` source, fs and UUID | The ingest service checks the mount by UUID and fails closed (exFAT confirmed). |
| Free space | Sizes the spool limit and disk alerts. |
| `chronyc tracking` | Receive timestamps are only trustworthy when the clock is synced. |
| Docker / compose versions, ports in use | Avoids port and network conflicts with existing services. |
| Outbound reachability to Kalshi and Coinbase | Confirms the collector host can reach both APIs. |
| `vcgencmd get_throttled` | Shows Pi under-voltage or thermal throttling, which can stall the collector. |
