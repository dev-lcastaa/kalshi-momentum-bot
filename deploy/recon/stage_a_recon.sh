#!/usr/bin/env bash
# Stage A reconnaissance: READ-ONLY. Changes nothing on the host (no installs, mounts, formats or restarts).
# Usage on each node:  bash stage_a_recon.sh > "recon-$(hostname).txt" 2>&1
set -u

section() { printf '\n===== %s =====\n' "$1"; }
run() { printf '$ %s\n' "$*"; "$@" 2>&1 || printf '(exit %s)\n' "$?"; }

section "identity"
run hostname
run uname -a
run cat /etc/os-release
run uptime

section "cpu / memory"
run nproc
run free -h
command -v vcgencmd >/dev/null && run vcgencmd get_throttled
command -v vcgencmd >/dev/null && run vcgencmd measure_temp

section "block devices (ROTA=1 spinning, TRAN=usb/nvme/mmc)"
run lsblk -o NAME,SIZE,TYPE,FSTYPE,ROTA,TRAN,MODEL,UUID,MOUNTPOINTS
run df -hT -x tmpfs -x devtmpfs -x overlay

section "archive mounts"
for m in /mnt/storage1 /mnt/storage2; do
  run findmnt -no SOURCE,FSTYPE,OPTIONS,UUID "$m"
  [ -d "$m" ] && run ls -la "$m"
done
run cat /etc/fstab

section "clock sync"
run timedatectl
command -v chronyc >/dev/null && run chronyc tracking
command -v chronyc >/dev/null && run chronyc sources -v

section "docker"
command -v docker >/dev/null && run docker version --format '{{.Server.Version}} {{.Server.Arch}}'
command -v docker >/dev/null && run docker compose version
command -v docker >/dev/null && run docker ps --format '{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
command -v docker >/dev/null && run docker network ls
command -v docker >/dev/null && run docker system df

section "network / ports in use"
command -v ss >/dev/null && run ss -ltnp
run ip -brief addr

section "python"
run python3 --version
command -v uv >/dev/null && run uv --version

section "outbound reachability (no auth, read-only)"
command -v curl >/dev/null && run curl -s -o /dev/null -w '%{http_code} %{time_total}s\n' \
  https://external-api.kalshi.com/trade-api/v2/exchange/status
command -v curl >/dev/null && run curl -s -o /dev/null -w '%{http_code} %{time_total}s\n' \
  https://api.exchange.coinbase.com/products/BTC-USD/ticker

printf '\nDone. Nothing was modified.\n'
