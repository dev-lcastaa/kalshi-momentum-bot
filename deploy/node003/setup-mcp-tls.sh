#!/usr/bin/env bash
# One-time local CA and node003 certificate provisioning. Never exports private keys.
set -euo pipefail
secrets="${KMB_SECRETS_DIR:-/etc/kmb/secrets}"
[ "$(id -u)" = 0 ] || { echo 'Run with sudo.' >&2; exit 1; }
command -v openssl >/dev/null
for name in mcp_ca_key.pem mcp_ca_cert.pem mcp_tls_key.pem mcp_tls_cert.pem; do
    [ ! -e "$secrets/$name" ] || { echo "$secrets/$name already exists; refusing to replace it." >&2; exit 1; }
done
install -d -o 1000 -g 1000 -m 0750 "$secrets"
umask 077
work=$(mktemp -d)
trap 'rm -f "$work/ca.key" "$work/ca.pem" "$work/server.key" "$work/server.csr" "$work/server.pem" "$work/extensions"; rmdir "$work"' EXIT
openssl req -x509 -newkey rsa:3072 -nodes -days 3650 \
    -keyout "$work/ca.key" -out "$work/ca.pem" \
    -subj '/CN=AQLab MCP Local CA' \
    -addext 'basicConstraints=critical,CA:TRUE,pathlen:0' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign'
openssl req -new -newkey rsa:3072 -nodes \
    -keyout "$work/server.key" -out "$work/server.csr" -subj '/CN=node003'
cat > "$work/extensions" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:node003,DNS:node003.local,IP:192.168.1.181
EOF
openssl x509 -req -in "$work/server.csr" -CA "$work/ca.pem" -CAkey "$work/ca.key" \
    -set_serial "0x$(openssl rand -hex 16)" -days 365 -sha256 \
    -extfile "$work/extensions" -out "$work/server.pem"
install -o root -g root -m 0400 "$work/ca.key" "$secrets/mcp_ca_key.pem"
install -o 1000 -g 1000 -m 0444 "$work/ca.pem" "$secrets/mcp_ca_cert.pem"
install -o 1000 -g 1000 -m 0400 "$work/server.key" "$secrets/mcp_tls_key.pem"
cat "$work/server.pem" "$work/ca.pem" > "$secrets/mcp_tls_cert.pem"
chown 1000:1000 "$secrets/mcp_tls_cert.pem"
chmod 0444 "$secrets/mcp_tls_cert.pem"
echo "TLS provisioned. Trust only the public $secrets/mcp_ca_cert.pem on clients."
