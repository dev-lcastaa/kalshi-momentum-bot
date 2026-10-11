from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path

from kmb.kalshi.endpoints import KalshiEnvironment


class ConfigError(ValueError):
    pass


def _integer(env: dict[str, str], name: str, default: int) -> int:
    value = env.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


@dataclass(frozen=True)
class CollectorConfig:
    environment: str = "production"
    kalshi_key_id: str | None = None
    kalshi_private_key_path: Path | None = None
    series: tuple[str, ...] = ("KXBTC15M", "KXETH15M")
    spool_dir: Path = Path("/var/lib/kmb-collector/spool")
    spool_cap_bytes: int = 5 * 1024**3
    batch_max_bytes: int = 2 * 1024**2
    batch_max_age_seconds: int = 60
    ingest_url: str = "http://192.168.1.129:8701"
    ingest_token_file: Path = Path("/run/secrets/ingest_token")
    ingest_token: str | None = None
    nats_url: str = "nats://nats:4222"
    db_url: str | None = None
    rest_snapshot_interval: int = 45
    discovery_interval: int = 60
    status_port: int = 8710
    collector_id: str = socket.gethostname()

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> CollectorConfig:
        values = dict(os.environ if env is None else env)
        prefix = "KMB_COLLECTOR_"
        key_id = values.get(prefix + "KALSHI_KEY_ID") or values.get("KALSHI_KEY_ID")
        key_path = values.get(prefix + "KALSHI_PRIVATE_KEY_PATH") or values.get("KALSHI_PRIVATE_KEY_PATH")
        token_file = Path(values.get(prefix + "INGEST_TOKEN_FILE", "/run/secrets/ingest_token"))
        token = values.get(prefix + "INGEST_TOKEN")
        if not token and token_file.is_file():
            token = token_file.read_text(encoding="utf-8").strip()
        try:
            environment = KalshiEnvironment.from_name(values.get(prefix + "KALSHI_ENV", "production")).name
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc
        series = tuple(
            item.strip().upper()
            for item in values.get(prefix + "SERIES", "KXBTC15M,KXETH15M").split(",")
            if item.strip()
        )
        for item in series:
            if not item.startswith("KX") or not item.endswith("15M"):
                raise ConfigError(f"invalid 15-minute series ticker: {item}")
        spool_cap = _integer(values, prefix + "SPOOL_CAP_BYTES", 5 * 1024**3)
        batch_size = _integer(values, prefix + "BATCH_MAX_BYTES", 2 * 1024**2)
        batch_age = _integer(values, prefix + "BATCH_MAX_AGE_SECONDS", 60)
        if spool_cap <= 0 or batch_size <= 0 or batch_age <= 0:
            raise ConfigError("spool cap, batch size, and batch age must be positive")
        db_url = values.get("KMB_DATABASE_URL")
        if not db_url and values.get("KMB_DB_PASSWORD_FILE"):
            try:
                from kmb.db import database_url

                db_url = database_url()
            except (RuntimeError, OSError) as exc:
                raise ConfigError(str(exc)) from exc
        return cls(
            environment=environment,
            kalshi_key_id=key_id,
            kalshi_private_key_path=Path(key_path) if key_path else None,
            series=series,
            spool_dir=Path(values.get(prefix + "SPOOL_DIR", "/var/lib/kmb-collector/spool")),
            spool_cap_bytes=spool_cap,
            batch_max_bytes=batch_size,
            batch_max_age_seconds=batch_age,
            ingest_url=values.get(prefix + "INGEST_URL", "http://192.168.1.129:8701").rstrip("/"),
            ingest_token_file=token_file,
            ingest_token=token,
            nats_url=values.get(prefix + "NATS_URL", "nats://nats:4222"),
            db_url=db_url,
            rest_snapshot_interval=_integer(values, prefix + "REST_SNAPSHOT_INTERVAL", 45),
            discovery_interval=_integer(values, prefix + "DISCOVERY_INTERVAL", 60),
            status_port=_integer(values, prefix + "STATUS_PORT", 8710),
            collector_id=values.get(prefix + "COLLECTOR_ID", socket.gethostname()),
        )

    @property
    def kalshi_environment(self) -> KalshiEnvironment:
        return KalshiEnvironment.from_name(self.environment)

    def check(self) -> None:
        if not self.series:
            raise ConfigError("at least one series is required")
        if not self.ingest_token or len(self.ingest_token) < 32:
            raise ConfigError("KMB_COLLECTOR_INGEST_TOKEN or token file must contain 32+ characters")
        if bool(self.kalshi_key_id) != bool(self.kalshi_private_key_path):
            raise ConfigError(
                "set both KMB_COLLECTOR_KALSHI_KEY_ID and KMB_COLLECTOR_KALSHI_PRIVATE_KEY_PATH"
            )
        if self.kalshi_private_key_path and not self.kalshi_private_key_path.is_file():
            raise ConfigError(f"Kalshi private key file does not exist: {self.kalshi_private_key_path}")
        if self.rest_snapshot_interval <= 0 or self.discovery_interval <= 0:
            raise ConfigError("REST snapshot and discovery intervals must be positive")
        if not 1 <= self.status_port <= 65535:
            raise ConfigError("status port must be between 1 and 65535")
