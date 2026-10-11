"""Backend settings from environment variables (``KMB_BACKEND_*`` plus shared ``KMB_*``)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _read_file(path: str | None) -> str | None:
    if path and os.path.exists(path):
        return Path(path).read_text(encoding="utf-8").strip()
    return None


@dataclass
class BackendSettings:
    host: str = "0.0.0.0"
    port: int = 8800
    database_url: str | None = None
    vault_key_hex: str | None = None
    static_dir: str | None = None
    cookie_secure: bool = False
    session_ttl_hours: int = 24 * 7
    collector_status_url: str | None = "http://collector:8710/status"
    ingest_status_url: str | None = "http://192.168.1.129:8701/ready"
    worker_stale_s: int = 60
    login_max_failures: int = 10
    login_window_s: int = 900
    allowed_origins: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> BackendSettings:
        env = os.environ
        vault = env.get("KMB_VAULT_KEY") or _read_file(
            env.get("KMB_VAULT_KEY_FILE", "/run/secrets/vault_key")
        )
        default_static = Path(__file__).resolve().parents[3] / "frontend" / "dist"
        return cls(
            host=env.get("KMB_BACKEND_HOST", "0.0.0.0"),
            port=int(env.get("KMB_BACKEND_PORT", "8800")),
            database_url=env.get("KMB_DATABASE_URL"),
            vault_key_hex=vault,
            static_dir=env.get("KMB_BACKEND_STATIC_DIR", str(default_static)),
            cookie_secure=env.get("KMB_BACKEND_COOKIE_SECURE", "0") == "1",
            session_ttl_hours=int(env.get("KMB_BACKEND_SESSION_TTL_HOURS", str(24 * 7))),
            collector_status_url=env.get("KMB_COLLECTOR_STATUS_URL", "http://collector:8710/status") or None,
            ingest_status_url=env.get("KMB_INGEST_STATUS_URL", "http://192.168.1.129:8701/ready") or None,
            allowed_origins=[o for o in env.get("KMB_BACKEND_ALLOWED_ORIGINS", "").split(",") if o],
        )
