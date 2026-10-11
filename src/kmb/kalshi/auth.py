"""RSA-PSS request signing for Kalshi's API (REST and WebSocket handshake).

See https://docs.kalshi.com/getting_started/api_keys
"""

from __future__ import annotations

import base64
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def load_private_key_pem(pem: bytes) -> rsa.RSAPrivateKey:
    key = serialization.load_pem_private_key(pem, password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("Kalshi API keys must be RSA private keys")
    return key


def load_private_key_from_file(file_path: str) -> rsa.RSAPrivateKey:
    with open(file_path, "rb") as key_file:
        return load_private_key_pem(key_file.read())


def sign_pss_text(private_key: rsa.RSAPrivateKey, text: str) -> str:
    signature = private_key.sign(
        text.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("ascii")


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class KalshiAuth:
    key_id: str
    private_key: rsa.RSAPrivateKey = field(repr=False)
    clock_ms: Callable[[], int] = field(default=_now_ms, repr=False)

    @classmethod
    def from_file(cls, key_id: str, private_key_path: str) -> KalshiAuth:
        return cls(key_id=key_id, private_key=load_private_key_from_file(private_key_path))

    @classmethod
    def from_pem(cls, key_id: str, pem: bytes) -> KalshiAuth:
        return cls(key_id=key_id, private_key=load_private_key_pem(pem))

    @classmethod
    def from_env(cls, prefix: str = "KALSHI") -> KalshiAuth:
        """Load from `<prefix>_KEY_ID` and `<prefix>_PRIVATE_KEY_PATH`."""
        key_id = os.environ.get(f"{prefix}_KEY_ID")
        key_path = os.environ.get(f"{prefix}_PRIVATE_KEY_PATH")
        if not key_id or not key_path:
            raise RuntimeError(f"{prefix}_KEY_ID and {prefix}_PRIVATE_KEY_PATH must be set")
        return cls.from_file(key_id, key_path)

    def headers(self, method: str, path: str) -> dict[str, str]:
        """Build KALSHI-ACCESS-* headers. The signature covers `path` without its query string."""
        path_without_query = path.split("?", 1)[0]
        timestamp_ms = str(self.clock_ms())
        signature = sign_pss_text(self.private_key, timestamp_ms + method.upper() + path_without_query)
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }
