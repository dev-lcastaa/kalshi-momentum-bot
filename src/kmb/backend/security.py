"""Passwords (argon2id), session tokens + CSRF, and the AES-GCM vault for Kalshi private keys."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import threading
import time
from collections import defaultdict, deque

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_ph = PasswordHasher()
MIN_PASSWORD_LEN = 10


def hash_password(password: str) -> str:
    return _ph.hash(password)


def verify_password(stored: str, password: str) -> bool:
    try:
        return _ph.verify(stored, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def check_password_policy(password: str) -> str | None:
    if len(password) < MIN_PASSWORD_LEN:
        return f"password must be at least {MIN_PASSWORD_LEN} characters"
    return None


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def session_id_for(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def csrf_for(token: str) -> str:
    return hashlib.sha256(b"csrf:" + token.encode()).hexdigest()[:40]


def csrf_ok(token: str, header: str | None) -> bool:
    return bool(header) and hmac.compare_digest(csrf_for(token), header or "")


class LoginLimiter:
    """In-memory sliding window of failed logins per key (username and client IP)."""

    def __init__(self, max_failures: int, window_s: int):
        self.max = max_failures
        self.window = window_s
        self._fails: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def _prune(self, key: str, now: float) -> deque[float]:
        q = self._fails[key]
        while q and now - q[0] > self.window:
            q.popleft()
        return q

    def blocked(self, *keys: str) -> bool:
        now = time.monotonic()
        with self._lock:
            return any(len(self._prune(k, now)) >= self.max for k in keys)

    def fail(self, *keys: str) -> None:
        now = time.monotonic()
        with self._lock:
            for k in keys:
                self._prune(k, now).append(now)

    def reset(self, *keys: str) -> None:
        with self._lock:
            for k in keys:
                self._fails.pop(k, None)


class Vault:
    """Encrypts Kalshi private keys at rest. The 32-byte key lives in a Docker secret, never in the DB."""

    def __init__(self, key_hex: str):
        key = bytes.fromhex(key_hex.strip())
        if len(key) != 32:
            raise ValueError("vault key must be 32 bytes (64 hex chars)")
        self._aes = AESGCM(key)

    @staticmethod
    def generate_key_hex() -> str:
        return os.urandom(32).hex()

    def encrypt(self, plaintext: bytes, aad: str) -> bytes:
        nonce = os.urandom(12)
        return nonce + self._aes.encrypt(nonce, plaintext, aad.encode())

    def decrypt(self, blob: bytes, aad: str) -> bytes:
        return self._aes.decrypt(blob[:12], blob[12:], aad.encode())


def key_aad(user_id: int, api_key_id: str) -> str:
    return f"kmb-key:{user_id}:{api_key_id}"
