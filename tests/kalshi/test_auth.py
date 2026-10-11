import base64

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from kmb.kalshi.auth import KalshiAuth, sign_pss_text

PSS = padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH)


@pytest.fixture(scope="module")
def private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def test_sign_pss_text_produces_verifiable_signature(private_key):
    message = "1700000000000GET/trade-api/v2/markets"
    signature = base64.b64decode(sign_pss_text(private_key, message))
    private_key.public_key().verify(signature, message.encode(), PSS, hashes.SHA256())


def test_headers_sign_method_and_path_without_query(private_key):
    auth = KalshiAuth(key_id="my-key-id", private_key=private_key, clock_ms=lambda: 1700000000123)

    headers = auth.headers("get", "/trade-api/v2/portfolio/orders?limit=5")

    assert headers["KALSHI-ACCESS-KEY"] == "my-key-id"
    assert headers["KALSHI-ACCESS-TIMESTAMP"] == "1700000000123"
    expected = b"1700000000123GET/trade-api/v2/portfolio/orders"
    signature = base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"])
    private_key.public_key().verify(signature, expected, PSS, hashes.SHA256())


def test_from_pem_and_from_env(private_key, tmp_path, monkeypatch):
    pem = private_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    assert KalshiAuth.from_pem("k", pem).key_id == "k"

    key_file = tmp_path / "key.pem"
    key_file.write_bytes(pem)
    monkeypatch.setenv("KALSHI_DATA_KEY_ID", "data-key")
    monkeypatch.setenv("KALSHI_DATA_PRIVATE_KEY_PATH", str(key_file))
    assert KalshiAuth.from_env("KALSHI_DATA").key_id == "data-key"


def test_from_env_requires_both_variables(monkeypatch):
    monkeypatch.delenv("KALSHI_KEY_ID", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PATH", raising=False)
    with pytest.raises(RuntimeError, match="must be set"):
        KalshiAuth.from_env()


def test_repr_never_contains_private_key(private_key):
    assert "private_key" not in repr(KalshiAuth(key_id="k", private_key=private_key))
