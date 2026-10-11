"""Kalshi API base URLs."""

from __future__ import annotations

from dataclasses import dataclass

WS_PATH = "/trade-api/ws/v2"
REST_PREFIX = "/trade-api/v2"


@dataclass(frozen=True)
class KalshiEnvironment:
    name: str
    rest_base_url: str
    ws_url: str

    @classmethod
    def production(cls) -> KalshiEnvironment:
        return cls(
            name="prod",
            rest_base_url="https://external-api.kalshi.com" + REST_PREFIX,
            ws_url="wss://external-api-ws.kalshi.com" + WS_PATH,
        )

    @classmethod
    def demo(cls) -> KalshiEnvironment:
        return cls(
            name="demo",
            rest_base_url="https://external-api.demo.kalshi.co" + REST_PREFIX,
            ws_url="wss://external-api-ws.demo.kalshi.co" + WS_PATH,
        )

    @classmethod
    def from_name(cls, name: str) -> KalshiEnvironment:
        match name.lower():
            case "prod" | "production":
                return cls.production()
            case "demo":
                return cls.demo()
        raise ValueError(f"Unknown Kalshi environment: {name!r}")
