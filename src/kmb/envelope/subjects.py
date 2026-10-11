"""NATS subjects for the live market-data bus (node003). Payload is the envelope JSON (see ``event.py``).

The bus is ephemeral fan-out; the node001 archive stays authoritative.
"""

from __future__ import annotations

from typing import Any

PREFIX = "md"
KALSHI_MARKET = PREFIX + ".kalshi.{ticker}"  # orderbook, ticker, trade, lifecycle for one market
KALSHI_OTHER = PREFIX + ".kalshi._"  # control frames without a market (subscribed, ok, error)
CFB = PREFIX + ".cfb.{index}"  # cfbenchmarks_value / _5hz per index (BRTI, ETHUSD_RTI)
COINBASE = PREFIX + ".coinbase.{pair}"  # BTC-USD, ETH-USD
REST = PREFIX + ".rest.{ticker}"  # REST snapshots/market metadata for one market
CONTROL = PREFIX + ".control"  # transport + control events
ALL = PREFIX + ".>"
STREAM_NAME = "MD"  # JetStream stream covering ALL (memory storage, ~1 h)


def _token(value: str) -> str:
    return value.replace(".", "_").replace("*", "_").replace(">", "_").replace(" ", "_")


def subject_for(envelope: dict[str, Any]) -> str:
    source = envelope.get("source")
    kind = envelope.get("kind")
    ticker = envelope.get("market_ticker")
    if kind in ("transport", "control"):
        return CONTROL
    if source == "coinbase":
        return COINBASE.format(pair=_token(ticker)) if ticker else CONTROL
    if envelope.get("stream") == "kalshi_rest":
        return REST.format(ticker=_token(ticker)) if ticker else KALSHI_OTHER
    msg_type = envelope.get("msg_type") or ""
    if msg_type.startswith("cfbenchmarks"):
        return CFB.format(index=_token(ticker)) if ticker else KALSHI_OTHER
    return KALSHI_MARKET.format(ticker=_token(ticker)) if ticker else KALSHI_OTHER
