"""Discover Kalshi 15-minute crypto series and their markets.

Matching requires `frequency == "fifteen_min"` plus either the exact `KX<COIN>15M` ticker or a coin tag.
Kalshi's display tags change over time (e.g. "BTC" became "Bitcoin"), so tags alone are not trusted.
Unopened markets are included so the collector can subscribe before a window opens; Kalshi sends no
WebSocket event for market open.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from .rest import KalshiRestClient

logger = logging.getLogger(__name__)

FIFTEEN_MIN_FREQUENCY = "fifteen_min"
DEFAULT_COINS = ("BTC", "ETH")
DEFAULT_STATUSES = ("unopened", "open")


def series_ticker_for(coin: str) -> str:
    return f"KX{coin.upper()}15M"


async def find_crypto_series(client: KalshiRestClient, coins: Iterable[str] = DEFAULT_COINS) -> list[str]:
    data = await client.get_series_list(category="Crypto")
    coin_set = {c.upper() for c in coins}
    exact = {series_ticker_for(c) for c in coin_set}
    matches: list[str] = []
    for series in data.get("series") or []:
        if series.get("frequency") != FIFTEEN_MIN_FREQUENCY:
            continue
        ticker = str(series.get("ticker", ""))
        tags = {str(t).upper() for t in (series.get("tags") or [])}
        if ticker.upper() in exact or tags & coin_set:
            matches.append(ticker)
    return matches


async def list_markets(
    client: KalshiRestClient, series_ticker: str, statuses: Iterable[str] = DEFAULT_STATUSES
) -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    for status in statuses:
        cursor: str | None = None
        while True:
            page = await client.get_markets(series_ticker=series_ticker, status=status, cursor=cursor)
            markets.extend(page.get("markets") or [])
            cursor = page.get("cursor") or None
            if not cursor:
                break
    return markets


async def find_15min_markets(
    client: KalshiRestClient,
    coins: Iterable[str] = DEFAULT_COINS,
    statuses: Iterable[str] = DEFAULT_STATUSES,
) -> list[dict[str, Any]]:
    """Return markets for the matching series, de-duplicated by ticker, in discovery order."""
    coins = [c.upper() for c in coins]
    statuses = list(statuses)
    series_tickers = await find_crypto_series(client, coins)
    if not series_tickers:
        logger.warning("No 15-minute crypto series found matching %s", coins)

    by_ticker: dict[str, dict[str, Any]] = {}
    for series_ticker in series_tickers:
        for market in await list_markets(client, series_ticker, statuses):
            by_ticker.setdefault(str(market.get("ticker")), market)

    for coin in coins:
        prefix = series_ticker_for(coin) + "-"
        if not any(t.upper().startswith(prefix) for t in by_ticker):
            logger.warning("Expected %s 15-minute markets missing from discovery", coin)
    return list(by_ticker.values())
