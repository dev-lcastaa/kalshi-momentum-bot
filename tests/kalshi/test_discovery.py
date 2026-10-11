from unittest.mock import AsyncMock

import httpx
import pytest

from kmb.kalshi.discovery import find_15min_markets, find_crypto_series
from kmb.kalshi.rest import KalshiRestClient


def client_with_series(series):
    client = AsyncMock(spec=KalshiRestClient)
    client.get_series_list.return_value = {"series": series}
    return client


@pytest.mark.parametrize(
    "tags",
    [
        (["BTC", "15 min"], ["ETH", "15 min"]),
        (["15 min", "Bitcoin", "Crypto"], ["15 min", "Crypto", "Ethereum"]),
        (None, []),
    ],
)
async def test_exact_series_recognized_despite_tag_changes(tags):
    client = client_with_series(
        [
            {"ticker": "KXBTC15M", "frequency": "fifteen_min", "tags": tags[0]},
            {"ticker": "KXETH15M", "frequency": "fifteen_min", "tags": tags[1]},
        ]
    )
    assert await find_crypto_series(client) == ["KXBTC15M", "KXETH15M"]
    client.get_series_list.assert_awaited_once_with(category="Crypto")


async def test_excludes_unrelated_and_non_15min_series():
    client = client_with_series(
        [
            {"ticker": "LEGACY", "frequency": "fifteen_min", "tags": ["btc"]},
            {"ticker": "KXBTC15M", "frequency": "daily", "tags": ["BTC"]},
            {"ticker": "KXSOL15M", "frequency": "fifteen_min", "tags": ["Solana"]},
            {"ticker": "KXBTCDOM", "frequency": "daily", "tags": ["Bitcoin"]},
            {"ticker": "KXBTC15MEXTRA", "frequency": "fifteen_min", "tags": ["Bitcoin"]},
            {
                "ticker": "KXCRYPTOCOMP15M",
                "frequency": "fifteen_min",
                "tags": ["Head to Heads", "15 Min Markets"],
            },
        ]
    )
    assert await find_crypto_series(client) == ["LEGACY"]


async def test_markets_include_unopened_and_open_paginated_and_deduplicated():
    client = client_with_series(
        [
            {"ticker": "KXBTC15M", "frequency": "fifteen_min"},
            {"ticker": "KXETH15M", "frequency": "fifteen_min"},
        ]
    )
    client.get_markets.side_effect = [
        {"markets": [{"ticker": "KXBTC15M-B"}], "cursor": ""},
        {"markets": [{"ticker": "KXBTC15M-A"}], "cursor": "next"},
        {"markets": [{"ticker": "KXBTC15M-B"}]},
        {"markets": [], "cursor": None},
        {"markets": [{"ticker": "KXETH15M-A"}]},
    ]

    markets = await find_15min_markets(client)

    assert [m["ticker"] for m in markets] == ["KXBTC15M-B", "KXBTC15M-A", "KXETH15M-A"]
    assert [call.kwargs for call in client.get_markets.await_args_list] == [
        {"series_ticker": "KXBTC15M", "status": "unopened", "cursor": None},
        {"series_ticker": "KXBTC15M", "status": "open", "cursor": None},
        {"series_ticker": "KXBTC15M", "status": "open", "cursor": "next"},
        {"series_ticker": "KXETH15M", "status": "unopened", "cursor": None},
        {"series_ticker": "KXETH15M", "status": "open", "cursor": None},
    ]


async def test_no_matching_series_warns_and_skips_market_calls(caplog):
    client = client_with_series([])
    assert await find_15min_markets(client) == []
    assert "No 15-minute crypto series found" in caplog.text
    client.get_markets.assert_not_awaited()


async def test_missing_coin_warns_only_for_that_coin(caplog):
    client = client_with_series([{"ticker": "KXETH15M", "frequency": "fifteen_min"}])
    client.get_markets.return_value = {"markets": [{"ticker": "KXETH15M-A"}]}
    await find_15min_markets(client)
    assert "Expected BTC 15-minute markets missing" in caplog.text
    assert "Expected ETH" not in caplog.text


async def test_api_errors_propagate():
    client = AsyncMock(spec=KalshiRestClient)
    client.get_series_list.side_effect = httpx.ConnectError("unavailable")
    with pytest.raises(httpx.ConnectError, match="unavailable"):
        await find_15min_markets(client)
