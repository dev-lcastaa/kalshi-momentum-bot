"""Window-level conventions shared by the normalizer, data API, replay and backend.

See docs/data-formats.md.
"""

from __future__ import annotations

from dataclasses import dataclass

WINDOW_FORMAT_VERSION = 1
PRE_OPEN_MS = 60_000
POST_CLOSE_MS = 180_000

PARQUET_COLUMNS = (
    "ord",
    "capture_id",
    "local_event_seq",
    "recv_wall_ms",
    "stream",
    "kind",
    "source",
    "conn_id",
    "msg_type",
    "market_ticker",
    "sid",
    "seq",
    "raw",
    "data",
)


@dataclass(frozen=True)
class Coin:
    symbol: str
    series: str
    cf_index: str
    coinbase_pair: str


COINS: dict[str, Coin] = {
    "BTC": Coin("BTC", "KXBTC15M", "BRTI", "BTC-USD"),
    "ETH": Coin("ETH", "KXETH15M", "ETHUSD_RTI", "ETH-USD"),
}
BY_SERIES: dict[str, Coin] = {c.series: c for c in COINS.values()}


def series_of(ticker: str) -> str:
    return ticker.split("-", 1)[0]


def coin_for_ticker(ticker: str) -> Coin | None:
    return BY_SERIES.get(series_of(ticker))


def window_relpath(series: str, open_date: str, ticker: str) -> tuple[str, str]:
    base = f"windows/{series}/{open_date}/{ticker}"
    return base + ".parquet", base + ".window.json"
