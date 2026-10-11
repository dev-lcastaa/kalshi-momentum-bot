from fastapi.testclient import TestClient

from kmb.collector.live import LiveMarkets
from kmb.collector.status import create_status_app


def test_both_current_coins_and_window_rotation():
    live = LiveMarkets()
    series = ("KXBTC15M", "KXETH15M")
    markets = {}
    for coin in series:
        for suffix, opened, closed in (
            ("old", "2026-10-11T00:00:00Z", "2026-10-11T00:15:00Z"),
            ("now", "2026-10-11T00:15:00Z", "2026-10-11T00:30:00Z"),
            ("next", "2026-10-11T00:30:00Z", "2026-10-11T00:45:00Z"),
        ):
            ticker = f"{coin}-{suffix}"
            markets[ticker] = dict(ticker=ticker, open_time=opened, close_time=closed, status="active",
                                   floor_strike="62500")
    now = 1791678300000  # 2026-10-11 00:25 UTC
    live.apply(dict(kind="frame", stream="kalshi_ws", msg_type="ticker", recv_wall_ms=now,
                    raw='{"type":"ticker","msg":{"market_ticker":"KXBTC15M-now",'
                        '"yes_bid_dollars":"0.45","yes_ask_dollars":"0.47","price_dollars":"0.46"}}'))
    live.apply(dict(kind="frame", stream="coinbase_ws", recv_wall_ms=now,
                    raw='{"type":"ticker","product_id":"ETH-USD","price":"3200.50"}'))
    result = live.snapshot(markets, series, now_ms=now, kalshi_connected=True, coinbase_connected=True)
    btc, eth = result["markets"]
    assert btc["market_ticker"] == "KXBTC15M-now"
    assert eth["market_ticker"] == "KXETH15M-now"
    assert btc["yes_bid"] == "0.45" and btc["yes_ask"] == "0.47"
    assert btc["quote_ms"] == now
    assert eth["coinbase"] == "3200.50" and eth["coinbase_ms"] == now
    assert eth["yes_bid"] is None  # Never substitute another coin's quote.
    rotated = live.snapshot(markets, series, now_ms=now + 300_000,
                            kalshi_connected=False, coinbase_connected=False)
    assert [m["market_ticker"] for m in rotated["markets"]] == [
        "KXBTC15M-next", "KXETH15M-next",
    ]
    assert rotated["markets"][0]["yes_bid"] is None
    assert rotated["kalshi_connected"] is False


def test_missing_market_keeps_both_coin_cards():
    result = LiveMarkets().snapshot({}, ("KXBTC15M", "KXETH15M"), now_ms=1,
                                   kalshi_connected=False, coinbase_connected=False)
    assert len(result["markets"]) == 2
    assert all(m["status"] == "waiting" and m["market_ticker"] is None for m in result["markets"])


def test_status_api_exposes_live_snapshot():
    payload = {"markets": [{"series_ticker": "KXBTC15M"}, {"series_ticker": "KXETH15M"}]}
    app = create_status_app(lambda: {}, lambda: True, lambda: payload)
    assert TestClient(app).get("/live-markets").json() == payload
    missing = create_status_app(lambda: {}, lambda: True)
    assert TestClient(missing).get("/live-markets").status_code == 503
