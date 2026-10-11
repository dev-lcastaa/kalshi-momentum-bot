from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from kmb.collector.app import CollectorApp
from kmb.collector.bus import NatsPublisher
from kmb.collector.coinbase_feed import CoinbaseFeed
from kmb.collector.config import CollectorConfig
from kmb.collector.kalshi_feed import KalshiFeed
from kmb.collector.status import create_status_app
from kmb.envelope import KIND_FRAME, SOURCE_KALSHI, Sequencer, make_envelope
from kmb.kalshi.rest import RestResponse
from kmb.kalshi.ws import WsFrame


def test_kalshi_frame_envelope_preserves_raw_and_timestamps() -> None:
    stored = []
    feed = KalshiFeed(None, None, stored.append, series=())
    feed.configure_capture(Sequencer(), "capture")
    raw = '{"type":"ticker","msg":{"market_ticker":"KXBTC15M-test"}}'
    feed.on_frame(WsFrame("conn", raw, 100, 200))
    envelope = stored[0]
    assert envelope["raw"] == raw
    assert envelope["stream"] == "kalshi_ws"
    assert envelope["market_ticker"] == "KXBTC15M-test"
    assert envelope["recv_wall_ms"] == 100
    assert envelope["recv_mono_ns"] == 200


def test_rest_response_envelope_keeps_body_and_explicit_market(tmp_path: Path) -> None:
    config = CollectorConfig(spool_dir=tmp_path, ingest_token="x" * 32)
    app = CollectorApp(config)
    body = '{"orderbook_fp":{"yes_dollars":[]}}'
    app.on_rest_response(
        RestResponse(
            method="GET",
            path="/markets/KXBTC15M-test/orderbook",
            params={},
            request_body=None,
            status=200,
            text=body,
            headers={},
            sent_wall_ms=10,
            recv_wall_ms=11,
            sent_mono_ns=20,
            recv_mono_ns=21,
            attempt=0,
        )
    )
    envelope = app.spool._segments["kalshi_rest"].events[0]
    assert envelope["raw"] == body
    assert envelope["market_ticker"] == "KXBTC15M-test"
    assert envelope["msg_type"] == "rest.orderbook"
    assert envelope["data"] == {
        "endpoint": "orderbook",
        "method": "GET",
        "path": "/markets/KXBTC15M-test/orderbook",
        "params": {},
        "status": 200,
        "sent_wall_ms": 10,
        "recv_wall_ms": 11,
    }
    asyncio.run(app.uploader.close())


async def test_coinbase_feed_records_matches_and_skips_heartbeat() -> None:
    events = []

    class FakeSocket:
        def __init__(self) -> None:
            self.sent = []
            self.frames = [
                '{"type":"heartbeat","product_id":"BTC-USD"}',
                '{"type":"match","product_id":"BTC-USD","price":"10"}',
            ]
            self.closed = False

        async def send(self, payload: str) -> None:
            self.sent.append(json.loads(payload))

        async def recv(self) -> str:
            return self.frames.pop(0)

        async def close(self) -> None:
            self.closed = True

    socket = FakeSocket()

    async def connector(url: str) -> FakeSocket:
        assert url == "wss://ws-feed.exchange.coinbase.com"
        return socket

    feed = CoinbaseFeed(events.append, Sequencer(), "capture", connector=connector)
    original_emit = feed.emit

    def stop_after_frame(envelope):
        original_emit(envelope)
        if envelope["kind"] == "frame":
            feed._stop.set()

    feed.emit = stop_after_frame
    await asyncio.wait_for(feed.run(), timeout=1)
    assert socket.sent[0] == {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["matches", "ticker", "heartbeat"],
    }
    frames = [event for event in events if event["kind"] == "frame"]
    assert len(frames) == 1
    assert json.loads(frames[0]["raw"])["type"] == "match"
    assert frames[0]["msg_type"] == "match"
    assert socket.closed


async def test_discovery_subscribes_open_and_near_future_only() -> None:
    now = datetime.now(UTC)

    def market(ticker: str, offset: int) -> dict:
        opened = now + timedelta(minutes=offset)
        return {
            "ticker": ticker,
            "open_time": opened.isoformat(),
            "close_time": (opened + timedelta(minutes=15)).isoformat(),
        }

    records = [
        market("KXBTC15M-old", -20),
        market("KXBTC15M-open", -3),
        market("KXBTC15M-next", 30),
        market("KXBTC15M-far", 60),
    ]

    class Rest:
        async def get_series_list(self, **kwargs):
            return {"series": [{"ticker": "KXBTC15M", "frequency": "fifteen_min"}]}

        async def get_markets(self, **kwargs):
            return {"markets": records if kwargs["status"] == "unopened" else []}

        async def get_market(self, ticker):
            return {"market": next(m for m in records if m["ticker"] == ticker)}

    class Ws:
        subscriptions = {"markets": SimpleNamespace(market_tickers=[])}

        async def add_markets(self, key, tickers):
            self.subscriptions[key].market_tickers.extend(tickers)

        async def remove_markets(self, key, tickers):
            self.subscriptions[key].market_tickers = [
                t for t in self.subscriptions[key].market_tickers if t not in tickers
            ]

    emitted = []
    feed = KalshiFeed(Rest(), Ws(), emitted.append, series=("KXBTC15M",))
    feed.configure_capture(Sequencer(), "capture")
    await feed.discover()
    assert feed.subscribed == {"KXBTC15M-open", "KXBTC15M-next"}
    assert emitted[0]["data"]["event"] == "discovery"


def test_status_endpoints() -> None:
    app = create_status_app(lambda: {"capture_id": "x"}, lambda: True)
    with TestClient(app) as client:
        assert client.get("/health").json()["ok"]
        assert client.get("/ready").json()["ready"]
        assert client.get("/status").json() == {"capture_id": "x"}


async def test_bus_publisher_uses_jetstream_with_fake_nats() -> None:
    published = []
    configs = []

    class FakeJetStream:
        async def add_stream(self, *, config):
            configs.append(config)

        async def publish(self, subject: str, payload: bytes):
            published.append((subject, json.loads(payload)))

    class FakeNats:
        is_connected = True

        def jetstream(self):
            return FakeJetStream()

        async def publish(self, subject: str, payload: bytes):
            published.append((subject, json.loads(payload)))

        async def close(self):
            self.is_connected = False

    nc = FakeNats()

    async def connector(url: str):
        assert url == "nats://fake"
        return nc

    bus = NatsPublisher("nats://fake", connector=connector)
    task = asyncio.create_task(bus.run())
    for _ in range(100):
        if bus.connected:
            break
        await asyncio.sleep(0.001)
    envelope = make_envelope(
        Sequencer(),
        capture_id="capture",
        stream="kalshi_ws",
        kind=KIND_FRAME,
        source=SOURCE_KALSHI,
        raw='{"type":"ticker","msg":{"market_ticker":"KXBTC15M-test"}}',
    )
    bus.publish(envelope)
    await asyncio.wait_for(bus.queue.join(), timeout=1)
    assert bus.published == 1
    assert published[0][0] == "md.kalshi.KXBTC15M-test"
    assert published[0][1] == envelope
    assert configs[0].name == "MD"
    assert configs[0].subjects == ["md.>"]
    await bus.stop()
    await asyncio.wait_for(task, timeout=1)
