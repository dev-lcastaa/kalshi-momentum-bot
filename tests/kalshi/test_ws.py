import asyncio
import json

import pytest

from kmb.kalshi.ws import KalshiWsClient, Subscription


class FakeWs:
    def __init__(self, frames: list[str], *, hold_open: bool = True):
        self.inbound: asyncio.Queue[str] = asyncio.Queue()
        for frame in frames:
            self.inbound.put_nowait(frame)
        self.sent: list[dict] = []
        self.closed = False
        self.hold_open = hold_open

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def recv(self) -> str:
        if self.inbound.empty() and not self.hold_open:
            raise ConnectionResetError("server closed")
        return await self.inbound.get()

    async def close(self) -> None:
        self.closed = True


class FakeConnector:
    def __init__(self, connections: list[FakeWs]):
        self.connections = list(connections)
        self.calls: list[tuple[str, dict]] = []

    async def __call__(self, url, headers):
        self.calls.append((url, headers))
        return self.connections.pop(0)


def snapshot(sid: int, seq: int) -> str:
    return json.dumps({"type": "orderbook_snapshot", "sid": sid, "seq": seq, "msg": {"market_ticker": "T"}})


def delta(sid: int, seq: int) -> str:
    return json.dumps({"type": "orderbook_delta", "sid": sid, "seq": seq, "msg": {"market_ticker": "T"}})


async def wait_for(predicate, timeout: float = 2.0) -> None:  # noqa: ASYNC109
    async with asyncio.timeout(timeout):
        while not predicate():  # noqa: ASYNC110 - polling test helper
            await asyncio.sleep(0.005)


async def test_frames_are_observed_raw_before_parsing_and_subscriptions_sent_on_connect():
    raw_delta = '{"type":"orderbook_delta", "sid":7,"seq":2 ,"msg":{"market_ticker":"T"}}'
    ws = FakeWs(
        [
            json.dumps({"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 7}}),
            snapshot(7, 1),
            raw_delta,
            "not json",
        ]
    )
    frames, messages, transport = [], [], []

    async def on_message(data, frame):
        messages.append(data)

    client = KalshiWsClient(
        "wss://x/trade-api/ws/v2",
        None,
        on_message=on_message,
        on_frame=frames.append,
        on_transport=transport.append,
        connector=FakeConnector([ws]),
    )
    sub = Subscription("books", ["orderbook_delta"], market_tickers=["T"])
    await client.subscribe(sub)
    task = asyncio.create_task(client.run_forever())
    await wait_for(lambda: len(frames) == 4)
    client.stop()
    await task

    assert ws.sent == [
        {"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": ["T"]}}
    ]
    assert frames[2].text == raw_delta
    assert frames[3].text == "not json"
    assert len({f.conn_id for f in frames}) == 1
    assert [m["type"] for m in messages] == ["subscribed", "orderbook_snapshot", "orderbook_delta"]
    events = [e["event"] for e in transport]
    assert events[:3] == ["connecting", "connected", "sent"]
    assert events[-1] == "disconnected"
    assert transport[-1]["reason"] == "stopped"
    assert ws.closed


async def test_seq_gap_reports_and_reconnects_with_fresh_subscription():
    first = FakeWs(
        [
            json.dumps({"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 3}}),
            snapshot(3, 1),
            delta(3, 2),
            delta(3, 5),
        ]
    )
    second = FakeWs([snapshot(4, 1)])
    connector = FakeConnector([first, second])
    transport = []
    client = KalshiWsClient(
        "wss://x", None, on_transport=transport.append, connector=connector, max_backoff=0.01
    )
    await client.subscribe(Subscription("books", ["orderbook_delta"], market_tickers=["T"]))
    task = asyncio.create_task(client.run_forever())
    await wait_for(lambda: len(second.sent) == 1, timeout=5)
    client.stop()
    await task

    gap = next(e for e in transport if e["event"] == "seq_gap")
    assert (gap["sid"], gap["expected"], gap["got"]) == (3, 3, 5)
    disconnect = next(e for e in transport if e["event"] == "disconnected")
    assert disconnect["reason"] == "seq_gap"
    assert second.sent[0]["cmd"] == "subscribe"
    conn_ids = {e["conn_id"] for e in transport if e["event"] == "connected"}
    assert len(conn_ids) == 2


async def test_gap_handler_can_suppress_reconnect():
    ws = FakeWs([snapshot(3, 1), delta(3, 4), delta(3, 5)])
    gaps = []

    def on_gap(sid, expected, got):
        gaps.append((sid, expected, got))
        return False

    frames = []
    client = KalshiWsClient(
        "wss://x", None, on_frame=frames.append, on_gap=on_gap, connector=FakeConnector([ws])
    )
    task = asyncio.create_task(client.run_forever())
    await wait_for(lambda: len(frames) == 3)
    client.stop()
    await task
    assert gaps == [(3, 2, 4)]


async def test_add_and_remove_markets_use_update_subscription_with_acked_sids():
    ws = FakeWs([json.dumps({"id": 1, "type": "subscribed", "msg": {"channel": "ticker", "sid": 9}})])
    client = KalshiWsClient("wss://x", None, connector=FakeConnector([ws]))
    sub = Subscription("tick", ["ticker"], market_tickers=["A"])
    await client.subscribe(sub)
    task = asyncio.create_task(client.run_forever())
    await wait_for(lambda: sub.sids == {"ticker": 9})

    await client.add_markets("tick", ["A", "B"])
    await client.remove_markets("tick", ["A"])
    client.stop()
    await task

    assert ws.sent[1:] == [
        {
            "id": 2,
            "cmd": "update_subscription",
            "params": {"sids": [9], "market_tickers": ["B"], "action": "add_markets"},
        },
        {
            "id": 3,
            "cmd": "update_subscription",
            "params": {"sids": [9], "market_tickers": ["A"], "action": "delete_markets"},
        },
    ]
    assert sub.market_tickers == ["B"]
    assert sub.sids == {}


async def test_market_updates_send_one_command_per_channel_sid():
    ws = FakeWs(
        [
            json.dumps({"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 4}}),
            json.dumps({"id": 1, "type": "subscribed", "msg": {"channel": "ticker", "sid": 2}}),
            json.dumps({"id": 1, "type": "subscribed", "msg": {"channel": "trade", "sid": 7}}),
        ]
    )
    client = KalshiWsClient("wss://x", None, connector=FakeConnector([ws]))
    sub = Subscription("markets", ["orderbook_delta", "ticker", "trade"], market_tickers=["A"])
    await client.subscribe(sub)
    task = asyncio.create_task(client.run_forever())
    await wait_for(lambda: len(sub.sids) == 3)

    await client.add_markets("markets", ["B"])
    await client.remove_markets("markets", ["A"])
    client.stop()
    await task

    updates = [msg for msg in ws.sent if msg["cmd"] == "update_subscription"]
    assert [(msg["params"]["sids"], msg["params"]["action"]) for msg in updates] == [
        ([2], "add_markets"),
        ([4], "add_markets"),
        ([7], "add_markets"),
        ([2], "delete_markets"),
        ([4], "delete_markets"),
        ([7], "delete_markets"),
    ]
    assert all(len(msg["params"]["sids"]) == 1 for msg in updates)
    assert [msg["params"]["market_tickers"] for msg in updates] == [["B"]] * 3 + [["A"]] * 3


async def test_markets_added_while_disconnected_are_included_on_next_subscribe():
    client = KalshiWsClient("wss://x", None, connector=FakeConnector([]))
    sub = Subscription("tick", ["ticker"], market_tickers=["A"])
    await client.subscribe(sub)
    await client.add_markets("tick", ["B"])
    assert sub.params()["market_tickers"] == ["A", "B"]


async def test_command_errors_are_reported():
    ws = FakeWs([json.dumps({"id": 1, "type": "error", "msg": {"code": 6, "msg": "Already subscribed"}})])
    transport = []
    client = KalshiWsClient("wss://x", None, on_transport=transport.append, connector=FakeConnector([ws]))
    await client.subscribe(Subscription("t", ["ticker"]))
    task = asyncio.create_task(client.run_forever())
    await wait_for(lambda: any(e["event"] == "command_error" for e in transport))
    client.stop()
    await task
    err = next(e for e in transport if e["event"] == "command_error")
    assert err["cmd"] == "subscribe"
    assert err["msg"]["code"] == 6


async def test_idle_timeout_forces_reconnect():
    first, second = FakeWs([]), FakeWs([])
    connector = FakeConnector([first, second])
    transport = []
    client = KalshiWsClient(
        "wss://x",
        None,
        on_transport=transport.append,
        connector=connector,
        idle_timeout=0.05,
        max_backoff=0.01,
    )
    task = asyncio.create_task(client.run_forever())
    await wait_for(lambda: len(connector.calls) == 2, timeout=5)
    client.stop()
    await task
    assert first.closed


async def test_duplicate_subscription_key_rejected():
    client = KalshiWsClient("wss://x", None, connector=FakeConnector([]))
    await client.subscribe(Subscription("a", ["ticker"]))
    with pytest.raises(ValueError):
        await client.subscribe(Subscription("a", ["trade"]))
