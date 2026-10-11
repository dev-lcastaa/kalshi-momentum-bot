from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from kmb.envelope import (
    KIND_FRAME,
    KIND_TRANSPORT,
    SOURCE_COLLECTOR,
    SOURCE_KALSHI,
    STREAM_KALSHI_WS,
    Sequencer,
    make_envelope,
)
from kmb.kalshi.discovery import find_15min_markets
from kmb.kalshi.ws import KalshiWsClient, Subscription, WsFrame

logger = logging.getLogger(__name__)
Emit = Callable[[dict[str, Any]], None]


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _market_obj(payload: dict[str, Any]) -> dict[str, Any]:
    nested = payload.get("market")
    return nested if isinstance(nested, dict) else payload


class KalshiFeed:
    def __init__(
        self,
        rest: Any,
        ws: KalshiWsClient,
        emit: Emit,
        *,
        series: tuple[str, ...],
        discovery_interval: int = 60,
        snapshot_interval: int = 45,
    ) -> None:
        self.rest = rest
        self.ws = ws
        self.emit = emit
        self.series = series
        self.discovery_interval = discovery_interval
        self.snapshot_interval = snapshot_interval
        self.markets: dict[str, dict[str, Any]] = {}
        self.subscribed: set[str] = set()
        self.ws_reconnects = 0
        self.ws_gaps = 0
        self.last_ws_event_mono: float | None = None
        self._last_snapshot: dict[str, float] = {}
        self._open_snapshots: set[str] = set()
        self._close_snapshots: set[str] = set()
        self._closed_seen: dict[str, float] = {}
        self._stop = asyncio.Event()

    def on_frame(self, frame: WsFrame) -> None:
        self.last_ws_event_mono = time.monotonic()
        self.emit(
            make_envelope(
                self._sequencer,
                capture_id=self._capture_id,
                stream=STREAM_KALSHI_WS,
                kind=KIND_FRAME,
                source=SOURCE_KALSHI,
                raw=frame.text,
                conn_id=frame.conn_id,
                recv_wall_ms=frame.recv_wall_ms,
                recv_mono_ns=frame.recv_mono_ns,
            )
        )

    def configure_capture(self, sequencer: Sequencer, capture_id: str) -> None:
        self._sequencer = sequencer
        self._capture_id = capture_id

    def on_transport(self, event: dict[str, Any]) -> None:
        self.last_ws_event_mono = time.monotonic()
        name = str(event.get("event", "transport"))
        if name == "disconnected":
            self.ws_reconnects += 1
        elif name in {"seq_gap", "gap"}:
            self.ws_gaps += 1
        data = dict(event)
        self.emit(
            make_envelope(
                self._sequencer,
                capture_id=self._capture_id,
                stream="kalshi_ws",
                kind=KIND_TRANSPORT,
                source=SOURCE_COLLECTOR,
                data=data,
                conn_id=event.get("conn_id"),
                recv_wall_ms=event.get("wall_ms"),
                recv_mono_ns=event.get("mono_ns"),
            )
        )

    async def run(self) -> None:
        self.configure_capture(self._sequencer, self._capture_id)
        try:
            await self.discover()
        except Exception:
            logger.exception("initial Kalshi market discovery failed; retrying in background")
        for series_ticker in self.series:
            try:
                await self.rest.get_series(series_ticker)
            except Exception:
                logger.exception("series metadata request failed for %s", series_ticker)
        ticker_list = sorted(self.subscribed)
        await self.ws.subscribe(
            Subscription(
                key="markets",
                channels=["orderbook_delta", "ticker", "trade", "market_lifecycle_v2"],
                market_tickers=ticker_list,
            )
        )
        coin_indexes = []
        if any("BTC" in name for name in self.series):
            coin_indexes.append("BRTI")
        if any("ETH" in name for name in self.series):
            coin_indexes.append("ETHUSD_RTI")
        if coin_indexes:
            try:
                await self.ws.subscribe(
                    Subscription(
                        key="cfbenchmarks",
                        channels=["cfbenchmarks_value", "cfbenchmarks_value_5hz"],
                        index_ids=coin_indexes,
                    )
                )
            except Exception:
                logger.exception("unable to subscribe to CF Benchmarks; continuing without it")
        ws_task = asyncio.create_task(self.ws.run_forever(), name="kalshi-ws")
        next_discovery = time.monotonic() + self.discovery_interval
        try:
            while not self._stop.is_set():
                if time.monotonic() >= next_discovery:
                    try:
                        await self.discover()
                    except Exception:
                        logger.exception("Kalshi market discovery failed")
                    next_discovery = time.monotonic() + self.discovery_interval
                await self._snapshots_and_settlement()
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=1)
                except TimeoutError:
                    pass
        finally:
            self.ws.stop()
            ws_task.cancel()
            await asyncio.gather(ws_task, return_exceptions=True)

    async def stop(self) -> None:
        self._stop.set()
        self.ws.stop()

    async def discover(self) -> None:
        coins = tuple(name[2:-3] for name in self.series)
        found = await find_15min_markets(self.rest, coins=coins, statuses=("unopened", "open"))
        now = datetime.now(UTC)
        desired: set[str] = set()
        for item in found:
            ticker = str(item.get("ticker", ""))
            if not ticker:
                continue
            self.markets[ticker] = item
            market_open = _parse_time(item.get("open_time"))
            market_close = _parse_time(item.get("close_time"))
            if (
                market_open
                and market_open <= now + timedelta(minutes=35)
                and (market_close is None or market_close >= now - timedelta(minutes=5))
            ):
                desired.add(ticker)
            if ticker in desired:
                try:
                    self.markets[ticker] = _market_obj(await self.rest.get_market(ticker))
                except Exception:
                    logger.exception("market metadata refresh failed for %s", ticker)
        add = sorted(desired - self.subscribed)
        remove = sorted(self.subscribed - desired)
        if add:
            try:
                await self.ws.add_markets("markets", add)
            except (KeyError, ConnectionError):
                # Declarative subscriptions are replayed after reconnect.
                sub = self.ws.subscriptions.get("markets")
                if sub is not None:
                    sub.market_tickers = sorted(set(sub.market_tickers or []) | set(add))
            self.subscribed.update(add)
        if remove:
            try:
                await self.ws.remove_markets("markets", remove)
            except (KeyError, ConnectionError):
                pass
            self.subscribed.difference_update(remove)
        self.emit_control("discovery", {"markets": sorted(desired), "added": add, "removed": remove})

    async def _snapshots_and_settlement(self) -> None:
        now = datetime.now(UTC)
        mono = time.monotonic()
        for ticker in sorted(self.subscribed):
            item = _market_obj(self.markets.get(ticker, {}))
            opened = _parse_time(item.get("open_time"))
            closed = _parse_time(item.get("close_time"))
            last = self._last_snapshot.get(ticker, mono - self.snapshot_interval)
            at_open = (
                ticker not in self._open_snapshots
                and opened
                and opened <= now <= opened + timedelta(minutes=2)
            )
            at_close = (
                ticker not in self._close_snapshots
                and closed
                and closed <= now <= closed + timedelta(minutes=2)
            )
            if at_open or at_close or mono - last >= self.snapshot_interval:
                try:
                    await self.rest.get_market_orderbook(ticker)
                    self._last_snapshot[ticker] = mono
                    if at_open:
                        self._open_snapshots.add(ticker)
                    if at_close:
                        self._close_snapshots.add(ticker)
                except Exception:
                    logger.exception("orderbook snapshot failed for %s", ticker)
        for ticker, item in list(self.markets.items()):
            market = _market_obj(item)
            close = _parse_time(market.get("close_time"))
            if not close or now < close or market.get("result"):
                continue
            started = self._closed_seen.setdefault(ticker, mono)
            if mono - started > 15 * 60:
                continue
            last_poll = self._last_snapshot.get(ticker + ":settlement", 0)
            if mono - last_poll < 15:
                continue
            self._last_snapshot[ticker + ":settlement"] = mono
            try:
                response = await self.rest.get_market(ticker)
                payload = _market_obj(response)
                self.markets[ticker] = payload
            except Exception:
                logger.exception("settlement polling failed for %s", ticker)

    def emit_control(self, event: str, fields: dict[str, Any]) -> None:
        self.emit(
            make_envelope(
                self._sequencer,
                capture_id=self._capture_id,
                stream="control",
                kind="control",
                source=SOURCE_COLLECTOR,
                data={"event": event, **fields},
            )
        )
