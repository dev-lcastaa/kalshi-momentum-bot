from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from kmb.db import init_db, make_engine, session_factory, session_scope
from kmb.db.models import CollectorStatus, MarketResult, MarketRow, SeriesRow
from kmb.kalshi.rest import RestResponse

logger = logging.getLogger(__name__)


def _datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


class DatabaseSink:
    def __init__(self, url: str) -> None:
        self.engine = make_engine(url)
        self.sessions = session_factory(self.engine)
        self.ok = False
        self.errors = 0

    def initialize(self) -> None:
        init_db(self.engine)
        self.ok = True

    def close(self) -> None:
        self.engine.dispose()

    def market_response(self, response: RestResponse) -> None:
        try:
            payload = response.json()
        except ValueError:
            return
        if response.path.startswith("/series/"):
            raw = payload.get("series", payload) if isinstance(payload, dict) else {}
            ticker = raw.get("ticker") if isinstance(raw, dict) else None
            if ticker:
                with session_scope(self.sessions) as session:
                    row = session.get(SeriesRow, ticker)
                    if row is None:
                        row = SeriesRow(ticker=ticker)
                        session.add(row)
                    row.title = raw.get("title")
                    row.raw = raw
            return
        if not response.path.startswith("/markets/") or response.path.endswith("/orderbook"):
            return
        market = payload.get("market", payload) if isinstance(payload, dict) else {}
        ticker = market.get("ticker") if isinstance(market, dict) else None
        if not ticker:
            return
        series = str(market.get("series_ticker") or ticker.split("-", 1)[0])
        with session_scope(self.sessions) as session:
            row = session.get(MarketRow, ticker)
            if row is None:
                row = MarketRow(ticker=ticker, series_ticker=series)
                session.add(row)
            row.series_ticker = series
            row.event_ticker = market.get("event_ticker")
            row.open_time = _datetime(market.get("open_time"))
            row.close_time = _datetime(market.get("close_time"))
            row.strike = _decimal(market.get("floor_strike") or market.get("strike"))
            row.status = market.get("status")
            row.price_level_structure = market.get("price_level_structure")
            row.raw = market
            result = market.get("result")
            if isinstance(result, str) and result in {"yes", "no", "void"}:
                known = session.get(MarketResult, ticker)
                if known is None:
                    known = MarketResult(ticker=ticker, series_ticker=series, result=result)
                    session.add(known)
                known.result = result
                known.expiration_value = market.get("expiration_value")
                known.strike = row.strike
                known.close_time = row.close_time
                known.settled_time = _datetime(market.get("settled_time"))
                known.raw = market

    def write_status(self, collector_id: str, capture_id: str, status: dict[str, Any]) -> None:
        with session_scope(self.sessions) as session:
            row = session.get(CollectorStatus, collector_id)
            if row is None:
                row = CollectorStatus(collector_id=collector_id, capture_id=capture_id, status=status)
                session.add(row)
            else:
                row.capture_id = capture_id
                row.status = status
