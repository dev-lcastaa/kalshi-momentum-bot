"""Read-only current-market view from the collector, independent of NATS delivery."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from kmb.replay.events import event_from_record
from kmb.replay.state import MarketState
from kmb.replay.view import refs_for


def _string(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


class LiveMarkets:
    def __init__(self) -> None:
        self.state = MarketState()

    def apply(self, envelope: dict[str, Any]) -> None:
        if envelope.get("msg_type") in ("orderbook_delta", "orderbook_snapshot"):
            return
        self.state.apply(event_from_record(envelope))

    def snapshot(
        self,
        markets: dict[str, dict[str, Any]],
        series: tuple[str, ...],
        *,
        now_ms: int,
        kalshi_connected: bool,
        coinbase_connected: bool,
    ) -> dict[str, Any]:
        for market in markets.values():
            self.state.update_market_from_rest(market)
        items = []
        keep = set()
        for name in series:
            candidates = [
                m for m in self.state.markets.values()
                if m.series == name and m.open_ms is not None and m.close_ms is not None
                and m.open_ms <= now_ms < m.close_ms and m.result is None
                and m.status not in ("closed", "determined", "settled", "finalized")
            ]
            market = max(candidates, key=lambda m: m.open_ms or 0) if candidates else None
            ticker = market.ticker if market else None
            if ticker:
                keep.add(ticker)
            ref_key, cb_key = refs_for(ticker or name)
            quote = self.state.tickers.get(ticker) if ticker else None
            last = self.state.last_trade.get(ticker) if ticker else None
            quote_ms = quote.ts_ms if quote else None
            price_ms = max(quote_ms or 0, last.ts_ms if last else 0) or None
            items.append({
                "series_ticker": name,
                "market_ticker": ticker,
                "open_ms": market.open_ms if market else None,
                "close_ms": market.close_ms if market else None,
                "strike": _string(market.strike) if market else None,
                "status": market.status if market else "waiting",
                "yes_bid": _string(quote.yes_bid) if quote else None,
                "yes_ask": _string(quote.yes_ask) if quote else None,
                "last": _string(last.price if last and last.ts_ms >= (quote_ms or 0)
                                else quote.price if quote else None),
                "quote_ms": quote_ms,
                "price_ms": price_ms,
                "ref": _string(self.state.reference.get(ref_key)) if ref_key else None,
                "ref_ms": self.state.reference_ts_ms.get(ref_key) if ref_key else None,
                "coinbase": _string(self.state.reference.get(cb_key)) if cb_key else None,
                "coinbase_ms": self.state.reference_ts_ms.get(cb_key) if cb_key else None,
            })
        keep.update(markets)
        for values in (self.state.markets, self.state.tickers, self.state.last_trade):
            for ticker in list(values):
                if ticker not in keep:
                    del values[ticker]
        return {
            "ts_ms": now_ms,
            "kalshi_connected": kalshi_connected,
            "coinbase_connected": coinbase_connected,
            "markets": items,
        }
