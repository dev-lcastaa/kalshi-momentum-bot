from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable

from kmb.book import BookManager
from kmb.kalshi.messages import ZERO, D
from kmb.strategy.base import Event


def run_g2(events: Iterable[Event], *, event_window: int = 10, time_window_ms: int = 1000) -> dict:
    rows = list(events)
    books = BookManager()
    pending = defaultdict(deque)
    matched: set[int] = set()
    mismatches: list[dict] = []
    trades: dict[int, dict] = {}

    def expire(current_index: int, current_ts: int) -> None:
        for key, queue in list(pending.items()):
            while queue and queue[0]["remaining"] == ZERO:
                queue.popleft()
            while queue and (
                current_index - queue[0]["index"] > event_window
                or current_ts - queue[0]["ts_ms"] > time_window_ms
            ):
                trade = queue.popleft()
                mismatches.append(
                    {
                        "trade_id": trade["trade_id"],
                        "ticker": key[0],
                        "ts_ms": trade["ts_ms"],
                        "side": key[1],
                        "price": str(key[2]),
                        "trade_count": str(trade["remaining"]),
                        "delta_decrease": str(trade["count"] - trade["remaining"]),
                        "reason": "insufficient_following_delta",
                    }
                )
            if not queue:
                pending.pop(key, None)

    for index, ev in enumerate(rows):
        expire(index, ev.ts_ms)
        if ev.stream != "kalshi_ws" or ev.kind != "frame":
            continue
        payload = ev.data if isinstance(ev.data, dict) else {}
        books.on_message(payload, ev.ts_ms)
        msg = payload.get("msg") or {}
        ticker = msg.get("market_ticker") or ev.market_ticker
        if ev.msg_type == "trade":
            taker_side = msg.get("taker_side")
            if taker_side == "yes":
                side, price = "no", D(str(msg.get("no_price_dollars")))
            elif taker_side == "no":
                side, price = "yes", D(str(msg.get("yes_price_dollars")))
            else:
                mismatches.append({"trade_id": msg.get("trade_id"), "reason": "unknown taker_side"})
                continue
            count = D(str(msg.get("count_fp", msg.get("count", "0"))))
            key = (ticker, side, price)
            trades[index] = {
                "index": index,
                "ts_ms": ev.ts_ms,
                "trade_id": msg.get("trade_id"),
                "count": count,
                "remaining": count,
            }
            pending[key].append(trades[index])
        elif ev.msg_type == "orderbook_delta":
            delta = D(str(msg.get("delta_fp", msg.get("delta", "0"))))
            if delta >= ZERO:
                continue
            side, price = msg.get("side"), D(str(msg.get("price_dollars")))
            key = (ticker, side, price)
            volume = -delta
            queue = pending.get(key, ())
            for trade in queue:
                used = min(volume, trade["remaining"])
                trade["remaining"] -= used
                volume -= used
                if trade["remaining"] == ZERO:
                    matched.add(trade["index"])
                if volume == ZERO:
                    break
            if queue:
                while queue and queue[0]["remaining"] == ZERO:
                    queue.popleft()
                if not queue:
                    pending.pop(key, None)
    expire(len(rows) + event_window + 1, rows[-1].ts_ms + time_window_ms + 1 if rows else 0)
    unknown_side = sum(1 for item in mismatches if item.get("reason") == "unknown taker_side")
    n_trades = len(trades) + unknown_side
    n_matched = len(matched)
    return {
        "gate": "G2",
        "passed": n_matched == n_trades,
        "n_trades": n_trades,
        "n_matched": n_matched,
        "n_mismatch": n_trades - n_matched,
        "mismatches": mismatches[:50],
        "gaps": books.gaps,
    }
