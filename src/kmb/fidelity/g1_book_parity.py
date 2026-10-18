from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from decimal import Decimal

from kmb.book import BookManager, OrderBook
from kmb.kalshi.messages import D
from kmb.strategy.base import Event


def _levels(rows) -> dict[Decimal, Decimal]:
    levels: dict[Decimal, Decimal] = {}
    for price, count in rows or []:
        price_value, count_value = D(str(price)), D(str(count))
        if count_value > 0:
            levels[price_value] = levels.get(price_value, Decimal(0)) + count_value
    return levels


def _snapshot(book, raw) -> bool:
    orderbook = raw.get("orderbook_fp", raw.get("orderbook", raw))
    yes = _levels(orderbook.get("yes_dollars_fp", orderbook.get("yes_dollars", [])))
    no = _levels(orderbook.get("no_dollars_fp", orderbook.get("no_dollars", [])))
    return book.yes == yes and book.no == no


def _bbo(book, msg) -> bool:
    for key, actual in (("yes_bid_dollars", book.best_yes_bid()), ("yes_ask_dollars", book.best_yes_ask())):
        if key not in msg:
            continue
        expected = None if msg[key] is None else D(str(msg[key]))
        observed = actual[0] if actual else None
        if expected != observed:
            return False
    return True


HISTORY_MS = 10_000  # REST requests and ticker frames are compared against recent book states only
TICKER_LAG_MS = 1_000  # a ticker frame describes the book slightly before the deltas that follow it
TICKER_MATCH_MIN = 0.95


def _states_during(states, sent: int, recv: int):
    prior = None
    for ts, state in states:
        if ts < sent:
            prior = (ts, state)
        elif ts <= recv:
            if prior is not None:
                yield prior
                prior = None
            yield ts, state
    if prior is not None:
        yield prior


def run_g1(events: Iterable[Event]) -> dict:
    books = BookManager(strict_seq=False)
    history: dict[str, deque[tuple[int, dict]]] = {}
    mismatches: list[dict] = []
    n_snapshots = n_exact = n_ticker = n_ticker_match = 0
    for ev in events:
        payload = ev.data if isinstance(ev.data, dict) else {}
        if ev.stream == "kalshi_ws" and ev.kind == "frame":
            books.on_message(payload, ev.ts_ms)
            msg = payload.get("msg") or {}
            ticker = msg.get("market_ticker") or ev.market_ticker
            if ticker and books.book(ticker).valid:
                book = books.book(ticker)
                states = history.setdefault(ticker, deque())
                states.append((ev.ts_ms, {"yes": dict(book.yes), "no": dict(book.no)}))
                while states and states[0][0] < ev.ts_ms - HISTORY_MS:
                    states.popleft()
            if ev.msg_type == "ticker" and ticker:
                n_ticker += 1
                book = books.book(ticker)
                if _bbo(book, msg) or any(
                    ev.ts_ms - TICKER_LAG_MS <= ts <= ev.ts_ms
                    and _bbo(OrderBook(ticker, state["yes"], state["no"]), msg)
                    for ts, state in history.get(ticker, ())
                ):
                    n_ticker_match += 1
                else:
                    mismatches.append(
                        {
                            "type": "ticker_bbo",
                            "ticker": ticker,
                            "ts_ms": ev.ts_ms,
                            "expected": {k: msg.get(k) for k in ("yes_bid_dollars", "yes_ask_dollars")},
                            "actual": book.to_dict(),
                        }
                    )
        elif ev.stream == "kalshi_rest" and ev.msg_type == "rest.orderbook":
            n_snapshots += 1
            body = payload.get("response", payload)
            meta = getattr(ev, "envelope_data", None) or payload.get("_envelope_data") or {}
            ticker = ev.market_ticker
            current = books.book(ticker) if ticker else None
            matched = bool(current and _snapshot(current, body))
            if not matched and ticker:
                sent = int(meta.get("sent_wall_ms", ev.ts_ms))
                recv = int(meta.get("recv_wall_ms", ev.ts_ms))
                target = body.get("orderbook_fp", body.get("orderbook", body))
                expected_yes = _levels(target.get("yes_dollars_fp", target.get("yes_dollars", [])))
                expected_no = _levels(target.get("no_dollars_fp", target.get("no_dollars", [])))
                # The response reflects the book at some instant in [sent, recv]: the state already in effect
                # when the request was sent counts too, not only states created while it was in flight.
                matched = any(
                    state["yes"] == expected_yes and state["no"] == expected_no
                    for ts, state in _states_during(history.get(ticker, ()), sent, recv)
                )
            if matched:
                n_exact += 1
            else:
                mismatches.append(
                    {
                        "type": "rest_depth",
                        "ticker": ticker,
                        "ts_ms": ev.ts_ms,
                        "sent_wall_ms": meta.get("sent_wall_ms"),
                        "recv_wall_ms": meta.get("recv_wall_ms"),
                        "expected": body.get("orderbook_fp", body),
                        "actual": current.to_dict() if current else None,
                    }
                )
    return {
        "gate": "G1",
        # REST depth must match exactly; ticker summaries lag the book slightly, so allow a small residue.
        "passed": n_exact == n_snapshots and n_ticker_match >= n_ticker * TICKER_MATCH_MIN,
        "n_snapshots": n_snapshots,
        "n_exact": n_exact,
        "n_mismatch": n_snapshots - n_exact,
        "n_ticker": n_ticker,
        "n_ticker_match": n_ticker_match,
        "mismatches": mismatches[:50],
        "gaps": books.gaps,
    }
