import json

from kmb.book import BookManager
from kmb.fidelity.events import event_from_row
from kmb.fidelity.g1_book_parity import run_g1
from kmb.replay.state import MarketState

T = "KXBTC15M-26JAN011200-00"


def frame(msg_type: str, seq: int, msg: dict) -> dict:
    return {"type": msg_type, "sid": 1, "seq": seq, "msg": {"market_ticker": T, **msg}}


SNAPSHOT = frame("orderbook_snapshot", 1, {"yes_dollars_fp": [["0.40", "2.00"]], "no_dollars_fp": []})
# Seq jumps because the other markets' frames on the same sid were filtered out of this window.
DELTA = frame("orderbook_delta", 9, {"side": "yes", "price_dollars": "0.41", "delta_fp": "5.00"})


def test_strict_manager_invalidates_on_seq_jump_but_replay_manager_does_not():
    strict = BookManager()
    strict.on_message(SNAPSHOT)
    strict.on_message(DELTA)
    assert strict.gaps == 1 and not strict.book(T).valid

    replay = BookManager(strict_seq=False)
    replay.on_message(SNAPSHOT)
    replay.on_message(DELTA)
    assert replay.gaps == 0 and replay.book(T).valid
    assert replay.book(T).best_yes_bid() is not None


def test_replay_state_keeps_book_valid_across_filtered_sequence_numbers():
    state = MarketState()
    for message in (SNAPSHOT, DELTA):
        event = event_from_row(
            {
                "stream": "kalshi_ws",
                "kind": "frame",
                "msg_type": message["type"],
                "market_ticker": T,
                "recv_wall_ms": 10,
                "raw": json.dumps(message),
            }
        )
        state.apply(event)
    assert state.books.book(T).valid and state.gaps == 0


def test_g1_uses_rest_request_timing_from_parquet_rows():
    def row(stream, msg_type, ts, raw, data=None, kind="frame"):
        return event_from_row(
            {
                "stream": stream,
                "kind": kind,
                "msg_type": msg_type,
                "market_ticker": T,
                "recv_wall_ms": ts,
                "raw": json.dumps(raw),
                "data": json.dumps(data) if data else None,
            }
        )

    rest_body = {"orderbook_fp": {"yes_dollars": [["0.40", "2.00"]], "no_dollars": []}}
    stale_rest = row(
        "kalshi_rest",
        "rest.orderbook",
        30,
        rest_body,
        {"sent_wall_ms": 12, "recv_wall_ms": 30},
        kind="rest",
    )
    report = run_g1(
        [
            row("kalshi_ws", "orderbook_snapshot", 10, SNAPSHOT),
            row("kalshi_ws", "orderbook_delta", 20, DELTA),
            stale_rest,
        ]
    )
    # The REST response was generated before the delta was applied, so it must match the earlier state.
    assert report["n_snapshots"] == 1 and report["n_exact"] == 1
    assert report["gaps"] == 0
