"""Seekable cursor over one window's events, with periodic state checkpoints (default every 5 s)."""

from __future__ import annotations

import bisect
from collections.abc import Sequence

from kmb.replay.state import MarketState, Note
from kmb.strategy.base import Event


class ReplayCursor:
    def __init__(self, events: Sequence[Event], checkpoint_every_ms: int = 5000, start_ms: int | None = None):
        self.events = events
        self.every = checkpoint_every_ms
        self.start_ms = start_ms if start_ms is not None else (events[0].ts_ms if events else 0)
        self.end_ms = events[-1].ts_ms if events else self.start_ms
        self.state = MarketState()
        self.state.now_ms = self.start_ms
        self.index = 0  # next event to apply
        self.now_ms = self.start_ms
        self._cp_ts: list[int] = [self.start_ms]
        self._cp: list[tuple[int, MarketState]] = [(0, self.state.snapshot())]

    @property
    def done(self) -> bool:
        return self.index >= len(self.events) and self.now_ms >= self.end_ms

    def advance_to(self, ts_ms: int) -> tuple[list[Event], list[Note]]:
        """Apply every event with ``ts_ms <= target``; returns (applied events, notes)."""
        applied: list[Event] = []
        notes: list[Note] = []
        events = self.events
        while self.index < len(events) and events[self.index].ts_ms <= ts_ms:
            ev = events[self.index]
            notes.extend(self.state.apply(ev))
            applied.append(ev)
            self.index += 1
            if ev.ts_ms - self._cp_ts[-1] >= self.every and (
                self.index >= len(events) or events[self.index].ts_ms != ev.ts_ms
            ):
                self._cp_ts.append(ev.ts_ms)
                self._cp.append((self.index, self.state.snapshot()))
        self.now_ms = max(self.now_ms, ts_ms)
        self.state.now_ms = self.now_ms
        return applied, notes

    def seek(self, ts_ms: int) -> None:
        ts_ms = max(self.start_ms, min(ts_ms, self.end_ms))
        if ts_ms >= self.now_ms:
            self.advance_to(ts_ms)
            return
        k = bisect.bisect_right(self._cp_ts, ts_ms) - 1
        idx, snap = self._cp[max(k, 0)]
        self.index = idx
        self.state = snap.snapshot()
        self.now_ms = self._cp_ts[max(k, 0)]
        self.advance_to(ts_ms)
        self.now_ms = ts_ms
        self.state.now_ms = ts_ms
