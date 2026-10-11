from __future__ import annotations

import random


class LatencyModel:
    def __init__(self, latency_ms: int = 150, jitter_ms: int = 0, seed: int = 0):
        if latency_ms < 0 or jitter_ms < 0:
            raise ValueError("latency and jitter must be non-negative")
        self.latency_ms = latency_ms
        self.jitter_ms = jitter_ms
        self.rng = random.Random(seed)

    def sample(self) -> int:
        return max(0, self.latency_ms + self.rng.randint(-self.jitter_ms, self.jitter_ms))
