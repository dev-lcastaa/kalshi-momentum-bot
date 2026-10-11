from __future__ import annotations

from collections.abc import Iterable

from kmb.fidelity.g1_book_parity import run_g1
from kmb.fidelity.g2_tape import run_g2
from kmb.strategy.base import Event


def run_fidelity(events: Iterable[Event]) -> list[dict]:
    materialized = list(events)
    return [run_g1(materialized), run_g2(materialized)]
