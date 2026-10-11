from __future__ import annotations

from decimal import Decimal

from kmb.kalshi.messages import ZERO

QUEUE_MODELS = ("front", "pro_rata", "back")


def queue_decrease(ahead: Decimal, behind: Decimal, decrease: Decimal, model: str) -> Decimal:
    if decrease <= ZERO or ahead <= ZERO:
        return max(ZERO, ahead)
    if model == "front":
        return max(ZERO, ahead - decrease)
    if model == "back":
        return max(ZERO, ahead - max(ZERO, decrease - behind))
    total = ahead + behind
    if total <= ZERO:
        return ZERO
    return max(ZERO, ahead - min(ahead, decrease * ahead / total))
