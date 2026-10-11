from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from decimal import Decimal

from kmb.kalshi.messages import D


def _value(fill: dict, *keys: str) -> Decimal:
    for key in keys:
        if key in fill and fill[key] is not None:
            return D(str(fill[key]))
    return D("0")


def _group(fills: Iterable[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for fill in fills:
        client_id = fill.get("client_order_id")
        if client_id is not None:
            grouped[str(client_id)].append(fill)
    for group in grouped.values():
        group.sort(key=lambda f: int(f.get("ts_ms", f.get("created_time_ms", 0))))
    return grouped


def compare_fills(
    real_fills: Iterable[dict], simulated_fills: Iterable[dict], *, maker_tolerance_ms: int = 1000
) -> dict:
    real, sim = _group(real_fills), _group(simulated_fills)
    mismatches = []
    matches = 0
    for client_id in sorted(real.keys() | sim.keys()):
        actual_rows, modeled_rows = real.get(client_id, []), sim.get(client_id, [])
        if len(actual_rows) != len(modeled_rows):
            mismatches.append(
                {
                    "client_order_id": client_id,
                    "reason": "fill_count_mismatch",
                    "n_real": len(actual_rows),
                    "n_simulated": len(modeled_rows),
                }
            )
        for idx, (actual, modeled) in enumerate(zip(actual_rows, modeled_rows, strict=False)):
            fields = {
                "price": (
                    ("price", "yes_price_dollars", "yes_price"),
                    ("price", "yes_price_dollars", "yes_price"),
                ),
                "count": (("count", "count_fp"), ("count", "count_fp")),
                "fee": (("fee", "fee_cost", "fee_paid"), ("fee", "fee_cost", "fee_paid")),
            }
            errors = {}
            for name, (real_keys, sim_keys) in fields.items():
                left, right = _value(actual, *real_keys), _value(modeled, *sim_keys)
                if left != right:
                    errors[name] = {"real": str(left), "simulated": str(right)}
            sim_taker = bool(modeled.get("is_taker", modeled.get("is_maker") is False))
            real_taker = bool(actual.get("is_taker", not actual.get("is_maker", not sim_taker)))
            timing = None
            if not real_taker:
                timing = abs(
                    int(actual.get("ts_ms", actual.get("created_time_ms", 0)))
                    - int(modeled.get("ts_ms", modeled.get("created_time_ms", 0)))
                )
                if timing > maker_tolerance_ms:
                    errors["timing_delta_ms"] = {"real": timing, "tolerance": maker_tolerance_ms}
            if real_taker != sim_taker:
                errors["is_taker"] = {"real": real_taker, "simulated": sim_taker}
            if errors:
                mismatches.append(
                    {
                        "client_order_id": client_id,
                        "fill_index": idx,
                        "fields": errors,
                        "timing_delta_ms": timing,
                    }
                )
            else:
                matches += 1
    return {
        "gate": "G4",
        "passed": not mismatches,
        "n_real": sum(map(len, real.values())),
        "n_simulated": sum(map(len, sim.values())),
        "n_match": matches,
        "n_mismatch": len(mismatches),
        "mismatches": mismatches[:50],
    }
