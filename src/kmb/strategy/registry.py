"""Strategy registry used by the backend (``GET /api/strategies``) and workers."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from kmb.strategy.base import Strategy
from kmb.strategy.momentum import MomentumStrategy


class NoopStrategy(Strategy):
    name = "noop"
    description = "Does nothing. Useful to test data, workers and plumbing."
    param_schema: list[dict[str, Any]] = []


STRATEGIES: dict[str, type[Strategy]] = {cls.name: cls for cls in (MomentumStrategy, NoopStrategy)}


def create_strategy(name: str, params: dict[str, Any] | None = None) -> Strategy:
    cls = STRATEGIES.get(name)
    if cls is None:
        raise KeyError(f"unknown strategy {name!r}")
    return cls(params)


def describe() -> list[dict[str, Any]]:
    return [
        {
            "name": cls.name,
            "description": getattr(cls, "description", ""),
            "params": getattr(cls, "param_schema", []),
        }
        for cls in STRATEGIES.values()
    ]


def validate_params(name: str, params: dict[str, Any] | None) -> dict[str, Any]:
    """Coerce/validate user params against the strategy's ``param_schema``; raises ``ValueError``.

    Decimals are kept as strings (JSON-safe, exact); unknown keys are rejected.
    """
    cls = STRATEGIES.get(name)
    if cls is None:
        raise ValueError(f"unknown strategy {name!r}")
    schema = {p["name"]: p for p in getattr(cls, "param_schema", [])}
    out: dict[str, Any] = {}
    for key, raw in (params or {}).items():
        spec = schema.get(key)
        if spec is None:
            raise ValueError(f"unknown param {key!r} for strategy {name!r}")
        typ = spec.get("type", "str")
        try:
            if typ == "int":
                if isinstance(raw, bool) or (isinstance(raw, float) and not raw.is_integer()):
                    raise ValueError
                val: Any = int(raw)
                num: Decimal | None = Decimal(val)
            elif typ == "float":
                if isinstance(raw, bool):
                    raise ValueError
                val = float(raw)
                num = Decimal(str(val))
            elif typ == "decimal":
                if isinstance(raw, bool):
                    raise ValueError
                num = Decimal(str(raw))
                if not num.is_finite():
                    raise ValueError
                val = format(num, "f")
            elif typ == "bool":
                if not isinstance(raw, bool):
                    raise ValueError
                val, num = raw, None
            else:
                val, num = str(raw), None
        except (ValueError, TypeError, ArithmeticError):
            raise ValueError(f"param {key!r} must be {typ}") from None
        if num is not None:
            if "min" in spec and num < Decimal(str(spec["min"])):
                raise ValueError(f"param {key!r} must be >= {spec['min']}")
            if "max" in spec and num > Decimal(str(spec["max"])):
                raise ValueError(f"param {key!r} must be <= {spec['max']}")
        out[key] = val
    return out
