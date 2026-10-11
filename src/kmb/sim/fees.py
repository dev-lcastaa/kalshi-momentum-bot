from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from kmb.kalshi.messages import FEE_Q, ZERO, D, ceil_to, floor_to


@dataclass(frozen=True)
class FeeModel:
    fee_type: str = "quadratic"
    multiplier: Decimal = Decimal("1")
    coefficient: Decimal = Decimal("0.07")
    maker_fraction: Decimal = Decimal("0.25")
    precision: Decimal = Decimal("0.01")
    flat_fee: Decimal = Decimal("0")

    def _model(self, price: Decimal, count: Decimal, maker: bool) -> Decimal:
        if self.fee_type == "flat":
            return self.flat_fee * count
        if maker and self.fee_type == "quadratic":
            return ZERO
        factor = self.maker_fraction if maker else Decimal(1)
        return self.coefficient * factor * self.multiplier * count * price * (Decimal(1) - price)

    def taker_fee(self, price: Decimal, count: Decimal) -> Decimal:
        return ceil_to(self._model(price, count, False), FEE_Q)

    def maker_fee(self, price: Decimal, count: Decimal) -> Decimal:
        return ceil_to(self._model(price, count, True), FEE_Q)

    def apply_rounding(
        self, revenue: Decimal, trade_fee: Decimal, accumulated: Decimal = ZERO
    ) -> tuple[Decimal, Decimal]:
        rounded_revenue = floor_to(revenue - trade_fee, self.precision)
        rounding_fee = revenue - trade_fee - rounded_revenue
        accrued = accumulated + rounding_fee
        rebate = floor_to(accrued, self.precision)
        net_fee = max(ZERO, trade_fee + rounding_fee - rebate)
        return net_fee, accrued - rebate


@dataclass
class FeeAccumulator:
    accumulated_rounding: Decimal = ZERO

    def charge(self, model: FeeModel, revenue: Decimal, trade_fee: Decimal) -> Decimal:
        fee, self.accumulated_rounding = model.apply_rounding(revenue, trade_fee, self.accumulated_rounding)
        return fee


def fee_model_from_dict(values: dict) -> FeeModel:
    allowed = {"fee_type", "multiplier", "coefficient", "maker_fraction", "precision", "flat_fee"}
    return FeeModel(**{k: (D(v) if k != "fee_type" else v) for k, v in values.items() if k in allowed})
