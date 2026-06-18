import random
from decimal import Decimal

from .config import settings


class RateUnavailableError(RuntimeError):
    pass


_USD_RATES: dict[str, Decimal] = {
    "USD": Decimal("1.0"),
    "EUR": Decimal("1.08"),
    "GBP": Decimal("1.27"),
    "JPY": Decimal("0.0067"),
    "CAD": Decimal("0.74"),
    "AUD": Decimal("0.66"),
    "CHF": Decimal("1.12"),
    "CNY": Decimal("0.14"),
    "INR": Decimal("0.012"),
    "BRL": Decimal("0.20"),
}

_CENTS = Decimal("0.0001")


class RateProvider:
    def __init__(
        self,
        rates: dict[str, Decimal] | None = None,
        failure_probability: float = 0.0,
    ):
        self._rates = rates or dict(_USD_RATES)
        self._failure_probability = failure_probability

    def get_usd_rate(self, currency: str) -> Decimal:
        if self._failure_probability and random.random() < self._failure_probability:
            raise RateUnavailableError("rate source temporarily unavailable")
        try:
            return self._rates[currency.upper()]
        except KeyError as exc:
            raise ValueError(f"unsupported currency: {currency}") from exc

    def to_usd(self, amount: Decimal, currency: str) -> tuple[Decimal, Decimal]:
        rate = self.get_usd_rate(currency)
        usd = (amount * rate).quantize(_CENTS)
        return usd, rate


rate_provider = RateProvider(failure_probability=settings.rate_failure_probability)
