from decimal import Decimal

import pytest

from app.rates import RateProvider, RateUnavailableError


def test_usd_is_identity():
    usd, rate = RateProvider().to_usd(Decimal("100"), "USD")
    assert usd == Decimal("100.0000")
    assert rate == Decimal("1.0")


def test_eur_conversion():
    provider = RateProvider(rates={"EUR": Decimal("1.08")})
    usd, rate = provider.to_usd(Decimal("50"), "EUR")
    assert usd == Decimal("54.0000")
    assert rate == Decimal("1.08")


def test_currency_is_case_insensitive():
    usd, _ = RateProvider().to_usd(Decimal("10"), "eur")
    assert usd == Decimal("10.80")


def test_rounding_to_four_places():
    provider = RateProvider(rates={"JPY": Decimal("0.0067")})
    usd, _ = provider.to_usd(Decimal("12345"), "JPY")
    assert usd == Decimal("82.7115")


def test_unsupported_currency_raises_value_error():
    with pytest.raises(ValueError):
        RateProvider().to_usd(Decimal("10"), "XXX")


def test_rate_source_unavailable_is_retryable():
    provider = RateProvider(failure_probability=1.0)
    with pytest.raises(RateUnavailableError):
        provider.to_usd(Decimal("10"), "USD")
