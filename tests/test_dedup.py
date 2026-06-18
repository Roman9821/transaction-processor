from datetime import datetime, timezone
from decimal import Decimal

from app.models import Transaction
from app.repository import save_transaction, user_summary


def _tx(tx_id: str = "t1", user: str = "u1", usd: str = "100.0000") -> Transaction:
    return Transaction(
        id=tx_id,
        user_id=user,
        original_amount=Decimal("100.0000"),
        original_currency="USD",
        amount_usd=Decimal(usd),
        rate=Decimal("1.0"),
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


async def test_first_insert_succeeds(session):
    assert await save_transaction(session, _tx()) is True


async def test_duplicate_id_is_skipped(session):
    assert await save_transaction(session, _tx("dup")) is True
    assert await save_transaction(session, _tx("dup")) is False


async def test_duplicate_not_double_counted(session):
    await save_transaction(session, _tx("a", usd="100.0000"))
    await save_transaction(session, _tx("a", usd="100.0000"))
    await save_transaction(session, _tx("b", usd="50.0000"))

    total, count = await user_summary(session, "u1")
    assert count == 2
    assert total == Decimal("150.0000")


async def test_summary_for_unknown_user_is_zero(session):
    total, count = await user_summary(session, "ghost")
    assert count == 0
    assert total == Decimal("0")
