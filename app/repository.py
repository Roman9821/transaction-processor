from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Transaction


async def save_transaction(session: AsyncSession, tx: Transaction) -> bool:
    session.add(tx)
    try:
        await session.commit()
        return True
    except IntegrityError:
        await session.rollback()
        return False


async def user_summary(session: AsyncSession, user_id: str) -> tuple[Decimal, int]:
    stmt = select(
        func.coalesce(func.sum(Transaction.amount_usd), 0),
        func.count(Transaction.id),
    ).where(Transaction.user_id == user_id)
    total, count = (await session.execute(stmt)).one()
    return Decimal(str(total)), int(count)


async def transaction_history(
    session: AsyncSession,
    *,
    user_id: str | None,
    start: datetime | None,
    end: datetime | None,
    page: int,
    page_size: int,
) -> tuple[int, list[Transaction]]:
    conditions = []
    if user_id:
        conditions.append(Transaction.user_id == user_id)
    if start:
        conditions.append(Transaction.timestamp >= start)
    if end:
        conditions.append(Transaction.timestamp <= end)

    count_stmt = select(func.count(Transaction.id))
    if conditions:
        count_stmt = count_stmt.where(*conditions)
    total = int((await session.execute(count_stmt)).scalar_one())

    stmt = select(Transaction)
    if conditions:
        stmt = stmt.where(*conditions)
    stmt = (
        stmt.order_by(Transaction.timestamp.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    return total, rows
