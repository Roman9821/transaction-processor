from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Numeric, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(String, index=True)

    original_amount: Mapped[Decimal] = mapped_column(Numeric(20, 4))
    original_currency: Mapped[str] = mapped_column(String(3))
    amount_usd: Mapped[Decimal] = mapped_column(Numeric(20, 4))
    rate: Mapped[Decimal] = mapped_column(Numeric(20, 8))

    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
