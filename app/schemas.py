from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class TransactionIn(BaseModel):
    id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    amount: Decimal = Field(..., gt=0)
    currency: str = Field(..., min_length=3, max_length=3)
    timestamp: datetime

    @field_validator("currency")
    @classmethod
    def _uppercase(cls, v: str) -> str:
        return v.upper()


class IngestResponse(BaseModel):
    status: str
    id: str


class TransactionOut(BaseModel):
    id: str
    user_id: str
    original_amount: Decimal
    original_currency: str
    amount_usd: Decimal
    rate: Decimal
    timestamp: datetime

    model_config = {"from_attributes": True}


class SummaryOut(BaseModel):
    user_id: str
    total_usd: Decimal
    count: int


class HistoryOut(BaseModel):
    user_id: str | None
    page: int
    page_size: int
    total: int
    items: list[TransactionOut]
