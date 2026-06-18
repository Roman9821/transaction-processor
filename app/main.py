import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import Depends, FastAPI, Query
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import Response

from .db import get_session, init_db
from .metrics import events_ingested
from .queue import ensure_group, make_redis, publish
from .repository import transaction_history, user_summary
from .schemas import HistoryOut, IngestResponse, SummaryOut, TransactionIn, TransactionOut

logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    app.state.redis = make_redis()
    await ensure_group(app.state.redis)
    yield
    await app.state.redis.aclose()


app = FastAPI(title="Transaction Processor", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/transactions", status_code=202, response_model=IngestResponse)
async def ingest(tx: TransactionIn):
    await publish(app.state.redis, tx.model_dump(mode="json"))
    events_ingested.inc()
    return IngestResponse(status="accepted", id=tx.id)


@app.get("/users/{user_id}/summary", response_model=SummaryOut)
async def summary(user_id: str, session: AsyncSession = Depends(get_session)):
    total, count = await user_summary(session, user_id)
    return SummaryOut(user_id=user_id, total_usd=total, count=count)


@app.get("/users/{user_id}/transactions", response_model=HistoryOut)
async def history(
    user_id: str,
    from_: datetime | None = Query(None, alias="from", description="Inclusive lower bound (ISO 8601)"),
    to: datetime | None = Query(None, alias="to", description="Inclusive upper bound (ISO 8601)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    total, rows = await transaction_history(
        session, user_id=user_id, start=from_, end=to, page=page, page_size=page_size
    )
    return HistoryOut(
        user_id=user_id,
        page=page,
        page_size=page_size,
        total=total,
        items=[TransactionOut.model_validate(r) for r in rows],
    )
