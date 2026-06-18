import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal

import redis.asyncio as redis
from prometheus_client import start_http_server
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError

from .config import settings
from .db import SessionLocal, init_db
from .metrics import events_duplicate, events_failed, events_processed, queue_lag
from .models import Transaction
from .queue import ensure_group, make_redis
from .rates import RateUnavailableError, rate_provider
from .repository import save_transaction
from .retry import with_retry

logger = logging.getLogger("worker")

DEAD_LETTER_KEY = f"{settings.stream_key}:dead"
_TRANSIENT_DB_ERRORS = (OperationalError, InterfaceError, DBAPIError, ConnectionError, OSError)


async def _process_one(client: redis.Redis, msg_id: str, fields: dict[str, str]) -> None:
    payload = json.loads(fields["data"])
    amount = Decimal(str(payload["amount"]))

    async def convert() -> tuple[Decimal, Decimal]:
        return rate_provider.to_usd(amount, payload["currency"])

    usd, rate = await with_retry(convert, retryable=(RateUnavailableError,))

    tx = Transaction(
        id=payload["id"],
        user_id=payload["user_id"],
        original_amount=amount,
        original_currency=payload["currency"].upper(),
        amount_usd=usd,
        rate=rate,
        timestamp=datetime.fromisoformat(payload["timestamp"]),
    )

    async def persist() -> bool:
        async with SessionLocal() as session:
            return await save_transaction(session, tx)

    inserted = await with_retry(persist, retryable=_TRANSIENT_DB_ERRORS)

    if inserted:
        events_processed.inc()
    else:
        events_duplicate.inc()

    await client.xack(settings.stream_key, settings.consumer_group, msg_id)


async def _delivery_count(client: redis.Redis, msg_id: str) -> int:
    pending = await client.xpending_range(
        settings.stream_key, settings.consumer_group, min=msg_id, max=msg_id, count=1
    )
    return pending[0]["times_delivered"] if pending else 1


async def _dead_letter(client: redis.Redis, msg_id: str, fields: dict[str, str], reason: str) -> None:
    await client.xadd(DEAD_LETTER_KEY, {**fields, "error": reason})
    await client.xack(settings.stream_key, settings.consumer_group, msg_id)
    events_failed.inc()


async def _handle(client: redis.Redis, msg_id: str, fields: dict[str, str]) -> None:
    try:
        await _process_one(client, msg_id, fields)
    except (ValueError, KeyError) as exc:
        logger.warning("poison message %s: %s", msg_id, exc)
        await _dead_letter(client, msg_id, fields, str(exc))
    except Exception as exc:
        count = await _delivery_count(client, msg_id)
        if count >= settings.max_deliveries:
            logger.error("dead-lettering %s after %s deliveries: %s", msg_id, count, exc)
            await _dead_letter(client, msg_id, fields, str(exc))
        else:
            logger.warning("will retry %s (delivery %s): %s", msg_id, count, exc)


async def _update_lag(client: redis.Redis) -> None:
    try:
        pending = await client.xpending(settings.stream_key, settings.consumer_group)
        queue_lag.set(pending["pending"] if isinstance(pending, dict) else pending[0])
    except redis.ResponseError:
        pass


async def run() -> None:
    await init_db()
    start_http_server(settings.metrics_port)
    client = make_redis()
    await ensure_group(client)
    logger.info(
        "worker started: consumer=%s group=%s metrics=:%s",
        settings.consumer_name,
        settings.consumer_group,
        settings.metrics_port,
    )

    while True:
        try:
            _, claimed, _ = await client.xautoclaim(
                settings.stream_key,
                settings.consumer_group,
                settings.consumer_name,
                min_idle_time=settings.claim_min_idle_ms,
                start_id="0",
                count=settings.batch_size,
            )
            for msg_id, fields in claimed:
                if fields:
                    await _handle(client, msg_id, fields)
        except redis.ResponseError as exc:
            logger.warning("xautoclaim error: %s", exc)

        resp = await client.xreadgroup(
            groupname=settings.consumer_group,
            consumername=settings.consumer_name,
            streams={settings.stream_key: ">"},
            count=settings.batch_size,
            block=settings.block_ms,
        )
        if resp:
            for _stream, messages in resp:
                for msg_id, fields in messages:
                    await _handle(client, msg_id, fields)

        await _update_lag(client)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(run())
