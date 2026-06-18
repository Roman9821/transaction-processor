import json
from typing import Any

import redis.asyncio as redis

from .config import settings


def make_redis() -> redis.Redis:
    return redis.from_url(settings.redis_url, decode_responses=True)


async def ensure_group(client: redis.Redis) -> None:
    try:
        await client.xgroup_create(
            name=settings.stream_key,
            groupname=settings.consumer_group,
            id="0",
            mkstream=True,
        )
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def publish(client: redis.Redis, payload: dict[str, Any]) -> str:
    return await client.xadd(settings.stream_key, {"data": json.dumps(payload)})
