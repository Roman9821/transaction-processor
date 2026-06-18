import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from .config import settings

T = TypeVar("T")


async def with_retry(
    fn: Callable[[], Awaitable[T]],
    *,
    retryable: tuple[type[Exception], ...],
    max_retries: int | None = None,
    base_delay: float | None = None,
    max_delay: float | None = None,
) -> T:
    max_retries = settings.max_retries if max_retries is None else max_retries
    base_delay = settings.backoff_base_seconds if base_delay is None else base_delay
    max_delay = settings.backoff_max_seconds if max_delay is None else max_delay

    attempt = 0
    while True:
        try:
            return await fn()
        except retryable:
            attempt += 1
            if attempt > max_retries:
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            await asyncio.sleep(delay)
