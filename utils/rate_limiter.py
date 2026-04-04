from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


def async_token_bucket(*, rate_per_sec: float, capacity: float) -> Callable[
    [Callable[P, Awaitable[R]]],
    Callable[P, Awaitable[R]],
]:
    """Async token-bucket: up to ``rate_per_sec`` tokens/s (burst ``capacity``)."""

    if rate_per_sec <= 0:
        raise ValueError("rate_per_sec must be positive")
    if capacity <= 0:
        raise ValueError("capacity must be positive")

    tokens = capacity
    last = time.monotonic()
    lock = asyncio.Lock()

    def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            nonlocal tokens, last
            async with lock:
                now = time.monotonic()
                elapsed = now - last
                last = now
                tokens = min(capacity, tokens + elapsed * rate_per_sec)
                if tokens < 1.0:
                    wait = (1.0 - tokens) / rate_per_sec
                    await asyncio.sleep(wait)
                    tokens = 0.0
                    last = time.monotonic()
                else:
                    tokens -= 1.0
            return await fn(*args, **kwargs)

        return wrapper

    return decorator
