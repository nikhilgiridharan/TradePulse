"""
FastAPI dependencies: DynamoDB writer, aiocache (1s TTL), allowed ticker universe.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Awaitable, Callable, Optional, TypeVar

from aiocache import Cache
from fastapi import Depends, HTTPException
from pydantic import BaseModel

from src.config import get_settings
from src.storage.dynamo_writer import AsyncDynamoWriter

ALLOWED_TICKERS = frozenset({"AAPL", "NVDA", "MSFT"})

API_RATE_LIMIT_PER_MINUTE = f"{get_settings().api_rate_limit_per_minute}/minute"

T = TypeVar("T")


def _build_cache() -> Cache:
    return Cache(Cache.MEMORY, serializer=None, namespace="tradepulse", ttl=1)


_api_cache: Optional[Cache] = None


def get_api_cache() -> Cache:
    global _api_cache
    if _api_cache is None:
        _api_cache = _build_cache()
    return _api_cache


def reset_api_cache() -> None:
    """Replace the in-process API read-through cache (tests, dev reload)."""
    global _api_cache
    _api_cache = _build_cache()


async def get_dynamo_writer() -> AsyncDynamoWriter:
    return await AsyncDynamoWriter.instance()


DynamoWriterDep = Annotated[AsyncDynamoWriter, Depends(get_dynamo_writer)]
CacheDep = Annotated[Cache, Depends(get_api_cache)]


def require_allowed_ticker(ticker: str) -> str:
    u = ticker.strip().upper()
    if u not in ALLOWED_TICKERS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Invalid ticker {ticker!r}; must be one of "
                f"{', '.join(sorted(ALLOWED_TICKERS))}"
            ),
        )
    return u


def not_found_detail(ticker: str) -> str:
    return f"No data found for ticker {ticker}"


async def cached_fetch(
    cache: Cache,
    key: str,
    factory: Callable[[], Awaitable[T]],
    *,
    request_state: Any,
) -> T:
    """1s TTL read-through; sets ``request.state.cache_hit``."""
    hit = await cache.get(key)
    if hit is not None:
        request_state.cache_hit = True
        return hit  # type: ignore[no-any-return]
    data = await factory()
    await cache.set(key, data, ttl=1)
    request_state.cache_hit = False
    return data


def pydantic_to_cacheable(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")


def validate_time_window(
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> None:
    if start_time is not None and end_time is not None and start_time > end_time:
        raise HTTPException(
            status_code=422,
            detail="start_time must be less than or equal to end_time",
        )
