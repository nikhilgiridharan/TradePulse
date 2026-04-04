"""GET /quotes/{ticker} — recent rows from ``market_quotes`` (1s cache)."""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from src.api.converters import quote_row_to_item
from src.api.dependencies import (
    API_RATE_LIMIT_PER_MINUTE,
    CacheDep,
    DynamoWriterDep,
    cached_fetch,
    not_found_detail,
    pydantic_to_cacheable,
    require_allowed_ticker,
    validate_time_window,
)
from src.api.limiter_ext import limiter
from src.config import get_settings
from src.schemas.api_responses import MarketQuoteItem, QuotesListResponse

router = APIRouter(prefix="/quotes", tags=["quotes"])


@router.get("/{ticker}", response_model=QuotesListResponse)
@limiter.limit(API_RATE_LIMIT_PER_MINUTE)
async def get_quotes(
    request: Request,
    writer: DynamoWriterDep,
    cache: CacheDep,
    ticker: str = Depends(require_allowed_ticker),
    limit: int = Query(1, ge=1, le=100),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> QuotesListResponse:
    validate_time_window(start_time, end_time)
    settings = get_settings()
    if settings.demo_mode:
        now = datetime.now(timezone.utc)
        request.state.cache_hit = False
        return QuotesListResponse(
            ticker=ticker,
            items=[
                MarketQuoteItem(
                    ticker=ticker,
                    price=150.0,
                    volume=100,
                    timestamp=now,
                    vwap_1min=149.5,
                    vwap_5min=149.0,
                    volume_zscore=0.0,
                    momentum=0.0,
                    event_id="demo",
                )
            ],
        )

    cache_key = f"quotes:{ticker}:{limit}:{start_time}:{end_time}"

    async def _load() -> dict:
        rows = await writer.query_quotes_for_ticker(
            ticker, limit=limit, start_time=start_time, end_time=end_time
        )
        if not rows:
            raise HTTPException(status_code=404, detail=not_found_detail(ticker))
        items = [quote_row_to_item(ticker, r) for r in rows]
        return pydantic_to_cacheable(QuotesListResponse(ticker=ticker, items=items))

    try:
        raw = await cached_fetch(cache, cache_key, _load, request_state=request.state)
    except HTTPException:
        raise
    return QuotesListResponse.model_validate(raw)
