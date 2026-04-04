"""GET /sentiment/{ticker} — recent ``market_sentiment`` rows."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from src.api.converters import sentiment_row_to_item
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
from src.schemas.api_responses import SentimentListResponse, SentimentSummary

router = APIRouter(prefix="/sentiment", tags=["sentiment"])


@router.get("/{ticker}", response_model=SentimentListResponse)
@limiter.limit(API_RATE_LIMIT_PER_MINUTE)
async def get_sentiment(
    request: Request,
    writer: DynamoWriterDep,
    cache: CacheDep,
    ticker: str = Depends(require_allowed_ticker),
    limit: int = Query(1, ge=1, le=100),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> SentimentListResponse:
    validate_time_window(start_time, end_time)
    settings = get_settings()
    if settings.demo_mode:
        request.state.cache_hit = False
        return SentimentListResponse(
            ticker=ticker,
            items=[],
            summary=SentimentSummary(),
        )

    cache_key = f"sentiment:{ticker}:{limit}:{start_time}:{end_time}"

    async def _load() -> dict:
        rows = await writer.query_sentiment_for_ticker(
            ticker, limit=limit, start_time=start_time, end_time=end_time
        )
        if not rows:
            raise HTTPException(status_code=404, detail=not_found_detail(ticker))
        items = [sentiment_row_to_item(r) for r in rows]
        pos_c = neg_c = neu_c = 0
        compound_sum = 0.0
        for it in items:
            compound_sum += it.compound
            if it.compound > 0.05:
                pos_c += 1
            elif it.compound < -0.05:
                neg_c += 1
            else:
                neu_c += 1
        n = len(items)
        summary = SentimentSummary(
            positive=pos_c,
            neutral=neu_c,
            negative=neg_c,
            avg_compound=(compound_sum / n) if n else 0.0,
        )
        return pydantic_to_cacheable(
            SentimentListResponse(ticker=ticker, items=items, summary=summary)
        )

    try:
        raw = await cached_fetch(cache, cache_key, _load, request_state=request.state)
    except HTTPException:
        raise
    return SentimentListResponse.model_validate(raw)
