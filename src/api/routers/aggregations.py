"""GET /aggregations/{ticker} — rolling metrics from latest ``market_quotes`` row."""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

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
from src.schemas.api_responses import AggregationResponse

router = APIRouter(prefix="/aggregations", tags=["aggregations"])


@router.get("/{ticker}", response_model=AggregationResponse)
@limiter.limit(API_RATE_LIMIT_PER_MINUTE)
async def get_aggregations(
    request: Request,
    writer: DynamoWriterDep,
    cache: CacheDep,
    ticker: str = Depends(require_allowed_ticker),
    limit: int = Query(1, ge=1, le=100),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> AggregationResponse:
    """Time window filters which quote row is chosen (latest in-window); typically use defaults."""
    validate_time_window(start_time, end_time)
    settings = get_settings()
    now = datetime.now(timezone.utc)
    if settings.demo_mode:
        request.state.cache_hit = False
        return AggregationResponse(
            ticker=ticker,
            vwap_1min=149.5,
            vwap_5min=149.0,
            rolling_avg_5min=150.0,
            volume_zscore=0.0,
            price_momentum=0.0,
            window_start=now,
            window_end=now,
        )

    cache_key = f"aggregations:{ticker}:{limit}:{start_time}:{end_time}"

    async def _load() -> dict:
        if start_time is not None or end_time is not None:
            rows = await writer.query_quotes_for_ticker(
                ticker, limit=1, start_time=start_time, end_time=end_time
            )
            row = rows[0] if rows else None
        else:
            row = await writer.get_latest_quote_metrics(ticker)
        if not row:
            raise HTTPException(status_code=404, detail=not_found_detail(ticker))
        price = float(row.get("price", 0.0))
        body = AggregationResponse(
            ticker=ticker,
            vwap_1min=float(row.get("vwap_1min", 0.0)),
            vwap_5min=float(row.get("vwap_5min", 0.0)),
            rolling_avg_5min=price,
            volume_zscore=float(row.get("volume_zscore", 0.0)),
            price_momentum=float(row.get("momentum", 0.0)),
            window_start=now,
            window_end=now,
        )
        return pydantic_to_cacheable(body)

    try:
        raw = await cached_fetch(cache, cache_key, _load, request_state=request.state)
    except HTTPException:
        raise
    return AggregationResponse.model_validate(raw)
