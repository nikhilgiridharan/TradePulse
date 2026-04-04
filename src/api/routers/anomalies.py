"""GET /anomalies/{ticker} — recent ``market_anomalies`` rows."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from src.api.converters import anomaly_row_to_event
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
from src.schemas.api_responses import AnomaliesListResponse

router = APIRouter(prefix="/anomalies", tags=["anomalies"])


@router.get("/{ticker}", response_model=AnomaliesListResponse)
@limiter.limit(API_RATE_LIMIT_PER_MINUTE)
async def get_anomalies(
    request: Request,
    writer: DynamoWriterDep,
    cache: CacheDep,
    ticker: str = Depends(require_allowed_ticker),
    limit: int = Query(1, ge=1, le=100),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> AnomaliesListResponse:
    validate_time_window(start_time, end_time)
    settings = get_settings()
    if settings.demo_mode:
        request.state.cache_hit = False
        return AnomaliesListResponse(ticker=ticker, items=[])

    cache_key = f"anomalies:{ticker}:{limit}:{start_time}:{end_time}"

    async def _load() -> dict:
        rows = await writer.query_anomalies_for_ticker(
            ticker, limit=limit, start_time=start_time, end_time=end_time
        )
        items = [anomaly_row_to_event(ticker, r) for r in rows if r.get("is_anomaly", False)]
        if not items:
            raise HTTPException(status_code=404, detail=not_found_detail(ticker))
        return pydantic_to_cacheable(AnomaliesListResponse(ticker=ticker, items=items))

    try:
        raw = await cached_fetch(cache, cache_key, _load, request_state=request.state)
    except HTTPException:
        raise
    return AnomaliesListResponse.model_validate(raw)
