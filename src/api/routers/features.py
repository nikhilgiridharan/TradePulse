"""GET /features/{ticker} — latest ``market_features`` row."""

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
from src.ml.feature_store import FeatureStore
from src.schemas.api_responses import FeaturesResponse

router = APIRouter(prefix="/features", tags=["features"])


def _parse_updated_at(raw: object) -> datetime:
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


@router.get("/{ticker}", response_model=FeaturesResponse)
@limiter.limit(API_RATE_LIMIT_PER_MINUTE)
async def get_features(
    request: Request,
    writer: DynamoWriterDep,
    cache: CacheDep,
    ticker: str = Depends(require_allowed_ticker),
    limit: int = Query(1, ge=1, le=100),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> FeaturesResponse:
    validate_time_window(start_time, end_time)
    settings = get_settings()
    if settings.demo_mode:
        request.state.cache_hit = False
        now = datetime.now(timezone.utc)
        return FeaturesResponse(
            ticker=ticker,
            vwap_5min=0.0,
            volume_zscore=0.0,
            price_momentum=0.0,
            trade_frequency=0.0,
            bid_ask_spread=None,
            updated_at=now,
            feature_vector={},
        )

    cache_key = f"features:{ticker}:{limit}:{start_time}:{end_time}"

    async def _load() -> dict:
        store = FeatureStore(writer)
        vec = await store.get_features(ticker)
        if not vec:
            raise HTTPException(status_code=404, detail=not_found_detail(ticker))
        raw_row = await writer.get_latest_features(ticker)
        updated_at = _parse_updated_at((raw_row or {}).get("updated_at"))
        body = FeaturesResponse(
            ticker=ticker,
            vwap_5min=float(vec.get("vwap_5min", vec.get("vwap_5m", 0.0))),
            volume_zscore=float(vec.get("volume_zscore", 0.0)),
            price_momentum=float(vec.get("price_momentum", vec.get("momentum", 0.0))),
            trade_frequency=float(vec.get("trade_frequency", 0.0)),
            bid_ask_spread=(
                float(vec["bid_ask_spread"]) if vec.get("bid_ask_spread") is not None else None
            ),
            updated_at=updated_at,
            feature_vector=dict(vec),
        )
        return pydantic_to_cacheable(body)

    try:
        raw = await cached_fetch(cache, cache_key, _load, request_state=request.state)
    except HTTPException:
        raise
    return FeaturesResponse.model_validate(raw)
