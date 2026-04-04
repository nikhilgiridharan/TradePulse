"""FastAPI response models for Dynamo-backed read routes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field

from src.schemas.anomaly import AnomalyEvent


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MarketQuoteItem(BaseModel):
    """Single quote / trade metric row (``market_quotes``)."""

    model_config = {"extra": "ignore"}

    ticker: str
    price: float
    volume: int
    timestamp: datetime
    vwap_1min: float = 0.0
    vwap_5min: float = 0.0
    volume_zscore: float = 0.0
    momentum: float = 0.0
    event_id: Optional[str] = None


class QuotesListResponse(BaseModel):
    ticker: str
    items: list[MarketQuoteItem]


class AggregationResponse(BaseModel):
    """Rolling metrics derived from the latest quote row."""

    ticker: str
    vwap_1min: float
    vwap_5min: float
    rolling_avg_5min: float
    volume_zscore: float
    price_momentum: float
    window_start: datetime
    window_end: datetime


class AnomaliesListResponse(BaseModel):
    ticker: str
    items: list[AnomalyEvent]


class SentimentItem(BaseModel):
    model_config = {"extra": "ignore"}

    headline: str = ""
    compound: float = 0.0
    positive: float = 0.0
    negative: float = 0.0
    neutral: float = 0.0
    sk: str = ""


class SentimentSummary(BaseModel):
    positive: int = 0
    neutral: int = 0
    negative: int = 0
    avg_compound: float = 0.0


class SentimentListResponse(BaseModel):
    ticker: str
    items: list[SentimentItem]
    summary: SentimentSummary


class FeaturesResponse(BaseModel):
    ticker: str
    vwap_5min: float = 0.0
    volume_zscore: float = 0.0
    price_momentum: float = 0.0
    trade_frequency: float = 0.0
    bid_ask_spread: Optional[float] = None
    updated_at: datetime = Field(default_factory=_utc_now)
    feature_vector: dict[str, float] = Field(default_factory=dict)
