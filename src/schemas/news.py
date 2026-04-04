"""
News headlines from Finnhub REST polling, published to `market.news`.

Dedupe key is typically Finnhub `id` — poller maintains `seen_ids` in-memory
with eviction to cap memory (same headline should not be re-published).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class NewsEvent(BaseModel):
    """Raw news article metadata prior to VADER scoring in Faust."""

    model_config = {"extra": "ignore"}

    finnhub_id: int = Field(..., validation_alias="id", description="Finnhub unique article id")
    ticker: str = Field(..., description="Related symbol")
    headline: str = Field(..., min_length=1)
    summary: Optional[str] = Field(default=None)
    url: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None)
    datetime_utc: datetime = Field(default_factory=_utc_now, alias="datetime")

    @field_validator("headline", mode="before")
    @classmethod
    def strip_headline(cls, v: str) -> str:
        return v.strip() if isinstance(v, str) else v

    @field_validator("ticker", mode="before")
    @classmethod
    def uppercase_ticker(cls, v: str) -> str:
        return v.strip().upper()

    def to_dynamo_item(self, pk: str, sk: str, ttl_epoch: int) -> dict[str, Any]:
        # TODO: store compound VADER score post-processing in correlator path
        return {
            "pk": pk,
            "sk": sk,
            "ticker": self.ticker,
            "headline": self.headline,
            "finnhub_id": self.finnhub_id,
            "ts": self.datetime_utc.isoformat(),
            "ttl": ttl_epoch,
        }
