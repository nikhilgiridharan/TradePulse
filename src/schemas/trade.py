"""
Trade and quote events ingested from Polygon WebSocket into `market.trades`.

Validation runs before Kafka produce; invalid payloads route to validation DLQ
(SQS) with error context — never written to the hot path topic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TradeEvent(BaseModel):
    """Equity trade tick: price, size, exchange timestamp, idempotency key."""

    model_config = {"extra": "ignore"}

    ticker: str = Field(..., description="Uppercase symbol, e.g. AAPL")
    price: float = Field(..., gt=0, description="Trade price in USD")
    size: int = Field(..., ge=1, description="Share count")
    timestamp: datetime = Field(default_factory=_utc_now, description="Event time (UTC)")
    exchange: Optional[str] = Field(default=None, description="Polygon exchange id")
    trade_id: Optional[str] = Field(default=None, description="Vendor trade id for dedupe")
    conditions: Optional[list[str]] = Field(default=None, description="Trade condition codes")
    sequence: Optional[int] = Field(default=None, description="Monotonic sequence per stream")

    @field_validator("ticker", mode="before")
    @classmethod
    def uppercase_ticker(cls, v: str) -> str:
        return v.strip().upper()

    def to_dynamo_item(self, pk: str, sk: str, ttl_epoch: int) -> dict[str, Any]:
        """Serialize for DynamoDB item including TTL and shard pk."""
        # TODO: align with dynamo_writer key layout (pk=ticker#shard, sk=iso_timestamp#trade_id)
        return {
            "pk": pk,
            "sk": sk,
            "ticker": self.ticker,
            "price": str(self.price),
            "size": self.size,
            "ts": self.timestamp.isoformat(),
            "ttl": ttl_epoch,
        }


class QuoteEvent(BaseModel):
    """NBBO or quote update; optional secondary stream if enabled on Polygon feed."""

    model_config = {"extra": "ignore"}

    ticker: str = Field(...)
    bid_price: float = Field(..., ge=0)
    ask_price: float = Field(..., ge=0)
    bid_size: int = Field(default=0, ge=0)
    ask_size: int = Field(default=0, ge=0)
    timestamp: datetime = Field(default_factory=_utc_now)

    @field_validator("ticker", mode="before")
    @classmethod
    def uppercase_ticker(cls, v: str) -> str:
        return v.strip().upper()
