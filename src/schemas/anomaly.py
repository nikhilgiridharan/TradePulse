"""
Isolation Forest output persisted to DynamoDB and optional `market.anomalies` topic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AnomalyEvent(BaseModel):
    """Single anomaly detection record tied to a ticker and feature snapshot."""

    model_config = {"extra": "ignore", "protected_namespaces": ()}

    ticker: str = Field(...)
    anomaly_score: float = Field(..., description="Isolation Forest decision function / score")
    is_anomaly: bool = Field(default=True)
    feature_vector: dict[str, float] = Field(default_factory=dict)
    model_version: int = Field(default=1, ge=1)
    detected_at: datetime = Field(default_factory=_utc_now)

    @field_validator("ticker", mode="before")
    @classmethod
    def uppercase_ticker(cls, v: str) -> str:
        return v.strip().upper()

    def to_dynamo_item(self, pk: str, sk: str, ttl_epoch: int) -> dict[str, Any]:
        # TODO: JSON-encode feature_vector if Dynamo client requires string map values
        return {
            "pk": pk,
            "sk": sk,
            "ticker": self.ticker,
            "anomaly_score": str(self.anomaly_score),
            "is_anomaly": self.is_anomaly,
            "feature_vector": self.feature_vector,
            "model_version": self.model_version,
            "detected_at": self.detected_at.isoformat(),
            "ttl": ttl_epoch,
        }
