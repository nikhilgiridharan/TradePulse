"""
Schema validation layer for MarketFlow market events.

Validates raw JSON messages from Kafka against a Pydantic model. Invalid messages
are never raised as exceptions — we return (event, None) or (None, error) so the
caller can route failures to the DLQ without disrupting the stream. Per-field
failure tracking is emitted to CloudWatch to identify upstream data quality issues.
"""

import json
import re
from datetime import datetime
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, field_validator

from src.config import get_settings
from src.monitoring.cloudwatch_metrics import get_metrics

logger = structlog.get_logger(__name__)


class EventType(str, Enum):
    """Event type discriminator. Trade = single execution; Quote = NBBO update."""

    TRADE = "trade"
    QUOTE = "quote"


class MarketEvent(BaseModel):
    """
    Canonical market event schema. All downstream components depend on this shape.
    Validators enforce business rules (e.g. positive price) to catch corruption early.
    """

    ticker: str  # 1–5 uppercase letters; reject anything else as likely corrupted
    price: float  # Must be > 0 (zero/negative physically impossible for a stock trade)
    volume: int  # Must be >= 0
    timestamp: datetime  # ISO 8601
    event_type: EventType  # trade or quote
    sequence_number: int  # Used for exactly-once deduplication

    @field_validator("ticker")
    @classmethod
    def ticker_must_be_uppercase(cls, v: str) -> str:
        """Tickers are always uppercase — reject anything else as likely corrupted."""
        if not v or not v.isupper():
            raise ValueError("ticker must be 1–5 uppercase letters")
        if not re.match(r"^[A-Z]{1,5}$", v):
            raise ValueError("ticker must be 1–5 uppercase letters")
        return v

    @field_validator("price")
    @classmethod
    def price_must_be_positive(cls, v: float) -> float:
        """Zero or negative price is physically impossible; catches upstream corruption."""
        if v <= 0:
            raise ValueError("price must be positive")
        return v

    @field_validator("volume")
    @classmethod
    def volume_must_be_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("volume must be non-negative")
        return v


class ValidationMetrics:
    """
    Tracks validation outcomes for CloudWatch. Per-field failure counts help
    identify upstream data quality issues (e.g. one source always sending bad price).
    """

    def __init__(self) -> None:
        self.total_validated = 0
        self.total_failed = 0
        self.failures_by_field: dict[str, int] = {}
        self._metrics = get_metrics()

    def record_success(self) -> None:
        self.total_validated += 1

    def record_failure(self, field: str | None) -> None:
        self.total_failed += 1
        key = field or "unknown"
        self.failures_by_field[key] = self.failures_by_field.get(key, 0) + 1

    def emit_to_cloudwatch(self) -> None:
        """Emit every 60 seconds from the Faust agent."""
        if not get_settings().cloudwatch_enabled:
            return
        self._metrics.emit_metric("ValidationSuccess", float(self.total_validated), "Count", {})
        self._metrics.emit_metric("ValidationFailures", float(self.total_failed), "Count", {})
        for field, count in self.failures_by_field.items():
            self._metrics.emit_metric(
                "ValidationFailuresByField", float(count), "Count", {"field": field}
            )
        self._metrics.flush()


_validation_metrics = ValidationMetrics()


def validate_message(raw_message: str) -> tuple[MarketEvent | None, BaseException | None]:
    """
    Parse and validate raw JSON. Never raises — returns (event, None) or (None, error).

    Args:
        raw_message: Raw Kafka message value (JSON string).

    Returns:
        On success: (MarketEvent, None). On failure: (None, ValidationError or JSON decode error).

    Side effects:
        Logs at WARNING on failure with full raw message and failing field.
        Updates ValidationMetrics (success or failure by field).
    """
    try:
        data = json.loads(raw_message)
    except json.JSONDecodeError as e:
        logger.warning("validation_json_decode_error", raw_message=raw_message, error=str(e))
        _validation_metrics.record_failure("json")
        return None, e

    try:
        event = MarketEvent.model_validate(data)
        _validation_metrics.record_success()
        return event, None
    except Exception as e:
        # Pydantic ValidationError has .errors() with field path
        field = "unknown"
        if hasattr(e, "errors"):
            errs = getattr(e, "errors")()
            if errs:
                loc = errs[0].get("loc", ())
                field = ".".join(str(x) for x in loc) if loc else "unknown"
        logger.warning(
            "validation_failed",
            raw_message=raw_message,
            field=field,
            error=str(e),
        )
        _validation_metrics.record_failure(field)
        return None, e
