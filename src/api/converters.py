"""Map DynamoDB deserialized rows to Pydantic API models."""

from __future__ import annotations

from datetime import datetime, timezone

from src.schemas.anomaly import AnomalyEvent
from src.schemas.api_responses import MarketQuoteItem, SentimentItem


def _parse_sk_ts(sk: str) -> datetime:
    raw = str(sk).split("#", 1)[0]
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def quote_row_to_item(ticker: str, row: dict) -> MarketQuoteItem:
    sk = str(row.get("sk", ""))
    parts = sk.split("#", 1)
    eid = parts[1] if len(parts) > 1 else None
    return MarketQuoteItem(
        ticker=ticker,
        price=float(row.get("price", 0.0)),
        volume=int(row.get("volume", 0)),
        timestamp=_parse_sk_ts(sk),
        vwap_1min=float(row.get("vwap_1min", 0.0)),
        vwap_5min=float(row.get("vwap_5min", 0.0)),
        volume_zscore=float(row.get("volume_zscore", 0.0)),
        momentum=float(row.get("momentum", 0.0)),
        event_id=eid,
    )


def anomaly_row_to_event(ticker: str, row: dict) -> AnomalyEvent:
    sk = str(row.get("sk", ""))
    return AnomalyEvent(
        ticker=ticker,
        anomaly_score=float(row.get("score", 0.0)),
        is_anomaly=bool(row.get("is_anomaly", True)),
        feature_vector=dict(row.get("features") or {}),
        model_version=max(1, int(row.get("model_version", 1))),
        detected_at=_parse_sk_ts(sk),
    )


def sentiment_row_to_item(row: dict) -> SentimentItem:
    return SentimentItem(
        headline=str(row.get("headline", "")),
        compound=float(row.get("compound", 0.0)),
        positive=float(row.get("positive", 0.0)),
        negative=float(row.get("negative", 0.0)),
        neutral=float(row.get("neutral", 0.0)),
        sk=str(row.get("sk", "")),
    )
