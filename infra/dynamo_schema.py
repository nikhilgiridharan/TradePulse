"""
DynamoDB table definitions for TradePulse.

Canonical tables (LocalStack + prod):
  - ``market_quotes``: pk ``{TICKER}#{shard}``, sk ISO timestamp (+ event id);
    price, volume, vwap_1min, vwap_5min, volume_zscore, momentum, ttl (N, Unix epoch).
  - ``market_anomalies``: pk ``{TICKER}#{shard}``, sk; score, is_anomaly (BOOL),
    features (M), ttl (N).
  - ``market_sentiment``: pk ticker (S), sk timestamp#article_id; compound, positive,
    negative, neutral, headline, ttl (N).
  - ``market_features``: pk ``{TICKER}#{shard}``, sk; feature_vector (M), updated_at (S), ttl (N).

Optional: ``market_correlations`` for tumbling-window join output (pk ``TICKER#CORR``, sk window_id).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DynamoTableSpec:
    name: str
    pk_attr: str
    sk_attr: str | None
    ttl_attr: str
    description: str


MARKET_QUOTES = DynamoTableSpec(
    name="market_quotes",
    pk_attr="pk",
    sk_attr="sk",
    ttl_attr="ttl",
    description="Latest quote + rolling VWAP/z-score/momentum per trade event",
)

MARKET_ANOMALIES = DynamoTableSpec(
    name="market_anomalies",
    pk_attr="pk",
    sk_attr="sk",
    ttl_attr="ttl",
    description="Isolation Forest detections",
)

MARKET_SENTIMENT = DynamoTableSpec(
    name="market_sentiment",
    pk_attr="pk",
    sk_attr="sk",
    ttl_attr="ttl",
    description="VADER-scored news rows",
)

MARKET_FEATURES = DynamoTableSpec(
    name="market_features",
    pk_attr="pk",
    sk_attr="sk",
    ttl_attr="ttl",
    description="Precomputed feature vectors (7-day TTL)",
)

MARKET_CORRELATIONS = DynamoTableSpec(
    name="market_correlations",
    pk_attr="pk",
    sk_attr="sk",
    ttl_attr="ttl",
    description="Strong news × volume-z correlation events",
)

ALL_TABLES: tuple[DynamoTableSpec, ...] = (
    MARKET_QUOTES,
    MARKET_ANOMALIES,
    MARKET_SENTIMENT,
    MARKET_FEATURES,
    MARKET_CORRELATIONS,
)


def attribute_definitions(pk: str, sk: str | None) -> list[dict[str, str]]:
    defs = [{"AttributeName": pk, "AttributeType": "S"}]
    if sk:
        defs.append({"AttributeName": sk, "AttributeType": "S"})
    return defs
