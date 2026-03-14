"""
Real-time feature store for TradePulse.

Stores computed features (VWAP, volume z-score, price momentum, trade frequency)
per ticker with hour-level partition key to distribute DynamoDB writes (write
sharding). TTL 7 days. Used by API and anomaly detection for current feature vector.
"""

from datetime import datetime, timezone
from typing import Any

from src.config import get_settings
from src.storage.dynamo_writer import DynamoWriter, TABLES

# Partition key: ticker#date_bucket (e.g. AAPL#2025-02-22-14).
# Hour bucketing distributes writes across 24 partitions per day per ticker,
# avoiding a single hot partition for high-volume tickers (write sharding).
TTL_FEATURES_SEC = 7 * 24 * 3600


def _build_partition_key(ticker: str, ts: datetime | None = None) -> str:
    """Returns ticker#YYYY-MM-DD-HH bucket."""
    ts = ts or datetime.now(timezone.utc)
    return f"{ticker}#{ts.strftime('%Y-%m-%d-%H')}"


class FeatureVector:
    """Current feature set for one ticker."""

    def __init__(
        self,
        ticker: str,
        vwap_5min: float = 0.0,
        volume_zscore: float = 0.0,
        price_momentum_1min: float = 0.0,
        trade_frequency: float = 0.0,
        bid_ask_spread: float | None = None,
        updated_at: datetime | None = None,
    ) -> None:
        self.ticker = ticker
        self.vwap_5min = vwap_5min
        self.volume_zscore = volume_zscore
        self.price_momentum_1min = price_momentum_1min
        self.trade_frequency = trade_frequency
        self.bid_ask_spread = bid_ask_spread
        self.updated_at = updated_at or datetime.now(timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "vwap_5min": self.vwap_5min,
            "volume_zscore": self.volume_zscore,
            "price_momentum_1min": self.price_momentum_1min,
            "trade_frequency": self.trade_frequency,
            "bid_ask_spread": self.bid_ask_spread,
            "updated_at": self.updated_at.isoformat(),
        }


class FeatureStore:
    """
    Updates and retrieves feature vectors. Writes to DynamoDB with pk =
    ticker#YYYY-MM-DD-HH, sort_key = timestamp, TTL = now + 7 days.
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._writer = DynamoWriter()
        self._table_name = self._settings.dynamo.table_features

    def update_features(
        self,
        event: Any,
        aggregations: Any,
    ) -> FeatureVector:
        """
        Build feature vector from event and aggregations; write to DynamoDB.
        Returns the FeatureVector for downstream use.
        """
        ticker = getattr(event, "ticker", "")
        ts = getattr(event, "timestamp", None) or datetime.now(timezone.utc)
        pk = _build_partition_key(ticker, ts)
        ttl = int(datetime.now(timezone.utc).timestamp()) + TTL_FEATURES_SEC
        vwap_5min = getattr(aggregations, "vwap_5min", 0.0) or 0.0
        volume_zscore = getattr(aggregations, "volume_zscore", 0.0) or 0.0
        price_momentum = getattr(aggregations, "price_momentum", 0.0) or 0.0
        trade_frequency = getattr(aggregations, "trade_frequency", 0.0) or 0.0
        vec = FeatureVector(
            ticker=ticker,
            vwap_5min=vwap_5min,
            volume_zscore=volume_zscore,
            price_momentum_1min=price_momentum,
            trade_frequency=trade_frequency,
            bid_ask_spread=None,
            updated_at=ts,
        )
        schema = TABLES["feature_store"]
        item = {
            schema["partition_key"]: pk,
            schema["sort_key"]: ts.isoformat(),
            schema["ttl_field"]: ttl,
            "ticker": ticker,
            "vwap_5min": vwap_5min,
            "volume_zscore": volume_zscore,
            "price_momentum_1min": price_momentum,
            "trade_frequency": trade_frequency,
            "bid_ask_spread": None,
            "updated_at": ts.isoformat(),
        }
        table = self._writer._resource.Table(self._writer._table_name("feature_store"))
        table.put_item(Item=item)
        return vec

    def get_features(self, ticker: str) -> FeatureVector | None:
        """Query latest feature row for ticker (current hour bucket)."""
        pk = _build_partition_key(ticker)
        table = self._writer._resource.Table(self._table_name)
        from boto3.dynamodb.conditions import Key
        resp = table.query(
            KeyConditionExpression=Key("pk").eq(pk),
            Limit=1,
            ScanIndexForward=False,
        )
        items = resp.get("Items", [])
        if not items:
            return None
        row = items[0]
        return FeatureVector(
            ticker=ticker,
            vwap_5min=float(row.get("vwap_5min", 0)),
            volume_zscore=float(row.get("volume_zscore", 0)),
            price_momentum_1min=float(row.get("price_momentum_1min", 0)),
            trade_frequency=float(row.get("trade_frequency", 0)),
            bid_ask_spread=float(row["bid_ask_spread"]) if row.get("bid_ask_spread") is not None else None,
            updated_at=datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00")) if isinstance(row.get("updated_at"), str) else datetime.now(timezone.utc),
        )
