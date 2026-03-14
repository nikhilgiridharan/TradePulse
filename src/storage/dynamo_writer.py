"""
Production DynamoDB writer for TradePulse.

Handles trades, aggregations, anomalies, and feature store with sharded partition
keys to avoid hot partitions. Uses conditional writes for exactly-once semantics;
measures latency for backpressure and emits CloudWatch metrics. All AWS calls
handle ClientError explicitly with specific error code handling.
"""

import time
from typing import Any

import structlog
from botocore.exceptions import ClientError
from pydantic import BaseModel

from src.config import get_settings
from src.monitoring.cloudwatch_metrics import get_metrics

logger = structlog.get_logger(__name__)

# Table schemas: partition key design prevents hot partitions.
# Without sharding, all AAPL writes hit one partition → throttling at ~1000 WCU/sec.
# With 8 shards, writes distribute → each partition ~125 WCU/sec.
TABLES = {
    "market_trades": {
        # pk = ticker#shard (shard = hash(ticker+timestamp) % 8)
        "partition_key": "pk",
        "sort_key": "timestamp",
        "ttl_field": "ttl",
    },
    "market_aggregations": {
        "partition_key": "pk",  # Format: ticker (e.g. AAPL); one row per window with all metrics
        "sort_key": "window_start",
        "ttl_field": "ttl",
    },
    "market_anomalies": {
        "partition_key": "pk",  # Format: ticker
        "sort_key": "timestamp",
        "ttl_field": "ttl",
    },
    "feature_store": {
        "partition_key": "pk",  # Format: ticker#YYYY-MM-DD-HH
        "sort_key": "timestamp",
        "ttl_field": "ttl",
    },
}

# TTL durations (seconds): 48h trades, 7d aggregations/features, 30d anomalies
TTL_TRADES_SEC = 48 * 3600
TTL_AGGREGATIONS_SEC = 7 * 24 * 3600
TTL_ANOMALIES_SEC = 30 * 24 * 3600
TTL_FEATURES_SEC = 7 * 24 * 3600

# Exponential backoff for throttling: 1s, 2s, 4s, 8s
THROTTLE_RETRIES = 4
THROTTLE_BASE_DELAY = 1.0


def _get_ttl_epoch(table_name: str) -> int:
    """Return TTL value (epoch seconds) for the table."""
    now = int(time.time())
    if "trades" in table_name:
        return now + TTL_TRADES_SEC
    if "aggregations" in table_name or "feature" in table_name:
        return now + TTL_AGGREGATIONS_SEC
    if "anomalies" in table_name:
        return now + TTL_ANOMALIES_SEC
    return now + TTL_AGGREGATIONS_SEC


class DynamoWriter:
    """
    DynamoDB writer with sharding, conditional writes, and CloudWatch metrics.
    Caller uses returned latency_ms for backpressure calculation.
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._metrics = get_metrics()
        self._client = None
        self._init_client()

    def _init_client(self) -> None:
        import boto3
        self._boto = boto3
        self._client = boto3.client(
            "dynamodb",
            region_name=self._settings.aws.region,
            aws_access_key_id=self._settings.aws.access_key_id or None,
            aws_secret_access_key=self._settings.aws.secret_access_key or None,
        )
        self._resource = boto3.resource(
            "dynamodb",
            region_name=self._settings.aws.region,
            aws_access_key_id=self._settings.aws.access_key_id or None,
            aws_secret_access_key=self._settings.aws.secret_access_key or None,
        )

    def _table_name(self, key: str) -> str:
        mapping = {
            "market_trades": self._settings.dynamo.table_trades,
            "market_aggregations": self._settings.dynamo.table_aggregations,
            "market_anomalies": self._settings.dynamo.table_anomalies,
            "feature_store": self._settings.dynamo.table_features,
        }
        return mapping.get(key, key)

    def write_trade(self, event: BaseModel) -> float:
        """
        Write a single trade with exactly-once semantics. Returns latency in ms.

        Uses conditional write (pk and timestamp must not exist) so replays
        are idempotent. On ConditionalCheckFailedException we return success
        (duplicate). On ProvisionedThroughputExceededException we retry with
        exponential backoff and emit DynamoThrottles; after 4 attempts we raise.

        Args:
            event: MarketEvent-like with ticker, timestamp, price, volume, event_type, sequence_number.

        Returns:
            Latency in milliseconds (for backpressure).

        Raises:
            Exception: After THROTTLE_RETRIES throttled attempts.
        """
        table_key = "market_trades"
        table_name = self._table_name(table_key)
        schema = TABLES[table_key]
        ts = getattr(event, "timestamp")
        ts_iso = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        # Shard distributes writes across 8 partitions to avoid hot partition
        shard = hash(getattr(event, "ticker") + ts_iso) % 8
        pk = f"{getattr(event, 'ticker')}#{shard}"
        ttl = _get_ttl_epoch(table_name)
        event_type_val = getattr(event, "event_type")
        event_type_str = event_type_val.value if hasattr(event_type_val, "value") else str(event_type_val)
        item = {
            schema["partition_key"]: pk,
            schema["sort_key"]: ts_iso,
            schema["ttl_field"]: ttl,
            "ticker": getattr(event, "ticker"),
            "price": float(getattr(event, "price")),
            "volume": int(getattr(event, "volume")),
            "event_type": event_type_str,
            "sequence_number": int(getattr(event, "sequence_number")),
        }
        start = time.perf_counter()
        from boto3.dynamodb.conditions import Attr
        table = self._resource.Table(table_name)
        for attempt in range(THROTTLE_RETRIES):
            try:
                table.put_item(
                    Item=item,
                    ConditionExpression=Attr(schema["partition_key"]).not_exists() & Attr(schema["sort_key"]).not_exists(),
                )
                break
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code == "ConditionalCheckFailedException":
                    # Expected on replay — idempotent success
                    logger.debug("dynamo_conditional_check_failed", pk=pk, timestamp=ts_iso)
                    break
                if code == "ProvisionedThroughputExceededException":
                    self._metrics.emit_metric("DynamoThrottles", 1.0, "Count", {"table": table_name})
                    delay = THROTTLE_BASE_DELAY * (2 ** attempt)
                    if attempt < THROTTLE_RETRIES - 1:
                        time.sleep(delay)
                    else:
                        logger.error("dynamo_throttle_exhausted", table=table_name, attempts=THROTTLE_RETRIES)
                        raise
                else:
                    self._metrics.emit_metric("DynamoWriteErrors", 1.0, "Count", {"table": table_name})
                    logger.error("dynamo_write_error", code=code, table=table_name, error=str(e))
                    raise
        elapsed_ms = (time.perf_counter() - start) * 1000
        if self._settings.cloudwatch_enabled:
            self._metrics.emit_metric("DynamoWriteLatency", elapsed_ms, "Milliseconds", {"table": table_name})
        return elapsed_ms

    def batch_write(self, items: list[dict[str, Any]], table_name: str) -> int:
        """
        Batch write items. Uses batch_writer for automatic chunking and retry of
        unprocessed items. More efficient than N single put_item calls (fewer API
        round-trips and lower cost).

        Args:
            items: List of item dicts with partition_key, sort_key, and other attributes.
            table_name: Logical table key (e.g. market_aggregations).

        Returns:
            Count of successfully written items.
        """
        resolved = self._table_name(table_name)
        schema = TABLES.get(table_name, TABLES.get("market_trades"))
        written = 0
        table = self._resource.Table(resolved)
        with table.batch_writer() as writer:
            for item in items:
                try:
                    writer.put_item(Item=item)
                    written += 1
                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code", "")
                    self._metrics.emit_metric("DynamoWriteErrors", 1.0, "Count", {"table": resolved})
                    logger.warning("batch_write_item_failed", code=code, table=resolved, error=str(e))
        return written
