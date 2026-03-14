"""
Buffered S3 Parquet writer for TradePulse.

Buffers events in memory and flushes to S3 as Parquet (Snappy) to reduce PUT
cost and storage size. Flush triggers: buffer age > S3_BUFFER_FLUSH_INTERVAL_SECONDS
or buffer size > S3_BUFFER_MAX_MB. Registers partitions with Glue so Athena
can query new data immediately.
"""

import time
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from src.config import get_settings
from src.monitoring.cloudwatch_metrics import get_metrics

logger = structlog.get_logger(__name__)

# Parquet schema: date/hour are partition columns (in path), not stored in file
# Athena uses them for partition pruning
PARQUET_SCHEMA = pa.schema([
    pa.field("ticker", pa.string()),
    pa.field("price", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("timestamp", pa.timestamp("ms", tz="UTC")),
    pa.field("event_type", pa.string()),
    pa.field("sequence_number", pa.int64()),
    pa.field("date", pa.string()),
    pa.field("hour", pa.int32()),
])


class S3Writer:
    """
    Buffers market events and flushes to S3 as Parquet.

    Why buffer: S3 PUT costs $0.005/1000 requests; at 15k events/sec that's
    ~$270/hour. Buffering 5 minutes and one file per flush reduces to ~12
    requests/hour per ticker. Parquet + Snappy cuts storage ~70% vs JSON.
    Flush triggers: buffer age > interval or buffer size > max_mb.
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._metrics = get_metrics()
        self._buffer: list[dict[str, Any]] = []
        self._buffer_created_at: float | None = None
        self._client = None
        self._init_client()

    def _init_client(self) -> None:
        import boto3
        self._client = boto3.client(
            "s3",
            region_name=self._settings.aws.region,
            aws_access_key_id=self._settings.aws.access_key_id or None,
            aws_secret_access_key=self._settings.aws.secret_access_key or None,
        )

    def _build_s3_key(self, ticker: str, ts: datetime | None = None) -> str:
        """Path: trades/year=YYYY/month=MM/day=DD/hour=HH/ticker_epoch.parquet."""
        ts = ts or datetime.now(timezone.utc)
        epoch = int(ts.timestamp())
        return (
            f"{self._settings.s3.prefix_trades}/year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}/{ticker}_{epoch}.parquet"
        )

    def add_event(self, event: Any) -> None:
        """Add event to buffer; flush if interval or size exceeded."""
        ts = getattr(event, "timestamp", None) or datetime.now(timezone.utc)
        if hasattr(ts, "isoformat"):
            ts = ts
        else:
            ts = datetime.now(timezone.utc)
        event_type = getattr(event, "event_type", "trade")
        event_type_str = event_type.value if hasattr(event_type, "value") else str(event_type)
        row = {
            "ticker": getattr(event, "ticker"),
            "price": float(getattr(event, "price")),
            "volume": int(getattr(event, "volume")),
            "timestamp": ts,
            "event_type": event_type_str,
            "sequence_number": int(getattr(event, "sequence_number")),
            "date": ts.strftime("%Y-%m-%d"),
            "hour": ts.hour,
        }
        if self._buffer_created_at is None:
            self._buffer_created_at = time.time()
        self._buffer.append(row)
        interval = self._settings.pipeline.s3_buffer_flush_interval_seconds
        max_mb = self._settings.pipeline.s3_buffer_max_mb
        if (time.time() - self._buffer_created_at) >= interval:
            self.flush()
        else:
            # Approximate buffer size (rough bytes)
            approx_bytes = len(self._buffer) * 80
            if approx_bytes >= max_mb * 1024 * 1024:
                self.flush()

    def flush(self) -> int:
        """
        Convert buffer to Parquet and upload to S3. Register Glue partition.
        Returns number of events flushed.
        """
        if not self._buffer:
            return 0
        start = time.perf_counter()
        bucket = self._settings.s3.bucket_name
        # Group by ticker for one file per ticker per flush (or one combined file)
        by_ticker: dict[str, list] = {}
        for row in self._buffer:
            t = row["ticker"]
            by_ticker.setdefault(t, []).append(row)
        total_bytes = 0
        for ticker, rows in by_ticker.items():
            ts = rows[0]["timestamp"] if rows else datetime.now(timezone.utc)
            key = self._build_s3_key(ticker, ts)
            # Convert to Arrow; timestamps in ms UTC
            arrays = [
                pa.array([r["ticker"] for r in rows]),
                pa.array([r["price"] for r in rows]),
                pa.array([r["volume"] for r in rows]),
                pa.array([r["timestamp"] for r in rows], type=pa.timestamp("ms", tz="UTC")),
                pa.array([r["event_type"] for r in rows]),
                pa.array([r["sequence_number"] for r in rows]),
                pa.array([r["date"] for r in rows]),
                pa.array([r["hour"] for r in rows]),
            ]
            table = pa.table(arrays, schema=PARQUET_SCHEMA)
            buf = pa.BufferOutputStream()
            pq.write_table(table, buf, compression="snappy")
            body = buf.getvalue().to_pybytes()
            total_bytes += len(body)
            try:
                self._client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/octet-stream")
            except Exception as e:
                logger.error("s3_put_failed", key=key, bucket=bucket, error=str(e))
                raise
            self._register_glue_partition(key, ticker, rows[0]["date"], rows[0]["hour"])
        count = len(self._buffer)
        self._buffer = []
        self._buffer_created_at = None
        elapsed_ms = (time.perf_counter() - start) * 1000
        if self._settings.cloudwatch_enabled:
            self._metrics.emit_metric("S3WriteLatency", elapsed_ms, "Milliseconds", {})
            self._metrics.emit_metric("S3BytesWritten", float(total_bytes), "Bytes", {})
        return count

    def _register_glue_partition(self, s3_key: str, ticker: str, date: str, hour: int) -> None:
        """Register partition with Glue so Athena can query new data immediately."""
        try:
            import boto3
            glue = boto3.client("glue", region_name=self._settings.aws.region)
            # Assume table name matches bucket/prefix convention
            db = "tradepulse"
            table = "trades"
            location = f"s3://{self._settings.s3.bucket_name}/{self._settings.s3.prefix_trades}/"
            glue.batch_create_partition(
                DatabaseName=db,
                TableName=table,
                PartitionInput={
                    "Values": [date, str(hour)],
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "ticker", "Type": "string"},
                            {"Name": "price", "Type": "double"},
                            {"Name": "volume", "Type": "bigint"},
                            {"Name": "timestamp", "Type": "timestamp"},
                            {"Name": "event_type", "Type": "string"},
                            {"Name": "sequence_number", "Type": "bigint"},
                        ],
                        "Location": f"s3://{self._settings.s3.bucket_name}/{s3_key.rsplit('/', 1)[0]}/",
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
                    },
                },
            )
        except Exception as e:
            # Non-fatal: data is in S3; Athena can add partition via MSCK REPAIR TABLE
            logger.warning("glue_partition_register_failed", key=s3_key, error=str(e))
