"""Reusable CloudWatch metrics client for TradePulse.

Buffers metric data points and flushes in batches (CloudWatch PutMetricData
accepts up to 20 metrics per request). A background thread flushes every 10
seconds so metrics are eventually emitted even at low throughput. On API
failure we log at WARNING and drop the batch — metrics loss is acceptable,
pipeline data loss is not.

Standard dimensions (Environment, Service) allow filtering in CloudWatch
dashboards, e.g. Environment=production.

Documented metrics emitted across the codebase:
- ProducerThroughput: events/sec per ticker (producer)
- ProducerErrors: count (producer)
- WebSocketReconnects: count (producer)
- ValidationFailures / ValidationFailuresByField: count (validator)
- ConsumerLag: messages per partition (Faust app)
- BackpressureActivations: count (Faust app)
- DynamoWriteLatency: milliseconds p50/p95/p99 (dynamo_writer)
- DynamoThrottles: count (dynamo_writer)
- DynamoWriteErrors: count (dynamo_writer)
- S3WriteLatency: milliseconds (s3_writer)
- S3BytesWritten: bytes (s3_writer)
- AnomaliesDetected: count per ticker per minute (anomaly_detection)
- DLQDepth: message count per queue (dlq_handler)
- DLQRetrySuccess: count (dlq_handler)
- DLQPermanentFailures: count (dlq_handler)
- APIRequestCount: count per endpoint (api)
- APILatency: milliseconds per endpoint (api)
- APIErrors: count per endpoint per status (api)
"""
from __future__ import annotations

import itertools
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import structlog
from botocore.exceptions import ClientError

from src.config import get_settings

logger = structlog.get_logger(__name__)

# Dimensions for all metrics — filter in CloudWatch by Environment=production, etc.
DEFAULT_DIMENSIONS = {
    "Environment": os.getenv("ENVIRONMENT", "development"),
    "Service": "TradePulse",
}

# Max metrics per PutMetricData request (AWS limit)
CLOUDWATCH_BATCH_SIZE = 20

# Flush interval when buffer is not full (seconds)
FLUSH_INTERVAL_SECONDS = 10


class CloudWatchMetrics:
    """
    Buffered CloudWatch metrics emitter. Thread-safe; use get_metrics() for singleton.
    """

    def __init__(self) -> None:
        self._namespace = get_settings().cloudwatch_namespace
        self._enabled = get_settings().cloudwatch_enabled
        self._buffer: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._client = None
        if self._enabled:
            try:
                import boto3
                self._client = boto3.client("cloudwatch", region_name=get_settings().aws.region)
            except Exception as e:
                logger.warning("cloudwatch_client_init_failed", error=str(e))
                self._enabled = False
        if self._enabled:
            self._start_background_flush()

    def _start_background_flush(self) -> None:
        """Background thread flushes buffer every FLUSH_INTERVAL_SECONDS."""

        def _run() -> None:
            while True:
                time.sleep(FLUSH_INTERVAL_SECONDS)
                self.flush()

        t = threading.Thread(target=_run, daemon=True)
        t.start()

    def emit_metric(
        self,
        name: str,
        value: float,
        unit: str,
        dimensions: dict[str, str] | None = None,
    ) -> None:
        """
        Add a metric to the buffer; flush if buffer reaches CLOUDWATCH_BATCH_SIZE.

        Args:
            name: Metric name (e.g. ProducerThroughput).
            value: Numeric value.
            unit: CloudWatch unit (Count, Milliseconds, Bytes, etc.).
            dimensions: Optional extra dimensions (e.g. ticker=AAPL).
        """
        if not self._enabled or not self._client:
            return
        dims = list(DEFAULT_DIMENSIONS.items())
        if dimensions:
            dims.extend(dimensions.items())
        datum = {
            "MetricName": name,
            "Value": value,
            "Unit": unit,
            "Dimensions": [{"Name": k, "Value": str(v)} for k, v in dims],
        }
        with self._lock:
            self._buffer.append(datum)
            if len(self._buffer) >= CLOUDWATCH_BATCH_SIZE:
                self._flush_unsafe()

    def flush(self) -> None:
        """Send all buffered metrics to CloudWatch. Safe to call from any thread."""
        if not self._enabled or not self._client:
            return
        with self._lock:
            self._flush_unsafe()

    def _flush_unsafe(self) -> None:
        """Must hold _lock. Sends buffer in batches of CLOUDWATCH_BATCH_SIZE."""
        buf = self._buffer
        self._buffer = []
        it = iter(buf)
        while True:
            batch = list(itertools.islice(it, CLOUDWATCH_BATCH_SIZE))
            if not batch:
                break
            try:
                self._client.put_metric_data(
                    Namespace=self._namespace,
                    MetricData=[
                        {
                            "MetricName": d["MetricName"],
                            "Value": d["Value"],
                            "Unit": d["Unit"],
                            "Dimensions": d["Dimensions"],
                            "Timestamp": datetime.now(timezone.utc),
                        }
                        for d in batch
                    ],
                )
            except ClientError as e:
                # Handle specific error codes; do not raise — metrics loss is acceptable
                error_code = e.response.get("Error", {}).get("Code", "")
                logger.warning(
                    "cloudwatch_put_metric_failed",
                    code=error_code,
                    message=str(e),
                    batch_size=len(batch),
                )
            except Exception as e:
                logger.warning("cloudwatch_put_metric_error", error=str(e), batch_size=len(batch))

    @contextmanager
    def emit_latency(self, operation: str, dimensions: dict[str, str] | None = None):
        """
        Context manager that measures elapsed time and emits as Milliseconds.

        Usage:
            with metrics.emit_latency("DynamoWrite"):
                dynamo.put_item(...)
        """
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self.emit_metric(
                f"{operation}Latency",
                elapsed_ms,
                "Milliseconds",
                dimensions=dimensions or {},
            )


_metrics_instance: CloudWatchMetrics | None = None


def get_metrics() -> CloudWatchMetrics:
    """Return singleton CloudWatch metrics client."""
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = CloudWatchMetrics()
    return _metrics_instance
