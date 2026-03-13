"""
Dead Letter Queue (DLQ) system for MarketFlow.

Failed messages are sent to SQS DLQ with full context. A processor long-polls
SQS, retries reprocessing, and archives to S3 after DLQ_MAX_RETRIES. Retry
interval (15 min) gives transient failures time to resolve without hammering
the system. Archive after max retries ensures persistent failures get human review.
"""

import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import structlog
from botocore.exceptions import ClientError

from src.config import get_settings
from src.monitoring.cloudwatch_metrics import get_metrics

logger = structlog.get_logger(__name__)


@dataclass
class DLQMessage:
    """Payload sent to SQS and stored in S3 for failed messages."""

    original_message: str
    error_reason: str
    error_type: str
    kafka_topic: str
    kafka_offset: int
    kafka_partition: int
    ticker: str | None
    retry_count: int
    first_failed_at: datetime
    last_failed_at: datetime


class DLQHandler:
    """
    Sends failed messages to SQS DLQ; processes DLQ with retries; archives to S3
    after DLQ_MAX_RETRIES. get_dlq_depth() reports queue depth for CloudWatch.
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._metrics = get_metrics()
        self._sqs = None
        self._s3 = None
        self._init_clients()

    def _init_clients(self) -> None:
        import boto3
        self._sqs = boto3.client(
            "sqs",
            region_name=self._settings.aws.region,
            aws_access_key_id=self._settings.aws.access_key_id or None,
            aws_secret_access_key=self._settings.aws.secret_access_key or None,
        )
        self._s3 = boto3.client(
            "s3",
            region_name=self._settings.aws.region,
            aws_access_key_id=self._settings.aws.access_key_id or None,
            aws_secret_access_key=self._settings.aws.secret_access_key or None,
        )

    def send_to_dlq(self, message: DLQMessage, queue_type: str = "main") -> None:
        """
        Serialize DLQ message to JSON and send to SQS with message attributes
        for filtering (e.g. by error_type or ticker).

        Args:
            message: DLQMessage with full context.
            queue_type: 'main' → SQS_DLQ_URL; 'validation' → SQS_VALIDATION_DLQ_URL.
        """
        url = self._settings.sqs.validation_dlq_url if queue_type == "validation" else self._settings.sqs.dlq_url
        body = json.dumps(
            {
                **asdict(message),
                "first_failed_at": message.first_failed_at.isoformat(),
                "last_failed_at": message.last_failed_at.isoformat(),
            },
            default=str,
        )
        attrs = {
            "error_type": {"DataType": "String", "StringValue": message.error_type},
            "ticker": {"DataType": "String", "StringValue": message.ticker or "unknown"},
        }
        try:
            self._sqs.send_message(
                QueueUrl=url,
                MessageBody=body,
                MessageAttributes=attrs,
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            logger.error("dlq_send_failed", code=code, queue_type=queue_type, error=str(e))
            raise

    def _should_archive(self, message: DLQMessage) -> bool:
        """Returns True if retry_count >= DLQ_MAX_RETRIES (permanent failure)."""
        return message.retry_count >= self._settings.pipeline.dlq_max_retries

    def _archive_to_s3(self, message: DLQMessage) -> None:
        """Write to s3://bucket/dead-letters/YYYY/MM/DD/ for manual inspection."""
        now = message.last_failed_at or datetime.now(timezone.utc)
        key = (
            f"{self._settings.s3.prefix_dead_letters}/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{message.ticker or 'unknown'}_{int(now.timestamp())}.json"
        )
        body = json.dumps(
            {
                **asdict(message),
                "first_failed_at": message.first_failed_at.isoformat(),
                "last_failed_at": message.last_failed_at.isoformat(),
            },
            default=str,
            indent=2,
        )
        try:
            self._s3.put_object(
                Bucket=self._settings.s3.bucket_name,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            logger.error("dlq_archive_s3_failed", code=code, key=key, error=str(e))
            raise
        if self._settings.cloudwatch_enabled:
            self._metrics.emit_metric("DLQPermanentFailures", 1.0, "Count", {})

    def process_dlq(self, process_fn: Any, queue_type: str = "main") -> int:
        """
        Long-poll SQS for messages, call process_fn(body); on success delete;
        on failure increment retry_count and re-queue or archive.

        Args:
            process_fn: Callable that takes message body (str), returns True if processed.
            queue_type: 'main' or 'validation'.

        Returns:
            Number of messages successfully processed.
        """
        url = self._settings.sqs.validation_dlq_url if queue_type == "validation" else self._settings.sqs.dlq_url
        processed = 0
        resp = self._sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            MessageAttributeNames=["All"],
        )
        for msg in resp.get("Messages", []):
            body = msg.get("Body", "{}")
            try:
                data = json.loads(body)
                first_failed = datetime.fromisoformat(data.get("first_failed_at", datetime.now(timezone.utc).isoformat()))
                last_failed = datetime.fromisoformat(data.get("last_failed_at", datetime.now(timezone.utc).isoformat()))
                retry_count = data.get("retry_count", 0)
                dlq_msg = DLQMessage(
                    original_message=data.get("original_message", ""),
                    error_reason=data.get("error_reason", ""),
                    error_type=data.get("error_type", ""),
                    kafka_topic=data.get("kafka_topic", ""),
                    kafka_offset=data.get("kafka_offset", 0),
                    kafka_partition=data.get("kafka_partition", 0),
                    ticker=data.get("ticker"),
                    retry_count=retry_count,
                    first_failed_at=first_failed,
                    last_failed_at=last_failed,
                )
                if process_fn(dlq_msg.original_message):
                    self._sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
                    processed += 1
                    if self._settings.cloudwatch_enabled:
                        self._metrics.emit_metric("DLQRetrySuccess", 1.0, "Count", {})
                else:
                    dlq_msg.retry_count = retry_count + 1
                    dlq_msg.last_failed_at = datetime.now(timezone.utc)
                    if self._should_archive(dlq_msg):
                        self._archive_to_s3(dlq_msg)
                        self._sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
                    else:
                        self.send_to_dlq(dlq_msg, queue_type=queue_type)
                        self._sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
            except Exception as e:
                logger.warning("dlq_process_message_error", message_id=msg.get("MessageId"), error=str(e))
        return processed

    def get_dlq_depth(self) -> dict[str, int]:
        """Return message count per queue; emit as CloudWatch metric DLQDepth."""
        result = {}
        for name, url in [
            ("main", self._settings.sqs.dlq_url),
            ("validation", self._settings.sqs.validation_dlq_url),
        ]:
            try:
                resp = self._sqs.get_queue_attributes(
                    QueueUrl=url,
                    AttributeNames=["ApproximateNumberOfMessages"],
                )
                count = int(resp.get("Attributes", {}).get("ApproximateNumberOfMessages", 0))
                result[name] = count
                if self._settings.cloudwatch_enabled:
                    self._metrics.emit_metric("DLQDepth", float(count), "Count", {"queue": name})
            except ClientError as e:
                logger.warning("dlq_depth_failed", queue=name, error=str(e))
                result[name] = -1
        return result
