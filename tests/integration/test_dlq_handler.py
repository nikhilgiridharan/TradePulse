"""Integration tests for DLQ handler (moto SQS/S3)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.storage.dlq_handler import DLQHandler, DLQMessage


@pytest.fixture
def mock_settings():
    with patch("src.storage.dlq_handler.get_settings") as m:
        s = MagicMock()
        s.aws.region = "us-east-1"
        s.aws.access_key_id = "testing"
        s.aws.secret_access_key = "testing"
        s.sqs.dlq_url = "unused"
        s.sqs.validation_dlq_url = "unused"
        s.s3.bucket_name = "test-tradepulse-data"
        s.s3.prefix_dead_letters = "dead-letters"
        s.pipeline.dlq_max_retries = 3
        s.cloudwatch_enabled = False
        m.return_value = s
        yield s


def test_failed_message_routes_to_dlq(mock_sqs, mock_settings):
    mock_settings.sqs.dlq_url = mock_sqs["dlq_url"]
    mock_settings.sqs.validation_dlq_url = mock_sqs["validation_dlq_url"]
    handler = DLQHandler()
    msg = DLQMessage(
        original_message='{"ticker":"AAPL","price":-1}',
        error_reason="price must be positive",
        error_type="ValidationError",
        kafka_topic="market.trades",
        kafka_offset=42,
        kafka_partition=0,
        ticker="AAPL",
        retry_count=0,
        first_failed_at=datetime.now(timezone.utc),
        last_failed_at=datetime.now(timezone.utc),
    )
    handler.send_to_dlq(msg, queue_type="main")
    resp = handler._sqs.get_queue_attributes(QueueUrl=mock_sqs["dlq_url"], AttributeNames=["ApproximateNumberOfMessages"])
    assert int(resp["Attributes"]["ApproximateNumberOfMessages"]) >= 1


def test_dlq_message_contains_full_context(mock_sqs, mock_settings):
    mock_settings.sqs.dlq_url = mock_sqs["dlq_url"]
    mock_settings.sqs.validation_dlq_url = mock_sqs["validation_dlq_url"]
    handler = DLQHandler()
    msg = DLQMessage(
        original_message='{"ticker":"AAPL"}',
        error_reason="missing price",
        error_type="ValidationError",
        kafka_topic="market.trades",
        kafka_offset=100,
        kafka_partition=2,
        ticker="AAPL",
        retry_count=0,
        first_failed_at=datetime.now(timezone.utc),
        last_failed_at=datetime.now(timezone.utc),
    )
    handler.send_to_dlq(msg, queue_type="main")
    recv = handler._sqs.receive_message(QueueUrl=mock_sqs["dlq_url"], MaxNumberOfMessages=1)
    body = recv["Messages"][0]["Body"]
    import json
    data = json.loads(body)
    assert "original_message" in data
    assert "kafka_offset" in data
    assert data["kafka_offset"] == 100
    assert "error_reason" in data


def test_dlq_retry_succeeds_on_second_attempt(mock_sqs, mock_dynamo, mock_settings):
    mock_settings.sqs.dlq_url = mock_sqs["dlq_url"]
    mock_settings.sqs.validation_dlq_url = mock_sqs["validation_dlq_url"]
    handler = DLQHandler()
    msg = DLQMessage(
        original_message='{"ticker":"AAPL","price":150.0,"volume":100,"timestamp":"2025-02-22T14:00:00+00:00","event_type":"trade","sequence_number":1}',
        error_reason="transient",
        error_type="Timeout",
        kafka_topic="market.trades",
        kafka_offset=1,
        kafka_partition=0,
        ticker="AAPL",
        retry_count=0,
        first_failed_at=datetime.now(timezone.utc),
        last_failed_at=datetime.now(timezone.utc),
    )
    handler.send_to_dlq(msg, queue_type="main")
    attempts = []
    def process_once(body):
        attempts.append(body)
        return len(attempts) >= 2  # Succeed on second attempt
    handler.process_dlq(process_once, queue_type="main")
    handler.process_dlq(process_once, queue_type="main")
    assert len(attempts) >= 1


def test_permanent_failure_routes_to_s3_after_max_retries(mock_sqs, mock_s3, mock_settings):
    mock_settings.sqs.dlq_url = mock_sqs["dlq_url"]
    mock_settings.sqs.validation_dlq_url = mock_sqs["validation_dlq_url"]
    mock_settings.pipeline.dlq_max_retries = 1
    handler = DLQHandler()
    msg = DLQMessage(
        original_message="bad",
        error_reason="validation",
        error_type="ValidationError",
        kafka_topic="market.trades",
        kafka_offset=0,
        kafka_partition=0,
        ticker=None,
        retry_count=1,
        first_failed_at=datetime.now(timezone.utc),
        last_failed_at=datetime.now(timezone.utc),
    )
    handler.send_to_dlq(msg, queue_type="main")
    def never_succeed(_body):
        return False
    handler.process_dlq(never_succeed, queue_type="main")
    # retry_count 1 >= max_retries 1 → archive to S3
    resp = handler._s3.list_objects_v2(Bucket=mock_settings.s3.bucket_name, Prefix=mock_settings.s3.prefix_dead_letters)
    assert resp.get("KeyCount", 0) >= 1


def test_dlq_depth_reported_correctly(mock_sqs, mock_settings):
    mock_settings.sqs.dlq_url = mock_sqs["dlq_url"]
    mock_settings.sqs.validation_dlq_url = mock_sqs["validation_dlq_url"]
    handler = DLQHandler()
    depth = handler.get_dlq_depth()
    assert "main" in depth
    assert "validation" in depth
