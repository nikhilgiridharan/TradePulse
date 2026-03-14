"""
Shared test fixtures for TradePulse.

Uses moto to mock AWS (DynamoDB, S3, SQS) so tests run without real AWS calls —
no cost, deterministic, and safe for CI. mock_settings provides test config;
sample_market_event and sample_invalid_event for validation tests; mock_cloudwatch
captures emitted metrics for assertion.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from moto import mock_aws

from src.validation.schema_validator import MarketEvent, EventType


@pytest.fixture(autouse=True)
def mock_settings():
    """Settings with test values: localhost Kafka, us-east-1, test table names."""
    with patch("src.config.get_settings") as m:
        settings = MagicMock()
        settings.kafka.bootstrap_servers = "localhost:9092"
        settings.kafka.topic_trades = "market.trades"
        settings.kafka.topic_dlq = "market.trades.dlq"
        settings.aws.region = "us-east-1"
        settings.aws.access_key_id = "testing"
        settings.aws.secret_access_key = "testing"
        settings.dynamo.table_trades = "test_market_trades"
        settings.dynamo.table_aggregations = "test_market_aggregations"
        settings.dynamo.table_anomalies = "test_market_anomalies"
        settings.dynamo.table_features = "test_feature_store"
        settings.s3.bucket_name = "test-tradepulse-data"
        settings.s3.prefix_dead_letters = "dead-letters"
        settings.sqs.dlq_url = "https://sqs.us-east-1.amazonaws.com/123456789/test-dlq"
        settings.sqs.validation_dlq_url = "https://sqs.us-east-1.amazonaws.com/123456789/test-validation-dlq"
        settings.cloudwatch_enabled = False
        settings.cloudwatch_namespace = "TradePulse/Test"
        settings.pipeline.dlq_max_retries = 3
        settings.pipeline.backpressure_latency_threshold_ms = 100
        settings.pipeline.backpressure_pause_ms = 500
        settings.polygon_api_key = "test-key"
        m.return_value = settings
        yield settings


@pytest.fixture
def mock_dynamo():
    """Fake DynamoDB tables matching production schema (moto)."""
    with mock_aws():
        import boto3
        client = boto3.client("dynamodb", region_name="us-east-1")
        for name, key_schema in [
            ("test_market_trades", [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "timestamp", "KeyType": "RANGE"}]),
            ("test_market_aggregations", [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "window_start", "KeyType": "RANGE"}]),
            ("test_market_anomalies", [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "timestamp", "KeyType": "RANGE"}]),
            ("test_feature_store", [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "timestamp", "KeyType": "RANGE"}]),
        ]:
            client.create_table(
                TableName=name,
                KeySchema=[{"AttributeName": k["AttributeName"], "KeyType": k["KeyType"]} for k in key_schema],
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "timestamp", "AttributeType": "S"},
                    {"AttributeName": "window_start", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        yield client


@pytest.fixture
def mock_s3():
    """Fake S3 bucket (moto)."""
    with mock_aws():
        import boto3
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-tradepulse-data")
        yield client


@pytest.fixture
def mock_sqs():
    """Fake SQS queues (moto)."""
    with mock_aws():
        import boto3
        client = boto3.client("sqs", region_name="us-east-1")
        q1 = client.create_queue(QueueName="test-dlq")
        q2 = client.create_queue(QueueName="test-validation-dlq")
        yield {"dlq_url": q1["QueueUrl"], "validation_dlq_url": q2["QueueUrl"], "client": client}


@pytest.fixture
def sample_market_event():
    """Valid MarketEvent for AAPL at $185.50, volume 1000."""
    return MarketEvent(
        ticker="AAPL",
        price=185.50,
        volume=1000,
        timestamp=datetime.now(timezone.utc),
        event_type=EventType.TRADE,
        sequence_number=1,
    )


@pytest.fixture
def sample_invalid_event():
    """Dict with negative price for validation testing."""
    return {
        "ticker": "AAPL",
        "price": -10.0,
        "volume": 100,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "trade",
        "sequence_number": 1,
    }


@pytest.fixture
def mock_cloudwatch():
    """Patch CloudWatch client to capture emitted metrics for assertion."""
    emitted = []
    with patch("src.monitoring.cloudwatch_metrics.CloudWatchMetrics.emit_metric", side_effect=lambda n, v, u, d=None: emitted.append({"name": n, "value": v, "unit": u, "dimensions": d or {}})):
        yield emitted
