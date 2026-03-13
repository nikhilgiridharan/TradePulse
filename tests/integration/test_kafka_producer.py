"""Integration tests for Kafka producer (optional real broker or mock)."""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock


@pytest.fixture
def mock_kafka_broker():
    """Mock Kafka producer so tests don't require a real broker."""
    with patch("src.producer.polygon_producer.Producer") as mock_producer:
        prod = MagicMock()
        mock_producer.return_value = prod
        yield prod


@pytest.mark.asyncio
async def test_producer_connects_to_kafka(mock_kafka_broker):
    from src.producer.polygon_producer import PolygonProducer
    with patch("src.producer.polygon_producer.get_settings") as m:
        s = MagicMock()
        s.kafka.topic_trades = "market.trades"
        s.kafka.bootstrap_servers = "localhost:9092"
        s.cloudwatch_enabled = False
        s.ticker_list = ["AAPL"]
        m.return_value = s
        producer = PolygonProducer()
        producer.publish('{"ticker":"AAPL","price":185.5,"volume":100}', "AAPL")
    assert mock_kafka_broker.produce.called


@pytest.mark.asyncio
async def test_message_published_to_correct_topic(mock_kafka_broker):
    with patch("src.producer.polygon_producer.get_settings") as m:
        s = MagicMock()
        s.kafka.topic_trades = "market.trades"
        s.cloudwatch_enabled = False
        m.return_value = s
        from src.producer.polygon_producer import PolygonProducer
        producer = PolygonProducer()
        producer.publish('{"ticker":"AAPL"}', "AAPL")
    mock_kafka_broker.produce.assert_called_once()
    call = mock_kafka_broker.produce.call_args
    assert call[0][0] == "market.trades"


@pytest.mark.asyncio
async def test_message_partitioned_by_ticker(mock_kafka_broker):
    with patch("src.producer.polygon_producer.get_settings") as m:
        s = MagicMock()
        s.kafka.topic_trades = "market.trades"
        s.cloudwatch_enabled = False
        m.return_value = s
        from src.producer.polygon_producer import PolygonProducer
        producer = PolygonProducer()
        producer.publish('{"ticker":"AAPL"}', "AAPL")
    call = mock_kafka_broker.produce.call_args
    assert call[1]["key"] == b"AAPL"
