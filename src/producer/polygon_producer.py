"""
Production Kafka producer that consumes Polygon.io WebSocket (trades and quotes)
and publishes to market.trades. Partition by ticker for ordering; idempotent
producer and acks=all for durability. Emits throughput and error metrics.
"""

import json
import random
import time
from datetime import datetime, timezone
from typing import Any

import structlog
from confluent_kafka import Producer
from confluent_kafka import KafkaException

from src.config import get_settings
from src.monitoring.cloudwatch_metrics import get_metrics

logger = structlog.get_logger(__name__)

# Exponential backoff: base 1s, multiplier 2, jitter [0,1] to avoid thundering herd
# when multiple producers reconnect simultaneously. max_delay 60s, max_retries 10.
BACKOFF_BASE = 1.0
BACKOFF_MULTIPLIER = 2.0
BACKOFF_MAX_DELAY = 60.0
MAX_RECONNECT_RETRIES = 10

# Throughput window for metric emission (seconds)
THROUGHPUT_WINDOW_SEC = 10


def _build_producer_config() -> dict[str, Any]:
    settings = get_settings()
    conf = {
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "enable.idempotence": True,  # Prevents duplicate messages on retry
        "acks": "all",  # Wait for all replicas to acknowledge
        "retries": 10,  # Retry up to 10 times on transient failure
        "max.in.flight.requests.per.connection": 5,  # Max allowed with idempotence
        "compression.type": "snappy",  # Compress for network efficiency
        "batch.size": 65536,  # 64KB batch for throughput
        "linger.ms": 5,  # Wait up to 5ms to fill batch
    }
    return conf


def _serialize_event(
    ticker: str,
    price: float,
    volume: int,
    timestamp: datetime,
    event_type: str,
    sequence_number: int,
) -> str:
    """JSON with ticker, price, volume, timestamp (ISO 8601), event_type, sequence_number."""
    return json.dumps({
        "ticker": ticker,
        "price": price,
        "volume": volume,
        "timestamp": timestamp.isoformat(),
        "event_type": event_type,
        "sequence_number": sequence_number,
    })


class PolygonProducer:
    """
    Connects to Polygon WebSocket, subscribes to T.* (trades) and Q.* (quotes)
    for configured tickers, and publishes each event to Kafka with partition key = ticker.
    Tracks throughput per 10s and emits ProducerThroughput, ProducerErrors, WebSocketReconnects.
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._metrics = get_metrics()
        self._producer = Producer(_build_producer_config())
        self._sequence = 0
        self._throughput_count = 0
        self._throughput_start = time.time()
        self._run = True

    def _emit_throughput(self, ticker: str) -> None:
        """Emit ProducerThroughput every 10s per ticker."""
        self._throughput_count += 1
        now = time.time()
        if now - self._throughput_start >= THROUGHPUT_WINDOW_SEC:
            rate = self._throughput_count / (now - self._throughput_start)
            if self._settings.cloudwatch_enabled:
                self._metrics.emit_metric("ProducerThroughput", rate, "Count", {"ticker": ticker})
            self._throughput_count = 0
            self._throughput_start = now

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Called per message on produce completion. Log and emit on failure."""
        if err is not None:
            logger.error(
                "producer_delivery_failed",
                error=str(err),
                topic=getattr(msg, "topic", ""),
                partition=getattr(msg, "partition", -1),
                offset=getattr(msg, "offset", -1),
            )
            if self._settings.cloudwatch_enabled:
                self._metrics.emit_metric("ProducerErrors", 1.0, "Count", {})
        else:
            logger.debug("producer_delivery_ok", topic=msg.topic(), partition=msg.partition(), offset=msg.offset())

    def publish(self, payload: str, ticker: str) -> None:
        """Publish JSON payload to market.trades with key=ticker for partitioning."""
        try:
            self._producer.produce(
                self._settings.kafka.topic_trades,
                key=ticker.encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=self._delivery_callback,
            )
            self._emit_throughput(ticker)
        except KafkaException as e:
            logger.error("producer_produce_error", error=str(e), ticker=ticker)
            if self._settings.cloudwatch_enabled:
                self._metrics.emit_metric("ProducerErrors", 1.0, "Count", {})

    def _normalize_polygon_message(self, msg: Any) -> tuple[str, float, int, str] | None:
        """Extract (ticker, price, volume, event_type) from Polygon WS message."""
        try:
            # Polygon trade: symbol, price, size, etc.
            if hasattr(msg, "symbol"):
                ticker = getattr(msg, "symbol", "")
                price = float(getattr(msg, "price", 0) or getattr(msg, "price_1", 0))
                size = int(getattr(msg, "size", 0) or getattr(msg, "size_1", 0))
                return (ticker, price, size, "trade")
            if isinstance(msg, dict):
                ticker = msg.get("symbol") or msg.get("sym", "")
                price = float(msg.get("price", 0) or msg.get("p", 0))
                size = int(msg.get("size", 0) or msg.get("s", 0))
                ev = "quote" if "ask" in msg or "bid" in msg else "trade"
                return (ticker, price, size, ev)
        except (TypeError, ValueError, KeyError) as e:
            logger.debug("polygon_message_skip", msg=msg, error=str(e))
        return None

    def _handle_message(self, msg: Any) -> None:
        """Parse Polygon message and publish to Kafka."""
        parsed = self._normalize_polygon_message(msg)
        if not parsed:
            return
        ticker, price, volume, event_type = parsed
        if not ticker or price <= 0:
            return
        self._sequence += 1
        ts = datetime.now(timezone.utc)
        payload = _serialize_event(ticker, price, volume, ts, event_type, self._sequence)
        self.publish(payload, ticker)

    def run(self) -> None:
        """Connect to Polygon WebSocket with exponential backoff on disconnect."""
        try:
            from polygon import WebSocketClient, STOCKS_CLUSTER
        except ImportError:
            logger.warning("polygon_websocket_not_available", hint="Install polygon-api-client")
            return
        retries = 0
        while self._run and retries < MAX_RECONNECT_RETRIES:
            try:
                if self._settings.cloudwatch_enabled and retries > 0:
                    self._metrics.emit_metric("WebSocketReconnects", 1.0, "Count", {})
                client = WebSocketClient(
                    STOCKS_CLUSTER,
                    self._settings.polygon_api_key,
                    self._handle_message,
                )
                # Subscribe T.* (trades) and Q.* (quotes) for each ticker
                for t in self._settings.ticker_list:
                    client.subscribe(f"T.{t}", f"Q.{t}")
                client.run()
            except Exception as e:
                retries += 1
                delay = min(
                    BACKOFF_MAX_DELAY,
                    BACKOFF_BASE * (BACKOFF_MULTIPLIER ** (retries - 1)) + random.uniform(0, 1),
                )
                logger.warning("polygon_websocket_disconnect", error=str(e), retry_in=delay, retries=retries)
                time.sleep(delay)
        self._producer.flush()


def main() -> None:
    """Entrypoint for producer process."""
    producer = PolygonProducer()
    producer.run()


if __name__ == "__main__":
    main()
