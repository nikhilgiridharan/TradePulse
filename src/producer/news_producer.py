"""
news_producer.py — TradePulse News Sentiment Producer

Role in pipeline:
    Connects to Finnhub's WebSocket API to receive real-time news headlines
    for tracked tickers. Publishes each headline as a structured JSON message
    to the Kafka topic `market.news` for downstream sentiment analysis.

Why a separate producer:
    News events are a fundamentally different data type from trade ticks —
    they arrive at much lower frequency (tens per hour vs thousands per second)
    and require different processing (NLP vs numeric aggregation). Keeping them
    in a separate Kafka topic maintains clean separation of concerns and allows
    independent scaling of each stream.

Why Finnhub:
    Finnhub provides a free WebSocket API for real-time news headlines with
    company-level filtering. No API key tier upgrade required for the news feed.
    Alternative considered: NewsAPI (HTTP polling only, not WebSocket — would
    require a scheduled pull rather than true real-time push delivery).
"""

import json
import logging
import time
import random
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import finnhub
from confluent_kafka import Producer, KafkaException

from src.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Tickers to track for news — matches the equity stream
TRACKED_TICKERS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA"]

# Finnhub company symbols map to their news category identifiers
# Finnhub uses full company names for some news filters
TICKER_TO_CATEGORY = {
    "AAPL":  "AAPL",
    "MSFT":  "MSFT",
    "AMZN":  "AMZN",
    "TSLA":  "TSLA",
    "NVDA":  "NVDA",
}


class NewsProducer:
    """
    Connects to Finnhub news WebSocket and publishes headlines to Kafka.

    Design decisions:
    - Polls Finnhub REST API every 60 seconds per ticker (WebSocket news feed
      requires paid tier — free tier uses polling, which is sufficient given
      news arrives at much lower frequency than trades)
    - Deduplicates headlines using a seen-IDs set to prevent reprocessing
      the same article multiple times across polling intervals
    - Publishes to Kafka partitioned by ticker symbol — same partitioning
      strategy as the equity stream for consistent downstream processing
    """

    def __init__(self):
        """
        Initializes Finnhub client and Kafka producer.
        Kafka producer config mirrors polygon_producer.py for consistency.
        """
        self.finnhub_client = finnhub.Client(
            api_key=settings.pipeline.finnhub_api_key
        )

        # Idempotent producer config — same as equity producer
        # See polygon_producer.py for detailed config comments
        self.producer = Producer({
            'bootstrap.servers': settings.kafka.bootstrap_servers,
            'enable.idempotence': True,
            'acks': 'all',
            'retries': 10,
            'compression.type': 'snappy',
        })

        # Track seen article IDs to prevent duplicate publishing
        # across polling intervals. Uses a bounded set to prevent
        # unbounded memory growth — evict oldest entries after 10,000
        self.seen_ids: set = set()
        self.seen_ids_order: deque[str] = deque()
        self.MAX_SEEN_IDS = 10_000

        self.running = False

    def _delivery_callback(self, err, msg):
        """
        Kafka delivery confirmation callback.
        Logs failures for monitoring — news delivery failures are WARNING
        not ERROR because missing a news item is less critical than missing
        a trade tick.
        """
        if err:
            logger.warning(
                f"News message delivery failed: {err} | "
                f"topic={msg.topic()} | key={msg.key()}"
            )
        else:
            logger.debug(
                f"News delivered: topic={msg.topic()} | "
                f"partition={msg.partition()} | offset={msg.offset()}"
            )

    def _is_duplicate(self, article_id: str) -> bool:
        """
        Checks if an article ID has already been published.
        Maintains a bounded seen-IDs set with FIFO eviction.

        Args:
            article_id: Unique identifier from Finnhub for the article

        Returns:
            True if already seen, False if new
        """
        if article_id in self.seen_ids:
            return True

        # Add to seen set with FIFO eviction to bound memory usage
        self.seen_ids.add(article_id)
        self.seen_ids_order.append(article_id)

        if len(self.seen_ids_order) > self.MAX_SEEN_IDS:
            oldest = self.seen_ids_order.popleft()
            self.seen_ids.discard(oldest)

        return False

    def _fetch_and_publish(self, ticker: str):
        """
        Fetches recent news for a ticker from Finnhub and publishes
        any new articles to the market.news Kafka topic.

        Args:
            ticker: Stock ticker symbol e.g. 'AAPL'
        """
        try:
            # Fetch news from last 24 hours
            # Finnhub returns articles newest-first
            from_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            to_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

            news_items = self.finnhub_client.company_news(
                ticker,
                _from=from_date,
                to=to_date
            )

            new_count = 0
            for item in news_items:
                article_id = str(item.get('id', ''))

                if not article_id or self._is_duplicate(article_id):
                    continue

                # Build structured message for Kafka
                message = {
                    'article_id':  article_id,
                    'ticker':      ticker,
                    'headline':    item.get('headline', ''),
                    'summary':     item.get('summary', ''),
                    'source':      item.get('source', ''),
                    'url':         item.get('url', ''),
                    'published_at': datetime.fromtimestamp(
                        item.get('datetime', 0),
                        tz=timezone.utc
                    ).isoformat(),
                    'ingested_at': datetime.now(timezone.utc).isoformat(),
                    'category':    item.get('category', ''),
                }

                self.producer.produce(
                    topic=settings.kafka.topic_news,
                    key=ticker.encode('utf-8'),
                    value=json.dumps(message).encode('utf-8'),
                    callback=self._delivery_callback,
                )
                new_count += 1

            if new_count > 0:
                logger.info(
                    f"Published {new_count} new articles for {ticker}"
                )
                self.producer.poll(0)

        except Exception as e:
            logger.error(
                f"Failed to fetch/publish news for {ticker}: {e}"
            )

    def run(self):
        """
        Main polling loop. Fetches news for all tickers every 60 seconds.

        Why 60 seconds:
            News arrives much less frequently than trades. A 60-second
            polling interval is short enough to surface breaking news
            quickly while staying well within Finnhub's free tier rate
            limits (60 API calls/minute). Each ticker is staggered by
            2 seconds to spread API calls evenly across the interval.
        """
        self.running = True
        logger.info("TradePulse news producer started")

        while self.running:
            for i, ticker in enumerate(TRACKED_TICKERS):
                self._fetch_and_publish(ticker)
                # Stagger requests by 2 seconds to avoid rate limit bursts
                time.sleep(2)

            # Flush any buffered messages before sleeping
            self.producer.flush()

            # Sleep remaining time in the 60-second window
            remaining = 60 - (len(TRACKED_TICKERS) * 2)
            if remaining > 0:
                time.sleep(remaining)

    def stop(self):
        """Gracefully stops the producer and flushes remaining messages."""
        self.running = False
        self.producer.flush(timeout=10)
        logger.info("News producer stopped, all messages flushed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    producer = NewsProducer()
    try:
        producer.run()
    except KeyboardInterrupt:
        producer.stop()
