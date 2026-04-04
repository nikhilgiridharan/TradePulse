"""
Finnhub REST poller (default 60s) → validated ``NewsEvent`` → Kafka ``market.news``.

Dedupe: ordered FIFO cap 10_000 Finnhub article ids (evict oldest when full).
Each article is tagged with the poll ticker (Finnhub may omit symbol on some rows).

Run: ``python -m src.ingestion.finnhub_poller``
"""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from datetime import date, datetime, timedelta, timezone
from typing import Any, Deque, Optional, Set

import finnhub
import structlog
from confluent_kafka import Producer
from pydantic import ValidationError

from src.config import get_settings
from src.schemas.news import NewsEvent

logger = structlog.get_logger(__name__)

MAX_SEEN_IDS = 10_000


class NewsDeduper:
    """FIFO-bounded ``seen`` set: evict oldest ids when capacity is exceeded."""

    __slots__ = ("_order", "_seen", "_max")

    def __init__(self, max_ids: int = MAX_SEEN_IDS) -> None:
        self._order: Deque[int] = deque()
        self._seen: Set[int] = set()
        self._max = max_ids

    def is_new(self, finnhub_id: int) -> bool:
        if finnhub_id in self._seen:
            return False
        self._seen.add(finnhub_id)
        self._order.append(finnhub_id)
        while len(self._order) > self._max:
            old = self._order.popleft()
            self._seen.discard(old)
        return True


def _build_producer_config(settings: Any) -> dict[str, str]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "security.protocol": settings.kafka_security_protocol,
        "enable.idempotence": "true",
        "acks": "all",
        "retries": "5",
    }


def _article_to_news_payload(article: dict[str, Any], ticker: str) -> dict[str, Any]:
    """Build a dict suitable for ``NewsEvent`` validation (Finnhub company-news shape)."""
    dt_raw = article.get("datetime")
    if isinstance(dt_raw, (int, float)):
        dt = datetime.fromtimestamp(int(dt_raw), tz=timezone.utc)
    elif isinstance(dt_raw, str):
        try:
            dt = datetime.fromisoformat(dt_raw.replace("Z", "+00:00"))
        except ValueError:
            dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)

    return {
        "id": article.get("id"),
        "ticker": ticker.upper(),
        "headline": (article.get("headline") or "").strip(),
        "summary": article.get("summary"),
        "url": article.get("url"),
        "source": article.get("source"),
        "datetime": dt,
    }


def _produce_news(producer: Producer, topic: str, ev: NewsEvent) -> None:
    payload = ev.model_dump_json().encode("utf-8")
    producer.produce(topic, value=payload)
    producer.poll(0)


class FinnhubPoller:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._deduper = NewsDeduper(MAX_SEEN_IDS)
        self._producer: Optional[Producer] = None
        self._topic = self._settings.kafka_topic_news

    def _ensure_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(_build_producer_config(self._settings))
        return self._producer

    def _poll_one_ticker(self, client: finnhub.Client, ticker: str) -> int:
        to_d: date = datetime.now(timezone.utc).date()
        from_d = to_d - timedelta(days=7)
        articles = client.company_news(ticker, _from=from_d.isoformat(), to=to_d.isoformat())
        if not articles:
            return 0
        if not isinstance(articles, list):
            logger.warning(
                "finnhub_unexpected_company_news_shape",
                ticker=ticker,
                type=type(articles).__name__,
            )
            return 0

        produced = 0
        producer = self._ensure_producer()
        for article in articles:
            if not isinstance(article, dict):
                continue
            fid = article.get("id")
            if fid is None:
                logger.debug("finnhub_article_skip_no_id", ticker=ticker)
                continue
            try:
                fid_int = int(fid)
            except (TypeError, ValueError):
                logger.debug("finnhub_article_skip_bad_id", ticker=ticker, id=fid)
                continue

            raw = _article_to_news_payload(article, ticker)
            try:
                ev = NewsEvent.model_validate(raw)
            except ValidationError as exc:
                logger.error(
                    "finnhub_news_validation_failed",
                    ticker=ticker,
                    finnhub_id=fid_int,
                    error=str(exc),
                )
                continue

            if not self._deduper.is_new(fid_int):
                continue

            _produce_news(producer, self._topic, ev)
            produced += 1
        return produced

    def _poll_all_sync(self) -> int:
        settings = self._settings
        client = finnhub.Client(api_key=settings.finnhub_api_key)
        total = 0
        try:
            for ticker in settings.ticker_list:
                total += self._poll_one_ticker(client, ticker.upper())
        finally:
            client.close()
        return total

    async def poll_once(self) -> None:
        total = await asyncio.to_thread(self._poll_all_sync)
        logger.info(
            "finnhub_poll_cycle_complete",
            tickers=self._settings.ticker_list,
            new_articles_produced=total,
        )

    async def run_forever(self) -> None:
        interval = self._settings.finnhub_poll_interval_seconds
        while True:
            await self.poll_once()
            await asyncio.sleep(float(interval))


async def run_finnhub_poller() -> None:
    settings = get_settings()
    if not settings.finnhub_api_key:
        logger.error("FINNHUB_API_KEY missing; cannot start Finnhub poller")
        sys.exit(1)

    logger.info(
        "finnhub_poller_starting",
        interval_seconds=settings.finnhub_poll_interval_seconds,
        topic=settings.kafka_topic_news,
        max_seen_ids=MAX_SEEN_IDS,
    )
    poller = FinnhubPoller()
    await poller.run_forever()


def main() -> None:
    asyncio.run(run_finnhub_poller())


if __name__ == "__main__":
    main()
