"""
Faust agent: ``market_news`` → VADER sentiment, DynamoDB ``market_sentiment`` rows
(30-day TTL), tumbling-window join sidecar state.

Errors are logged (structlog) and failed payloads are sent to the processing DLQ.
"""

from __future__ import annotations

import asyncio
import copy
from datetime import timezone
from typing import Any, Optional

import structlog
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.processing.faust_app import (
    app,
    join_news_window_changelog,
    market_news,
    sentiment_aggregation_changelog,
)
from src.schemas.news import NewsEvent
from src.storage.dlq_handler import DLQHandler
from src.storage.dynamo_writer import AsyncDynamoWriter

logger = structlog.get_logger(__name__)

_analyzer: Optional[SentimentIntensityAnalyzer] = None
_dlq: Optional[DLQHandler] = None


def _vader() -> SentimentIntensityAnalyzer:
    global _analyzer
    if _analyzer is None:
        _analyzer = SentimentIntensityAnalyzer()
    return _analyzer


def _dlq_handler() -> DLQHandler:
    global _dlq
    if _dlq is None:
        _dlq = DLQHandler()
    return _dlq


async def _writer() -> AsyncDynamoWriter:
    return await AsyncDynamoWriter.instance()


def _safe_payload(raw: Any) -> dict[str, Any]:
    try:
        if isinstance(raw, dict):
            return dict(raw)
        if hasattr(raw, "__dict__"):
            return {k: str(v) for k, v in vars(raw).items()}
        return {"value": repr(raw)}
    except Exception:
        return {"value": repr(raw)}


def _parse_news(raw: Any) -> NewsEvent:
    if isinstance(raw, NewsEvent):
        return raw
    if isinstance(raw, dict):
        return NewsEvent.model_validate(raw)
    return NewsEvent.model_validate(raw, from_attributes=True)


def _text_for_vader(ev: NewsEvent) -> str:
    parts = [ev.headline.strip()]
    if ev.summary:
        parts.append(ev.summary.strip())
    return " ".join(p for p in parts if p)


async def _handle_news_event(raw: Any, kafka_meta: Optional[dict[str, Any]] = None) -> None:
    ne = _parse_news(raw)
    text = _text_for_vader(ne)
    if not text:
        logger.warning("news_empty_text_skipped", ticker=ne.ticker, finnhub_id=ne.finnhub_id)
        return

    scores = await asyncio.to_thread(_vader().polarity_scores, text)
    compound = float(scores["compound"])
    pos = float(scores["pos"])
    neu = float(scores["neu"])
    neg = float(scores["neg"])

    ts = ne.datetime_utc
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts_sec = ts.timestamp()
    wid = int(ts_sec) // 60

    srow = copy.deepcopy(sentiment_aggregation_changelog[ne.ticker])
    srow["last_compound"] = compound
    srow["last_ts"] = ts_sec
    srow["last_headline"] = ne.headline[:512]
    sentiment_aggregation_changelog[ne.ticker] = srow

    jkey = f"{ne.ticker}|{wid}"
    jw = copy.deepcopy(join_news_window_changelog[jkey])
    jw["max_abs_compound"] = max(float(jw["max_abs_compound"]), abs(compound))
    if abs(compound) >= abs(float(jw["best_compound"])):
        jw["best_compound"] = compound
    jw["news_count"] = int(jw["news_count"]) + 1
    jw["had_strong_sentiment"] = bool(jw["had_strong_sentiment"]) or (abs(compound) > 0.5)
    join_news_window_changelog[jkey] = jw

    w = await _writer()
    await w.put_market_sentiment(
        ticker=ne.ticker,
        ts=ts,
        finnhub_id=ne.finnhub_id,
        compound=compound,
        positive=pos,
        negative=neg,
        neutral=neu,
        headline=ne.headline,
        use_batch=True,
    )

    logger.debug(
        "news_processed",
        ticker=ne.ticker,
        finnhub_id=ne.finnhub_id,
        compound=compound,
        kafka=kafka_meta,
    )


@app.agent(market_news)
async def process_news(stream: Any) -> None:
    async for raw in stream:
        kafka_meta = None
        try:
            ev = getattr(stream, "current_event", None)
            if ev is not None and getattr(ev, "message", None) is not None:
                msg = ev.message
                kafka_meta = {
                    "topic": getattr(msg, "topic", None),
                    "partition": getattr(msg, "partition", None),
                    "offset": getattr(msg, "offset", None),
                }
        except Exception:
            kafka_meta = None

        try:
            await _handle_news_event(raw, kafka_meta=kafka_meta)
        except Exception as exc:
            logger.exception(
                "news_processor_message_failed",
                error=str(exc),
                agent="news_processor",
                kafka=kafka_meta,
            )
            _dlq_handler().send_processing_failure(
                agent="news_processor",
                error=str(exc),
                payload=_safe_payload(raw),
                exc=exc,
                kafka_meta=kafka_meta,
            )
