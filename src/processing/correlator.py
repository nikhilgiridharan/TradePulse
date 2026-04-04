"""
60-second tumbling window join → ``market_correlations`` (async DynamoDB).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from src.config import get_settings
from src.processing.faust_app import (
    app,
    join_news_window_changelog,
    join_trade_window_changelog,
)
from src.storage.dlq_handler import DLQHandler
from src.storage.dynamo_writer import AsyncDynamoWriter

logger = structlog.get_logger(__name__)

_dlq: Optional[DLQHandler] = None


def _dlq_handler() -> DLQHandler:
    global _dlq
    if _dlq is None:
        _dlq = DLQHandler()
    return _dlq


async def _run_correlation_tick() -> None:
    settings = get_settings()
    now = time.time()
    completed_wid = int(now) // 60 - 1
    if completed_wid < 0:
        return

    ws = completed_wid * 60
    we = ws + 60
    window_start_iso = datetime.fromtimestamp(ws, tz=timezone.utc).isoformat()
    window_end_iso = datetime.fromtimestamp(we, tz=timezone.utc).isoformat()
    written = 0

    writer = await AsyncDynamoWriter.instance()

    for ticker in settings.ticker_list:
        key = f"{ticker}|{completed_wid}"
        trade = dict(join_trade_window_changelog[key])
        news = dict(join_news_window_changelog[key])

        tcount = int(trade.get("trade_count", 0))
        ncount = int(news.get("news_count", 0))
        if tcount == 0 or ncount == 0:
            continue

        raw_z = trade.get("max_volume_zscore")
        if raw_z is None:
            continue
        max_z = float(raw_z)

        had_strong = bool(news.get("had_strong_sentiment", False))
        best_c = float(news.get("best_compound", 0.0))

        if had_strong and max_z > 2.0:
            ts = datetime.now(timezone.utc)
            ok = await writer.put_correlation_event(
                ticker=ticker,
                window_id=completed_wid,
                window_start_iso=window_start_iso,
                window_end_iso=window_end_iso,
                volume_zscore=max_z,
                sentiment_compound=best_c,
                correlation_type="strong_correlation",
                ts=ts,
            )
            if ok:
                written += 1
                logger.info(
                    "strong_correlation_written",
                    ticker=ticker,
                    window_id=completed_wid,
                    volume_zscore=max_z,
                    sentiment_compound=best_c,
                )

    logger.debug(
        "correlation_window_tick",
        completed_window_id=completed_wid,
        correlations_written=written,
    )


@app.timer(interval=60.0)
async def tumbling_correlation_join(*_args: Any, **_kwargs: Any) -> None:
    try:
        await _run_correlation_tick()
    except Exception as exc:
        logger.exception(
            "correlator_timer_failed",
            error=str(exc),
            agent="correlator",
        )
        _dlq_handler().send_processing_failure(
            agent="correlator",
            error=str(exc),
            payload={"phase": "tumbling_window_timer"},
            exc=exc,
            kafka_meta=None,
        )
