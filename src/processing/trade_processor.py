"""
Faust agent: ``market_trades`` → rolling VWAP (1m / 5m), volume z-score (20 trades),
momentum vs prior tick, ``market_quotes`` + ``market_anomalies`` (async DynamoDB).

State lives in Faust Tables. Per-message errors → structured logs + processing DLQ.
"""

from __future__ import annotations

import asyncio
import copy
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import structlog
from pydantic import ValidationError

from src.ml.anomaly_detector import IsolationForestAnomalyDetector
from src.ml.feature_store import FeatureStore
from src.processing.faust_app import (
    app,
    join_trade_window_changelog,
    market_trades,
    vwap_aggregation_changelog,
)
from src.processing.metrics import (
    price_momentum_percent as _price_momentum_percent,
)
from src.processing.metrics import (
    prune_rows as _prune_rows,
)
from src.processing.metrics import (
    rolling_avg_price as _rolling_avg_price,
)
from src.processing.metrics import (
    volume_zscore_20 as _volume_zscore_20,
)
from src.processing.metrics import (
    vwap_from_rows as _vwap_from_rows,
)
from src.schemas.trade import QuoteEvent, TradeEvent
from src.storage.dlq_handler import DLQHandler
from src.storage.dynamo_writer import AsyncDynamoWriter

logger = structlog.get_logger(__name__)

_dlq: Optional[DLQHandler] = None
_detectors: dict[str, IsolationForestAnomalyDetector] = {}


def _dlq_handler() -> DLQHandler:
    global _dlq
    if _dlq is None:
        _dlq = DLQHandler()
    return _dlq


async def _writer() -> AsyncDynamoWriter:
    return await AsyncDynamoWriter.instance()


def _detector(ticker: str) -> IsolationForestAnomalyDetector:
    if ticker not in _detectors:
        _detectors[ticker] = IsolationForestAnomalyDetector(ticker)
    return _detectors[ticker]


def _event_ts_seconds(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _safe_payload(raw: Any) -> dict[str, Any]:
    try:
        if isinstance(raw, dict):
            return dict(raw)
        if hasattr(raw, "__dict__"):
            return {k: str(v) for k, v in vars(raw).items()}
        return {"value": repr(raw)}
    except Exception:
        return {"value": repr(raw)}


def _quote_synthetic_mid(qe: QuoteEvent) -> float:
    if qe.bid_price > 0 and qe.ask_price > 0:
        return (qe.bid_price + qe.ask_price) / 2.0
    return float(max(qe.bid_price, qe.ask_price, 1e-6))


def _quote_synthetic_size(qe: QuoteEvent) -> int:
    return max(int(qe.bid_size or 0), int(qe.ask_size or 0), 1)


def _quote_bid_ask_spread(qe: QuoteEvent) -> float:
    if qe.bid_price > 0 and qe.ask_price > 0:
        return max(qe.ask_price - qe.bid_price, 0.0)
    return 0.0


def _quote_event_id(qe: QuoteEvent) -> str:
    ns = int(qe.timestamp.timestamp() * 1e9)
    return f"Q|{qe.ticker}|{ns}|{qe.bid_price}|{qe.ask_price}"


def _trade_event_id(te: TradeEvent) -> str:
    if te.trade_id:
        return str(te.trade_id)
    if te.sequence is not None:
        return str(te.sequence)
    return str(uuid.uuid4())


async def _handle_trade_event(
    te: TradeEvent,
    *,
    kafka_meta: Optional[dict[str, Any]] = None,
    bid_ask_spread: float = 0.0,
    explicit_event_id: Optional[str] = None,
) -> None:
    ticker = te.ticker
    ts_sec = _event_ts_seconds(te.timestamp)
    now_dt = datetime.now(timezone.utc)
    window_start = datetime.fromtimestamp(ts_sec - (ts_sec % 60), tz=timezone.utc)
    window_end = datetime.fromtimestamp(ts_sec - (ts_sec % 60) + 60, tz=timezone.utc)

    row = copy.deepcopy(vwap_aggregation_changelog[ticker])
    m1 = row["m1"] + [[ts_sec, te.price, te.size]]
    m1 = _prune_rows(m1, ts_sec - 60.0)
    m5 = row["m5"] + [[ts_sec, te.price, te.size]]
    m5 = _prune_rows(m5, ts_sec - 300.0)

    vol_ring = list(row["vol_ring"])
    vol_ring.append(te.size)
    vol_ring = vol_ring[-20:]

    vwap_1m = _vwap_from_rows(m1)
    vwap_5m = _vwap_from_rows(m5)
    rolling_avg_5min = _rolling_avg_price(m5)
    vol_z = _volume_zscore_20(vol_ring)

    last_px = row.get("last_px")
    momentum = _price_momentum_percent(te.price, float(last_px) if last_px is not None else None)

    row["m1"] = m1
    row["m5"] = m5
    row["vol_ring"] = vol_ring
    row["last_px"] = te.price
    vwap_aggregation_changelog[ticker] = row

    wid = int(ts_sec) // 60
    jkey = f"{ticker}|{wid}"
    jw = copy.deepcopy(join_trade_window_changelog[jkey])
    prev_z = jw.get("max_volume_zscore")
    jw["max_volume_zscore"] = vol_z if prev_z is None else max(float(prev_z), vol_z)
    jw["vwap_1min"] = vwap_1m
    jw["vwap_5min"] = vwap_5m
    jw["momentum"] = momentum
    jw["trade_count"] = int(jw["trade_count"]) + 1
    join_trade_window_changelog[jkey] = jw

    w = await _writer()
    eid = explicit_event_id or _trade_event_id(te)
    await w.put_market_quote(
        ticker=ticker,
        event_id=eid,
        ts=te.timestamp,
        price=te.price,
        volume=te.size,
        vwap_1min=vwap_1m,
        vwap_5min=vwap_5m,
        volume_zscore=vol_z,
        momentum=momentum,
        use_batch=True,
    )

    det = _detector(ticker)
    feature_vec: dict[str, float] = {
        "price": float(te.price),
        "volume": float(te.size),
        "vwap_1min": vwap_1m,
        "volume_zscore": vol_z,
        "momentum": momentum,
        "bid_ask_spread": float(bid_ask_spread),
        "trade_frequency": float(len(m1)),
    }
    await asyncio.to_thread(det.update, feature_vec)
    ar = await asyncio.to_thread(det.predict, feature_vec, te.timestamp)
    fs = FeatureStore(w)
    await fs.write_features(ticker, feature_vec, feature_row_id=eid, use_batch=True)
    if ar.is_anomaly and not ar.warming_up:
        await w.put_market_anomaly(
            ticker=ticker,
            event_id=eid,
            sk_suffix=uuid.uuid4().hex[:12],
            score=ar.score,
            is_anomaly=True,
            features=ar.feature_vector,
            detected_at=now_dt,
            use_batch=False,
        )

    logger.debug(
        "trade_processed",
        ticker=ticker,
        vwap_1m=vwap_1m,
        vwap_5m=vwap_5m,
        volume_zscore=vol_z,
        momentum=momentum,
        rolling_avg_5min=rolling_avg_5min,
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        kafka=kafka_meta,
    )


async def _dispatch_market_trades_message(
    raw: Any, kafka_meta: Optional[dict[str, Any]] = None
) -> None:
    """Handle JSON trade ticks or NBBO quotes produced on ``market.trades``."""
    if isinstance(raw, TradeEvent):
        await _handle_trade_event(raw, kafka_meta=kafka_meta)
        return
    if isinstance(raw, dict):
        try:
            te = TradeEvent.model_validate(raw)
        except ValidationError:
            qe = QuoteEvent.model_validate(raw)
            te = TradeEvent(
                ticker=qe.ticker,
                price=_quote_synthetic_mid(qe),
                size=_quote_synthetic_size(qe),
                timestamp=qe.timestamp,
            )
            await _handle_trade_event(
                te,
                kafka_meta=kafka_meta,
                bid_ask_spread=_quote_bid_ask_spread(qe),
                explicit_event_id=_quote_event_id(qe),
            )
        else:
            await _handle_trade_event(te, kafka_meta=kafka_meta)
        return
    te = TradeEvent.model_validate(raw, from_attributes=True)
    await _handle_trade_event(te, kafka_meta=kafka_meta)


@app.agent(market_trades)
async def process_trades(stream: Any) -> None:
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
            await _dispatch_market_trades_message(raw, kafka_meta=kafka_meta)
        except Exception as exc:
            logger.exception(
                "trade_processor_message_failed",
                error=str(exc),
                agent="trade_processor",
                kafka=kafka_meta,
            )
            _dlq_handler().send_processing_failure(
                agent="trade_processor",
                error=str(exc),
                payload=_safe_payload(raw),
                exc=exc,
                kafka_meta=kafka_meta,
            )
