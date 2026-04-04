"""
Polygon.io WebSocket → validated ``TradeEvent`` / ``QuoteEvent`` → Kafka ``market.trades``.

Uses ``polygon`` ``WebSocketClient`` (stocks real-time feed). The upstream
``polygon-api-client`` package does not ship a separate ``StockClient`` symbol;
``WebSocketClient`` with ``Market.Stocks`` is the supported stocks WebSocket entrypoint.

Idempotent Kafka producer; validation failures → SQS validation DLQ (no raise).
Reconnect: exponential backoff between ``connect`` sessions (1s … max 30s).
"""

from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, List, Optional

import structlog
from confluent_kafka import Producer

# ``polygon-api-client`` exposes stocks WS as ``WebSocketClient`` + ``Market.Stocks`` (no ``StockClient`` symbol).
from polygon import WebSocketClient as StockClient
from polygon.websocket import Market
from polygon.websocket.models import EquityQuote, EquityTrade
from pydantic import ValidationError

from src.config import get_settings
from src.schemas.trade import QuoteEvent, TradeEvent
from src.storage.dlq_handler import DLQHandler

logger = structlog.get_logger(__name__)


def _ns_to_utc_dt(ns: Optional[int]) -> datetime:
    if ns is None:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(float(ns) / 1e9, tz=timezone.utc)


def _build_producer_config(settings: Any) -> dict[str, str]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "security.protocol": settings.kafka_security_protocol,
        "enable.idempotence": "true",
        "acks": "all",
        "retries": "5",
    }


def _polygon_object_to_dict(obj: Any) -> dict[str, Any]:
    """Best-effort JSON-serializable snapshot for DLQ / logging."""
    try:
        if hasattr(obj, "__dataclass_fields__"):
            d = asdict(obj)
            return json.loads(json.dumps(d, default=str))
    except Exception:
        pass
    try:
        return {k: v for k, v in vars(obj).items()}
    except Exception:
        return {"repr": repr(obj)}


def _equity_trade_to_payload(t: EquityTrade) -> dict[str, Any]:
    conds = [str(c) for c in (t.conditions or [])]
    return {
        "ticker": (t.symbol or "").strip().upper(),
        "price": float(t.price or 0.0),
        "size": int(t.size or 0),
        "timestamp": _ns_to_utc_dt(t.timestamp),
        "exchange": str(t.exchange) if t.exchange is not None else None,
        "trade_id": str(t.id) if t.id is not None else None,
        "conditions": conds if conds else None,
        "sequence": t.sequence_number,
    }


def _equity_quote_to_payload(q: EquityQuote) -> dict[str, Any]:
    return {
        "ticker": (q.symbol or "").strip().upper(),
        "bid_price": float(q.bid_price or 0.0),
        "ask_price": float(q.ask_price or 0.0),
        "bid_size": int(q.bid_size or 0),
        "ask_size": int(q.ask_size or 0),
        "timestamp": _ns_to_utc_dt(q.timestamp),
    }


class _PolygonIngestMetrics:
    __slots__ = ("_count", "_lock")

    def __init__(self) -> None:
        self._count = 0
        self._lock = asyncio.Lock()

    async def incr(self, n: int = 1) -> None:
        async with self._lock:
            self._count += n

    async def take_and_reset(self) -> int:
        async with self._lock:
            c = self._count
            self._count = 0
            return c


def _produce_value(producer: Producer, topic: str, value_bytes: bytes) -> None:
    producer.produce(topic, value=value_bytes)
    producer.poll(0)


class PolygonWsIngestion:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._dlq = DLQHandler()
        self._metrics = _PolygonIngestMetrics()
        self._producer: Optional[Producer] = None
        self._topic = self._settings.kafka_topic_trades

    def _ensure_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(_build_producer_config(self._settings))
        return self._producer

    def _validate_and_produce_trade(self, raw: dict[str, Any], snapshot: dict[str, Any]) -> None:
        try:
            ev = TradeEvent.model_validate(raw)
        except ValidationError as exc:
            logger.error("polygon_trade_validation_failed", error=str(exc))
            self._dlq.send_validation_failure(
                {"source": "polygon_ws", "kind": "trade", "raw": snapshot},
                reason=str(exc),
            )
            return
        p = self._ensure_producer()
        payload = ev.model_dump_json().encode("utf-8")
        _produce_value(p, self._topic, payload)

    def _validate_and_produce_quote(self, raw: dict[str, Any], snapshot: dict[str, Any]) -> None:
        try:
            ev = QuoteEvent.model_validate(raw)
        except ValidationError as exc:
            logger.error("polygon_quote_validation_failed", error=str(exc))
            self._dlq.send_validation_failure(
                {"source": "polygon_ws", "kind": "quote", "raw": snapshot},
                reason=str(exc),
            )
            return
        p = self._ensure_producer()
        payload = ev.model_dump_json().encode("utf-8")
        _produce_value(p, self._topic, payload)

    async def _handle_messages(self, msgs: List[Any]) -> None:
        n = 0
        for m in msgs:
            snap = _polygon_object_to_dict(m)
            if isinstance(m, EquityTrade):
                raw = _equity_trade_to_payload(m)
                await asyncio.to_thread(self._validate_and_produce_trade, raw, snap)
                n += 1
            elif isinstance(m, EquityQuote):
                raw = _equity_quote_to_payload(m)
                await asyncio.to_thread(self._validate_and_produce_quote, raw, snap)
                n += 1
        if n:
            await self._metrics.incr(n)

    async def _metrics_loop(self) -> None:
        while True:
            await asyncio.sleep(10.0)
            c = await self._metrics.take_and_reset()
            rate = c / 10.0
            logger.info("polygon_ws_events_per_sec", window_seconds=10, count=c, events_per_sec=rate)

    async def run_forever(self) -> None:
        tickers = [t.upper() for t in self._settings.ticker_list]
        subs: list[str] = []
        for t in tickers:
            subs.append(f"T.{t}")
            subs.append(f"Q.{t}")

        backoff = 1.0
        metrics_task = asyncio.create_task(self._metrics_loop())

        try:
            while True:
                client = StockClient(
                    api_key=self._settings.polygon_api_key,
                    market=Market.Stocks,
                    max_reconnects=None,
                )
                for s in subs:
                    client.subscribe(s)

                try:
                    logger.info(
                        "polygon_ws_connecting",
                        tickers=tickers,
                        topic=self._topic,
                        subscriptions=subs,
                    )
                    await client.connect(self._handle_messages, close_timeout=5)
                    logger.warning("polygon_ws_connect_returned_cleanly")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception("polygon_ws_session_failed", error=str(exc))

                await asyncio.sleep(min(backoff, 30.0))
                backoff = min(backoff * 2.0, 30.0)
        finally:
            metrics_task.cancel()
            try:
                await metrics_task
            except asyncio.CancelledError:
                pass
            if self._producer is not None:
                self._producer.flush(10)


async def run_polygon_ingestion() -> None:
    settings = get_settings()
    if not settings.polygon_api_key:
        logger.error("POLYGON_API_KEY missing; cannot start Polygon ingestion")
        sys.exit(1)

    logger.info(
        "polygon_ws_starting",
        tickers=settings.ticker_list,
        topic=settings.kafka_topic_trades,
    )
    ingest = PolygonWsIngestion()
    await ingest.run_forever()


def main() -> None:
    asyncio.run(run_polygon_ingestion())


if __name__ == "__main__":
    main()
