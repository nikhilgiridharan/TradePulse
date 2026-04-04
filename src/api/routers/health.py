"""GET /health — Kafka, DynamoDB, consumer lag, and pipeline sampling."""

import asyncio
import time
from typing import Any, Dict, Optional, Tuple

import structlog
from fastapi import APIRouter, Request

from src.config import get_settings

logger = structlog.get_logger(__name__)

router = APIRouter(tags=["health"])


def _kafka_list_topics_sync(bootstrap: str) -> Tuple[bool, Dict[str, Any]]:
    try:
        from confluent_kafka import Consumer

        c = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "tradepulse-api-health-probe",
                "session.timeout.ms": 2000,
            }
        )
        try:
            md = c.list_topics(timeout=2.0)
            names = sorted(md.topics.keys())
            return True, {"ok": True, "topic_count": len(names), "sample": names[:25]}
        finally:
            c.close()
    except Exception as exc:
        return False, {"ok": False, "error": str(exc)}


def _dynamo_describe_sync(
    endpoint_url: Optional[str], region: str, table: str
) -> Tuple[bool, Dict[str, Any]]:
    try:
        import boto3

        kwargs: Dict[str, Any] = {"region_name": region}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        client = boto3.client("dynamodb", **kwargs)
        resp = client.describe_table(TableName=table)
        status = resp.get("Table", {}).get("TableStatus", "UNKNOWN")
        return True, {"ok": True, "table": table, "status": status}
    except Exception as exc:
        return False, {"ok": False, "table": table, "error": str(exc)}


def _consumer_lag_sync(bootstrap: str, group_id: str, topic: str) -> Tuple[int, Dict[str, Any]]:
    meta: Dict[str, Any] = {"group": group_id, "topic": topic}
    try:
        from confluent_kafka import Consumer, KafkaException, TopicPartition

        conf = {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
        c = Consumer(conf)
        try:
            md = c.list_topics(timeout=2.0)
            if topic not in md.topics:
                return -1, {**meta, "error": "topic_not_found"}
            partitions = list(md.topics[topic].partitions.keys())
            tps = [TopicPartition(topic, p) for p in partitions]
            c.assign(tps)
            committed = c.committed(tps, timeout=2.0)
            total_lag = 0
            for tp, meta_c in zip(tps, committed):
                try:
                    low, high = c.get_watermark_offsets(tp, timeout=2.0, cached=False)
                except KafkaException:
                    continue
                off = meta_c.offset
                if off < 0:
                    total_lag += max(0, high - low)
                else:
                    total_lag += max(0, high - off)
            return int(total_lag), {**meta, "partitions": len(tps)}
        finally:
            c.close()
    except Exception as exc:
        return -1, {**meta, "error": str(exc)}


@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    settings = get_settings()
    app = request.app
    started = float(getattr(app.state, "started_at_wall", time.time()))
    uptime_seconds = int(time.time() - started)

    now_m = time.monotonic()
    dq = getattr(app.state, "pipeline_event_times", None)
    if dq is not None:
        while dq and now_m - dq[0] > 1.0:
            dq.popleft()
        pipeline_events_per_sec = float(len(dq))
    else:
        pipeline_events_per_sec = 0.0

    try:
        k_ok, kafka_body = await asyncio.wait_for(
            asyncio.to_thread(_kafka_list_topics_sync, settings.kafka_bootstrap_servers),
            timeout=2.0,
        )
    except asyncio.TimeoutError:
        k_ok, kafka_body = False, {"ok": False, "error": "timeout"}

    try:
        d_ok, dynamo_body = await asyncio.wait_for(
            asyncio.to_thread(
                _dynamo_describe_sync,
                settings.aws_endpoint_url,
                settings.aws_region,
                settings.dynamo_table_quotes,
            ),
            timeout=2.0,
        )
    except asyncio.TimeoutError:
        d_ok, dynamo_body = False, {"ok": False, "table": settings.dynamo_table_quotes, "error": "timeout"}

    try:
        lag, _lag_meta = await asyncio.wait_for(
            asyncio.to_thread(
                _consumer_lag_sync,
                settings.kafka_bootstrap_servers,
                settings.kafka_consumer_group,
                settings.kafka_topic_trades,
            ),
            timeout=2.0,
        )
    except asyncio.TimeoutError:
        lag, _lag_meta = -1, {"error": "timeout"}

    if k_ok and d_ok:
        status = "ok"
    elif k_ok or d_ok:
        status = "degraded"
    else:
        status = "down"

    return {
        "status": status,
        "kafka": kafka_body,
        "dynamo": dynamo_body,
        "pipeline_events_per_sec": pipeline_events_per_sec,
        "consumer_lag": lag,
        "uptime_seconds": uptime_seconds,
    }
