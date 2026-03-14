# TradePulse Architecture

## Overview

TradePulse is a real-time market data pipeline that ingests trades and quotes from Polygon.io, processes them with Faust, and stores results in DynamoDB and S3. The API layer serves live quotes, aggregations, anomalies, and feature vectors.

## Data Flow

```
Polygon WebSocket → Producer (Kafka) → market.trades
                        ↓
              Faust process_trades agent
                        ↓
         Validate → DLQ if invalid
                        ↓
         Aggregations (VWAP, z-score, momentum)
                        ↓
         Anomaly detection (Isolation Forest)
                        ↓
         Feature store update
                        ↓
         DynamoDB (trades, aggregations, anomalies, features)
         S3 (Parquet trades, anomalies, dead-letters)
```

## Components

- **Producer**: Polygon WebSocket → Kafka. Partition by ticker. Idempotent producer, acks=all.
- **Faust app**: Exactly-once consumption, schema validation, aggregations, anomaly detection, backpressure.
- **Storage**: DynamoDB (sharded partition keys), S3 (buffered Parquet), SQS DLQ.
- **API**: FastAPI with rate limiting and caching.
- **Monitoring**: CloudWatch metrics (throughput, latency, throttles, DLQ depth).

## Design Decisions

- **Shard key**: `ticker#shard` for trades to avoid hot partitions at market open.
- **Exactly-once**: Idempotent producer + Faust transactions + conditional DynamoDB writes.
- **DLQ**: Invalid or failed messages go to SQS; retry with backoff; archive to S3 after max retries.
- **Backpressure**: Pause consumption when DynamoDB write latency exceeds threshold.
