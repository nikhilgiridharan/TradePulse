# TradePulse Setup

## Prerequisites

- Docker and Docker Compose
- Polygon.io API key (free tier works for low volume)
- AWS account with DynamoDB, S3, SQS (and optionally CloudWatch) access

## Quick Start

```bash
git clone https://github.com/nikhilgiridharan/TradePulse
cd TradePulse
cp .env.example .env
# Edit .env: POLYGON_API_KEY, KAFKA_BOOTSTRAP_SERVERS (use kafka:29092 if running in Docker), AWS_*, SQS_*, table names
make up
# API: http://localhost:8000/docs
```

## Local Development (no Docker)

1. Start Kafka and Zookeeper (e.g. via Docker: `docker run -d ...` or local install).
2. Create DynamoDB tables (see docs/schema.md) or use LocalStack/moto for tests.
3. Create S3 bucket and SQS queues; set URLs in `.env`.
4. `pip install -r requirements.txt`, `python -m src.producer.polygon_producer` (terminal 1), `python -m src.processing.faust_app` (terminal 2), `python -m uvicorn src.api.main:app --reload` (terminal 3).

## Environment Variables

See `.env.example`. Required: `POLYGON_API_KEY`, `KAFKA_BOOTSTRAP_SERVERS`, `AWS_REGION`, DynamoDB table names, S3 bucket, SQS DLQ URLs. Optional: CloudWatch namespace, pipeline tuning (backpressure, DLQ retries, S3 buffer, anomaly parameters).
