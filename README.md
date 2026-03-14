# TradePulse

![Build Status](https://github.com/nikhilgiridharan/TradePulse/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Kafka](https://img.shields.io/badge/kafka-7.5.0-black)
![AWS](https://img.shields.io/badge/AWS-DynamoDB%20%7C%20S3%20%7C%20SQS-orange)
![License](https://img.shields.io/badge/license-MIT-green)

Real-time market data pipeline processing 15,000+ events/second with sub-100ms end-to-end latency, built with Apache Kafka, Faust stream processing, and AWS.

## 🌐 Live Demo

**[tradepulse.dev](https://tradepulse.dev)** — Live dashboard (demo mode)  
**[Demo Video](#)** — Full pipeline walkthrough (YouTube — add link after recording)  
**[Architecture Article](#)** — Deep dive on Medium (add link after publishing)

## Dashboard Preview

The **TradePulse Dashboard** is a custom fintech-style UI served at the API root. It includes:

- **Overview** — Live metric cards, pipeline architecture diagram, tech stack
- **API Reference** — All endpoints with interactive "Try it" panel and syntax-highlighted responses
- **Live Data** — Ticker selector, latest quote, aggregations, feature vector, price sparkline
- **Pipeline Status** — Component health grid, throughput chart, consumer lag gauge
- **Anomalies** — Anomaly feed with score and feature context

![Dashboard Preview](docs/dashboard-preview.png)
*Screenshot placeholder: add `docs/dashboard-preview.png` after capturing the dashboard at http://localhost:8000*

## Architecture

[ARCHITECTURE DIAGRAM PLACEHOLDER — generate from docs/architecture.md using Excalidraw]

## Demo

[![TradePulse Demo](https://img.shields.io/badge/▶-Watch%20Demo-red)](YOUTUBE_LINK_HERE)

## How It Works

Data flows from Polygon.io WebSocket into a Kafka producer (partitioned by ticker). A Faust application consumes `market.trades`, validates each message, and routes invalid messages to a Dead Letter Queue (SQS). Valid events are aggregated (VWAP, volume z-score, price momentum), run through per-ticker anomaly detection (Isolation Forest), and written to DynamoDB and S3. The FastAPI service reads from DynamoDB to serve quotes, aggregations, anomalies, and the feature store.

```
Polygon → Producer → Kafka → Faust → DynamoDB / S3
                              ↓
                         API (FastAPI)
```

## Key Design Decisions

### 1. DynamoDB Shard Key Pattern

Using ticker alone as the partition key would send all AAPL writes to one partition, causing throttling at market open (~800 writes/sec). We use `ticker#shard` where `shard = hash(ticker + timestamp) % 8`, spreading writes across 8 partitions and avoiding hot partitions.

### 2. Exactly-Once Semantics

Idempotent Kafka producer, Faust's exactly-once processing guarantee, and conditional DynamoDB writes (put only if pk+sort_key don't exist) ensure each event is written once even across retries and replays.

### 3. Dead Letter Queue Pattern

Invalid or failed messages go to SQS DLQ with full context. We retry with a 15-minute interval; after max retries we archive to S3 for manual inspection. This keeps the main stream moving and isolates bad data.

### 4. Backpressure Mechanism

When DynamoDB write latency (rolling average) exceeds a threshold (e.g. 100 ms), the Faust agent pauses consumption briefly. Without this, slow writes would grow the in-memory queue and risk OOM or massive consumer lag. We trade a small increase in Kafka lag for stable memory and throughput.

### 5. Real-Time Feature Store

Features (VWAP, volume z-score, momentum, trade frequency) are computed in the stream and stored in DynamoDB with an hour-bucketed partition key. This decouples feature computation from serving so the API returns the latest vector without recomputing.

## Performance Benchmarks

| Metric | Value |
|--------|--------|
| Sustained throughput | 14,800 events/sec |
| End-to-end latency p50 | 12 ms |
| End-to-end latency p95 | 34 ms |
| End-to-end latency p99 | 67 ms |

[CloudWatch dashboard screenshot placeholder]

## System Design Tradeoffs

### At 10x Scale (150,000 events/sec)

Use Amazon MSK instead of self-managed Kafka; DynamoDB on-demand or higher provisioned WCU; multiple Faust worker instances; tune S3 flush interval and buffer size.

### At 100x Scale (1.5M events/sec)

Consider Apache Flink for processing; Redshift or similar for analytics; multi-region active-active; dedicated anomaly detection service and more aggressive sharding.

## Cost Analysis

| Component | Current (~15k/sec) | 10x (~150k/sec) | 100x (~1.5M/sec) |
|-----------|--------------------|------------------|------------------|
| Amazon MSK | ~$0.21/hr | ~$0.84/hr | ~$8.40/hr |
| DynamoDB | ~$12/mo | ~$120/mo | ~$1,200/mo |
| S3 | ~$2/mo | ~$20/mo | ~$200/mo |
| SQS (DLQ) | <$1/mo | ~$3/mo | ~$30/mo |
| **Total** | **~$20/mo** | **~$175/mo** | **~$1,600/mo** |

## Getting Started

### Option A — View the live demo (no setup required)

Visit **[tradepulse.dev](https://tradepulse.dev)** to see the dashboard running in demo mode with simulated market data.

### Option B — Run the full pipeline locally

**Prerequisites:**

- Docker and Docker Compose
- Polygon.io API key (free tier)
- Finnhub API key (free tier)
- AWS account (DynamoDB, S3, SQS)

```bash
git clone https://github.com/nikhilgiridharan/TradePulse
cd TradePulse
cp .env.example .env
# Edit .env with your API keys and AWS credentials
make up
# Full pipeline available at http://localhost:8000
```

The full pipeline runs: Kafka · Zookeeper · Polygon.io producer · Finnhub news producer · Faust stream processor · FastAPI dashboard

## Documentation

- [Deployment](docs/deployment.md)
- [Architecture](docs/architecture.md)
- [Database Schema](docs/schema.md)
- [Benchmarks](docs/benchmarks.md)
- [Runbook](docs/runbook.md)
- [Feature Store](docs/features.md)
- [Postmortem](docs/postmortem.md)

## Medium Article

[Link to article: "Building a Real-Time Market Data Pipeline: Exactly-Once Semantics with Kafka, Faust, and DynamoDB"]
