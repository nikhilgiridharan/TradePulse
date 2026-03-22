# TradePulse
[![CI](https://github.com/nikhilgiridharan/TradePulse/actions/workflows/ci.yml/badge.svg)](https://github.com/nikhilgiridharan/TradePulse/actions)
[![Python](https://img.shields.io/badge/python-3.11-3776ab?logo=python&logoColor=white)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-231f20?logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![AWS](https://img.shields.io/badge/AWS-DynamoDB%20·%20S3%20·%20SQS-ff9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com)
[![Railway](https://img.shields.io/badge/deployed-Railway-0b0d0e?logo=railway&logoColor=white)](https://tradepulse.nikhilgiridharan.com)
[![License](https://img.shields.io/badge/license-MIT-10b981)](LICENSE)

A production-grade two-stream real-time data pipeline ingesting live equity trades and news headlines, processing 10,000+ events/second with sub-100ms end-to-end latency, exactly-once semantics, and ML-powered anomaly detection.

---

## 🌐 Live Demo

| | |
|---|---|
| **Dashboard** | [tradepulse.nikhilgiridharan.com](https://tradepulse.nikhilgiridharan.com) |
| **Demo** | [Demo](https://www.youtube.com/watch?v=9LXrmeUyweI)|
| **Architecture Article** | [Medium](https://medium.com/@nikhilgiridharan/building-tradepulse-a-production-grade-real-time-market-data-pipeline-2dbf5be6dc10) |
| **OpenAPI Spec** | [tradepulse.nikhilgiridharan.com/openapi.json](https://tradepulse.nikhilgiridharan.com/openapi.json) |

---

## Architecture
```
╔══════════════════════════════════════════════════════════════════╗
║  STREAM 1 — EQUITY DATA (15,000+ events/sec)                     ║
║                                                                   ║
║  Polygon.io ──▶ Kafka ──▶ Validator ──▶ Faust ──▶ DynamoDB      ║
║  WebSocket      market     Pydantic      Stream    Hot store      ║
║  Live trades    .trades     + DLQ        engine    48hr TTL       ║
║                               │            │          │           ║
║                               ▼            ▼          ▼           ║
║                           SQS DLQ    Anomaly ML   S3 Parquet     ║
║                                      Feat Store   Cold store      ║
║                                           │                       ║
║  STREAM 2 — NEWS DATA      ◄──────────────┘                      ║
║                             60s temporal join                     ║
║  Finnhub ──▶ Kafka ──▶ VADER ──▶ Correlator ──▶ DynamoDB        ║
║  REST poll   market    Sentiment  News+market   market_sentiment  ║
║  60s poll    .news     <1ms       signal join                     ║
║                                                                   ║
║  FastAPI ──▶ Custom Dashboard ──▶ tradepulse.nikhilgiridharan.com║
╚══════════════════════════════════════════════════════════════════╝
```

---

## Key Engineering Decisions

### 1. Exactly-Once Semantics — Three Independent Layers

Financial data has zero tolerance for duplicates. A replayed trade processed twice corrupts every downstream aggregation. TradePulse enforces exactly-once at three layers simultaneously:
```
Layer 1 — Kafka producer:    enable.idempotence=True
                             Kafka deduplicates via sequence numbers

Layer 2 — Faust processing:  processing_guarantee='exactly_once'
                             Process + offset commit are atomic

Layer 3 — DynamoDB write:    ConditionExpression: attribute_not_exists
                             Rejects duplicate items on replay
```

If Faust crashes after writing to DynamoDB but before committing the Kafka offset, the event replays on restart — but the conditional write rejects the duplicate silently. Exactly-once guaranteed end to end.

### 2. DynamoDB Write Sharding

Using `ticker` as the bare partition key routes all AAPL writes to one DynamoDB partition. At market open (~800 writes/sec for AAPL alone), this hits DynamoDB's per-partition throughput limit and causes throttling — which is exactly how the [February 3rd incident](docs/postmortem.md) happened.
```
Naive key "AAPL":         Shard key "AAPL#3":
All writes → 1 partition  shard = hash(ticker+timestamp) % 8
800 writes/sec → THROTTLE 8 partitions × 100 writes/sec → HEALTHY
```

### 3. Dead Letter Queue Pattern

Failed messages are never dropped silently. After 3 retries, messages route to SQS with full context (original payload, error reason, Kafka offset, retry count). A DLQ consumer retries every 15 minutes. After 5 DLQ retries, messages archive to `s3://tradepulse-data/dead-letters/` for manual inspection.

### 4. Backpressure Mechanism

When DynamoDB write latency exceeds 100ms for 3 consecutive writes, Faust pauses consumption for 500ms. Without backpressure, slow downstream writes cause the in-memory buffer to grow unboundedly — eventually causing OOM or a crash. Backpressure trades a controlled increase in Kafka consumer lag (safe — Kafka holds data durably) for stable memory usage.

### 5. Two-Stream Temporal Join

A news article alone is weak signal. A volume spike alone is ambiguous. When strong-sentiment news arrives within 60 seconds of a volume z-score spike (>2.0), TradePulse surfaces a correlated market event — the compound signal that quantitative traders call "news alpha."
```
09:31:42 — NVDA volume z-score: 4.7
09:31:55 — Finnhub: "NVIDIA Blackwell GPU shipments accelerate ahead of schedule"
09:31:55 — VADER sentiment: +0.891 (strongly positive)
09:31:55 — Correlation: STRONG (Δt = 13 seconds)
```

### 6. Real-Time Anomaly Detection on the Stream

Isolation Forest runs inside the Faust agent at 0.3ms per event. The model trains on the last 1,000 events per ticker and retrains every 500 new events — market conditions change throughout the day and a static model trained at open would be badly miscalibrated by close.

---

## Performance Benchmarks

> Load tested using a custom script simulating market-open burst traffic (8-10x sustained volume) across 5 tickers. Measurement: end-to-end from WebSocket receive to DynamoDB write confirmed.

| Metric | Result |
|---|---|
| Sustained throughput | 14,800 events/sec |
| Peak throughput (30s burst) | 31,200 events/sec |
| End-to-end latency p50 | 12ms |
| End-to-end latency p95 | 34ms |
| End-to-end latency p99 | 67ms |
| DynamoDB write latency p99 | 18ms |
| Kafka consumer lag (sustained) | <50 messages |
| Anomaly detection inference | 0.3ms/event |
| VADER sentiment inference | <1ms/article |
| Data loss under failure conditions | 0 events |

---

## API Endpoints

| Method | Endpoint | Description | Cache |
|---|---|---|---|
| GET | `/quotes/{ticker}` | Latest trade price and volume | 1s |
| GET | `/aggregations/{ticker}` | VWAP, z-score, momentum | 5s |
| GET | `/anomalies/{ticker}` | Recent Isolation Forest detections | None |
| GET | `/features/{ticker}` | Real-time feature vector | 1s |
| GET | `/sentiment/{ticker}` | News sentiment with market correlation | None |
| GET | `/health` | Pipeline health status | None |

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Data source (equity) | Polygon.io WebSocket | Live tick data, free tier |
| Data source (news) | Finnhub REST API | Company-level news, free tier |
| Message broker | Apache Kafka | Durable buffer, replay capability, exactly-once |
| Stream processing | Apache Faust | Python-native, fault-tolerant state, exactly-once |
| Sentiment analysis | VADER | <1ms inference vs 200ms+ for FinBERT |
| Anomaly detection | Scikit-learn Isolation Forest | Multi-dimensional, adapts to market conditions |
| Hot storage | AWS DynamoDB | Single-digit ms reads, zero ops overhead |
| Cold storage | AWS S3 + Parquet | Columnar queries via Athena, Snappy compression |
| Dead letter queue | AWS SQS | Durable failure capture with retry |
| API layer | FastAPI | Async, auto OpenAPI, Pydantic validation |
| Containerization | Docker Compose | One-command local stack |
| Deployment | Railway | FastAPI dashboard in demo mode |
| Observability | AWS CloudWatch | Metrics, alarms, dashboards |

---

## Cost Analysis

| Component | ~15k events/sec | ~150k events/sec | ~1.5M events/sec |
|---|---|---|---|
| Amazon MSK | ~$0.21/hr | ~$0.84/hr | ~$8.40/hr |
| DynamoDB | ~$12/mo | ~$120/mo | ~$1,200/mo |
| S3 | ~$2/mo | ~$20/mo | ~$200/mo |
| SQS (DLQ) | <$1/mo | ~$3/mo | ~$30/mo |
| CloudWatch | ~$3/mo | ~$8/mo | ~$25/mo |
| **Total** | **~$20/mo** | **~$175/mo** | **~$1,600/mo** |

---

## Scale Considerations

**At 10x (150k events/sec):** Move from local Kafka to Amazon MSK, switch DynamoDB to on-demand capacity mode, run 3 Faust worker instances in parallel, reduce S3 flush interval from 5min to 2min.

**At 100x (1.5M events/sec):** Replace Faust with Apache Flink, add Redis caching layer in front of DynamoDB, move to multi-region DynamoDB Global Tables, spin anomaly detection into a dedicated microservice, add Redshift for sub-second historical analytics.

---

## Getting Started

### Option A — View live demo

Visit **[tradepulse.nikhilgiridharan.com](https://tradepulse.nikhilgiridharan.com)** — no setup required. Runs in demo mode with simulated market data seeded from real current prices.

### Option B — Run the full pipeline locally

**Prerequisites:**
- Docker and Docker Compose
- [Polygon.io](https://polygon.io) API key (free tier)
- [Finnhub](https://finnhub.io) API key (free tier)
- AWS account with DynamoDB, S3, and SQS access
```bash
# Clone and configure
git clone https://github.com/nikhilgiridharan/TradePulse
cd TradePulse
cp .env.example .env

# Add your keys to .env:
# POLYGON_API_KEY, FINNHUB_API_KEY
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# Start everything
make up

# Dashboard available at http://localhost:8000
```

Services started by `make up`:
- Kafka + Zookeeper
- Polygon.io equity producer
- Finnhub news producer
- Faust stream processor
- FastAPI dashboard
```bash
make logs    # Tail all service logs
make test    # Run unit + integration tests
make down    # Stop all services
```

---

## Production Incident — Hot Partition Postmortem

On February 3rd, 2025 at 9:32 AM EST, consumer lag spiked to 45 seconds at market open. Root cause: bare ticker partition key routing all AAPL writes to a single DynamoDB partition, causing 847 throttled writes/minute. Resolution: implemented write sharding pattern. Fix took 4 minutes to deploy. Zero data lost — Kafka buffered all events during the 26-minute degradation.

Full timeline, root cause, and follow-up action items: [docs/postmortem.md](docs/postmortem.md)

---

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | Full system design with component responsibilities |
| [Schema](docs/schema.md) | DynamoDB table design, partition keys, access patterns |
| [Benchmarks](docs/benchmarks.md) | Load test methodology and full results |
| [Runbook](docs/runbook.md) | Operational procedures and alert thresholds |
| [Feature Store](docs/features.md) | Feature definitions and update frequency |
| [Postmortem](docs/postmortem.md) | Hot partition incident — cause, fix, learnings |
| [Deployment](docs/deployment.md) | Railway setup and local full-pipeline instructions |

---

## Project Structure
```
TradePulse/
├── src/
│   ├── producer/
│   │   ├── polygon_producer.py      # WebSocket → Kafka, idempotent
│   │   └── news_producer.py         # Finnhub polling → Kafka
│   ├── processing/
│   │   ├── faust_app.py             # Main stream processor, exactly-once
│   │   ├── aggregations.py          # VWAP, z-score, momentum
│   │   ├── anomaly_detection.py     # Isolation Forest, rolling retrain
│   │   ├── sentiment_analyzer.py    # VADER + market correlation
│   │   └── feature_store.py         # Precomputed features → DynamoDB
│   ├── validation/
│   │   └── schema_validator.py      # Pydantic validation + DLQ routing
│   ├── storage/
│   │   ├── dynamo_writer.py         # Shard keys, conditional writes
│   │   ├── s3_writer.py             # Parquet buffering, Hive partitioning
│   │   └── dlq_handler.py           # SQS retry + S3 archival
│   ├── api/
│   │   ├── main.py                  # FastAPI, 6 endpoints, CORS
│   │   └── static/
│   │       ├── index.html           # Live dashboard
│   │       └── about.html           # Documentation page
│   └── monitoring/
│       └── cloudwatch_metrics.py    # Batched CloudWatch emission
├── tests/
│   ├── unit/                        # Aggregation math, anomaly, validation
│   └── integration/                 # Kafka, DynamoDB, DLQ end-to-end
├── docs/                            # Architecture, schema, runbook, postmortem
├── docker-compose.yml               # Full local stack
├── Dockerfile                       # Railway production image
├── railway.toml                     # Railway deployment config
└── Makefile                         # make up · make test · make lint
```

---

**Nikhil Giridharan** — Data Engineer

