# MarketFlow Verification Summary

## File Summary

| File | Lines | Purpose |
|------|-------|---------|
| .env.example | 62 | Template for all required env vars |
| .gitignore | 68 | Ignore Python, IDE, secrets, Docker |
| requirements.txt | 47 | Pinned dependencies with group comments |
| Makefile | 42 | up, down, logs, test, lint, format, clean |
| src/config.py | 174 | Centralized Pydantic Settings; nested Kafka, AWS, Dynamo, S3, SQS, API, Pipeline |
| src/__init__.py | 8 | Package version |
| src/validation/__init__.py | 8 | Export MarketEvent, validate_message |
| src/validation/schema_validator.py | 147 | MarketEvent model, validate_message, ValidationMetrics |
| src/monitoring/__init__.py | 8 | Export CloudWatchMetrics, get_metrics |
| src/monitoring/cloudwatch_metrics.py | 193 | Buffered CloudWatch emitter; doc of all metrics |
| src/storage/__init__.py | 10 | Export DynamoWriter, S3Writer, DLQHandler, DLQMessage |
| src/storage/dynamo_writer.py | 207 | Sharded writes, conditional exactly-once, throttle retry |
| src/storage/s3_writer.py | 185 | Buffered Parquet flush; Glue partition registration |
| src/storage/dlq_handler.py | 212 | SQS DLQ send/process; S3 archive after max retries |
| src/processing/__init__.py | 4 | Package doc |
| src/processing/aggregations.py | 182 | VWAP, volume z-score, momentum, RollingAggregations |
| src/processing/anomaly_detection.py | 131 | Per-ticker Isolation Forest; AnomalyResult |
| src/processing/faust_app.py | 175 | Faust app, process_trades agent, backpressure, consumer lag stub |
| src/processing/feature_store.py | 136 | FeatureStore; hour-bucket pk; update_features, get_features |
| src/producer/__init__.py | 4 | Package doc |
| src/producer/polygon_producer.py | 195 | Polygon WebSocket → Kafka; throughput/error metrics |
| src/api/__init__.py | 3 | Package doc |
| src/api/main.py | 260 | FastAPI: quotes, aggregations, anomalies, features, health; rate limit, CORS |
| tests/conftest.py | 125 | mock_settings (autouse), mock_dynamo/s3/sqs, sample events, mock_cloudwatch |
| tests/unit/test_aggregations.py | 64 | VWAP, z-score, momentum, window reset |
| tests/unit/test_anomaly_detection.py | 104 | Train threshold, anomaly flag, feature vector length |
| tests/unit/test_schema_validator.py | 159 | Valid/invalid events; return (event, None) / (None, error) |
| tests/integration/test_dynamo_writer.py | 140 | write_trade, conditional write, shards, TTL, throttle |
| tests/integration/test_dlq_handler.py | 134 | send_to_dlq, depth, retry, archive to S3 |
| tests/integration/test_kafka_producer.py | 57 | Producer topic, partition key, produce call |
| docker-compose.yml | 94 | Zookeeper, Kafka, Schema Registry, producer, processing, API; volumes |
| Dockerfile.producer | 8 | Python 3.11; run polygon_producer |
| Dockerfile.processing | 9 | Python 3.11; run faust_app |
| Dockerfile.api | 9 | Python 3.11; uvicorn api |
| .github/workflows/ci.yml | 51 | Lint, mypy, unit + integration pytest, upload results |
| docs/architecture.md | 39 | Data flow, components, design decisions |
| docs/schema.md | 69 | DynamoDB tables, keys, TTL, access patterns, GSI |
| docs/runbook.md | 59 | Deployment, consumer lag, throttling, DLQ, alert table |
| docs/benchmarks.md | 31 | Methodology, results table, reproduce steps |
| docs/setup.md | 29 | Prerequisites, quick start, env vars |
| docs/features.md | 19 | Feature store, aggregations, anomaly |
| docs/postmortem.md | 54 | Hot partition incident; timeline, root cause, actions |
| README.md | 112 | Badges, architecture, design decisions, benchmarks, cost, getting started |

---

## Environment Variables (and where used)

| Variable | Used in |
|----------|--------|
| POLYGON_API_KEY | config, polygon_producer |
| KAFKA_BOOTSTRAP_SERVERS | config, faust_app, polygon_producer |
| KAFKA_TOPIC_* | config, faust_app, polygon_producer |
| AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY | config, dynamo_writer, s3_writer, dlq_handler, cloudwatch_metrics |
| DYNAMO_TABLE_* | config, dynamo_writer, feature_store |
| S3_BUCKET_NAME, S3_PREFIX_* | config, s3_writer, dlq_handler |
| SQS_DLQ_URL, SQS_VALIDATION_DLQ_URL | config, dlq_handler |
| API_HOST, API_PORT, API_RATE_LIMIT | config, api/main |
| TICKERS | config, polygon_producer |
| CLOUDWATCH_NAMESPACE, CLOUDWATCH_ENABLED | config, cloudwatch_metrics, schema_validator, dynamo_writer, etc. |
| BACKPRESSURE_* | config, faust_app |
| DLQ_MAX_RETRIES, DLQ_RETRY_INTERVAL_MINUTES | config, dlq_handler |
| S3_BUFFER_* | config, s3_writer |
| ANOMALY_* | config, anomaly_detection |

---

## Most Architecturally Significant Files

1. **src/processing/faust_app.py** — Central orchestration: consumes trades, runs validation → DLQ routing, aggregations, anomaly detection, feature store, DynamoDB/S3 writes, and backpressure. Defines exactly-once semantics and the shape of the pipeline.

2. **src/storage/dynamo_writer.py** — Implements the shard-key pattern that prevents hot partitions (postmortem fix). Conditional writes give exactly-once at the sink. Throttle handling and latency reporting drive backpressure in the Faust app.

3. **src/config.py** — Single source of truth for all configuration. Fail-fast validation and nested groups (Kafka, AWS, Dynamo, S3, SQS, API, Pipeline) ensure every component uses the same values and misconfiguration is caught at startup.

---

## Assumptions to Verify Before Running

1. **AWS region** — Code assumes `us-east-1` as default. Override with `AWS_REGION` if you use another region. DynamoDB tables, S3 bucket, and SQS queues must exist in that region.

2. **DynamoDB capacity** — Tables are used with **provisioned** or **on-demand** (PAY_PER_REQUEST). The runbook and schema doc describe WCU scaling. For local/moto tests, BillingMode is PAY_PER_REQUEST in conftest. For production, create tables with the desired capacity mode.

3. **Kafka** — docker-compose uses `KAFKA_BOOTSTRAP_SERVERS`; in containers the producer and Faust must reach Kafka at `kafka:29092`. Set `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` in .env when running with Docker. For local runs, use `localhost:9092`.

4. **Polygon WebSocket** — Producer uses `polygon-api-client` (WebSocketClient, STOCKS_CLUSTER). If the package API differs (e.g. subscribe format), adjust `polygon_producer.py`.

5. **Glue** — S3Writer registers partitions with Glue (`batch_create_partition`). Assumes a Glue database `marketflow` and table `trades`. Create these or disable/adapt the call if you don’t use Glue.

---

## Verification Commands (run locally)

```bash
# Validate docker-compose
docker compose config

# Install deps and run tests (set env vars or use .env)
pip install -r requirements.txt
pytest tests/ -v --tb=short --junitxml=pytest-results.xml

# Optional: run full stack
make up
```
