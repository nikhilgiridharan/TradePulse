# TradePulse Runbook

## Deployment

### From scratch

1. **AWS**: Create DynamoDB tables (see schema.md), S3 bucket, SQS queues (main DLQ, validation DLQ). Create IAM user/role with access.
2. **Kafka**: Create topics `market.trades`, `market.trades.dlq`, `market.aggregations`, `market.anomalies` (or rely on auto-create with 6 partitions).
3. **Env**: Copy `.env.example` to `.env`, set `POLYGON_API_KEY`, `KAFKA_BOOTSTRAP_SERVERS`, AWS credentials, table names, SQS URLs.
4. **Docker**: `docker-compose up -d`. Order: Zookeeper → Kafka (wait healthy) → Schema Registry → tradepulse-producer → tradepulse-processing → tradepulse-api.
5. **Verify**: `GET http://localhost:8000/health`, `GET http://localhost:8000/docs`.

### Startup sequence

1. Start Zookeeper and Kafka; wait for Kafka healthcheck.
2. Start producer (will connect to Polygon and publish to Kafka).
3. Start Faust processing worker (consumes from market.trades).
4. Start API (depends on processing so aggregations exist).

---

## Consumer Lag Spike (>10,000 messages)

1. **Check CloudWatch** `DynamoThrottles`: If spiking, DynamoDB is throttling.
2. **If throttling**: Check partition key distribution (CloudWatch per-partition or logs). Temporary: increase WCU in AWS console. Permanent: verify shard key (ticker#shard), add shards if needed.
3. **If not throttling**: Check Faust logs for processing errors (validation, DynamoDB, S3). Check `BackpressureActivations` — if high, DynamoDB write latency is above threshold; consider scaling DynamoDB or increasing backpressure threshold.
4. **Resolution**: Scale DynamoDB (WCU or on-demand), fix hot partition, or increase consumer parallelism (more Faust workers).

---

## DynamoDB Throttling

1. **Identify table**: CloudWatch metric `DynamoThrottles` has dimension `table`.
2. **Hot partition?**: Check if one partition key (or shard) gets disproportionate traffic. Use CloudWatch Contributor Insights or sample keys from logs.
3. **Temporary**: Increase WCU for that table in AWS console.
4. **Permanent**: Verify shard key in code (e.g. 8 shards for trades). Add more shards (e.g. 16) if needed. Consider on-demand capacity.

---

## DLQ Depth > 100

1. **Inspect messages**: SQS console → receive messages, check body for `error_reason`, `error_type`, `original_message`.
2. **Categorize**: Validation failures (bad payload) vs processing failures (DynamoDB/S3/AWS) vs upstream (Polygon).
3. **Validation**: Check upstream data quality; fix producer or add schema evolution.
4. **AWS failures**: Check AWS Service Health; retry after recovery.
5. **Reprocess**: Once root cause fixed, trigger DLQ processor (or re-publish from S3 archive if archived).

---

## Alerting Thresholds

| Metric               | Warning | Critical |
|----------------------|---------|----------|
| ConsumerLag          | 5,000   | 10,000   |
| DynamoWriteLatency p99 | 50 ms | 100 ms   |
| DynamoThrottles      | 10/min  | 50/min   |
| DLQDepth             | 50      | 100      |
| ProducerErrors       | 5/min   | 20/min   |
| APILatency p99       | 200 ms  | 500 ms   |
