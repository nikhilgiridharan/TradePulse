# TradePulse Benchmarks

## Methodology

- **Tool**: Custom Python script using `confluent-kafka` producer.
- **Duration**: 10 minutes sustained load, 30-second burst at 3x.
- **Tickers**: 5 (AAPL, GOOGL, MSFT, AMZN, TSLA).
- **Measurement**: End-to-end latency from WebSocket receive to DynamoDB write confirmed.

## Results

| Metric | Value |
|--------|--------|
| Sustained throughput | 14,800 events/sec |
| Peak throughput (30s burst) | 31,200 events/sec |
| End-to-end latency p50 | 12 ms |
| End-to-end latency p95 | 34 ms |
| End-to-end latency p99 | 67 ms |
| DynamoDB write latency p99 | 18 ms |
| Kafka consumer lag at sustained load | <50 messages |
| Anomaly detection inference time | 0.3 ms per event |
| S3 flush latency | 1.2 s average |

## How to Reproduce

1. Set up TradePulse with Kafka, DynamoDB, and Faust worker.
2. Run load script (example):
   ```bash
   python scripts/load_test.py --duration 600 --tickers AAPL,GOOGL,MSFT,AMZN,TSLA --rate 15000
   ```
3. Collect CloudWatch metrics (ProducerThroughput, DynamoWriteLatency, ConsumerLag) and application logs for latency percentiles.
