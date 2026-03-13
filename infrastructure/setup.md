# MarketFlow Infrastructure Setup

## DynamoDB Tables

Create in AWS Console or IaC (Terraform/CloudFormation):

- **market_trades**: PK `ticker#shard` (String), SK `timestamp` (String), TTL attribute `ttl`. Provisioned or on-demand WCU/RCU per docs/schema.md.
- **market_aggregations**: PK `ticker#window_type` (String), SK `window_start` (String).
- **market_anomalies**: PK `ticker` (String), SK `timestamp` (String).
- **feature_store**: PK `ticker#date_bucket` (String), SK `timestamp` (String), TTL attribute `ttl`.

## S3

- Bucket: e.g. `marketflow-data`. Prefixes: `trades/`, `anomalies/`, `dead-letters/`.
- Optional: lifecycle rules to move old trades to Glacier.

## SQS

- Queues: `marketflow-dlq`, `marketflow-validation-dlq`. Visibility timeout recommended 900 seconds (15 min) for retry interval.
- Dead-letter: after 5 receives, move to S3 via Lambda or custom consumer (see dlq_handler).

## Glue

- Database: `marketflow`. Table: `trades` with columns (ticker, price, volume, timestamp, event_type); partition keys year, month, day, hour; location s3://marketflow-data/trades/.

## IAM

Minimum permissions for app role:

- DynamoDB: PutItem, GetItem, Query, BatchWriteItem on the four tables.
- S3: PutObject, GetObject on the bucket/prefixes.
- SQS: SendMessage, ReceiveMessage, DeleteMessage, GetQueueAttributes on the two queues.
- CloudWatch: PutMetricData in namespace MarketFlow/Production.
- Glue: BatchCreatePartition (for S3 partition registration).

## Kafka

- Confluent Cloud or self-hosted. Topic `market.trades` (create if not auto-created); partitions ≥ number of Faust workers for parallelism. Topic `market.trades.dlq` for invalid messages.
