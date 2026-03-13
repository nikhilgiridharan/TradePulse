# DynamoDB Schema

## Tables

### market_trades

| Attribute   | Description |
|------------|-------------|
| Partition key | `pk` (format: `ticker#shard`, shard = hash(ticker+timestamp) % 8) |
| Sort key   | `timestamp` (ISO 8601) |
| TTL        | `ttl` (epoch), 48 hours |

**Partition key rationale**: A bare `ticker` key would route all AAPL writes to one partition (~800 writes/sec at open), causing throttling. Sharding across 8 partitions distributes load.

**Access patterns**: Latest trade per ticker (query each shard, take latest); range query by time per shard.

**Estimated WCU**: ~15k events/sec → ~15k WCU/sec; with 8 shards ~2k WCU/sec per partition. At 10x: scale to more shards or on-demand.

**TTL**: 48 hours for raw trades; downstream aggregations and features have longer retention.

### market_aggregations

| Attribute   | Description |
|------------|-------------|
| Partition key | `pk` (ticker, e.g. AAPL) |
| Sort key   | `window_start` (ISO 8601) |
| TTL        | `ttl`, 7 days |

**Access patterns**: Latest aggregations per ticker (query pk=ticker, ScanIndexForward=false, Limit=1).

**TTL**: 7 days; aggregations are derived data, 7 days supports backtesting and debugging.

### market_anomalies

| Attribute   | Description |
|------------|-------------|
| Partition key | `pk` (ticker) |
| Sort key   | `timestamp` |
| TTL        | `ttl`, 30 days |

**Access patterns**: List anomalies per ticker in last N hours (query pk, sort_key between).

**TTL**: 30 days for incident review and model tuning.

### feature_store

| Attribute   | Description |
|------------|-------------|
| Partition key | `pk` (ticker#YYYY-MM-DD-HH) |
| Sort key   | `timestamp` |
| TTL        | `ttl`, 7 days |

**Partition key rationale**: Hour bucket distributes writes (write sharding); 24 partitions per day per ticker.

**Access patterns**: Get current feature vector (query current hour bucket, latest item).

## GSI Recommendations

- **market_trades**: GSI on `ticker` (or `ticker#date`) + `timestamp` for "latest trade by ticker" without scanning shards.
- **market_anomalies**: GSI on `ticker` + `timestamp` for time-range queries (already supported by base table).

## Capacity

| Table              | ~15k/sec WCU | 10x WCU |
|--------------------|--------------|---------|
| market_trades      | ~15k (sharded) | On-demand or higher provisioned |
| market_aggregations| ~5 (per ticker window) | ~50 |
| market_anomalies   | ~1k (anomalies only) | ~10k |
| feature_store     | ~5 (per ticker per hour) | ~50 |
