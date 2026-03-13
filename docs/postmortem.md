# Postmortem: DynamoDB Hot Partition Causing 45-Second Consumer Lag During Market Open — February 3, 2025

**Severity**: SEV-2 (Degraded service; data delayed but not lost)

**Duration**: 9:32 AM EST — 9:58 AM EST (26 minutes)

**Impact**: Market trade data delayed by up to 45 seconds. Aggregations (VWAP, volume z-score) stale. Anomaly detection effectively offline during the window. API returning cached/stale data. No data loss — all events buffered in Kafka.

## Timeline (minute by minute)

- **9:30 AM**: Market opens; trade volume spikes 8x vs pre-market baseline.
- **9:31 AM**: DynamoDB write latency begins climbing (p99 crosses 200 ms).
- **9:32 AM**: CloudWatch alarm fires — ConsumerLag > 10,000 messages.
- **9:33 AM**: On-call engineer acknowledges alert, begins investigation.
- **9:35 AM**: CloudWatch shows DynamoThrottles metric spiking — 847 throttled writes in 60 seconds.
- **9:37 AM**: Engineer identifies hot partition — all AAPL writes routing to single partition key "AAPL".
- **9:40 AM**: Temporary mitigation — manually increased DynamoDB write capacity from 100 to 500 WCU.
- **9:44 AM**: Throttling reduces but not eliminated; root cause is partition design, not capacity.
- **9:48 AM**: Deploy fix — updated partition key from "AAPL" to "AAPL#shard" with 8 shards.
- **9:52 AM**: New partition key live; throttling stops immediately.
- **9:55 AM**: Consumer lag begins decreasing.
- **9:58 AM**: Consumer lag returns to baseline (<100 messages); all systems normal.

## Root Cause

The DynamoDB `market_trades` table used ticker symbol as the bare partition key (e.g. "AAPL"). DynamoDB distributes data across internal partitions by partition key hash. With one key per ticker, all writes for that ticker go to the same internal partition. At market open, AAPL alone generated ~800 writes/second, exceeding DynamoDB’s per-partition throughput (~1000 WCU/sec). The partition became hot and throttled writes, causing the Faust consumer to back up.

## Resolution

Implemented write sharding: partition key is now "AAPL#n" where n = hash(ticker + timestamp) % 8. This spreads AAPL writes across 8 partitions, reducing per-partition load from ~800/sec to ~100/sec.

## Why This Wasn’t Caught Earlier

Load testing used 1,000 events/second total across all tickers. The hot partition only appears when a single ticker exceeds ~800 writes/second, which occurs in the first minutes of market open on high-volatility days.

## Action Items

- Implement shard key pattern on all DynamoDB tables (completed Feb 3).
- Add backpressure to slow consumption when write latency exceeds 100 ms (completed Feb 5).
- Add DLQDepth CloudWatch alarm to catch message buildup earlier (completed Feb 5).
- Update load testing to simulate market-open burst traffic (due Feb 17).
- Add per-partition DynamoDB write latency to CloudWatch dashboard (due Feb 17).

## What Went Well

- No data loss; Kafka buffered all events during the 26-minute window.
- CloudWatch alarm fired within 2 minutes of the issue starting.
- Fix was straightforward once root cause was identified.

## What Went Poorly

- Six minutes between alarm and root cause identification.
- Temporary capacity increase was a distraction; it helped briefly but didn’t fix the design.
- Load testing did not reflect realistic burst patterns.
