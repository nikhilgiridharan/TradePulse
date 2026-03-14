# TradePulse Features

## Real-Time Feature Store

The feature store holds the current feature vector per ticker: VWAP (5 min), volume z-score, price momentum (1 min), trade frequency, and optional bid-ask spread. Partition key is `ticker#YYYY-MM-DD-HH` (hour bucket) for write distribution. TTL 7 days. Used by the API (`GET /features/{ticker}`) and by anomaly detection as inputs. Features are updated on every validated trade in the Faust pipeline.

## Aggregations

- **VWAP** (1 min and 5 min): Volume-weighted average price; tumbling windows.
- **Rolling average price**: 5-minute hopping window (hop 1 min).
- **Volume z-score**: (volume - mean_20) / std_20 over last 20 periods.
- **Price momentum**: (current_price - price_60s_ago) / price_60s_ago * 100.
- **Trade frequency**: Trades per minute in current window.

Stored in DynamoDB `market_aggregations` (pk = ticker, sort_key = window_start).

## Anomaly Detection

Per-ticker Isolation Forest with rolling training window. Contamination 0.01. Features: price, volume, volume_zscore, price_momentum, vwap_deviation, trade_frequency. Retrain every N events. Anomalies written to DynamoDB and S3.
