#!/usr/bin/env bash
# TradePulse — LocalStack ready hook: DynamoDB (4 pipeline tables + correlations),
# SQS (tradepulse-dlq, tradepulse-archive, legacy validation/processing), S3 cold storage.
set -euo pipefail

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"

aws() {
  command aws --endpoint-url "$ENDPOINT" "$@"
}

echo "[tradepulse] LocalStack init @ $ENDPOINT"

# --- S3 cold storage ---
aws s3 mb "s3://tradepulse-cold-storage" 2>/dev/null || true

# --- SQS ---
aws sqs create-queue --queue-name tradepulse-dlq 2>/dev/null || true
aws sqs create-queue --queue-name tradepulse-archive 2>/dev/null || true
aws sqs create-queue --queue-name tradepulse-validation-dlq 2>/dev/null || true
aws sqs create-queue --queue-name tradepulse-processing-dlq 2>/dev/null || true

# --- DynamoDB (pk + sk + ttl attribute) ---
create_table() {
  local name="$1"
  aws dynamodb create-table \
    --table-name "$name" \
    --billing-mode PAY_PER_REQUEST \
    --attribute-definitions \
      AttributeName=pk,AttributeType=S \
      AttributeName=sk,AttributeType=S \
    --key-schema \
      AttributeName=pk,KeyType=HASH \
      AttributeName=sk,KeyType=RANGE \
    2>/dev/null || echo "  (exists) $name"
}

create_table "market_quotes"
create_table "market_anomalies"
create_table "market_sentiment"
create_table "market_features"
create_table "market_correlations"

echo "[tradepulse] LocalStack init complete."
