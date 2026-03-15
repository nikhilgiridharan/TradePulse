"""
Centralized configuration for TradePulse pipeline.

This module is the single source of truth for all configuration. Loading from
environment variables (and .env) with validation at startup ensures we fail fast
on misconfiguration rather than discovering missing credentials or typos at runtime.
Used by every component: producer, Faust app, storage, API, monitoring.
"""

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseSettings):
    """Kafka broker and topic configuration."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore")

    bootstrap_servers: str = Field(default="localhost:9092", description="Broker list, e.g. localhost:9092")
    security_protocol: str = Field(default="PLAINTEXT", description="PLAINTEXT or SASL_SSL")
    sasl_mechanism: Optional[str] = Field(default=None, description="PLAIN or SCRAM-SHA-256")
    sasl_username: Optional[str] = Field(default=None)
    sasl_password: Optional[str] = Field(default=None)
    topic_trades: str = Field(default="market.trades", alias="topic_trades")
    topic_dlq: str = Field(default="market.trades.dlq", alias="topic_dlq")
    topic_aggregations: str = Field(default="market.aggregations", alias="topic_aggregations")
    topic_anomalies: str = Field(default="market.anomalies", alias="topic_anomalies")
    topic_news: str = Field(default="market.news", env="KAFKA_TOPIC_NEWS")


class AWSConfig(BaseSettings):
    """AWS credentials and region."""

    model_config = SettingsConfigDict(env_prefix="AWS_", extra="ignore")

    region: str = Field(default="us-east-1", description="Primary region for DynamoDB, S3, SQS")
    access_key_id: Optional[str] = Field(default=None, description="Leave unset to use IAM role")
    secret_access_key: Optional[str] = Field(default=None)


class DynamoConfig(BaseSettings):
    """DynamoDB table names."""

    model_config = SettingsConfigDict(env_prefix="DYNAMO_", extra="ignore")

    table_trades: str = Field(default="market_trades", alias="table_trades")
    table_aggregations: str = Field(default="market_aggregations", alias="table_aggregations")
    table_anomalies: str = Field(default="market_anomalies", alias="table_anomalies")
    table_features: str = Field(default="feature_store", alias="table_features")
    table_sentiment: str = Field(default="market_sentiment", env="DYNAMO_TABLE_SENTIMENT")


class S3Config(BaseSettings):
    """S3 bucket and key prefixes."""

    model_config = SettingsConfigDict(env_prefix="S3_", extra="ignore")

    bucket_name: str = Field(default="", description="Bucket for trades, anomalies, dead-letters")
    prefix_trades: str = Field(default="trades", alias="prefix_trades")
    prefix_anomalies: str = Field(default="anomalies", alias="prefix_anomalies")
    prefix_dead_letters: str = Field(default="dead-letters", alias="prefix_dead_letters")


class SQSConfig(BaseSettings):
    """SQS queue URLs for DLQ."""

    model_config = SettingsConfigDict(env_prefix="SQS_", extra="ignore")

    dlq_url: str = Field(default="", description="Main DLQ for processing failures")
    validation_dlq_url: str = Field(default="", description="DLQ for validation failures only")


class APIConfig(BaseSettings):
    """FastAPI server and rate limit."""

    model_config = SettingsConfigDict(env_prefix="API_", extra="ignore")

    host: str = Field(default="0.0.0.0", description="Bind to all interfaces in container")
    port: int = Field(default=8000, ge=1, le=65535)
    rate_limit: int = Field(default=100, description="Requests per minute per IP")


class PipelineConfig(BaseSettings):
    """Pipeline tuning: backpressure, DLQ, S3 buffer, anomaly detection."""

    model_config = SettingsConfigDict(extra="ignore")

    backpressure_latency_threshold_ms: int = Field(
        default=100,
        alias="BACKPRESSURE_LATENCY_THRESHOLD_MS",
        description="Pause consumption when DynamoDB write latency exceeds this (ms)",
    )
    backpressure_pause_ms: int = Field(
        default=500,
        alias="BACKPRESSURE_PAUSE_MS",
        description="Sleep duration when backpressure is active",
    )
    dlq_max_retries: int = Field(
        default=3,
        alias="DLQ_MAX_RETRIES",
        description="After this many retries, archive to S3",
    )
    dlq_retry_interval_minutes: int = Field(
        default=15,
        alias="DLQ_RETRY_INTERVAL_MINUTES",
        description="Minutes between DLQ retry attempts",
    )
    s3_buffer_flush_interval_seconds: int = Field(
        default=300,
        alias="S3_BUFFER_FLUSH_INTERVAL_SECONDS",
        description="Max age of buffer before flush (5 min)",
    )
    s3_buffer_max_mb: float = Field(
        default=10.0,
        alias="S3_BUFFER_MAX_MB",
        description="Flush when buffer size exceeds this (MB)",
    )
    anomaly_contamination: float = Field(
        default=0.01,
        alias="ANOMALY_CONTAMINATION",
        description="Expected fraction of anomalies (Isolation Forest)",
    )
    anomaly_training_window: int = Field(
        default=1000,
        alias="ANOMALY_TRAINING_WINDOW",
        description="Min samples before model is trained",
    )
    anomaly_retrain_interval: int = Field(
        default=500,
        alias="ANOMALY_RETRAIN_INTERVAL",
        description="Retrain after this many new events",
    )
    finnhub_api_key: str = Field(default="", env="FINNHUB_API_KEY")


class Settings(BaseSettings):
    """
    Root settings: loads from env and .env, nests component configs.
    Validation runs on first access; missing required fields raise immediately.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_nested_delimiter="__",
    )

    polygon_api_key: str = Field(default="", alias="POLYGON_API_KEY")
    tickers: str = Field(default="AAPL,GOOGL,MSFT,AMZN,TSLA", description="Comma-separated symbols")
    cloudwatch_namespace: str = Field(default="TradePulse/Production", alias="CLOUDWATCH_NAMESPACE")
    cloudwatch_enabled: bool = Field(default=False, alias="CLOUDWATCH_ENABLED")

    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    aws: AWSConfig = Field(default_factory=AWSConfig)
    dynamo: DynamoConfig = Field(default_factory=DynamoConfig)
    s3: S3Config = Field(default_factory=S3Config)
    sqs: SQSConfig = Field(default_factory=SQSConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)

    @property
    def ticker_list(self) -> list[str]:
        """Parsed list of ticker symbols from TICKERS env."""
        return [t.strip() for t in self.tickers.split(",") if t.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return cached Settings instance. Loads and validates once per process.
    Call this from all modules that need config; do not instantiate Settings directly.
    """
    return Settings()
