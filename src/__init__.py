"""
MarketFlow — real-time market data pipeline.

Components: producer (Polygon → Kafka), processing (Faust), validation,
storage (DynamoDB, S3, DLQ), API (FastAPI), monitoring (CloudWatch).
"""

__version__ = "1.0.0"
