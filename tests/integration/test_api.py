"""FastAPI TestClient: mocked DynamoDB, status codes, rate limit, validation."""

from __future__ import annotations

import time
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.api.dependencies import get_dynamo_writer, reset_api_cache
from src.api.limiter_ext import limiter
from src.api.main import app
from src.config import clear_settings_cache


@pytest.fixture
def client() -> TestClient:
    with TestClient(app) as c:
        yield c


@pytest.fixture
def client_mock_dynamo(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("DEMO_MODE", "false")
    clear_settings_cache()
    reset_api_cache()

    class FakeWriter:
        def __init__(self) -> None:
            self.quotes_empty = False
            self.agg_empty = False
            self.anomalies_empty = False
            self.features_empty = False
            self.sentiment_empty = False

        async def query_quotes_for_ticker(
            self, ticker: str, limit: int = 1, start_time=None, end_time=None
        ) -> list[dict[str, Any]]:
            if self.quotes_empty:
                return []
            return [
                {
                    "sk": "2024-06-01T12:00:00+00:00#e1",
                    "price": 120.5,
                    "volume": 50,
                    "vwap_1min": 120.0,
                    "vwap_5min": 119.0,
                    "volume_zscore": 0.5,
                    "momentum": 1.0,
                }
            ]

        async def get_latest_quote_metrics(self, ticker: str) -> dict[str, Any] | None:
            if self.agg_empty:
                return None
            return {
                "sk": "2024-06-01T12:00:00+00:00#e1",
                "price": 130.0,
                "volume": 40,
                "vwap_1min": 129.0,
                "vwap_5min": 128.0,
                "volume_zscore": 0.1,
                "momentum": 0.2,
            }

        async def query_anomalies_for_ticker(
            self, ticker: str, limit: int = 50, start_time=None, end_time=None
        ) -> list[dict[str, Any]]:
            if self.anomalies_empty:
                return []
            return [
                {
                    "sk": "2024-06-01T12:00:00+00:00#1",
                    "score": 0.5,
                    "is_anomaly": True,
                    "features": {"price": 100.0},
                    "model_version": 1,
                }
            ]

        async def get_latest_features(self, ticker: str) -> dict[str, Any] | None:
            if self.features_empty:
                return None
            return {
                "updated_at": "2024-06-01T12:00:00+00:00",
                "feature_vector": {
                    "price": 1.0,
                    "volume": 2.0,
                    "vwap_1min": 3.0,
                    "volume_zscore": 0.0,
                    "momentum": 0.0,
                    "bid_ask_spread": 0.0,
                    "trade_frequency": 1.0,
                    "vwap_5min": 3.5,
                    "price_momentum": 0.1,
                },
                "ttl": int(time.time()) + 86_400,
            }

        async def query_sentiment_for_ticker(
            self, ticker: str, limit: int = 50, start_time=None, end_time=None
        ) -> list[dict[str, Any]]:
            if self.sentiment_empty:
                return []
            return [
                {
                    "headline": "Hello",
                    "compound": 0.1,
                    "positive": 0.2,
                    "negative": 0.0,
                    "neutral": 0.8,
                    "sk": "2024-06-01T12:00:00+00:00#1",
                }
            ]

    fake = FakeWriter()

    async def _writer_override() -> FakeWriter:
        return fake

    app.dependency_overrides[get_dynamo_writer] = _writer_override
    with TestClient(app) as c:
        c.fake_writer = fake  # type: ignore[attr-defined]
        yield c
    app.dependency_overrides.clear()
    clear_settings_cache()
    reset_api_cache()


def test_health_contains_kafka_dynamo_status(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] in ("ok", "degraded", "down")
    assert "kafka" in body
    assert "dynamo" in body
    assert isinstance(body["pipeline_events_per_sec"], float)
    assert isinstance(body["consumer_lag"], int)
    assert isinstance(body["uptime_seconds"], int)


def test_quotes_200_mocked(client_mock_dynamo: TestClient) -> None:
    r = client_mock_dynamo.get("/quotes/AAPL")
    assert r.status_code == 200
    data = r.json()
    assert data["ticker"] == "AAPL"
    assert len(data["items"]) >= 1
    assert data["items"][0]["price"] == 120.5


def test_quotes_404_mocked(client_mock_dynamo: TestClient) -> None:
    client_mock_dynamo.fake_writer.quotes_empty = True
    r = client_mock_dynamo.get("/quotes/AAPL")
    assert r.status_code == 404
    assert "No data found" in r.json()["detail"]


def test_aggregations_200_and_404_mocked(client_mock_dynamo: TestClient) -> None:
    r = client_mock_dynamo.get("/aggregations/MSFT")
    assert r.status_code == 200
    assert r.json()["ticker"] == "MSFT"

    client_mock_dynamo.fake_writer.agg_empty = True
    reset_api_cache()
    r2 = client_mock_dynamo.get("/aggregations/MSFT")
    assert r2.status_code == 404


def test_anomalies_200_and_404_mocked(client_mock_dynamo: TestClient) -> None:
    r = client_mock_dynamo.get("/anomalies/NVDA")
    assert r.status_code == 200
    assert r.json()["ticker"] == "NVDA"
    assert len(r.json()["items"]) >= 1

    client_mock_dynamo.fake_writer.anomalies_empty = True
    reset_api_cache()
    r2 = client_mock_dynamo.get("/anomalies/NVDA")
    assert r2.status_code == 404


def test_features_200_and_404_mocked(client_mock_dynamo: TestClient) -> None:
    r = client_mock_dynamo.get("/features/AMZN")
    assert r.status_code == 200
    assert r.json()["ticker"] == "AMZN"

    client_mock_dynamo.fake_writer.features_empty = True
    reset_api_cache()
    r2 = client_mock_dynamo.get("/features/AMZN")
    assert r2.status_code == 404


def test_sentiment_200_and_404_mocked(client_mock_dynamo: TestClient) -> None:
    r = client_mock_dynamo.get("/sentiment/TSLA")
    assert r.status_code == 200
    body = r.json()
    assert body["ticker"] == "TSLA"
    assert "summary" in body

    client_mock_dynamo.fake_writer.sentiment_empty = True
    reset_api_cache()
    r2 = client_mock_dynamo.get("/sentiment/TSLA")
    assert r2.status_code == 404


def test_invalid_ticker_422(client_mock_dynamo: TestClient) -> None:
    r = client_mock_dynamo.get("/quotes/IBM")
    assert r.status_code == 422


def test_zzz_rate_limit_101st_request_returns_429(client_mock_dynamo: TestClient) -> None:
    """Run last (name prefix): exhaust per-IP quota for ``GET /quotes/{ticker}``."""
    limiter.reset()
    for i in range(100):
        resp = client_mock_dynamo.get("/quotes/AAPL")
        assert resp.status_code == 200, f"request {i + 1}"
    resp101 = client_mock_dynamo.get("/quotes/AAPL")
    assert resp101.status_code == 429
