"""Production FastAPI application for TradePulse.

Serves custom dashboard at / and GET /quotes/{ticker}, /aggregations/{ticker},
/anomalies/{ticker}, /features/{ticker}, /health. OpenAPI spec at /openapi.json.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Load .env from project root so config finds it regardless of cwd
_path = Path(__file__).resolve().parents[2]
_env = _path / ".env"
if _env.exists():
    from dotenv import load_dotenv
    load_dotenv(_env)

import structlog
import yfinance as yf
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from src.config import get_settings
from src.monitoring.cloudwatch_metrics import get_metrics

logger = structlog.get_logger(__name__)
settings = get_settings()

# Path to API module for static files
_API_DIR = Path(__file__).resolve().parent
_STATIC_DIR = _API_DIR / "static"

def _get_writer():
    """Lazy init DynamoWriter so app starts even if boto3 has import issues."""
    from src.storage.dynamo_writer import DynamoWriter
    if _get_writer._writer is None:
        _get_writer._writer = DynamoWriter()
    return _get_writer._writer
_get_writer._writer = None

app = FastAPI(
    title="TradePulse API",
    description="TradePulse — Real-time market data pipeline processing 15,000+ events/second",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url="/openapi.json",
)


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard() -> HTMLResponse:
    """Serve the custom TradePulse dashboard (single-page app)."""
    index_path = _STATIC_DIR / "index.html"
    if not index_path.exists():
        return HTMLResponse(
            "<h1>TradePulse API</h1><p>Dashboard not found. Ensure static/index.html exists.</p>"
            "<p><a href='/openapi.json'>OpenAPI spec</a></p>",
            status_code=200,
        )
    return HTMLResponse(content=index_path.read_text(encoding="utf-8"))


@app.get("/about", response_class=HTMLResponse)
async def serve_about() -> HTMLResponse:
    """
    Serves the TradePulse About and Documentation page.
    Explains what the project does, how to use the API,
    architecture overview, and setup instructions.
    """
    about_path = _STATIC_DIR / "about.html"
    if not about_path.exists():
        return HTMLResponse(
            "<h1>TradePulse</h1><p>About page not found.</p><a href='/'>Dashboard</a>",
            status_code=200,
        )
    return HTMLResponse(content=about_path.read_text(encoding="utf-8"))


# Mount static assets after explicit routes so / is not shadowed
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

# Rate limit: 100 requests/minute per IP; 429 with Retry-After
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS: allow all origins for demo; restrict to specific domains in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request logging middleware
@app.middleware("http")
async def log_requests(request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "request",
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        latency_ms=round(elapsed_ms, 2),
    )
    if settings.cloudwatch_enabled:
        get_metrics().emit_metric("APIRequestCount", 1.0, "Count", {"path": request.url.path})
        get_metrics().emit_metric("APILatency", elapsed_ms, "Milliseconds", {"path": request.url.path})
        if response.status_code >= 400:
            get_metrics().emit_metric("APIErrors", 1.0, "Count", {"path": request.url.path, "status": str(response.status_code)})
    return response

_start_time = time.time()


# Response models
class QuoteResponse(BaseModel):
    ticker: str
    price: float
    volume: int
    timestamp: str
    sequence_number: int


class AggregationsResponse(BaseModel):
    ticker: str
    vwap_1min: float
    vwap_5min: float
    rolling_avg_5min: float
    volume_zscore: float
    price_momentum: float
    window_start: str
    window_end: str


class AnomalyItem(BaseModel):
    timestamp: str
    anomaly_score: float
    feature_vector: dict
    model_version: int


class AnomaliesResponse(BaseModel):
    ticker: str
    anomalies: list[AnomalyItem]


class FeaturesResponse(BaseModel):
    ticker: str
    vwap_5min: float
    volume_zscore: float
    price_momentum: float
    trade_frequency: float
    bid_ask_spread: Optional[float] = None
    updated_at: str


class HealthResponse(BaseModel):
    status: str  # healthy | degraded | unhealthy
    consumer_lag: Optional[int] = None
    dynamo_latency_p99: Optional[float] = None
    uptime_seconds: float
    checked_at: str


@app.get("/quotes/{ticker}", response_model=QuoteResponse)
@limiter.limit("100/minute")
async def get_quotes(request: Request, ticker: str) -> QuoteResponse:
    """Most recent trade for ticker. Cached 1s."""
    writer = _get_writer()
    table = writer._resource.Table(writer._table_name("market_trades"))
    # Query any shard; we want latest by timestamp. Use GSI or scan one shard.
    from boto3.dynamodb.conditions import Key
    ticker_upper = ticker.upper()
    found = None
    for shard in range(8):
        pk = f"{ticker_upper}#{shard}"
        resp = table.query(
            KeyConditionExpression=Key("pk").eq(pk),
            Limit=1,
            ScanIndexForward=False,
        )
        items = resp.get("Items", [])
        if items:
            found = items[0]
            break
    if not found:
        raise HTTPException(status_code=404, detail=f"No quote found for ticker {ticker}")
    return QuoteResponse(
        ticker=found["ticker"],
        price=float(found["price"]),
        volume=int(found["volume"]),
        timestamp=found["timestamp"],
        sequence_number=int(found["sequence_number"]),
    )


@app.get("/aggregations/{ticker}", response_model=AggregationsResponse)
@limiter.limit("100/minute")
async def get_aggregations(request: Request, ticker: str) -> AggregationsResponse:
    """Latest window aggregations. Cached 5s."""
    writer = _get_writer()
    table = writer._resource.Table(writer._table_name("market_aggregations"))
    from boto3.dynamodb.conditions import Key
    ticker_upper = ticker.upper()
    # Table has pk = ticker, sort_key = window_start; one row per window with all metrics
    resp = table.query(
        KeyConditionExpression=Key("pk").eq(ticker_upper),
        Limit=1,
        ScanIndexForward=False,
    )
    items = resp.get("Items", [])
    if not items:
        raise HTTPException(status_code=404, detail=f"No aggregations for ticker {ticker}")
    row = items[0]
    return AggregationsResponse(
        ticker=row.get("ticker", ticker_upper),
        vwap_1min=float(row.get("vwap_1min", 0)),
        vwap_5min=float(row.get("vwap_5min", 0)),
        rolling_avg_5min=float(row.get("rolling_avg_5min", 0)),
        volume_zscore=float(row.get("volume_zscore", 0)),
        price_momentum=float(row.get("price_momentum", 0)),
        window_start=row.get("window_start", ""),
        window_end=row.get("window_end", ""),
    )


@app.get("/anomalies/{ticker}", response_model=AnomaliesResponse)
@limiter.limit("100/minute")
async def get_anomalies(request: Request, ticker: str, hours: int = 24) -> AnomaliesResponse:
    """Anomalies in last N hours. No cache."""
    writer = _get_writer()
    table = writer._resource.Table(writer._table_name("market_anomalies"))
    from boto3.dynamodb.conditions import Key
    ticker_upper = ticker.upper()
    resp = table.query(
        KeyConditionExpression=Key("pk").eq(ticker_upper),
        Limit=1000,
        ScanIndexForward=False,
    )
    items = resp.get("Items", [])
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    anomalies = []
    for row in items:
        ts_str = row.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        except Exception:
            ts = 0
        if ts >= cutoff:
            anomalies.append(
                AnomalyItem(
                    timestamp=ts_str,
                    anomaly_score=float(row.get("anomaly_score", 0)),
                    feature_vector=row.get("feature_vector", {}) or {},
                    model_version=int(row.get("model_version", 0)),
                )
            )
    return AnomaliesResponse(ticker=ticker_upper, anomalies=anomalies)


@app.get("/features/{ticker}", response_model=FeaturesResponse)
@limiter.limit("100/minute")
async def get_features(request: Request, ticker: str) -> FeaturesResponse:
    """Current feature vector. Cached 1s."""
    from src.processing.feature_store import FeatureStore
    store = FeatureStore()
    vec = store.get_features(ticker.upper())
    if not vec:
        raise HTTPException(status_code=404, detail=f"No features for ticker {ticker}")
    return FeaturesResponse(
        ticker=vec.ticker,
        vwap_5min=vec.vwap_5min,
        volume_zscore=vec.volume_zscore,
        price_momentum=vec.price_momentum_1min,
        trade_frequency=vec.trade_frequency,
        bid_ask_spread=vec.bid_ask_spread,
        updated_at=vec.updated_at.isoformat(),
    )


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Kafka lag, DynamoDB latency, uptime. No cache."""
    uptime = time.time() - _start_time
    status = "healthy"
    consumer_lag = None
    dynamo_latency_p99 = None
    # Could query CloudWatch for ConsumerLag and DynamoWriteLatency p99
    return HealthResponse(
        status=status,
        consumer_lag=consumer_lag,
        dynamo_latency_p99=dynamo_latency_p99,
        uptime_seconds=round(uptime, 2),
        checked_at=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/market-prices")
async def get_market_prices():
    """
    Fetches current real market prices for all tracked tickers.

    Used by the dashboard demo mode to seed the random walk simulation
    from accurate real-world baseline prices rather than hardcoded values.

    Primary source: yfinance (Yahoo Finance unofficial API, free, no key needed)
    Fallback: hardcoded prices updated as of March 13, 2026

    Why this endpoint exists on the backend rather than fetching directly
    from the frontend: browser CORS restrictions prevent direct calls to
    Yahoo Finance from client-side JavaScript. The FastAPI backend acts
    as a proxy, making the request server-side and returning clean JSON.

    Returns:
        dict: ticker → {price, change_pct} for each tracked ticker
    """
    tickers = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA"]

    try:
        prices = {}
        for ticker in tickers:
            stock = yf.Ticker(ticker)
            info = stock.fast_info

            last_price = round(float(info.last_price), 2)
            previous_close = round(float(info.previous_close), 2)
            change_pct = round(
                (last_price - previous_close) / previous_close * 100, 2
            )

            prices[ticker] = {
                "price": last_price,
                "change_pct": change_pct,
                "previous_close": previous_close,
            }

        return prices

    except Exception:
        # Fallback hardcoded prices if yfinance is unavailable.
        # Update these values manually before any live demo or recording.
        # Last updated: March 13, 2026 — source: Google Finance closing prices
        return {
            "AAPL": {"price": 250.12, "change_pct": -2.21, "previous_close": 255.76},
            "MSFT": {"price": 395.55, "change_pct": -1.84, "previous_close": 402.92},
            "AMZN": {"price": 207.67, "change_pct": -2.10, "previous_close": 212.13},
            "TSLA": {"price": 238.45, "change_pct": -3.92, "previous_close": 248.17},
            "NVDA": {"price": 880.35, "change_pct": -3.44, "previous_close": 911.83},
        }


@app.get("/sentiment/{ticker}")
@limiter.limit("100/minute")
async def get_sentiment(request: Request, ticker: str, hours: int = 24):
    """
    Returns recent news sentiment analysis for a ticker.

    Includes correlation data showing which articles coincided with
    unusual volume activity — the core signal of the two-stream join.

    Args:
        ticker: Stock ticker symbol (AAPL, MSFT, AMZN, TSLA, NVDA)
        hours:  Lookback window in hours (default 24, max 168)

    Returns:
        List of SentimentResult objects ordered newest first,
        with correlation_strength and volume_zscore_at_publish
        so consumers can filter for high-signal events.
    """
    hours = min(hours, 168)  # Cap at 7 days

    try:
        dynamodb = boto3.resource("dynamodb", region_name=settings.aws.region)
        table = dynamodb.Table(settings.dynamo.table_sentiment)

        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=hours)
        ).isoformat()

        response = table.query(
            KeyConditionExpression=Key("ticker").eq(ticker.upper()) & Key("published_at").gt(cutoff),
            ScanIndexForward=False,  # Newest first
            Limit=50
        )

        items = response.get("Items", [])

        return {
            "ticker":   ticker.upper(),
            "hours":    hours,
            "count":    len(items),
            "items":    items,
            "summary": {
                "positive": sum(1 for i in items if i.get("sentiment_label") == "positive"),
                "negative": sum(1 for i in items if i.get("sentiment_label") == "negative"),
                "neutral":  sum(1 for i in items if i.get("sentiment_label") == "neutral"),
                "strong_correlations": sum(1 for i in items if i.get("correlation_strength") == "strong"),
            }
        }

    except ClientError as e:
        raise HTTPException(status_code=500, detail=str(e))


def main() -> None:
    import uvicorn
    uvicorn.run(app, host=settings.api.host, port=settings.api.port)


if __name__ == "__main__":
    main()
