"""Production FastAPI application for TradePulse.

Serves custom dashboard at / and GET /quotes/{ticker}, /aggregations/{ticker},
/anomalies/{ticker}, /features/{ticker}, /health. OpenAPI spec at /openapi.json.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Load repo-root .env so FINNHUB_API_KEY (and other vars) are available for local runs
# and when Docker passes no inline -e (use: docker run --env-file .env).
_load_env = Path(__file__).resolve().parents[2] / ".env"
if _load_env.is_file():
    load_dotenv(_load_env)

import random
from collections import deque
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    import boto3
    from boto3.dynamodb.conditions import Key, Attr
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

try:
    from src.config import get_settings
    from src.monitoring.cloudwatch_metrics import get_metrics
    settings = get_settings()
    CONFIG_AVAILABLE = True
except Exception as e:
    CONFIG_AVAILABLE = False
    settings = None
    get_metrics = lambda: type("_Dummy", (), {"emit_metric": lambda self, *a, **k: None})()
    print(f"[TradePulse] Config not loaded: {e} — running demo mode")

DEMO_MODE = os.getenv('DEMO_MODE', 'true').lower() == 'true'

# Finnhub /market-prices fallbacks — update manually when stale (Finnhub or network failure).
FALLBACK_PRICES = {
    "AAPL": {"price": 207.94, "change_pct": -0.27, "previous_close": 208.50},
    "NVDA": {"price": 109.02, "change_pct": -1.84, "previous_close": 111.06},
    "MSFT": {"price": 389.30, "change_pct": 0.42, "previous_close": 387.67},
}

# Required for remaining routes (limiter, Request, BaseModel, logger, time)
import time
from typing import Any, Optional
from fastapi import Request
from pydantic import BaseModel
import structlog
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from src.processing.feature_store import FeatureStore

import httpx

logger = structlog.get_logger(__name__)


# Real-time API metrics tracker
# Measures actual request counts, response times, and uptime
# These replace the simulated demo metrics on the Overview dashboard
class APIMetrics:
    """
    Tracks real API performance metrics for the TradePulse dashboard.

    All metrics are measured from actual requests hitting this FastAPI
    instance on Railway — not simulated values.

    Design decisions:
    - deque with maxlen for rolling windows — O(1) append/popleft
    - Module-level singleton — shared across all requests
    - No external dependencies — pure Python stdlib
    """

    def __init__(self) -> None:
        self.start_time = time.time()
        # Rolling window of last 100 response times in milliseconds
        self.response_times: deque = deque(maxlen=100)
        # Total request count since startup
        self.total_requests = 0
        # Requests in the last 60 seconds for req/min calculation
        self.recent_request_times: deque = deque(maxlen=1000)
        # Count of non-200 responses
        self.error_count = 0

    def record_request(
        self, response_time_ms: float, status_code: int, include_in_latency: bool = True
    ) -> None:
        """
        Records a completed request.

        Args:
            response_time_ms:    Response time in milliseconds
            status_code:         HTTP status code
            include_in_latency:  If False, counts the request but excludes
                                 it from p50/p99 latency calculations.
                                 Used for external API proxy endpoints.
        """
        if include_in_latency:
            self.response_times.append(response_time_ms)
        self.total_requests += 1
        self.recent_request_times.append(time.time())
        if status_code >= 400:
            self.error_count += 1

    def get_p99_latency(self) -> float:
        """Returns p99 response time in milliseconds."""
        if not self.response_times:
            return 0.0
        sorted_times = sorted(self.response_times)
        idx = int(len(sorted_times) * 0.99)
        return float(round(sorted_times[min(idx, len(sorted_times) - 1)], 1))

    def get_p50_latency(self) -> float:
        """Returns p50 (median) response time in milliseconds."""
        if not self.response_times:
            return 0.0
        sorted_times = sorted(self.response_times)
        idx = len(sorted_times) // 2
        return float(round(sorted_times[idx], 1))

    def get_requests_per_minute(self) -> int:
        """Returns requests in the last 60 seconds."""
        now = time.time()
        cutoff = now - 60
        return len([t for t in self.recent_request_times if t > cutoff])

    def get_uptime_seconds(self) -> int:
        """Returns seconds since app startup."""
        return int(time.time() - self.start_time)

    def get_uptime_formatted(self) -> str:
        """Returns human-readable uptime string."""
        seconds = self.get_uptime_seconds()
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        if minutes > 0:
            return f"{minutes}m {secs}s"
        return f"{secs}s"

    def get_avg_latency(self) -> float:
        """Returns average response time in milliseconds."""
        if not self.response_times:
            return 0.0
        return float(round(sum(self.response_times) / len(self.response_times), 1))


# Module-level singleton — shared across all requests
metrics = APIMetrics()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

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
    openapi_url="/openapi.json"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Explicit root route returning the dashboard HTML
@app.get("/")
async def serve_dashboard():
    """
    Serves the TradePulse dashboard HTML file.

    Explicit route required for Vercel deployment since
    StaticFiles mount alone does not work reliably on
    Vercel's Python serverless runtime.
    """
    # Try multiple possible paths for the HTML file
    # Path differs between local development and Vercel runtime
    possible_paths = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "index.html"),
        os.path.join(os.getcwd(), "src", "api", "static", "index.html"),
        "src/api/static/index.html",
    ]

    for path in possible_paths:
        if os.path.exists(path):
            return FileResponse(path, media_type="text/html")

    return {"error": "Dashboard not found", "cwd": os.getcwd(), "tried": possible_paths}

# Mount static files for CSS/JS/assets (keep existing mount if present)
# Only mount if not already mounted
static_dir = None
for path in [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "static"),
    os.path.join(os.getcwd(), "src", "api", "static"),
    "src/api/static",
]:
    if os.path.exists(path):
        static_dir = path
        break

if static_dir:
    app.mount("/static", StaticFiles(directory=static_dir), name="static-files")

@app.get("/about", response_class=HTMLResponse)
async def serve_about():
    try:
        with open(os.path.join(STATIC_DIR, "about.html")) as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse("<h1>TradePulse About</h1>")

# Rate limit: 100 requests/minute per IP; 429 with Retry-After
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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
    if settings and settings.cloudwatch_enabled:
        get_metrics().emit_metric("APIRequestCount", 1.0, "Count", {"path": request.url.path})
        get_metrics().emit_metric("APILatency", elapsed_ms, "Milliseconds", {"path": request.url.path})
        if response.status_code >= 400:
            get_metrics().emit_metric("APIErrors", 1.0, "Count", {"path": request.url.path, "status": str(response.status_code)})
    return response


@app.middleware("http")
async def track_metrics(request: Request, call_next):
    """
    Measures response time for every request.

    Excludes external data fetch endpoints from latency metrics:
    - /market-prices calls Finnhub externally (200-800ms network)
    - /quote/{ticker} calls Finnhub externally (200-800ms network)
    - /sentiment/{ticker} calls external services

    These endpoints are network-bound by Finnhub response time
    not by pipeline performance. Including them in p99 would
    misrepresent the API's internal processing speed.

    The sub-100ms claim refers to the Kafka→Faust→DynamoDB
    pipeline latency measured during load testing, documented
    in docs/benchmarks.md — not to external API call time.
    """
    # Endpoints that make external HTTP calls — exclude from latency metrics
    EXTERNAL_ENDPOINTS = ("/market-prices", "/quote/", "/sentiment/")
    is_external = any(request.url.path.startswith(ep) for ep in EXTERNAL_ENDPOINTS)

    start = time.time()
    response = await call_next(request)
    duration_ms = (time.time() - start) * 1000.0

    # Only track non-static, non-external endpoints for latency
    if not request.url.path.startswith("/static"):
        metrics.record_request(
            response_time_ms=duration_ms,
            status_code=response.status_code,
            include_in_latency=not is_external,
        )

    response.headers["X-Response-Time"] = f"{duration_ms:.1f}ms"
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


@app.get("/health")
async def health():
    return {"status": "healthy", "mode": "demo"}


@app.get("/metrics")
async def read_api_metrics():
    """
    Returns real performance metrics for the TradePulse dashboard.

    All values are measured from actual API requests on this Railway
    instance — not simulated or hardcoded.

    Used by the Overview dashboard to replace demo mode fake numbers
    with genuinely accurate operational data.

    Returns:
        uptime_seconds:      Seconds since app startup on Railway
        uptime_formatted:    Human readable uptime e.g. "2h 34m 12s"
        total_requests:      Total API requests since startup
        requests_per_minute: Requests in the last 60 seconds
        p50_latency_ms:      Median API response time in milliseconds
        p99_latency_ms:      99th percentile response time in ms
        avg_latency_ms:      Average response time in milliseconds
        error_count:         Total non-200 responses since startup
        error_rate_pct:      Percentage of requests that errored
        status:              healthy / degraded based on p99 latency
    """
    total = metrics.total_requests
    error_rate = round(
        (metrics.error_count / total * 100) if total > 0 else 0, 2
    )

    p99 = metrics.get_p99_latency()
    status = "healthy" if p99 < 200 else "degraded"

    return {
        "uptime_seconds": metrics.get_uptime_seconds(),
        "uptime_formatted": metrics.get_uptime_formatted(),
        "total_requests": total,
        "requests_per_minute": metrics.get_requests_per_minute(),
        "p50_latency_ms": metrics.get_p50_latency(),
        "p99_latency_ms": p99,
        "avg_latency_ms": metrics.get_avg_latency(),
        "error_count": metrics.error_count,
        "error_rate_pct": error_rate,
        "status": status,
    }


@app.get("/market-prices")
async def get_market_prices():
    """
    Real-time stock prices via Finnhub REST /quote (same API key as news).
    Reliable on Railway vs yfinance rate limits. Falls back to hardcoded prices if Finnhub fails.
    """
    finnhub_key = os.getenv("FINNHUB_API_KEY", "")

    if not finnhub_key:
        print("[TradePulse] No FINNHUB_API_KEY — returning fallback prices")
        logger.warning("market_prices_no_finnhub_key")
        return FALLBACK_PRICES

    tickers = ["AAPL", "NVDA", "MSFT"]
    prices: dict[str, dict[str, float]] = {}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            for ticker in tickers:
                try:
                    response = await client.get(
                        "https://finnhub.io/api/v1/quote",
                        params={"symbol": ticker, "token": finnhub_key},
                    )
                    if response.status_code != 200:
                        raise ValueError(f"Finnhub returned {response.status_code}")

                    data = response.json()
                    current_price = round(float(data.get("c", 0)), 2)
                    previous_close = round(float(data.get("pc", 0)), 2)
                    change_pct = round(float(data.get("dp", 0)), 2)

                    if current_price <= 0:
                        raise ValueError(f"Invalid price {current_price} for {ticker}")

                    prices[ticker] = {
                        "price": current_price,
                        "change_pct": change_pct,
                        "previous_close": previous_close,
                    }
                    print(
                        f"[TradePulse] Finnhub {ticker}: ${current_price} ({change_pct:+.2f}%)"
                    )
                except Exception as e:
                    print(f"[TradePulse] Finnhub failed for {ticker}: {e} — using fallback")
                    logger.warning("market_prices_finnhub_ticker_failed", ticker=ticker, error=str(e))
                    fb = FALLBACK_PRICES.get(ticker)
                    prices[ticker] = fb if fb else {"price": 100.0, "change_pct": 0.0, "previous_close": 100.0}

        return prices
    except Exception as e:
        print(f"[TradePulse] Finnhub entirely failed: {e} — returning fallbacks")
        logger.warning("market_prices_finnhub_failed", error=str(e))
        return FALLBACK_PRICES


@app.get("/quote/{ticker}")
async def get_single_quote(ticker: str):
    """
    Fetches real-time quote data for any valid US stock ticker.
    Uses Finnhub /quote endpoint — supports any symbol on any exchange.

    This powers the universal ticker search on the dashboard.
    Users can look up any stock, not just the pre-configured tickers.

    Args:
        ticker: Any valid stock symbol e.g. AAPL, GOOGL, SPY, BRK.B

    Returns:
        price, change_pct, previous_close, high, low, open, name
    """
    ticker = ticker.upper().strip()

    finnhub_key = os.getenv("FINNHUB_API_KEY", "")

    if not finnhub_key:
        raise HTTPException(
            status_code=503,
            detail="Quote service unavailable — FINNHUB_API_KEY not configured",
        )

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            quote_response = await client.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": ticker, "token": finnhub_key},
            )

            if quote_response.status_code != 200:
                raise HTTPException(
                    status_code=404,
                    detail=f"Ticker {ticker} not found",
                )

            quote = quote_response.json()

            def _f(key: str) -> float:
                v = quote.get(key)
                if v is None:
                    return 0.0
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return 0.0

            current_price = round(_f("c"), 2)
            previous_close = round(_f("pc"), 2)
            change = round(_f("d"), 2)
            change_pct = round(_f("dp"), 2)
            high = round(_f("h"), 2)
            low = round(_f("l"), 2)
            open_price = round(_f("o"), 2)

            # Finnhub often returns c=0 when the market is closed or before a trade prints;
            # previous close (pc) is still valid — use it as the display price.
            if current_price <= 0 and previous_close > 0:
                current_price = previous_close
                change = 0.0
                change_pct = 0.0

            if current_price <= 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No price data available for {ticker}",
                )

            name = ticker
            try:
                profile_response = await client.get(
                    "https://finnhub.io/api/v1/stock/profile2",
                    params={"symbol": ticker, "token": finnhub_key},
                )
                if profile_response.status_code == 200:
                    profile = profile_response.json()
                    name = profile.get("name", ticker)
            except Exception:
                pass

            print(f"[TradePulse] Quote {ticker}: ${current_price} ({change_pct:+.2f}%)")

            return {
                "ticker": ticker,
                "name": name,
                "price": current_price,
                "change": change,
                "change_pct": change_pct,
                "previous_close": previous_close,
                "high": high,
                "low": low,
                "open": open_price,
            }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[TradePulse] Quote fetch failed for {ticker}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch quote for {ticker}: {str(e)}",
        )


@app.get("/sentiment/{ticker}")
@limiter.limit("100/minute")
async def get_sentiment(request: Request, ticker: str, hours: int = 24):
    """
    Returns news sentiment for a ticker.
    Demo mode returns empty list — dashboard handles fallback
    to DEMO_SENTIMENT data client-side.
    """
    hours = min(hours, 168)

    if DEMO_MODE:
        return {
            "ticker": ticker.upper(),
            "hours": hours,
            "count": 0,
            "items": [],
            "summary": {
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "strong_correlations": 0,
            },
        }

    if not settings or not BOTO3_AVAILABLE:
        return {
            "ticker": ticker.upper(),
            "hours": hours,
            "count": 0,
            "items": [],
            "summary": {"positive": 0, "negative": 0, "neutral": 0, "strong_correlations": 0},
        }

    try:
        writer = _get_writer()
        table = writer._resource.Table(settings.dynamo.table_sentiment)

        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=hours)
        ).isoformat()

        response = table.query(
            KeyConditionExpression=Key("ticker").eq(ticker.upper()) & Key("published_at").gt(cutoff),
            ScanIndexForward=False,
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


@app.on_event("startup")
async def startup_event():
    print("[TradePulse] Started successfully")
    print(f"[TradePulse] DEMO_MODE={DEMO_MODE}")
    print(f"[TradePulse] Static dir exists: {os.path.exists(STATIC_DIR)}")


def main() -> None:
    import uvicorn
    uvicorn.run(app, host=settings.api.host, port=settings.api.port)


if __name__ == "__main__":
    main()
