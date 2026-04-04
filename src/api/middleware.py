"""Request telemetry: structured logging and pipeline RPS sampling."""

from __future__ import annotations

import time
from collections import deque

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = structlog.get_logger(__name__)

_DATA_PREFIXES = ("/quotes/", "/aggregations/", "/anomalies/", "/features/", "/sentiment/")


def _extract_ticker(path: str) -> str | None:
    for pref in _DATA_PREFIXES:
        if path.startswith(pref):
            rest = path[len(pref) :].split("/")[0]
            return rest.upper() if rest else None
    return None


class RequestTelemetryMiddleware(BaseHTTPMiddleware):
    """Log latency, optional ticker, cache flag; sample data-plane traffic for health RPS."""

    async def dispatch(self, request: Request, call_next) -> Response:
        t0 = time.perf_counter()
        request.state.cache_hit = getattr(request.state, "cache_hit", False)
        response = await call_next(request)
        latency_ms = (time.perf_counter() - t0) * 1000.0
        path = request.url.path
        ticker = _extract_ticker(path)
        logger.info(
            "api_request",
            path=path,
            method=request.method,
            ticker=ticker,
            latency_ms=round(latency_ms, 3),
            cache_hit=bool(getattr(request.state, "cache_hit", False)),
            status_code=response.status_code,
        )
        if response.status_code < 500:
            dq = getattr(request.app.state, "pipeline_event_times", None)
            if dq is not None and any(path.startswith(p) for p in _DATA_PREFIXES):
                dq.append(time.monotonic())
        return response


def new_pipeline_event_deque(maxlen: int = 10_000) -> deque[float]:
    return deque(maxlen=maxlen)
