"""
Windowed aggregations for TradePulse.

VWAP (volume-weighted average price), rolling average price, volume z-score,
and price momentum. Tumbling windows (non-overlapping) for VWAP; hopping
windows (overlapping) for rolling metrics. Results written to DynamoDB
market_aggregations after each window closes.
"""

from collections import deque
from dataclasses import dataclass
from typing import Deque

# VWAP = Σ(Price × Volume) / Σ(Volume). We keep running totals per window.
# Institutional benchmark: fill below VWAP = good execution.
VWAP_1MIN_WINDOW_SEC = 60
VWAP_5MIN_WINDOW_SEC = 300

# Z-score: z = (x - μ) / σ. 20-period matches Bollinger Band convention.
VOLUME_ZSCORE_WINDOW = 20

# Momentum = (current - price_N_ago) / price_N_ago * 100 (percent over 1 min).
MOMENTUM_WINDOW_SEC = 60

# Rolling avg: 5-min hopping window, hop every 1 min (overlapping).
ROLLING_AVG_WINDOW_SEC = 300
ROLLING_AVG_HOP_SEC = 60


@dataclass
class AggregationResult:
    """Result of aggregations for one ticker/window."""

    ticker: str
    vwap_1min: float
    vwap_5min: float
    rolling_avg_5min: float
    volume_zscore: float
    price_momentum: float
    trade_frequency: float  # trades per minute
    window_start: str
    window_end: str


def calculate_vwap_1min(
    price_volume_sum: float,
    volume_sum: float,
) -> float:
    """
    VWAP = price_volume_sum / volume_sum for 1-minute tumbling window.
    Returns 0.0 if volume_sum is 0.
    """
    if volume_sum <= 0:
        return 0.0
    return price_volume_sum / volume_sum


def calculate_vwap_5min(
    price_volume_sum: float,
    volume_sum: float,
) -> float:
    """VWAP for 5-minute tumbling window."""
    if volume_sum <= 0:
        return 0.0
    return price_volume_sum / volume_sum


def volume_zscore(current_volume: float, mean: float, std: float) -> float:
    """
    Z-score = (x - μ) / σ. Returns 0.0 if std is 0.
    z > 2: unusually high volume; z > 3: extreme.
    """
    if std <= 0:
        return 0.0
    return (current_volume - mean) / std


def price_momentum(current_price: float, price_n_ago: float) -> float:
    """
    Percentage change over 1-minute window.
    (current - price_N_ago) / price_N_ago * 100.
    Returns 0.0 if price_n_ago is 0.
    """
    if price_n_ago <= 0:
        return 0.0
    return (current_price - price_n_ago) / price_n_ago * 100.0


class RollingAggregations:
    """
    Maintains per-ticker state: price_volume_sum, volume_sum for VWAP;
    deque of last N prices for rolling avg and momentum; volume history for z-score.
    Tumbling: window resets, no carryover. Hopping: window slides, overlapping.
    """

    def __init__(self) -> None:
        # 1min VWAP state (tumbling)
        self._pv_1: dict[str, float] = {}
        self._vol_1: dict[str, float] = {}
        # 5min VWAP state (tumbling)
        self._pv_5: dict[str, float] = {}
        self._vol_5: dict[str, float] = {}
        # Rolling 20 volumes for z-score
        self._volumes: dict[str, Deque[float]] = {}
        # Last N prices for rolling avg (5min window, 1min hop)
        self._prices_5min: dict[str, Deque[tuple[float, float]]] = {}  # (ts, price)
        # Price 60s ago for momentum
        self._price_60s_ago: dict[str, tuple[float, float]] = {}  # (ts, price)
        # Trade count in current minute
        self._trade_count_1min: dict[str, int] = {}

    def update_vwap_1min(self, ticker: str, price: float, volume: int) -> tuple[float, float]:
        """Update 1min running totals; return (price_volume_sum, volume_sum)."""
        pv = price * volume
        self._pv_1[ticker] = self._pv_1.get(ticker, 0) + pv
        self._vol_1[ticker] = self._vol_1.get(ticker, 0) + volume
        return self._pv_1[ticker], self._vol_1[ticker]

    def update_vwap_5min(self, ticker: str, price: float, volume: int) -> tuple[float, float]:
        """Update 5min running totals."""
        pv = price * volume
        self._pv_5[ticker] = self._pv_5.get(ticker, 0) + pv
        self._vol_5[ticker] = self._vol_5.get(ticker, 0) + volume
        return self._pv_5[ticker], self._vol_5[ticker]

    def get_vwap_1min(self, ticker: str) -> float:
        return calculate_vwap_1min(self._pv_1.get(ticker, 0), self._vol_1.get(ticker, 0))

    def get_vwap_5min(self, ticker: str) -> float:
        return calculate_vwap_5min(self._pv_5.get(ticker, 0), self._vol_5.get(ticker, 0))

    def reset_vwap_1min(self, ticker: str) -> None:
        self._pv_1[ticker] = 0
        self._vol_1[ticker] = 0

    def reset_vwap_5min(self, ticker: str) -> None:
        self._pv_5[ticker] = 0
        self._vol_5[ticker] = 0

    def update_volume_zscore(self, ticker: str, volume: float) -> float:
        """Append volume to 20-period deque; return z-score."""
        if ticker not in self._volumes:
            self._volumes[ticker] = deque(maxlen=VOLUME_ZSCORE_WINDOW)
        self._volumes[ticker].append(volume)
        arr = list(self._volumes[ticker])
        if len(arr) < 2:
            return 0.0
        mean = sum(arr) / len(arr)
        var = sum((x - mean) ** 2 for x in arr) / len(arr)
        std = var ** 0.5
        return volume_zscore(volume, mean, std)

    def update_price_momentum(self, ticker: str, ts: float, price: float) -> float:
        """Store (ts, price); return momentum vs price 60s ago if available."""
        if ticker not in self._price_60s_ago or ts - self._price_60s_ago[ticker][0] >= 60:
            self._price_60s_ago[ticker] = (ts, price)
            return 0.0
        _, price_60s = self._price_60s_ago[ticker]
        self._price_60s_ago[ticker] = (ts, price)
        return price_momentum(price, price_60s)

    def update_trade_frequency(self, ticker: str, window_start_ts: float, current_ts: float) -> float:
        """Trades per minute in current window."""
        self._trade_count_1min[ticker] = self._trade_count_1min.get(ticker, 0) + 1
        elapsed_min = max(1e-6, (current_ts - window_start_ts) / 60.0)
        return self._trade_count_1min[ticker] / elapsed_min

    def reset_trade_count_1min(self, ticker: str) -> None:
        self._trade_count_1min[ticker] = 0

    def update_rolling_avg_5min(self, ticker: str, ts: float, price: float) -> float:
        """Hopping 5min window: keep (ts, price) for last 300s; return average."""
        if ticker not in self._prices_5min:
            self._prices_5min[ticker] = deque()
        q = self._prices_5min[ticker]
        q.append((ts, price))
        cutoff = ts - ROLLING_AVG_WINDOW_SEC
        while q and q[0][0] < cutoff:
            q.popleft()
        if not q:
            return 0.0
        return sum(p for _, p in q) / len(q)
