"""
Pure rolling-window metrics for trade processing (VWAP, volume z-score, momentum).

Kept free of Faust / ML imports so unit tests can import without pulling sklearn.
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Optional


def prune_rows(rows: list[list[Any]], cutoff_sec: float) -> list[list[Any]]:
    return [r for r in rows if r[0] >= cutoff_sec]


def vwap_from_rows(rows: list[list[Any]]) -> float:
    if not rows:
        return 0.0
    sum_pv = sum(float(r[1]) * int(r[2]) for r in rows)
    sum_v = sum(int(r[2]) for r in rows)
    return sum_pv / sum_v if sum_v > 0 else 0.0


def rolling_avg_price(rows: list[list[Any]]) -> float:
    if not rows:
        return 0.0
    return statistics.mean(float(r[1]) for r in rows)


def price_momentum_percent(current: float, last_px: Optional[float]) -> float:
    """Percent change from prior price (trade tick momentum)."""
    if last_px is None or last_px <= 0:
        return 0.0
    return (current - last_px) / last_px * 100.0


def volume_zscore_20(vol_ring: list[int]) -> float:
    if len(vol_ring) < 2:
        return 0.0
    current = float(vol_ring[-1])
    prev = [float(x) for x in vol_ring[:-1]]
    if not prev:
        return 0.0
    mean = statistics.mean(prev)
    if len(prev) == 1:
        return 0.0 if current == mean else (1.0 if current > mean else -1.0)
    try:
        std = statistics.stdev(prev)
    except statistics.StatisticsError:
        std = 0.0
    if std <= 1e-9:
        return 0.0 if math.isclose(current, mean) else (1.0 if current > mean else -1.0)
    return (current - mean) / std
