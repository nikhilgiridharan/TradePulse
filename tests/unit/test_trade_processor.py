"""Unit tests for pure metrics helpers used by the Faust trade processor."""

from __future__ import annotations

import statistics
from datetime import datetime, timezone

import pytest

from src.processing.metrics import (
    price_momentum_percent,
    prune_rows,
    volume_zscore_20,
    vwap_from_rows,
)
from src.schemas.trade import TradeEvent


def test_vwap_1min_ten_trades_price_volume_weighted() -> None:
    """VWAP over rows [ts, price, size] equals sum(p*v)/sum(v)."""
    base_ts = 1_700_000_000.0
    rows: list[list[float | int]] = [
        [base_ts + i, 100.0 + float(i), 10 + i] for i in range(10)
    ]
    last_ts = base_ts + 9.0
    pruned = prune_rows(rows, last_ts - 60.0)
    assert len(pruned) == 10

    vwap = vwap_from_rows(pruned)
    sum_pv = sum(float(r[1]) * int(r[2]) for r in rows)
    sum_v = sum(int(r[2]) for r in rows)
    expected = sum_pv / sum_v
    assert abs(vwap - expected) < 1e-9


def test_volume_zscore_matches_statistics_on_prior_window() -> None:
    """
    _volume_zscore_20 uses current = last element; mean/stdev over prior elements only.
    """
    prev = list(range(10, 29))  # 19 values
    current = 38
    vol_ring = prev + [current]
    prev_f = [float(x) for x in prev]
    mean = statistics.mean(prev_f)
    std = statistics.stdev(prev_f)
    expected = (float(current) - mean) / std
    got = volume_zscore_20(vol_ring)
    assert abs(got - expected) < 1e-9


def test_volume_zscore_short_ring_returns_zero() -> None:
    assert volume_zscore_20([100]) == 0.0
    assert volume_zscore_20([]) == 0.0


def test_momentum_percent_formula() -> None:
    assert price_momentum_percent(110.0, 100.0) == pytest.approx(10.0)
    assert price_momentum_percent(50.0, 100.0) == pytest.approx(-50.0)
    assert price_momentum_percent(100.0, None) == 0.0
    assert price_momentum_percent(100.0, 0.0) == 0.0


def test_trade_event_schema_used_by_processor() -> None:
    """Sanity: synthetic TradeEvent matches fields the processor reads."""
    te = TradeEvent(
        ticker="AAPL",
        price=150.0,
        size=100,
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert te.ticker == "AAPL"
    assert te.price == 150.0
    assert te.size == 100
