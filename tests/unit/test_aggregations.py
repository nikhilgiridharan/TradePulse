"""Unit tests for windowed aggregations."""

import pytest

from src.processing.aggregations import (
    calculate_vwap_1min,
    calculate_vwap_5min,
    volume_zscore,
    price_momentum,
    RollingAggregations,
)


def test_vwap_calculation_with_known_inputs():
    # VWAP of two trades: (100*10 + 200*20) / (10+20) = 5000/30 = 166.67
    pv = 100 * 10 + 200 * 20
    vol = 10 + 20
    vwap = calculate_vwap_1min(pv, vol)
    assert abs(vwap - 5000 / 30) < 1e-6


def test_vwap_updates_correctly_on_new_trade():
    # Running totals update without full recalculation
    roll = RollingAggregations()
    roll.update_vwap_1min("AAPL", 100.0, 10)
    roll.update_vwap_1min("AAPL", 200.0, 20)
    vwap = roll.get_vwap_1min("AAPL")
    assert abs(vwap - 5000 / 30) < 1e-6


def test_volume_zscore_returns_zero_for_mean_volume():
    # Z-score of the mean should be 0.0
    mean = 100.0
    std = 10.0
    z = volume_zscore(100.0, mean, std)
    assert abs(z) < 1e-6


def test_volume_zscore_returns_positive_for_high_volume():
    # Volume 2 std devs above mean should return z ≈ 2.0
    mean = 100.0
    std = 10.0
    z = volume_zscore(120.0, mean, std)
    assert abs(z - 2.0) < 1e-6


def test_price_momentum_positive_on_price_increase():
    # Price from 100 to 105 over 60s = +5% momentum
    m = price_momentum(105.0, 100.0)
    assert abs(m - 5.0) < 1e-6


def test_price_momentum_negative_on_price_decrease():
    m = price_momentum(95.0, 100.0)
    assert abs(m - (-5.0)) < 1e-6


def test_vwap_window_resets_after_tumbling_window_closes():
    # Tumbling window state resets — no carryover between windows
    roll = RollingAggregations()
    roll.update_vwap_1min("AAPL", 100.0, 10)
    roll.reset_vwap_1min("AAPL")
    vwap = roll.get_vwap_1min("AAPL")
    assert vwap == 0.0
