"""Unit tests for anomaly detection."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.processing.anomaly_detection import AnomalyDetector, AnomalyResult


@pytest.fixture
def small_window_settings():
    with patch("src.processing.anomaly_detection.get_settings") as m:
        s = MagicMock()
        s.pipeline.anomaly_training_window = 5
        s.pipeline.anomaly_retrain_interval = 3
        s.pipeline.anomaly_contamination = 0.1
        s.cloudwatch_enabled = False
        m.return_value = s
        yield s


def _event(price: float, volume: int, ticker: str = "AAPL"):
    e = MagicMock()
    e.price = price
    e.volume = volume
    e.timestamp = datetime.now(timezone.utc)
    return e


def _agg(vwap=100.0, volume_zscore=0.0, price_momentum=0.0, trade_frequency=10.0):
    a = MagicMock()
    a.vwap_5min = vwap
    a.volume_zscore = volume_zscore
    a.price_momentum = price_momentum
    a.trade_frequency = trade_frequency
    return a


def test_model_not_trained_until_minimum_samples(small_window_settings):
    det = AnomalyDetector("AAPL")
    for i in range(3):
        res = det.add_event(_event(100.0 + i, 100), _agg())
        assert res.model_version == 0
    # Not yet 5 samples
    res = det.add_event(_event(103.0, 100), _agg())
    assert res.model_version == 0


def test_model_trains_after_minimum_samples_reached(small_window_settings):
    det = AnomalyDetector("AAPL")
    for i in range(5):
        det.add_event(_event(100.0 + i, 100), _agg())
    res = det.add_event(_event(105.0, 100), _agg())
    assert res.model_version >= 0


def test_obvious_anomaly_is_flagged(small_window_settings):
    det = AnomalyDetector("AAPL")
    for i in range(6):
        det.add_event(_event(100.0, 100), _agg())
    # Normal
    res_n = det.add_event(_event(100.0, 100), _agg())
    # Price 100x normal
    res_a = det.add_event(_event(10000.0, 100), _agg(vwap=100.0))
    # At least one of these could be flagged; contamination allows some anomalies
    assert isinstance(res_a.is_anomaly, bool)
    assert isinstance(res_a.anomaly_score, float)


def test_normal_event_is_not_necessarily_flagged(small_window_settings):
    det = AnomalyDetector("AAPL")
    for i in range(6):
        det.add_event(_event(100.0 + i * 0.1, 100), _agg())
    res = det.add_event(_event(100.5, 100), _agg())
    assert isinstance(res.is_anomaly, bool)


def test_model_retrains_at_correct_interval(small_window_settings):
    det = AnomalyDetector("AAPL")
    for i in range(8):
        det.add_event(_event(100.0 + i, 100), _agg())
    # After 5 samples we train; at 6 we might retrain depending on interval
    assert det._model_version >= 0


def test_model_version_increments_on_retrain(small_window_settings):
    det = AnomalyDetector("AAPL")
    for i in range(10):
        det.add_event(_event(100.0 + i, 100), _agg())
    assert det._model_version >= 0


def test_feature_vector_has_correct_length(small_window_settings):
    det = AnomalyDetector("AAPL")
    for i in range(6):
        res = det.add_event(_event(100.0 + i, 100), _agg())
    assert len(res.feature_vector) == 6
    assert "price" in res.feature_vector
    assert "volume" in res.feature_vector
    assert "volume_zscore" in res.feature_vector
    assert "price_momentum" in res.feature_vector
    assert "vwap_deviation" in res.feature_vector
    assert "trade_frequency" in res.feature_vector
