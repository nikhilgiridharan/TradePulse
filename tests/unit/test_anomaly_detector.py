"""IsolationForestAnomalyDetector: cold start, retrain, inference timing, threads."""

from __future__ import annotations

import os
import platform
import sys
import threading
import time

import pytest

# numpy/sklearn can segfault on older macOS + system Python during import.
if platform.system() == "Darwin" and sys.version_info < (3, 11):
    pytest.skip(
        "Skip sklearn tests on Darwin Python <3.11 (use CI or Docker / Python 3.11+)",
        allow_module_level=True,
    )

import numpy as np

from src.ml.anomaly_detector import (
    FEATURE_ORDER,
    IF_N_ESTIMATORS,
    IsolationForestAnomalyDetector,
)


def _rand_feat(rng: np.random.Generator) -> dict[str, float]:
    vals = rng.normal(size=len(FEATURE_ORDER))
    return {k: float(v) for k, v in zip(FEATURE_ORDER, vals)}


def test_cold_start_first_499_events_warming(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """While ``_total_events`` < 500, ``predict`` stays in warming mode."""
    monkeypatch.setenv("ANOMALY_USE_PER_TICKER_MODEL_FILES", "1")
    monkeypatch.setenv("ANOMALY_MODEL_DIR", str(tmp_path))
    det = IsolationForestAnomalyDetector("AAPL")
    rng = np.random.default_rng(42)
    checkpoints = {0, 100, 248, 498}
    for i in range(499):
        det.update(_rand_feat(rng))
        if i in checkpoints:
            r = det.predict(_rand_feat(rng))
            assert r.warming_up is True, f"after event {i + 1}"
            assert r.is_anomaly is False
            assert r.score == 0.0


def test_retrain_after_1000_events_has_full_estimator_forest(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("ANOMALY_USE_PER_TICKER_MODEL_FILES", "1")
    monkeypatch.setenv("ANOMALY_MODEL_DIR", str(tmp_path))
    det = IsolationForestAnomalyDetector("AAPL")
    rng = np.random.default_rng(7)
    for _ in range(1000):
        det.update(_rand_feat(rng))

    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        m = det._model  # noqa: SLF001 — test introspection
        if m is not None and hasattr(m, "estimators_"):
            est = m.estimators_
            if hasattr(est, "__len__") and len(est) == IF_N_ESTIMATORS:
                break
        time.sleep(0.05)
    else:
        pytest.fail("Model did not finish training with 100 estimators in time")

    assert det._model is not None  # noqa: SLF001
    assert len(det._model.estimators_) == IF_N_ESTIMATORS  # noqa: SLF001


@pytest.mark.skipif(os.environ.get("CI") == "true", reason="CI runners may exceed 5ms sklearn inference")
def test_predict_inference_under_5ms(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("ANOMALY_USE_PER_TICKER_MODEL_FILES", "1")
    monkeypatch.setenv("ANOMALY_MODEL_DIR", str(tmp_path))
    det = IsolationForestAnomalyDetector("ZZZ")
    rng = np.random.default_rng(99)
    for _ in range(1200):
        det.update(_rand_feat(rng))
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline and det._model is None:  # noqa: SLF001
        time.sleep(0.05)
    assert det._model is not None  # noqa: SLF001

    feat = _rand_feat(rng)
    # Warm JIT / allocator
    det.predict(feat)

    t0 = time.perf_counter()
    det.predict(feat)
    elapsed = time.perf_counter() - t0
    assert elapsed < 0.005, f"predict took {elapsed * 1000:.3f}ms (expected <5ms)"


def test_concurrent_predict_no_exceptions(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("ANOMALY_USE_PER_TICKER_MODEL_FILES", "1")
    monkeypatch.setenv("ANOMALY_MODEL_DIR", str(tmp_path))
    det = IsolationForestAnomalyDetector("THRD")
    rng = np.random.default_rng(3)
    for _ in range(1200):
        det.update(_rand_feat(rng))
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline and det._model is None:  # noqa: SLF001
        time.sleep(0.05)
    assert det._model is not None  # noqa: SLF001

    errors: list[BaseException] = []
    barrier = threading.Barrier(10)

    def worker() -> None:
        try:
            barrier.wait()
            for _ in range(20):
                det.predict(_rand_feat(rng))
        except BaseException as exc:  # noqa: BLE001 — collect any failure
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60.0)
        assert not t.is_alive()
    assert not errors, f"thread errors: {errors}"


def test_shared_pickle_roundtrip_two_tickers(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "anomaly_model.pkl"
    monkeypatch.setenv("ANOMALY_MODEL_PATH", str(p))
    monkeypatch.delenv("ANOMALY_USE_PER_TICKER_MODEL_FILES", raising=False)

    d1 = IsolationForestAnomalyDetector("AAPL")
    d2 = IsolationForestAnomalyDetector("MSFT")
    rng = np.random.default_rng(1)
    for _ in range(1200):
        d1.update(_rand_feat(rng))
        d2.update(_rand_feat(rng))
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        if d1.model_version >= 1 and d2.model_version >= 1:
            break
        time.sleep(0.05)
    assert p.is_file()

    d1b = IsolationForestAnomalyDetector("AAPL")
    d2b = IsolationForestAnomalyDetector("MSFT")
    assert d1b.model_version >= 1
    assert d2b.model_version >= 1
