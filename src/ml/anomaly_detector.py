"""
Per-ticker Isolation Forest anomaly detector with rolling buffer, async retrain thread,
and joblib persistence under ``/tmp``.

Feature order (7-D): price, volume, vwap_1min, volume_zscore, momentum,
bid_ask_spread, trade_frequency.

``predict()`` only runs inference (no fit); retrain runs in a background thread with a
lock held during ``fit``. Cold start: first 500 events return ``warming_up=True``.
"""

from __future__ import annotations

import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Mapping, Optional

import joblib
import numpy as np
import structlog
from sklearn.ensemble import IsolationForest

logger = structlog.get_logger(__name__)

FEATURE_ORDER: tuple[str, ...] = (
    "price",
    "volume",
    "vwap_1min",
    "volume_zscore",
    "momentum",
    "bid_ask_spread",
    "trade_frequency",
)
N_FEATURES = len(FEATURE_ORDER)

BUFFER_MAX = 5000
RETRAIN_EVERY_NEW_EVENTS = 1000
WARMUP_EVENTS = 500

# sklearn params (fixed per product spec)
IF_N_ESTIMATORS = 100
IF_CONTAMINATION = 0.05
IF_RANDOM_STATE = 42

# Spec default: one joblib file; multiple tickers are stored under ``tickers`` (read-modify-write).
_DEFAULT_SHARED_MODEL = Path(os.environ.get("ANOMALY_MODEL_PATH", "/tmp/anomaly_model.pkl"))
_MODEL_DIR = Path(os.environ.get("ANOMALY_MODEL_DIR", "/tmp"))
_USE_PER_TICKER_FILES = os.environ.get("ANOMALY_USE_PER_TICKER_MODEL_FILES", "").lower() in (
    "1",
    "true",
    "yes",
)

# Serialize concurrent writes to the shared pickle (multiple detector instances / threads).
_STORE_IO_LOCK = threading.Lock()


@dataclass
class AnomalyResult:
    ticker: str
    timestamp: datetime
    score: float
    is_anomaly: bool
    feature_vector: dict[str, float]
    warming_up: bool = False


def _model_path(ticker: str) -> Path:
    """
    Default ``/tmp/anomaly_model.pkl`` (spec) holds all tickers.

    Set ``ANOMALY_USE_PER_TICKER_MODEL_FILES=1`` for ``{ANOMALY_MODEL_DIR}/anomaly_model_{TICKER}.pkl``.
    """
    if _USE_PER_TICKER_FILES:
        safe = "".join(c if c.isalnum() else "_" for c in ticker.upper())[:16]
        return _MODEL_DIR / f"anomaly_model_{safe}.pkl"
    return _DEFAULT_SHARED_MODEL


def _normalize_store_blob(raw: object) -> dict:
    """Ensure ``{"schema": int, "tickers": {TICKER: block}}``; migrate legacy flat payloads."""
    if isinstance(raw, dict) and isinstance(raw.get("tickers"), dict):
        out = dict(raw)
        out.setdefault("schema", 1)
        return out
    if isinstance(raw, dict) and "model" in raw and raw.get("ticker"):
        t = str(raw["ticker"]).upper()
        block = {
            "model": raw.get("model"),
            "buffer_rows": raw.get("buffer_rows"),
            "total_events": int(raw.get("total_events", 0)),
            "version": int(raw.get("version", 0)),
        }
        return {"schema": 1, "tickers": {t: block}}
    return {"schema": 1, "tickers": {}}


def _restore_buffer_from_rows(rows: object) -> list[np.ndarray]:
    out: list[np.ndarray] = []
    if rows is None:
        return out
    arr = np.asarray(rows, dtype=np.float64)
    if arr.ndim != 2 or arr.shape[1] != N_FEATURES:
        return out
    for i in range(min(len(arr), BUFFER_MAX)):
        out.append(arr[i : i + 1].copy())
    return out


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def feature_dict_to_array(d: Mapping[str, float]) -> np.ndarray:
    return np.array([[float(d.get(k, 0.0)) for k in FEATURE_ORDER]], dtype=np.float64)


class IsolationForestAnomalyDetector:
    """
    Thread-safe detector: buffer updates and predict under a lock; ``fit`` runs in a
    daemon thread with the same lock held only around ``IsolationForest.fit``.
    """

    def __init__(self, ticker: str) -> None:
        self._ticker = ticker.upper()
        self._lock = threading.Lock()
        self._buffer: Deque[np.ndarray] = deque(maxlen=BUFFER_MAX)
        self._model: Optional[IsolationForest] = None
        self._total_events = 0
        self._events_since_train = 0
        self._retrain_thread: Optional[threading.Thread] = None
        self._version = 0
        self._path = _model_path(self._ticker)
        self._load_if_exists()

    def _load_if_exists(self) -> None:
        if not self._path.is_file():
            return
        try:
            with _STORE_IO_LOCK:
                raw = joblib.load(self._path)
            if _USE_PER_TICKER_FILES:
                payload = raw if isinstance(raw, dict) else {}
            else:
                blob = _normalize_store_blob(raw)
                payload = blob.get("tickers", {}).get(self._ticker, {})
            model = payload.get("model")
            rows = payload.get("buffer_rows")
            total = int(payload.get("total_events", 0))
            ver = int(payload.get("version", 0))
            if model is not None and isinstance(model, IsolationForest):
                buf_rows = _restore_buffer_from_rows(rows)
                with self._lock:
                    self._model = model
                    self._version = ver
                    self._total_events = total
                    self._buffer.clear()
                    for row in buf_rows:
                        self._buffer.append(row)
                logger.info(
                    "anomaly_model_loaded",
                    ticker=self._ticker,
                    path=str(self._path),
                    version=self._version,
                    buffer_len=len(self._buffer),
                )
        except Exception as exc:
            logger.warning(
                "anomaly_model_load_failed",
                ticker=self._ticker,
                path=str(self._path),
                error=str(exc),
            )

    def _persist_locked(self) -> None:
        """Call with ``self._lock`` held (writes shared store under ``_STORE_IO_LOCK``)."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            rows = (
                np.vstack(list(self._buffer))
                if self._buffer
                else np.zeros((0, N_FEATURES), dtype=np.float64)
            )
            block = {
                "model": self._model,
                "buffer_rows": rows.copy(),
                "total_events": self._total_events,
                "version": self._version,
            }
            if _USE_PER_TICKER_FILES:
                flat = {
                    **block,
                    "ticker": self._ticker,
                }
                with _STORE_IO_LOCK:
                    joblib.dump(flat, self._path)
            else:
                with _STORE_IO_LOCK:
                    blob: dict = {"schema": 1, "tickers": {}}
                    if self._path.is_file():
                        try:
                            blob = _normalize_store_blob(joblib.load(self._path))
                        except Exception:
                            blob = {"schema": 1, "tickers": {}}
                    tickers = blob.setdefault("tickers", {})
                    if not isinstance(tickers, dict):
                        tickers = {}
                        blob["tickers"] = tickers
                    tickers[self._ticker] = block
                    joblib.dump(blob, self._path)
        except Exception as exc:
            logger.warning(
                "anomaly_model_persist_failed",
                ticker=self._ticker,
                error=str(exc),
            )

    def _fit_job(self) -> None:
        """Runs in background thread; lock held only around ``IsolationForest.fit``."""
        with self._lock:
            if len(self._buffer) < WARMUP_EVENTS:
                return
            X = np.vstack(list(self._buffer)).copy()
        clf = IsolationForest(
            n_estimators=IF_N_ESTIMATORS,
            contamination=IF_CONTAMINATION,
            random_state=IF_RANDOM_STATE,
        )
        with self._lock:
            clf.fit(X)
        with self._lock:
            self._model = clf
            self._events_since_train = 0
            self._version += 1
            self._persist_locked()
        logger.info(
            "anomaly_model_retrained",
            ticker=self._ticker,
            version=self._version,
            samples=len(X),
        )

    def _maybe_start_retrain_thread(self) -> None:
        with self._lock:
            if self._events_since_train < RETRAIN_EVERY_NEW_EVENTS:
                return
            if len(self._buffer) < WARMUP_EVENTS:
                self._events_since_train = 0
                return
            if self._retrain_thread is not None and self._retrain_thread.is_alive():
                return
            self._events_since_train = 0
            t = threading.Thread(target=self._fit_job, name=f"if-fit-{self._ticker}", daemon=True)
            self._retrain_thread = t
            t.start()

    def update(self, feature_vector: Mapping[str, float]) -> None:
        """Append one event; may start background retrain (every 1000 new events)."""
        x = feature_dict_to_array(feature_vector)
        with self._lock:
            self._buffer.append(x.copy())
            self._total_events += 1
            self._events_since_train += 1
        self._maybe_start_retrain_thread()

    def predict(
        self,
        feature_vector: Mapping[str, float],
        timestamp: Optional[datetime] = None,
    ) -> AnomalyResult:
        """
        Inference only (no training). Target <1ms with pre-trained model.
        """
        ts = timestamp if timestamp is not None else _utcnow()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        fv = {k: float(feature_vector.get(k, 0.0)) for k in FEATURE_ORDER}

        with self._lock:
            n = self._total_events
            model = self._model

        if n < WARMUP_EVENTS:
            return AnomalyResult(
                ticker=self._ticker,
                timestamp=ts,
                score=0.0,
                is_anomaly=False,
                feature_vector=fv,
                warming_up=True,
            )

        if model is None:
            return AnomalyResult(
                ticker=self._ticker,
                timestamp=ts,
                score=0.0,
                is_anomaly=False,
                feature_vector=fv,
                warming_up=True,
            )

        x = np.array([[fv[k] for k in FEATURE_ORDER]], dtype=np.float64)
        t0 = time.perf_counter()
        score = float(model.decision_function(x)[0])
        label = int(model.predict(x)[0])
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if elapsed_ms > 1.0:
            logger.warning(
                "anomaly_predict_slow",
                ticker=self._ticker,
                elapsed_ms=round(elapsed_ms, 3),
            )
        is_anomaly = label == -1
        return AnomalyResult(
            ticker=self._ticker,
            timestamp=ts,
            score=score,
            is_anomaly=is_anomaly,
            feature_vector=fv,
            warming_up=False,
        )

    @property
    def model_version(self) -> int:
        return self._version


def detectors_for_tickers(tickers: list[str]) -> dict[str, IsolationForestAnomalyDetector]:
    return {t: IsolationForestAnomalyDetector(t) for t in tickers}
