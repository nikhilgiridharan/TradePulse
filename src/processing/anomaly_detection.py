"""
Real-time anomaly detection using Isolation Forest.

Per-ticker models with rolling training window; contamination=0.01 (expect ~1%
anomalies). Features: price, volume, volume_zscore, price_momentum, vwap_deviation,
trade_frequency. Standardized before training. Anomalies written to DynamoDB
and S3.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import numpy as np
import structlog
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from src.config import get_settings
from src.monitoring.cloudwatch_metrics import get_metrics

logger = structlog.get_logger(__name__)


@dataclass
class AnomalyResult:
    """Result of anomaly scoring for one event."""

    ticker: str
    timestamp: datetime
    is_anomaly: bool
    anomaly_score: float  # More negative = more anomalous in Isolation Forest
    feature_vector: dict
    model_version: int


class AnomalyDetector:
    """
    Per-ticker Isolation Forest. Rolling training window; retrain every
    ANOMALY_RETRAIN_INTERVAL events. Contamination=0.01 for market microstructure.
    Features standardized so high-magnitude (e.g. volume) doesn't dominate.
    """

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self._settings = get_settings()
        self._metrics = get_metrics()
        self._model: IsolationForest | None = None
        self._scaler = StandardScaler()
        self._training_buffer: list[np.ndarray] = []
        self._model_version = 0

    def _extract_features(self, event: Any, aggregations: Any) -> np.ndarray:
        """Build feature vector: price, volume, volume_zscore, momentum, vwap_dev, trade_freq."""
        price = float(getattr(event, "price", 0))
        volume = int(getattr(event, "volume", 0))
        vol_z = float(getattr(aggregations, "volume_zscore", 0) or 0)
        momentum = float(getattr(aggregations, "price_momentum", 0) or 0)
        vwap = float(getattr(aggregations, "vwap_5min", 0) or 0)
        vwap_dev = (price - vwap) if vwap else 0.0
        trade_freq = float(getattr(aggregations, "trade_frequency", 0) or 0)
        return np.array([[price, volume, vol_z, momentum, vwap_dev, trade_freq]], dtype=np.float64)

    def _should_retrain(self) -> bool:
        """True if we have enough samples and retrain interval reached."""
        n = len(self._training_buffer)
        if n < self._settings.pipeline.anomaly_training_window:
            return False
        return n % self._settings.pipeline.anomaly_retrain_interval == 0 and n > 0

    def _train_model(self) -> None:
        """Fit IsolationForest on current buffer; log training metrics."""
        if len(self._training_buffer) < self._settings.pipeline.anomaly_training_window:
            return
        X = np.vstack(self._training_buffer)
        self._scaler.fit(X)
        X_scaled = self._scaler.transform(X)
        self._model = IsolationForest(
            contamination=self._settings.pipeline.anomaly_contamination,
            random_state=42,
            n_jobs=-1,
        )
        self._model.fit(X_scaled)
        self._model_version += 1
        logger.info(
            "anomaly_model_trained",
            ticker=self.ticker,
            n_samples=X.shape[0],
            feature_means=X.mean(axis=0).tolist(),
            feature_stds=X.std(axis=0).tolist(),
            model_version=self._model_version,
        )

    def add_event(self, event: Any, features: Any) -> AnomalyResult:
        """
        Add event to buffer; score if model trained; retrain at interval.
        Returns AnomalyResult with score and feature_vector for storage.
        """
        vec = self._extract_features(event, features)
        self._training_buffer.append(vec[0])
        if self._should_retrain():
            self._train_model()
        feature_dict = {
            "price": float(vec[0, 0]),
            "volume": float(vec[0, 1]),
            "volume_zscore": float(vec[0, 2]),
            "price_momentum": float(vec[0, 3]),
            "vwap_deviation": float(vec[0, 4]),
            "trade_frequency": float(vec[0, 5]),
        }
        is_anomaly = False
        score = 0.0
        if self._model is not None and len(self._training_buffer) >= self._settings.pipeline.anomaly_training_window:
            X = vec
            try:
                X_scaled = self._scaler.transform(X)
                score = float(self._model.score_samples(X_scaled)[0])
                pred = int(self._model.predict(X_scaled)[0])
                is_anomaly = pred == -1
            except Exception as e:
                logger.warning("anomaly_score_failed", ticker=self.ticker, error=str(e))
        ts = getattr(event, "timestamp", None) or datetime.utcnow()
        if is_anomaly and self._settings.cloudwatch_enabled:
            self._metrics.emit_metric("AnomaliesDetected", 1.0, "Count", {"ticker": self.ticker})
        return AnomalyResult(
            ticker=self.ticker,
            timestamp=ts,
            is_anomaly=is_anomaly,
            anomaly_score=score,
            feature_vector=feature_dict,
            model_version=self._model_version,
        )
