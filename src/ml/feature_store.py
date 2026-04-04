"""
Async feature store for ``market_features``: DynamoDB Map type (not JSON strings).

``write_features`` / ``get_features`` are the primary API. ``get_latest`` /
``FeatureStoreRecord`` remain for FastAPI compatibility.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Mapping, Optional

from src.config import get_settings
from src.storage.dynamo_writer import AsyncDynamoWriter


class FeatureStoreRecord:
    """API-facing snapshot (legacy shape for /features)."""

    def __init__(
        self,
        ticker: str,
        vwap_5min: float,
        volume_zscore: float,
        price_momentum: float,
        trade_frequency: float,
        updated_at: datetime,
        bid_ask_spread: Optional[float] = None,
    ) -> None:
        self.ticker = ticker
        self.vwap_5min = vwap_5min
        self.volume_zscore = volume_zscore
        self.price_momentum = price_momentum
        self.trade_frequency = trade_frequency
        self.bid_ask_spread = bid_ask_spread
        self.updated_at = updated_at


class FeatureStore:
    def __init__(self, writer: Optional[AsyncDynamoWriter] = None) -> None:
        self._settings = get_settings()
        self._writer = writer

    async def _w(self) -> AsyncDynamoWriter:
        if self._writer is not None:
            return self._writer
        return await AsyncDynamoWriter.instance()

    async def write_features(
        self,
        ticker: str,
        feature_vector: Mapping[str, float],
        *,
        feature_row_id: str = "latest",
        use_batch: bool = True,
    ) -> bool:
        """
        Persist ``feature_vector`` as a DynamoDB Map (via low-level ``M`` / ``N`` types).

        All values are coerced to float for numeric map attributes.
        """
        vec = {k: float(v) for k, v in feature_vector.items()}
        now = datetime.now(timezone.utc)
        w = await self._w()
        return await w.put_market_features(
            ticker=ticker.upper(),
            feature_row_id=feature_row_id,
            updated_at=now,
            feature_vector=vec,
            use_batch=use_batch,
        )

    async def get_features(self, ticker: str) -> Optional[dict[str, float]]:
        """
        Load latest feature row across write shards. Returns ``None`` if missing or TTL expired.

        Dynamo TTL is best-effort; we also treat ``ttl`` epoch < now as expired.
        """
        w = await self._w()
        raw = await w.get_latest_features(ticker.upper())
        if not raw:
            return None
        ttl_raw = raw.get("ttl")
        if ttl_raw is not None:
            try:
                ttl_epoch = int(float(ttl_raw))
                if ttl_epoch < int(time.time()):
                    return None
            except (TypeError, ValueError):
                pass
        fv = raw.get("feature_vector")
        if not isinstance(fv, dict):
            return None
        out: dict[str, float] = {}
        for k, v in fv.items():
            try:
                out[str(k)] = float(v)
            except (TypeError, ValueError):
                continue
        return out if out else None

    async def get_latest(self, ticker: str) -> Optional[FeatureStoreRecord]:
        """Map stored vector keys into ``FeatureStoreRecord`` when present."""
        data = await self.get_features(ticker)
        if not data:
            return None
        w = await self._w()
        raw = await w.get_latest_features(ticker.upper())
        updated_at = datetime.now(timezone.utc)
        if raw:
            ua = raw.get("updated_at")
            if isinstance(ua, str):
                try:
                    updated_at = datetime.fromisoformat(ua.replace("Z", "+00:00"))
                except ValueError:
                    pass
        return FeatureStoreRecord(
            ticker=ticker.upper(),
            vwap_5min=float(
                data.get("vwap_5min", data.get("vwap_5m", data.get("vwap_1min", 0.0)))
            ),
            volume_zscore=float(data.get("volume_zscore", 0.0)),
            price_momentum=float(data.get("price_momentum", data.get("momentum", 0.0))),
            trade_frequency=float(data.get("trade_frequency", 0.0)),
            updated_at=updated_at,
            bid_ask_spread=(
                float(data["bid_ask_spread"]) if data.get("bid_ask_spread") is not None else None
            ),
        )

    async def write_features_record(
        self, record: FeatureStoreRecord, dedupe_salt: str
    ) -> bool:
        """Legacy path: build dict from record fields."""
        vec: dict[str, float] = {
            "vwap_5min": record.vwap_5min,
            "volume_zscore": record.volume_zscore,
            "price_momentum": record.price_momentum,
            "trade_frequency": record.trade_frequency,
        }
        if record.bid_ask_spread is not None:
            vec["bid_ask_spread"] = record.bid_ask_spread
        return await self.write_features(
            record.ticker, vec, feature_row_id=dedupe_salt, use_batch=True
        )
