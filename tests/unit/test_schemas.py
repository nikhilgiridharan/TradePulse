"""Pydantic schema validation pass/fail cases."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.schemas.news import NewsEvent
from src.schemas.trade import TradeEvent


def test_trade_event_valid_passes() -> None:
    t = TradeEvent(ticker="aapl", price=150.5, size=10)
    assert t.ticker == "AAPL"
    assert t.price == 150.5
    assert t.size == 10


def test_trade_event_missing_required_field_raises() -> None:
    with pytest.raises(ValidationError) as exc:
        TradeEvent.model_validate({"ticker": "AAPL", "price": 1.0})  # missing size
    assert "size" in str(exc.value).lower() or "field required" in str(exc.value).lower()


def test_trade_event_negative_price_raises() -> None:
    with pytest.raises(ValidationError):
        TradeEvent(ticker="AAPL", price=-1.0, size=10)


def test_trade_event_zero_price_raises() -> None:
    with pytest.raises(ValidationError):
        TradeEvent(ticker="AAPL", price=0.0, size=10)


def test_news_event_valid_passes() -> None:
    raw = {
        "id": 42,
        "ticker": "nvda",
        "headline": "Earnings beat",
        "datetime": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }
    n = NewsEvent.model_validate(raw)
    assert n.finnhub_id == 42
    assert n.ticker == "NVDA"
    assert n.headline == "Earnings beat"


def test_news_event_empty_headline_raises() -> None:
    with pytest.raises(ValidationError):
        NewsEvent.model_validate(
            {
                "id": 1,
                "ticker": "AAPL",
                "headline": "",
            }
        )


def test_news_event_whitespace_only_headline_raises() -> None:
    with pytest.raises(ValidationError):
        NewsEvent.model_validate(
            {
                "id": 2,
                "ticker": "AAPL",
                "headline": "   ",
            }
        )
