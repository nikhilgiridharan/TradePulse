"""Unit tests for schema validation."""

import json
from datetime import datetime, timezone

import pytest

from src.validation.schema_validator import validate_message, MarketEvent, EventType


def test_valid_trade_event_passes_validation():
    payload = json.dumps({
        "ticker": "AAPL",
        "price": 150.25,
        "volume": 100,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "trade",
        "sequence_number": 1,
    })
    event, err = validate_message(payload)
    assert err is None
    assert event is not None
    assert event.ticker == "AAPL"
    assert event.price == 150.25
    assert event.event_type == EventType.TRADE


def test_valid_quote_event_passes_validation():
    payload = json.dumps({
        "ticker": "MSFT",
        "price": 380.50,
        "volume": 0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "quote",
        "sequence_number": 2,
    })
    event, err = validate_message(payload)
    assert err is None
    assert event is not None
    assert event.event_type == EventType.QUOTE


def test_negative_price_fails_validation():
    payload = json.dumps({
        "ticker": "AAPL",
        "price": -1.0,
        "volume": 100,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "trade",
        "sequence_number": 1,
    })
    event, err = validate_message(payload)
    assert event is None
    assert err is not None


def test_zero_price_fails_validation():
    payload = json.dumps({
        "ticker": "AAPL",
        "price": 0.0,
        "volume": 100,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "trade",
        "sequence_number": 1,
    })
    event, err = validate_message(payload)
    assert event is None
    assert err is not None


def test_negative_volume_fails_validation():
    payload = json.dumps({
        "ticker": "AAPL",
        "price": 150.0,
        "volume": -1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "trade",
        "sequence_number": 1,
    })
    event, err = validate_message(payload)
    assert event is None
    assert err is not None


def test_missing_ticker_fails_validation():
    payload = json.dumps({
        "price": 150.0,
        "volume": 100,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "trade",
        "sequence_number": 1,
    })
    event, err = validate_message(payload)
    assert event is None
    assert err is not None


def test_invalid_event_type_fails_validation():
    payload = json.dumps({
        "ticker": "AAPL",
        "price": 150.0,
        "volume": 100,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "invalid",
        "sequence_number": 1,
    })
    event, err = validate_message(payload)
    assert event is None
    assert err is not None


def test_lowercase_ticker_fails_validation():
    payload = json.dumps({
        "ticker": "aapl",
        "price": 150.0,
        "volume": 100,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "trade",
        "sequence_number": 1,
    })
    event, err = validate_message(payload)
    assert event is None
    assert err is not None


def test_invalid_timestamp_fails_validation():
    payload = json.dumps({
        "ticker": "AAPL",
        "price": 150.0,
        "volume": 100,
        "timestamp": "not-a-date",
        "event_type": "trade",
        "sequence_number": 1,
    })
    event, err = validate_message(payload)
    assert event is None
    assert err is not None


def test_validation_failure_returns_none_event():
    """Confirm function signature: returns (None, error) not raises."""
    event, err = validate_message('{"ticker":"AAPL","price":-1}')
    assert event is None
    assert err is not None


def test_validation_success_returns_none_error():
    """Confirm function signature: returns (event, None) not raises."""
    payload = json.dumps({
        "ticker": "AAPL",
        "price": 1.0,
        "volume": 0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "trade",
        "sequence_number": 1,
    })
    event, err = validate_message(payload)
    assert event is not None
    assert err is None
