"""
Schema validation for market events. Validates incoming Kafka messages
before processing to prevent malformed data from polluting aggregations.
"""

from src.validation.schema_validator import MarketEvent, EventType, validate_message

__all__ = ["MarketEvent", "EventType", "validate_message"]
