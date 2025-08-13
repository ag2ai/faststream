"""Common serialization utilities for testing modules."""

from typing import Any


def create_json_serializer() -> Any:
    """Create a JSON serializer for custom types."""
    from datetime import date, datetime

    def json_serializer(obj: Any) -> Any:
        """Custom JSON serializer for common Python types."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        error_msg = f"Object of type {type(obj).__name__} is not JSON serializable"
        raise TypeError(error_msg)

    return json_serializer


def serialize_with_broker_serializer(message_data: Any, serializer: Any) -> bytes:
    """Serialize message data using broker's serializer."""
    try:
        # Try using the broker's serializer first
        data = serializer.dumps(message_data)
        if isinstance(data, str):
            return data.encode()
        if isinstance(data, bytes):
            return data
        # Convert any other type to bytes via JSON
        return serialize_with_json(message_data)
    except Exception:
        # Fall back to JSON serialization
        return serialize_with_json(message_data)


def serialize_with_json(message_data: Any) -> bytes:
    """Serialize message data using JSON."""
    import json
    from dataclasses import asdict, is_dataclass

    json_serializer = create_json_serializer()

    if is_dataclass(message_data) and not isinstance(message_data, type):
        return json.dumps(asdict(message_data), default=json_serializer).encode()
    # Try to serialize as dict if it has __dict__
    try:
        return json.dumps(
            message_data.__dict__ if hasattr(message_data, "__dict__") else message_data,
            default=json_serializer,
        ).encode()
    except (TypeError, AttributeError):
        # Last resort - convert to string
        return str(message_data).encode()
