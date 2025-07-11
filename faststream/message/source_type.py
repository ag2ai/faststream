from enum import Enum


class SourceType(str, Enum):
    CONSUME = "CONSUME"
    """Message consumed by basic subscriber flow."""

    RESPONSE = "RESPONSE"
    """RPC response consumed."""
