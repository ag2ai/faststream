from enum import StrEnum


class SourceType(StrEnum):
    CONSUME = "CONSUME"
    """Message consumed by basic subscriber flow."""

    RESPONSE = "RESPONSE"
    """RPC response consumed."""
