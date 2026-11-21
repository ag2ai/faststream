"""Header encoding shared by the Kafka drivers and their test brokers."""

from collections.abc import Mapping


def encode_headers(
    headers: Mapping[str, str | bytes | None],
) -> list[tuple[str, bytes | None]]:
    """Encode text values while preserving binary and null headers."""
    return [
        (name, value.encode() if isinstance(value, str) else value)
        for name, value in headers.items()
    ]
