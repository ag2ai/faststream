"""GCP Pub/Sub response type markers."""

from collections import UserDict, UserString

# Type markers for explicit tuple returns
# These make it clear what each element in the tuple represents


class ResponseAttributes(UserDict[str, str]):
    """Marker type for message attributes in response tuples.

    Use this to explicitly mark attributes in tuple returns:
        return "message", ResponseAttributes({"key": "value"})
    """

    def __init__(self, attributes: dict[str, str]) -> None:
        """Initialize with string key-value pairs."""
        # Validate all keys and values are strings
        for k, v in attributes.items():
            if not isinstance(k, str) or not isinstance(v, str):
                msg = f"Attributes must have string keys and values, got {k!r}: {v!r}"
                raise TypeError(msg)
        super().__init__(attributes)


class ResponseOrderingKey(UserString):
    """Marker type for ordering key in response tuples.

    Use this to explicitly mark ordering key in tuple returns:
        return "message", ResponseOrderingKey("user-123")
    """

    def __init__(self, ordering_key: str) -> None:
        """Initialize with non-empty string."""
        if not ordering_key:
            msg = "Ordering key cannot be empty"
            raise ValueError(msg)
        super().__init__(ordering_key)
