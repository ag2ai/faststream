from enum import Enum
from typing import Any

ContentType = str


class ContentTypes(str, Enum):
    """A class to represent content types."""

    TEXT = "text/plain"
    JSON = "application/json"


class EmptyPlaceholder:
    def __repr__(self) -> str:
        return "EMPTY"

    def __bool__(self) -> bool:
        return False

    def __eq__(self, other: object) -> bool:
        return isinstance(other, EmptyPlaceholder)

    def __hash__(self) -> int:
        return hash(EmptyPlaceholder)


EMPTY: Any = EmptyPlaceholder()

PATH_CONTEXT_PREFIX = "message.path."
"""Where a `Path()` parameter reads from: the Path parameters an Address captured."""
