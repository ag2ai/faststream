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


EMPTY: Any = EmptyPlaceholder()
