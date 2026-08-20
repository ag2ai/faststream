from enum import Enum
from typing import Any

from typing_extensions import Self

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


# NOTE: subclasses bytes so a tombstone body is `b""` to every consumer that
# doesn't ask; `isinstance(body, Tombstone)` is the only way to tell them apart.
class Tombstone(bytes):
    __slots__ = ()

    # NOTE: bytes.__getnewargs__ passes b"" back on unpickle, so accept it
    def __new__(cls, value: bytes = b"") -> "Self":
        if value:
            msg = f"{cls.__name__} carries no data, got {value!r}"
            raise ValueError(msg)
        return super().__new__(cls, b"")

    def __repr__(self) -> str:
        return "TOMBSTONE"


TOMBSTONE: Tombstone = Tombstone()
