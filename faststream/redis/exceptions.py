from faststream.exceptions import FastStreamException


class StreamGroupNotFoundError(FastStreamException):
    """Raised when a consumer group is not found in a Redis stream.

    Typically happens after a ``FLUSHALL``, ``FLUSHDB``, or manual
    deletion of the stream/group.  The subscriber cannot proceed and
    must be restarted to recreate the group.
    """


class UnreachablePathError(FastStreamException):
    """Raised when an allegedly unreachable code path is hit."""

    def __init__(self) -> None:
        super().__init__(
            "This code path should never be reached — it indicates a logic bug."
        )
