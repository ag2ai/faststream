from faststream.exceptions import FastStreamException


class StreamGroupNotFoundError(FastStreamException):
    """Raised when a consumer group is not found in a Redis stream.

    Typically happens after a ``FLUSHALL``, ``FLUSHDB``, or manual
    deletion of the stream/group.  The subscriber cannot proceed and
    must be restarted to recreate the group.
    """


class StreamClaimUnsupportedError(FastStreamException):
    """Raised when the Redis server rejects the XREADGROUP CLAIM option.

    ``StreamSub.claim_min_idle_time`` requires Redis server 8.4+; older
    servers reject the CLAIM option with a syntax error.  The subscriber
    cannot proceed - upgrade the server or remove the option.
    """


class UnreachablePathError(FastStreamException):
    """Raised when an allegedly unreachable code path is hit."""

    def __init__(self) -> None:
        super().__init__(
            "This code path should never be reached — it indicates a logic bug."
        )
