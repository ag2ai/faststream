from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    TypeVar,
)
from uuid import uuid4

from .source_type import SourceType

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable

# prevent circular imports
MsgType = TypeVar("MsgType")


class AckStatus(str, Enum):
    ACKED = "ACKED"
    NACKED = "NACKED"
    REJECTED = "REJECTED"


class StreamMessage(Generic[MsgType]):
    """Generic class to represent a stream message."""

    def __init__(
        self,
        raw_message: "MsgType",
        body: bytes | Any,
        *,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        batch_headers: list[dict[str, Any]] | None = None,
        path: dict[str, Any] | None = None,
        content_type: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        source_type: SourceType = SourceType.CONSUME,
    ) -> None:
        self.raw_message = raw_message
        self.body = body
        self.reply_to = reply_to
        self.content_type = content_type
        self.source_type = source_type

        self.headers = headers or {}
        self.batch_headers = batch_headers or []
        self.path = path or {}
        self.correlation_id = correlation_id or str(uuid4())
        self.message_id = message_id or self.correlation_id

        self.committed: AckStatus | None = None
        self.processed = False

        # Setup later
        self.__decoder: AsyncCallable | None = None
        self.__decoded_caches: dict[
            Any,
            Any,
        ] = {}  # Cache values between filters and tests

    def set_decoder(self, decoder: "AsyncCallable") -> None:
        self.__decoder = decoder

    def clear_cache(self) -> None:
        self.__decoded_caches.clear()

    def __repr__(self) -> str:
        inner = ", ".join(
            filter(
                bool,
                (
                    f"body={self.body!r}",
                    f"content_type={self.content_type}",
                    f"message_id={self.message_id}",
                    f"correlation_id={self.correlation_id}",
                    f"reply_to={self.reply_to}" if self.reply_to else "",
                    f"headers={self.headers}",
                    f"path={self.path}",
                    f"committed={self.committed}",
                    f"raw_message={self.raw_message}",
                ),
            ),
        )

        return f"{self.__class__.__name__}({inner})"

    async def decode(self) -> Optional["Any"]:
        """Serialize the message by lazy decoder.

        Returns a cache after first usage. To prevent such behavior, please call
        `message.clear_cache()` after `message.body` changes.
        """
        assert self.__decoder, "You should call `set_decoder()` method first."

        if (result := self.__decoded_caches.get(self.__decoder)) is None:
            result = self.__decoded_caches[self.__decoder] = await self.__decoder(self)

        return result

    async def ack(self) -> None:
        if self.committed is None:
            self.committed = AckStatus.ACKED

    async def nack(self) -> None:
        if self.committed is None:
            self.committed = AckStatus.NACKED

    async def reject(self) -> None:
        if self.committed is None:
            self.committed = AckStatus.REJECTED
