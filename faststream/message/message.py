from enum import StrEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    Self,
    TypeVar,
)
from uuid import uuid4

from .source_type import SourceType

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable

# prevent circular imports
MsgType = TypeVar("MsgType")

_NOT_CACHED = object()
_UNSET = object()


def _slot_names(cls: type) -> tuple[str, ...]:
    """Every slot a message class holds, private names mangled as they are stored.

    `__weakref__` is left out: a weak reference belongs to the object, not its copy.
    """
    return tuple(
        f"_{owner.__name__.lstrip('_')}{name}"
        if name.startswith("__") and not name.endswith("__")
        else name
        for owner in cls.__mro__
        for name in owner.__dict__.get("__slots__", ())
        if name != "__weakref__"
    )


class AckStatus(StrEnum):
    ACKED = "ACKED"
    NACKED = "NACKED"
    REJECTED = "REJECTED"


class StreamMessage(Generic[MsgType]):
    """Generic class to represent a stream message."""

    __slots__ = (
        "__decoded_caches",
        "__decoder",
        # User code may key a WeakKeyDictionary by the message
        "__weakref__",
        # The FastAPI plugin parks its BackgroundTasks here and
        # `_BackgroundMiddleware` runs them; unset on every other path.
        "background",
        "batch_headers",
        "body",
        "committed",
        "content_type",
        "correlation_id",
        "headers",
        "message_id",
        "path",
        "processed",
        "raw_message",
        "reply_to",
        "source_type",
    )

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

    def __copy__(self) -> Self:
        message = self.__class__.__new__(self.__class__)
        # Walks the MRO, so a broker's own slots (`KafkaMessage.consumer`) come
        # along with this class's; a slot never assigned stays unset on the copy.
        for name in _slot_names(self.__class__):
            if (value := getattr(self, name, _UNSET)) is not _UNSET:
                setattr(message, name, value)
        # A copy answers for its own body, so it must not share the decode cache
        message.__decoded_caches = {}
        return message

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

        if (
            result := self.__decoded_caches.get(self.__decoder, _NOT_CACHED)
        ) is _NOT_CACHED:
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
