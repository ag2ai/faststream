from typing import (
    TYPE_CHECKING,
    Literal,
    Optional,
    TypeAlias,
    TypeVar,
    Union,
)

from typing_extensions import NotRequired, TypedDict, override

from faststream.message import StreamMessage as BrokerStreamMessage

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from faststream._internal.basic_types import DecodedMessage


BaseMessage: TypeAlias = Union[
    "PubSubMessage",
    "DefaultListMessage",
    "BatchListMessage",
    "DefaultStreamMessage",
    "BatchStreamMessage",
]


class UnifyRedisDict(TypedDict):
    type: Literal[
        "pmessage",
        "message",
        "list",
        "blist",
        "stream",
        "bstream",
    ]
    channel: str
    data: bytes | list[bytes] | dict[bytes, bytes] | list[dict[bytes, bytes]]
    pattern: NotRequired[bytes | None]


class RedisMessage(BrokerStreamMessage[UnifyRedisDict]):
    pass


class PubSubMessage(TypedDict):
    """A class to represent a PubSub message."""

    type: Literal["pmessage", "message"]
    channel: str
    data: bytes
    pattern: bytes | None


class RedisChannelMessage(BrokerStreamMessage[PubSubMessage]):
    pass


class _ListMessage(TypedDict):
    """A class to represent an Abstract List message."""

    channel: str


class DefaultListMessage(_ListMessage):
    """A class to represent a single List message."""

    type: Literal["list"]
    data: bytes


class BatchListMessage(_ListMessage):
    """A class to represent a List messages batch."""

    type: Literal["blist"]
    data: list[bytes]


class RedisListMessage(BrokerStreamMessage[DefaultListMessage]):
    """StreamMessage for single List message."""


class RedisBatchListMessage(BrokerStreamMessage[BatchListMessage]):
    """StreamMessage for single List message."""

    decoded_body: list["DecodedMessage"]


DATA_KEY = "__data__"
bDATA_KEY = DATA_KEY.encode()  # noqa: N816


class _StreamMessage(TypedDict):
    channel: str
    message_ids: list[bytes]


class DefaultStreamMessage(_StreamMessage):
    type: Literal["stream"]
    data: dict[bytes, bytes]


class BatchStreamMessage(_StreamMessage):
    type: Literal["bstream"]
    data: list[dict[bytes, bytes]]


_StreamMsgType = TypeVar("_StreamMsgType", bound=_StreamMessage)


class _RedisStreamMessageMixin(BrokerStreamMessage[_StreamMsgType]):
    @override
    async def ack(
        self,
        redis: Optional["Redis[bytes]"] = None,
        group: str | None = None,
    ) -> None:
        if not self.committed and group is not None and redis is not None:
            ids = self.raw_message["message_ids"]
            channel = self.raw_message["channel"]
            await redis.xack(channel, group, *ids)  # type: ignore[no-untyped-call]
        await super().ack()

    @override
    async def nack(
        self,
        redis: Optional["Redis[bytes]"] = None,
        group: str | None = None,
    ) -> None:
        await super().nack()

    @override
    async def reject(
        self,
        redis: Optional["Redis[bytes]"] = None,
        group: str | None = None,
    ) -> None:
        await super().reject()

    async def delete(self, redis: Optional["Redis[bytes]"]) -> None:
        if redis is not None:
            ids = self.raw_message["message_ids"]
            channel = self.raw_message["channel"]
            await redis.xdel(channel, *ids)


class RedisStreamMessage(_RedisStreamMessageMixin[DefaultStreamMessage]):
    pass


class RedisBatchStreamMessage(_RedisStreamMessageMixin[BatchStreamMessage]):
    decoded_body: list["DecodedMessage"]
