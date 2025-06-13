from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Literal,
    Optional,
    TypeVar,
    Union,
)

from typing_extensions import NotRequired, TypeAlias, TypedDict, override

from faststream.broker.message import StreamMessage as BrokerStreamMessage

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from faststream.types import DecodedMessage


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
    data: Union[
        bytes,
        List[bytes],
        Dict[bytes, bytes],
        List[Dict[bytes, bytes]],
    ]
    pattern: NotRequired[Optional[bytes]]


class UnifyRedisMessage(BrokerStreamMessage[UnifyRedisDict]):
    pass


class PubSubMessage(TypedDict):
    """A class to represent a PubSub message."""

    type: Literal["pmessage", "message"]
    channel: str
    data: bytes
    pattern: Optional[bytes]


class RedisMessage(BrokerStreamMessage[PubSubMessage]):
    pass


class ListMessage(TypedDict):
    """A class to represent an Abstract List message."""

    channel: str


class DefaultListMessage(ListMessage):
    """A class to represent a single List message."""

    type: Literal["list"]
    data: bytes


class BatchListMessage(ListMessage):
    """A class to represent a List messages batch."""

    type: Literal["blist"]
    data: List[bytes]


class RedisListMessage(BrokerStreamMessage[DefaultListMessage]):
    """StreamMessage for single List message."""

    pass


class RedisBatchListMessage(BrokerStreamMessage[BatchListMessage]):
    """StreamMessage for single List message."""

    decoded_body: List["DecodedMessage"]


DATA_KEY = "__data__"
bDATA_KEY = DATA_KEY.encode()  # noqa: N816


class StreamMessage(TypedDict):
    channel: str
    message_ids: List[bytes]


class DefaultStreamMessage(StreamMessage):
    type: Literal["stream"]
    data: Dict[bytes, bytes]


class BatchStreamMessage(StreamMessage):
    type: Literal["bstream"]
    data: List[Dict[bytes, bytes]]


_StreamMsgType = TypeVar("_StreamMsgType", bound=StreamMessage)


class _RedisStreamMessageMixin(BrokerStreamMessage[_StreamMsgType]):
    @override
    async def ack(
        self,
        redis: Optional["Redis[bytes]"] = None,
        group: Optional[str] = None,
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
        group: Optional[str] = None,
    ) -> None:
        await super().nack()

    @override
    async def reject(
        self,
        redis: Optional["Redis[bytes]"] = None,
        group: Optional[str] = None,
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
    decoded_body: List["DecodedMessage"]
