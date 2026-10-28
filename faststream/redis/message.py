from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NotRequired,
    Protocol,
    TypeAlias,
    TypeVar,
    Union,
)

from typing_extensions import TypedDict, override

from faststream.message import StreamMessage as BrokerStreamMessage

if TYPE_CHECKING:
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
    __slots__ = ()


class PubSubMessage(TypedDict):
    """A class to represent a PubSub message."""

    type: Literal["pmessage", "message"]
    channel: str
    data: bytes
    pattern: bytes | None


class RedisChannelMessage(BrokerStreamMessage[PubSubMessage]):
    __slots__ = ()


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

    __slots__ = ()


class RedisBatchListMessage(BrokerStreamMessage[BatchListMessage]):
    """StreamMessage for single List message."""

    __slots__ = ()

    decoded_body: list["DecodedMessage"]


DATA_KEY = "__data__"
bDATA_KEY = DATA_KEY.encode()  # ruff: ignore[mixed-case-variable-in-global-scope]


class _StreamMessage(TypedDict):
    channel: str
    message_ids: list[bytes]
    # Only with `StreamSub.claim_min_idle_time`, aligned with `message_ids`;
    # `delivery_counts` is XPENDING's `times_delivered` - 1 (0 = new message).
    idle_times: NotRequired[list[int]]
    delivery_counts: NotRequired[list[int]]


class DefaultStreamMessage(_StreamMessage):
    type: Literal["stream"]
    data: dict[bytes, bytes]


class BatchStreamMessage(_StreamMessage):
    type: Literal["bstream"]
    data: list[dict[bytes, bytes]]


_StreamMsgType = TypeVar("_StreamMsgType", bound=_StreamMessage)


class ConsumerProtocol(Protocol):
    """A protocol for Redis consumers."""

    __slots__ = ()

    async def xack(self, name: str, groupname: str, *ids: bytes) -> None:
        pass

    async def xdel(self, name: str, *ids: bytes) -> None:
        pass

    async def xpending_range(
        self, name: str, groupname: str, min: bytes, max: bytes, count: int
    ) -> list[dict[str, Any]]:
        pass


class FakeConsumer(ConsumerProtocol):
    """A fake Redis consumer."""

    __slots__ = ()

    async def xack(self, name: str, groupname: str, *ids: bytes) -> None:
        pass

    async def xdel(self, name: str, *ids: bytes) -> None:
        pass

    async def xpending_range(  # ruff: ignore[no-self-use]
        self, name: str, groupname: str, min: bytes, max: bytes, count: int
    ) -> list[dict[str, Any]]:
        return []


FAKE_CONSUMER = FakeConsumer()


class _RedisStreamMessageMixin(BrokerStreamMessage[_StreamMsgType]):
    __slots__ = ("consumer", "group")

    def __init__(
        self, *args: Any, consumer: ConsumerProtocol, group: str | None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.consumer = consumer
        self.group = group

    @override
    async def ack(self) -> None:
        if not self.committed and self.group is not None:
            ids = self.raw_message["message_ids"]
            channel = self.raw_message["channel"]
            await self.consumer.xack(channel, self.group, *ids)
        await super().ack()

    @override
    async def nack(self) -> None:
        await super().nack()

    @override
    async def reject(self) -> None:
        await super().reject()

    async def delete(
        self,
    ) -> None:
        ids = self.raw_message["message_ids"]
        channel = self.raw_message["channel"]
        await self.consumer.xdel(channel, *ids)


class RedisStreamMessage(_RedisStreamMessageMixin[DefaultStreamMessage]):
    __slots__ = ()

    async def get_delivery_count(self) -> int:
        """Return this message's current delivery count from the Redis PEL.

        The count is queried on every call. Messages without an ID or a pending
        entry, including acknowledged messages, return ``1``.
        """
        message_ids = self.raw_message["message_ids"]
        if not message_ids or self.group is None:
            return 1

        message_id = message_ids[0]
        entries = await self.consumer.xpending_range(
            name=self.raw_message["channel"],
            groupname=self.group,
            min=message_id,
            max=message_id,
            count=1,
        )
        return int(entries[0]["times_delivered"]) if entries else 1


class RedisBatchStreamMessage(_RedisStreamMessageMixin[BatchStreamMessage]):
    __slots__ = ()

    decoded_body: list["DecodedMessage"]
