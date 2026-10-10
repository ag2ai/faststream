import warnings
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
    Optional,
    Protocol,
    TypeAlias,
    TypeVar,
    Union,
)

from typing_extensions import NotRequired, TypedDict, deprecated, override

from faststream._internal.constants import EMPTY
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

    async def xack(self, name: str, groupname: str, *ids: bytes) -> None:
        pass

    async def xdel(self, name: str, *ids: bytes) -> None:
        pass

    async def xpending_range(
        self, name: str, groupname: str, min: bytes, max: bytes, count: int
    ) -> list[dict[str, Any]]:
        pass


class FakeConsumer:
    """A fake Redis consumer."""

    async def xack(self, name: str, groupname: str, *ids: bytes) -> None:
        pass

    async def xdel(self, name: str, *ids: bytes) -> None:
        pass

    async def xpending_range(  # noqa: PLR6301
        self, name: str, groupname: str, min: bytes, max: bytes, count: int
    ) -> list[dict[str, Any]]:
        return []


FAKE_CONSUMER = FakeConsumer()


_DEPRECATION_MESSAGE = (
    "The client and group are bound to the message now."
    " `redis` and `group` arguments will be removed in 1.0.0."
)
_RedisDeprecatedType = Annotated[
    Optional["Redis[bytes]"], deprecated(_DEPRECATION_MESSAGE)
]
_GroupDeprecatedType = Annotated[str | None, deprecated(_DEPRECATION_MESSAGE)]


class _RedisStreamMessageMixin(BrokerStreamMessage[_StreamMsgType]):
    def __init__(
        self, *args: Any, consumer: ConsumerProtocol, group: str | None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.consumer = consumer
        self.group = group

    def _resolve_consumer_context(
        self, redis: Optional["Redis[bytes]"], group: str | None
    ) -> tuple[ConsumerProtocol | None, str | None]:
        if redis is not EMPTY or group is not EMPTY:
            warnings.warn(
                _DEPRECATION_MESSAGE,
                DeprecationWarning,
                stacklevel=3,
            )
        return (
            redis if redis is not EMPTY else self.consumer,
            group if group is not EMPTY else self.group,
        )

    @override
    async def ack(
        self,
        redis: _RedisDeprecatedType = EMPTY,
        group: _GroupDeprecatedType = EMPTY,
    ) -> None:
        redis_resolved, group_resolved = self._resolve_consumer_context(redis, group)
        if (
            not self.committed
            and group_resolved is not None
            and redis_resolved is not None
        ):
            ids = self.raw_message["message_ids"]
            channel = self.raw_message["channel"]
            await redis_resolved.xack(channel, group_resolved, *ids)
        await super().ack()

    @override
    async def nack(
        self,
        redis: _RedisDeprecatedType = EMPTY,
        group: _GroupDeprecatedType = EMPTY,
    ) -> None:
        self._resolve_consumer_context(redis, group)
        await super().nack()

    @override
    async def reject(
        self,
        redis: _RedisDeprecatedType = EMPTY,
        group: _GroupDeprecatedType = EMPTY,
    ) -> None:
        self._resolve_consumer_context(redis, group)
        await super().reject()

    async def delete(self, redis: _RedisDeprecatedType = EMPTY) -> None:
        redis_resolved, _ = self._resolve_consumer_context(redis, EMPTY)
        if redis_resolved is not None:
            ids = self.raw_message["message_ids"]
            channel = self.raw_message["channel"]
            await redis_resolved.xdel(channel, *ids)


class RedisStreamMessage(_RedisStreamMessageMixin[DefaultStreamMessage]):
    async def get_delivery_count(
        self,
        redis: Annotated["Redis[bytes]", deprecated(_DEPRECATION_MESSAGE)] = EMPTY,
        group: Annotated[str, deprecated(_DEPRECATION_MESSAGE)] = EMPTY,
    ) -> int:
        """Return this message's current delivery count from the Redis PEL.

        The count is queried on every call. Messages without an ID or a pending
        entry, including acknowledged messages, return ``1``.
        """
        redis_resolved, group_resolved = self._resolve_consumer_context(redis, group)
        message_ids = self.raw_message["message_ids"]
        if not message_ids or redis_resolved is None or group_resolved is None:
            return 1

        message_id = message_ids[0]
        entries = await redis_resolved.xpending_range(
            name=self.raw_message["channel"],
            groupname=group_resolved,
            min=message_id,
            max=message_id,
            count=1,
        )
        return int(entries[0]["times_delivered"]) if entries else 1


class RedisBatchStreamMessage(_RedisStreamMessageMixin[BatchStreamMessage]):
    decoded_body: list["DecodedMessage"]
