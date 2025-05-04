import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Protocol,
    Union,
    cast,
)
from unittest.mock import AsyncMock, MagicMock

import anyio
from typing_extensions import TypedDict, override

from faststream._internal.subscriber.utils import resolve_custom_func
from faststream._internal.testing.broker import TestBroker
from faststream.exceptions import SetupError, SubscriberNotFound
from faststream.message import gen_cor_id
from faststream.redis.broker.broker import RedisBroker
from faststream.redis.message import (
    BatchListMessage,
    BatchStreamMessage,
    DefaultListMessage,
    DefaultStreamMessage,
    PubSubMessage,
    bDATA_KEY,
)
from faststream.redis.parser import RawMessage, RedisPubSubParser
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import DestinationType, RedisPublishCommand
from faststream.redis.schemas import INCORRECT_SETUP_MSG
from faststream.redis.subscriber.usecases.channel_subscriber import ChannelSubscriber
from faststream.redis.subscriber.usecases.list_subscriber import _ListHandlerMixin
from faststream.redis.subscriber.usecases.stream_subscriber import _StreamHandlerMixin

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict, SendableMessage
    from faststream.redis.publisher.specified import SpecificationPublisher
    from faststream.redis.subscriber.usecases.basic import LogicSubscriber

__all__ = ("TestRedisBroker",)


class TestRedisBroker(TestBroker[RedisBroker]):
    """A class to test Redis brokers."""

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: RedisBroker,
        publisher: "SpecificationPublisher",
    ) -> tuple["LogicSubscriber", bool]:
        sub: Optional[LogicSubscriber] = None

        named_property = publisher.subscriber_property(name_only=True)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in broker._subscribers:  # pragma: no branch
            for visitor in visitors:
                if visitor.visit(**named_property, sub=handler):
                    sub = handler
                    break

        if sub is None:
            is_real = False
            sub = broker.subscriber(**publisher.subscriber_property(name_only=False))

        else:
            is_real = True

        return sub, is_real

    @contextmanager
    def _patch_producer(self, broker: RedisBroker) -> Iterator[None]:
        old_producer = broker._state.get().producer
        broker._state.patch_value(producer=FakeProducer(broker))
        yield
        broker._state.patch_value(producer=old_producer)

    @staticmethod
    async def _fake_connect(  # type: ignore[override]
        broker: RedisBroker,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncMock:
        connection = MagicMock()

        pub_sub = AsyncMock()

        async def get_msg(*args: Any, timeout: float, **kwargs: Any) -> None:
            await anyio.sleep(timeout)

        pub_sub.get_message = get_msg

        connection.pubsub.side_effect = lambda: pub_sub
        return connection


class FakeProducer(RedisFastProducer):
    def __init__(self, broker: RedisBroker) -> None:
        self.broker = broker

        default = RedisPubSubParser()
        self._parser = resolve_custom_func(
            broker._parser,
            default.parse_message,
        )
        self._decoder = resolve_custom_func(
            broker._decoder,
            default.decode_message,
        )

    @override
    async def publish(
        self,
        cmd: "RedisPublishCommand",
    ) -> None:
        body = build_message(
            message=cmd.body,
            reply_to=cmd.reply_to,
            correlation_id=cmd.correlation_id or gen_cor_id(),
            headers=cmd.headers,
        )

        destination = _make_destionation_kwargs(cmd)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in self.broker._subscribers:  # pragma: no branch
            for visitor in visitors:
                if visited_ch := visitor.visit(**destination, sub=handler):
                    msg = visitor.get_message(
                        visited_ch,
                        body,
                        handler,  # type: ignore[arg-type]
                    )

                    await self._execute_handler(msg, handler)

    @override
    async def request(  # type: ignore[override]
        self,
        cmd: "RedisPublishCommand",
    ) -> "PubSubMessage":
        body = build_message(
            message=cmd.body,
            correlation_id=cmd.correlation_id or gen_cor_id(),
            headers=cmd.headers,
        )

        destination = _make_destionation_kwargs(cmd)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in self.broker._subscribers:  # pragma: no branch
            for visitor in visitors:
                if visited_ch := visitor.visit(**destination, sub=handler):
                    msg = visitor.get_message(
                        visited_ch,
                        body,
                        handler,  # type: ignore[arg-type]
                    )

                    with anyio.fail_after(cmd.timeout):
                        return await self._execute_handler(msg, handler)

        raise SubscriberNotFound

    async def publish_batch(
        self,
        cmd: "RedisPublishCommand",
    ) -> None:
        data_to_send = [
            build_message(
                m,
                correlation_id=cmd.correlation_id or gen_cor_id(),
                headers=cmd.headers,
            )
            for m in cmd.batch_bodies
        ]

        visitor = ListVisitor()
        for handler in self.broker._subscribers:  # pragma: no branch
            if visitor.visit(list=cmd.destination, sub=handler):
                casted_handler = cast("_ListHandlerMixin", handler)

                if casted_handler.list_sub.batch:
                    msg = visitor.get_message(
                        cmd.destination, data_to_send, casted_handler
                    )

                    await self._execute_handler(msg, handler)

    async def _execute_handler(
        self,
        msg: Any,
        handler: "LogicSubscriber",
    ) -> "PubSubMessage":
        result = await handler.process_message(msg)

        return PubSubMessage(
            type="message",
            data=build_message(
                message=result.body,
                headers=result.headers,
                correlation_id=result.correlation_id or "",
            ),
            channel="",
            pattern=None,
        )


def build_message(
    message: Union[Sequence["SendableMessage"], "SendableMessage"],
    *,
    correlation_id: str,
    reply_to: str = "",
    headers: Optional["AnyDict"] = None,
) -> bytes:
    return RawMessage.encode(
        message=message,
        reply_to=reply_to,
        headers=headers,
        correlation_id=correlation_id,
    )


class Visitor(Protocol):
    def visit(
        self,
        *,
        channel: Optional[str],
        list: Optional[str],
        stream: Optional[str],
        sub: "LogicSubscriber",
    ) -> Optional[str]: ...

    def get_message(self, channel: str, body: Any, sub: "LogicSubscriber") -> Any: ...


class ChannelVisitor(Visitor):
    def visit(
        self,
        *,
        sub: "LogicSubscriber",
        channel: Optional[str] = None,
        list: Optional[str] = None,
        stream: Optional[str] = None,
    ) -> Optional[str]:
        if channel is None or not isinstance(sub, ChannelSubscriber):
            return None

        sub_channel = sub.channel

        if (
            sub_channel.pattern
            and bool(
                re.match(
                    sub_channel.name.replace(".", "\\.").replace("*", ".*"),
                    channel or "",
                ),
            )
        ) or channel == sub_channel.name:
            return channel

        return None

    def get_message(  # type: ignore[override]
        self,
        channel: str,
        body: Any,
        sub: "ChannelSubscriber",
    ) -> Any:
        return PubSubMessage(
            type="message",
            data=body,
            channel=channel,
            pattern=sub.channel.pattern.encode() if sub.channel.pattern else None,
        )


class ListVisitor(Visitor):
    def visit(
        self,
        *,
        sub: "LogicSubscriber",
        channel: Optional[str] = None,
        list: Optional[str] = None,
        stream: Optional[str] = None,
    ) -> Optional[str]:
        if list is None or not isinstance(sub, _ListHandlerMixin):
            return None

        if list == sub.list_sub.name:
            return list

        return None

    def get_message(  # type: ignore[override]
        self,
        channel: str,
        body: Any,
        sub: "_ListHandlerMixin",
    ) -> Any:
        if sub.list_sub.batch:
            return BatchListMessage(
                type="blist",
                channel=channel,
                data=body if isinstance(body, list) else [body],
            )

        return DefaultListMessage(
            type="list",
            channel=channel,
            data=body,
        )


class StreamVisitor(Visitor):
    def visit(
        self,
        *,
        sub: "LogicSubscriber",
        channel: Optional[str] = None,
        list: Optional[str] = None,
        stream: Optional[str] = None,
    ) -> Optional[str]:
        if stream is None or not isinstance(sub, _StreamHandlerMixin):
            return None

        if stream == sub.stream_sub.name:
            return stream

        return None

    def get_message(  # type: ignore[override]
        self,
        channel: str,
        body: Any,
        sub: "_StreamHandlerMixin",
    ) -> Any:
        if sub.stream_sub.batch:
            return BatchStreamMessage(
                type="bstream",
                channel=channel,
                data=[{bDATA_KEY: body}],
                message_ids=[],
            )

        return DefaultStreamMessage(
            type="stream",
            channel=channel,
            data={bDATA_KEY: body},
            message_ids=[],
        )


class _DestinationKwargs(TypedDict, total=False):
    channel: str
    list: str
    stream: str


def _make_destionation_kwargs(cmd: RedisPublishCommand) -> _DestinationKwargs:
    destination: _DestinationKwargs = {}
    if cmd.destination_type is DestinationType.Channel:
        destination["channel"] = cmd.destination
    if cmd.destination_type is DestinationType.List:
        destination["list"] = cmd.destination
    if cmd.destination_type is DestinationType.Stream:
        destination["stream"] = cmd.destination

    if len(destination) != 1:
        raise SetupError(INCORRECT_SETUP_MSG)

    return destination
