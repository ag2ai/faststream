import re
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
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

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.testing.broker import TestBroker, change_producer
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
from faststream.redis.parser import MessageFormat, ParserConfig, RedisPubSubParser
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import DestinationType, RedisPublishCommand
from faststream.redis.schemas import INCORRECT_SETUP_MSG
from faststream.redis.subscriber.usecases.channel_subscriber import ChannelSubscriber
from faststream.redis.subscriber.usecases.list_subscriber import _ListHandlerMixin
from faststream.redis.subscriber.usecases.stream_subscriber import _StreamHandlerMixin

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage
    from faststream.redis.publisher.usecase import LogicPublisher
    from faststream.redis.subscriber.usecases.basic import LogicSubscriber

__all__ = ("TestRedisBroker",)


class TestRedisBroker(TestBroker[RedisBroker]):
    """A class to test Redis brokers."""

    @contextmanager
    def _patch_producer(self, broker: RedisBroker) -> Iterator[None]:
        with ExitStack() as es:
            es.enter_context(
                change_producer(
                    broker.config.broker_config, FakeProducer(broker, broker.config)
                ),
            )

            for publisher in cast("list[LogicPublisher]", broker.publishers):
                es.enter_context(
                    change_producer(publisher, FakeProducer(broker, publisher.config)),
                )

            yield

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: RedisBroker,
        publisher: "LogicPublisher",
    ) -> tuple["LogicSubscriber", bool]:
        sub: LogicSubscriber | None = None

        named_property = publisher.subscriber_property(name_only=True)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in broker.subscribers:  # pragma: no branch
            handler = cast("LogicSubscriber", handler)
            for visitor in visitors:
                if visitor.visit(**named_property, sub=handler):
                    sub = handler
                    break

        if sub is None:
            is_real = False
            sub_options = publisher.subscriber_property(name_only=False)
            sub = broker.subscriber(**sub_options, persistent=False)
        else:
            is_real = True

        return sub, is_real

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
        connection.aclose = AsyncMock()

        connection.xack = AsyncMock()
        connection.xdel = AsyncMock()

        broker.config.broker_config.connection._client = connection
        return connection


class FakeProducer(RedisFastProducer):
    def __init__(self, broker: RedisBroker, config: ParserConfig) -> None:
        self.broker = broker

        default = RedisPubSubParser(config)

        self._parser = ParserComposition(
            broker._parser,
            default.parse_message,
        )
        self._decoder = ParserComposition(
            broker._decoder,
            default.decode_message,
        )

    @override
    async def publish(self, cmd: "RedisPublishCommand") -> int | bytes:
        body = build_message(
            message=cmd.body,
            reply_to=cmd.reply_to,
            correlation_id=cmd.correlation_id or gen_cor_id(),
            headers=cmd.headers,
            message_format=cmd.message_format,
            serializer=self.broker.config.fd_config._serializer,
        )

        destination = _make_destination_kwargs(cmd)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in self.broker.subscribers:  # pragma: no branch
            handler = cast("LogicSubscriber", handler)
            for visitor in visitors:
                if visited_ch := visitor.visit(**destination, sub=handler):
                    msg = visitor.get_message(
                        visited_ch,
                        body,
                        handler,  # type: ignore[arg-type]
                    )

                    await self._execute_handler(msg, handler)

        return 0

    @override
    async def request(self, cmd: "RedisPublishCommand") -> "PubSubMessage":
        body = build_message(
            message=cmd.body,
            correlation_id=cmd.correlation_id or gen_cor_id(),
            headers=cmd.headers,
            message_format=cmd.message_format,
            serializer=self.broker.config.fd_config._serializer,
        )

        destination = _make_destination_kwargs(cmd)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in self.broker.subscribers:  # pragma: no branch
            handler = cast("LogicSubscriber", handler)
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

    @override
    async def publish_batch(self, cmd: "RedisPublishCommand") -> int:
        data_to_send = [
            build_message(
                m,
                correlation_id=cmd.correlation_id or gen_cor_id(),
                headers=cmd.headers,
                message_format=cmd.message_format,
                serializer=self.broker.config.fd_config._serializer,
            )
            for m in cmd.batch_bodies
        ]

        visitor = ListVisitor()
        for handler in self.broker.subscribers:  # pragma: no branch
            handler = cast("LogicSubscriber", handler)
            if visitor.visit(list=cmd.destination, sub=handler):
                casted_handler = cast("_ListHandlerMixin", handler)

                if casted_handler.list_sub.batch:
                    msg = visitor.get_message(
                        channel=cmd.destination,
                        body=data_to_send,
                        sub=casted_handler,
                    )

                    await self._execute_handler(msg, handler)

        return 0

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
                message_format=handler.config.message_format,
                serializer=self.broker.config.fd_config._serializer,
            ),
            channel="",
            pattern=None,
        )


def build_message(
    message: Union[Sequence["SendableMessage"], "SendableMessage"],
    *,
    correlation_id: str,
    message_format: type["MessageFormat"],
    reply_to: str = "",
    headers: dict[str, Any] | None = None,
    serializer: Optional["SerializerProto"] = None,
) -> bytes:
    return message_format.encode(
        message=message,
        reply_to=reply_to,
        headers=headers,
        correlation_id=correlation_id,
        serializer=serializer,
    )


class Visitor(Protocol):
    def visit(
        self,
        *,
        channel: str | None,
        list: str | None,
        stream: str | None,
        sub: "LogicSubscriber",
    ) -> str | None: ...

    def get_message(self, channel: str, body: Any, sub: "LogicSubscriber") -> Any: ...


class ChannelVisitor(Visitor):
    def visit(
        self,
        *,
        sub: "LogicSubscriber",
        channel: str | None = None,
        list: str | None = None,
        stream: str | None = None,
    ) -> str | None:
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
        channel: str | None = None,
        list: str | None = None,
        stream: str | None = None,
    ) -> str | None:
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
        channel: str | None = None,
        list: str | None = None,
        stream: str | None = None,
    ) -> str | None:
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


def _make_destination_kwargs(cmd: RedisPublishCommand) -> _DestinationKwargs:
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
