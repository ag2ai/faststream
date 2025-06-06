import re
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
    cast,
)
from unittest.mock import AsyncMock, MagicMock

import anyio
from typing_extensions import TypedDict, override

from faststream.broker.message import gen_cor_id
from faststream.broker.utils import resolve_custom_func
from faststream.exceptions import WRONG_PUBLISH_ARGS, SetupError, SubscriberNotFound
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
from faststream.redis.schemas import INCORRECT_SETUP_MSG
from faststream.redis.subscriber.usecase import (
    ChannelSubscriber,
    LogicSubscriber,
    _ListHandlerMixin,
    _StreamHandlerMixin,
)
from faststream.testing.broker import TestBroker
from faststream.utils.functions import timeout_scope

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline

    from faststream.redis.publisher.asyncapi import AsyncAPIPublisher
    from faststream.types import AnyDict, SendableMessage

__all__ = ("TestRedisBroker",)


class TestRedisBroker(TestBroker[RedisBroker]):
    """A class to test Redis brokers."""

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: RedisBroker,
        publisher: "AsyncAPIPublisher",
    ) -> Tuple["LogicSubscriber", bool]:
        sub: Optional[LogicSubscriber] = None

        named_property = publisher.subscriber_property(name_only=True)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in broker._subscribers.values():  # pragma: no branch
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

    @staticmethod
    async def _fake_connect(  # type: ignore[override]
        broker: RedisBroker,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncMock:
        broker._producer = FakeProducer(broker)
        connection = MagicMock()

        pub_sub = AsyncMock()

        async def get_msg(*args: Any, timeout: float, **kwargs: Any) -> None:
            await anyio.sleep(timeout)
            return None

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
        message: "SendableMessage",
        *,
        channel: Optional[str] = None,
        list: Optional[str] = None,
        stream: Optional[str] = None,
        maxlen: Optional[int] = None,
        headers: Optional["AnyDict"] = None,
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        rpc: bool = False,
        rpc_timeout: Optional[float] = 30.0,
        raise_timeout: bool = False,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> Optional[Any]:
        if rpc and reply_to:
            raise WRONG_PUBLISH_ARGS

        correlation_id = correlation_id or gen_cor_id()

        body = build_message(
            message=message,
            reply_to=reply_to,
            correlation_id=correlation_id,
            headers=headers,
        )

        destination = _make_destionation_kwargs(channel, list, stream)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in self.broker._subscribers.values():  # pragma: no branch
            for visitor in visitors:
                if visited_ch := visitor.visit(**destination, sub=handler):
                    msg = visitor.get_message(
                        visited_ch,
                        body,
                        handler,  # type: ignore[arg-type]
                    )

                    with timeout_scope(rpc_timeout, raise_timeout):
                        response_msg = await self._execute_handler(msg, handler)
                        if rpc:
                            return await self._decoder(await self._parser(response_msg))

        return None

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        *,
        correlation_id: str,
        channel: Optional[str] = None,
        list: Optional[str] = None,
        stream: Optional[str] = None,
        maxlen: Optional[int] = None,
        headers: Optional["AnyDict"] = None,
        timeout: Optional[float] = 30.0,
    ) -> "PubSubMessage":
        correlation_id = correlation_id or gen_cor_id()

        body = build_message(
            message=message,
            correlation_id=correlation_id,
            headers=headers,
        )

        destination = _make_destionation_kwargs(channel, list, stream)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in self.broker._subscribers.values():  # pragma: no branch
            for visitor in visitors:
                if visited_ch := visitor.visit(**destination, sub=handler):
                    msg = visitor.get_message(
                        visited_ch,
                        body,
                        handler,  # type: ignore[arg-type]
                    )

                    with anyio.fail_after(timeout):
                        return await self._execute_handler(msg, handler)

        raise SubscriberNotFound

    async def publish_batch(
        self,
        *msgs: "SendableMessage",
        list: str,
        headers: Optional["AnyDict"] = None,
        correlation_id: Optional[str] = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> None:
        data_to_send = [
            build_message(
                m,
                correlation_id=correlation_id or gen_cor_id(),
                headers=headers,
            )
            for m in msgs
        ]

        visitor = ListVisitor()
        for handler in self.broker._subscribers.values():  # pragma: no branch
            if visitor.visit(list=list, sub=handler):
                casted_handler = cast("_ListHandlerMixin", handler)

                if casted_handler.list_sub.batch:
                    msg = visitor.get_message(list, data_to_send, casted_handler)

                    await self._execute_handler(msg, handler)

        return None

    async def _execute_handler(
        self, msg: Any, handler: "LogicSubscriber"
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
    data = RawMessage.encode(
        message=message,
        reply_to=reply_to,
        headers=headers,
        correlation_id=correlation_id,
    )
    return data


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
                )
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
                data=body if isinstance(body, List) else [body],
            )

        else:
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

        else:
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


def _make_destionation_kwargs(
    channel: Optional[str],
    list: Optional[str],
    stream: Optional[str],
) -> _DestinationKwargs:
    destination: _DestinationKwargs = {}
    if channel:
        destination["channel"] = channel
    if list:
        destination["list"] = list
    if stream:
        destination["stream"] = stream

    if len(destination) != 1:
        raise SetupError(INCORRECT_SETUP_MSG)

    return destination
