import re
import uuid
from collections.abc import AsyncGenerator, Generator, Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, asynccontextmanager, contextmanager
from dataclasses import dataclass
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Protocol,
    Union,
    cast,
    overload,
)
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import anyio
from typing_extensions import TypedDict, override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from faststream._internal.testing.broker import (
    EnterType,
    TestBroker,
    change_producer,
)
from faststream.exceptions import SetupError, SubscriberNotFound
from faststream.redis.broker.broker import RedisBroker
from faststream.redis.configs.state import RedisClusterConnectionState
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
    from faststream._internal.parser import CodecProto
    from faststream.redis.publisher.usecase import LogicPublisher
    from faststream.redis.subscriber.usecases.basic import LogicSubscriber
    from faststream.response import Response

__all__ = (
    "PEL",
    "TestRedisBroker",
)


@dataclass(kw_only=True)
class Entry:
    handler: "LogicSubscriber"
    msg: Any


PELKey = tuple[str | None, uuid.UUID, str | None]


class PEL:
    def __init__(self) -> None:
        self.entries: dict[PELKey, Entry] = {}
        self.put = MagicMock(wraps=self._put)
        self.remove = MagicMock(wraps=self._remove)

    def _remove(self, correlation_id: PELKey) -> None:
        self.entries.pop(correlation_id)

    def _put(
        self,
        msg: Any,
        handler: "LogicSubscriber",
        correlation_id: PELKey,
    ) -> None:
        self.entries.update({correlation_id: Entry(msg=msg, handler=handler)})

    def get_entry(self, correlation_id: PELKey) -> Entry | None:
        return self.entries.get(correlation_id)


class TestRedisBroker(TestBroker[RedisBroker, EnterType]):
    """A class to test Redis brokers."""

    @overload
    def __init__(
        self: "TestRedisBroker[RedisBroker]",
        broker: RedisBroker,
        /,
        *,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: "TestRedisBroker[tuple[RedisBroker, ...]]",
        *brokers: RedisBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    def __init__(
        self,
        *brokers: RedisBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
        pel: PEL | None = None,
    ) -> None:

        self.pel = pel or PEL()

        super().__init__(
            *brokers,
            with_real=with_real,
            connect_only=connect_only,
        )

    @asynccontextmanager
    async def _create_ctx(self) -> AsyncGenerator[list[RedisBroker], None]:
        with ExitStack() as cluster_stack:
            for broker in self.brokers:
                is_cluster = isinstance(
                    broker.config.broker_config.connection,
                    RedisClusterConnectionState,
                )
                if self.with_real and is_cluster:
                    with mock.patch.object(
                        broker,
                        "_connect",
                        wraps=partial(self._fake_connect, broker),
                    ):
                        await broker.connect()
                    cluster_stack.enter_context(self._patch_producer(broker))
            async with super()._create_ctx() as brokers:
                yield brokers

    @contextmanager
    def _patch_producer(self, broker: RedisBroker) -> Generator[None, None, None]:
        with ExitStack() as es:
            es.enter_context(
                change_producer(
                    broker.config.broker_config,
                    FakeProducer(broker, self.brokers, broker.config, self.pel),
                ),
            )

            for publisher in cast("list[LogicPublisher]", broker.publishers):
                es.enter_context(
                    change_producer(
                        publisher,
                        FakeProducer(broker, self.brokers, publisher.config, self.pel),
                    ),
                )

            yield

    def create_publisher_fake_subscriber(
        self,
        broker: RedisBroker,
        publisher: "LogicPublisher",
    ) -> tuple["LogicSubscriber", bool]:
        sub: LogicSubscriber | None = None

        named_property = publisher.subscriber_property(name_only=True)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())

        for handler in (
            s for b in self.brokers for s in b.subscribers
        ):  # pragma: no branch
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

        connection_state = broker.config.broker_config.connection
        connection_state._client = connection
        connection_state._sync_cluster = MagicMock()
        connection_state._thread_pool = ThreadPoolExecutor(max_workers=1)
        connection_state._connected = True
        return connection


class FakeProducer(RedisFastProducer):
    def __init__(
        self,
        broker: RedisBroker,
        brokers: Sequence[RedisBroker],
        config: ParserConfig,
        pel: PEL | None = None,
    ) -> None:
        self.broker = broker
        self.brokers = brokers
        self._fake_config = config

        default = RedisPubSubParser(config)

        self._parser = ParserComposition(
            broker._parser,
            default.parse_message,
        )
        self._decoder = ParserComposition(
            broker._decoder,
            default.decode_message,
        )
        self.codec = broker.config.broker_codec or DefaultCodec()
        self.pel = pel or PEL()

    @property
    def subscribers(self) -> "Iterable[LogicSubscriber]":
        return (cast("LogicSubscriber", s) for b in self.brokers for s in b.subscribers)

    @override
    def _build_child(self, **kwargs: Any) -> "FakeProducer":
        return FakeProducer(
            broker=self.broker,
            brokers=self.brokers,
            config=self._fake_config,
        )

    @override
    async def publish(self, cmd: "RedisPublishCommand") -> int | bytes:
        body = await build_message(
            message=cmd.body,
            reply_to=cmd.reply_to,
            correlation_id=cmd.correlation_id or self.broker.config.id_generator(),
            headers=cmd.headers,
            message_format=cmd.message_format,
            serializer=self.broker.config.fd_config._serializer,
            codec=self.codec,
        )
        destination = _make_destination_kwargs(cmd)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())
        session_id = uuid.uuid4()
        for visitor, visited_ch, handler in self._find_handlers(
            destination=destination,
            visitors=visitors,
            cmd=cmd,
            session_id=session_id,
        ):
            msg = visitor.get_message(
                visited_ch,
                body,
                handler,
            )
            self._put_pel(msg=msg, cmd=cmd, handler=handler, session_id=session_id)
            await self._execute_handler(msg, handler, session_id=session_id)

        return 0

    @override
    async def request(self, cmd: "RedisPublishCommand") -> "PubSubMessage":
        body = await build_message(
            message=cmd.body,
            correlation_id=cmd.correlation_id or self.broker.config.id_generator(),
            headers=cmd.headers,
            message_format=cmd.message_format,
            serializer=self.broker.config.fd_config._serializer,
            codec=self.codec,
        )

        destination = _make_destination_kwargs(cmd)
        visitors = (ChannelVisitor(), ListVisitor(), StreamVisitor())
        session_id = uuid.uuid4()

        for visitor, visited_ch, handler in self._find_handlers(
            destination=destination,
            visitors=visitors,
            cmd=cmd,
            session_id=session_id,
        ):
            msg = visitor.get_message(
                visited_ch,
                body,
                handler,
            )
            self._put_pel(msg=msg, cmd=cmd, handler=handler, session_id=session_id)
            with anyio.fail_after(cmd.timeout):
                return await self._execute_handler(msg, handler, session_id=session_id)

        raise SubscriberNotFound

    @override
    async def publish_batch(self, cmd: "RedisPublishCommand") -> int:
        data_to_send = [
            await build_message(
                m,
                correlation_id=cmd.correlation_id or self.broker.config.id_generator(),
                headers=cmd.headers,
                message_format=cmd.message_format,
                serializer=self.broker.config.fd_config._serializer,
                codec=self.codec,
            )
            for m in cmd.batch_bodies
        ]
        session_id = uuid.uuid4()

        for visitor, visited_ch, handler in self._find_handlers(
            {"list": cmd.destination},
            (ListVisitor(),),
            cmd=cmd,
            session_id=session_id,
        ):
            casted_handler = cast("_ListHandlerMixin", handler)

            if casted_handler.list_sub.batch:
                msg = visitor.get_message(
                    visited_ch,
                    data_to_send,
                    casted_handler,
                )
                self._put_pel(msg=msg, cmd=cmd, handler=handler, session_id=session_id)
                await self._execute_handler(msg, handler, session_id=session_id)

        return 0

    async def _execute_handler(
        self,
        msg: Any,
        handler: "LogicSubscriber",
        session_id: uuid.UUID,
    ) -> "PubSubMessage":
        result = await handler.process_message(msg)
        self._remove_pel(handler=handler, result=result, session_id=session_id)
        return PubSubMessage(
            type="message",
            data=await build_message(
                message=result.body,
                headers=result.headers,
                correlation_id=result.correlation_id or "",
                message_format=handler.config.message_format,
                serializer=self.broker.config.fd_config._serializer,
                codec=self.codec,
            ),
            channel="",
            pattern=None,
        )

    def _find_handlers(
        self,
        destination: "_DestinationKwargs",
        visitors: "Sequence[Visitor]",
        cmd: "RedisPublishCommand",
        session_id: uuid.UUID,
    ) -> "Iterator[tuple[Visitor, str, LogicSubscriber]]":
        published_groups: set[tuple[str, str]] = set()

        for handler in self.subscribers:  # pragma: no branch
            for visitor in visitors:
                visited_ch = visitor.visit(**destination, sub=handler)
                if visited_ch is None:
                    continue
                if not self._return_handlers(
                    handler=handler,
                    visited_ch=visited_ch,
                    published_groups=published_groups,
                    cmd=cmd,
                    session_id=session_id,
                ):
                    break
                yield visitor, visited_ch, handler
                break

    def _return_handlers(
        self,
        handler: "LogicSubscriber",
        visited_ch: str,
        published_groups: set[tuple[str, str]],
        cmd: "RedisPublishCommand",
        session_id: uuid.UUID,
    ) -> bool:
        if isinstance(handler, _StreamHandlerMixin) and handler.stream_sub.group:
            group_key = (visited_ch, handler.stream_sub.group)

            if self._handler_min_idle_time(handler) and self._check_pel(
                handler=handler,
                cmd=cmd,
                session_id=session_id,
            ):
                return True
            if group_key in published_groups:
                return False
            published_groups.add(group_key)
            return True
        return True

    def _handler_group(self, handler: "LogicSubscriber") -> str | None:
        if isinstance(handler, _StreamHandlerMixin):
            return handler.stream_sub.group
        return None

    def _handler_no_ack(self, handler: "LogicSubscriber") -> bool:
        return isinstance(handler, _StreamHandlerMixin) and handler.stream_sub.no_ack

    def _handler_min_idle_time(self, handler: "LogicSubscriber") -> int | None:
        if isinstance(handler, _StreamHandlerMixin):
            return handler.stream_sub.min_idle_time
        return None

    def _check_pel(
        self,
        handler: "LogicSubscriber",
        cmd: "RedisPublishCommand",
        session_id: uuid.UUID,
    ) -> Optional["Entry"]:
        return self.pel.get_entry(
            correlation_id=(
                cmd.correlation_id,
                session_id,
                self._handler_group(handler),
            )
        )

    def _put_pel(
        self,
        handler: "LogicSubscriber",
        msg: Any,
        cmd: "RedisPublishCommand",
        session_id: uuid.UUID,
    ) -> None:
        if not self._handler_no_ack(handler):
            self.pel.put(
                msg=msg,
                handler=handler,
                correlation_id=(
                    cmd.correlation_id,
                    session_id,
                    self._handler_group(handler),
                ),
            )

    def _remove_pel(
        self,
        result: "Response",
        handler: "LogicSubscriber",
        session_id: uuid.UUID,
    ) -> None:
        if result.correlation_id and not self._handler_no_ack(handler):
            self.pel.remove(
                correlation_id=(
                    result.correlation_id,
                    session_id,
                    self._handler_group(handler),
                )
            )


async def build_message(
    message: Union[Sequence["SendableMessage"], "SendableMessage"],
    *,
    correlation_id: str,
    message_format: type["MessageFormat"],
    reply_to: str = "",
    headers: dict[str, Any] | None = None,
    serializer: Optional["SerializerProto"] = None,
    codec: Optional["CodecProto"] = None,
) -> bytes:
    return await message_format.encode(
        message=message,
        reply_to=reply_to,
        headers=headers,
        correlation_id=correlation_id,
        serializer=serializer,
        codec=codec,
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
            # Real Redis reports the pattern it was psubscribed with; this keeps
            # reporting the template, as it did before the two were split apart.
            # Aligning it belongs with #2450, which gives the template a meaning.
            pattern=sub.channel.address.template.encode()
            if sub.channel.pattern
            else None,
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
