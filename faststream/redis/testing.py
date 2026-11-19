import uuid
from collections.abc import AsyncGenerator, Generator, Iterator, Sequence
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


class TestRedisBroker(
    TestBroker[RedisBroker, EnterType],
    broker=RedisBroker,
):
    """A class to test Redis brokers."""

    @overload
    def __init__(
        self: "TestRedisBroker[RedisBroker]",
        broker: RedisBroker,
        /,
        *,
        with_real: bool = False,
        connect_only: bool | None = None,
        pel: PEL | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: "TestRedisBroker[tuple[RedisBroker, ...]]",
        *brokers: RedisBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
        pel: PEL | None = None,
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
                        _ = await broker.connect()
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
        channel_encoding = broker.config.broker_config.connection.encoding

        for b in self.brokers:  # pragma: no branch
            visitors = (
                ChannelVisitor(
                    channel_encoding,
                    b.config.broker_config.connection.encoding,
                ),
                ListVisitor(),
                StreamVisitor(),
            )
            for handler in b.subscribers:
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
        session_id = uuid.uuid4()
        for visitor, visited_ch, handler in self._find_handlers(
            destination=destination,
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
        session_id = uuid.uuid4()

        for visitor, visited_ch, handler in self._find_handlers(
            destination=destination,
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
            cmd=cmd,
            session_id=session_id,
        ):
            casted_handler = cast("_ListHandlerMixin", handler)

            messages = [data_to_send] if casted_handler.list_sub.batch else data_to_send
            for body in messages:
                msg = visitor.get_message(
                    visited_ch,
                    body,
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
        cmd: "RedisPublishCommand",
        session_id: uuid.UUID,
    ) -> "Iterator[tuple[Visitor, str, LogicSubscriber]]":
        published_groups: set[tuple[str, str]] = set()
        channel_encoding = self.broker.config.broker_config.connection.encoding

        for b in self.brokers:  # pragma: no branch
            visitors = (
                ChannelVisitor(
                    channel_encoding,
                    b.config.broker_config.connection.encoding,
                ),
                ListVisitor(),
                StreamVisitor(),
            )
            for handler in b.subscribers:
                handler = cast("LogicSubscriber", handler)
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

            if _handler_min_idle_time(handler) and self._check_pel(
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
                _handler_group(handler),
            )
        )

    def _put_pel(
        self,
        handler: "LogicSubscriber",
        msg: Any,
        cmd: "RedisPublishCommand",
        session_id: uuid.UUID,
    ) -> None:
        if not _handler_no_ack(handler):
            self.pel.put(
                msg=msg,
                handler=handler,
                correlation_id=(
                    cmd.correlation_id,
                    session_id,
                    _handler_group(handler),
                ),
            )

    def _remove_pel(
        self,
        result: "Response",
        handler: "LogicSubscriber",
        session_id: uuid.UUID,
    ) -> None:
        if result.correlation_id and not _handler_no_ack(handler):
            self.pel.remove(
                correlation_id=(
                    result.correlation_id,
                    session_id,
                    _handler_group(handler),
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


def _signed_byte(value: int) -> int:
    """Read a pattern byte as C's signed `char` on x86-64 and Windows."""
    return value - 256 if value > 127 else value


def _redis_glob_match_class(
    pattern: bytes,
    pattern_index: int,
    subject_byte: int,
) -> tuple[bool, int]:
    """Match one character class, returning the result and its end index."""
    pattern_end = len(pattern)
    close_bracket = ord("]")
    backslash = ord("\\")
    negation = ord("^")
    range_separator = ord("-")

    pattern_index += 1  # skip '['
    negate = pattern_index < pattern_end and pattern[pattern_index] == negation
    if negate:
        pattern_index += 1

    matched = False
    while True:
        if (
            pattern_index < pattern_end
            and pattern[pattern_index] == backslash
            and pattern_end - pattern_index >= 2
        ):
            pattern_index += 1
            if pattern[pattern_index] == subject_byte:
                matched = True
        elif pattern_index < pattern_end and pattern[pattern_index] == close_bracket:
            break
        elif pattern_index >= pattern_end:
            # An unterminated class ends with the pattern.
            pattern_index -= 1
            break
        elif (
            pattern_end - pattern_index >= 3
            and pattern[pattern_index + 1] == range_separator
        ):
            # Redis reads the bounds as `char`. Sign-extend them to mirror the
            # signed-char builds (x86-64, Windows); unsigned-char platforms
            # differ only for non-ASCII bounds.
            start = _signed_byte(pattern[pattern_index])
            end = _signed_byte(pattern[pattern_index + 2])
            subject = _signed_byte(subject_byte)
            if start > end:
                start, end = end, start
            if start <= subject <= end:
                matched = True
            pattern_index += 2
        elif pattern[pattern_index] == subject_byte:
            matched = True
        pattern_index += 1

    return (not matched if negate else matched), pattern_index


def _redis_glob_match(pattern: bytes, channel: bytes) -> bool:
    """Match the whole channel, mirroring `stringmatchlen_impl`.

    The C recursion for `*` is kept as an explicit stack of active star loops:
    Python's own recursion limit is lower than Redis' nesting guard.
    """
    pattern_end = len(pattern)
    channel_end = len(channel)
    star = ord("*")
    question = ord("?")
    open_bracket = ord("[")
    backslash = ord("\\")

    pattern_index = 0
    subject_index = 0
    # The index after each active `*` and the next subject position to try.
    # Redis aborts the whole match once one such loop runs out of positions,
    # so only the innermost loop ever has to advance.
    star_loops: list[tuple[int, int]] = []

    while True:
        while pattern_index < pattern_end and subject_index < channel_end:
            byte = pattern[pattern_index]

            if byte == star:
                while (
                    pattern_index + 1 < pattern_end and pattern[pattern_index + 1] == star
                ):
                    pattern_index += 1
                if pattern_end - pattern_index == 1:
                    return True
                if len(star_loops) >= 1000:
                    # Redis aborts once matching nests deeper than 1000.
                    return False
                star_loops.append((pattern_index + 1, subject_index))
                pattern_index += 1
                continue

            if byte == question:
                subject_index += 1
            elif byte == open_bracket:
                matched, pattern_index = _redis_glob_match_class(
                    pattern, pattern_index, channel[subject_index]
                )
                if not matched:
                    break
                subject_index += 1
            elif byte == backslash:
                if pattern_end - pattern_index >= 2:
                    pattern_index += 1
                if pattern[pattern_index] != channel[subject_index]:
                    break
                subject_index += 1
            elif byte != channel[subject_index]:
                break
            else:
                subject_index += 1

            pattern_index += 1
            if subject_index == channel_end:
                while pattern_index < pattern_end and pattern[pattern_index] == star:
                    pattern_index += 1
                break

        if pattern_index == pattern_end and subject_index == channel_end:
            return True

        if not star_loops:
            return False

        pattern_index, subject_index = star_loops[-1]
        subject_index += 1
        if subject_index >= channel_end:
            return False
        star_loops[-1] = (pattern_index, subject_index)


def _redis_glob_matches(
    pattern: str,
    channel: str,
    pattern_encoding: str = "utf-8",
    channel_encoding: str = "utf-8",
) -> bool:
    """Match a channel against a Redis PSUBSCRIBE glob pattern.

    A port of `stringmatchlen_impl` from `redis/src/util.c`: matching is done
    over each side's configured encoding bytes. Redis encodes the subscriber's
    pattern and the publisher's channel independently, so `?` and character
    classes consume one pattern byte and one channel byte, not code points.
    """
    return _redis_glob_match(
        pattern.encode(pattern_encoding),
        channel.encode(channel_encoding),
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
    def __init__(
        self,
        channel_encoding: str = "utf-8",
        pattern_encoding: str = "utf-8",
    ) -> None:
        self.channel_encoding = channel_encoding
        self.pattern_encoding = pattern_encoding

    @override
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

        if sub_channel.pattern:
            # A pattern is a Redis PSUBSCRIBE glob, not a broker address
            # template: it must not fall back to literal equality.
            if _redis_glob_matches(
                sub_channel.name,
                channel,
                self.pattern_encoding,
                self.channel_encoding,
            ):
                return channel
            return None

        if channel == sub_channel.name:
            return channel

        return None

    @override
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
    @override
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

    @override
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
    @override
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

    @override
    def get_message(  # type: ignore[override]
        self,
        channel: str,
        body: Any,
        sub: "_StreamHandlerMixin",
    ) -> Any:
        message: BatchStreamMessage | DefaultStreamMessage
        if sub.stream_sub.batch:
            message = BatchStreamMessage(
                type="bstream",
                channel=channel,
                data=[{bDATA_KEY: body}],
                message_ids=[],
            )
        else:
            message = DefaultStreamMessage(
                type="stream",
                channel=channel,
                data={bDATA_KEY: body},
                message_ids=[],
            )

        if sub.stream_sub.claim_min_idle_time is not None:
            # The in-memory broker has no claiming: every delivery is a new
            # message, so expose the new-message metadata values
            message["idle_times"] = [0]
            message["delivery_counts"] = [0]

        return message


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


def _handler_group(handler: "LogicSubscriber") -> str | None:
    if isinstance(handler, _StreamHandlerMixin):
        return handler.stream_sub.group
    return None


def _handler_no_ack(handler: "LogicSubscriber") -> bool:
    return isinstance(handler, _StreamHandlerMixin) and handler.stream_sub.no_ack


def _handler_min_idle_time(handler: "LogicSubscriber") -> int | None:
    if isinstance(handler, _StreamHandlerMixin):
        return handler.stream_sub.min_idle_time
    return None
