import asyncio
import logging
import math
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeAlias

import anyio
from redis.exceptions import ResponseError
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.utils.path import Address
from faststream.redis.address import AddressRead
from faststream.redis.exceptions import StreamGroupNotFoundError
from faststream.redis.message import (
    BatchStreamMessage,
    DefaultStreamMessage,
    RedisStreamMessage,
)
from faststream.redis.parser import (
    RedisBatchStreamParser,
    RedisStreamParser,
)
from faststream.redis.schemas import StreamSub

from .basic import CONSUME_ERROR_BACKOFF_SECONDS, LogicSubscriber

if TYPE_CHECKING:
    from anyio import Event

    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream.message import StreamMessage as BrokerStreamMessage
    from faststream.redis.subscriber.config import RedisSubscriberConfig


TopicName: TypeAlias = bytes
Offset: TypeAlias = bytes

ReadResponse = tuple[
    tuple[
        TopicName,
        tuple[
            tuple[
                Offset,
                dict[bytes, bytes],
            ],
            ...,
        ],
    ],
    ...,
]
ReadCallable = Callable[[str], Awaitable[ReadResponse]]


class _StreamHandlerMixin(LogicSubscriber):
    #: Whether this Subscriber reads the stream in batches. Read while it is
    #: being constructed — it chooses the class — so a Config value cannot
    #: change it, and neither can the consumer group, which picks the
    #: acknowledgement policy the same way.
    batch: ClassVar[bool] = False

    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(config, specification, calls)

        assert config.stream_sub is not None
        self._stream_sub = AddressRead(
            config.stream_sub,
            StreamSub,
            built_as={
                "batch": self.batch,
                "group": None,
                "consumer": None,
                "no_ack": False,
            },
        )

        # Where in the stream to read from next. Left unset until asked for,
        # because its starting point comes out of the `StreamSub` — which is
        # not built until the address is known in full.
        self._last_id: str | None = None
        self._read_id: str | None = None

        self.autoclaim_start_id = b"0-0"

    @override
    def _invalidate(self) -> None:
        super()._invalidate()
        self._stream_sub.reset()

    @property
    def stream_sub(self) -> "StreamSub":
        """The stream this Subscriber reads, built on first read.

        The Router prefix is composed and any Config value resolved by then,
        which is the first moment the stream is known in full.
        """
        return self._stream_sub.read(self._outer_config)

    @override
    def subscription_addresses(self) -> Iterable["Address"]:
        """The stream this Subscriber reads, and it is never a template.

        Redis matches a pattern on a channel only — a stream is read by the
        name given, verbatim. So a `{param}` in one is a character like any
        other, and `Address.literal` is what says so: a `Path()` parameter
        naming it is refused at `connect()` rather than going unfilled for
        every message.
        """
        yield Address.literal(
            self.stream_sub.name,
            self._stream_sub.config_key(self._outer_config),
        )

    @property
    def min_idle_time(self) -> int | None:
        return self.stream_sub.min_idle_time

    @property
    def last_id(self) -> str:
        """The id this Subscriber has read up to, starting where it was told to."""
        if self._last_id is None:
            self._last_id = self.stream_sub.last_id

        return self._last_id

    @last_id.setter
    def last_id(self, value: str) -> None:
        self._last_id = value

    @property
    def read_id(self) -> str:
        """The id handed to Redis on the next read."""
        if self._read_id is None:
            self._read_id = self.last_id

        return self._read_id

    @read_id.setter
    def read_id(self, value: str) -> None:
        self._read_id = value

    def get_log_context(
        self,
        message: Optional["BrokerStreamMessage[Any]"],
    ) -> dict[str, str]:
        return self.build_log_context(
            message=message,
            channel=self.stream_sub.name,
        )

    @override
    async def _consume(self, *args: Any, start_signal: "Event") -> None:
        if await self._client.ping():
            start_signal.set()

        while self.running:
            try:
                await self._get_msgs(*args)

            except ResponseError as e:  # noqa: PERF203
                if "NOGROUP" in str(e):
                    msg = (
                        f"Consumer group `{self.stream_sub.group}` for stream "
                        f"`{self.stream_sub.name}` no longer exists. "
                        "The stream was likely deleted or flushed. "
                        "Stopping subscriber — restart the application to recreate the group."
                    )
                    raise StreamGroupNotFoundError(msg) from e
                raise

            except Exception as e:
                self._log(
                    log_level=logging.ERROR,
                    message="Message fetch error",
                    exc_info=e,
                )
                await anyio.sleep(CONSUME_ERROR_BACKOFF_SECONDS)

            finally:
                if not start_signal.is_set():
                    start_signal.set()

    @override
    async def start(self) -> None:
        # Ahead of the group declaration: this `start()` reaches the base one,
        # where Preparation is otherwise driven, only after it has declared.
        self.prepare()

        client = self._client

        self.extra_watcher_options.update(
            redis=client,
            group=self.stream_sub.group,
        )

        stream = self.stream_sub

        read: ReadCallable

        if stream.group and stream.consumer:
            group_create_id = "$" if self.last_id == ">" else self.last_id
            try:
                await client.xgroup_create(
                    name=stream.name,
                    id=group_create_id,
                    groupname=stream.group,
                    mkstream=stream.declare,
                )
            except ResponseError as e:
                if "already exists" not in str(e):
                    raise
            else:
                self.read_id = ">"

            self.last_id = self.read_id

            if stream.min_idle_time is None:

                def read(
                    _: str,
                ) -> Awaitable[ReadResponse]:
                    return client.xreadgroup(
                        groupname=stream.group,
                        consumername=stream.consumer,
                        streams={stream.name: self.read_id},
                        count=stream.max_records,
                        block=stream.polling_interval,
                        noack=stream.no_ack,
                    )

            else:

                async def read(_: str) -> ReadResponse:
                    stream_message = await client.xautoclaim(
                        name=self.stream_sub.name,
                        groupname=self.stream_sub.group,
                        consumername=self.stream_sub.consumer,
                        min_idle_time=self.min_idle_time,
                        start_id=self.autoclaim_start_id,
                        count=1,
                    )
                    stream_name = self.stream_sub.name.encode()
                    (next_id, messages, *_) = stream_message

                    # Update start_id for next call
                    self.autoclaim_start_id = next_id

                    if next_id == b"0-0" and not messages:
                        await asyncio.sleep(stream.polling_interval / 1000)  # ms to s
                        return ()

                    return ((stream_name, messages),)

        else:

            def read(
                last_id: str,
            ) -> Awaitable[ReadResponse]:
                return client.xread(
                    {stream.name: last_id},
                    block=stream.polling_interval,
                    count=stream.max_records,
                )

        await super().start(read)

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "RedisStreamMessage | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )
        if self.stream_sub.group and self.stream_sub.consumer:
            if self.min_idle_time is None:
                stream_message = await self._client.xreadgroup(
                    groupname=self.stream_sub.group,
                    consumername=self.stream_sub.consumer,
                    streams={self.stream_sub.name: self.last_id},
                    block=math.ceil(timeout * 1000),
                    count=1,
                )
                if not stream_message:
                    return None

                ((stream_name, ((message_id, raw_message),)),) = stream_message
            else:
                stream_message = await self._client.xautoclaim(
                    name=self.stream_sub.name,
                    groupname=self.stream_sub.group,
                    consumername=self.stream_sub.consumer,
                    min_idle_time=self.min_idle_time,
                    start_id=self.autoclaim_start_id,
                    count=1,
                )
                (next_id, messages, *_) = stream_message
                # Update start_id for next call
                self.autoclaim_start_id = next_id
                if not messages:
                    return None
                stream_name = self.stream_sub.name.encode()
                ((message_id, raw_message),) = messages
        else:
            stream_message = await self._client.xread(
                {self.stream_sub.name: self.last_id},
                block=math.ceil(timeout * 1000),
                count=1,
            )
            if not stream_message:
                return None

            ((stream_name, ((message_id, raw_message),)),) = stream_message

        self.last_id = message_id.decode()

        redis_incoming_msg = DefaultStreamMessage(
            type="stream",
            channel=stream_name.decode(),
            message_ids=[message_id],
            data=raw_message,
        )

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        msg: RedisStreamMessage = await process_msg(  # type: ignore[assignment]
            msg=redis_incoming_msg,
            middlewares=(
                m(redis_incoming_msg, context=context) for m in self._broker_middlewares
            ),
            parser=async_parser,
            decoder=async_decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["RedisStreamMessage"]:  # type: ignore[override]
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        timeout = 5

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        while True:
            if self.stream_sub.group and self.stream_sub.consumer:
                if self.min_idle_time is None:
                    stream_message = await self._client.xreadgroup(
                        groupname=self.stream_sub.group,
                        consumername=self.stream_sub.consumer,
                        streams={self.stream_sub.name: self.last_id},
                        block=math.ceil(timeout * 1000),
                        count=1,
                    )
                    if not stream_message:
                        continue

                    ((stream_name, ((message_id, raw_message),)),) = stream_message
                else:
                    stream_message = await self._client.xautoclaim(
                        name=self.stream_sub.name,
                        groupname=self.stream_sub.group,
                        consumername=self.stream_sub.consumer,
                        min_idle_time=self.min_idle_time,
                        start_id=self.autoclaim_start_id,
                        count=1,
                    )
                    (next_id, messages, *_) = stream_message
                    # Update start_id for next call
                    self.autoclaim_start_id = next_id
                    if not messages:
                        continue
                    stream_name = self.stream_sub.name.encode()
                    ((message_id, raw_message),) = messages
            else:
                stream_message = await self._client.xread(
                    {self.stream_sub.name: self.last_id},
                    block=math.ceil(timeout * 1000),
                    count=1,
                )
                if not stream_message:
                    continue

                ((stream_name, ((message_id, raw_message),)),) = stream_message

            self.last_id = message_id.decode()

            redis_incoming_msg = DefaultStreamMessage(
                type="stream",
                channel=stream_name.decode(),
                message_ids=[message_id],
                data=raw_message,
            )

            msg: RedisStreamMessage = await process_msg(  # type: ignore[assignment]
                msg=redis_incoming_msg,
                middlewares=(
                    m(redis_incoming_msg, context=context)
                    for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            )
            yield msg


class StreamSubscriber(_StreamHandlerMixin):
    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        parser = RedisStreamParser(config)
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(config, specification, calls)

    async def _get_msgs(
        self,
        read: Callable[
            [str],
            Awaitable[
                tuple[
                    tuple[
                        TopicName,
                        tuple[
                            tuple[
                                Offset,
                                dict[bytes, bytes],
                            ],
                            ...,
                        ],
                    ],
                    ...,
                ],
            ],
        ],
    ) -> None:
        for stream_name, msgs in await read(self.last_id):
            if msgs:
                self.last_id = msgs[-1][0].decode()

                for message_id, raw_msg in msgs:
                    msg = DefaultStreamMessage(
                        type="stream",
                        channel=stream_name.decode(),
                        message_ids=[message_id],
                        data=raw_msg,
                    )

                    await self.consume_one(msg)


class StreamBatchSubscriber(_StreamHandlerMixin):
    batch: ClassVar[bool] = True

    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        parser = RedisBatchStreamParser(config)
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(config, specification, calls)

    async def _get_msgs(
        self,
        read: Callable[
            [str],
            Awaitable[
                tuple[tuple[bytes, tuple[tuple[bytes, dict[bytes, bytes]], ...]], ...],
            ],
        ],
    ) -> None:
        for stream_name, msgs in await read(self.last_id):
            if msgs:
                self.last_id = msgs[-1][0].decode()

                data: list[dict[bytes, bytes]] = []
                ids: list[bytes] = []
                for message_id, i in msgs:
                    data.append(i)
                    ids.append(message_id)

                msg = BatchStreamMessage(
                    type="bstream",
                    channel=stream_name.decode(),
                    data=data,
                    message_ids=ids,
                )

                await self.consume_one(msg)


class StreamConcurrentSubscriber(
    ConcurrentMixin["BrokerStreamMessage[Any]"],
    StreamSubscriber,
):
    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "BrokerStreamMessage[Any]") -> None:
        await self._put_msg(msg)
