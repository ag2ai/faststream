import asyncio
import math
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any, Optional, TypeAlias

from redis.exceptions import ResponseError
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.redis.message import (
    BatchStreamMessage,
    DefaultStreamMessage,
    RedisStreamMessage,
)
from faststream.redis.parser import (
    RedisBatchStreamParser,
    RedisStreamParser,
)

from .basic import LogicSubscriber

if TYPE_CHECKING:
    from anyio import Event

    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream.message import StreamMessage as BrokerStreamMessage
    from faststream.redis.schemas import StreamSub
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
    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(config, specification, calls)

        assert config.stream_sub
        self._stream_sub = config.stream_sub
        self.last_id = config.stream_sub.last_id
        self.min_idle_time = config.stream_sub.min_idle_time
        self.autoclaim_start_id = b"0-0"

    @property
    def stream_sub(self) -> "StreamSub":
        return self._stream_sub.add_prefix(self._outer_config.prefix)

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
        await super()._consume(*args, start_signal=start_signal)

    @override
    async def start(self) -> None:
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
                    mkstream=True,
                )
            except ResponseError as e:
                if "already exists" not in str(e):
                    raise

            if stream.min_idle_time is None:

                def read(
                    _: str,
                ) -> Awaitable[ReadResponse]:
                    return client.xreadgroup(
                        groupname=stream.group,
                        consumername=stream.consumer,
                        streams={stream.name: stream.last_id},
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
                    (next_id, messages, _) = stream_message

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
                (next_id, messages, _) = stream_message
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
                    (next_id, messages, _) = stream_message
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
