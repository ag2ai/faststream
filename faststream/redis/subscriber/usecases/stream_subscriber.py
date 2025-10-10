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

RedisStreamReadResponse: TypeAlias = tuple[
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

RedisStreamReader: TypeAlias = Callable[[str], Awaitable[RedisStreamReadResponse]]


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
        self.read_pending = False

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
        if self.tasks:
            return

        client = self._client

        self.extra_watcher_options.update(
            redis=client,
            group=self.stream_sub.group,
        )

        stream = self.stream_sub

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

        await super().start(self._create_reader(stream))

    def _create_reader(self, stream: "StreamSub") -> RedisStreamReader:
        if stream.group and stream.consumer:

            async def read_pending_or_next(_: str) -> RedisStreamReadResponse:
                if self.read_pending:
                    response = await self._read_stream("0", stream=stream)
                    if response and response[0][1]:
                        return response
                return await self._read_stream(stream.last_id, stream=stream)

            return read_pending_or_next
        return self._read_stream

    def _read_stream(
        self, last_id: str, *, stream: Optional["StreamSub"] = None
    ) -> Awaitable[RedisStreamReadResponse]:
        client = self._client
        stream = stream or self.stream_sub

        if stream.group and stream.consumer:
            return client.xreadgroup(
                groupname=stream.group,
                consumername=stream.consumer,
                streams={stream.name: last_id},
                count=stream.max_records,
                block=stream.polling_interval,
                noack=stream.no_ack,
            )

        return client.xread(
            {stream.name: last_id},
            block=stream.polling_interval,
            count=stream.max_records,
        )

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "RedisStreamMessage | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

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

        msg: RedisStreamMessage = await process_msg(  # type: ignore[assignment]
            msg=redis_incoming_msg,
            middlewares=(
                m(redis_incoming_msg, context=context) for m in self._broker_middlewares
            ),
            parser=self._parser,
            decoder=self._decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["RedisStreamMessage"]:  # type: ignore[override]
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        timeout = 5
        while True:
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

            context = self._outer_config.fd_config.context

            msg: RedisStreamMessage = await process_msg(  # type: ignore[assignment]
                msg=redis_incoming_msg,
                middlewares=(
                    m(redis_incoming_msg, context=context)
                    for m in self._broker_middlewares
                ),
                parser=self._parser,
                decoder=self._decoder,
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

    async def _get_msgs(self, read: RedisStreamReader) -> None:
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

    async def _get_msgs(self, read: RedisStreamReader) -> None:
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
