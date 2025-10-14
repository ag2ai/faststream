import math
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Optional, TypeAlias

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
    from redis.asyncio import Redis

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


@dataclass(kw_only=True, repr=False)
class RedisStreamReader:
    stream_sub: "StreamSub"
    resumable: Literal[False] = False

    _last_id: str | None = field(default=None, init=False)
    _client: Optional["Redis[bytes]"] = field(default=None, init=False)

    async def __call__(self, last_id: str | None = None) -> RedisStreamReadResponse:
        last_id = last_id or self._last_id or self.stream_sub.last_id

        response = await self.read(last_id)
        if response and response[0][1]:
            self._last_id = response[0][1][-1][0].decode()
            return response

        return response

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self.stream_sub.name}]"

    @property
    def client(self) -> "Redis[bytes]":
        assert self._client, f"{self} has not been configured"
        return self._client

    async def configure(self, client: "Redis[bytes]", prefix: str) -> None:
        self._client = client
        self._last_id = self.stream_sub.last_id

        self.stream_sub = self.stream_sub.add_prefix(prefix)

    def read(self, last_id: str) -> Awaitable[RedisStreamReadResponse]:
        stream = self.stream_sub

        return self.client.xread(
            {stream.name: last_id},
            block=stream.polling_interval,
            count=stream.max_records or 1,
        )


@dataclass(kw_only=True, repr=False)
class RedisGroupReader(RedisStreamReader):
    async def __call__(self, last_id: str | None = None) -> RedisStreamReadResponse:
        last_id = last_id or self.stream_sub.last_id

        response = await self.read(last_id)
        if response and response[0][1]:
            self._last_id = response[0][1][-1][0].decode()
            return response

        return response

    @override
    async def configure(self, client: "Redis[bytes]", prefix: str) -> None:
        await super().configure(client, prefix)

        stream = self.stream_sub

        assert stream.group, f"{self} requires a stream with group"
        assert stream.consumer, f"{self} requires a stream with consumer"

        group_create_id = "$" if stream.last_id == ">" else stream.last_id
        try:
            await self.client.xgroup_create(
                name=stream.name,
                id=group_create_id,
                groupname=stream.group,
                mkstream=True,
            )
        except ResponseError as e:
            if "already exists" not in str(e):
                raise

    def read(self, last_id: str) -> Awaitable[RedisStreamReadResponse]:
        stream = self.stream_sub

        assert stream.group, f"{self} requires a stream with group"
        assert stream.consumer, f"{self} requires a stream with consumer"

        return self.client.xreadgroup(
            groupname=stream.group,
            consumername=stream.consumer,
            streams={stream.name: last_id},
            count=stream.max_records or 1,
            block=stream.polling_interval,
            noack=stream.no_ack,
        )


@dataclass(kw_only=True, repr=False)
class RedisConsumerReader(RedisGroupReader):
    resumable: bool = True  # type: ignore[assignment] # redefinition
    _resume_from: str | None = field(default=None, init=False)

    @override
    async def configure(self, client: "Redis[bytes]", prefix: str) -> None:
        await super().configure(client, prefix)

        if self.stream_sub.last_id != ">":
            self.resume_from(self.stream_sub.last_id)
            self.stream_sub.last_id = ">"

    def resume_from(self, last_id: str, *, resumable: bool | None = None) -> None:
        self._resume_from = last_id
        if resumable is not None:
            self.resumable = resumable

    @override
    async def __call__(self, last_id: str | None = None) -> RedisStreamReadResponse:
        if last_id:
            self._resume_from = last_id

        resume_from = self._resume_from
        resumable = self.resumable

        if resume_from in {">", None}:
            resumable = False

        response = await super().__call__(resume_from)
        if response and response[0][1]:
            return response

        if resumable:
            self._resume_from = None
            return await super().__call__(">")

        return response


class _StreamHandlerMixin(LogicSubscriber):
    reader: RedisStreamReader | RedisGroupReader | RedisConsumerReader

    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        super().__init__(config, specification, calls)

        assert config.stream_sub
        stream = self._stream_sub = config.stream_sub

        if stream.consumer:
            self.reader = RedisConsumerReader(stream_sub=stream)
        else:
            self.reader = RedisStreamReader(stream_sub=stream)

        self.last_id = config.stream_sub.last_id

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

        await self.reader.configure(client=client, prefix=self._outer_config.prefix)

        await super().start(self.reader)

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
        for stream_name, msgs in await read():
            if msgs:
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
        for stream_name, msgs in await read():
            if msgs:
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
