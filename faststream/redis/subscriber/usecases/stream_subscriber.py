import asyncio
import logging
import math
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, Optional, TypeAlias

import anyio
from redis.exceptions import ResponseError
from typing_extensions import TypedDict, override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.redis.exceptions import (
    StreamClaimUnsupportedError,
    StreamGroupNotFoundError,
)
from faststream.redis.message import (
    BatchStreamMessage,
    DefaultStreamMessage,
    RedisStreamMessage,
)
from faststream.redis.parser import (
    RedisBatchStreamParser,
    RedisStreamParser,
)

from .basic import CONSUME_ERROR_BACKOFF_SECONDS, LogicSubscriber

if TYPE_CHECKING:
    from anyio import Event

    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream.message import StreamMessage as BrokerStreamMessage
    from faststream.redis.message import _StreamMessage
    from faststream.redis.schemas import StreamSub
    from faststream.redis.subscriber.config import RedisSubscriberConfig


TopicName: TypeAlias = bytes
Offset: TypeAlias = bytes

# With `StreamSub.claim_min_idle_time` (XREADGROUP CLAIM), every entry carries
# two extra fields: idle time (ms) and previous-delivery count.
StreamEntry: TypeAlias = (
    tuple[Offset, dict[bytes, bytes]] | tuple[Offset, dict[bytes, bytes], int, int]
)

ReadResponse = tuple[
    tuple[
        TopicName,
        tuple[StreamEntry, ...],
    ],
    ...,
]
ReadCallable = Callable[[str], Awaitable[ReadResponse]]


class _ClaimKwargs(TypedDict, total=False):
    claim_min_idle_time: int


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
        self.read_id = self.last_id
        self.min_idle_time = config.stream_sub.min_idle_time
        self.claim_min_idle_time = config.stream_sub.claim_min_idle_time
        self.autoclaim_start_id = b"0-0"

    @property
    def stream_sub(self) -> "StreamSub":
        return self._stream_sub.add_prefix(self._outer_config.prefix)

    @property
    def _claim_kwargs(self) -> _ClaimKwargs:
        """`xreadgroup` kwargs enabling the XREADGROUP CLAIM option.

        Passed conditionally: redis-py older than 7.1.0 doesn't accept the
        `claim_min_idle_time` argument at all. The `type: ignore[misc]` at the
        call sites is needed because the types-redis stubs (4.6) predate the
        argument, while redis-py itself accepts it since 7.1.0.
        """
        if self.claim_min_idle_time is None:
            return {}
        return {"claim_min_idle_time": self.claim_min_idle_time}

    def _parse_stream_entry(
        self,
        entry: "StreamEntry",
    ) -> tuple[bytes, dict[bytes, bytes], tuple[int, int] | None]:
        """Split a stream entry into id, payload and optional CLAIM metadata.

        With `claim_min_idle_time` enabled, Redis extends every entry with two
        extra fields: milliseconds since the last delivery and the number of
        *previous* deliveries (0 for new messages, one less than XPENDING's
        `times_delivered`). An entry missing the metadata means Redis ignored
        the CLAIM option (it does so when reading with an explicit id), so the
        requested recovery behavior is off - fail loudly instead of degrading
        silently.
        """
        message_id, data, *claim_meta = entry

        if self.claim_min_idle_time is None:
            return message_id, data, None

        if len(claim_meta) == 2:
            return message_id, data, (claim_meta[0], claim_meta[1])

        msg = (
            "Stream entry is missing XREADGROUP CLAIM metadata. Redis ignores "
            "the CLAIM option when reading with an explicit id, so the "
            "requested `claim_min_idle_time` behavior is disabled."
        )
        raise ValueError(msg)

    def _attach_claim_metadata(
        self,
        message: "_StreamMessage",
        claim_metas: "Sequence[tuple[int, int] | None]",
    ) -> None:
        """Expose per-entry CLAIM metadata via the raw message.

        Set only when `claim_min_idle_time` is enabled, so messages of regular
        subscribers stay unchanged. `delivery_counts` counts *previous*
        deliveries (0 = new message) - one less than XPENDING's
        `times_delivered`.
        """
        if self.claim_min_idle_time is None:
            return

        # `_parse_stream_entry` guarantees the metadata when claiming is
        # enabled; the None-filter below only narrows the type.
        message["idle_times"] = [m[0] for m in claim_metas if m is not None]
        message["delivery_counts"] = [m[1] for m in claim_metas if m is not None]

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

                if (
                    self.claim_min_idle_time is not None
                    and "syntax error" in str(e).lower()
                ):
                    msg = (
                        "Redis server rejected the XREADGROUP CLAIM option for "
                        f"stream `{self.stream_sub.name}`. `claim_min_idle_time` "
                        "requires Redis server 8.4+. Stopping subscriber."
                    )
                    raise StreamClaimUnsupportedError(msg) from e

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
                    return client.xreadgroup(  # type: ignore[misc]
                        groupname=stream.group,
                        consumername=stream.consumer,
                        streams={stream.name: self.read_id},
                        count=stream.max_records,
                        block=stream.polling_interval,
                        noack=stream.no_ack,
                        **self._claim_kwargs,
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
        claim_meta: tuple[int, int] | None = None

        if self.stream_sub.group and self.stream_sub.consumer:
            if self.min_idle_time is None:
                stream_message = await self._client.xreadgroup(  # type: ignore[misc]
                    groupname=self.stream_sub.group,
                    consumername=self.stream_sub.consumer,
                    streams={self.stream_sub.name: self.read_id},
                    block=math.ceil(timeout * 1000),
                    count=1,
                    **self._claim_kwargs,
                )
                if not stream_message:
                    return None

                ((stream_name, (entry,)),) = stream_message
                message_id, raw_message, claim_meta = self._parse_stream_entry(entry)
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
        self._attach_claim_metadata(redis_incoming_msg, [claim_meta])

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
    async def __aiter__(self) -> AsyncIterator["RedisStreamMessage"]:
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        timeout = 5

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        claim_meta: tuple[int, int] | None

        while True:
            claim_meta = None

            if self.stream_sub.group and self.stream_sub.consumer:
                if self.min_idle_time is None:
                    stream_message = await self._client.xreadgroup(  # type: ignore[misc]
                        groupname=self.stream_sub.group,
                        consumername=self.stream_sub.consumer,
                        streams={self.stream_sub.name: self.read_id},
                        block=math.ceil(timeout * 1000),
                        count=1,
                        **self._claim_kwargs,
                    )
                    if not stream_message:
                        continue

                    ((stream_name, (entry,)),) = stream_message
                    message_id, raw_message, claim_meta = self._parse_stream_entry(
                        entry,
                    )
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
            self._attach_claim_metadata(redis_incoming_msg, [claim_meta])

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
        read: ReadCallable,
    ) -> None:
        for stream_name, msgs in await read(self.last_id):
            if msgs:
                self.last_id = msgs[-1][0].decode()

                for entry in msgs:
                    message_id, raw_msg, claim_meta = self._parse_stream_entry(entry)

                    msg = DefaultStreamMessage(
                        type="stream",
                        channel=stream_name.decode(),
                        message_ids=[message_id],
                        data=raw_msg,
                    )
                    self._attach_claim_metadata(msg, [claim_meta])

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
        read: ReadCallable,
    ) -> None:
        for stream_name, msgs in await read(self.last_id):
            if msgs:
                self.last_id = msgs[-1][0].decode()

                data: list[dict[bytes, bytes]] = []
                ids: list[bytes] = []
                claim_metas: list[tuple[int, int] | None] = []
                for entry in msgs:
                    message_id, i, claim_meta = self._parse_stream_entry(entry)
                    data.append(i)
                    ids.append(message_id)
                    claim_metas.append(claim_meta)

                msg = BatchStreamMessage(
                    type="bstream",
                    channel=stream_name.decode(),
                    data=data,
                    message_ids=ids,
                )
                self._attach_claim_metadata(msg, claim_metas)

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
