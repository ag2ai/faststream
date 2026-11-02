import asyncio
import logging
import math
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, TypeAlias, cast

import anyio
from redis.exceptions import ResponseError
from typing_extensions import override

from faststream._internal.endpoint.utils import process_msg
from faststream.redis.exceptions import (
    StreamClaimUnsupportedError,
    StreamGroupNotFoundError,
)
from faststream.redis.message import (
    BatchStreamMessage,
    ConsumerProtocol,
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
    from faststream.redis.parser.parsers import _StreamParser
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
ReadCallable = Callable[[str], Coroutine[None, None, ReadResponse]]


class ClaimMeta(NamedTuple):
    """Per-entry XREADGROUP CLAIM metadata."""

    idle_time: int
    """Milliseconds since the last delivery."""

    delivery_count: int
    """Previous deliveries: 0 for a new entry, XPENDING's `times_delivered` - 1."""


class _StreamHandlerMixin(LogicSubscriber):
    __slots__ = (
        "_stream_sub",
        "autoclaim_start_id",
        "claim_min_idle_time",
        "last_id",
        "min_idle_time",
        "parser",
        "read_id",
    )

    parser: "_StreamParser"

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

            except StreamClaimUnsupportedError as e:
                await self._stop_on_terminal_error(e, str(e))
                return

            except ResponseError as e:
                if "NOGROUP" in str(e):
                    msg = (
                        f"Consumer group `{self.stream_sub.group}` for stream "
                        f"`{self.stream_sub.name}` no longer exists. "
                        "The stream was likely deleted or flushed. "
                        "Stopping subscriber — restart the application to recreate the group."
                    )
                    await self._stop_on_terminal_error(StreamGroupNotFoundError(msg), msg)
                    return

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

    async def _stop_on_terminal_error(self, exc: Exception, msg: str) -> None:
        # A terminal error: re-running the read cannot fix it, and raising it
        # would make the task supervisor restart the read in a hot loop. Stop
        # the subscriber for good and, if the broker runs inside an app, exit.
        self._log(log_level=logging.CRITICAL, message=msg, exc_info=exc)
        await self.stop(blocking=False)
        if app := self._outer_config.fd_config.context.get("app"):
            app.exit()

    @override
    async def start(self) -> None:
        client = self._client

        stream = self.stream_sub
        stream_group = stream.group
        stream_consumer = stream.consumer
        stream_min_idle_time = stream.min_idle_time

        self.parser._setup(cast("ConsumerProtocol", client), stream_group)

        read: ReadCallable

        if stream_group and stream_consumer:
            group_create_id = "$" if self.last_id == ">" else self.last_id
            try:
                await client.xgroup_create(
                    name=stream.name,
                    id=group_create_id,
                    groupname=stream_group,
                    mkstream=stream.declare,
                )
            except ResponseError as e:
                if "already exists" not in str(e):
                    raise
            else:
                self.read_id = ">"

            self.last_id = self.read_id

            if stream_min_idle_time is None:

                async def read(_: str) -> ReadResponse:
                    return await self._xreadgroup(
                        count=stream.max_records,
                        block=stream.polling_interval,
                        noack=stream.no_ack,
                        group=stream_group,
                        stream=stream.name,
                        consumer=stream_consumer,
                    )

            else:

                async def read(_: str) -> ReadResponse:
                    stream_message = await client.xautoclaim(
                        name=stream.name,
                        groupname=stream_group,
                        consumername=stream_consumer,
                        min_idle_time=stream_min_idle_time,
                        start_id=self.autoclaim_start_id,
                        count=1,
                    )
                    stream_name = stream.name.encode()
                    (next_id, messages, *_) = stream_message  # type: ignore[assignment]

                    # Update start_id for next call
                    self.autoclaim_start_id = next_id

                    if next_id == b"0-0" and not messages:
                        await asyncio.sleep(stream.polling_interval / 1000)  # ms to s
                        return ()

                    return ((stream_name, messages),)

        else:

            async def read(
                last_id: str,
            ) -> ReadResponse:
                return cast(
                    "ReadResponse",
                    await client.xread(
                        {stream.name: last_id},
                        block=stream.polling_interval,
                        count=stream.max_records,
                    ),
                )

        await super().start(read)

    @override
    async def get_one(  # ruff: ignore[too-many-locals]
        self,
        *,
        timeout: float = 5.0,
    ) -> "RedisStreamMessage | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )
        stream = self.stream_sub
        claim_meta: ClaimMeta | None = None

        if stream.group and stream.consumer:
            if self.min_idle_time is None:
                xreadgroup_message = await self._xreadgroup(
                    count=1,
                    block=math.ceil(timeout * 1000),
                    noack=stream.no_ack,
                    group=stream.group,
                    stream=stream.name,
                    consumer=stream.consumer,
                )
                if not xreadgroup_message:
                    return None

                ((stream_name, (entry,)),) = xreadgroup_message
                message_id, raw_message, claim_meta = self._parse_stream_entry(entry)
            else:
                xautoclaim_message = await self._client.xautoclaim(
                    name=stream.name,
                    groupname=stream.group,
                    consumername=stream.consumer,
                    min_idle_time=self.min_idle_time,
                    start_id=self.autoclaim_start_id,
                    count=1,
                )
                (next_id, messages, *_) = xautoclaim_message
                # Update start_id for next call
                self.autoclaim_start_id = next_id
                if not messages:
                    return None
                stream_name = stream.name.encode()
                ((message_id, raw_message),) = messages
        else:
            xread_message = cast(
                "ReadResponse",
                await self._client.xread(
                    {stream.name: self.last_id},
                    block=math.ceil(timeout * 1000),
                    count=1,
                ),
            )
            if not xread_message:
                return None

            ((stream_name, ((message_id, raw_message, *_),)),) = xread_message

        self.last_id = message_id.decode()

        redis_incoming_msg = DefaultStreamMessage(
            type="stream",
            channel=stream_name.decode(),
            message_ids=[message_id],
            data=raw_message,
        )
        _attach_claim_metadata(redis_incoming_msg, [claim_meta])

        context = self._outer_config.context
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
    async def __aiter__(self) -> AsyncIterator["RedisStreamMessage"]:  # ruff: ignore[too-many-locals]
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        timeout = 5

        stream = self.stream_sub
        context = self._outer_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        claim_meta: ClaimMeta | None

        while True:
            claim_meta = None

            if stream.group and stream.consumer:
                if self.min_idle_time is None:
                    xreadgroup_message = await self._xreadgroup(
                        count=1,
                        block=math.ceil(timeout * 1000),
                        noack=stream.no_ack,
                        group=stream.group,
                        stream=stream.name,
                        consumer=stream.consumer,
                    )
                    if not xreadgroup_message:
                        continue

                    ((stream_name, (entry,)),) = xreadgroup_message
                    message_id, raw_message, claim_meta = self._parse_stream_entry(
                        entry,
                    )
                else:
                    xautoclaim_message = await self._client.xautoclaim(
                        name=stream.name,
                        groupname=stream.group,
                        consumername=stream.consumer,
                        min_idle_time=self.min_idle_time,
                        start_id=self.autoclaim_start_id,
                        count=1,
                    )
                    (next_id, messages, *_) = xautoclaim_message
                    # Update start_id for next call
                    self.autoclaim_start_id = next_id
                    if not messages:
                        continue
                    stream_name = stream.name.encode()
                    ((message_id, raw_message),) = messages
            else:
                xread_message = cast(
                    "ReadResponse",
                    await self._client.xread(
                        {stream.name: self.last_id},
                        block=math.ceil(timeout * 1000),
                        count=1,
                    ),
                )
                if not xread_message:
                    continue

                ((stream_name, ((message_id, raw_message, *_),)),) = xread_message

            self.last_id = message_id.decode()

            redis_incoming_msg = DefaultStreamMessage(
                type="stream",
                channel=stream_name.decode(),
                message_ids=[message_id],
                data=raw_message,
            )
            _attach_claim_metadata(redis_incoming_msg, [claim_meta])

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

    async def _xreadgroup(
        self,
        *,
        count: int | None,
        block: int | None,
        noack: bool = False,
        group: str,
        stream: str,
        consumer: str,
    ) -> ReadResponse:
        if self.claim_min_idle_time is None:
            return cast(
                "ReadResponse",
                await self._client.xreadgroup(
                    groupname=group,
                    consumername=consumer,
                    streams={stream: self.read_id},
                    count=count,
                    block=block,
                    noack=noack,
                ),
            )

        try:
            # types-redis stubs predate `claim_min_idle_time`; StreamSub validation
            # guarantees redis-py 7.1.0+ (which accepts it) whenever it is set.
            return cast(
                "ReadResponse",
                await self._client.xreadgroup(
                    groupname=group,
                    consumername=consumer,
                    streams={stream: self.read_id},
                    count=count,
                    block=block,
                    noack=noack,
                    claim_min_idle_time=self.claim_min_idle_time,
                ),
            )
        except ResponseError as e:
            if "syntax error" not in str(e).lower():
                raise

            msg = (
                "Redis server rejected the XREADGROUP CLAIM option for "
                f"stream `{stream}`. `claim_min_idle_time` "
                "requires Redis server 8.4+."
            )
            raise StreamClaimUnsupportedError(msg) from e

    def _parse_stream_entry(
        self,
        entry: "StreamEntry",
    ) -> tuple[bytes, dict[bytes, bytes], ClaimMeta | None]:
        message_id, data, *claim_meta = entry

        if self.claim_min_idle_time is None:
            return message_id, data, None

        if len(claim_meta) == 2:
            return message_id, data, ClaimMeta(*claim_meta)

        # Redis ignores CLAIM for an explicit id, so a bare entry means the
        # requested recovery is silently off; surface it instead of continuing.
        msg = (
            "Stream entry is missing XREADGROUP CLAIM metadata. Redis ignores "
            "the CLAIM option when reading with an explicit id, so the "
            "requested `claim_min_idle_time` behavior is disabled."
        )
        raise ValueError(msg)


class StreamSubscriber(_StreamHandlerMixin):
    __slots__ = ()

    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        self.parser = RedisStreamParser(config)
        config.decoder = self.parser.decode_message
        config.parser = self.parser.parse_message
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
                    _attach_claim_metadata(msg, [claim_meta])

                    await self.consume_dispatcher.dispatch(msg)


class StreamBatchSubscriber(_StreamHandlerMixin):
    __slots__ = ()

    def __init__(
        self,
        config: "RedisSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        self.parser = RedisBatchStreamParser(config)
        config.decoder = self.parser.decode_message
        config.parser = self.parser.parse_message
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
                claim_metas: list[ClaimMeta | None] = []
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
                _attach_claim_metadata(msg, claim_metas)

                await self.consume_dispatcher.dispatch(msg)


def _attach_claim_metadata(
    message: "_StreamMessage",
    claim_metas: "Sequence[ClaimMeta | None]",
) -> None:
    # `_parse_stream_entry` yields metadata for every entry or for none,
    # so plain subscribers leave the message untouched.
    metas = [m for m in claim_metas if m is not None]
    if not metas:
        return

    message["idle_times"] = [m.idle_time for m in metas]
    message["delivery_counts"] = [m.delivery_count for m in metas]
