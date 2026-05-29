from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional, cast

import anyio
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream._internal.utils.nuid import NUID
from faststream.redis.message import DATA_KEY
from faststream.redis.parser import RedisPubSubParser, SimpleParserConfig
from faststream.redis.response import DestinationType, RedisPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto
    from redis.asyncio.client import Redis

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import CustomCallable
    from faststream.redis.configs import ConnectionState
    from faststream.redis.parser import MessageFormat


class BaseRedisFastProducer(ProducerProto[RedisPublishCommand]):
    """Shared logic for Redis producers."""

    _connection: "ConnectionState[Any]"
    _decoder: "ParserComposition"
    _parser: "ParserComposition"

    def __init__(
        self,
        connection: "ConnectionState[Any]",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        message_format: type["MessageFormat"],
        serializer: Optional["SerializerProto"],
        codec: Optional["CodecProto"] = None,
    ) -> None:
        self._connection = connection

        default = RedisPubSubParser(SimpleParserConfig(message_format))
        self._parser = ParserComposition(
            parser,
            default.parse_message,
        )
        self._decoder = ParserComposition(
            decoder,
            default.decode_message,
        )
        self.serializer = serializer
        self.codec = codec or DefaultCodec()

    @override
    async def publish_batch(self, cmd: "RedisPublishCommand") -> int:
        batch = [
            await cmd.message_format.encode(
                message=msg,
                correlation_id=cmd.correlation_id or "",
                reply_to=cmd.reply_to,
                headers=cmd.headers,
                serializer=self.serializer,
                codec=self.codec,
            )
            for msg in cmd.batch_bodies
        ]

        connection = cmd.pipeline or self._connection.client
        return cast("int", await connection.rpush(cmd.destination, *batch))

    def connect(
        self,
        serializer: Optional["SerializerProto"] = None,
        codec: Optional["CodecProto"] = None,
    ) -> None:
        self.serializer = serializer
        if codec is not None:
            self.codec = codec

    def _build_child(self, **kwargs: Any) -> "RedisFastProducer":
        return self.__class__(**kwargs)  # type: ignore[return-value]


class RedisFastProducer(BaseRedisFastProducer):
    """Producer for single-node Redis and Redis Cluster.

    Both the async ``Redis`` and ``RedisCluster`` clients share the same
    command API (``publish`` / ``rpush`` / ``xadd`` / ``pubsub``), so the
    cluster broker reuses this producer directly.
    """

    _connection: "ConnectionState[Redis[bytes]]"

    def __init__(
        self,
        connection: "ConnectionState[Any]",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        message_format: type["MessageFormat"],
        serializer: Optional["SerializerProto"],
    ) -> None:
        super().__init__(
            connection=connection,
            parser=parser,
            decoder=decoder,
            message_format=message_format,
            serializer=serializer,
        )

    @override
    async def publish(self, cmd: "RedisPublishCommand") -> int | bytes:
        msg = await cmd.message_format.encode(
            message=cmd.body,
            reply_to=cmd.reply_to,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id or "",
            serializer=self.serializer,
            codec=self.codec,
        )

        return await self.__publish(msg, cmd)

    async def __publish(
        self,
        msg: bytes,
        cmd: "RedisPublishCommand",
    ) -> int | bytes:
        connection = cmd.pipeline or self._connection.client

        if cmd.destination_type is DestinationType.Channel:
            return await connection.publish(cmd.destination, msg)

        if cmd.destination_type is DestinationType.List:
            return await connection.rpush(cmd.destination, msg)

        if cmd.destination_type is DestinationType.Stream:
            return cast(
                "bytes",
                await connection.xadd(
                    name=cmd.destination,
                    fields={DATA_KEY: msg},
                    maxlen=cmd.maxlen,
                ),
            )

        error_msg = "unreachable"
        raise AssertionError(error_msg)

    @override
    async def request(self, cmd: "RedisPublishCommand") -> "Any":
        nuid = NUID()
        reply_to = str(nuid.next(), "utf-8")
        psub = self._connection.client.pubsub()

        try:
            await psub.subscribe(reply_to)

            msg = await cmd.message_format.encode(
                message=cmd.body,
                reply_to=reply_to,
                headers=cmd.headers,
                correlation_id=cmd.correlation_id or "",
                serializer=self.serializer,
                codec=self.codec,
            )

            await self.__publish(msg, cmd)

            with anyio.fail_after(cmd.timeout) as scope:
                # skip subscribe message
                await psub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=cmd.timeout or 0.0,
                )

                # get real response
                response_msg = await psub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=cmd.timeout or 0.0,
                )

            if scope.cancel_called:
                raise TimeoutError

            return response_msg

        finally:
            with suppress(Exception):
                await psub.unsubscribe()
                await psub.aclose()  # type: ignore[attr-defined]
