from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional, TypeVar, cast

import anyio
from redis.asyncio.client import Pipeline
from redis.asyncio.cluster import ClusterPipeline
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream._internal.utils.nuid import NUID
from faststream.redis.exceptions import UnreachablePathError
from faststream.redis.message import DATA_KEY
from faststream.redis.parser import RedisPubSubParser, SimpleParserConfig
from faststream.redis.response import DestinationType, RedisPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import CustomCallable
    from faststream.redis.configs import ConnectionState
    from faststream.redis.parser import MessageFormat

_PipelineT = TypeVar("_PipelineT", bound=Pipeline | ClusterPipeline)


class RedisFastProducer(ProducerProto[RedisPublishCommand[Any]]):
    """Producer for both a single-node Redis and a Redis Cluster.

    Since redis-py 8.0.0 the async ``RedisCluster`` speaks the same command
    API as ``Redis`` — ``publish``, ``rpush``, ``xadd``, ``pubsub`` — so both
    connections drive the very same publish path.
    """

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
    async def publish(
        self, cmd: "RedisPublishCommand[_PipelineT]"
    ) -> int | bytes | _PipelineT:
        msg = await cmd.message_format.encode(
            message=cmd.body,
            reply_to=cmd.reply_to,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id or "",
            serializer=self.serializer,
            codec=self.codec,
        )

        return await self.__publish(msg, cmd)

    @override
    async def publish_batch(
        self, cmd: "RedisPublishCommand[_PipelineT]"
    ) -> int | _PipelineT:
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
        return cast("int | _PipelineT", await connection.rpush(cmd.destination, *batch))

    @override
    async def request(self, cmd: "RedisPublishCommand[Any]") -> "Any":
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
                await psub.aclose()

    def connect(
        self,
        serializer: Optional["SerializerProto"] = None,
        codec: Optional["CodecProto"] = None,
    ) -> None:
        self.serializer = serializer
        if codec is not None:
            self.codec = codec

    async def __publish(
        self,
        msg: bytes,
        cmd: "RedisPublishCommand[_PipelineT]",
    ) -> int | bytes | _PipelineT:
        connection = cmd.pipeline or self._connection.client

        if cmd.destination_type is DestinationType.Channel:
            return cast(
                "int | _PipelineT", await connection.publish(cmd.destination, msg)
            )

        if cmd.destination_type is DestinationType.List:
            return cast("int | _PipelineT", await connection.rpush(cmd.destination, msg))

        if cmd.destination_type is DestinationType.Stream:
            return cast(
                "bytes | _PipelineT",
                await connection.xadd(
                    name=cmd.destination,
                    fields={DATA_KEY: msg},
                    maxlen=cmd.maxlen,
                ),
            )

        raise UnreachablePathError

    def _build_child(self, **kwargs: Any) -> "RedisFastProducer":
        return self.__class__(**kwargs)
