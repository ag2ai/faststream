from typing import TYPE_CHECKING, Any, Optional, cast

import anyio
from typing_extensions import override

from faststream._internal.endpoint.utils import resolve_custom_func
from faststream._internal.producer import ProducerProto
from faststream._internal.utils.nuid import NUID
from faststream.redis.message import DATA_KEY
from faststream.redis.parser import RawMessage, RedisPubSubParser
from faststream.redis.response import DestinationType, RedisPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.types import AsyncCallable, CustomCallable
    from faststream.redis.configs import ConnectionState


class RedisFastProducer(ProducerProto):
    """A class to represent a Redis producer."""

    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        connection: "ConnectionState",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._connection = connection
        self.serializer: SerializerProto | None = None

        default = RedisPubSubParser()
        self._parser = resolve_custom_func(
            parser,
            default.parse_message,
        )
        self._decoder = resolve_custom_func(
            decoder,
            default.decode_message,
        )

    @override
    async def publish(  # type: ignore[override]
        self,
        cmd: "RedisPublishCommand",
    ) -> int | bytes:
        msg = RawMessage.encode(
            message=cmd.body,
            reply_to=cmd.reply_to,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id or "",
            serializer=self.serializer,
        )

        return await self.__publish(msg, cmd)

    @override
    async def request(  # type: ignore[override]
        self,
        cmd: "RedisPublishCommand",
    ) -> "Any":
        nuid = NUID()
        reply_to = str(nuid.next(), "utf-8")
        psub = self._connection.client.pubsub()
        await psub.subscribe(reply_to)

        msg = RawMessage.encode(
            message=cmd.body,
            reply_to=reply_to,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id or "",
            serializer=self.serializer,
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

        await psub.unsubscribe()
        await psub.aclose()  # type: ignore[attr-defined]

        if scope.cancel_called:
            raise TimeoutError

        return response_msg

    @override
    async def publish_batch(
        self,
        cmd: "RedisPublishCommand",
    ) -> int:
        batch = [
            RawMessage.encode(
                message=msg,
                correlation_id=cmd.correlation_id or "",
                reply_to=cmd.reply_to,
                headers=cmd.headers,
                serializer=self.serializer,
            )
            for msg in cmd.batch_bodies
        ]

        connection = cmd.pipeline or self._connection.client
        return await connection.rpush(cmd.destination, *batch)

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

    def connect(self, serializer: Optional["SerializerProto"] = None) -> None:
        self.serializer = serializer
