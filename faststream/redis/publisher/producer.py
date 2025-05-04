from typing import TYPE_CHECKING, Any, Optional, Union, cast

import anyio
from typing_extensions import override

from faststream._internal.publisher.proto import ProducerProto
from faststream._internal.subscriber.utils import resolve_custom_func
from faststream._internal.utils.nuid import NUID
from faststream.redis.helpers.state import (
    ConnectedState,
    ConnectionState,
    EmptyConnectionState,
)
from faststream.redis.message import DATA_KEY
from faststream.redis.parser import RawMessage, RedisPubSubParser
from faststream.redis.response import DestinationType, RedisPublishCommand

if TYPE_CHECKING:
    from redis.asyncio.client import Redis

    from faststream._internal.types import (
        AsyncCallable,
        CustomCallable,
    )


class RedisFastProducer(ProducerProto):
    """A class to represent a Redis producer."""

    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._connection: ConnectionState = EmptyConnectionState()

        default = RedisPubSubParser()
        self._parser = resolve_custom_func(
            parser,
            default.parse_message,
        )
        self._decoder = resolve_custom_func(
            decoder,
            default.decode_message,
        )

    def connect(self, client: "Redis[bytes]") -> None:
        self._connection = ConnectedState(client)

    def disconnect(self) -> None:
        self._connection = EmptyConnectionState()

    @override
    async def publish(  # type: ignore[override]
        self,
        cmd: "RedisPublishCommand",
    ) -> Union[int, bytes]:
        msg = RawMessage.encode(
            message=cmd.body,
            reply_to=cmd.reply_to,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id or "",
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
            )
            for msg in cmd.batch_bodies
        ]
        return await self._connection.client.rpush(cmd.destination, *batch)

    async def __publish(
        self, msg: bytes, cmd: "RedisPublishCommand"
    ) -> Union[int, bytes]:
        if cmd.destination_type is DestinationType.Channel:
            return await self._connection.client.publish(cmd.destination, msg)

        if cmd.destination_type is DestinationType.List:
            return await self._connection.client.rpush(cmd.destination, msg)

        if cmd.destination_type is DestinationType.Stream:
            return cast(
                "bytes",
                await self._connection.client.xadd(
                    name=cmd.destination,
                    fields={DATA_KEY: msg},
                    maxlen=cmd.maxlen,
                ),
            )

        error_msg = "unreachable"
        raise AssertionError(error_msg)
