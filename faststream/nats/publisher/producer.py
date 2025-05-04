import asyncio
from typing import TYPE_CHECKING, Optional

import anyio
import nats
from typing_extensions import override

from faststream._internal.publisher.proto import ProducerProto
from faststream._internal.subscriber.utils import resolve_custom_func
from faststream.exceptions import FeatureNotSupportedException
from faststream.message import encode_message
from faststream.nats.helpers.state import (
    ConnectedState,
    ConnectionState,
    EmptyConnectionState,
)
from faststream.nats.parser import NatsParser

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.aio.msg import Msg
    from nats.js import JetStreamContext

    from faststream._internal.types import (
        AsyncCallable,
        CustomCallable,
    )
    from faststream.nats.response import NatsPublishCommand
    from faststream.nats.schemas import PubAck


class NatsFastProducer(ProducerProto):
    """A class to represent a NATS producer."""

    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        default = NatsParser(pattern="", is_ack_disabled=True)
        self._parser = resolve_custom_func(parser, default.parse_message)
        self._decoder = resolve_custom_func(decoder, default.decode_message)

        self.__state: ConnectionState[Client] = EmptyConnectionState()

    def connect(self, connection: "Client") -> None:
        self.__state = ConnectedState(connection)

    def disconnect(self) -> None:
        self.__state = EmptyConnectionState()

    @override
    async def publish(  # type: ignore[override]
        self,
        cmd: "NatsPublishCommand",
    ) -> None:
        payload, content_type = encode_message(cmd.body)

        headers_to_send = {
            "content-type": content_type or "",
            **cmd.headers_to_publish(),
        }

        return await self.__state.connection.publish(
            subject=cmd.destination,
            payload=payload,
            reply=cmd.reply_to,
            headers=headers_to_send,
        )

    @override
    async def request(  # type: ignore[override]
        self,
        cmd: "NatsPublishCommand",
    ) -> "Msg":
        payload, content_type = encode_message(cmd.body)

        headers_to_send = {
            "content-type": content_type or "",
            **cmd.headers_to_publish(),
        }

        return await self.__state.connection.request(
            subject=cmd.destination,
            payload=payload,
            headers=headers_to_send,
            timeout=cmd.timeout,
        )

    @override
    async def publish_batch(
        self,
        cmd: "NatsPublishCommand",
    ) -> None:
        msg = "NATS doesn't support publishing in batches."
        raise FeatureNotSupportedException(msg)


class NatsJSFastProducer(ProducerProto):
    """A class to represent a NATS JetStream producer."""

    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        *,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        default = NatsParser(
            pattern="", is_ack_disabled=True
        )  # core parser to serializer responses
        self._parser = resolve_custom_func(parser, default.parse_message)
        self._decoder = resolve_custom_func(decoder, default.decode_message)

        self.__state: ConnectionState[JetStreamContext] = EmptyConnectionState()

    def connect(self, connection: "Client") -> None:
        self.__state = ConnectedState(connection)

    def disconnect(self) -> None:
        self.__state = EmptyConnectionState()

    @override
    async def publish(  # type: ignore[override]
        self,
        cmd: "NatsPublishCommand",
    ) -> "PubAck":
        payload, content_type = encode_message(cmd.body)

        headers_to_send = {
            "content-type": content_type or "",
            **cmd.headers_to_publish(js=True),
        }

        return await self.__state.connection.publish(
            subject=cmd.destination,
            payload=payload,
            headers=headers_to_send,
            stream=cmd.stream,
            timeout=cmd.timeout,
        )

    @override
    async def request(  # type: ignore[override]
        self,
        cmd: "NatsPublishCommand",
    ) -> "Msg":
        payload, content_type = encode_message(cmd.body)

        reply_to = self.__state.connection._nc.new_inbox()
        future: asyncio.Future[Msg] = asyncio.Future()
        sub = await self.__state.connection._nc.subscribe(
            reply_to, future=future, max_msgs=1
        )
        await sub.unsubscribe(limit=1)

        headers_to_send = {
            "content-type": content_type or "",
            "reply_to": reply_to,
            **cmd.headers_to_publish(js=False),
        }

        with anyio.fail_after(cmd.timeout):
            await self.__state.connection.publish(
                subject=cmd.destination,
                payload=payload,
                headers=headers_to_send,
                stream=cmd.stream,
                timeout=cmd.timeout,
            )

            msg = await future

            if (  # pragma: no cover
                msg.headers
                and (
                    msg.headers.get(nats.js.api.Header.STATUS)
                    == nats.aio.client.NO_RESPONDERS_STATUS
                )
            ):
                raise nats.errors.NoRespondersError

            return msg

    @override
    async def publish_batch(
        self,
        cmd: "NatsPublishCommand",
    ) -> None:
        msg = "NATS doesn't support publishing in batches."
        raise FeatureNotSupportedException(msg)
