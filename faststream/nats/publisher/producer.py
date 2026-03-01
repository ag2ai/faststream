import asyncio
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional

import anyio
import nats
from nats.aio.client import NO_RESPONDERS_STATUS
from nats.js.api import Header
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException
from faststream.message import encode_message
from faststream.nats.helpers.state import (
    ConnectedState,
    ConnectionState,
    EmptyConnectionState,
)
from faststream.nats.parser import NatsParser
from faststream.nats.response import NatsPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto
    from nats.aio.client import Client
    from nats.aio.msg import Msg
    from nats.js import JetStreamContext

    from faststream._internal.types import (
        AsyncCallable,
        CustomCallable,
    )
    from faststream.nats.schemas import PubAck


class NatsFastProducer(ProducerProto[NatsPublishCommand]):
    def connect(
        self,
        connection: Any,
        serializer: Optional["SerializerProto"],
    ) -> None: ...

    def disconnect(self) -> None: ...

    @abstractmethod
    async def publish(self, cmd: "NatsPublishCommand") -> Optional["PubAck"]: ...

    @abstractmethod
    async def request(self, cmd: "NatsPublishCommand") -> "Msg": ...

    async def publish_batch(self, cmd: "NatsPublishCommand") -> None:
        msg = "NATS doesn't support publishing in batches."
        raise FeatureNotSupportedException(msg)


class NatsFastProducerImpl(NatsFastProducer):
    """A class to represent a NATS producer."""

    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self.serializer: SerializerProto | None = None

        default = NatsParser(pattern="", is_ack_disabled=True)
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

        self.__state: ConnectionState[Client] = EmptyConnectionState()

    def connect(
        self,
        connection: "Client",
        serializer: Optional["SerializerProto"],
    ) -> None:
        self.serializer = serializer
        self.__state = ConnectedState(connection)

    def disconnect(self) -> None:
        self.__state = EmptyConnectionState()

    @override
    async def publish(self, cmd: "NatsPublishCommand") -> None:
        payload, content_type = encode_message(cmd.body, self.serializer)

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
    async def request(self, cmd: "NatsPublishCommand") -> "Msg":
        payload, content_type = encode_message(cmd.body, self.serializer)

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


class NatsJSFastProducer(NatsFastProducer):
    """A class to represent a NATS JetStream producer."""

    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        *,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self.serializer: SerializerProto | None = None

        default = NatsParser(pattern="", is_ack_disabled=True)
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

        self.__state: ConnectionState[JetStreamContext] = EmptyConnectionState()

    def connect(
        self,
        connection: "JetStreamContext",
        serializer: Optional["SerializerProto"],
    ) -> None:
        self.serializer = serializer
        self.__state = ConnectedState(connection)

    def disconnect(self) -> None:
        self.__state = EmptyConnectionState()

    @override
    async def publish(self, cmd: "NatsPublishCommand") -> "PubAck":
        payload, content_type = encode_message(cmd.body, self.serializer)

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
    async def request(self, cmd: "NatsPublishCommand") -> "Msg":
        payload, content_type = encode_message(cmd.body, self.serializer)

        reply_to = self.__state.connection._nc.new_inbox()
        future: asyncio.Future[Msg] = asyncio.Future()
        sub = await self.__state.connection._nc.subscribe(
            reply_to,
            future=future,
            max_msgs=1,
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
                msg.headers and (msg.headers.get(Header.STATUS) == NO_RESPONDERS_STATUS)
            ):
                raise nats.errors.NoRespondersError

            return msg


class FakeNatsFastProducer(NatsFastProducer):
    def connect(self, connection: Any, serializer: Optional["SerializerProto"]) -> None:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError

    @override
    async def publish(self, cmd: "NatsPublishCommand") -> None:
        raise NotImplementedError

    @override
    async def request(self, cmd: "NatsPublishCommand") -> "Msg":
        raise NotImplementedError

    @override
    async def publish_batch(self, cmd: "NatsPublishCommand") -> None:
        msg = "NATS doesn't support publishing in batches."
        raise FeatureNotSupportedException(msg)
