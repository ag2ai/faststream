from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream.mq.parser import MQParser
from faststream.mq.response import MQPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.types import CustomCallable
    from faststream.mq.helpers.client import AsyncMQConnection, MQConnectionConfig


class AsyncMQFastProducer(ProducerProto[MQPublishCommand]):
    @abstractmethod
    async def connect(
        self,
        *,
        connection_config: "MQConnectionConfig",
        serializer: Optional["SerializerProto"],
    ) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def ping(self, timeout: float) -> bool: ...


class AsyncMQConnectionProducer(ProducerProto[MQPublishCommand]):
    def __init__(
        self,
        connection: "AsyncMQConnection",
        serializer: Optional["SerializerProto"],
    ) -> None:
        self.connection = connection
        self.serializer = serializer

    async def publish(self, cmd: MQPublishCommand) -> None:
        await self.connection.publish(cmd, serializer=self.serializer)

    async def request(self, cmd: MQPublishCommand) -> Any:
        return await self.connection.request(cmd, serializer=self.serializer)

    async def publish_batch(self, cmd: MQPublishCommand) -> None:
        msg = "IBM MQ doesn't support publishing in batches."
        raise NotImplementedError(msg)


class FakeMQFastProducer(AsyncMQFastProducer):
    async def connect(
        self,
        *,
        connection_config: "MQConnectionConfig",
        serializer: Optional["SerializerProto"],
    ) -> None:
        raise NotImplementedError

    async def disconnect(self) -> None:
        raise NotImplementedError

    async def ping(self, timeout: float) -> bool:
        raise NotImplementedError

    async def publish(self, cmd: MQPublishCommand) -> None:
        raise NotImplementedError

    async def request(self, cmd: MQPublishCommand) -> Any:
        raise NotImplementedError

    async def publish_batch(self, cmd: MQPublishCommand) -> None:
        raise NotImplementedError


class AsyncMQFastProducerImpl(AsyncMQFastProducer):
    _connection: "AsyncMQConnection | None"

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._connection = None
        self.serializer: SerializerProto | None = None

        default_parser = MQParser()
        self._parser = ParserComposition(parser, default_parser.parse_message)
        self._decoder = ParserComposition(decoder, default_parser.decode_message)

    @property
    def connection(self) -> "AsyncMQConnection | None":
        return self._connection

    async def connect(
        self,
        *,
        connection_config: "MQConnectionConfig",
        serializer: Optional["SerializerProto"],
    ) -> None:
        from faststream.mq.helpers.client import AsyncMQConnection

        self.serializer = serializer
        self._connection = AsyncMQConnection(connection_config=connection_config)
        await self._connection.connect()

    async def disconnect(self) -> None:
        if self._connection is not None:
            await self._connection.disconnect()
        self._connection = None

    async def ping(self, timeout: float) -> bool:
        if self._connection is None:
            return False
        return await self._connection.ping(timeout)

    @override
    async def publish(self, cmd: MQPublishCommand) -> None:
        assert self._connection is not None, "Producer is not connected yet."
        await self._connection.publish(cmd, serializer=self.serializer)

    @override
    async def request(self, cmd: MQPublishCommand) -> Any:
        assert self._connection is not None, "Producer is not connected yet."
        return await self._connection.request(cmd, serializer=self.serializer)

    @override
    async def publish_batch(self, cmd: MQPublishCommand) -> None:
        msg = "IBM MQ doesn't support publishing in batches."
        raise NotImplementedError(msg)
