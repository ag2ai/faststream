from abc import abstractmethod
from typing import Any, Optional, override
from fast_depends.library.serializer import SerializerProto
from sqlalchemy.ext.asyncio import AsyncEngine
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException
from faststream.message.utils import encode_message
from faststream.sqla.client import SqlaClient
from faststream.sqla.response import SqlaPublishCommand


class SqlaProducerProto(ProducerProto[SqlaPublishCommand]):
    def connect(
        self,
        connection: Any,
        serializer: Optional["SerializerProto"],
    ) -> None: ...

    def disconnect(self) -> None: ...

    @abstractmethod
    async def publish(self, cmd: "SqlaPublishCommand") -> None:
        ...

    async def request(self, cmd: "SqlaPublishCommand") -> None:
        msg = "SqlaBroker doesn't support synchronous requests."
        raise FeatureNotSupportedException(msg)

    async def publish_batch(self, cmd: "SqlaPublishCommand") -> None:
        msg = "SqlaBroker doesn't support publishing in batches."
        raise FeatureNotSupportedException(msg)


class SqlaProducer(SqlaProducerProto):
    # _decoder: "AsyncCallable"
    # _parser: "AsyncCallable"

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        # parser: Optional["CustomCallable"],
        # decoder: Optional["CustomCallable"],
    ) -> None:
        self.client = SqlaClient(engine)

        self.serializer: SerializerProto | None = None

        # default = NatsParser(pattern="", is_ack_disabled=True)
        # self._parser = ParserComposition(parser, default.parse_message)
        # self._decoder = ParserComposition(decoder, default.decode_message)

        # self.__state: ConnectionState[JetStreamContext] = EmptyConnectionState()

    def connect(
        self,
        connection: Any,
        serializer: Optional["SerializerProto"],
    ) -> None:
        self.serializer = serializer
        # self.__state = ConnectedState(connection)

    @override
    async def publish(self, cmd: "SqlaPublishCommand") -> None:
        payload, _ = encode_message(cmd.body, self.serializer)

        return await self.client.enqueue(
            queue=cmd.destination,
            payload=payload,
            next_attempt_at=cmd.next_attempt_at,
            connection=cmd.connection,
        )