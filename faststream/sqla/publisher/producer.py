from abc import abstractmethod
from typing import Any, Optional, override
from fast_depends.library.serializer import SerializerProto
from sqlalchemy.ext.asyncio import AsyncEngine
from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream._internal.types import AsyncCallable, CustomCallable
from faststream.exceptions import FeatureNotSupportedException
from faststream.message.utils import encode_message
from faststream.sqla.client import SqlaPostgresClient, create_sqla_client
from faststream.sqla.parser import SqlaParser
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
    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self.client = create_sqla_client(engine)

        self.serializer: SerializerProto | None = None

        default = SqlaParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

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
        payload, content_type = encode_message(cmd.body, self.serializer)

        headers_to_add = {}
        if content_type:
            headers_to_add["content-type"] = content_type

        cmd.add_headers(headers_to_add)
        
        return await self.client.enqueue(
            payload=payload,
            queue=cmd.destination,
            headers=cmd.headers,
            next_attempt_at=cmd.next_attempt_at,
            connection=cmd.connection,
        )
