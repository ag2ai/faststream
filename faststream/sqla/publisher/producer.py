from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy.ext.asyncio import AsyncEngine
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException
from faststream.message.utils import encode_message
from faststream.sqla.client import create_sqla_client
from faststream.sqla.parser import SqlaParser
from faststream.sqla.response import SqlaPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.types import AsyncCallable, CustomCallable


class SqlaProducerProto(ProducerProto[SqlaPublishCommand]):
    def connect(
        self,
        connection: Any,
        serializer: Optional["SerializerProto"],
    ) -> None: ...

    def disconnect(self) -> None: ...

    @abstractmethod
    async def publish(self, cmd: "SqlaPublishCommand") -> None: ...

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
        engine: AsyncEngine,  # todo
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self.client = create_sqla_client(engine)

        self.serializer: SerializerProto | None = None

        default = SqlaParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

    def connect(
        self,
        connection: Any,
        serializer: Optional["SerializerProto"],
    ) -> None:
        self.serializer = serializer

    @override
    async def publish(self, cmd: "SqlaPublishCommand") -> None:
        payload, content_type = encode_message(cmd.body, self.serializer)

        headers_to_send = {
            **({"content-type": content_type} if content_type else {}),
            **cmd.headers_to_publish(),
        }

        return await self.client.enqueue(
            payload=payload,
            queue=cmd.destination,
            headers=headers_to_send,
            next_attempt_at=cmd.next_attempt_at,
            connection=cmd.connection,
        )
