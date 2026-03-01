from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.producer import ProducerProto
from faststream.confluent.parser import AsyncConfluentParser
from faststream.confluent.response import KafkaPublishCommand
from faststream.exceptions import FeatureNotSupportedException
from faststream.message import encode_message

from .state import EmptyProducerState, ProducerState, RealProducer

if TYPE_CHECKING:
    import asyncio

    from confluent_kafka import Message
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.types import CustomCallable
    from faststream.confluent.helpers.client import AsyncConfluentProducer


class AsyncConfluentFastProducer(ProducerProto[KafkaPublishCommand]):
    """A class to represent Kafka producer."""

    def connect(
        self,
        producer: "AsyncConfluentProducer",
        serializer: Optional["SerializerProto"],
    ) -> None: ...

    def __bool__(self) -> bool:
        return False

    async def disconnect(self) -> None:
        return None

    async def flush(self) -> None:
        return None

    @abstractmethod
    async def ping(self, timeout: float) -> bool:
        return False

    @override
    @abstractmethod
    async def publish(
        self,
        cmd: "KafkaPublishCommand",
    ) -> "asyncio.Future[Message | None] | Message | None": ...

    @override
    @abstractmethod
    async def publish_batch(self, cmd: "KafkaPublishCommand") -> None: ...

    @override
    async def request(self, cmd: "KafkaPublishCommand") -> Any:
        msg = "Kafka doesn't support `request` method without test client."
        raise FeatureNotSupportedException(msg)


class FakeConfluentFastProducer(AsyncConfluentFastProducer):
    def connect(
        self,
        producer: "AsyncConfluentProducer",
        serializer: Optional["SerializerProto"],
    ) -> None:
        raise NotImplementedError

    async def disconnect(self) -> None:
        raise NotImplementedError

    async def flush(self) -> None:
        raise NotImplementedError

    async def ping(self, timeout: float) -> bool:
        raise NotImplementedError

    @override
    async def publish(
        self,
        cmd: "KafkaPublishCommand",
    ) -> "asyncio.Future[Message | None] | Message | None":
        raise NotImplementedError

    @override
    async def publish_batch(self, cmd: "KafkaPublishCommand") -> None:
        raise NotImplementedError


class AsyncConfluentFastProducerImpl(AsyncConfluentFastProducer):
    """A class to represent Kafka producer."""

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._producer: ProducerState = EmptyProducerState()
        self.serializer: SerializerProto | None = None

        # NOTE: register default parser to be compatible with request
        default = AsyncConfluentParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

    def connect(
        self,
        producer: "AsyncConfluentProducer",
        serializer: Optional["SerializerProto"],
    ) -> None:
        self._producer = RealProducer(producer)
        self.serializer = serializer

    async def disconnect(self) -> None:
        await self._producer.stop()
        self._producer = EmptyProducerState()

    def __bool__(self) -> bool:
        return bool(self._producer)

    async def ping(self, timeout: float) -> bool:
        return await self._producer.ping(timeout=timeout)

    async def flush(self) -> None:
        await self._producer.flush()

    @override
    async def publish(
        self,
        cmd: "KafkaPublishCommand",
    ) -> "asyncio.Future[Message | None] | Message | None":
        """Publish a message to a topic."""
        message, content_type = encode_message(cmd.body, serializer=self.serializer)

        headers_to_send = {
            "content-type": content_type or "",
            **cmd.headers_to_publish(),
        }

        return await self._producer.producer.send(
            topic=cmd.destination,
            value=message,
            key=cmd.key,
            partition=cmd.partition,
            timestamp_ms=cmd.timestamp_ms,
            headers=[(i, (j or "").encode()) for i, j in headers_to_send.items()],
            no_confirm=cmd.no_confirm,
        )

    @override
    async def publish_batch(self, cmd: "KafkaPublishCommand") -> None:
        """Publish a batch of messages to a topic."""
        batch = self._producer.producer.create_batch()

        headers_to_send = cmd.headers_to_publish()

        for message_position, msg in enumerate(cmd.batch_bodies):
            message, content_type = encode_message(msg, serializer=self.serializer)

            if content_type:
                final_headers = {
                    "content-type": content_type,
                    **headers_to_send,
                }
            else:
                final_headers = headers_to_send.copy()

            batch.append(
                key=cmd.key_for(message_position),
                value=message,
                timestamp=cmd.timestamp_ms,
                headers=[(i, j.encode()) for i, j in final_headers.items()],
            )

        await self._producer.producer.send_batch(
            batch,
            cmd.destination,
            partition=cmd.partition,
            no_confirm=cmd.no_confirm,
        )
