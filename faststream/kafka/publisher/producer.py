from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import override

from faststream._internal.publisher.proto import ProducerProto
from faststream._internal.subscriber.utils import resolve_custom_func
from faststream.exceptions import FeatureNotSupportedException
from faststream.kafka.exceptions import BatchBufferOverflowException
from faststream.kafka.message import KafkaMessage
from faststream.kafka.parser import AioKafkaParser
from faststream.message import encode_message

from .state import EmptyProducerState, ProducerState, RealProducer

if TYPE_CHECKING:
    import asyncio

    from aiokafka import AIOKafkaProducer
    from aiokafka.structs import RecordMetadata

    from faststream._internal.types import CustomCallable
    from faststream.kafka.response import KafkaPublishCommand


class AioKafkaFastProducer(ProducerProto):
    """A class to represent Kafka producer."""

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._producer: ProducerState = EmptyProducerState()

        # NOTE: register default parser to be compatible with request
        default = AioKafkaParser(
            msg_class=KafkaMessage,
            regex=None,
        )

        self._parser = resolve_custom_func(parser, default.parse_message)
        self._decoder = resolve_custom_func(decoder, default.decode_message)

    async def connect(self, producer: "AIOKafkaProducer") -> None:
        await producer.start()
        self._producer = RealProducer(producer)

    async def disconnect(self) -> None:
        await self._producer.stop()
        self._producer = EmptyProducerState()

    def __bool__(self) -> bool:
        return bool(self._producer)

    @property
    def closed(self) -> bool:
        return self._producer.closed

    async def flush(self) -> None:
        await self._producer.flush()

    @override
    async def publish(  # type: ignore[override]
        self,
        cmd: "KafkaPublishCommand",
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]:
        """Publish a message to a topic."""
        message, content_type = encode_message(cmd.body)

        headers_to_send = {
            "content-type": content_type or "",
            **cmd.headers_to_publish(),
        }

        send_future = await self._producer.producer.send(
            topic=cmd.destination,
            value=message,
            key=cmd.key,
            partition=cmd.partition,
            timestamp_ms=cmd.timestamp_ms,
            headers=[(i, (j or "").encode()) for i, j in headers_to_send.items()],
        )

        if not cmd.no_confirm:
            return await send_future
        return send_future

    async def publish_batch(
        self,
        cmd: "KafkaPublishCommand",
    ) -> Union["asyncio.Future[RecordMetadata]", "RecordMetadata"]:
        """Publish a batch of messages to a topic."""
        batch = self._producer.producer.create_batch()

        headers_to_send = cmd.headers_to_publish()

        for message_position, body in enumerate(cmd.batch_bodies):
            message, content_type = encode_message(body)

            if content_type:
                final_headers = {
                    "content-type": content_type,
                    **headers_to_send,
                }
            else:
                final_headers = headers_to_send.copy()

            metadata = batch.append(
                key=None,
                value=message,
                timestamp=cmd.timestamp_ms,
                headers=[(i, j.encode()) for i, j in final_headers.items()],
            )
            if metadata is None:
                raise BatchBufferOverflowException(message_position=message_position)

        send_future = await self._producer.producer.send_batch(
            batch,
            cmd.destination,
            partition=cmd.partition,
        )
        if not cmd.no_confirm:
            return await send_future
        return send_future

    @override
    async def request(
        self,
        cmd: "KafkaPublishCommand",
    ) -> Any:
        msg = "Kafka doesn't support `request` method without test client."
        raise FeatureNotSupportedException(msg)
