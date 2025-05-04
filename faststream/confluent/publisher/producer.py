from typing import TYPE_CHECKING, NoReturn, Optional, Union

from typing_extensions import override

from faststream._internal.publisher.proto import ProducerProto
from faststream._internal.subscriber.utils import resolve_custom_func
from faststream.confluent.parser import AsyncConfluentParser
from faststream.exceptions import FeatureNotSupportedException
from faststream.message import encode_message

from .state import EmptyProducerState, ProducerState, RealProducer

if TYPE_CHECKING:
    import asyncio

    from confluent_kafka import Message

    from faststream._internal.types import CustomCallable
    from faststream.confluent.client import AsyncConfluentProducer
    from faststream.confluent.response import KafkaPublishCommand


class AsyncConfluentFastProducer(ProducerProto):
    """A class to represent Kafka producer."""

    def __init__(
        self,
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._producer: ProducerState = EmptyProducerState()

        # NOTE: register default parser to be compatible with request
        default = AsyncConfluentParser()
        self._parser = resolve_custom_func(parser, default.parse_message)
        self._decoder = resolve_custom_func(decoder, default.decode_message)

    def connect(self, producer: "AsyncConfluentProducer") -> None:
        self._producer = RealProducer(producer)

    async def disconnect(self) -> None:
        await self._producer.stop()
        self._producer = EmptyProducerState()

    def __bool__(self) -> bool:
        return bool(self._producer)

    async def ping(self, timeout: float) -> bool:
        return await self._producer.ping(timeout=timeout)

    @override
    async def publish(  # type: ignore[override]
        self,
        cmd: "KafkaPublishCommand",
    ) -> "Union[asyncio.Future[Optional[Message]], Optional[Message]]":
        """Publish a message to a topic."""
        message, content_type = encode_message(cmd.body)

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

    async def stop(self) -> None:
        await self._producer.stop()

    async def flush(self) -> None:
        await self._producer.flush()

    async def publish_batch(
        self,
        cmd: "KafkaPublishCommand",
    ) -> None:
        """Publish a batch of messages to a topic."""
        batch = self._producer.producer.create_batch()

        headers_to_send = cmd.headers_to_publish()

        for msg in cmd.batch_bodies:
            message, content_type = encode_message(msg)

            if content_type:
                final_headers = {
                    "content-type": content_type,
                    **headers_to_send,
                }
            else:
                final_headers = headers_to_send.copy()

            batch.append(
                key=None,
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

    @override
    async def request(  # type: ignore[override]
        self,
        cmd: "KafkaPublishCommand",
    ) -> NoReturn:
        msg = "Kafka doesn't support `request` method without test client."
        raise FeatureNotSupportedException(msg)
