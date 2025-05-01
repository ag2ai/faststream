from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from typing_extensions import override

from faststream.broker.message import encode_message
from faststream.broker.publisher.proto import ProducerProto
from faststream.broker.utils import resolve_custom_func
from faststream.exceptions import OperationForbiddenError
from faststream.kafka.exceptions import BatchBufferOverflowException
from faststream.kafka.message import KafkaMessage
from faststream.kafka.parser import AioKafkaParser

if TYPE_CHECKING:
    from aiokafka import AIOKafkaProducer

    from faststream.broker.types import CustomCallable
    from faststream.types import SendableMessage


class AioKafkaFastProducer(ProducerProto):
    """A class to represent Kafka producer."""

    def __init__(
        self,
        producer: "AIOKafkaProducer",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._producer = producer

        # NOTE: register default parser to be compatible with request
        default = AioKafkaParser(
            msg_class=KafkaMessage,
            regex=None,
        )
        self._parser = resolve_custom_func(parser, default.parse_message)
        self._decoder = resolve_custom_func(decoder, default.decode_message)

    async def flush(self) -> None:
        await self._producer.flush()

    @override
    async def publish(  # type: ignore[override]
        self,
        message: "SendableMessage",
        topic: str,
        *,
        correlation_id: str,
        key: Union[bytes, Any, None] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> None:
        """Publish a message to a topic."""
        message, content_type = encode_message(message)

        headers_to_send = {
            "content-type": content_type or "",
            "correlation_id": correlation_id,
            **(headers or {}),
        }

        if reply_to:
            headers_to_send["reply_to"] = headers_to_send.get(
                "reply_to",
                reply_to,
            )

        send_future = await self._producer.send(
            topic=topic,
            value=message,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=[(i, (j or "").encode()) for i, j in headers_to_send.items()],
        )
        if not no_confirm:
            await send_future

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish_batch(
        self,
        *msgs: "SendableMessage",
        correlation_id: str,
        topic: str,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> None:
        """Publish a batch of messages to a topic."""
        batch = self._producer.create_batch()

        headers_to_send = {"correlation_id": correlation_id, **(headers or {})}

        if reply_to:
            headers_to_send["reply_to"] = headers_to_send.get(
                "reply_to",
                reply_to,
            )

        for message_position, msg in enumerate(msgs):
            message, content_type = encode_message(msg)

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
                timestamp=timestamp_ms,
                headers=[(i, j.encode()) for i, j in final_headers.items()],
            )
            if metadata is None:
                raise BatchBufferOverflowException(message_position=message_position)

        send_future = await self._producer.send_batch(batch, topic, partition=partition)
        if not no_confirm:
            await send_future

    @override
    async def request(self, *args: Any, **kwargs: Any) -> Optional[Any]:
        raise OperationForbiddenError(
            "Kafka doesn't support `request` method without test client."
        )
