from typing import TYPE_CHECKING, Any, Dict, Optional

from typing_extensions import override

from faststream.broker.message import encode_message
from faststream.broker.publisher.proto import ProducerProto
from faststream.broker.utils import resolve_custom_func
from faststream.confluent.parser import AsyncConfluentParser
from faststream.exceptions import OperationForbiddenError

if TYPE_CHECKING:
    from faststream.broker.types import CustomCallable
    from faststream.confluent.client import AsyncConfluentProducer
    from faststream.types import SendableMessage


class AsyncConfluentFastProducer(ProducerProto):
    """A class to represent Kafka producer."""

    def __init__(
        self,
        producer: "AsyncConfluentProducer",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._producer = producer

        # NOTE: register default parser to be compatible with request
        default = AsyncConfluentParser
        self._parser = resolve_custom_func(parser, default.parse_message)
        self._decoder = resolve_custom_func(decoder, default.decode_message)

    @override
    async def publish(  # type: ignore[override]
        self,
        message: "SendableMessage",
        topic: str,
        *,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        correlation_id: str = "",
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

        await self._producer.send(
            topic=topic,
            value=message,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=[(i, (j or "").encode()) for i, j in headers_to_send.items()],
            no_confirm=no_confirm,
        )

    async def stop(self) -> None:
        await self._producer.stop()

    async def flush(self) -> None:
        await self._producer.flush()

    async def publish_batch(
        self,
        *msgs: "SendableMessage",
        topic: str,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        reply_to: str = "",
        correlation_id: str = "",
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

        for msg in msgs:
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
                timestamp=timestamp_ms,
                headers=[(i, j.encode()) for i, j in final_headers.items()],
            )

        await self._producer.send_batch(
            batch,
            topic,
            partition=partition,
            no_confirm=no_confirm,
        )

    @override
    async def request(self, *args: Any, **kwargs: Any) -> Optional[Any]:
        raise OperationForbiddenError(
            "Kafka doesn't support `request` method without test client."
        )
