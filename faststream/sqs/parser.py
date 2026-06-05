from typing import TYPE_CHECKING, Any

from faststream.message import StreamMessage, decode_message

from .message import SQSMessage

if TYPE_CHECKING:
    from types_aiobotocore_sqs import SQSClient

    from faststream._internal.basic_types import DecodedMessage

    from .message import SQSRawMessage


# Message attribute names FastStream reserves for transport metadata.
RESERVED_ATTRS = ("content-type", "reply_to", "correlation_id")


class SQSParser:
    """Parses raw SQS messages into FastStream ``SQSMessage`` objects.

    ``client`` and ``queue_url`` are injected by the subscriber at start time
    so the resulting message can ack/nack/reject against the right queue.
    """

    def __init__(self) -> None:
        self.client: SQSClient | None = None
        self.queue_url: str = ""

    def bind(self, client: "SQSClient", queue_url: str) -> None:
        self.client = client
        self.queue_url = queue_url

    async def parse_message(self, message: "SQSRawMessage") -> SQSMessage:
        attributes: dict[str, Any] = message.get("MessageAttributes", {}) or {}

        headers: dict[str, Any] = {}
        content_type: str | None = None
        reply_to: str = ""
        correlation_id: str | None = None

        for name, attr in attributes.items():
            value = attr.get("StringValue", "")
            if name == "content-type":
                content_type = value
            elif name == "reply_to":
                reply_to = value
            elif name == "correlation_id":
                correlation_id = value
            else:
                headers[name] = value

        body = message.get("Body", "")
        raw_body = body.encode() if isinstance(body, str) else (body or b"")

        parsed = SQSMessage(
            raw_message=message,
            body=raw_body,
            headers=headers,
            content_type=content_type,
            reply_to=reply_to,
            correlation_id=correlation_id,
            message_id=message.get("MessageId"),
        )
        parsed.sqs_client = self.client
        parsed.queue_url = self.queue_url
        return parsed

    async def decode_message(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        return decode_message(msg)
