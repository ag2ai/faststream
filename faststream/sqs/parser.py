import base64
from typing import TYPE_CHECKING, Any, cast

from faststream.message import StreamMessage, decode_message

from .message import SQSBatchMessage, SQSMessage

if TYPE_CHECKING:
    from types_aiobotocore_sqs import SQSClient

    from faststream._internal.basic_types import DecodedMessage

    from .message import SQSRawMessage


# SQS forbids an empty ``MessageBody``; we send a placeholder and flag it with
# this reserved attribute so the parser can restore the original empty body.
EMPTY_BODY_ATTR = "empty-body"
EMPTY_BODY_PLACEHOLDER = " "

# SQS accepts only text bodies; non-UTF-8 payloads are sent base64-encoded and
# flagged with this reserved attribute so the parser can restore the raw bytes.
BASE64_BODY_ATTR = "base64-body"

# Message attribute names FastStream reserves for transport metadata.
RESERVED_ATTRS = (
    "content-type",
    "reply_to",
    "correlation_id",
    EMPTY_BODY_ATTR,
    BASE64_BODY_ATTR,
)


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

    @staticmethod
    def _attr_value(attr: dict[str, Any]) -> Any:
        """Decode a single ``MessageAttribute`` honouring its ``DataType``.

        SQS attributes are typed String/Number/Binary. Binary values arrive as
        raw bytes; String/Number arrive as strings (Number is a numeric string).
        """
        data_type = str(attr.get("DataType", "String"))
        if data_type.startswith("Binary"):
            return attr.get("BinaryValue", b"")
        return attr.get("StringValue", "")

    async def parse_message(self, message: "SQSRawMessage") -> SQSMessage:
        attributes: dict[str, Any] = message.get("MessageAttributes", {}) or {}

        headers: dict[str, Any] = {}
        content_type: str | None = None
        reply_to: str = ""
        correlation_id: str | None = None
        empty_body = False
        base64_body = False

        for name, attr in attributes.items():
            value = self._attr_value(attr)
            if name == "content-type":
                content_type = value
            elif name == "reply_to":
                reply_to = value
            elif name == "correlation_id":
                correlation_id = value
            elif name == EMPTY_BODY_ATTR:
                empty_body = True
            elif name == BASE64_BODY_ATTR:
                base64_body = True
            else:
                headers[name] = value

        body = message.get("Body", "")
        raw_body = body.encode() if isinstance(body, str) else (body or b"")
        if empty_body:
            raw_body = b""
        elif base64_body:
            raw_body = base64.b64decode(raw_body)

        # SQS system attributes (ApproximateReceiveCount, MessageGroupId, ...)
        system_attributes = cast("dict[str, str]", message.get("Attributes", {}) or {})

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
        parsed.system_attributes = system_attributes
        return parsed

    async def decode_message(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        return decode_message(msg)

    async def parse_batch(
        self,
        messages: list["SQSRawMessage"],
    ) -> SQSBatchMessage:
        """Parse a batch of raw SQS messages into a single ``SQSBatchMessage``.

        The message body is the list of raw bodies; transport metadata is taken
        from the first message, matching the batch convention of other brokers.
        """
        bodies: list[Any] = []
        batch_headers: list[dict[str, Any]] = []
        singles: list[SQSMessage] = []

        for message in messages:
            single = await self.parse_message(message)
            singles.append(single)
            bodies.append(single.body)
            batch_headers.append(single.headers)

        first = singles[0] if singles else None
        parsed = SQSBatchMessage(
            raw_message=cast("Any", messages),
            body=bodies,
            headers=batch_headers[0] if batch_headers else {},
            batch_headers=batch_headers,
            content_type=first.content_type if first else None,
            reply_to=first.reply_to if first else "",
            correlation_id=first.correlation_id if first else None,
            message_id=first.message_id if first else None,
        )
        parsed.sqs_client = self.client
        parsed.queue_url = self.queue_url
        parsed.system_attributes = first.system_attributes if first else {}
        parsed.batch_system_attributes = [s.system_attributes for s in singles]
        # Keep the per-message parses so decode_batch doesn't re-parse the batch.
        parsed.parsed_messages = singles
        return parsed

    async def decode_batch(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        singles: list[SQSMessage] = getattr(msg, "parsed_messages", [])
        if not singles:  # a batch built outside parse_batch
            singles = [await self.parse_message(m) for m in msg.raw_message]
        return [decode_message(single) for single in singles]
