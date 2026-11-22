from typing import TYPE_CHECKING, Any

from faststream._internal.constants import ContentTypes
from faststream.message import decode_message

if TYPE_CHECKING:
    from faststream._internal.basic_types import DecodedMessage
    from faststream.message import StreamMessage


def decode_kafka_message(message: "StreamMessage[Any]") -> "DecodedMessage":
    """Decode a single Kafka record."""
    body = message.body
    content_type = message.content_type
    # A null record can still carry a JSON header (see issue #1967).
    if (
        not message.has_body
        and isinstance(body, bytes)
        and not body
        and isinstance(content_type, str)
        and content_type.startswith(ContentTypes.JSON.value)
    ):
        return body
    return decode_message(message)
