"""GCP Pub/Sub message parser."""

from typing import TYPE_CHECKING, Optional

from gcloud.aio.pubsub import PubsubMessage

from faststream.message import StreamMessage, decode_message, gen_cor_id
from faststream.gcppubsub.message import GCPPubSubMessage

if TYPE_CHECKING:
    from faststream._internal.basic_types import DecodedMessage


class GCPPubSubParser:
    """A class for parsing, encoding, and decoding GCP Pub/Sub messages."""

    def __init__(self) -> None:
        pass

    async def parse_message(
        self,
        message: PubsubMessage,
    ) -> StreamMessage[PubsubMessage]:
        """Parses an incoming message and returns a GCPPubSubMessage object."""
        # Handle both PubsubMessage and SubscriberMessage objects
        message_id = gen_cor_id()
        attributes = {}
        
        if hasattr(message, 'message_id') and message.message_id:
            message_id = message.message_id
        if hasattr(message, 'attributes') and message.attributes:
            attributes = message.attributes
            
        return GCPPubSubMessage(
            raw_message=message,
            correlation_id=attributes.get("correlation_id") or gen_cor_id(),
            reply_to="",  # GCP Pub/Sub doesn't have built-in reply-to
        )

    async def decode_message(
        self,
        msg: StreamMessage[PubsubMessage],
    ) -> "DecodedMessage":
        """Decode a message."""
        return decode_message(msg)