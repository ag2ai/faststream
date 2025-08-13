"""GCP Pub/Sub message parser."""

from typing import TYPE_CHECKING

from gcloud.aio.pubsub import PubsubMessage

from faststream.gcppubsub.message import GCPPubSubMessage
from faststream.message import StreamMessage, decode_message, gen_cor_id

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
        attributes = {}

        if hasattr(message, "attributes") and message.attributes:
            # For PubsubMessage, user attributes are nested in attributes['attributes']
            if "attributes" in message.attributes and isinstance(
                message.attributes["attributes"], dict
            ):
                attributes = message.attributes["attributes"]
            else:
                attributes = message.attributes

        return GCPPubSubMessage(
            raw_message=message,
            correlation_id=attributes.get("correlation_id") or gen_cor_id(),
            # Don't set reply_to - let it default to None
        )

    async def decode_message(
        self,
        msg: StreamMessage[PubsubMessage],
    ) -> "DecodedMessage":
        """Decode a message."""
        return decode_message(msg)
