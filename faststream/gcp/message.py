"""GCP Pub/Sub message wrapper."""

from typing import TYPE_CHECKING, Any

from gcloud.aio.pubsub import PubsubMessage

from faststream.message import StreamMessage

if TYPE_CHECKING:
    AnyDict = dict[str, Any]


class GCPMessage(StreamMessage[PubsubMessage]):
    """Wrapper around GCP Pub/Sub message."""

    def __init__(
        self,
        raw_message: PubsubMessage,
        *,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        ack_id: str | None = None,
        subscription: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize message wrapper.

        Args:
            raw_message: Raw Pub/Sub message
            correlation_id: Message correlation ID
            reply_to: Reply topic
            ack_id: Acknowledgment ID for the message
            subscription: Source subscription
            **kwargs: Additional message metadata
        """
        self._ack_id = ack_id
        self._subscription = subscription
        self._acknowledged = False

        # Extract correlation ID from attributes if not provided
        if correlation_id is None:
            correlation_id = raw_message.attributes.get("correlation_id")

        # Extract reply-to from attributes if not provided
        if reply_to is None:
            reply_to = raw_message.attributes.get("reply_to")

        # Get message_id and publish_time from attributes
        message_id = raw_message.attributes.get("message_id", "")
        publish_time = raw_message.attributes.get("publish_time", "")

        # Handle empty message marker
        body = raw_message.data
        if raw_message.attributes.get("__faststream_empty") == "true" and body == b" ":
            body = b""

        super().__init__(
            raw_message=raw_message,
            body=body,
            path={
                "topic": raw_message.attributes.get("topic", ""),
                "subscription": subscription or "",
            },
            reply_to=reply_to or "",
            headers={
                **raw_message.attributes,
                "message_id": message_id,
                "publish_time": publish_time,
                "ordering_key": raw_message.ordering_key or "",
            },
            content_type=self._get_content_type_from_attributes(raw_message.attributes),
            message_id=message_id,
            correlation_id=correlation_id,
            **kwargs,
        )

    def _get_content_type_from_attributes(self, attributes: dict[str, str]) -> str | None:
        """Extract content_type from potentially nested attributes structure."""
        if not attributes:
            return None

        # For PubsubMessage, user attributes are nested in attributes['attributes']
        if "attributes" in attributes and isinstance(attributes["attributes"], dict):
            return attributes["attributes"].get("content_type")
        return attributes.get("content_type")

    @property
    def ack_id(self) -> str | None:
        """Get the acknowledgment ID."""
        return self._ack_id

    @property
    def subscription(self) -> str | None:
        """Get the source subscription."""
        return self._subscription

    @property
    def acknowledged(self) -> bool:
        """Check if message has been acknowledged."""
        return self._acknowledged

    @property
    def publish_time(self) -> str | None:
        """Get the publish time."""
        return self.raw_message.attributes.get("publish_time")

    @property
    def ordering_key(self) -> str | None:
        """Get the ordering key."""
        return getattr(self.raw_message, "ordering_key", None)

    @property
    def attributes(self) -> dict[str, str]:
        """Get message attributes."""
        # For PubsubMessage, user attributes are nested in attributes['attributes']
        if hasattr(self.raw_message, "attributes") and self.raw_message.attributes:
            if "attributes" in self.raw_message.attributes and isinstance(
                self.raw_message.attributes["attributes"], dict
            ):
                return dict(self.raw_message.attributes["attributes"])
            return dict(self.raw_message.attributes)
        return {}

    @property
    def message_id(self) -> str:
        """Get the message ID."""
        return (
            getattr(self, "_message_id", "")
            or self.raw_message.attributes.get("message_id", "")
            or self.correlation_id
        )

    @message_id.setter
    def message_id(self, value: str | None) -> None:
        """Set the message ID."""
        self._message_id = value

    async def ack(self) -> None:
        """Acknowledge the message."""
        # This will be implemented by the subscriber
        self._acknowledged = True

    async def nack(self) -> None:
        """Negative acknowledge the message."""
        # This will be implemented by the subscriber
        self._acknowledged = False

    def as_dict(self) -> "AnyDict":
        """Convert message to dictionary.

        Returns:
            Dictionary representation of the message
        """
        return {
            "message_id": self.message_id,
            "data": self.body,
            "attributes": self.attributes,
            "publish_time": self.publish_time,
            "ordering_key": self.ordering_key,
            "ack_id": self.ack_id,
            "subscription": self.subscription,
            "correlation_id": self.correlation_id,
        }
