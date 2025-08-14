"""GCP Pub/Sub OpenTelemetry telemetry settings provider."""

from typing import TYPE_CHECKING, Any

from gcloud.aio.pubsub import PubsubMessage
from opentelemetry.semconv.trace import SpanAttributes

from faststream.gcp.response import GCPPublishCommand
from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME

if TYPE_CHECKING:
    from faststream.message import StreamMessage


class GCPTelemetrySettingsProvider(
    TelemetrySettingsProvider[PubsubMessage, GCPPublishCommand],
):
    """GCP Pub/Sub telemetry settings provider for OpenTelemetry."""

    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        """Initialize GCP telemetry settings provider."""
        self.messaging_system = "gcp_pubsub"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[PubsubMessage]",
    ) -> dict[str, Any]:
        """Extract telemetry attributes from incoming GCP Pub/Sub message.

        Args:
            msg: StreamMessage containing PubsubMessage

        Returns:
            Dictionary of span attributes for consumer operations
        """
        attrs = {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_MESSAGE_ID: msg.message_id,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            MESSAGING_DESTINATION_PUBLISH_NAME: msg.path.get("topic", ""),
        }

        # Add GCP Pub/Sub specific attributes
        if "topic" in msg.path:
            attrs["messaging.gcp_pubsub.topic"] = msg.path["topic"]

        if "subscription" in msg.path:
            attrs["messaging.gcp_pubsub.subscription"] = msg.path["subscription"]

        # Add ordering key if available
        if hasattr(msg, "ordering_key") and msg.ordering_key:
            attrs["messaging.gcp_pubsub.ordering_key"] = msg.ordering_key

        # Add publish time if available
        if hasattr(msg, "publish_time") and msg.publish_time:
            attrs["messaging.gcp_pubsub.publish_time"] = str(msg.publish_time)

        return attrs

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[PubsubMessage]",
    ) -> str:
        """Get destination name for consumer spans.

        Args:
            msg: StreamMessage containing PubsubMessage

        Returns:
            Destination name for the span (subscription name)
        """
        # Use subscription name for consumer spans
        subscription = msg.path.get("subscription")
        if subscription:
            return str(subscription)
        return "unknown-subscription"

    def get_publish_attrs_from_cmd(
        self,
        cmd: GCPPublishCommand,
    ) -> dict[str, Any]:
        """Extract telemetry attributes from publish command.

        Args:
            cmd: GCP publish command

        Returns:
            Dictionary of span attributes for publisher operations
        """
        attrs = {
            SpanAttributes.MESSAGING_SYSTEM: self.messaging_system,
            SpanAttributes.MESSAGING_DESTINATION_NAME: cmd.topic,
            SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: cmd.correlation_id,
        }

        # Add GCP Pub/Sub specific attributes
        attrs["messaging.gcp_pubsub.topic"] = cmd.topic

        # Add ordering key if specified
        if cmd.ordering_key:
            attrs["messaging.gcp_pubsub.ordering_key"] = cmd.ordering_key

        # Add message size if available
        if hasattr(cmd, "message") and cmd.message:
            try:
                # Try to get size of the message body
                if isinstance(cmd.message, (str, bytes, list, tuple)):
                    attrs[SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES] = len(
                        cmd.message
                    )  # type: ignore[assignment]
            except (TypeError, AttributeError):
                # If we can't determine size, skip this attribute
                pass

        return attrs

    def get_publish_destination_name(
        self,
        cmd: GCPPublishCommand,
    ) -> str:
        """Get destination name for publisher spans.

        Args:
            cmd: GCP publish command

        Returns:
            Destination name for the span (topic name)
        """
        return cmd.topic

    def _get_project_id(self, msg: "StreamMessage[PubsubMessage]") -> str:
        """Extract project ID from message context.

        Args:
            msg: StreamMessage containing PubsubMessage

        Returns:
            Project ID string, defaults to "unknown" if not found
        """
        # Try to get project ID from message context or broker config
        # This might be available in the message headers or path
        if hasattr(msg, "raw_message") and hasattr(msg.raw_message, "attributes"):
            project_id = msg.raw_message.attributes.get("project_id")
            if project_id:
                return str(project_id)

        # Could also check message headers
        project_id = msg.headers.get("project_id")
        if project_id:
            return str(project_id)

        # Default fallback
        return "unknown"


def telemetry_attributes_provider_factory(
    msg: PubsubMessage | None,
) -> GCPTelemetrySettingsProvider | None:
    """Factory function to create GCP telemetry settings provider.

    Args:
        msg: PubsubMessage or None

    Returns:
        GCPTelemetrySettingsProvider instance or None
    """
    if isinstance(msg, PubsubMessage) or msg is None:
        return GCPTelemetrySettingsProvider()

    # For unsupported message types, return None
    return None
