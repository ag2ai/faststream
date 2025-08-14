"""GCP Pub/Sub Prometheus metrics settings provider."""

from typing import TYPE_CHECKING

from gcloud.aio.pubsub import PubsubMessage

from faststream.gcp.response import GCPPublishCommand
from faststream.prometheus import ConsumeAttrs, MetricsSettingsProvider

if TYPE_CHECKING:
    from faststream.message.message import StreamMessage


class GCPMetricsSettingsProvider(
    MetricsSettingsProvider[PubsubMessage, GCPPublishCommand],
):
    """GCP Pub/Sub metrics settings provider for Prometheus."""

    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        """Initialize GCP metrics settings provider."""
        self.messaging_system = "gcp_pubsub"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[PubsubMessage]",
    ) -> ConsumeAttrs:
        """Extract consume attributes from GCP Pub/Sub message."""
        # Use topic name for destination, fallback to subscription
        destination_name = (
            msg.path.get("topic") or msg.path.get("subscription") or "unknown"
        )

        return {
            "destination_name": destination_name,
            "message_size": len(msg.body),
            "messages_count": 1,
        }

    def get_publish_destination_name_from_cmd(
        self,
        cmd: GCPPublishCommand,
    ) -> str:
        """Get destination name from publish command."""
        return cmd.topic


def settings_provider_factory(
    msg: PubsubMessage | None,
) -> GCPMetricsSettingsProvider:
    """Factory function to create GCP metrics settings provider.

    Args:
        msg: PubsubMessage or None

    Returns:
        GCPMetricsSettingsProvider instance
    """
    return GCPMetricsSettingsProvider()
