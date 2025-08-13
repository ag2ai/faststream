"""GCP Pub/Sub response types."""

from typing import TYPE_CHECKING, Any

from faststream.response.publish_type import PublishType
from faststream.response.response import PublishCommand

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage


class GCPPublishCommand(PublishCommand):
    """GCP Pub/Sub publish command."""

    def __init__(
        self,
        message: "SendableMessage",
        *,
        topic: str,
        attributes: dict[str, str] | None = None,
        ordering_key: str | None = None,
        correlation_id: str | None = None,
        _publish_type: PublishType = PublishType.PUBLISH,
        timeout: float | None = 30.0,
    ) -> None:
        """Initialize publish command.

        Args:
            message: Message to publish
            topic: Topic to publish to
            attributes: Message attributes
            ordering_key: Message ordering key
            correlation_id: Correlation ID for tracking
            _publish_type: Type of publish operation
            timeout: Publish timeout
        """
        super().__init__(
            body=message,
            destination=topic,
            correlation_id=correlation_id,
            headers=attributes or {},
            _publish_type=_publish_type,
        )
        # Store GCP-specific attributes
        self.message = message
        self.topic = topic
        self.attributes = attributes or {}
        self.ordering_key = ordering_key
        self.timeout = timeout

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "message": self.message,
            "topic": self.topic,
            "attributes": self.attributes,
            "ordering_key": self.ordering_key,
            "correlation_id": self.correlation_id,
        }
