"""GCP Pub/Sub response types."""

from typing import TYPE_CHECKING, Any, Dict, Optional

from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from gcloud.aio.pubsub import PubsubMessage


class GCPPubSubPublishCommand:
    """GCP Pub/Sub publish command."""
    
    def __init__(
        self,
        topic: str,
        message: "PubsubMessage",
        ordering_key: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Initialize publish command.
        
        Args:
            topic: Topic to publish to
            message: Message to publish
            ordering_key: Message ordering key
            correlation_id: Correlation ID for tracking
        """
        self.topic = topic
        self.message = message
        self.ordering_key = ordering_key
        self.correlation_id = correlation_id
        self.publish_type = PublishType.PUBLISH
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.
        
        Returns:
            Dictionary representation
        """
        return {
            "topic": self.topic,
            "data": self.message.data,
            "attributes": self.message.attributes,
            "ordering_key": self.ordering_key,
            "correlation_id": self.correlation_id,
        }