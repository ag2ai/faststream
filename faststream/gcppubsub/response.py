"""GCP Pub/Sub response types."""

from typing import TYPE_CHECKING, Any, Dict, Optional

from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage


class GCPPubSubPublishCommand:
    """GCP Pub/Sub publish command."""
    
    def __init__(
        self,
        message: "SendableMessage",
        *,
        topic: str,
        attributes: Optional[Dict[str, str]] = None,
        ordering_key: Optional[str] = None,
        correlation_id: Optional[str] = None,
        _publish_type: PublishType = PublishType.PUBLISH,
        timeout: Optional[float] = 30.0,
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
        self.message = message
        self.topic = topic
        self.attributes = attributes or {}
        self.ordering_key = ordering_key
        self.correlation_id = correlation_id
        self.publish_type = _publish_type
        self.timeout = timeout
    
    def to_dict(self) -> Dict[str, Any]:
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