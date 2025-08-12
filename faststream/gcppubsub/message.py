"""GCP Pub/Sub message wrapper."""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from gcloud.aio.pubsub import PubsubMessage

from faststream.message import StreamMessage

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict


class GCPPubSubMessage(StreamMessage[PubsubMessage]):
    """Wrapper around GCP Pub/Sub message."""
    
    def __init__(
        self,
        raw_message: PubsubMessage,
        *,
        correlation_id: Optional[str] = None,
        reply_to: Optional[str] = None,
        ack_id: Optional[str] = None,
        subscription: Optional[str] = None,
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
        
        super().__init__(
            raw_message=raw_message,
            body=raw_message.data,
            path={
                "topic": raw_message.attributes.get("topic", ""),
                "subscription": subscription or "",
            },
            reply_to=reply_to,
            headers={
                **raw_message.attributes,
                "message_id": message_id,
                "publish_time": publish_time,
                "ordering_key": raw_message.ordering_key or "",
            },
            content_type=raw_message.attributes.get("content_type"),
            message_id=message_id,
            correlation_id=correlation_id,
            **kwargs,
        )
    
    @property
    def ack_id(self) -> Optional[str]:
        """Get the acknowledgment ID."""
        return self._ack_id
    
    @property
    def subscription(self) -> Optional[str]:
        """Get the source subscription."""
        return self._subscription
    
    @property
    def acknowledged(self) -> bool:
        """Check if message has been acknowledged."""
        return self._acknowledged
    
    
    @property
    def publish_time(self) -> Optional[str]:
        """Get the publish time."""
        return self.raw_message.attributes.get("publish_time")
    
    @property
    def ordering_key(self) -> Optional[str]:
        """Get the ordering key."""
        return getattr(self.raw_message, "ordering_key", None)
    
    @property
    def attributes(self) -> Dict[str, str]:
        """Get message attributes."""
        return dict(self.raw_message.attributes)
    
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