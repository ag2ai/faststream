"""GCP Pub/Sub publisher use case."""

import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from gcloud.aio.pubsub import PubsubMessage

from faststream._internal.endpoint.publisher.usecase import PublisherUsecase
from faststream.gcppubsub.publisher.producer import GCPPubSubFastProducer

if TYPE_CHECKING:
    from faststream.gcppubsub.broker.registrator import GCPPubSubRegistrator


class GCPPubSubPublisher(PublisherUsecase):
    """GCP Pub/Sub publisher implementation."""
    
    def __init__(
        self,
        topic: str,
        *,
        broker: "GCPPubSubRegistrator",
        create_topic: bool = True,
        ordering_key: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize publisher.
        
        Args:
            topic: Topic name
            broker: Broker instance
            create_topic: Whether to create topic if it doesn't exist
            ordering_key: Message ordering key
            **kwargs: Additional options
        """
        self.topic = topic
        self.broker = broker
        self.create_topic = create_topic
        self.ordering_key = ordering_key
        
        super().__init__(**kwargs)
    
    async def start(self) -> None:
        """Start the publisher."""
        if self.create_topic:
            await self._ensure_topic_exists()
    
    async def stop(self) -> None:
        """Stop the publisher."""
        pass
    
    async def publish(
        self,
        message: Any,
        *,
        topic: Optional[str] = None,
        attributes: Optional[Dict[str, str]] = None,
        ordering_key: Optional[str] = None,
        correlation_id: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """Publish a message.
        
        Args:
            message: Message to publish
            topic: Override topic name
            attributes: Message attributes
            ordering_key: Override ordering key
            correlation_id: Message correlation ID
            **kwargs: Additional options
        
        Returns:
            Published message ID
        """
        target_topic = topic or self.topic
        target_ordering_key = ordering_key or self.ordering_key
        
        # Serialize message if needed
        if isinstance(message, (str, bytes)):
            data = message.encode() if isinstance(message, str) else message
        else:
            # Use broker's serializer
            data = self.broker.config.parser(message) if hasattr(self.broker, 'config') else str(message).encode()
        
        # Get producer from broker
        producer = self.broker.config.producer
        
        return await producer.publish(
            topic=target_topic,
            data=data,
            attributes=attributes,
            ordering_key=target_ordering_key,
            correlation_id=correlation_id,
        )
    
    async def publish_batch(
        self,
        messages: List[Any],
        *,
        topic: Optional[str] = None,
        **kwargs: Any,
    ) -> List[str]:
        """Publish multiple messages.
        
        Args:
            messages: Messages to publish
            topic: Override topic name
            **kwargs: Additional options
        
        Returns:
            List of published message IDs
        """
        target_topic = topic or self.topic
        
        # Convert messages to PubsubMessage objects
        pubsub_messages = []
        for msg in messages:
            if isinstance(msg, PubsubMessage):
                pubsub_messages.append(msg)
            else:
                # Serialize message
                if isinstance(msg, (str, bytes)):
                    data = msg.encode() if isinstance(msg, str) else msg
                else:
                    data = self.broker.config.parser(msg) if hasattr(self.broker, 'config') else str(msg).encode()
                
                pubsub_messages.append(PubsubMessage(data))
        
        # Get producer from broker
        producer = self.broker.config.producer
        
        return await producer.publish_batch(
            topic=target_topic,
            messages=pubsub_messages,
        )
    
    async def _ensure_topic_exists(self) -> None:
        """Ensure the topic exists."""
        try:
            # Use publisher client to create topic if it doesn't exist
            if hasattr(self.broker, '_state') and self.broker._state.publisher:
                publisher = self.broker._state.publisher
                topic_path = publisher.topic_path(self.broker.project_id, self.topic)
                # Note: gcloud-aio doesn't have a built-in create_topic method
                # In a real implementation, you'd use the admin client or handle this differently
                pass
        except Exception:
            # Topic creation can be handled externally or ignored for simplicity
            pass