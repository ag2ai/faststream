"""GCP Pub/Sub broker registrator."""

from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.broker.registrator import Registrator
from faststream.gcppubsub.publisher.factory import create_publisher
from faststream.gcppubsub.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from gcloud.aio.pubsub import PubsubMessage

    from faststream.gcppubsub.publisher.usecase import GCPPubSubPublisher
    from faststream.gcppubsub.subscriber.usecase import GCPPubSubSubscriber


class GCPPubSubRegistrator(Registrator["PubsubMessage"]):
    """GCP Pub/Sub broker registrator."""
    
    def subscriber(
        self,
        subscription: str,
        *,
        topic: Optional[str] = None,
        create_subscription: bool = True,
        ack_deadline: Optional[int] = None,
        max_messages: int = 10,
        **kwargs: Any,
    ) -> "GCPPubSubSubscriber":
        """Create a subscriber.
        
        Args:
            subscription: Subscription name
            topic: Topic name (required if creating subscription)
            create_subscription: Whether to create subscription if it doesn't exist
            ack_deadline: ACK deadline in seconds
            max_messages: Maximum messages to pull at once
            **kwargs: Additional subscriber options
        
        Returns:
            GCPPubSubSubscriber instance
        """
        subscriber = create_subscriber(
            subscription=subscription,
            topic=topic,
            create_subscription=create_subscription,
            ack_deadline=ack_deadline,
            max_messages=max_messages,
            broker=self,
            **kwargs,
        )
        
        self.subscribers.append(subscriber)
        return subscriber
    
    def publisher(
        self,
        topic: str,
        *,
        create_topic: bool = True,
        ordering_key: Optional[str] = None,
        **kwargs: Any,
    ) -> "GCPPubSubPublisher":
        """Create a publisher.
        
        Args:
            topic: Topic name
            create_topic: Whether to create topic if it doesn't exist
            ordering_key: Message ordering key
            **kwargs: Additional publisher options
        
        Returns:
            GCPPubSubPublisher instance
        """
        publisher = create_publisher(
            topic=topic,
            create_topic=create_topic,
            ordering_key=ordering_key,
            broker=self,
            **kwargs,
        )
        
        self.publishers.append(publisher)
        return publisher