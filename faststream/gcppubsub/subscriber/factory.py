"""GCP Pub/Sub subscriber factory."""

from typing import TYPE_CHECKING, Any, Optional

from faststream.gcppubsub.subscriber.usecase import GCPPubSubSubscriber

if TYPE_CHECKING:
    from faststream.gcppubsub.broker.registrator import GCPPubSubRegistrator


def create_subscriber(
    subscription: str,
    *,
    broker: "GCPPubSubRegistrator",
    topic: Optional[str] = None,
    create_subscription: bool = True,
    ack_deadline: Optional[int] = None,
    max_messages: int = 10,
    **kwargs: Any,
) -> GCPPubSubSubscriber:
    """Create a GCP Pub/Sub subscriber.
    
    Args:
        subscription: Subscription name
        broker: Broker instance
        topic: Topic name (required if creating subscription)
        create_subscription: Whether to create subscription if it doesn't exist
        ack_deadline: ACK deadline in seconds
        max_messages: Maximum messages to pull at once
        **kwargs: Additional subscriber options
    
    Returns:
        GCPPubSubSubscriber instance
    """
    return GCPPubSubSubscriber(
        subscription=subscription,
        broker=broker,
        topic=topic,
        create_subscription=create_subscription,
        ack_deadline=ack_deadline,
        max_messages=max_messages,
        **kwargs,
    )