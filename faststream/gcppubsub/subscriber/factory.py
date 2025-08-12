"""GCP Pub/Sub subscriber factory."""

from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.gcppubsub.subscriber.config import GCPPubSubSubscriberConfig
from faststream.gcppubsub.subscriber.specification import GCPPubSubSubscriberSpecification
from faststream.gcppubsub.subscriber.usecase import GCPPubSubSubscriber

if TYPE_CHECKING:
    from faststream.gcppubsub.broker.registrator import GCPPubSubRegistrator


def create_subscriber(
    subscription: str,
    *,
    broker: "GCPPubSubRegistrator",
    topic: str | None = None,
    create_subscription: bool = True,
    ack_deadline: int | None = None,
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
    calls: CallsCollection[Any] = CallsCollection()

    # Create subscriber configuration
    subscriber_config = GCPPubSubSubscriberConfig(
        _outer_config=broker.config,  # type: ignore[arg-type]
        subscription=subscription,
        topic=topic,
        create_subscription=create_subscription,
        ack_deadline=ack_deadline or broker.config.subscriber_ack_deadline,
        max_messages=max_messages,
        **kwargs,
    )

    # Create specification
    specification = GCPPubSubSubscriberSpecification(
        subscription=subscription,
        topic=topic,
        _outer_config=broker.config,  # type: ignore[arg-type]
        calls=calls,
    )

    return GCPPubSubSubscriber(
        config=subscriber_config,
        specification=specification,
        calls=calls,
    )
