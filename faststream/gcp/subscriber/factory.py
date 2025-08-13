"""GCP Pub/Sub subscriber factory."""

from typing import TYPE_CHECKING, Any

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.gcp.subscriber.config import GCPSubscriberConfig
from faststream.gcp.subscriber.specification import GCPSubscriberSpecification
from faststream.gcp.subscriber.usecase import GCPSubscriber

if TYPE_CHECKING:
    from faststream.gcp.broker.registrator import GCPRegistrator


def create_subscriber(
    subscription: str,
    *,
    broker: "GCPRegistrator",
    topic: str | None = None,
    create_subscription: bool = True,
    ack_deadline: int | None = None,
    max_messages: int = 10,
    no_ack: bool = EMPTY,
    **kwargs: Any,
) -> GCPSubscriber:
    """Create a GCP Pub/Sub subscriber.

    Args:
        subscription: Subscription name
        broker: Broker instance
        topic: Topic name (required if creating subscription)
        create_subscription: Whether to create subscription if it doesn't exist
        ack_deadline: ACK deadline in seconds
        max_messages: Maximum messages to pull at once
        no_ack: Whether to automatically acknowledge messages
        **kwargs: Additional subscriber options

    Returns:
        GCPSubscriber instance
    """
    calls: CallsCollection[Any] = CallsCollection()

    # Remove parser and decoder from kwargs (they are handled via add_call)
    kwargs.pop("parser", None)
    kwargs.pop("decoder", None)

    # Create subscriber configuration
    subscriber_config = GCPSubscriberConfig(
        _outer_config=broker.config,  # type: ignore[arg-type]
        subscription=subscription,
        topic=topic,
        create_subscription=create_subscription,
        ack_deadline=ack_deadline
        or getattr(broker.config, "subscriber_ack_deadline", 600),
        max_messages=max_messages,
        _no_ack=no_ack,
        **kwargs,
    )

    # Create specification
    specification = GCPSubscriberSpecification(
        subscription=subscription,
        topic=topic,
        _outer_config=broker.config,  # type: ignore[arg-type]
        calls=calls,
    )

    return GCPSubscriber(
        config=subscriber_config,
        specification=specification,
        calls=calls,
    )
