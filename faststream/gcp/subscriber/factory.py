"""GCP Pub/Sub subscriber factory."""

from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.gcp.subscriber.config import GCPSubscriberConfig
from faststream.gcp.subscriber.specification import GCPSubscriberSpecification
from faststream.gcp.subscriber.usecase import GCPSubscriber

if TYPE_CHECKING:
    from faststream._internal.types import CustomCallable
    from faststream.gcp.broker.registrator import GCPRegistrator


def create_subscriber(
    subscription: str,
    *,
    broker: "GCPRegistrator",
    topic: str | None = None,
    create_subscription: bool = True,
    no_ack: bool = EMPTY,
    parser: Optional["CustomCallable"] = None,
    decoder: Optional["CustomCallable"] = None,
    ack_deadline: int | None = None,
    max_messages: int | None = None,
    **kwargs: Any,
) -> GCPSubscriber:
    """Create a GCP Pub/Sub subscriber.

    Args:
        subscription: Subscription name
        broker: Broker instance
        topic: Topic name (required if creating subscription)
        create_subscription: Whether to create subscription if it doesn't exist
        no_ack: Whether to automatically acknowledge messages
        parser: Parser to map original PubsubMessage to FastStream one
        decoder: Function to decode FastStream msg bytes body to python objects
        ack_deadline: Message acknowledgment deadline (overrides broker config)
        max_messages: Maximum messages to pull at once (overrides broker config)
        **kwargs: Additional subscriber options

    Returns:
        GCPSubscriber instance
    """
    calls: CallsCollection[Any] = CallsCollection()

    # Use provided parameters or fallback to broker config
    final_ack_deadline = (
        ack_deadline
        if ack_deadline is not None
        else broker.config.subscriber.ack_deadline
    )
    final_max_messages = (
        max_messages
        if max_messages is not None
        else broker.config.subscriber.max_messages
    )

    # Create subscriber configuration - let kwargs handle all other legitimate parameters
    subscriber_config = GCPSubscriberConfig(
        _outer_config=broker.config,  # type: ignore[arg-type]
        subscription=subscription,
        topic=topic,
        create_subscription=create_subscription,
        ack_deadline=final_ack_deadline,
        max_messages=final_max_messages,
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
