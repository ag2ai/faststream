"""GCP Pub/Sub publisher factory."""

from typing import TYPE_CHECKING, Any

from faststream.gcppubsub.publisher.usecase import GCPPubSubPublisher

if TYPE_CHECKING:
    from faststream.gcppubsub.broker.registrator import GCPPubSubRegistrator


def create_publisher(
    topic: str,
    *,
    broker: "GCPPubSubRegistrator",
    create_topic: bool = True,
    ordering_key: str | None = None,
    **kwargs: Any,
) -> GCPPubSubPublisher:
    """Create a GCP Pub/Sub publisher.

    Args:
        topic: Topic name
        broker: Broker instance
        create_topic: Whether to create topic if it doesn't exist
        ordering_key: Message ordering key
        **kwargs: Additional publisher options

    Returns:
        GCPPubSubPublisher instance
    """
    return GCPPubSubPublisher(
        topic=topic,
        create_topic=create_topic,
        ordering_key=ordering_key,
        config=broker.config,
        **kwargs,
    )
