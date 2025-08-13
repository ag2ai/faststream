"""GCP Pub/Sub publisher factory."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from faststream.gcp.publisher.usecase import GCPPublisher

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware
    from faststream.gcp.broker.registrator import GCPRegistrator


def create_publisher(
    topic: str,
    *,
    broker: "GCPRegistrator",
    create_topic: bool = True,
    ordering_key: str | None = None,
    middlewares: Sequence["PublisherMiddleware"] = (),
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
    **kwargs: Any,
) -> GCPPublisher:
    """Create a GCP Pub/Sub publisher.

    Args:
        topic: Topic name
        broker: Broker instance
        create_topic: Whether to create topic if it doesn't exist
        ordering_key: Message ordering key
        middlewares: Publisher middlewares
        title_: AsyncAPI title
        description_: AsyncAPI description
        include_in_schema: Whether to include in schema
        **kwargs: Additional publisher options

    Returns:
        GCPPublisher instance
    """
    return GCPPublisher(
        topic=topic,
        create_topic=create_topic,
        ordering_key=ordering_key,
        middlewares=middlewares,
        config=broker.config,  # type: ignore[arg-type]
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
        **kwargs,
    )
