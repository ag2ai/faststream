"""GCP Pub/Sub broker registrator."""

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from gcloud.aio.pubsub import PubsubMessage

from faststream._internal.broker.registrator import Registrator
from faststream.gcppubsub.publisher.factory import create_publisher
from faststream.gcppubsub.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.gcppubsub.publisher.usecase import GCPPubSubPublisher
    from faststream.gcppubsub.subscriber.usecase import GCPPubSubSubscriber


class GCPPubSubRegistrator(Registrator[PubsubMessage]):
    """GCP Pub/Sub broker registrator."""

    def subscriber(  # type: ignore[override]
        self,
        subscription: str,
        *,
        topic: str | None = None,
        create_subscription: bool = True,
        ack_deadline: int | None = None,
        max_messages: int = 10,
        # Handler arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        **kwargs: Any,
    ) -> "GCPPubSubSubscriber":
        """Create a subscriber.

        Args:
            subscription: Subscription name
            topic: Topic name (required if creating subscription)
            create_subscription: Whether to create subscription if it doesn't exist
            ack_deadline: ACK deadline in seconds
            max_messages: Maximum messages to pull at once
            dependencies: Dependencies list to apply to the subscriber
            parser: Parser to map original **PubsubMessage** to FastStream one
            decoder: Function to decode FastStream msg bytes body to python objects
            middlewares: Subscriber middlewares to wrap incoming message processing
            title: AsyncAPI subscriber object title
            description: AsyncAPI subscriber object description
            include_in_schema: Whether to include operation in AsyncAPI schema
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
            parser=parser,
            decoder=decoder,
            **kwargs,
        )

        super().subscriber(subscriber)

        return subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

    def publisher(  # type: ignore[override]
        self,
        topic: str,
        *,
        create_topic: bool = True,
        ordering_key: str | None = None,
        middlewares: Sequence["PublisherMiddleware"] = (),
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        **kwargs: Any,
    ) -> "GCPPubSubPublisher":
        """Create a publisher.

        Args:
            topic: Topic name
            create_topic: Whether to create topic if it doesn't exist
            ordering_key: Message ordering key
            middlewares: Publisher middlewares to wrap outgoing message processing
            title: AsyncAPI publisher object title
            description: AsyncAPI publisher object description
            include_in_schema: Whether to include operation in AsyncAPI schema
            **kwargs: Additional publisher options

        Returns:
            GCPPubSubPublisher instance
        """
        publisher = create_publisher(
            topic=topic,
            create_topic=create_topic,
            ordering_key=ordering_key,
            middlewares=middlewares,
            broker=self,
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
            **kwargs,
        )

        super().publisher(publisher)
        return publisher
