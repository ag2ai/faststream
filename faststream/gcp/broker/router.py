"""GCP Pub/Sub broker router."""

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional

from gcloud.aio.pubsub import PubsubMessage
from typing_extensions import Doc

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream.gcp.broker.registrator import GCPRegistrator

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from fast_depends.dependencies import Dependant

    from faststream._internal.broker.registrator import Registrator
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )


class GCPPublisher(ArgsContainer):
    """Delayed GCP Pub/Sub publisher registration object.

    Just a copy of `GCPRegistrator.publisher(...)` arguments.
    """

    def __init__(
        self,
        topic: Annotated[
            str,
            Doc("Topic name to publish messages to"),
        ],
        *,
        create_topic: Annotated[
            bool,
            Doc("Whether to create topic if it doesn't exist"),
        ] = True,
        ordering_key: Annotated[
            str | None,
            Doc("Message ordering key"),
        ] = None,
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            Doc("Publisher middlewares to wrap outgoing message processing"),
        ] = (),
        # AsyncAPI information
        title: Annotated[
            str | None,
            Doc("AsyncAPI publisher object title"),
        ] = None,
        description: Annotated[
            str | None,
            Doc("AsyncAPI publisher object description"),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whether to include operation in AsyncAPI schema"),
        ] = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            topic=topic,
            create_topic=create_topic,
            ordering_key=ordering_key,
            middlewares=middlewares,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
            **kwargs,
        )


class GCPRoute(SubscriberRoute):
    """Class to store delayed GCP Pub/Sub subscriber registration.

    Just a copy of `GCPRegistrator.subscriber(...)` arguments.
    """

    def __init__(
        self,
        call: Annotated[
            "Callable[..., Any] | Callable[..., Awaitable[Any]]",
            Doc("Message handler function"),
        ],
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
    ) -> None:
        super().__init__(
            call=call,
            subscription=subscription,
            topic=topic,
            create_subscription=create_subscription,
            ack_deadline=ack_deadline,
            max_messages=max_messages,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
            **kwargs,
        )


class GCPRouter(GCPRegistrator, BrokerRouter[PubsubMessage]):
    """GCP Pub/Sub message router."""

    def __init__(
        self,
        prefix: Annotated[
            str,
            Doc("String prefix to add to all subscribers and publishers topics."),
        ] = "",
        handlers: Annotated[
            Iterable[GCPRoute],
            Doc("Route object to include."),
        ] = (),
        *,
        dependencies: Annotated[
            Iterable["Dependant"],
            Doc(
                "Dependencies list (`[Dependant(),]`) to apply to all routers' publishers/subscribers.",
            ),
        ] = (),
        middlewares: Annotated[
            Sequence["BrokerMiddleware[Any]"],
            Doc("Router middlewares to apply to all routers' publishers/subscribers."),
        ] = (),
        routers: Annotated[
            Sequence["Registrator[PubsubMessage]"],
            Doc("Routers to apply to broker."),
        ] = (),
        parser: Annotated[
            "CustomCallable | None",
            Doc("Parser to map original **PubsubMessage** to FastStream one."),
        ] = None,
        decoder: Annotated[
            "CustomCallable | None",
            Doc("Function to decode FastStream msg bytes body to python objects."),
        ] = None,
        include_in_schema: Annotated[
            bool | None,
            Doc("Whether to include operation in AsyncAPI schema or not."),
        ] = None,
        tags: Annotated[
            list[str] | None,
            Doc("AsyncAPI tags for documentation"),
        ] = None,
    ) -> None:
        """Initialize GCP Pub/Sub router.

        Args:
            prefix: String prefix to add to all topics
            handlers: Route objects to include
            dependencies: Dependencies to apply to all publishers/subscribers
            middlewares: Router middlewares
            routers: Routers to apply to broker
            parser: Parser to map PubsubMessage to FastStream message
            decoder: Function to decode message bytes body
            include_in_schema: Whether to include in AsyncAPI schema
            tags: AsyncAPI tags for documentation
        """
        from faststream._internal.configs.broker import BrokerConfig

        # Store tags for documentation purposes
        self.tags = tags or []

        # Initialize lifespan hooks
        self._on_startup_hooks: list[Any] = []
        self._on_shutdown_hooks: list[Any] = []

        super().__init__(
            handlers=handlers,
            config=BrokerConfig(
                broker_middlewares=middlewares,
                broker_dependencies=dependencies,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
                prefix=prefix,
            ),
            routers=routers,
        )

    def on_startup(self, func: Any) -> Any:
        """Add startup hook to router."""
        self._on_startup_hooks.append(func)
        return func

    def on_shutdown(self, func: Any) -> Any:
        """Add shutdown hook to router."""
        self._on_shutdown_hooks.append(func)
        return func
