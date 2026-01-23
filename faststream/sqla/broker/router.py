from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Optional,
)

from sqlalchemy.ext.asyncio import AsyncEngine
from typing_extensions import deprecated

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream.middlewares import AckPolicy
from faststream.sqla.broker.registrator import SqlaRegistrator
from faststream.sqla.configs.broker import SqlaBrokerConfig
from faststream.sqla.message import SqlaInnerMessage
from faststream.sqla.retry import RetryStrategyProto

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )


class SqlaPublisher(ArgsContainer):
    """Delayed SqlaPublisher registration object.

    Just a copy of `SqlaRegistrator.publisher(...)` arguments.
    """

    def __init__(
        self,
        queue: str = "",
        *,
        headers: dict[str, str] | None = None,
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialize SqlaPublisher.

        Args:
            queue: Queue name where the message will be published.
            headers:
                Message headers to store metainformation.
                **content-type** and **correlation_id** will be set automatically by framework anyway.
                Can be overridden by `publish.headers` if specified.
            middlewares: Publisher middlewares to wrap outgoing messages.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema:
                AsyncAPI publishing message type.
                Should be any python-native object annotation or `pydantic.BaseModel`.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            queue=queue,
            headers=headers,
            middlewares=middlewares,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class SqlaRoute(SubscriberRoute):
    """Class to store delayed SqlaBroker subscriber registration."""

    def __init__(
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        queues: list[str],
        *,
        publishers: Iterable[SqlaPublisher] = (),
        engine: AsyncEngine,
        max_workers: int = 1,
        retry_strategy: RetryStrategyProto | None = None,
        max_fetch_interval: float,
        min_fetch_interval: float,
        fetch_batch_size: int,
        overfetch_factor: float,
        flush_interval: float,
        release_stuck_interval: float,
        release_stuck_timeout: float,
        max_deliveries: int | None = None,
        ack_policy: AckPolicy = AckPolicy.REJECT_ON_ERROR,
        # broker args
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # AsyncAPI args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialize SqlaRoute.

        Args:
            call:
                Message handler function
                to wrap the same with `@broker.subscriber(...)` way.
            queues: Queue names to consume messages from.
            publishers: Sqla publishers to broadcast the handler result.
            engine: SQLAlchemy async engine.
            max_workers: Number of workers to process messages concurrently.
            retry_strategy:
                Called to determine if and when a message might be retried.
            max_fetch_interval:
                The maximum allowed interval between consecutive fetches.
            min_fetch_interval:
                The minimum allowed interval between consecutive fetches.
            fetch_batch_size:
                The maximum allowed number of messages to fetch in a single batch.
            overfetch_factor:
                The factor by which the fetch_batch_size is multiplied.
            flush_interval:
                The interval at which the state of messages is flushed to the database.
            release_stuck_interval:
                The interval at which the PROCESSING-state messages are marked back as PENDING.
            release_stuck_timeout:
                Timeout for releasing stuck messages.
            max_deliveries:
                The maximum number of deliveries allowed for a message.
            ack_policy: Acknowledgement policy.
            dependencies: Dependencies list to apply to the subscriber.
            parser: Parser to map original message object to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            middlewares: Subscriber middlewares to wrap incoming message processing.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            call,
            publishers=publishers,
            queues=queues,
            engine=engine,
            max_workers=max_workers,
            retry_strategy=retry_strategy,
            max_fetch_interval=max_fetch_interval,
            min_fetch_interval=min_fetch_interval,
            fetch_batch_size=fetch_batch_size,
            overfetch_factor=overfetch_factor,
            flush_interval=flush_interval,
            release_stuck_interval=release_stuck_interval,
            release_stuck_timeout=release_stuck_timeout,
            max_deliveries=max_deliveries,
            ack_policy=ack_policy,
            # basic args
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            # AsyncAPI args
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )


class SqlaRouter(
    SqlaRegistrator,
    BrokerRouter[SqlaInnerMessage],
):
    """Includable to SqlaBroker router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[SqlaRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[SqlaRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
    ) -> None:
        """Initialize SqlaRouter.

        Args:
            prefix: String prefix to add to all subscribers queues.
            handlers: Route object to include.
            dependencies: Dependencies list to apply to all routers' publishers/subscribers.
            middlewares: Router middlewares to apply to all routers' publishers/subscribers.
            routers: Routers to apply to broker.
            parser: Parser to map original message object to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            handlers=handlers,
            config=SqlaBrokerConfig(
                broker_middlewares=middlewares,
                broker_dependencies=dependencies,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
                prefix=prefix,
            ),
            routers=routers,
        )
