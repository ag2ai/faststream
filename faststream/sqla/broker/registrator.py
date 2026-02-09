from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional, cast

from sqlalchemy.ext.asyncio import AsyncEngine
from typing_extensions import deprecated, override

from faststream._internal.broker.registrator import Registrator
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.configs.broker import SqlaBrokerConfig
from faststream.sqla.message import SqlaInnerMessage
from faststream.sqla.publisher.factory import create_publisher
from faststream.sqla.retry import RetryStrategyProto
from faststream.sqla.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.sqla.publisher.usecase import LogicPublisher
    from faststream.sqla.subscriber.usecase import SqlaSubscriber


class SqlaRegistrator(Registrator[SqlaInnerMessage, SqlaBrokerConfig]):
    @override
    def subscriber(  # type: ignore[override]
        self,
        queues: list[str],
        *,
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
        persistent: bool = True,
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        # AsyncAPI args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "SqlaSubscriber":
        """Args:
            max_workers:
                Number of workers to process messages concurrently.
            retry_strategy:
                Called to determine if and when a message might be retried. If None,
                AckPolicy.NACK_ON_ERROR has the same effect as
                AckPolicy.REJECT_ON_ERROR.
            min_fetch_interval:
                The minimum allowed interval between consecutive fetches. The
                minimum interval is used if the last fetch returned the same number
                of messages as the fetch's limit.
            max_fetch_interval:
                The maximum allowed interval between consecutive fetches. The
                maximum interval is used if the last fetch returned fewer messages
                than the fetch's limit.
            fetch_batch_size:
                The maximum allowed number of messages to fetch in a single batch.
                A fetch's actual limit might be lower if the free capacity of the
                acquired-but-not-yet-in-processing buffer is smaller.
            overfetch_factor:
                The factor by which the fetch_batch_size is multiplied to determine
                the capacity of the acquired-but-not-yet-in-processing buffer.
            flush_interval:
                The interval at which the state of messages for which the processing
                attempt has been completed or aborted is flushed to the database.
            release_stuck_interval:
                The interval at which the PROCESSING-state messages are marked back
                as PENDING if the release_stuck_timeout since acquired_at has passed.
            max_deliveries:
                The maximum number of deliveries allowed for a message. If
                set, messages that have reached this limit are Reject'ed without
                processing.
        """
        subscriber = create_subscriber(
            engine=engine,
            queues=queues,
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
            config=cast("SqlaBrokerConfig", self.config),
            ack_policy=ack_policy,
        )

        super().subscriber(subscriber, persistent=persistent)

        subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

        return subscriber

    @override
    def publisher(  # type: ignore[override]
        self,
        queue: str = "",
        *,
        headers: dict[str, str] | None = None,
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. "
                "Use router-level middlewares instead. "
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "LogicPublisher":
        publisher = create_publisher(
            queue=queue,
            headers=headers,
            # Specific
            broker_config=cast("SqlaBrokerConfig", self.config),
            middlewares=middlewares,
            # AsyncAPI
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )

        super().publisher(publisher)

        return publisher
