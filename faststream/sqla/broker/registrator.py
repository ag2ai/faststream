from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional, cast

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
        queues:
            List of queue names to consume from.
        max_workers:
            Number of workers to process messages concurrently.
        retry_strategy:
            Called to determine if and when a Nack'ed message is retried. If None,
            `AckPolicy.NACK_ON_ERROR` has the same effect as
            `AckPolicy.REJECT_ON_ERROR`.
        min_fetch_interval:
            Minimum interval between consecutive fetches, used if the last fetch
            was full (returned as many messages as the fetch's limit).
        max_fetch_interval:
            Maximum interval between consecutive fetches.
        fetch_batch_size:
            Maximum number of messages to fetch in a single batch. A fetch's actual
            limit might be lower if the free capacity of the
            acquired-but-not-yet-in-processing buffer is smaller.
        overfetch_factor:
            Multiplier for `fetch_batch_size` to size the internal buffer of
            acquired-but-not-yet-processing messages.
        flush_interval:
            Interval between flushes of processed message state to the database.
        release_stuck_interval:
            Interval between checks for stuck `PROCESSING` messages.
        release_stuck_timeout:
            Interval since `acquired_at` after which a `PROCESSING` message is considered
            stuck and is released back to `PENDING`.
        max_deliveries:
            Maximum number of deliveries allowed for a message. If set, messages that
            have reached this limit are Reject'ed to `FAILED` without processing.
        ack_policy:
            Controls acknowledgement behavior.
        """
        subscriber = create_subscriber(
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
