from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional, cast, override
from faststream.sqla.message import SqlaInnerMessage, SqlaMessage
from faststream.sqla.publisher.usecase import LogicPublisher
from typing_extensions import deprecated

from faststream._internal.types import PublisherMiddleware
from faststream.sqla.publisher.factory import create_publisher
from sqlalchemy.ext.asyncio import AsyncEngine
from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.sqla.configs.broker import SqlaBrokerConfig
from faststream.sqla.subscriber.factory import create_subscriber
from faststream.sqla.retry import NoRetryStrategy, RetryStrategyProto

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        CustomCallable,
        SubscriberMiddleware,
    )
    from faststream.sqla.subscriber.usecase import SqlaSubscriber


class SqlaRegistrator(Registrator[SqlaInnerMessage, SqlaBrokerConfig]):
    def subscriber(
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
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
    ) -> "SqlaSubscriber":
        """
        Args:
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

        Flow:
            On start, the subscriber spawns four types of concurrent loops:

            1. Fetch loop:
               Periodically fetches PENDING or RETRYABLE messages from the database,
               simultaneously updating them in the database: marking as PROCESSING,
               setting acquired_at to now, and incrementing deliveries_count. Only
               messages with next_attempt_at <= now are fetched, ordered by
               next_attempt_at. The fetched messages are placed into an internal
               queue. The fetch limit is the minimum of fetch_batch_size and the
               free buffer capacity (fetch_batch_size * overfetch_factor minus
               currently queued messages). If the last fetch was "full" (returned
               as many messages as the limit), the next fetch happens after
               min_fetch_interval; otherwise after max_fetch_interval.

            2. Worker loops (max_workers instances):
               Each worker takes a message from the internal queue and first checks
               if max_deliveries has been exceeded; if so, the message is Reject'ed
               without processing. Otherwise, processing proceeds. Depending on the
               processing result, AckPolicy, and manual Ack/Nack/Reject, the message
               is Ack'ed, Nack'ed, or Reject'ed. For Nack'ed messages, the
               retry_strategy is consulted to determine if and when the message
               might be retried. If allowed to be retried, the message
               is marked as RETRYABLE, otherwise as FAILED. Ack'ed messages are
               marked as COMPLETED and Reject'ed messages are marked as FAILED.
               The message is then buffered for flushing.

            3. Flush loop:
               Periodically flushes the buffered message state changes to the
               database. COMPLETED and FAILED messages are moved from the primary
               table to the archive table. The state of RETRYABLE messages is
               updated in the primary table.

            4. Release stuck loop:
               Periodically releases messages that have been stuck in PROCESSING
               state for longer than release_stuck_timeout since acquired_at. These
               messages are marked back as PENDING.

            On stop, all loops are gracefully stopped. Messages that have been
            acquired but are not yet being processed are drained from the internal
            queue and marked back as PENDING. The subscriber waits for all tasks to
            complete within graceful_shutdown_timeout, then performs a final flush.

        Notes:
            This design allows for work sharing between processes/nodes because
            "select for update skip locked" is utilized.

            This design adheres to the "at least once" processing guarantee because
            flushing changes to the database happens only after a processing
            attempt. A flush might not happen due to e.g. a crash. This might
            lead to messages being processed more times than allowed by the
            retry_strategy, and to the database state being inconsistent with
            the true number of attempts.

            Setting max_deliveries to a non-None value provides protection from
            the poison message problem (messages that crash the worker without
            the ability to catch the exception due to e.g. OOM terminations)
            because deliveries_count is incremented and max_deliveries is
            checked prior to a processing attempt. However, this comes
            at the expense of violating the "at most once" processing guarantee,
            especially for messages that are being processed at the same time
            with the poison message (a crash would leave them with incremented
            deliveries_count despite possibly not having been processed).
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

        super().subscriber(subscriber)

        subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

        return subscriber
    
    @override
    def publisher(
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