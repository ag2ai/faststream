from typing import Annotated, Any, Sequence, cast, override
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
from faststream.sqla.retry import RetryStrategyProto


class SqlaRegistrator(Registrator[Any, Any]):
    def subscriber(
        self,
        queues: list[str],
        *,
        engine: AsyncEngine,
        max_workers: int,
        retry_strategy: RetryStrategyProto,
        max_fetch_interval: float,
        min_fetch_interval: float,
        fetch_batch_size: int,
        overfetch_factor: float,
        flush_interval: float,
        release_stuck_interval: float,
        graceful_shutdown_timeout: float,
        release_stuck_timeout: int,
        ack_policy: AckPolicy = AckPolicy.NACK_ON_ERROR,
    ) -> Any:
        """
        Args:
            max_workers:
                Number of workers to process messages concurrently.
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

        Flow:
            On start, the subscriber spawns four types of concurrent loops:

            1. Fetch loop:
               Periodically fetches PENDING or RETRYABLE messages from the database,
               simultaneously updating them in the database: marking as PROCESSING,
               setting acquired_at to now, and incrementing attempts_count. Only
               messages with next_attempt_at <= now are fetched, ordered by
               next_attempt_at. The fetched messages are placed into an internal
               queue. The fetch limit is the minimum of fetch_batch_size and the
               free buffer capacity (fetch_batch_size * overfetch_factor minus
               currently queued messages). If the last fetch was "full" (returned
               as many messages as the limit), the next fetch happens after
               min_fetch_interval; otherwise after max_fetch_interval.

            2. Worker loops (max_workers instances):
               Each worker takes a message from the internal queue and checks if
               the attempt is allowed by the retry_strategy. If allowed, the message
               is processed, if not, Reject'ed. Depending on the processing result,
               AckPolicy, and manual Ack/Nack/Reject, the message is Ack'ed, Nack'ed,
               or Reject'ed. For Nack'ed messages the retry_strategy is consulted to
               determine if and when the message might be retried. If allowed to be
               retried, the message is marked as RETRYABLE, otherwise as FAILED.
               Ack'ed messages are marked as COMPLETED and Reject'ed messages are
               marked as FAILED. The message is then buffered for flushing.

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
            attempt. Messages might be processed more times than allowed by the
            retry_strategy if, among other things, the flush doesn't happen due to
            crash or failure after a message is processed.

            This design handles the poison message problem (messages that crash the
            worker without the ability to catch the exception due to e.g. OOM
            terminations) because attempts_count is incremented and retry_strategy
            is consulted with prior to processing attempt.
        
        SQL queries:
            Fetch:
                WITH ready AS
                    (SELECT message.id AS id,
                            message.queue AS queue,
                            message.payload AS payload,
                            message.state AS state,
                            message.attempts_count AS attempts_count,
                            message.created_at AS created_at,
                            message.first_attempt_at AS first_attempt_at,
                            message.next_attempt_at AS next_attempt_at,
                            message.last_attempt_at AS last_attempt_at,
                            message.acquired_at AS acquired_at
                    FROM message
                    WHERE (message.state = $3::sqlamessagestate
                            OR message.state = $4::sqlamessagestate)
                        AND message.next_attempt_at <= now()
                        AND (message.queue = $5::VARCHAR OR message.queue = $6::VARCHAR) 
                    ORDER BY message.next_attempt_at
                    LIMIT $7::INTEGER
                    FOR UPDATE SKIP LOCKED),
                    updated AS
                    (UPDATE message
                    SET state=$1::sqlamessagestate,
                        attempts_count=(message.attempts_count + $2::SMALLINT),
                        acquired_at=now()
                    WHERE message.id IN
                            (SELECT ready.id
                            FROM ready) RETURNING message.id,
                                                  message.queue,
                                                  message.payload,
                                                  message.state,
                                                  message.attempts_count,
                                                  message.created_at,
                                                  message.first_attempt_at,
                                                  message.next_attempt_at,
                                                  message.last_attempt_at,
                                                  message.acquired_at)
                SELECT updated.id,
                       updated.queue,
                       updated.payload,
                       updated.state,
                       updated.attempts_count,
                       updated.created_at,
                       updated.first_attempt_at,
                       updated.next_attempt_at,
                       updated.last_attempt_at,
                       updated.acquired_at
                FROM updated
                ORDER BY updated.next_attempt_at;
            
            Flush:
                For RETRYABLE messages:
                    UPDATE message
                    SET state=$1::sqlamessagestate,
                        first_attempt_at=$2::datetime,
                        next_attempt_at=$3::datetime,
                        last_attempt_at=$4::datetime,
                        acquired_at=$5::datetime
                    WHERE message.id = $6::BIGINT;

                For COMPLETED and FAILED messages:
                    BEGIN;
                    INSERT INTO message_archive (
                        id,
                        queue,
                        payload,
                        state,
                        attempts_count,
                        created_at,
                        first_attempt_at,
                        last_attempt_at,
                        archived_at
                    )
                    VALUES (
                        $1::BIGINT,
                        $2::VARCHAR,
                        $3::BYTEA,
                        $4::sqlamessagestate,
                        $5::SMALLINT,
                        $6::TIMESTAMP WITH TIME ZONE,
                        $7::TIMESTAMP WITH TIME ZONE,
                        $8::TIMESTAMP WITH TIME ZONE,
                        $9::TIMESTAMP WITH TIME ZONE
                    );
                    DELETE
                    FROM message
                    WHERE message.id IN ($1::BIGINT);
                    COMMIT;
            
            Release stuck:
                UPDATE message
                SET state=$1::sqlamessagestate,
                    next_attempt_at=now(),
                    acquired_at=$2::TIMESTAMP WITH TIME ZONE
                WHERE message.id IN
                    (SELECT message.id
                    FROM message
                    WHERE message.state = $3::sqlamessagestate
                        AND message.acquired_at < $4::TIMESTAMP WITH TIME ZONE)
        """
        workers = max_workers or 1

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
            graceful_shutdown_timeout=graceful_shutdown_timeout,
            release_stuck_timeout=release_stuck_timeout,
            config=cast("SqlaBrokerConfig", self.config),
            ack_policy=ack_policy,
        )

        super().subscriber(subscriber)

        # subscriber.add_call(
        #     parser_=parser,
        #     decoder_=decoder,
        #     dependencies_=dependencies,
        #     middlewares_=middlewares,
        # )

        return subscriber
    
    @override
    def publisher(
        self,
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
    ) -> "LogicPublisher":
        publisher = create_publisher(
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