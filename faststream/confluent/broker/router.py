from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Literal, Optional, Union

from typing_extensions import deprecated

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream._internal.constants import EMPTY
from faststream.confluent.configs import KafkaBrokerConfig
from faststream.middlewares import AckPolicy

from .registrator import KafkaRegistrator

if TYPE_CHECKING:
    from confluent_kafka import Message
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.confluent.message import KafkaMessage
    from faststream.confluent.schemas import TopicPartition


class KafkaPublisher(ArgsContainer):
    """Delayed KafkaPublisher registration object.

    Just a copy of `KafkaRegistrator.publisher(...)` arguments.
    """

    def __init__(
        self,
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        batch: bool = False,
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # AsyncAPI args
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialize KafkaPublisher.

        Args:
            topic: Topic where the message will be published.
            key: A key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is `None` (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is `None`, partition is chosen randomly).
                Must be type `bytes`, or be serializable to bytes via configured
                `key_serializer`.
            partition: Specify a partition. If not set, the partition will be
                selected using the configured `partitioner`.
            headers: Message headers to store metainformation.
                **content-type** and **correlation_id** will be set automatically by framework anyway.
                Can be overridden by `publish.headers` if specified.
            reply_to: Topic name to send response.
            batch: Whether to send messages in batches or not.
            middlewares: Publisher middlewares to wrap outgoing messages.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type.
                Should be any python-native object annotation or `pydantic.BaseModel`.
            include_in_schema: Whetever to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            topic=topic,
            key=key,
            partition=partition,
            batch=batch,
            headers=headers,
            reply_to=reply_to,
            # basic args
            middlewares=middlewares,
            # AsyncAPI args
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class KafkaRoute(SubscriberRoute):
    """Class to store delaied KafkaBroker subscriber registration."""

    def __init__(
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        *topics: str,
        publishers: Iterable[KafkaPublisher] = (),
        partitions: Sequence["TopicPartition"] = (),
        polling_interval: float = 0.1,
        group_id: str | None = None,
        group_instance_id: str | None = None,
        fetch_max_wait_ms: int = 500,
        fetch_max_bytes: int = 50 * 1024 * 1024,
        fetch_min_bytes: int = 1,
        max_partition_fetch_bytes: int = 1 * 1024 * 1024,
        auto_offset_reset: Literal["latest", "earliest", "none"] = "latest",
        auto_commit: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        auto_commit_interval_ms: int = 5 * 1000,
        check_crcs: bool = True,
        partition_assignment_strategy: Sequence[str] = ("roundrobin",),
        max_poll_interval_ms: int = 5 * 60 * 1000,
        session_timeout_ms: int = 10 * 1000,
        heartbeat_interval_ms: int = 3 * 1000,
        isolation_level: Literal[
            "read_uncommitted",
            "read_committed",
        ] = "read_uncommitted",
        batch: bool = False,
        max_records: int | None = None,
        # broker args
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        no_ack: Annotated[
            bool,
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.MANUAL**. "
                "Scheduled to remove in 0.7.0",
            ),
        ] = EMPTY,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: int | None = None,
    ) -> None:
        """Initialize KafkaRoute.

        Args:
            call: Message handler function.
            *topics: Kafka topics to consume messages from.
            publishers: Kafka publishers to broadcast the handler result.
            partitions: Sequence of topic partitions.
            polling_interval: Polling interval in seconds.
            group_id: Name of the consumer group to join for dynamic
                partition assignment (if enabled), and to use for fetching and
                committing offsets. If `None`, auto-partition assignment (via
                group coordinator) and offset commits are disabled.
            group_instance_id: A unique string that identifies the consumer instance.
                If set, the consumer is treated as a static member of the group
                and does not participate in consumer group management (e.g.
                partition assignment, rebalances). This can be used to assign
                partitions to specific consumers, rather than letting the group
                assign partitions based on consumer metadata.
            fetch_max_wait_ms: The maximum amount of time in milliseconds
                the server will block before answering the fetch request if
                there isn't sufficient data to immediately satisfy the
                requirement given by `fetch_min_bytes`.
            fetch_max_bytes: The maximum amount of data the server should
                return for a fetch request. This is not an absolute maximum, if
                the first message in the first non-empty partition of the fetch
                is larger than this value, the message will still be returned
                to ensure that the consumer can make progress. NOTE: consumer
                performs fetches to multiple brokers in parallel so memory
                usage will depend on the number of brokers containing
                partitions for the topic.
            fetch_min_bytes: Minimum amount of data the server should
                return for a fetch request, otherwise wait up to
                `fetch_max_wait_ms` for more data to accumulate.
            max_partition_fetch_bytes: The maximum amount of data
                per-partition the server will return. The maximum total memory
                used for a request ``= #partitions * max_partition_fetch_bytes``.
                This size must be at least as large as the maximum message size
                the server allows or else it is possible for the producer to
                send messages larger than the consumer can fetch. If that
                happens, the consumer can get stuck trying to fetch a large
                message on a certain partition.
            auto_offset_reset: A policy for resetting offsets on `OffsetOutOfRangeError` errors:

                * `earliest` will move to the oldest available message
                * `latest` will move to the most recent
                * `none` will raise an exception so you can handle this case
            auto_commit: If `True` the consumer's offset will be
                periodically committed in the background.
            auto_commit_interval_ms: Milliseconds between automatic
                offset commits, if `auto_commit` is `True`.
            check_crcs: Automatically check the CRC32 of the records
                consumed. This ensures no on-the-wire or on-disk corruption to
                the messages occurred. This check adds some overhead, so it may
                be disabled in cases seeking extreme performance.
            partition_assignment_strategy: List of objects to use to
                distribute partition ownership amongst consumer instances when
                group management is used. This preference is implicit in the order
                of the strategies in the list. When assignment strategy changes:
                to support a change to the assignment strategy, new versions must
                enable support both for the old assignment strategy and the new
                one. The coordinator will choose the old assignment strategy until
                all members have been updated. Then it will choose the new
                strategy.
            max_poll_interval_ms: Maximum allowed time between calls to
                consume messages in batches. If this interval
                is exceeded the consumer is considered failed and the group will
                rebalance in order to reassign the partitions to another consumer
                group member. If API methods block waiting for messages, that time
                does not count against this timeout.
            session_timeout_ms: Client group session and failure detection
                timeout. The consumer sends periodic heartbeats
                (`heartbeat.interval.ms`) to indicate its liveness to the broker.
                If no hearts are received by the broker for a group member within
                the session timeout, the broker will remove the consumer from the
                group and trigger a rebalance. The allowed range is configured with
                the **broker** configuration properties
                `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            heartbeat_interval_ms: The expected time in milliseconds
                between heartbeats to the consumer coordinator when using
                Kafka's group management feature. Heartbeats are used to ensure
                that the consumer's session stays active and to facilitate
                rebalancing when new consumers join or leave the group. The
                value must be set lower than `session_timeout_ms`, but typically
                should be set no higher than 1/3 of that value. It can be
                adjusted even lower to control the expected time for normal
                rebalances.
            isolation_level: Controls how to read messages written
                transactionally.

                * `read_committed`, batch consumer will only return
                transactional messages which have been committed.

                * `read_uncommitted` (the default), batch consumer will
                return all messages, even transactional messages which have been
                aborted.

                Non-transactional messages will be returned unconditionally in
                either mode.

                Messages will always be returned in offset order. Hence, in
                `read_committed` mode, batch consumer will only return
                messages up to the last stable offset (ALSO), which is the one less
                than the offset of the first open transaction. In particular any
                messages appearing after messages belonging to ongoing transactions
                will be withheld until the relevant transaction has been completed.
                As a result, `read_committed` consumers will not be able to read up
                to the high watermark when there are in flight transactions.
                Further, when in `read_committed` the seek_to_end method will
                return the ALSO. See method docs below.
            batch: Whether to consume messages in batches or not.
            max_records: Number of messages to consume as one batch.
            dependencies: Dependencies list (`[Dependant(),]`) to apply to the subscriber.
            parser: Parser to map original **Message** object to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            middlewares: Subscriber middlewares to wrap incoming message processing.
            no_ack: Whether to disable **FastStream** auto acknowledgement logic or not.
            ack_policy: Acknowledgement policy.
            no_reply: Whether to disable **FastStream** RPC and Reply To auto responses or not.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description.
                Uses decorated docstring as default.
            include_in_schema: Whetever to include operation in AsyncAPI schema or not.
            max_workers: Number of workers to process messages concurrently.
        """
        super().__init__(
            call,
            *topics,
            publishers=publishers,
            max_workers=max_workers,
            partitions=partitions,
            polling_interval=polling_interval,
            group_id=group_id,
            group_instance_id=group_instance_id,
            fetch_max_wait_ms=fetch_max_wait_ms,
            fetch_max_bytes=fetch_max_bytes,
            fetch_min_bytes=fetch_min_bytes,
            max_partition_fetch_bytes=max_partition_fetch_bytes,
            auto_offset_reset=auto_offset_reset,
            auto_commit=auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            check_crcs=check_crcs,
            partition_assignment_strategy=partition_assignment_strategy,
            max_poll_interval_ms=max_poll_interval_ms,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            isolation_level=isolation_level,
            max_records=max_records,
            batch=batch,
            # basic args
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            no_reply=no_reply,
            # AsyncAPI args
            title=title,
            description=description,
            include_in_schema=include_in_schema,
            ack_policy=ack_policy,
            no_ack=no_ack,
        )


class KafkaRouter(
    KafkaRegistrator,
    BrokerRouter[
        Union[
            "Message",
            tuple["Message", ...],
        ]
    ],
):
    """Includable to KafkaBroker router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[KafkaRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[KafkaRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
    ) -> None:
        """Initialize KafkaRouter.

        Args:
            prefix: String prefix to add to all subscribers queues.
            handlers: Route object to include.
            dependencies: Dependencies list (`[Dependant(),]`) to apply to all routers' publishers/subscribers.
            middlewares: Router middlewares to apply to all routers' publishers/subscribers.
            routers: Routers to apply to broker.
            parser: Parser to map original **Message** object to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            include_in_schema: Whetever to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            handlers=handlers,
            config=KafkaBrokerConfig(
                broker_middlewares=middlewares,
                broker_dependencies=dependencies,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
                prefix=prefix,
            ),
            routers=routers,
        )
