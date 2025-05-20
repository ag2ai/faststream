from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
)

from aiokafka import ConsumerRecord
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from typing_extensions import Annotated, Doc, deprecated, override

from faststream.broker.core.abc import ABCBroker
from faststream.broker.utils import default_filter
from faststream.exceptions import SetupError
from faststream.kafka.subscriber.factory import create_publisher, create_subscriber

if TYPE_CHECKING:
    from aiokafka import TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener
    from aiokafka.coordinator.assignors.abstract import AbstractPartitionAssignor
    from fast_depends.dependencies import Depends

    from faststream.broker.types import (
        BrokerMiddleware,
        CustomCallable,
        Filter,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.kafka.message import KafkaMessage
    from faststream.kafka.publisher.asyncapi import (
        AsyncAPIBatchPublisher,
        AsyncAPIDefaultPublisher,
    )
    from faststream.kafka.subscriber.asyncapi import (
        AsyncAPIBatchSubscriber,
        AsyncAPIConcurrentBetweenPartitionsSubscriber,
        AsyncAPIConcurrentDefaultSubscriber,
        AsyncAPIDefaultSubscriber,
    )


class KafkaRegistrator(
    ABCBroker[
        Union[
            ConsumerRecord,
            Tuple[ConsumerRecord, ...],
        ]
    ]
):
    """Includable to KafkaBroker router."""

    _subscribers: Dict[
        int,
        Union[
            "AsyncAPIBatchSubscriber",
            "AsyncAPIDefaultSubscriber",
            "AsyncAPIConcurrentDefaultSubscriber",
            "AsyncAPIConcurrentBetweenPartitionsSubscriber",
        ],
    ]
    _publishers: Dict[
        int,
        Union["AsyncAPIBatchPublisher", "AsyncAPIDefaultPublisher"],
    ]

    @overload  # type: ignore[override]
    def subscriber(
        self,
        *topics: Annotated[
            str,
            Doc("Kafka topics to consume messages from."),
        ],
        batch: Annotated[
            Literal[False],
            Doc("Whether to consume messages in batches or not."),
        ] = False,
        group_id: Annotated[
            Optional[str],
            Doc(
                """
            Name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If `None`, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            """
            ),
        ] = None,
        key_deserializer: Annotated[
            Optional[Callable[[bytes], Any]],
            Doc(
                "Any callable that takes a raw message `bytes` "
                "key and returns a deserialized one."
            ),
        ] = None,
        value_deserializer: Annotated[
            Optional[Callable[[bytes], Any]],
            Doc(
                "Any callable that takes a raw message `bytes` "
                "value and returns a deserialized value."
            ),
        ] = None,
        fetch_max_bytes: Annotated[
            int,
            Doc(
                """
            The maximum amount of data the server should
            return for a fetch request. This is not an absolute maximum, if
            the first message in the first non-empty partition of the fetch
            is larger than this value, the message will still be returned
            to ensure that the consumer can make progress. NOTE: consumer
            performs fetches to multiple brokers in parallel so memory
            usage will depend on the number of brokers containing
            partitions for the topic.
            """
            ),
        ] = 50 * 1024 * 1024,
        fetch_min_bytes: Annotated[
            int,
            Doc(
                """
            Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            `fetch_max_wait_ms` for more data to accumulate.
            """
            ),
        ] = 1,
        fetch_max_wait_ms: Annotated[
            int,
            Doc(
                """
            The maximum amount of time in milliseconds
            the server will block before answering the fetch request if
            there isn't sufficient data to immediately satisfy the
            requirement given by `fetch_min_bytes`.
            """
            ),
        ] = 500,
        max_partition_fetch_bytes: Annotated[
            int,
            Doc(
                """
            The maximum amount of data
            per-partition the server will return. The maximum total memory
            used for a request ``= #partitions * max_partition_fetch_bytes``.
            This size must be at least as large as the maximum message size
            the server allows or else it is possible for the producer to
            send messages larger than the consumer can fetch. If that
            happens, the consumer can get stuck trying to fetch a large
            message on a certain partition.
            """
            ),
        ] = 1 * 1024 * 1024,
        auto_offset_reset: Annotated[
            Literal["latest", "earliest", "none"],
            Doc(
                """
            A policy for resetting offsets on `OffsetOutOfRangeError` errors:

            * `earliest` will move to the oldest available message
            * `latest` will move to the most recent
            * `none` will raise an exception so you can handle this case
            """
            ),
        ] = "latest",
        auto_commit: Annotated[
            bool,
            Doc(
                """
            If `True` the consumer's offset will be
            periodically committed in the background.
            """
            ),
        ] = True,
        auto_commit_interval_ms: Annotated[
            int,
            Doc(
                """
            Milliseconds between automatic
            offset commits, if `auto_commit` is `True`."""
            ),
        ] = 5 * 1000,
        check_crcs: Annotated[
            bool,
            Doc(
                """
            Automatically check the CRC32 of the records
            consumed. This ensures no on-the-wire or on-disk corruption to
            the messages occurred. This check adds some overhead, so it may
            be disabled in cases seeking extreme performance.
            """
            ),
        ] = True,
        partition_assignment_strategy: Annotated[
            Sequence["AbstractPartitionAssignor"],
            Doc(
                """
            List of objects to use to
            distribute partition ownership amongst consumer instances when
            group management is used. This preference is implicit in the order
            of the strategies in the list. When assignment strategy changes:
            to support a change to the assignment strategy, new versions must
            enable support both for the old assignment strategy and the new
            one. The coordinator will choose the old assignment strategy until
            all members have been updated. Then it will choose the new
            strategy.
            """
            ),
        ] = (RoundRobinPartitionAssignor,),
        max_poll_interval_ms: Annotated[
            int,
            Doc(
                """
            Maximum allowed time between calls to
            consume messages in batches. If this interval
            is exceeded the consumer is considered failed and the group will
            rebalance in order to reassign the partitions to another consumer
            group member. If API methods block waiting for messages, that time
            does not count against this timeout.
            """
            ),
        ] = 5 * 60 * 1000,
        rebalance_timeout_ms: Annotated[
            Optional[int],
            Doc(
                """
            The maximum time server will wait for this
            consumer to rejoin the group in a case of rebalance. In Java client
            this behaviour is bound to `max.poll.interval.ms` configuration,
            but as ``aiokafka`` will rejoin the group in the background, we
            decouple this setting to allow finer tuning by users that use
            `ConsumerRebalanceListener` to delay rebalacing. Defaults
            to ``session_timeout_ms``
            """
            ),
        ] = None,
        session_timeout_ms: Annotated[
            int,
            Doc(
                """
            Client group session and failure detection
            timeout. The consumer sends periodic heartbeats
            (`heartbeat.interval.ms`) to indicate its liveness to the broker.
            If no hearts are received by the broker for a group member within
            the session timeout, the broker will remove the consumer from the
            group and trigger a rebalance. The allowed range is configured with
            the **broker** configuration properties
            `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            """
            ),
        ] = 10 * 1000,
        heartbeat_interval_ms: Annotated[
            int,
            Doc(
                """
            The expected time in milliseconds
            between heartbeats to the consumer coordinator when using
            Kafka's group management feature. Heartbeats are used to ensure
            that the consumer's session stays active and to facilitate
            rebalancing when new consumers join or leave the group. The
            value must be set lower than `session_timeout_ms`, but typically
            should be set no higher than 1/3 of that value. It can be
            adjusted even lower to control the expected time for normal
            rebalances.
            """
            ),
        ] = 3 * 1000,
        consumer_timeout_ms: Annotated[
            int,
            Doc(
                """
            Maximum wait timeout for background fetching
            routine. Mostly defines how fast the system will see rebalance and
            request new data for new partitions.
            """
            ),
        ] = 200,
        max_poll_records: Annotated[
            Optional[int],
            Doc(
                """
            The maximum number of records returned in a
            single call by batch consumer. Has no limit by default.
            """
            ),
        ] = None,
        exclude_internal_topics: Annotated[
            bool,
            Doc(
                """
            Whether records from internal topics
            (such as offsets) should be exposed to the consumer. If set to True
            the only way to receive records from an internal topic is
            subscribing to it.
            """
            ),
        ] = True,
        isolation_level: Annotated[
            Literal["read_uncommitted", "read_committed"],
            Doc(
                """
            Controls how to read messages written
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
            messages up to the last stable offset (LSO), which is the one less
            than the offset of the first open transaction. In particular any
            messages appearing after messages belonging to ongoing transactions
            will be withheld until the relevant transaction has been completed.
            As a result, `read_committed` consumers will not be able to read up
            to the high watermark when there are in flight transactions.
            Further, when in `read_committed` the seek_to_end method will
            return the LSO. See method docs below.
            """
            ),
        ] = "read_uncommitted",
        batch_timeout_ms: Annotated[
            int,
            Doc(
                """
            Milliseconds spent waiting if
            data is not available in the buffer. If 0, returns immediately
            with any records that are available currently in the buffer,
            else returns empty.
            """
            ),
        ] = 200,
        max_records: Annotated[
            Optional[int],
            Doc("Number of messages to consume as one batch."),
        ] = None,
        listener: Annotated[
            Optional["ConsumerRebalanceListener"],
            Doc(
                """
            Optionally include listener
               callback, which will be called before and after each rebalance
               operation.
               As part of group management, the consumer will keep track of
               the list of consumers that belong to a particular group and
               will trigger a rebalance operation if one of the following
               events trigger:

               * Number of partitions change for any of the subscribed topics
               * Topic is created or deleted
               * An existing member of the consumer group dies
               * A new member is added to the consumer group

               When any of these events are triggered, the provided listener
               will be invoked first to indicate that the consumer's
               assignment has been revoked, and then again when the new
               assignment has been received. Note that this listener will
               immediately override any listener set in a previous call
               to subscribe. It is guaranteed, however, that the partitions
               revoked/assigned
               through this interface are from topics subscribed in this call.
            """
            ),
        ] = None,
        pattern: Annotated[
            Optional[str],
            Doc(
                """
            Pattern to match available topics. You must provide either topics or pattern, but not both.
            """
            ),
        ] = None,
        partitions: Annotated[
            Collection["TopicPartition"],
            Doc(
                """
            An explicit partitions list to assign.
            You can't use 'topics' and 'partitions' in the same time.
            """
            ),
        ] = (),
        # broker args
        dependencies: Annotated[
            Iterable["Depends"],
            Doc("Dependencies list (`[Depends(),]`) to apply to the subscriber."),
        ] = (),
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Parser to map original **ConsumerRecord** object to FastStream one."),
        ] = None,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Function to decode FastStream msg bytes body to python objects."),
        ] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            Doc("Subscriber middlewares to wrap incoming message processing."),
        ] = (),
        max_workers: Annotated[
            int,
            Doc(
                "Maximum number of messages being processed concurrently. With "
                "`auto_commit=False` processing is concurrent between partitions and "
                "sequential within a partition. With `auto_commit=False` maximum "
                "concurrency is achieved when total number of workers across all "
                "application instances running workers in the same consumer group "
                "is equal to the number of partitions in the topic."
            ),
        ] = 1,
        filter: Annotated[
            "Filter[KafkaMessage]",
            Doc(
                "Overload subscriber to consume various messages from the same source."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.0**. "
                "Please, create `subscriber` object and use it explicitly instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = default_filter,
        retry: Annotated[
            bool,
            Doc("Whether to `nack` message at processing exception."),
            deprecated(
                "Deprecated in **FastStream 0.5.40**."
                "Please, manage acknowledgement policy manually."
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        no_ack: Annotated[
            bool,
            Doc("Whether to disable **FastStream** autoacknowledgement logic or not."),
        ] = False,
        no_reply: Annotated[
            bool,
            Doc(
                "Whether to disable **FastStream** RPC and Reply To auto responses or not."
            ),
        ] = False,
        # AsyncAPI args
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI subscriber object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc(
                "AsyncAPI subscriber object description. "
                "Uses decorated docstring as default."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> "AsyncAPIDefaultSubscriber": ...

    @overload
    def subscriber(
        self,
        *topics: Annotated[
            str,
            Doc("Kafka topics to consume messages from."),
        ],
        batch: Annotated[
            Literal[True],
            Doc("Whether to consume messages in batches or not."),
        ],
        group_id: Annotated[
            Optional[str],
            Doc(
                """
            Name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If `None`, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            """
            ),
        ] = None,
        key_deserializer: Annotated[
            Optional[Callable[[bytes], Any]],
            Doc(
                "Any callable that takes a raw message `bytes` "
                "key and returns a deserialized one."
            ),
        ] = None,
        value_deserializer: Annotated[
            Optional[Callable[[bytes], Any]],
            Doc(
                "Any callable that takes a raw message `bytes` "
                "value and returns a deserialized value."
            ),
        ] = None,
        fetch_max_bytes: Annotated[
            int,
            Doc(
                """
            The maximum amount of data the server should
            return for a fetch request. This is not an absolute maximum, if
            the first message in the first non-empty partition of the fetch
            is larger than this value, the message will still be returned
            to ensure that the consumer can make progress. NOTE: consumer
            performs fetches to multiple brokers in parallel so memory
            usage will depend on the number of brokers containing
            partitions for the topic.
            """
            ),
        ] = 50 * 1024 * 1024,
        fetch_min_bytes: Annotated[
            int,
            Doc(
                """
            Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            `fetch_max_wait_ms` for more data to accumulate.
            """
            ),
        ] = 1,
        fetch_max_wait_ms: Annotated[
            int,
            Doc(
                """
            The maximum amount of time in milliseconds
            the server will block before answering the fetch request if
            there isn't sufficient data to immediately satisfy the
            requirement given by `fetch_min_bytes`.
            """
            ),
        ] = 500,
        max_partition_fetch_bytes: Annotated[
            int,
            Doc(
                """
            The maximum amount of data
            per-partition the server will return. The maximum total memory
            used for a request ``= #partitions * max_partition_fetch_bytes``.
            This size must be at least as large as the maximum message size
            the server allows or else it is possible for the producer to
            send messages larger than the consumer can fetch. If that
            happens, the consumer can get stuck trying to fetch a large
            message on a certain partition.
            """
            ),
        ] = 1 * 1024 * 1024,
        auto_offset_reset: Annotated[
            Literal["latest", "earliest", "none"],
            Doc(
                """
            A policy for resetting offsets on `OffsetOutOfRangeError` errors:

            * `earliest` will move to the oldest available message
            * `latest` will move to the most recent
            * `none` will raise an exception so you can handle this case
            """
            ),
        ] = "latest",
        auto_commit: Annotated[
            bool,
            Doc(
                """
            If `True` the consumer's offset will be
            periodically committed in the background.
            """
            ),
        ] = True,
        auto_commit_interval_ms: Annotated[
            int,
            Doc(
                """
            Milliseconds between automatic
            offset commits, if `auto_commit` is `True`."""
            ),
        ] = 5 * 1000,
        check_crcs: Annotated[
            bool,
            Doc(
                """
            Automatically check the CRC32 of the records
            consumed. This ensures no on-the-wire or on-disk corruption to
            the messages occurred. This check adds some overhead, so it may
            be disabled in cases seeking extreme performance.
            """
            ),
        ] = True,
        partition_assignment_strategy: Annotated[
            Sequence["AbstractPartitionAssignor"],
            Doc(
                """
            List of objects to use to
            distribute partition ownership amongst consumer instances when
            group management is used. This preference is implicit in the order
            of the strategies in the list. When assignment strategy changes:
            to support a change to the assignment strategy, new versions must
            enable support both for the old assignment strategy and the new
            one. The coordinator will choose the old assignment strategy until
            all members have been updated. Then it will choose the new
            strategy.
            """
            ),
        ] = (RoundRobinPartitionAssignor,),
        max_poll_interval_ms: Annotated[
            int,
            Doc(
                """
            Maximum allowed time between calls to
            consume messages in batches. If this interval
            is exceeded the consumer is considered failed and the group will
            rebalance in order to reassign the partitions to another consumer
            group member. If API methods block waiting for messages, that time
            does not count against this timeout.
            """
            ),
        ] = 5 * 60 * 1000,
        rebalance_timeout_ms: Annotated[
            Optional[int],
            Doc(
                """
            The maximum time server will wait for this
            consumer to rejoin the group in a case of rebalance. In Java client
            this behaviour is bound to `max.poll.interval.ms` configuration,
            but as ``aiokafka`` will rejoin the group in the background, we
            decouple this setting to allow finer tuning by users that use
            `ConsumerRebalanceListener` to delay rebalacing. Defaults
            to ``session_timeout_ms``
            """
            ),
        ] = None,
        session_timeout_ms: Annotated[
            int,
            Doc(
                """
            Client group session and failure detection
            timeout. The consumer sends periodic heartbeats
            (`heartbeat.interval.ms`) to indicate its liveness to the broker.
            If no hearts are received by the broker for a group member within
            the session timeout, the broker will remove the consumer from the
            group and trigger a rebalance. The allowed range is configured with
            the **broker** configuration properties
            `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            """
            ),
        ] = 10 * 1000,
        heartbeat_interval_ms: Annotated[
            int,
            Doc(
                """
            The expected time in milliseconds
            between heartbeats to the consumer coordinator when using
            Kafka's group management feature. Heartbeats are used to ensure
            that the consumer's session stays active and to facilitate
            rebalancing when new consumers join or leave the group. The
            value must be set lower than `session_timeout_ms`, but typically
            should be set no higher than 1/3 of that value. It can be
            adjusted even lower to control the expected time for normal
            rebalances.
            """
            ),
        ] = 3 * 1000,
        consumer_timeout_ms: Annotated[
            int,
            Doc(
                """
            Maximum wait timeout for background fetching
            routine. Mostly defines how fast the system will see rebalance and
            request new data for new partitions.
            """
            ),
        ] = 200,
        max_poll_records: Annotated[
            Optional[int],
            Doc(
                """
            The maximum number of records returned in a
            single call by batch consumer. Has no limit by default.
            """
            ),
        ] = None,
        exclude_internal_topics: Annotated[
            bool,
            Doc(
                """
            Whether records from internal topics
            (such as offsets) should be exposed to the consumer. If set to True
            the only way to receive records from an internal topic is
            subscribing to it.
            """
            ),
        ] = True,
        isolation_level: Annotated[
            Literal["read_uncommitted", "read_committed"],
            Doc(
                """
            Controls how to read messages written
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
            messages up to the last stable offset (LSO), which is the one less
            than the offset of the first open transaction. In particular any
            messages appearing after messages belonging to ongoing transactions
            will be withheld until the relevant transaction has been completed.
            As a result, `read_committed` consumers will not be able to read up
            to the high watermark when there are in flight transactions.
            Further, when in `read_committed` the seek_to_end method will
            return the LSO. See method docs below.
            """
            ),
        ] = "read_uncommitted",
        batch_timeout_ms: Annotated[
            int,
            Doc(
                """
            Milliseconds spent waiting if
            data is not available in the buffer. If 0, returns immediately
            with any records that are available currently in the buffer,
            else returns empty.
            """
            ),
        ] = 200,
        max_records: Annotated[
            Optional[int],
            Doc("Number of messages to consume as one batch."),
        ] = None,
        listener: Annotated[
            Optional["ConsumerRebalanceListener"],
            Doc(
                """
            Optionally include listener
               callback, which will be called before and after each rebalance
               operation.
               As part of group management, the consumer will keep track of
               the list of consumers that belong to a particular group and
               will trigger a rebalance operation if one of the following
               events trigger:

               * Number of partitions change for any of the subscribed topics
               * Topic is created or deleted
               * An existing member of the consumer group dies
               * A new member is added to the consumer group

               When any of these events are triggered, the provided listener
               will be invoked first to indicate that the consumer's
               assignment has been revoked, and then again when the new
               assignment has been received. Note that this listener will
               immediately override any listener set in a previous call
               to subscribe. It is guaranteed, however, that the partitions
               revoked/assigned
               through this interface are from topics subscribed in this call.
            """
            ),
        ] = None,
        pattern: Annotated[
            Optional[str],
            Doc(
                """
            Pattern to match available topics. You must provide either topics or pattern, but not both.
            """
            ),
        ] = None,
        partitions: Annotated[
            Collection["TopicPartition"],
            Doc(
                """
            An explicit partitions list to assign.
            You can't use 'topics' and 'partitions' in the same time.
            """
            ),
        ] = (),
        # broker args
        dependencies: Annotated[
            Iterable["Depends"],
            Doc("Dependencies list (`[Depends(),]`) to apply to the subscriber."),
        ] = (),
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Parser to map original **ConsumerRecord** object to FastStream one."),
        ] = None,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Function to decode FastStream msg bytes body to python objects."),
        ] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            Doc("Subscriber middlewares to wrap incoming message processing."),
        ] = (),
        max_workers: Annotated[
            int,
            Doc(
                "Maximum number of messages being processed concurrently. With "
                "`auto_commit=False` processing is concurrent between partitions and "
                "sequential within a partition. With `auto_commit=False` maximum "
                "concurrency is achieved when total number of workers across all "
                "application instances running workers in the same consumer group "
                "is equal to the number of partitions in the topic."
            ),
        ] = 1,
        filter: Annotated[
            "Filter[KafkaMessage]",
            Doc(
                "Overload subscriber to consume various messages from the same source."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.0**. "
                "Please, create `subscriber` object and use it explicitly instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = default_filter,
        retry: Annotated[
            bool,
            Doc("Whether to `nack` message at processing exception."),
            deprecated(
                "Deprecated in **FastStream 0.5.40**."
                "Please, manage acknowledgement policy manually."
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        no_ack: Annotated[
            bool,
            Doc("Whether to disable **FastStream** autoacknowledgement logic or not."),
        ] = False,
        no_reply: Annotated[
            bool,
            Doc(
                "Whether to disable **FastStream** RPC and Reply To auto responses or not."
            ),
        ] = False,
        # AsyncAPI args
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI subscriber object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc(
                "AsyncAPI subscriber object description. "
                "Uses decorated docstring as default."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> "AsyncAPIBatchSubscriber": ...

    @overload
    def subscriber(
        self,
        *topics: Annotated[
            str,
            Doc("Kafka topics to consume messages from."),
        ],
        batch: Annotated[
            bool,
            Doc("Whether to consume messages in batches or not."),
        ] = False,
        group_id: Annotated[
            Optional[str],
            Doc(
                """
            Name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If `None`, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            """
            ),
        ] = None,
        key_deserializer: Annotated[
            Optional[Callable[[bytes], Any]],
            Doc(
                "Any callable that takes a raw message `bytes` "
                "key and returns a deserialized one."
            ),
        ] = None,
        value_deserializer: Annotated[
            Optional[Callable[[bytes], Any]],
            Doc(
                "Any callable that takes a raw message `bytes` "
                "value and returns a deserialized value."
            ),
        ] = None,
        fetch_max_bytes: Annotated[
            int,
            Doc(
                """
            The maximum amount of data the server should
            return for a fetch request. This is not an absolute maximum, if
            the first message in the first non-empty partition of the fetch
            is larger than this value, the message will still be returned
            to ensure that the consumer can make progress. NOTE: consumer
            performs fetches to multiple brokers in parallel so memory
            usage will depend on the number of brokers containing
            partitions for the topic.
            """
            ),
        ] = 50 * 1024 * 1024,
        fetch_min_bytes: Annotated[
            int,
            Doc(
                """
            Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            `fetch_max_wait_ms` for more data to accumulate.
            """
            ),
        ] = 1,
        fetch_max_wait_ms: Annotated[
            int,
            Doc(
                """
            The maximum amount of time in milliseconds
            the server will block before answering the fetch request if
            there isn't sufficient data to immediately satisfy the
            requirement given by `fetch_min_bytes`.
            """
            ),
        ] = 500,
        max_partition_fetch_bytes: Annotated[
            int,
            Doc(
                """
            The maximum amount of data
            per-partition the server will return. The maximum total memory
            used for a request ``= #partitions * max_partition_fetch_bytes``.
            This size must be at least as large as the maximum message size
            the server allows or else it is possible for the producer to
            send messages larger than the consumer can fetch. If that
            happens, the consumer can get stuck trying to fetch a large
            message on a certain partition.
            """
            ),
        ] = 1 * 1024 * 1024,
        auto_offset_reset: Annotated[
            Literal["latest", "earliest", "none"],
            Doc(
                """
            A policy for resetting offsets on `OffsetOutOfRangeError` errors:

            * `earliest` will move to the oldest available message
            * `latest` will move to the most recent
            * `none` will raise an exception so you can handle this case
            """
            ),
        ] = "latest",
        auto_commit: Annotated[
            bool,
            Doc(
                """
            If `True` the consumer's offset will be
            periodically committed in the background.
            """
            ),
        ] = True,
        auto_commit_interval_ms: Annotated[
            int,
            Doc(
                """
            Milliseconds between automatic
            offset commits, if `auto_commit` is `True`."""
            ),
        ] = 5 * 1000,
        check_crcs: Annotated[
            bool,
            Doc(
                """
            Automatically check the CRC32 of the records
            consumed. This ensures no on-the-wire or on-disk corruption to
            the messages occurred. This check adds some overhead, so it may
            be disabled in cases seeking extreme performance.
            """
            ),
        ] = True,
        partition_assignment_strategy: Annotated[
            Sequence["AbstractPartitionAssignor"],
            Doc(
                """
            List of objects to use to
            distribute partition ownership amongst consumer instances when
            group management is used. This preference is implicit in the order
            of the strategies in the list. When assignment strategy changes:
            to support a change to the assignment strategy, new versions must
            enable support both for the old assignment strategy and the new
            one. The coordinator will choose the old assignment strategy until
            all members have been updated. Then it will choose the new
            strategy.
            """
            ),
        ] = (RoundRobinPartitionAssignor,),
        max_poll_interval_ms: Annotated[
            int,
            Doc(
                """
            Maximum allowed time between calls to
            consume messages in batches. If this interval
            is exceeded the consumer is considered failed and the group will
            rebalance in order to reassign the partitions to another consumer
            group member. If API methods block waiting for messages, that time
            does not count against this timeout.
            """
            ),
        ] = 5 * 60 * 1000,
        rebalance_timeout_ms: Annotated[
            Optional[int],
            Doc(
                """
            The maximum time server will wait for this
            consumer to rejoin the group in a case of rebalance. In Java client
            this behaviour is bound to `max.poll.interval.ms` configuration,
            but as ``aiokafka`` will rejoin the group in the background, we
            decouple this setting to allow finer tuning by users that use
            `ConsumerRebalanceListener` to delay rebalacing. Defaults
            to ``session_timeout_ms``
            """
            ),
        ] = None,
        session_timeout_ms: Annotated[
            int,
            Doc(
                """
            Client group session and failure detection
            timeout. The consumer sends periodic heartbeats
            (`heartbeat.interval.ms`) to indicate its liveness to the broker.
            If no hearts are received by the broker for a group member within
            the session timeout, the broker will remove the consumer from the
            group and trigger a rebalance. The allowed range is configured with
            the **broker** configuration properties
            `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            """
            ),
        ] = 10 * 1000,
        heartbeat_interval_ms: Annotated[
            int,
            Doc(
                """
            The expected time in milliseconds
            between heartbeats to the consumer coordinator when using
            Kafka's group management feature. Heartbeats are used to ensure
            that the consumer's session stays active and to facilitate
            rebalancing when new consumers join or leave the group. The
            value must be set lower than `session_timeout_ms`, but typically
            should be set no higher than 1/3 of that value. It can be
            adjusted even lower to control the expected time for normal
            rebalances.
            """
            ),
        ] = 3 * 1000,
        consumer_timeout_ms: Annotated[
            int,
            Doc(
                """
            Maximum wait timeout for background fetching
            routine. Mostly defines how fast the system will see rebalance and
            request new data for new partitions.
            """
            ),
        ] = 200,
        max_poll_records: Annotated[
            Optional[int],
            Doc(
                """
            The maximum number of records returned in a
            single call by batch consumer. Has no limit by default.
            """
            ),
        ] = None,
        exclude_internal_topics: Annotated[
            bool,
            Doc(
                """
            Whether records from internal topics
            (such as offsets) should be exposed to the consumer. If set to True
            the only way to receive records from an internal topic is
            subscribing to it.
            """
            ),
        ] = True,
        isolation_level: Annotated[
            Literal["read_uncommitted", "read_committed"],
            Doc(
                """
            Controls how to read messages written
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
            messages up to the last stable offset (LSO), which is the one less
            than the offset of the first open transaction. In particular any
            messages appearing after messages belonging to ongoing transactions
            will be withheld until the relevant transaction has been completed.
            As a result, `read_committed` consumers will not be able to read up
            to the high watermark when there are in flight transactions.
            Further, when in `read_committed` the seek_to_end method will
            return the LSO. See method docs below.
            """
            ),
        ] = "read_uncommitted",
        batch_timeout_ms: Annotated[
            int,
            Doc(
                """
            Milliseconds spent waiting if
            data is not available in the buffer. If 0, returns immediately
            with any records that are available currently in the buffer,
            else returns empty.
            """
            ),
        ] = 200,
        max_records: Annotated[
            Optional[int],
            Doc("Number of messages to consume as one batch."),
        ] = None,
        listener: Annotated[
            Optional["ConsumerRebalanceListener"],
            Doc(
                """
            Optionally include listener
               callback, which will be called before and after each rebalance
               operation.
               As part of group management, the consumer will keep track of
               the list of consumers that belong to a particular group and
               will trigger a rebalance operation if one of the following
               events trigger:

               * Number of partitions change for any of the subscribed topics
               * Topic is created or deleted
               * An existing member of the consumer group dies
               * A new member is added to the consumer group

               When any of these events are triggered, the provided listener
               will be invoked first to indicate that the consumer's
               assignment has been revoked, and then again when the new
               assignment has been received. Note that this listener will
               immediately override any listener set in a previous call
               to subscribe. It is guaranteed, however, that the partitions
               revoked/assigned
               through this interface are from topics subscribed in this call.
            """
            ),
        ] = None,
        pattern: Annotated[
            Optional[str],
            Doc(
                """
            Pattern to match available topics. You must provide either topics or pattern, but not both.
            """
            ),
        ] = None,
        partitions: Annotated[
            Collection["TopicPartition"],
            Doc(
                """
            An explicit partitions list to assign.
            You can't use 'topics' and 'partitions' in the same time.
            """
            ),
        ] = (),
        # broker args
        dependencies: Annotated[
            Iterable["Depends"],
            Doc("Dependencies list (`[Depends(),]`) to apply to the subscriber."),
        ] = (),
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Parser to map original **ConsumerRecord** object to FastStream one."),
        ] = None,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Function to decode FastStream msg bytes body to python objects."),
        ] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            Doc("Subscriber middlewares to wrap incoming message processing."),
        ] = (),
        max_workers: Annotated[
            int,
            Doc(
                "Maximum number of messages being processed concurrently. With "
                "`auto_commit=False` processing is concurrent between partitions and "
                "sequential within a partition. With `auto_commit=False` maximum "
                "concurrency is achieved when total number of workers across all "
                "application instances running workers in the same consumer group "
                "is equal to the number of partitions in the topic."
            ),
        ] = 1,
        filter: Annotated[
            "Filter[KafkaMessage]",
            Doc(
                "Overload subscriber to consume various messages from the same source."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.0**. "
                "Please, create `subscriber` object and use it explicitly instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = default_filter,
        retry: Annotated[
            bool,
            Doc("Whether to `nack` message at processing exception."),
            deprecated(
                "Deprecated in **FastStream 0.5.40**."
                "Please, manage acknowledgement policy manually."
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        no_ack: Annotated[
            bool,
            Doc("Whether to disable **FastStream** autoacknowledgement logic or not."),
        ] = False,
        no_reply: Annotated[
            bool,
            Doc(
                "Whether to disable **FastStream** RPC and Reply To auto responses or not."
            ),
        ] = False,
        # AsyncAPI args
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI subscriber object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc(
                "AsyncAPI subscriber object description. "
                "Uses decorated docstring as default."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> Union[
        "AsyncAPIDefaultSubscriber",
        "AsyncAPIBatchSubscriber",
    ]: ...

    @override
    def subscriber(
        self,
        *topics: Annotated[
            str,
            Doc("Kafka topics to consume messages from."),
        ],
        batch: Annotated[
            bool,
            Doc("Whether to consume messages in batches or not."),
        ] = False,
        group_id: Annotated[
            Optional[str],
            Doc(
                """
            Name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If `None`, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            """
            ),
        ] = None,
        key_deserializer: Annotated[
            Optional[Callable[[bytes], Any]],
            Doc(
                "Any callable that takes a raw message `bytes` "
                "key and returns a deserialized one."
            ),
        ] = None,
        value_deserializer: Annotated[
            Optional[Callable[[bytes], Any]],
            Doc(
                "Any callable that takes a raw message `bytes` "
                "value and returns a deserialized value."
            ),
        ] = None,
        fetch_max_bytes: Annotated[
            int,
            Doc(
                """
            The maximum amount of data the server should
            return for a fetch request. This is not an absolute maximum, if
            the first message in the first non-empty partition of the fetch
            is larger than this value, the message will still be returned
            to ensure that the consumer can make progress. NOTE: consumer
            performs fetches to multiple brokers in parallel so memory
            usage will depend on the number of brokers containing
            partitions for the topic.
            """
            ),
        ] = 50 * 1024 * 1024,
        fetch_min_bytes: Annotated[
            int,
            Doc(
                """
            Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            `fetch_max_wait_ms` for more data to accumulate.
            """
            ),
        ] = 1,
        fetch_max_wait_ms: Annotated[
            int,
            Doc(
                """
            The maximum amount of time in milliseconds
            the server will block before answering the fetch request if
            there isn't sufficient data to immediately satisfy the
            requirement given by `fetch_min_bytes`.
            """
            ),
        ] = 500,
        max_partition_fetch_bytes: Annotated[
            int,
            Doc(
                """
            The maximum amount of data
            per-partition the server will return. The maximum total memory
            used for a request ``= #partitions * max_partition_fetch_bytes``.
            This size must be at least as large as the maximum message size
            the server allows or else it is possible for the producer to
            send messages larger than the consumer can fetch. If that
            happens, the consumer can get stuck trying to fetch a large
            message on a certain partition.
            """
            ),
        ] = 1 * 1024 * 1024,
        auto_offset_reset: Annotated[
            Literal["latest", "earliest", "none"],
            Doc(
                """
            A policy for resetting offsets on `OffsetOutOfRangeError` errors:

            * `earliest` will move to the oldest available message
            * `latest` will move to the most recent
            * `none` will raise an exception so you can handle this case
            """
            ),
        ] = "latest",
        auto_commit: Annotated[
            bool,
            Doc(
                """
            If `True` the consumer's offset will be
            periodically committed in the background.
            """
            ),
        ] = True,
        auto_commit_interval_ms: Annotated[
            int,
            Doc(
                """
            Milliseconds between automatic
            offset commits, if `auto_commit` is `True`."""
            ),
        ] = 5 * 1000,
        check_crcs: Annotated[
            bool,
            Doc(
                """
            Automatically check the CRC32 of the records
            consumed. This ensures no on-the-wire or on-disk corruption to
            the messages occurred. This check adds some overhead, so it may
            be disabled in cases seeking extreme performance.
            """
            ),
        ] = True,
        partition_assignment_strategy: Annotated[
            Sequence["AbstractPartitionAssignor"],
            Doc(
                """
            List of objects to use to
            distribute partition ownership amongst consumer instances when
            group management is used. This preference is implicit in the order
            of the strategies in the list. When assignment strategy changes:
            to support a change to the assignment strategy, new versions must
            enable support both for the old assignment strategy and the new
            one. The coordinator will choose the old assignment strategy until
            all members have been updated. Then it will choose the new
            strategy.
            """
            ),
        ] = (RoundRobinPartitionAssignor,),
        max_poll_interval_ms: Annotated[
            int,
            Doc(
                """
            Maximum allowed time between calls to
            consume messages in batches. If this interval
            is exceeded the consumer is considered failed and the group will
            rebalance in order to reassign the partitions to another consumer
            group member. If API methods block waiting for messages, that time
            does not count against this timeout.
            """
            ),
        ] = 5 * 60 * 1000,
        rebalance_timeout_ms: Annotated[
            Optional[int],
            Doc(
                """
            The maximum time server will wait for this
            consumer to rejoin the group in a case of rebalance. In Java client
            this behaviour is bound to `max.poll.interval.ms` configuration,
            but as ``aiokafka`` will rejoin the group in the background, we
            decouple this setting to allow finer tuning by users that use
            `ConsumerRebalanceListener` to delay rebalacing. Defaults
            to ``session_timeout_ms``
            """
            ),
        ] = None,
        session_timeout_ms: Annotated[
            int,
            Doc(
                """
            Client group session and failure detection
            timeout. The consumer sends periodic heartbeats
            (`heartbeat.interval.ms`) to indicate its liveness to the broker.
            If no hearts are received by the broker for a group member within
            the session timeout, the broker will remove the consumer from the
            group and trigger a rebalance. The allowed range is configured with
            the **broker** configuration properties
            `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            """
            ),
        ] = 10 * 1000,
        heartbeat_interval_ms: Annotated[
            int,
            Doc(
                """
            The expected time in milliseconds
            between heartbeats to the consumer coordinator when using
            Kafka's group management feature. Heartbeats are used to ensure
            that the consumer's session stays active and to facilitate
            rebalancing when new consumers join or leave the group. The
            value must be set lower than `session_timeout_ms`, but typically
            should be set no higher than 1/3 of that value. It can be
            adjusted even lower to control the expected time for normal
            rebalances.
            """
            ),
        ] = 3 * 1000,
        consumer_timeout_ms: Annotated[
            int,
            Doc(
                """
            Maximum wait timeout for background fetching
            routine. Mostly defines how fast the system will see rebalance and
            request new data for new partitions.
            """
            ),
        ] = 200,
        max_poll_records: Annotated[
            Optional[int],
            Doc(
                """
            The maximum number of records returned in a
            single call by batch consumer. Has no limit by default.
            """
            ),
        ] = None,
        exclude_internal_topics: Annotated[
            bool,
            Doc(
                """
            Whether records from internal topics
            (such as offsets) should be exposed to the consumer. If set to True
            the only way to receive records from an internal topic is
            subscribing to it.
            """
            ),
        ] = True,
        isolation_level: Annotated[
            Literal["read_uncommitted", "read_committed"],
            Doc(
                """
            Controls how to read messages written
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
            messages up to the last stable offset (LSO), which is the one less
            than the offset of the first open transaction. In particular any
            messages appearing after messages belonging to ongoing transactions
            will be withheld until the relevant transaction has been completed.
            As a result, `read_committed` consumers will not be able to read up
            to the high watermark when there are in flight transactions.
            Further, when in `read_committed` the seek_to_end method will
            return the LSO. See method docs below.
            """
            ),
        ] = "read_uncommitted",
        batch_timeout_ms: Annotated[
            int,
            Doc(
                """
            Milliseconds spent waiting if
            data is not available in the buffer. If 0, returns immediately
            with any records that are available currently in the buffer,
            else returns empty.
            """
            ),
        ] = 200,
        max_records: Annotated[
            Optional[int],
            Doc("Number of messages to consume as one batch."),
        ] = None,
        listener: Annotated[
            Optional["ConsumerRebalanceListener"],
            Doc(
                """
            Optionally include listener
               callback, which will be called before and after each rebalance
               operation.
               As part of group management, the consumer will keep track of
               the list of consumers that belong to a particular group and
               will trigger a rebalance operation if one of the following
               events trigger:

               * Number of partitions change for any of the subscribed topics
               * Topic is created or deleted
               * An existing member of the consumer group dies
               * A new member is added to the consumer group

               When any of these events are triggered, the provided listener
               will be invoked first to indicate that the consumer's
               assignment has been revoked, and then again when the new
               assignment has been received. Note that this listener will
               immediately override any listener set in a previous call
               to subscribe. It is guaranteed, however, that the partitions
               revoked/assigned
               through this interface are from topics subscribed in this call.
            """
            ),
        ] = None,
        pattern: Annotated[
            Optional[str],
            Doc(
                """
            Pattern to match available topics. You must provide either topics or pattern, but not both.
            """
            ),
        ] = None,
        partitions: Annotated[
            Collection["TopicPartition"],
            Doc(
                """
            An explicit partitions list to assign.
            You can't use 'topics' and 'partitions' in the same time.
            """
            ),
        ] = (),
        # broker args
        dependencies: Annotated[
            Iterable["Depends"],
            Doc("Dependencies list (`[Depends(),]`) to apply to the subscriber."),
        ] = (),
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Parser to map original **ConsumerRecord** object to FastStream one."),
        ] = None,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Function to decode FastStream msg bytes body to python objects."),
        ] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            Doc("Subscriber middlewares to wrap incoming message processing."),
        ] = (),
        max_workers: Annotated[
            int,
            Doc(
                "Maximum number of messages being processed concurrently. With "
                "`auto_commit=False` processing is concurrent between partitions and "
                "sequential within a partition. With `auto_commit=False` maximum "
                "concurrency is achieved when total number of workers across all "
                "application instances running workers in the same consumer group "
                "is equal to the number of partitions in the topic."
            ),
        ] = 1,
        filter: Annotated[
            "Filter[KafkaMessage]",
            Doc(
                "Overload subscriber to consume various messages from the same source."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.0**. "
                "Please, create `subscriber` object and use it explicitly instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = default_filter,
        retry: Annotated[
            bool,
            Doc("Whether to `nack` message at processing exception."),
            deprecated(
                "Deprecated in **FastStream 0.5.40**."
                "Please, manage acknowledgement policy manually."
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        no_ack: Annotated[
            bool,
            Doc("Whether to disable **FastStream** autoacknowledgement logic or not."),
        ] = False,
        no_reply: Annotated[
            bool,
            Doc(
                "Whether to disable **FastStream** RPC and Reply To auto responses or not."
            ),
        ] = False,
        # AsyncAPI args
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI subscriber object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc(
                "AsyncAPI subscriber object description. "
                "Uses decorated docstring as default."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> Union[
        "AsyncAPIDefaultSubscriber",
        "AsyncAPIBatchSubscriber",
        "AsyncAPIConcurrentDefaultSubscriber",
        "AsyncAPIConcurrentBetweenPartitionsSubscriber",
    ]:
        subscriber = super().subscriber(
            create_subscriber(
                *topics,
                batch=batch,
                max_workers=max_workers,
                batch_timeout_ms=batch_timeout_ms,
                max_records=max_records,
                group_id=group_id,
                listener=listener,
                pattern=pattern,
                connection_args={
                    "key_deserializer": key_deserializer,
                    "value_deserializer": value_deserializer,
                    "fetch_max_wait_ms": fetch_max_wait_ms,
                    "fetch_max_bytes": fetch_max_bytes,
                    "fetch_min_bytes": fetch_min_bytes,
                    "max_partition_fetch_bytes": max_partition_fetch_bytes,
                    "auto_offset_reset": auto_offset_reset,
                    "enable_auto_commit": auto_commit,
                    "auto_commit_interval_ms": auto_commit_interval_ms,
                    "check_crcs": check_crcs,
                    "partition_assignment_strategy": partition_assignment_strategy,
                    "max_poll_interval_ms": max_poll_interval_ms,
                    "rebalance_timeout_ms": rebalance_timeout_ms,
                    "session_timeout_ms": session_timeout_ms,
                    "heartbeat_interval_ms": heartbeat_interval_ms,
                    "consumer_timeout_ms": consumer_timeout_ms,
                    "max_poll_records": max_poll_records,
                    "exclude_internal_topics": exclude_internal_topics,
                    "isolation_level": isolation_level,
                },
                partitions=partitions,
                is_manual=not auto_commit,
                # subscriber args
                no_ack=no_ack,
                no_reply=no_reply,
                retry=retry,
                broker_middlewares=self._middlewares,
                broker_dependencies=self._dependencies,
                # AsyncAPI
                title_=title,
                description_=description,
                include_in_schema=self._solve_include_in_schema(include_in_schema),
            )
        )

        if batch:
            return cast("AsyncAPIBatchSubscriber", subscriber).add_call(
                filter_=filter,
                parser_=parser or self._parser,
                decoder_=decoder or self._decoder,
                dependencies_=dependencies,
                middlewares_=middlewares,
            )

        else:
            if max_workers > 1:
                if not auto_commit:
                    return cast(
                        "AsyncAPIConcurrentBetweenPartitionsSubscriber", subscriber
                    ).add_call(
                        filter_=filter,
                        parser_=parser or self._parser,
                        decoder_=decoder or self._decoder,
                        dependencies_=dependencies,
                        middlewares_=middlewares,
                    )
                else:
                    return cast(
                        "AsyncAPIConcurrentDefaultSubscriber", subscriber
                    ).add_call(
                        filter_=filter,
                        parser_=parser or self._parser,
                        decoder_=decoder or self._decoder,
                        dependencies_=dependencies,
                        middlewares_=middlewares,
                    )
            else:
                return cast("AsyncAPIDefaultSubscriber", subscriber).add_call(
                    filter_=filter,
                    parser_=parser or self._parser,
                    decoder_=decoder or self._decoder,
                    dependencies_=dependencies,
                    middlewares_=middlewares,
                )

    @overload  # type: ignore[override]
    def publisher(
        self,
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ],
        *,
        key: Annotated[
            Union[bytes, Any, None],
            Doc(
                """
            A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            """
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc(
                "Message headers to store metainformation. "
                "**content-type** and **correlation_id** will be set automatically by framework anyway. "
                "Can be overridden by `publish.headers` if specified."
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Topic name to send response."),
        ] = "",
        batch: Annotated[
            Literal[False],
            Doc("Whether to send messages in batches or not."),
        ] = False,
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            Doc("Publisher middlewares to wrap outgoing messages."),
        ] = (),
        # AsyncAPI args
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object description."),
        ] = None,
        schema: Annotated[
            Optional[Any],
            Doc(
                "AsyncAPI publishing message type. "
                "Should be any python-native object annotation or `pydantic.BaseModel`."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
        autoflush: Annotated[
            bool,
            Doc("Whether to flush the producer or not on every publish call."),
        ] = False,
    ) -> "AsyncAPIDefaultPublisher": ...

    @overload
    def publisher(
        self,
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ],
        *,
        key: Annotated[
            Union[bytes, Any, None],
            Doc(
                """
            A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            """
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc(
                "Message headers to store metainformation. "
                "**content-type** and **correlation_id** will be set automatically by framework anyway. "
                "Can be overridden by `publish.headers` if specified."
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Topic name to send response."),
        ] = "",
        batch: Annotated[
            Literal[True],
            Doc("Whether to send messages in batches or not."),
        ],
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            Doc("Publisher middlewares to wrap outgoing messages."),
        ] = (),
        # AsyncAPI args
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object description."),
        ] = None,
        schema: Annotated[
            Optional[Any],
            Doc(
                "AsyncAPI publishing message type. "
                "Should be any python-native object annotation or `pydantic.BaseModel`."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
        autoflush: Annotated[
            bool,
            Doc("Whether to flush the producer or not on every publish call."),
        ] = False,
    ) -> "AsyncAPIBatchPublisher": ...

    @overload
    def publisher(
        self,
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ],
        *,
        key: Annotated[
            Union[bytes, Any, None],
            Doc(
                """
            A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            """
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc(
                "Message headers to store metainformation. "
                "**content-type** and **correlation_id** will be set automatically by framework anyway. "
                "Can be overridden by `publish.headers` if specified."
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Topic name to send response."),
        ] = "",
        batch: Annotated[
            bool,
            Doc("Whether to send messages in batches or not."),
        ] = False,
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            Doc("Publisher middlewares to wrap outgoing messages."),
        ] = (),
        # AsyncAPI args
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object description."),
        ] = None,
        schema: Annotated[
            Optional[Any],
            Doc(
                "AsyncAPI publishing message type. "
                "Should be any python-native object annotation or `pydantic.BaseModel`."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
        autoflush: Annotated[
            bool,
            Doc("Whether to flush the producer or not on every publish call."),
        ] = False,
    ) -> Union[
        "AsyncAPIBatchPublisher",
        "AsyncAPIDefaultPublisher",
    ]: ...

    @override
    def publisher(
        self,
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ],
        *,
        key: Annotated[
            Union[bytes, Any, None],
            Doc(
                """
            A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            """
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc(
                "Message headers to store metainformation. "
                "**content-type** and **correlation_id** will be set automatically by framework anyway. "
                "Can be overridden by `publish.headers` if specified."
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Topic name to send response."),
        ] = "",
        batch: Annotated[
            bool,
            Doc("Whether to send messages in batches or not."),
        ] = False,
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            Doc("Publisher middlewares to wrap outgoing messages."),
        ] = (),
        # AsyncAPI args
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object description."),
        ] = None,
        schema: Annotated[
            Optional[Any],
            Doc(
                "AsyncAPI publishing message type. "
                "Should be any python-native object annotation or `pydantic.BaseModel`."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
        autoflush: Annotated[
            bool,
            Doc("Whether to flush the producer or not on every publish call."),
        ] = False,
    ) -> Union[
        "AsyncAPIBatchPublisher",
        "AsyncAPIDefaultPublisher",
    ]:
        """Creates long-living and AsyncAPI-documented publisher object.

        You can use it as a handler decorator (handler should be decorated by `@broker.subscriber(...)` too) - `@broker.publisher(...)`.
        In such case publisher will publish your handler return value.

        Or you can create a publisher object to call it lately - `broker.publisher(...).publish(...)`.
        """
        publisher = create_publisher(
            # batch flag
            batch=batch,
            # default args
            key=key,
            # both args
            topic=topic,
            partition=partition,
            headers=headers,
            reply_to=reply_to,
            # publisher-specific
            broker_middlewares=self._middlewares,
            middlewares=middlewares,
            # AsyncAPI
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=self._solve_include_in_schema(include_in_schema),
            autoflush=autoflush,
        )

        if batch:
            return cast("AsyncAPIBatchPublisher", super().publisher(publisher))
        else:
            return cast("AsyncAPIDefaultPublisher", super().publisher(publisher))

    @override
    def include_router(
        self,
        router: "KafkaRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Depends"] = (),
        middlewares: Iterable[
            "BrokerMiddleware[Union[ConsumerRecord, Tuple[ConsumerRecord, ...]]]"
        ] = (),
        include_in_schema: Optional[bool] = None,
    ) -> None:
        if not isinstance(router, KafkaRegistrator):
            msg = (
                f"Router must be an instance of KafkaRegistrator, "
                f"got {type(router).__name__} instead"
            )
            raise SetupError(msg)

        super().include_router(
            router,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            include_in_schema=include_in_schema,
        )
