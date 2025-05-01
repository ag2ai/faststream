from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Collection,
    Dict,
    Iterable,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

from faststream.exceptions import SetupError
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

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord, TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener
    from fast_depends.dependencies import Depends

    from faststream.broker.types import BrokerMiddleware, PublisherMiddleware
    from faststream.types import AnyDict


@overload
def create_subscriber(
    *topics: str,
    batch: Literal[True],
    batch_timeout_ms: int,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    listener: Optional["ConsumerRebalanceListener"],
    pattern: Optional[str],
    connection_args: "AnyDict",
    partitions: Collection["TopicPartition"],
    is_manual: bool,
    # Subscriber args
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Sequence["BrokerMiddleware[Tuple[ConsumerRecord, ...]]"],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> "AsyncAPIBatchSubscriber": ...


@overload
def create_subscriber(
    *topics: str,
    batch: Literal[False],
    batch_timeout_ms: int,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    listener: Optional["ConsumerRebalanceListener"],
    pattern: Optional[str],
    connection_args: "AnyDict",
    partitions: Collection["TopicPartition"],
    is_manual: bool,
    # Subscriber args
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "AsyncAPIDefaultSubscriber",
    "AsyncAPIConcurrentDefaultSubscriber",
]: ...


@overload
def create_subscriber(
    *topics: str,
    batch: bool,
    batch_timeout_ms: int,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    listener: Optional["ConsumerRebalanceListener"],
    pattern: Optional[str],
    connection_args: "AnyDict",
    partitions: Collection["TopicPartition"],
    is_manual: bool,
    # Subscriber args
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Sequence[
        "BrokerMiddleware[Union[ConsumerRecord, Tuple[ConsumerRecord, ...]]]"
    ],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "AsyncAPIDefaultSubscriber",
    "AsyncAPIBatchSubscriber",
    "AsyncAPIConcurrentDefaultSubscriber",
]: ...


def create_subscriber(
    *topics: str,
    batch: bool,
    batch_timeout_ms: int,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    listener: Optional["ConsumerRebalanceListener"],
    pattern: Optional[str],
    connection_args: "AnyDict",
    partitions: Collection["TopicPartition"],
    is_manual: bool,
    # Subscriber args
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Sequence[
        "BrokerMiddleware[Union[ConsumerRecord, Tuple[ConsumerRecord, ...]]]"
    ],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "AsyncAPIDefaultSubscriber",
    "AsyncAPIBatchSubscriber",
    "AsyncAPIConcurrentDefaultSubscriber",
    "AsyncAPIConcurrentBetweenPartitionsSubscriber",
]:
    if is_manual and not group_id:
        raise SetupError("You must use `group_id` with manual commit mode.")

    if is_manual and max_workers > 1:
        if len(topics) > 1:
            raise SetupError(
                "You must use a single topic with concurrent manual commit mode."
            )
        if pattern is not None:
            raise SetupError(
                "You can not use a pattern with concurrent manual commit mode."
            )
        if partitions:
            raise SetupError(
                "Manual partition assignment is not supported with concurrent manual commit mode."
            )

    if not topics and not partitions and not pattern:
        raise SetupError(
            "You should provide either `topics` or `partitions` or `pattern`."
        )
    elif topics and partitions:
        raise SetupError("You can't provide both `topics` and `partitions`.")
    elif topics and pattern:
        raise SetupError("You can't provide both `topics` and `pattern`.")
    elif partitions and pattern:
        raise SetupError("You can't provide both `partitions` and `pattern`.")

    if batch:
        return AsyncAPIBatchSubscriber(
            *topics,
            batch_timeout_ms=batch_timeout_ms,
            max_records=max_records,
            group_id=group_id,
            listener=listener,
            pattern=pattern,
            connection_args=connection_args,
            partitions=partitions,
            is_manual=is_manual,
            no_ack=no_ack,
            no_reply=no_reply,
            retry=retry,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    else:
        if max_workers > 1:
            if is_manual:
                return AsyncAPIConcurrentBetweenPartitionsSubscriber(
                    topics[0],
                    max_workers=max_workers,
                    group_id=group_id,
                    listener=listener,
                    pattern=pattern,
                    connection_args=connection_args,
                    partitions=partitions,
                    is_manual=is_manual,
                    no_ack=no_ack,
                    no_reply=no_reply,
                    retry=retry,
                    broker_dependencies=broker_dependencies,
                    broker_middlewares=broker_middlewares,
                    title_=title_,
                    description_=description_,
                    include_in_schema=include_in_schema,
                )
            else:
                return AsyncAPIConcurrentDefaultSubscriber(
                    *topics,
                    max_workers=max_workers,
                    group_id=group_id,
                    listener=listener,
                    pattern=pattern,
                    connection_args=connection_args,
                    partitions=partitions,
                    is_manual=is_manual,
                    no_ack=no_ack,
                    no_reply=no_reply,
                    retry=retry,
                    broker_dependencies=broker_dependencies,
                    broker_middlewares=broker_middlewares,
                    title_=title_,
                    description_=description_,
                    include_in_schema=include_in_schema,
                )
        else:
            return AsyncAPIDefaultSubscriber(
                *topics,
                group_id=group_id,
                listener=listener,
                pattern=pattern,
                connection_args=connection_args,
                partitions=partitions,
                is_manual=is_manual,
                no_ack=no_ack,
                no_reply=no_reply,
                retry=retry,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )


@overload
def create_publisher(
    *,
    batch: Literal[True],
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[Dict[str, str]],
    reply_to: str,
    # Publisher args
    broker_middlewares: Sequence["BrokerMiddleware[Tuple[ConsumerRecord, ...]]"],
    middlewares: Sequence["PublisherMiddleware"],
    # AsyncAPI args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
    autoflush: bool = False,
) -> "AsyncAPIBatchPublisher": ...


@overload
def create_publisher(
    *,
    batch: Literal[False],
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[Dict[str, str]],
    reply_to: str,
    # Publisher args
    broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
    middlewares: Sequence["PublisherMiddleware"],
    # AsyncAPI args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
    autoflush: bool = False,
) -> "AsyncAPIDefaultPublisher": ...


@overload
def create_publisher(
    *,
    batch: bool,
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[Dict[str, str]],
    reply_to: str,
    # Publisher args
    broker_middlewares: Sequence[
        "BrokerMiddleware[Union[Tuple[ConsumerRecord, ...], ConsumerRecord]]"
    ],
    middlewares: Sequence["PublisherMiddleware"],
    # AsyncAPI args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
    autoflush: bool = False,
) -> Union[
    "AsyncAPIBatchPublisher",
    "AsyncAPIDefaultPublisher",
]: ...


def create_publisher(
    *,
    batch: bool,
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[Dict[str, str]],
    reply_to: str,
    # Publisher args
    broker_middlewares: Sequence[
        "BrokerMiddleware[Union[Tuple[ConsumerRecord, ...], ConsumerRecord]]"
    ],
    middlewares: Sequence["PublisherMiddleware"],
    # AsyncAPI args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
    autoflush: bool = False,
) -> Union[
    "AsyncAPIBatchPublisher",
    "AsyncAPIDefaultPublisher",
]:
    publisher: Union[AsyncAPIBatchPublisher, AsyncAPIDefaultPublisher]
    if batch:
        if key:
            raise SetupError("You can't setup `key` with batch publisher")

        publisher = AsyncAPIBatchPublisher(
            topic=topic,
            partition=partition,
            headers=headers,
            reply_to=reply_to,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
    else:
        publisher = AsyncAPIDefaultPublisher(
            key=key,
            # basic args
            topic=topic,
            partition=partition,
            headers=headers,
            reply_to=reply_to,
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    if autoflush:
        default_publish: Callable[..., Awaitable[Optional[Any]]] = publisher.publish

        @wraps(default_publish)
        async def autoflush_wrapper(*args: Any, **kwargs: Any) -> Optional[Any]:
            result = await default_publish(*args, **kwargs)
            await publisher.flush()
            return result

        publisher.publish = autoflush_wrapper  # type: ignore[method-assign]

    return publisher
