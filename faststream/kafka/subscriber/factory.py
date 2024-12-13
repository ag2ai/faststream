import warnings
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Literal, Optional, Union, overload

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.kafka.subscriber.specified import (
    SpecificationBatchSubscriber,
    SpecificationConcurrentDefaultSubscriber,
    SpecificationDefaultSubscriber,
)
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord, TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import BrokerMiddleware


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
    partitions: Iterable["TopicPartition"],
    auto_commit: bool,
    # Subscriber args
    ack_policy: "AckPolicy",
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Sequence["BrokerMiddleware[tuple[ConsumerRecord, ...]]"],
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationDefaultSubscriber",
    "SpecificationBatchSubscriber",
    "SpecificationConcurrentDefaultSubscriber",
]: ...


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
    partitions: Iterable["TopicPartition"],
    auto_commit: bool,
    # Subscriber args
    ack_policy: "AckPolicy",
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationDefaultSubscriber",
    "SpecificationBatchSubscriber",
    "SpecificationConcurrentDefaultSubscriber",
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
    partitions: Iterable["TopicPartition"],
    auto_commit: bool,
    # Subscriber args
    ack_policy: "AckPolicy",
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Sequence[
        "BrokerMiddleware[Union[ConsumerRecord, tuple[ConsumerRecord, ...]]]"
    ],
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationDefaultSubscriber",
    "SpecificationBatchSubscriber",
    "SpecificationConcurrentDefaultSubscriber",
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
    partitions: Iterable["TopicPartition"],
    auto_commit: bool,
    # Subscriber args
    ack_policy: "AckPolicy",
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Sequence[
        "BrokerMiddleware[Union[ConsumerRecord, tuple[ConsumerRecord, ...]]]"
    ],
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationDefaultSubscriber",
    "SpecificationBatchSubscriber",
    "SpecificationConcurrentDefaultSubscriber",
]:
    _validate_input_for_misconfigure(
        *topics,
        pattern=pattern,
        partitions=partitions,
        ack_policy=ack_policy,
        no_ack=no_ack,
        auto_commit=auto_commit,
        max_workers=max_workers,
        group_id=group_id,
    )

    if auto_commit is not EMPTY:
        ack_policy = AckPolicy.ACK_FIRST if auto_commit else AckPolicy.REJECT_ON_ERROR

    if no_ack is not EMPTY:
        ack_policy = AckPolicy.DO_NOTHING if no_ack else EMPTY

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.ACK_FIRST

    if ack_policy is AckPolicy.ACK_FIRST:
        connection_args["enable_auto_commit"] = True
        ack_policy = AckPolicy.DO_NOTHING

    if batch:
        return SpecificationBatchSubscriber(
            *topics,
            batch_timeout_ms=batch_timeout_ms,
            max_records=max_records,
            group_id=group_id,
            listener=listener,
            pattern=pattern,
            connection_args=connection_args,
            partitions=partitions,
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    if max_workers > 1:
        return SpecificationConcurrentDefaultSubscriber(
            *topics,
            max_workers=max_workers,
            group_id=group_id,
            listener=listener,
            pattern=pattern,
            connection_args=connection_args,
            partitions=partitions,
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    return SpecificationDefaultSubscriber(
        *topics,
        group_id=group_id,
        listener=listener,
        pattern=pattern,
        connection_args=connection_args,
        partitions=partitions,
        ack_policy=ack_policy,
        no_reply=no_reply,
        broker_dependencies=broker_dependencies,
        broker_middlewares=broker_middlewares,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )


def _validate_input_for_misconfigure(
    *topics: str,
    partitions: Iterable["TopicPartition"],
    pattern: Optional[str],
    ack_policy: "AckPolicy",
    auto_commit: bool,
    no_ack: bool,
    group_id: Optional[str],
    max_workers: int,
) -> None:
    if auto_commit is not EMPTY:
        warnings.warn(
            "`auto_commit` option was deprecated in prior to `ack_policy=AckPolicy.ACK_FIRST`. Scheduled to remove in 0.6.10",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `auto_commit` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

        ack_policy = AckPolicy.ACK_FIRST if auto_commit else AckPolicy.REJECT_ON_ERROR

    if no_ack is not EMPTY:
        warnings.warn(
            "`no_ack` option was deprecated in prior to `ack_policy=AckPolicy.DO_NOTHING`. Scheduled to remove in 0.6.10",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `no_ack` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

        ack_policy = AckPolicy.DO_NOTHING if no_ack else EMPTY

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.ACK_FIRST

    if max_workers > 1 and ack_policy is not AckPolicy.ACK_FIRST:
        msg = "You can't use `max_workers` option with manual commit mode."
        raise SetupError(msg)

    if not group_id and ack_policy is not AckPolicy.ACK_FIRST:
        msg = "You must use `group_id` with manual commit mode."
        raise SetupError(msg)

    if not topics and not partitions and not pattern:
        msg = "You should provide either `topics` or `partitions` or `pattern`."
        raise SetupError(msg)
    if topics and partitions:
        msg = "You can't provide both `topics` and `partitions`."
        raise SetupError(msg)
    if topics and pattern:
        msg = "You can't provide both `topics` and `pattern`."
        raise SetupError(msg)
    if partitions and pattern:
        msg = "You can't provide both `partitions` and `pattern`."
        raise SetupError(msg)
