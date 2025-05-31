import warnings
from collections.abc import Collection, Iterable
from typing import TYPE_CHECKING, Optional, Union

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.kafka.configs import KafkaSubscriberConfigFacade
from faststream.kafka.subscriber.specified import (
    SpecificationBatchSubscriber,
    SpecificationConcurrentBetweenPartitionsSubscriber,
    SpecificationConcurrentDefaultSubscriber,
    SpecificationDefaultSubscriber,
)
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from aiokafka import TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener

    from faststream._internal.basic_types import AnyDict
    from faststream.kafka.configs import KafkaBrokerConfig


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
    auto_commit: bool,
    # Subscriber args
    ack_policy: "AckPolicy",
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    config: "KafkaBrokerConfig",
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationDefaultSubscriber",
    "SpecificationBatchSubscriber",
    "SpecificationConcurrentDefaultSubscriber",
    "SpecificationConcurrentBetweenPartitionsSubscriber",
]:
    _validate_input_for_misconfigure(
        *topics,
        pattern=pattern,
        partitions=partitions,
        ack_policy=ack_policy,
        no_ack=no_ack,
        auto_commit=auto_commit,
        max_workers=max_workers,
    )

    config = KafkaSubscriberConfigFacade(
        topics=topics,
        partitions=partitions,
        connection_args=connection_args,
        group_id=group_id,
        listener=listener,
        pattern=pattern,
        no_reply=no_reply,
        config=config,
        _ack_policy=ack_policy,
        # deprecated options to remove in 0.7.0
        _auto_commit=auto_commit,
        _no_ack=no_ack,
        # specification
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    if batch:
        return SpecificationBatchSubscriber(
            config,
            batch_timeout_ms=batch_timeout_ms,
            max_records=max_records,
        )

    if max_workers > 1:
        if config.ack_first:
            return SpecificationConcurrentDefaultSubscriber(
                config,
                max_workers=max_workers,
            )

        config.topics = (topics[0],)
        return SpecificationConcurrentBetweenPartitionsSubscriber(
            config,
            max_workers=max_workers,
        )

    return SpecificationDefaultSubscriber(config)


def _validate_input_for_misconfigure(
    *topics: str,
    ack_policy: "AckPolicy",
    auto_commit: bool,
    no_ack: bool,
    max_workers: int,
    pattern: Optional[str],
    partitions: Iterable["TopicPartition"],
) -> None:
    if auto_commit is not EMPTY:
        warnings.warn(
            "`auto_commit` option was deprecated in prior to `ack_policy=AckPolicy.ACK_FIRST`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `auto_commit` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

        ack_policy = AckPolicy.ACK_FIRST if auto_commit else AckPolicy.REJECT_ON_ERROR

    if no_ack is not EMPTY:
        warnings.warn(
            "`no_ack` option was deprecated in prior to `ack_policy=AckPolicy.DO_NOTHING`. Scheduled to remove in 0.7.0",
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
        if len(topics) > 1:
            msg = "You must use a single topic with concurrent manual commit mode."
            raise SetupError(msg)

        if pattern is not None:
            msg = "You can not use a pattern with concurrent manual commit mode."
            raise SetupError(msg)

        if partitions:
            msg = "Manual partition assignment is not supported with concurrent manual commit mode."
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
