import warnings
from collections.abc import Collection, Iterable
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy

from .config import KafkaSubscriberConfig, KafkaSubscriberSpecificationConfig
from .specification import KafkaSubscriberSpecification
from .usecase import (
    BatchSubscriber,
    ConcurrentBetweenPartitionsSubscriber,
    ConcurrentDefaultSubscriber,
    DefaultSubscriber,
)

if TYPE_CHECKING:
    from aiokafka import TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener

    from faststream.kafka.configs import KafkaBrokerConfig


def create_subscriber(
    *topics: str,
    batch: bool,
    batch_timeout_ms: int,
    max_records: int | None,
    # Kafka information
    group_id: str | None,
    listener: Optional["ConsumerRebalanceListener"],
    pattern: str | None,
    connection_args: dict[str, Any],
    partitions: Collection["TopicPartition"],
    auto_commit: bool,
    # Subscriber args
    ack_policy: "AckPolicy",
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    config: "KafkaBrokerConfig",
    # Specification args
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> Union[
    "DefaultSubscriber",
    "BatchSubscriber",
    "ConcurrentDefaultSubscriber",
    "ConcurrentBetweenPartitionsSubscriber",
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

    subscriber_config = KafkaSubscriberConfig(
        topics=topics,
        partitions=partitions,
        connection_args=connection_args,
        group_id=group_id,
        listener=listener,
        pattern=pattern,
        no_reply=no_reply,
        _outer_config=config,
        _ack_policy=ack_policy,
        # deprecated options to remove in 0.7.0
        _auto_commit=auto_commit,
        _no_ack=no_ack,
    )

    calls = CallsCollection[Any]()

    specification = KafkaSubscriberSpecification(
        _outer_config=config,
        calls=calls,
        specification_config=KafkaSubscriberSpecificationConfig(
            topics=topics,
            partitions=partitions,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )

    if batch:
        return BatchSubscriber(
            subscriber_config,
            specification,
            calls,
            batch_timeout_ms=batch_timeout_ms,
            max_records=max_records,
        )

    if max_workers > 1:
        if subscriber_config.ack_first:
            return ConcurrentDefaultSubscriber(
                subscriber_config,
                specification,
                calls,
                max_workers=max_workers,
            )

        subscriber_config.topics = (topics[0],)
        return ConcurrentBetweenPartitionsSubscriber(
            subscriber_config,
            specification,
            calls,
            max_workers=max_workers,
        )

    return DefaultSubscriber(subscriber_config, specification, calls)


def _validate_input_for_misconfigure(
    *topics: str,
    ack_policy: "AckPolicy",
    auto_commit: bool,
    no_ack: bool,
    max_workers: int,
    pattern: str | None,
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
            "`no_ack` option was deprecated in prior to `ack_policy=AckPolicy.MANUAL`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `no_ack` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

        ack_policy = AckPolicy.MANUAL if no_ack else EMPTY

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.ACK_FIRST
    if ack_policy is AckPolicy.REJECT_ON_ERROR:
        warnings.warn(
            "AckPolicy.REJECT_ON_ERROR has the same effect as AckPolicy.ACK. "
            "Consider using ACK for clarity.",
            UserWarning,
            stacklevel=4,
        )

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
