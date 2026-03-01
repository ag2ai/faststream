import warnings
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy

from .config import KafkaSubscriberConfig, KafkaSubscriberSpecificationConfig
from .specification import KafkaSubscriberSpecification
from .usecase import (
    BatchSubscriber,
    ConcurrentDefaultSubscriber,
    DefaultSubscriber,
)

if TYPE_CHECKING:
    from faststream.confluent.configs import KafkaBrokerConfig
    from faststream.confluent.schemas import TopicPartition


def create_subscriber(
    *topics: str,
    partitions: Sequence["TopicPartition"],
    polling_interval: float,
    batch: bool,
    max_records: int | None,
    # Kafka information
    group_id: str | None,
    connection_data: dict[str, Any],
    auto_commit: bool,
    # Subscriber args
    ack_policy: "AckPolicy",
    no_ack: bool,
    max_workers: int,
    no_reply: bool,
    config: "KafkaBrokerConfig",
    # Specification args
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> BatchSubscriber | ConcurrentDefaultSubscriber | DefaultSubscriber:
    _validate_input_for_misconfigure(
        *topics,
        group_id=group_id,
        partitions=partitions,
        ack_policy=ack_policy,
        no_ack=no_ack,
        auto_commit=auto_commit,
        max_workers=max_workers,
    )

    subscriber_config = KafkaSubscriberConfig(
        topics=topics,
        partitions=partitions,
        polling_interval=polling_interval,
        group_id=group_id,
        connection_data=connection_data,
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
            max_records=max_records,
        )

    if max_workers > 1:
        return ConcurrentDefaultSubscriber(
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
    group_id: str | None,
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

    if ack_policy is not AckPolicy.ACK_FIRST and max_workers > 1:
        msg = "Max workers not work with manual commit mode."
        raise SetupError(msg)

    if not topics and not partitions:
        msg = "You should provide either `topics` or `partitions`."
        raise SetupError(msg)

    if topics and partitions:
        msg = "You can't provide both `topics` and `partitions`."
        raise SetupError(msg)

    if not group_id and ack_policy is not AckPolicy.ACK_FIRST:
        msg = "You must use `group_id` with manual commit mode."
        raise SetupError(msg)
