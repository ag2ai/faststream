import warnings
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Literal,
    Optional,
    Union,
    overload,
)

from faststream._internal.constants import EMPTY
from faststream.confluent.subscriber.specified import (
    SpecificationBatchSubscriber,
    SpecificationDefaultSubscriber,
)

if TYPE_CHECKING:
    from confluent_kafka import Message as ConfluentMsg
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import BrokerMiddleware
    from faststream.confluent.schemas import TopicPartition
    from faststream.middlewares import AckPolicy


@overload
def create_subscriber(
    *topics: str,
    partitions: Sequence["TopicPartition"],
    polling_interval: float,
    batch: Literal[True],
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    connection_data: "AnyDict",
    is_manual: bool,
    # Subscriber args
    ack_policy: "AckPolicy" = EMPTY,
    no_reply: bool,
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Iterable["BrokerMiddleware[tuple[ConfluentMsg, ...]]"],
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> "SpecificationBatchSubscriber": ...


@overload
def create_subscriber(
    *topics: str,
    partitions: Sequence["TopicPartition"],
    polling_interval: float,
    batch: Literal[False],
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    connection_data: "AnyDict",
    is_manual: bool,
    # Subscriber args
    ack_policy: "AckPolicy" = EMPTY,
    no_reply: bool,
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Iterable["BrokerMiddleware[ConfluentMsg]"],
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> "SpecificationDefaultSubscriber": ...


@overload
def create_subscriber(
    *topics: str,
    partitions: Sequence["TopicPartition"],
    polling_interval: float,
    batch: bool,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    connection_data: "AnyDict",
    is_manual: bool,
    # Subscriber args
    ack_policy: "AckPolicy" = EMPTY,
    no_reply: bool,
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Iterable[
        "BrokerMiddleware[Union[ConfluentMsg, tuple[ConfluentMsg, ...]]]"
    ],
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationDefaultSubscriber",
    "SpecificationBatchSubscriber",
]: ...


def create_subscriber(
    *topics: str,
    partitions: Sequence["TopicPartition"],
    polling_interval: float,
    batch: bool,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    connection_data: "AnyDict",
    is_manual: bool,
    # Subscriber args
    ack_policy: "AckPolicy" = EMPTY,
    no_reply: bool,
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Iterable[
        "BrokerMiddleware[Union[ConfluentMsg, tuple[ConfluentMsg, ...]]]"
    ],
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationDefaultSubscriber",
    "SpecificationBatchSubscriber",
]:
    if ack_policy is not EMPTY and is_manual == False:
        warnings.warn(RuntimeWarning, "You can't use acknowledgement policy with core subscriber", stacklevel=2)

    if batch:
        return SpecificationBatchSubscriber(
            *topics,
            partitions=partitions,
            polling_interval=polling_interval,
            max_records=max_records,
            group_id=group_id,
            connection_data=connection_data,
            is_manual=is_manual,
            ack_policy=AckPolicy.REJECT_ON_ERROR if ack_policy is EMPTY and is_manual == True else ack_policy,
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
    return SpecificationDefaultSubscriber(
        *topics,
        partitions=partitions,
        polling_interval=polling_interval,
        group_id=group_id,
        connection_data=connection_data,
        is_manual=is_manual,
        ack_policy=AckPolicy.REJECT_ON_ERROR if ack_policy is EMPTY and is_manual == True else ack_policy,
        no_reply=no_reply,
        broker_dependencies=broker_dependencies,
        broker_middlewares=broker_middlewares,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )
