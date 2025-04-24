from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, cast, overload

from faststream._internal.publisher.configs import (
    SpecificationPublisherConfigs,
)
from faststream.confluent.publisher.configs import ConfluentPublisherBaseConfigs
from faststream.exceptions import SetupError

from .specified import SpecificationBatchPublisher, SpecificationDefaultPublisher

if TYPE_CHECKING:
    from confluent_kafka import Message as ConfluentMsg

    from faststream._internal.types import BrokerMiddleware, PublisherMiddleware


@overload
def create_publisher(
    *,
    batch: Literal[True],
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[dict[str, str]],
    reply_to: str,
    # Publisher args
    broker_middlewares: Sequence["BrokerMiddleware[tuple[ConfluentMsg, ...]]"],
    middlewares: Sequence["PublisherMiddleware"],
    # Specification args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> "SpecificationBatchPublisher": ...


@overload
def create_publisher(
    *,
    batch: Literal[False],
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[dict[str, str]],
    reply_to: str,
    # Publisher args
    broker_middlewares: Sequence["BrokerMiddleware[ConfluentMsg]"],
    middlewares: Sequence["PublisherMiddleware"],
    # Specification args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> "SpecificationDefaultPublisher": ...


@overload
def create_publisher(
    *,
    batch: bool,
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[dict[str, str]],
    reply_to: str,
    # Publisher args
    broker_middlewares: Union[
        Sequence["BrokerMiddleware[ConfluentMsg]"],
        Sequence["BrokerMiddleware[tuple[ConfluentMsg, ...]]"],
    ],
    middlewares: Sequence["PublisherMiddleware"],
    # Specification args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationBatchPublisher",
    "SpecificationDefaultPublisher",
]: ...


def create_publisher(
    *,
    batch: bool,
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[dict[str, str]],
    reply_to: str,
    # Publisher args
    broker_middlewares: Union[
        Sequence["BrokerMiddleware[ConfluentMsg]"],
        Sequence["BrokerMiddleware[tuple[ConfluentMsg, ...]]"],
    ],
    middlewares: Sequence["PublisherMiddleware"],
    # Specification args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationBatchPublisher",
    "SpecificationDefaultPublisher",
]:
    base_configs = ConfluentPublisherBaseConfigs(
        key=key,
        topic=topic,
        partition=partition,
        headers=headers,
        reply_to=reply_to,
        broker_middlewares=cast(
            "Sequence[BrokerMiddleware[tuple[ConfluentMsg, ...]]]",
            broker_middlewares,
        ),
        middlewares=middlewares,
    )
    specification_configs = SpecificationPublisherConfigs(
        schema_=schema_,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    if batch:
        if key:
            msg = "You can't setup `key` with batch publisher"
            raise SetupError(msg)

        return SpecificationBatchPublisher(
            specification_configs=specification_configs,
            base_configs=base_configs,
        )

    return SpecificationDefaultPublisher(
        specification_configs=specification_configs,
        base_configs=base_configs,
    )
