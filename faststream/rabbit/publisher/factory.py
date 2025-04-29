from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.publisher.configs import (
    SpecificationPublisherConfigs,
)
from faststream.rabbit.publisher.configs import RabbitPublisherBaseConfigs
from faststream.rabbit.schemas.base import RabbitBaseConfigs

from .specified import SpecificationPublisher

if TYPE_CHECKING:
    from aio_pika import IncomingMessage

    from faststream._internal.types import BrokerMiddleware, PublisherMiddleware
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

    from .usecase import PublishKwargs


def create_publisher(
    *,
    routing_key: str,
    queue: "RabbitQueue",
    exchange: "RabbitExchange",
    message_kwargs: "PublishKwargs",
    # Publisher args
    broker_middlewares: Sequence["BrokerMiddleware[IncomingMessage]"],
    middlewares: Sequence["PublisherMiddleware"],
    # AsyncAPI args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> SpecificationPublisher:
    base_configs = RabbitPublisherBaseConfigs(
        routing_key=routing_key,
        queue=queue,
        exchange=exchange,
        message_kwargs=message_kwargs,
        broker_middlewares=broker_middlewares,
        middlewares=middlewares,
    )

    rmq_base_configs = RabbitBaseConfigs(
        queue=queue,
        exchange=exchange,
    )

    specification_configs = SpecificationPublisherConfigs(
        schema_=schema_,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    return SpecificationPublisher(
        base_configs=base_configs,
        rmq_base_configs=rmq_base_configs,
        specification_configs=specification_configs,
    )
