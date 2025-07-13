from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from .config import RabbitPublisherConfig, RabbitPublisherSpecificationConfig
from .specification import RabbitPublisherSpecification
from .usecase import RabbitPublisher

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware
    from faststream.rabbit.configs import RabbitBrokerConfig
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

    from .options import PublishKwargs


def create_publisher(
    *,
    routing_key: str,
    queue: "RabbitQueue",
    exchange: "RabbitExchange",
    message_kwargs: "PublishKwargs",
    # Broker args
    config: "RabbitBrokerConfig",
    # Publisher args
    middlewares: Sequence["PublisherMiddleware"],
    # Specification args
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> RabbitPublisher:
    publisher_config = RabbitPublisherConfig(
        routing_key=routing_key,
        message_kwargs=message_kwargs,
        queue=queue,
        exchange=exchange,
        # publisher
        middlewares=middlewares,
        # broker
        _outer_config=config,
    )

    specification = RabbitPublisherSpecification(
        _outer_config=config,
        specification_config=RabbitPublisherSpecificationConfig(
            message_kwargs=message_kwargs,
            routing_key=routing_key,
            queue=queue,
            exchange=exchange,
            # specification options
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )

    return RabbitPublisher(
        config=publisher_config,
        specification=specification,
    )
