from typing import TYPE_CHECKING, Any

from faststream._internal.config_value import Configurable

from .config import RabbitPublisherConfig, RabbitPublisherSpecificationConfig
from .specification import RabbitPublisherSpecification
from .usecase import RabbitPublisher

if TYPE_CHECKING:
    from faststream.rabbit.configs import (
        ConfigurableExchange,
        ConfigurableQueue,
        RabbitBrokerConfig,
    )

    from .options import PublishKwargs


def create_publisher(
    *,
    routing_key: Configurable[str],
    reply_to: Configurable[str] | None,
    queue: "ConfigurableQueue",
    exchange: "ConfigurableExchange",
    message_kwargs: "PublishKwargs",
    # Broker args
    config: "RabbitBrokerConfig",
    # Specification args
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> RabbitPublisher:
    publisher_config = RabbitPublisherConfig(
        routing_key=routing_key,
        reply_to=reply_to,
        message_kwargs=message_kwargs,
        queue=queue,
        exchange=exchange,
        # broker
        _outer_config=config,
    )

    specification = RabbitPublisherSpecification(
        _outer_config=config,
        specification_config=RabbitPublisherSpecificationConfig(
            message_kwargs=message_kwargs,
            routing_key=routing_key,
            reply_to=reply_to,
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
