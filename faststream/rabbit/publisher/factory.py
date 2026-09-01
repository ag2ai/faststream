from typing import TYPE_CHECKING, Any

from faststream._internal.utils.path import Address
from faststream.rabbit.schemas.queue import RABBIT_ADDRESS_SYNTAX

from .config import RabbitPublisherConfig, RabbitPublisherSpecificationConfig
from .specification import RabbitPublisherSpecification
from .usecase import RabbitPublisher

if TYPE_CHECKING:
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
    # Specification args
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> RabbitPublisher:
    # A bare `routing_key` is a declaration too — the same one `RabbitQueue`
    # holds for its own — so it is read as one rather than carried as a string.
    routing_address = Address(routing_key, RABBIT_ADDRESS_SYNTAX)

    publisher_config = RabbitPublisherConfig(
        routing_address=routing_address,
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
            routing_address=routing_address,
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
