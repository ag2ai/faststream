from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional

from faststream.rabbit.configs import RabbitPublisherConfigFacade

from .specified import SpecificationPublisher

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware
    from faststream.rabbit.configs import RabbitBrokerConfig
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

    from .usecase import PublishKwargs


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
    # AsyncAPI args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> SpecificationPublisher:
    config = RabbitPublisherConfigFacade(
        routing_key=routing_key,
        message_kwargs=message_kwargs,
        middlewares=middlewares,
        # broker
        config=config,
        # rmq
        queue=queue,
        exchange=exchange,
        # specification
        schema_=schema_,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    return SpecificationPublisher(config)
