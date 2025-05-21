from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional

from faststream.rabbit.configs import RabbitPublisherConfigFacade

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
    config = RabbitPublisherConfigFacade(
        routing_key=routing_key,
        message_kwargs=message_kwargs,
        broker_middlewares=broker_middlewares,
        middlewares=middlewares,
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
