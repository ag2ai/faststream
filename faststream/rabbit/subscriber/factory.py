from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.endpoint.subscriber.call_item import CallsCollection

from .config import (
    RabbitSubscriberConfig,
    RabbitSubscriberSpecificationConfig,
)
from .specification import RabbitSubscriberSpecification
from .usecase import RabbitSubscriber

if TYPE_CHECKING:
    from faststream.middlewares import AckPolicy
    from faststream.rabbit.configs import RabbitBrokerConfig
    from faststream.rabbit.schemas import Channel, RabbitExchange, RabbitQueue


def create_subscriber(
    *,
    queue: "RabbitQueue",
    exchange: "RabbitExchange",
    consume_args: dict[str, Any] | None,
    channel: Optional["Channel"],
    # Subscriber args
    no_reply: bool,
    ack_policy: "AckPolicy",
    # Broker args
    config: "RabbitBrokerConfig",
    # Specification args
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> RabbitSubscriber:
    subscriber_config = RabbitSubscriberConfig(
        no_reply=no_reply,
        consume_args=consume_args,
        channel=channel,
        queue=queue,
        exchange=exchange,
        _ack_policy=ack_policy,
        _outer_config=config,
    )

    calls = CallsCollection[Any]()

    specification = RabbitSubscriberSpecification(
        _outer_config=config,
        specification_config=RabbitSubscriberSpecificationConfig(
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
            queue=queue,
            exchange=exchange,
        ),
        calls=calls,
    )

    return RabbitSubscriber(
        config=subscriber_config,
        specification=specification,
        calls=calls,
    )
