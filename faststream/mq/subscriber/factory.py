from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber.call_item import CallsCollection

from .config import MQSubscriberConfig, MQSubscriberSpecificationConfig
from .specification import MQSubscriberSpecification
from .usecase import MQSubscriber

if TYPE_CHECKING:
    from faststream.middlewares import AckPolicy
    from faststream.mq.configs import MQBrokerConfig
    from faststream.mq.schemas import MQQueue


def create_subscriber(
    *,
    queue: "MQQueue",
    no_reply: bool,
    ack_policy: "AckPolicy",
    wait_interval: float,
    config: "MQBrokerConfig",
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> MQSubscriber:
    subscriber_config = MQSubscriberConfig(
        queue=queue,
        no_reply=no_reply,
        _ack_policy=ack_policy,
        wait_interval=wait_interval,
        _outer_config=config,
    )

    calls = CallsCollection[Any]()
    specification = MQSubscriberSpecification(
        _outer_config=config,
        specification_config=MQSubscriberSpecificationConfig(
            queue=queue,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
        calls=calls,
    )

    return MQSubscriber(
        config=subscriber_config,
        specification=specification,
        calls=calls,
    )
