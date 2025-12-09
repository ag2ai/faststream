from typing import Any

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.mqtt.configs.broker import MQTTBrokerConfig
from faststream.mqtt.subscriber.config import (
    MQTTSubscriberConfig,
    MQTTSubscriberSpecificationConfig,
)
from faststream.mqtt.subscriber.specification import MQTTSubscriberSpecification
from faststream.mqtt.subscriber.usecase import MQTTSubscriber


def create_subscriber(
    *,
    topic: str,
    config: MQTTBrokerConfig,
    # Specification args
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> MQTTSubscriber:
    subscriber_config = MQTTSubscriberConfig(topic=topic, _outer_config=config)

    calls = CallsCollection[Any]()

    specification = MQTTSubscriberSpecification(
        _outer_config=config,
        calls=calls,
        specification_config=MQTTSubscriberSpecificationConfig(
            topic=topic,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )
    return MQTTSubscriber(subscriber_config, specification, calls)
