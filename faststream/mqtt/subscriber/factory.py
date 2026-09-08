from typing import TYPE_CHECKING, Any

from zmqtt import QoS

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.mqtt.path import build_mqtt_address

from .config import MQTTSubscriberConfig, MQTTSubscriberSpecificationConfig
from .specification import MQTTSubscriberSpecification
from .usecase import MQTTConcurrentSubscriber, MQTTDefaultSubscriber

if TYPE_CHECKING:
    from faststream.middlewares import AckPolicy
    from faststream.mqtt.broker.config import MQTTBrokerConfig

SubscriberType = MQTTDefaultSubscriber | MQTTConcurrentSubscriber


def create_subscriber(
    *,
    topic: str,
    qos: QoS,
    shared: str | None,
    # Subscriber args
    ack_policy: "AckPolicy",
    no_reply: bool,
    config: "MQTTBrokerConfig",
    max_workers: int = 1,
    # AsyncAPI args
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
) -> SubscriberType:
    address = build_mqtt_address(topic)

    subscriber_config = MQTTSubscriberConfig(
        address=address,
        qos=qos,
        shared=shared,
        no_reply=no_reply,
        _outer_config=config,
        _ack_policy=ack_policy,
    )

    specification_config = MQTTSubscriberSpecificationConfig(
        address=address,
        qos=qos,
        shared=shared,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    calls = CallsCollection[Any]()

    specification = MQTTSubscriberSpecification(
        config,
        specification_config,
        calls,
    )

    if max_workers > 1:
        return MQTTConcurrentSubscriber(
            subscriber_config,
            specification,
            calls,
            max_workers=max_workers,
        )

    return MQTTDefaultSubscriber(subscriber_config, specification, calls)
