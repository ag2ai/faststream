from typing import TYPE_CHECKING, Any

from zmqtt import QoS

from faststream._internal.utils.path import Address
from faststream.mqtt.path import MQTT_ADDRESS_SYNTAX

from .config import MQTTPublisherConfig, MQTTPublisherSpecificationConfig
from .specification import MQTTPublisherSpecification
from .usecase import MQTTPublisher

if TYPE_CHECKING:
    from faststream.mqtt.broker.config import MQTTBrokerConfig


def create_publisher(
    *,
    topic: str,
    qos: QoS,
    retain: bool,
    headers: dict[str, str] | None,
    # Publisher args
    broker_config: "MQTTBrokerConfig",
    # AsyncAPI args
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> MQTTPublisher:
    address = Address(topic, MQTT_ADDRESS_SYNTAX)

    publisher_config = MQTTPublisherConfig(
        address=address,
        qos=qos,
        retain=retain,
        headers=headers,
        _outer_config=broker_config,
    )

    specification = MQTTPublisherSpecification(
        _outer_config=broker_config,
        specification_config=MQTTPublisherSpecificationConfig(
            address=address,
            qos=qos,
            retain=retain,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )

    return MQTTPublisher(publisher_config, specification)
