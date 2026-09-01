from dataclasses import dataclass, field

from zmqtt import QoS

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream._internal.utils.path import Address
from faststream.mqtt.broker.config import MQTTBrokerConfig


@dataclass(kw_only=True)
class MQTTPublisherSpecificationConfig(PublisherSpecificationConfig):
    address: Address
    qos: QoS = QoS.AT_MOST_ONCE
    retain: bool = False


@dataclass(kw_only=True)
class MQTTPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "MQTTBrokerConfig" = field(default_factory=MQTTBrokerConfig)

    address: Address
    qos: QoS
    retain: bool
    headers: dict[str, str] | None
