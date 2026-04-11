from dataclasses import dataclass, field

from zmqtt import QoS

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.mqtt.broker.config import MQTTBrokerConfig


@dataclass(kw_only=True)
class MQTTPublisherSpecificationConfig(PublisherSpecificationConfig):
    topic: str
    qos: QoS = QoS.AT_MOST_ONCE
    retain: bool = False


@dataclass(kw_only=True)
class MQTTPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "MQTTBrokerConfig" = field(default_factory=MQTTBrokerConfig)

    topic: str
    qos: QoS
    retain: bool
    headers: dict[str, str] | None
