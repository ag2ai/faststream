from dataclasses import dataclass, field

from zmqtt import QoS

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.mqtt.broker.config import MQTTBrokerConfig


@dataclass(kw_only=True)
class MQTTSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    topic: str
    qos: QoS = QoS.AT_MOST_ONCE
    shared: str | None = None


@dataclass(kw_only=True)
class MQTTSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "MQTTBrokerConfig" = field(default_factory=MQTTBrokerConfig)

    topic: str
    qos: QoS = QoS.AT_MOST_ONCE
    shared: str | None = None

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            if self._outer_config.ack_policy is not EMPTY:
                return self._outer_config.ack_policy
            return AckPolicy.ACK
        return self._ack_policy
