from dataclasses import dataclass, field
from re import Pattern

from typing_extensions import override
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
    outer_config: "MQTTBrokerConfig" = field(default_factory=MQTTBrokerConfig)

    topic: str
    qos: QoS = QoS.AT_MOST_ONCE
    shared: str | None = None
    path_regex: Pattern[str] | None = None

    @property
    @override
    def resolved_ack_policy(self) -> AckPolicy:
        if self.ack_policy is EMPTY:
            if self.outer_config.ack_policy is not EMPTY:
                return self.outer_config.ack_policy
            return AckPolicy.ACK
        return self.ack_policy
