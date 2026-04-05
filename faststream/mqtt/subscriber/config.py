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


@dataclass(kw_only=True)
class MQTTSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "MQTTBrokerConfig" = field(default_factory=MQTTBrokerConfig)

    topic: str
    qos: QoS = QoS.AT_MOST_ONCE
    shared: str | None = None

    @property
    def effective_topic(self) -> str:
        """Return topic with $share prefix when shared group is set."""
        return f"$share/{self.shared}/{self.topic}" if self.shared else self.topic

    @property
    def ack_policy(self) -> AckPolicy:
        # QoS 0 has no protocol-level ack — force MANUAL
        if self.qos == 0:
            return AckPolicy.MANUAL
        if self._ack_policy is not EMPTY:
            return self._ack_policy
        return AckPolicy.ACK_FIRST
