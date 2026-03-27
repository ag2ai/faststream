from dataclasses import dataclass, field

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.mq.configs import MQBrokerConfig, MQConfig


@dataclass(kw_only=True)
class MQSubscriberSpecificationConfig(MQConfig, SubscriberSpecificationConfig):
    pass


@dataclass(kw_only=True)
class MQSubscriberConfig(MQConfig, SubscriberUsecaseConfig):
    _outer_config: MQBrokerConfig = field(default_factory=MQBrokerConfig)  # type: ignore[arg-type]

    wait_interval: float = 1.0

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            return AckPolicy.NACK_ON_ERROR
        return self._ack_policy
