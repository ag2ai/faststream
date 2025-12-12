from dataclasses import dataclass

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream.middlewares import AckPolicy


@dataclass(kw_only=True)
class MQTTSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    topic: str


@dataclass(kw_only=True)
class MQTTSubscriberConfig(SubscriberUsecaseConfig):
    topic: str

    @property
    def ack_policy(self) -> AckPolicy:
        # TODO: Implement ack policy logic
        return AckPolicy.ACK_FIRST
