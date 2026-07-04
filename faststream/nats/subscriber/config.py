from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.nats.configs import NatsBrokerConfig

if TYPE_CHECKING:
    from nats.js.api import ConsumerConfig


@dataclass(kw_only=True)
class NatsSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    subject: str
    queue: str | None


@dataclass(kw_only=True)
class NatsSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "NatsBrokerConfig" = field(default_factory=NatsBrokerConfig)

    subject: str
    sub_config: "ConsumerConfig"
    extra_options: dict[str, Any] | None = field(default_factory=dict)

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            if self._outer_config.ack_policy is not EMPTY:
                return self._outer_config.ack_policy
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy
