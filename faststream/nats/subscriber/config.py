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

    _ack_first: bool = field(default_factory=lambda: EMPTY, repr=False)
    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)

    @property
    def ack_policy(self) -> AckPolicy:
        if self._no_ack is not EMPTY and self._no_ack:
            return AckPolicy.MANUAL

        if self._ack_first is not EMPTY and self._ack_first:
            return AckPolicy.ACK_FIRST

        if self._ack_policy is EMPTY:
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy
