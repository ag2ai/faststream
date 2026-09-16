from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from faststream._internal.constants import EMPTY
from faststream.api.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream.middlewares import AckPolicy
from faststream.nats.configs import NatsBrokerConfig

if TYPE_CHECKING:
    from nats.js.api import ConsumerConfig


@dataclass(kw_only=True)
class NatsSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    subject: str
    queue: str | None
    # A JetStream consumer may address a stream through `filter_subjects` instead of `subject`,
    # so the specification layer needs them to render a meaningful address.
    filter_subjects: list[str] = field(default_factory=list)


@dataclass(kw_only=True)
class NatsSubscriberConfig(SubscriberUsecaseConfig):
    outer_config: "NatsBrokerConfig" = field(default_factory=NatsBrokerConfig)

    subject: str
    sub_config: "ConsumerConfig"
    extra_options: dict[str, Any] | None = field(default_factory=dict)

    @property
    @override
    def resolved_ack_policy(self) -> AckPolicy:
        if self.ack_policy is EMPTY:
            if self.outer_config.ack_policy is not EMPTY:
                return self.outer_config.ack_policy
            return AckPolicy.REJECT_ON_ERROR

        return self.ack_policy
