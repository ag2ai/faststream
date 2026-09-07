from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import override

from faststream._internal.constants import EMPTY
from faststream.api.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream.middlewares import AckPolicy
from faststream.rabbit.configs import RabbitBrokerConfig, RabbitConfig

if TYPE_CHECKING:
    from faststream.rabbit.schemas import Channel


@dataclass(kw_only=True)
class RabbitSubscriberSpecificationConfig(
    RabbitConfig,
    SubscriberSpecificationConfig,
):
    pass


@dataclass(kw_only=True)
class RabbitSubscriberConfig(RabbitConfig, SubscriberUsecaseConfig):
    outer_config: "RabbitBrokerConfig" = field(default_factory=RabbitBrokerConfig)

    consume_args: dict[str, Any] | None = None
    channel: Optional["Channel"] = None

    @property
    def ack_first(self) -> bool:
        return self.resolved_ack_policy is AckPolicy.ACK_FIRST

    @property
    def auto_ack_disabled(self) -> bool:
        return self.resolved_ack_policy in {AckPolicy.MANUAL, AckPolicy.ACK_FIRST}

    @property
    @override
    def resolved_ack_policy(self) -> AckPolicy:
        if self.ack_policy is EMPTY:
            if self.outer_config.ack_policy is not EMPTY:
                return self.outer_config.ack_policy
            return AckPolicy.REJECT_ON_ERROR

        return self.ack_policy
