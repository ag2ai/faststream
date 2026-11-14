from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.rabbit.configs import RabbitBrokerConfig

if TYPE_CHECKING:
    from faststream.rabbit.schemas import Channel, RabbitExchange, RabbitQueue


@dataclass(kw_only=True, slots=True)
class RabbitSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    queue: "RabbitQueue"
    exchange: "RabbitExchange"


@dataclass(kw_only=True, slots=True)
class RabbitSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "RabbitBrokerConfig" = field(default_factory=RabbitBrokerConfig)

    queue: "RabbitQueue"
    exchange: "RabbitExchange"
    consume_args: dict[str, Any] | None = None
    channel: Optional["Channel"] = None

    @property
    def ack_first(self) -> bool:
        return self.ack_policy is AckPolicy.ACK_FIRST

    @property
    def auto_ack_disabled(self) -> bool:
        return self.ack_policy in {AckPolicy.MANUAL, AckPolicy.ACK_FIRST}

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            if self._outer_config.ack_policy is not EMPTY:
                return self._outer_config.ack_policy
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy
