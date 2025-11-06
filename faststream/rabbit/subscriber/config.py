from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
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
    _outer_config: "RabbitBrokerConfig" = field(default_factory=RabbitBrokerConfig)

    consume_args: dict[str, Any] | None = None
    channel: Optional["Channel"] = None

    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)

    @property
    def ack_first(self) -> bool:
        return self.ack_policy is AckPolicy.ACK_FIRST

    @property
    def auto_ack_disabled(self) -> bool:
        return self.ack_policy in {AckPolicy.MANUAL, AckPolicy.ACK_FIRST}

    @property
    def ack_policy(self) -> AckPolicy:
        if self._no_ack is not EMPTY and self._no_ack:
            return AckPolicy.MANUAL

        if self._ack_policy is EMPTY:
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy
