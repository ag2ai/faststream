from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.constants import EMPTY
from faststream._internal.subscriber.configs import SubscriberUseCaseConfigs
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream.rabbit.schemas import (
        Channel,
        RabbitExchange,
        RabbitQueue,
    )


@dataclass
class RabbitSubscriberBaseConfigs(SubscriberUseCaseConfigs):
    queue: "RabbitQueue"
    consume_args: Optional["AnyDict"]
    no_ack: bool
    channel: Optional["Channel"]
    exchange: "RabbitExchange"

    _no_ack: AckPolicy = field(init=False, repr=False)

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            return AckPolicy.DO_NOTHING if self.no_ack else AckPolicy.REJECT_ON_ERROR
        return self._ack_policy

    @ack_policy.setter
    def ack_policy(self, policy: AckPolicy) -> None:
        self._ack_policy = policy

    @property
    def no_ack(self) -> bool:
        self._no_ack = self.ack_policy is AckPolicy.ACK_FIRST
        if self._no_ack:
            self.ack_policy(AckPolicy.DO_NOTHING)
        return self._no_ack

    @no_ack.setter
    def no_ack(self, consumer_no_ack: AckPolicy) -> None:
        self._no_ack = consumer_no_ack
