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
    channel: Optional["Channel"]
    exchange: "RabbitExchange"
    no_ack: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self._ack_policy is AckPolicy.ACK_FIRST:
            self.no_ack = True
        else:
            self.no_ack = False

    @property
    def ack_policy(self) -> AckPolicy:
        consumer_no_ack = self._ack_policy is AckPolicy.ACK_FIRST

        if self._ack_policy is EMPTY:
            return AckPolicy.DO_NOTHING if consumer_no_ack else AckPolicy.REJECT_ON_ERROR

        if consumer_no_ack:
            return AckPolicy.DO_NOTHING
        return self._ack_policy

    @ack_policy.setter
    def ack_policy(self, policy: AckPolicy) -> None:
        self._ack_policy = policy
