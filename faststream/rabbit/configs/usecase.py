from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.publisher import PublisherUsecaseConfig
from faststream._internal.endpoint.subscriber import SubscriberUsecaseConfig
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream.rabbit.publisher.usecase import PublishKwargs
    from faststream.rabbit.schemas import (
        Channel,
        RabbitExchange,
        RabbitQueue,
    )

    from .broker import RabbitBrokerConfig


@dataclass(kw_only=True)
class RabbitEndpointConfig:
    config: "RabbitBrokerConfig"
    queue: "RabbitQueue"
    exchange: "RabbitExchange"


@dataclass(kw_only=True)
class RabbitSubscriberConfig(RabbitEndpointConfig, SubscriberUsecaseConfig):
    consume_args: Optional["AnyDict"] = None
    channel: Optional["Channel"] = None

    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)

    @property
    def ack_first(self) -> bool:
        return self.__ack_policy is AckPolicy.ACK_FIRST

    @property
    def ack_policy(self) -> AckPolicy:
        if (policy := self.__ack_policy) is AckPolicy.ACK_FIRST:
            return AckPolicy.DO_NOTHING

        return policy

    @property
    def __ack_policy(self) -> AckPolicy:
        if self._no_ack is not EMPTY and self._no_ack:
            return AckPolicy.DO_NOTHING

        if self._ack_policy is EMPTY:
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy


@dataclass(kw_only=True)
class RabbitPublisherConfig(RabbitEndpointConfig, PublisherUsecaseConfig):
    routing_key: str
    message_kwargs: "PublishKwargs"
