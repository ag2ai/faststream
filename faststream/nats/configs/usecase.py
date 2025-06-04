from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.publisher import PublisherUsecaseConfig
from faststream._internal.endpoint.subscriber import SubscriberUsecaseConfig
from faststream.middlewares import AckPolicy

from .broker import NatsBrokerConfig

if TYPE_CHECKING:
    from nats.js.api import ConsumerConfig

    from faststream._internal.basic_types import (
        AnyDict,
    )
    from faststream.nats.schemas import JStream


@dataclass(kw_only=True)
class NatsSubscriberConfig(SubscriberUsecaseConfig):
    config: "NatsBrokerConfig" = field(default_factory=NatsBrokerConfig)

    subject: str
    sub_config: "ConsumerConfig"
    extra_options: Optional["AnyDict"] = field(default_factory=dict)

    _ack_first: bool = field(default_factory=lambda: EMPTY, repr=False)
    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)

    @property
    def ack_policy(self) -> AckPolicy:
        if self._no_ack is not EMPTY and self._no_ack:
            return AckPolicy.DO_NOTHING

        if self._ack_first is not EMPTY and self._ack_first:
            return AckPolicy.ACK_FIRST

        if self._ack_policy is EMPTY:
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy


@dataclass(kw_only=True)
class NatsPublisherConfig(PublisherUsecaseConfig):
    config: "NatsBrokerConfig" = field(default_factory=NatsBrokerConfig)

    subject: str
    reply_to: str
    headers: Optional[dict[str, str]]
    stream: Optional["JStream"]
    timeout: Optional[float]
