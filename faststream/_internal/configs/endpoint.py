from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable

    from .broker import BrokerConfig


@dataclass(kw_only=True)
class EndpointConfig:
    _outer_config: "BrokerConfig"


@dataclass(kw_only=True)
class PublisherUsecaseConfig(EndpointConfig):
    pass


@dataclass(kw_only=True)
class SubscriberUsecaseConfig(EndpointConfig):
    no_reply: bool = False

    _ack_policy: AckPolicy = field(default_factory=lambda: EMPTY, repr=False)

    # What a Subscriber's constructor built, for the Brokers that still build
    # their parser there. `None` for a Broker that builds its own in Preparation,
    # because the capture regex it holds needs an address that is resolved by then.
    parser: Optional["AsyncCallable"] = field(init=False, default=None)
    decoder: Optional["AsyncCallable"] = field(init=False, default=None)

    @property
    def auto_ack_disabled(self) -> bool:
        return self.ack_policy is AckPolicy.MANUAL

    @property
    def ack_policy(self) -> AckPolicy:
        raise NotImplementedError
