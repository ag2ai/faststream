from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable, PublisherMiddleware

    from .broker import BrokerConfig


@dataclass(kw_only=True)
class EndpointConfig:
    _outer_config: "BrokerConfig"


@dataclass(kw_only=True)
class PublisherUsecaseConfig(EndpointConfig):
    middlewares: Sequence["PublisherMiddleware[Any]"]


@dataclass(kw_only=True)
class SubscriberUsecaseConfig(EndpointConfig):
    no_reply: bool = False

    _ack_policy: AckPolicy = field(default_factory=lambda: EMPTY, repr=False)

    parser: "AsyncCallable" = field(init=False)
    decoder: "AsyncCallable" = field(init=False)

    @property
    def auto_ack_disabled(self) -> bool:
        return self.ack_policy is AckPolicy.MANUAL

    @property
    def ack_policy(self) -> AckPolicy:
        raise NotImplementedError
