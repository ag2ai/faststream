from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable

    from .broker import BrokerConfig


@dataclass(kw_only=True)
class EndpointConfig:
    outer_config: "BrokerConfig"


@dataclass(kw_only=True)
class PublisherUsecaseConfig(EndpointConfig):
    pass


@dataclass(kw_only=True)
class SubscriberUsecaseConfig(EndpointConfig, ABC):
    no_reply: bool = False

    ack_policy: AckPolicy = field(default_factory=lambda: EMPTY, repr=False)

    parser: "AsyncCallable" = field(init=False)
    decoder: "AsyncCallable" = field(init=False)

    @property
    @abstractmethod
    def resolved_ack_policy(self) -> AckPolicy:
        raise NotImplementedError

    @property
    def auto_ack_disabled(self) -> bool:
        return self.resolved_ack_policy is AckPolicy.MANUAL
