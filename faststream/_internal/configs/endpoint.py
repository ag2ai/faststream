from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic

from faststream._internal.constants import EMPTY
from faststream._internal.types import MsgTypeWithFallback, StreamMsgType
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.types import Decoder, Parser

    from .broker import BrokerConfig


@dataclass(kw_only=True)
class EndpointConfig:
    _outer_config: "BrokerConfig"


@dataclass(kw_only=True)
class PublisherUsecaseConfig(EndpointConfig):
    pass


@dataclass(kw_only=True)
class SubscriberUsecaseConfig(
    EndpointConfig, Generic[MsgTypeWithFallback, StreamMsgType]
):
    no_reply: bool = False

    _ack_policy: AckPolicy = field(default_factory=lambda: EMPTY, repr=False)

    parser: "Parser[MsgTypeWithFallback, StreamMsgType]" = field(init=False)
    decoder: "Decoder[StreamMsgType]" = field(init=False)

    @property
    def auto_ack_disabled(self) -> bool:
        return self.ack_policy is AckPolicy.MANUAL

    @property
    def ack_policy(self) -> AckPolicy:
        raise NotImplementedError
