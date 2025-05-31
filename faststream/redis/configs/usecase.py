from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.publisher import PublisherUsecaseConfig
from faststream._internal.endpoint.subscriber import SubscriberUsecaseConfig
from faststream.middlewares.acknowledgement.conf import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict
    from faststream.redis.schemas import ListSub, PubSub, StreamSub

    from .broker import RedisBrokerConfig


@dataclass
class RedisPublisherConfig(PublisherUsecaseConfig):
    config: "RedisBrokerConfig"

    reply_to: str
    headers: Optional["AnyDict"]


@dataclass
class RedisSubscriberConfig(SubscriberUsecaseConfig):
    config: "RedisBrokerConfig"

    list_sub: Optional["ListSub"] = field(default=None, repr=False)
    channel_sub: Optional["PubSub"] = field(default=None, repr=False)
    stream_sub: Optional["StreamSub"] = field(default=None, repr=False)

    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)

    @property
    def ack_policy(self) -> AckPolicy:
        if self._no_ack is not EMPTY and self._no_ack:
            return AckPolicy.DO_NOTHING

        if self.list_sub:
            return AckPolicy.DO_NOTHING

        if self.channel_sub:
            return AckPolicy.DO_NOTHING

        if self.stream_sub and (self.stream_sub.no_ack or not self.stream_sub.group):
            return AckPolicy.DO_NOTHING

        if self._ack_policy is EMPTY:
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy
