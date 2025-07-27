from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.redis.configs import RedisBrokerConfig
from faststream.redis.schemas import ListSub, PubSub, StreamSub

if TYPE_CHECKING:
    from faststream.redis.parser import MessageFormat


class RedisSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    pass


@dataclass(kw_only=True)
class RedisSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: RedisBrokerConfig

    list_sub: ListSub | None = field(default=None, repr=False)
    channel_sub: PubSub | None = field(default=None, repr=False)
    stream_sub: StreamSub | None = field(default=None, repr=False)

    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)

    _message_format: type["MessageFormat"] | None = field(default=None, repr=False)

    @property
    def message_format(self) -> type["MessageFormat"]:
        return self._message_format or self._outer_config.message_format

    @property
    def ack_policy(self) -> AckPolicy:
        if self._no_ack is not EMPTY and self._no_ack:
            return AckPolicy.MANUAL

        if self.list_sub:
            return AckPolicy.MANUAL

        if self.channel_sub:
            return AckPolicy.MANUAL

        if self.stream_sub and (self.stream_sub.no_ack or not self.stream_sub.group):
            return AckPolicy.MANUAL

        if self._ack_policy is EMPTY:
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy
