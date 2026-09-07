from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from typing_extensions import override

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
    outer_config: RedisBrokerConfig

    list_sub: ListSub | None = field(default=None, repr=False)
    channel_sub: PubSub | None = field(default=None, repr=False)
    stream_sub: StreamSub | None = field(default=None, repr=False)

    _message_format: type["MessageFormat"] | None = field(default=None, repr=False)

    @property
    def message_format(self) -> type["MessageFormat"]:
        return self._message_format or self.outer_config.message_format

    @property
    @override
    def resolved_ack_policy(self) -> AckPolicy:
        if self.list_sub:
            return AckPolicy.MANUAL

        if self.channel_sub:
            return AckPolicy.MANUAL

        if self.stream_sub and (self.stream_sub.no_ack or not self.stream_sub.group):
            return AckPolicy.MANUAL

        if self.ack_policy is EMPTY:
            if self.outer_config.ack_policy is not EMPTY:
                return self.outer_config.ack_policy
            return AckPolicy.REJECT_ON_ERROR

        return self.ack_policy
