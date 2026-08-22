from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.config_value import Configurable
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

    # Held as declared, value object or Config placeholder alike: what a Redis
    # address means is settled by the read layer, never here.
    list_sub: Configurable[ListSub | str] | None = field(default=None, repr=False)
    channel_sub: Configurable[PubSub | str] | None = field(default=None, repr=False)
    stream_sub: Configurable[StreamSub | str] | None = field(default=None, repr=False)

    _message_format: type["MessageFormat"] | None = field(default=None, repr=False)

    @property
    def message_format(self) -> type["MessageFormat"]:
        return self._message_format or self._outer_config.message_format

    @property
    def ack_policy(self) -> AckPolicy:
        if self.list_sub:
            return AckPolicy.MANUAL

        if self.channel_sub:
            return AckPolicy.MANUAL

        # A stream declared as anything but a prepared `StreamSub` joins no
        # consumer group — which is also what a Config placeholder is built as,
        # since nothing about the value it stands for is known here.
        if stream := self.stream_sub:
            if not isinstance(stream, StreamSub):
                return AckPolicy.MANUAL

            if stream.no_ack or not stream.group:
                return AckPolicy.MANUAL

        if self._ack_policy is EMPTY:
            if self._outer_config.ack_policy is not EMPTY:
                return self._outer_config.ack_policy
            return AckPolicy.REJECT_ON_ERROR

        return self._ack_policy
