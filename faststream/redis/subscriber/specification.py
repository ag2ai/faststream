from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.redis.address import AddressRead
from faststream.redis.configs import RedisBrokerConfig
from faststream.redis.schemas import ListSub, PubSub, StreamSub
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import ChannelBinding, redis

from .config import RedisSubscriberSpecificationConfig

if TYPE_CHECKING:
    from faststream._internal.config_value import Configurable
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )


class RedisSubscriberSpecification(
    SubscriberSpecification[RedisBrokerConfig, RedisSubscriberSpecificationConfig],
):
    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        return {
            self.name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=None,
                ),
                bindings=ChannelBinding(
                    redis=self.channel_binding,
                ),
            ),
        }

    @property
    def channel_binding(self) -> redis.ChannelBinding:
        raise NotImplementedError


class ChannelSubscriberSpecification(RedisSubscriberSpecification):
    def __init__(
        self,
        _outer_config: "RedisBrokerConfig",
        specification_config: "RedisSubscriberSpecificationConfig",
        calls: "CallsCollection[Any]",
        channel: "Configurable[PubSub | str]",
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self._channel = self._derived.add(AddressRead(channel, PubSub))

    @property
    def channel(self) -> PubSub:
        """The channel this endpoint is documented under, built on first read."""
        return self._channel.read(self._outer_config)

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.channel_name}:{self.call_name}"

    @property
    def channel_name(self) -> str:
        return self.channel.name

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.channel_name,
            method="psubscribe" if self.channel.pattern else "subscribe",
        )


class ListSubscriberSpecification(RedisSubscriberSpecification):
    def __init__(
        self,
        _outer_config: "RedisBrokerConfig",
        specification_config: "RedisSubscriberSpecificationConfig",
        calls: "CallsCollection[Any]",
        list_sub: "Configurable[ListSub | str]",
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self._list_sub = self._derived.add(AddressRead(list_sub, ListSub))

    @property
    def list_sub(self) -> ListSub:
        """The list this endpoint is documented under, built on first read."""
        return self._list_sub.read(self._outer_config)

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.list_name}:{self.call_name}"

    @property
    def list_name(self) -> str:
        return self.list_sub.name

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.list_name,
            method="lpop",
        )


class StreamSubscriberSpecification(RedisSubscriberSpecification):
    def __init__(
        self,
        _outer_config: "RedisBrokerConfig",
        specification_config: "RedisSubscriberSpecificationConfig",
        calls: "CallsCollection[Any]",
        stream_sub: "Configurable[StreamSub | str]",
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self._stream_sub = self._derived.add(AddressRead(stream_sub, StreamSub))

    @property
    def stream_sub(self) -> StreamSub:
        """The stream this endpoint is documented under, built on first read."""
        return self._stream_sub.read(self._outer_config)

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.stream_name}:{self.call_name}"

    @property
    def stream_name(self) -> str:
        return self.stream_sub.name

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.stream_name,
            group_name=self.stream_sub.group,
            consumer_name=self.stream_sub.consumer,
            method="xreadgroup" if self.stream_sub.group else "xread",
        )
