from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.redis.configs import RedisBrokerConfig
from faststream.redis.schemas import ListSub, PubSub, StreamSub
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import ChannelBinding, redis

from .config import RedisSubscriberSpecificationConfig

if TYPE_CHECKING:
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
                address=self.address,
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
    def channel_labels(self) -> list[str]:
        return [self.address]

    @property
    def address(self) -> str:
        raise NotImplementedError

    @property
    def channel_binding(self) -> redis.ChannelBinding:
        raise NotImplementedError


class ChannelSubscriberSpecification(RedisSubscriberSpecification):
    def __init__(
        self,
        _outer_config: "RedisBrokerConfig",
        specification_config: "RedisSubscriberSpecificationConfig",
        calls: "CallsCollection[Any]",
        channel: PubSub,
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self.channel = channel

    @property
    def address(self) -> str:
        # Through `PubSub`, the way the usecase does it: a prefix decorates the
        # declaration, so a `{{` of its own comes off with the rest.
        return self.channel.add_prefix(self._outer_config.prefix).address.template

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.address,
            method="psubscribe" if self.channel.pattern else "subscribe",
        )


class ListSubscriberSpecification(RedisSubscriberSpecification):
    def __init__(
        self,
        _outer_config: "RedisBrokerConfig",
        specification_config: "RedisSubscriberSpecificationConfig",
        calls: "CallsCollection[Any]",
        list_sub: ListSub,
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self.list_sub = list_sub

    @property
    def address(self) -> str:
        return f"{self._outer_config.prefix}{self.list_sub.name}"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.address,
            method="lpop",
        )


class StreamSubscriberSpecification(RedisSubscriberSpecification):
    def __init__(
        self,
        _outer_config: "RedisBrokerConfig",
        specification_config: "RedisSubscriberSpecificationConfig",
        calls: "CallsCollection[Any]",
        stream_sub: StreamSub,
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self.stream_sub = stream_sub

    @property
    def address(self) -> str:
        return f"{self._outer_config.prefix}{self.stream_sub.name}"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.address,
            group_name=self.stream_sub.group,
            consumer_name=self.stream_sub.consumer,
            method="xreadgroup" if self.stream_sub.group else "xread",
        )
