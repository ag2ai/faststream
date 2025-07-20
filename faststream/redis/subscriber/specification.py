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
        channel: PubSub,
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self.channel = channel

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.channel_name}:{self.call_name}"

    @property
    def channel_name(self) -> str:
        return f"{self._outer_config.prefix}{self.channel.name}"

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
        list_sub: ListSub,
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self.list_sub = list_sub

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.list_name}:{self.call_name}"

    @property
    def list_name(self) -> str:
        return f"{self._outer_config.prefix}{self.list_sub.name}"

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
        stream_sub: StreamSub,
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self.stream_sub = stream_sub

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.stream_name}:{self.call_name}"

    @property
    def stream_name(self) -> str:
        return f"{self._outer_config.prefix}{self.stream_sub.name}"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.stream_name,
            group_name=self.stream_sub.group,
            consumer_name=self.stream_sub.consumer,
            method="xreadgroup" if self.stream_sub.group else "xread",
        )
