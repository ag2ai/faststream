from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.redis.configs import RedisBrokerConfig
from faststream.redis.schemas import ListSub, PubSub, StreamSub
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import ChannelBinding, redis

from .config import RedisPublisherSpecificationConfig


class RedisPublisherSpecification(
    PublisherSpecification[RedisBrokerConfig, RedisPublisherSpecificationConfig],
):
    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()

        return {
            self.name: PublisherSpec(
                description=self.config.description_,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
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


class ChannelPublisherSpecification(RedisPublisherSpecification):
    def __init__(
        self,
        _outer_config: RedisBrokerConfig,
        specification_config: RedisPublisherSpecificationConfig,
        channel: PubSub,
    ) -> None:
        super().__init__(_outer_config, specification_config)
        self.channel = channel

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.channel_name}:Publisher"

    @property
    def channel_name(self) -> str:
        return f"{self._outer_config.prefix}{self.channel.name}"

    @property
    def channel_binding(self) -> redis.ChannelBinding:
        return redis.ChannelBinding(
            channel=self.channel_name,
            method="publish",
        )


class ListPublisherSpecification(RedisPublisherSpecification):
    def __init__(
        self,
        _outer_config: RedisBrokerConfig,
        specification_config: RedisPublisherSpecificationConfig,
        list_sub: ListSub,
    ) -> None:
        super().__init__(_outer_config, specification_config)
        self.list_sub = list_sub

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.list_name}:Publisher"

    @property
    def list_name(self) -> str:
        return f"{self._outer_config.prefix}{self.list_sub.name}"

    @property
    def channel_binding(self) -> redis.ChannelBinding:
        return redis.ChannelBinding(
            channel=self.list_name,
            method="rpush",
        )


class StreamPublisherSpecification(RedisPublisherSpecification):
    def __init__(
        self,
        _outer_config: RedisBrokerConfig,
        specification_config: RedisPublisherSpecificationConfig,
        stream_sub: StreamSub,
    ) -> None:
        super().__init__(_outer_config, specification_config)
        self.stream_sub = stream_sub

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.stream_name}:Publisher"

    @property
    def stream_name(self) -> str:
        return f"{self._outer_config.prefix}{self.stream_sub.name}"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.stream_name,
            method="xadd",
        )
