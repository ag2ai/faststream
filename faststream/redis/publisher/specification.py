from typing import TYPE_CHECKING

from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.redis.address import AddressRead
from faststream.redis.configs import RedisBrokerConfig
from faststream.redis.schemas import ListSub, PubSub, StreamSub
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import ChannelBinding, redis

from .config import RedisPublisherSpecificationConfig

if TYPE_CHECKING:
    from faststream._internal.config_value import Configurable


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
        channel: "Configurable[PubSub | str]",
    ) -> None:
        super().__init__(_outer_config, specification_config)
        self._channel = AddressRead(channel, PubSub)

    @property
    def channel(self) -> PubSub:
        """The channel this Publisher is documented under, built on first read."""
        return self._channel.read(self._outer_config)

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.channel_name}:Publisher"

    @property
    def channel_name(self) -> str:
        return self.channel.name

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
        list_sub: "Configurable[ListSub | str]",
    ) -> None:
        super().__init__(_outer_config, specification_config)
        self._list_sub = AddressRead(list_sub, ListSub)

    @property
    def list_sub(self) -> ListSub:
        """The list this Publisher is documented under, built on first read."""
        return self._list_sub.read(self._outer_config)

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.list_name}:Publisher"

    @property
    def list_name(self) -> str:
        return self.list_sub.name

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
        stream_sub: "Configurable[StreamSub | str]",
    ) -> None:
        super().__init__(_outer_config, specification_config)
        self._stream_sub = AddressRead(stream_sub, StreamSub)

    @property
    def stream_sub(self) -> StreamSub:
        """The stream this Publisher is documented under, built on first read."""
        return self._stream_sub.read(self._outer_config)

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.stream_name}:Publisher"

    @property
    def stream_name(self) -> str:
        return self.stream_sub.name

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.stream_name,
            method="xadd",
        )
