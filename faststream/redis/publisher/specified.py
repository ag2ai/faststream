from typing import TYPE_CHECKING

from faststream._internal.publisher.specified import (
    SpecificationPublisher as SpecificationPublisherMixin,
)
from faststream.redis.publisher.usecase import (
    ChannelPublisher,
    ListBatchPublisher,
    ListPublisher,
    StreamPublisher,
)
from faststream.redis.schemas.proto import RedisSpecificationProtocol
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.specification.schema.bindings import ChannelBinding, redis

if TYPE_CHECKING:
    from faststream.redis.schemas import ListSub


class SpecificationPublisher(
    SpecificationPublisherMixin,
    RedisSpecificationProtocol[PublisherSpec],
):
    """A class to represent a Redis publisher."""

    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()

        return {
            self.name: PublisherSpec(
                description=self.description,
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


class SpecificationChannelPublisher(SpecificationPublisher, ChannelPublisher):
    def get_default_name(self) -> str:
        return f"{self.channel.name}:Publisher"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.channel.name,
            method="publish",
        )


class _ListPublisherMixin(SpecificationPublisher):
    list: "ListSub"

    def get_default_name(self) -> str:
        return f"{self.list.name}:Publisher"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.list.name,
            method="rpush",
        )


class SpecificationListPublisher(_ListPublisherMixin, ListPublisher):
    pass


class SpecificationListBatchPublisher(_ListPublisherMixin, ListBatchPublisher):
    pass


class SpecificationStreamPublisher(SpecificationPublisher, StreamPublisher):
    def get_default_name(self) -> str:
        return f"{self.stream.name}:Publisher"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.stream.name,
            method="xadd",
        )
