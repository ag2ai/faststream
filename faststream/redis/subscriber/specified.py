from faststream._internal.subscriber.specified import (
    SpecificationSubscriber as SpecificationSubscriberMixin,
)
from faststream.redis.schemas import ListSub, StreamSub
from faststream.redis.schemas.proto import RedisSpecificationProtocol
from faststream.redis.subscriber.usecases.basic import ConcurrentSubscriber
from faststream.redis.subscriber.usecases.channel_subscriber import (
    ChannelSubscriber,
    ConcurrentChannelSubscriber,
)
from faststream.redis.subscriber.usecases.list_subscriber import (
    BatchListSubscriber,
    ConcurrentListSubscriber,
    ListSubscriber,
)
from faststream.redis.subscriber.usecases.stream_subscriber import (
    ConcurrentStreamSubscriber,
    StreamBatchSubscriber,
    StreamSubscriber,
)
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import ChannelBinding, redis


class SpecificationSubscriber(
    SpecificationSubscriberMixin, RedisSpecificationProtocol[SubscriberSpec]
):
    """A class to represent a Redis handler."""

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


class SpecificationChannelSubscriber(SpecificationSubscriber, ChannelSubscriber):
    def get_default_name(self) -> str:
        return f"{self.channel.name}:{self.call_name}"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.channel.name,
            method="psubscribe" if self.channel.pattern else "subscribe",
        )


class _StreamSubscriberMixin(SpecificationSubscriber):
    stream_sub: StreamSub

    def get_default_name(self) -> str:
        return f"{self.stream_sub.name}:{self.call_name}"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.stream_sub.name,
            group_name=self.stream_sub.group,
            consumer_name=self.stream_sub.consumer,
            method="xreadgroup" if self.stream_sub.group else "xread",
        )


class SpecificationStreamSubscriber(_StreamSubscriberMixin, StreamSubscriber):
    pass


class SpecificationStreamBatchSubscriber(_StreamSubscriberMixin, StreamBatchSubscriber):
    pass


class _ListSubscriberMixin(SpecificationSubscriber):
    list_sub: ListSub

    def get_default_name(self) -> str:
        return f"{self.list_sub.name}:{self.call_name}"

    @property
    def channel_binding(self) -> "redis.ChannelBinding":
        return redis.ChannelBinding(
            channel=self.list_sub.name,
            method="lpop",
        )


class SpecificationListSubscriber(_ListSubscriberMixin, ListSubscriber):
    pass


class SpecificationListBatchSubscriber(_ListSubscriberMixin, BatchListSubscriber):
    pass


class SpecificationConcurrentSubscriber(
    ConcurrentSubscriber, RedisSpecificationProtocol[SubscriberSpec]
):
    pass


class SpecificationStreamConcurrentSubscriber(
    ConcurrentStreamSubscriber, SpecificationStreamSubscriber
):
    pass


class SpecificationChannelConcurrentSubscriber(
    ConcurrentChannelSubscriber, SpecificationChannelSubscriber
):
    pass


class SpecificationListConcurrentSubscriber(
    ConcurrentListSubscriber, SpecificationListSubscriber
):
    pass
