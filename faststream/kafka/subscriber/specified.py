from collections.abc import Iterable
from itertools import chain
from typing import TYPE_CHECKING, Optional

from faststream._internal.subscriber.specified import (
    SpecificationSubscriber as SpecificationSubscriberMixin,
)
from faststream.kafka.subscriber.usecase import (
    BatchSubscriber,
    ConcurrentBetweenPartitionsSubscriber,
    ConcurrentDefaultSubscriber,
    DefaultSubscriber,
)
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import ChannelBinding, kafka

if TYPE_CHECKING:
    from aiokafka import TopicPartition


class SpecificationSubscriber(SpecificationSubscriberMixin):
    """A class to handle logic and async API operations."""

    topics: Iterable[str]
    partitions: Iterable["TopicPartition"]
    _pattern: Optional[str]  # TODO: support pattern schema

    def get_default_name(self) -> str:
        return f"{','.join(self.topics)}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        channels = {}

        payloads = self.get_payloads()

        for t in chain(self.topics, {p.topic for p in self.partitions}):
            handler_name = self.title_ or f"{t}:{self.call_name}"

            channels[handler_name] = SubscriberSpec(
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{handler_name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=None,
                ),
                bindings=ChannelBinding(
                    kafka=kafka.ChannelBinding(topic=t, partitions=None, replicas=None),
                ),
            )

        return channels


class SpecificationDefaultSubscriber(
    SpecificationSubscriber,
    DefaultSubscriber,
):
    pass


class SpecificationBatchSubscriber(
    SpecificationSubscriber,
    BatchSubscriber,
):
    pass


class SpecificationConcurrentDefaultSubscriber(
    SpecificationSubscriber,
    ConcurrentDefaultSubscriber,
):
    pass


class SpecificationConcurrentBetweenPartitionsSubscriber(
    SpecificationSubscriber,
    ConcurrentBetweenPartitionsSubscriber,
):
    pass
