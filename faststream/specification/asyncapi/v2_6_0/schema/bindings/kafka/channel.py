"""AsyncAPI Kafka bindings.

References: https://github.com/asyncapi/bindings/tree/master/kafka
"""

from pydantic import BaseModel, PositiveInt
from typing_extensions import Self

from faststream.specification.schema.bindings import kafka


class ChannelBinding(BaseModel):
    """A class to represent a channel binding.

    Attributes:
        topic : optional string representing the topic
        partitions : optional positive integer representing the number of partitions
        replicas : optional positive integer representing the number of replicas
        bindingVersion : string representing the binding version
    """

    topic: str | None = None
    partitions: PositiveInt | None = None
    replicas: PositiveInt | None = None
    bindingVersion: str = "0.4.0"

    # TODO:
    # topicConfiguration

    @classmethod
    def from_sub(cls, binding: kafka.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            topic=binding.topic,
            partitions=binding.partitions,
            replicas=binding.replicas,
        )

    @classmethod
    def from_pub(cls, binding: kafka.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            topic=binding.topic,
            partitions=binding.partitions,
            replicas=binding.replicas,
        )
