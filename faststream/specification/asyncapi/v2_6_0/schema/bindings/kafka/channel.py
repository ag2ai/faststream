"""AsyncAPI Kafka bindings.

References: https://github.com/asyncapi/bindings/tree/master/kafka
"""

from typing import Optional

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

    topic: Optional[str] = None
    partitions: Optional[PositiveInt] = None
    replicas: Optional[PositiveInt] = None
    bindingVersion: str = "0.4.0"

    # TODO:
    # topicConfiguration

    @classmethod
    def from_sub(cls, binding: Optional[kafka.ChannelBinding]) -> Optional[Self]:
        if binding is None:
            return None

        return cls(
            topic=binding.topic,
            partitions=binding.partitions,
            replicas=binding.replicas,
        )

    @classmethod
    def from_pub(cls, binding: Optional[kafka.ChannelBinding]) -> Optional[Self]:
        if binding is None:
            return None

        return cls(
            topic=binding.topic,
            partitions=binding.partitions,
            replicas=binding.replicas,
        )
