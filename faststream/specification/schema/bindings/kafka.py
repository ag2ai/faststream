"""AsyncAPI Kafka bindings.

References: https://github.com/asyncapi/bindings/tree/master/kafka
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class ChannelBinding:
    """A class to represent a channel binding.

    Attributes:
        topic : optional string representing the topic
        partitions : optional positive integer representing the number of partitions
        replicas : optional positive integer representing the number of replicas
    """

    topic: str | None
    partitions: int | None
    replicas: int | None

    # TODO:
    # topicConfiguration


@dataclass
class OperationBinding:
    """A class to represent an operation binding.

    Attributes:
        group_id : optional dictionary representing the group ID
        client_id : optional dictionary representing the client ID
        reply_to : optional dictionary representing the reply-to
    """

    group_id: dict[str, Any] | None
    client_id: dict[str, Any] | None
    reply_to: dict[str, Any] | None
