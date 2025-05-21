"""AsyncAPI Kafka bindings.

References: https://github.com/asyncapi/bindings/tree/master/kafka
"""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ChannelBinding:
    """A class to represent a channel binding.

    Attributes:
        topic : optional string representing the topic
        partitions : optional positive integer representing the number of partitions
        replicas : optional positive integer representing the number of replicas
    """

    topic: Optional[str]
    partitions: Optional[int]
    replicas: Optional[int]

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

    group_id: Optional[dict[str, Any]]
    client_id: Optional[dict[str, Any]]
    reply_to: Optional[dict[str, Any]]
