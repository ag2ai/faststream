"""AsyncAPI GCP Pub/Sub bindings.

References: https://github.com/asyncapi/bindings
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class ChannelBinding:
    """A class to represent GCP Pub/Sub channel binding.

    Attributes:
        topic : Pub/Sub topic name
        subscription : optional subscription name
        project_id : GCP project ID
    """

    topic: str
    subscription: str | None = None
    project_id: str | None = None


@dataclass
class OperationBinding:
    """A class to represent GCP Pub/Sub operation binding.

    Attributes:
        ack_deadline : optional acknowledgement deadline in seconds
        message_retention_duration : optional message retention duration
        ordering_key : optional ordering key for message ordering
        attributes : optional message attributes
    """

    ack_deadline: int | None = None
    message_retention_duration: str | None = None
    ordering_key: str | None = None
    attributes: dict[str, Any] | None = None
