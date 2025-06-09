"""AsyncAPI SQS bindings.

References: https://github.com/asyncapi/bindings/tree/master/sqs
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class ChannelBinding:
    """A class to represent channel binding.

    Attributes:
        queue : a dictionary representing the queue
        bindingVersion : a string representing the binding version (default: "custom")
    """

    queue: dict[str, Any]
    bindingVersion: str = "custom"


@dataclass
class OperationBinding:
    """A class to represent an operation binding.

    Attributes:
        replyTo : optional dictionary containing reply information
        bindingVersion : version of the binding, default is "custom"
    """

    replyTo: dict[str, Any] | None = None
    bindingVersion: str = "custom"
