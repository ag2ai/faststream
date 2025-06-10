"""AsyncAPI Redis bindings.

References: https://github.com/asyncapi/bindings/tree/master/redis
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class ChannelBinding:
    """A class to represent channel binding.

    Attributes:
        channel : the channel name
        method : the method used for binding (ssubscribe, psubscribe, subscribe)
    """

    channel: str
    method: str | None = None
    group_name: str | None = None
    consumer_name: str | None = None


@dataclass
class OperationBinding:
    """A class to represent an operation binding.

    Attributes:
        reply_to : optional dictionary containing reply information
    """

    reply_to: dict[str, Any] | None = None
