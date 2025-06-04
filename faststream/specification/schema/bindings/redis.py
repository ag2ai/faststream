"""AsyncAPI Redis bindings.

References: https://github.com/asyncapi/bindings/tree/master/redis
"""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ChannelBinding:
    """A class to represent channel binding.

    Attributes:
        channel : the channel name
        method : the method used for binding (ssubscribe, psubscribe, subscribe)
    """

    channel: str
    method: Optional[str] = None
    group_name: Optional[str] = None
    consumer_name: Optional[str] = None


@dataclass
class OperationBinding:
    """A class to represent an operation binding.

    Attributes:
        reply_to : optional dictionary containing reply information
    """

    reply_to: Optional[dict[str, Any]] = None
