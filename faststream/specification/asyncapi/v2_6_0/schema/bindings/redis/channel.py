"""AsyncAPI Redis bindings.

References: https://github.com/asyncapi/bindings/tree/master/redis
"""

from typing import Optional

from pydantic import BaseModel
from typing_extensions import Self

from faststream.specification.schema.bindings import redis


class ChannelBinding(BaseModel):
    """A class to represent channel binding.

    Attributes:
        channel : the channel name
        method : the method used for binding (ssubscribe, psubscribe, subscribe)
        bindingVersion : the version of the binding
    """

    channel: str
    method: Optional[str] = None
    groupName: Optional[str] = None
    consumerName: Optional[str] = None
    bindingVersion: str = "custom"

    @classmethod
    def from_sub(cls, binding: Optional[redis.ChannelBinding]) -> Optional[Self]:
        if binding is None:
            return None

        return cls(
            channel=binding.channel,
            method=binding.method,
            groupName=binding.group_name,
            consumerName=binding.consumer_name,
        )

    @classmethod
    def from_pub(cls, binding: Optional[redis.ChannelBinding]) -> Optional[Self]:
        if binding is None:
            return None

        return cls(
            channel=binding.channel,
            method=binding.method,
            groupName=binding.group_name,
            consumerName=binding.consumer_name,
        )
