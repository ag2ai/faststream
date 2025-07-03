"""AsyncAPI Redis bindings.

References: https://github.com/asyncapi/bindings/tree/master/redis
"""

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
    method: str | None = None
    groupName: str | None = None
    consumerName: str | None = None
    bindingVersion: str = "custom"

    @classmethod
    def from_sub(cls, binding: redis.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            channel=binding.channel,
            method=binding.method,
            groupName=binding.group_name,
            consumerName=binding.consumer_name,
        )

    @classmethod
    def from_pub(cls, binding: redis.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            channel=binding.channel,
            method=binding.method,
            groupName=binding.group_name,
            consumerName=binding.consumer_name,
        )
