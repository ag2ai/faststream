from typing_extensions import Self

from faststream.specification.asyncapi.v2_6_0.schema.bindings.amqp import (
    ChannelBinding as V2Binding,
)
from faststream.specification.asyncapi.v2_6_0.schema.bindings.amqp.channel import (
    Exchange,
    Queue,
)
from faststream.specification.schema.bindings import amqp


class ChannelBinding(V2Binding):
    bindingVersion: str = "0.3.0"

    @classmethod
    def from_sub(cls, binding: amqp.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            **{
                "is": "queue",
                "queue": Queue.from_spec(binding.queue, binding.virtual_host),
            },
        )

    @classmethod
    def from_pub(cls, binding: amqp.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            **{
                "is": "routingKey",
                "exchange": Exchange.from_spec(binding.exchange, binding.virtual_host),
            },
        )
