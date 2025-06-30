"""AsyncAPI AMQP bindings.

References: https://github.com/asyncapi/bindings/tree/master/amqp
"""

from pydantic import BaseModel, PositiveInt
from typing_extensions import Self

from faststream.specification.schema.bindings import amqp


class OperationBinding(BaseModel):
    """A class to represent an operation binding.

    Attributes:
        cc : optional string representing the cc
        ack : boolean indicating if the operation is acknowledged
        replyTo : optional dictionary representing the replyTo
        bindingVersion : string representing the binding version
    """

    cc: str | None = None
    ack: bool
    replyTo: str | None = None
    deliveryMode: int | None = None
    mandatory: bool | None = None
    priority: PositiveInt | None = None

    bindingVersion: str = "0.2.0"

    @classmethod
    def from_sub(cls, binding: amqp.OperationBinding | None) -> Self | None:
        if not binding:
            return None

        return cls(
            cc=binding.routing_key if binding.exchange.is_respect_routing_key else None,
            ack=binding.ack,
            replyTo=binding.reply_to,
            deliveryMode=None if binding.persist is None else int(binding.persist) + 1,
            mandatory=binding.mandatory,
            priority=binding.priority,
        )

    @classmethod
    def from_pub(cls, binding: amqp.OperationBinding | None) -> Self | None:
        if not binding:
            return None

        return cls(
            cc=binding.routing_key if binding.exchange.is_respect_routing_key else None,
            ack=binding.ack,
            replyTo=binding.reply_to,
            deliveryMode=None if binding.persist is None else int(binding.persist) + 1,
            mandatory=binding.mandatory,
            priority=binding.priority,
        )
