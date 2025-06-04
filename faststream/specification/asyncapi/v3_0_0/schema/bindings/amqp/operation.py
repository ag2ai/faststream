"""AsyncAPI AMQP bindings.

References: https://github.com/asyncapi/bindings/tree/master/amqp
"""

from typing import Optional

from pydantic import BaseModel, PositiveInt
from typing_extensions import Self

from faststream.specification.schema.bindings import amqp


class OperationBinding(BaseModel):
    cc: Optional[list[str]] = None
    ack: bool
    replyTo: Optional[str] = None
    deliveryMode: Optional[int] = None
    mandatory: Optional[bool] = None
    priority: Optional[PositiveInt] = None

    bindingVersion: str = "0.3.0"

    @classmethod
    def from_sub(cls, binding: Optional[amqp.OperationBinding]) -> Optional[Self]:
        if not binding:
            return None

        return cls(
            cc=[binding.routing_key]
            if (binding.routing_key and binding.exchange.is_respect_routing_key)
            else None,
            ack=binding.ack,
            replyTo=binding.reply_to,
            deliveryMode=None if binding.persist is None else int(binding.persist) + 1,
            mandatory=binding.mandatory,
            priority=binding.priority,
        )

    @classmethod
    def from_pub(cls, binding: Optional[amqp.OperationBinding]) -> Optional[Self]:
        if not binding:
            return None

        return cls(
            cc=None
            if (not binding.routing_key or not binding.exchange.is_respect_routing_key)
            else [binding.routing_key],
            ack=binding.ack,
            replyTo=binding.reply_to,
            deliveryMode=None if binding.persist is None else int(binding.persist) + 1,
            mandatory=binding.mandatory,
            priority=binding.priority,
        )
