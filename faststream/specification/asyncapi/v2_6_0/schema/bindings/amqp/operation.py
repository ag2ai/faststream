"""AsyncAPI AMQP bindings.

References: https://github.com/asyncapi/bindings/tree/master/amqp
"""

from typing import Optional, overload, ClassVar

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

    cc: Optional[str]
    ack: bool
    replyTo: Optional[str]
    deliveryMode: Optional[int]
    mandatory: Optional[bool]
    priority: Optional[PositiveInt]
    bindingVersion: str

    delivery_mode_spec_to_asyncapi: ClassVar[dict] = {
        "persistent": 2,
        "transient": 1,
        None: None,
    }

    @overload
    @classmethod
    def from_sub(cls, binding: None) -> None: ...

    @overload
    @classmethod
    def from_sub(cls, binding: amqp.OperationBinding) -> Self: ...

    @classmethod
    def from_sub(cls, binding: Optional[amqp.OperationBinding]) -> Optional[Self]:
        if binding is None:
            return None

        return cls(
            cc=binding.routing_key,
            ack=binding.ack,
            replyTo=binding.reply_to,
            deliveryMode=cls.delivery_mode_spec_to_asyncapi[binding.delivery_mode],
            mandatory=binding.mandatory,
            priority=binding.priority,
            bindingVersion="0.2.0",
        )

    @overload
    @classmethod
    def from_pub(cls, binding: None) -> None: ...

    @overload
    @classmethod
    def from_pub(cls, binding: amqp.OperationBinding) -> Self: ...

    @classmethod
    def from_pub(cls, binding: Optional[amqp.OperationBinding]) -> Optional[Self]:
        if binding is None:
            return None

        return cls(
            cc=binding.routing_key,
            ack=binding.ack,
            replyTo=binding.reply_to,
            deliveryMode=cls.delivery_mode_spec_to_asyncapi[binding.delivery_mode],
            mandatory=binding.mandatory,
            priority=binding.priority,
            bindingVersion="0.2.0",
        )
