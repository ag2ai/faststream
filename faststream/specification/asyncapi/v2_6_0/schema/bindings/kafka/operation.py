"""AsyncAPI Kafka bindings.

References: https://github.com/asyncapi/bindings/tree/master/kafka
"""

from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal.basic_types import AnyDict
from faststream.specification.schema.bindings import kafka


class OperationBinding(BaseModel):
    """A class to represent an operation binding.

    Attributes:
        groupId : optional dictionary representing the group ID
        clientId : optional dictionary representing the client ID
        replyTo : optional dictionary representing the reply-to
        bindingVersion : version of the binding (default: "0.4.0")
    """

    groupId: AnyDict | None = None
    clientId: AnyDict | None = None
    replyTo: AnyDict | None = None
    bindingVersion: str = "0.4.0"

    @classmethod
    def from_sub(cls, binding: kafka.OperationBinding | None) -> Self | None:
        if not binding:
            return None

        return cls(
            groupId=binding.group_id,
            clientId=binding.client_id,
            replyTo=binding.reply_to,
        )

    @classmethod
    def from_pub(cls, binding: kafka.OperationBinding | None) -> Self | None:
        if not binding:
            return None

        return cls(
            groupId=binding.group_id,
            clientId=binding.client_id,
            replyTo=binding.reply_to,
        )
