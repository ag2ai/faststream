"""AsyncAPI SQS bindings.

References: https://github.com/asyncapi/bindings/tree/master/sqs
"""

from typing import Any

from pydantic import BaseModel
from typing_extensions import Self

from faststream.specification.schema.bindings import sqs


class OperationBinding(BaseModel):
    """A class to represent an operation binding.

    Attributes:
        replyTo : optional dictionary containing reply information
        bindingVersion : version of the binding, default is "custom"
    """

    replyTo: dict[str, Any] | None = None
    bindingVersion: str = "custom"

    @classmethod
    def from_pub(cls, binding: sqs.OperationBinding) -> Self:
        return cls(
            replyTo=binding.replyTo,
            bindingVersion=binding.bindingVersion,
        )

    @classmethod
    def from_sub(cls, binding: sqs.OperationBinding) -> Self:
        return cls(
            replyTo=binding.replyTo,
            bindingVersion=binding.bindingVersion,
        )
