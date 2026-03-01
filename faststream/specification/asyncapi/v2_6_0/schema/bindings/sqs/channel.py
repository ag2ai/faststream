"""AsyncAPI SQS bindings.

References: https://github.com/asyncapi/bindings/tree/master/sqs
"""

from typing import Any

from pydantic import BaseModel
from typing_extensions import Self

from faststream.specification.schema.bindings import sqs


class ChannelBinding(BaseModel):
    """A class to represent channel binding.

    Attributes:
        queue : a dictionary representing the queue
        bindingVersion : a string representing the binding version (default: "custom")
    """

    queue: dict[str, Any]
    bindingVersion: str = "custom"

    @classmethod
    def from_pub(cls, binding: sqs.ChannelBinding) -> Self:
        return cls(
            queue=binding.queue,
            bindingVersion=binding.bindingVersion,
        )

    @classmethod
    def from_sub(cls, binding: sqs.ChannelBinding) -> Self:
        return cls(
            queue=binding.queue,
            bindingVersion=binding.bindingVersion,
        )
