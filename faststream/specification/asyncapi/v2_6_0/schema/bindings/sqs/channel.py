"""AsyncAPI SQS bindings.

References: https://github.com/asyncapi/bindings/tree/master/sqs
"""

from pydantic import BaseModel
from typing_extensions import Self

from faststream.specification import schema as spec
from faststream.types import AnyDict


class ChannelBinding(BaseModel):
    """A class to represent channel binding.

    Attributes:
        queue : a dictionary representing the queue
        bindingVersion : a string representing the binding version (default: "custom")
    """

    queue: AnyDict
    bindingVersion: str = "custom"

    @classmethod
    def from_spec(cls, binding: spec.bindings.sqs.ChannelBinding) -> Self:
        return cls(
            queue=binding.queue,
            bindingVersion=binding.bindingVersion,
        )


def from_spec(binding: spec.bindings.sqs.ChannelBinding) -> ChannelBinding:
    return ChannelBinding.from_spec(binding)
