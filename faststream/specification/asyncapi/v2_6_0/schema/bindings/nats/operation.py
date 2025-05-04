"""AsyncAPI NATS bindings.

References: https://github.com/asyncapi/bindings/tree/master/nats
"""

from typing import Optional

from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal.basic_types import AnyDict
from faststream.specification.schema.bindings import nats


class OperationBinding(BaseModel):
    """A class to represent an operation binding.

    Attributes:
        replyTo : optional dictionary containing reply information
        bindingVersion : version of the binding (default is "custom")
    """

    replyTo: Optional[AnyDict] = None
    bindingVersion: str = "custom"

    @classmethod
    def from_sub(cls, binding: Optional[nats.OperationBinding]) -> Optional[Self]:
        if not binding:
            return None

        return cls(
            replyTo=binding.reply_to,
        )

    @classmethod
    def from_pub(cls, binding: Optional[nats.OperationBinding]) -> Optional[Self]:
        if not binding:
            return None

        return cls(
            replyTo=binding.reply_to,
        )
