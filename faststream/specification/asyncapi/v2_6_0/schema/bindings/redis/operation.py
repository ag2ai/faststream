"""AsyncAPI Redis bindings.

References: https://github.com/asyncapi/bindings/tree/master/redis
"""

from typing import Any

from pydantic import BaseModel
from typing_extensions import Self

from faststream.specification.schema.bindings import redis


class OperationBinding(BaseModel):
    """A class to represent an operation binding.

    Attributes:
        replyTo : optional dictionary containing reply information
        bindingVersion : version of the binding (default is "custom")
    """

    replyTo: dict[str, Any] | None = None
    bindingVersion: str = "custom"

    @classmethod
    def from_sub(cls, binding: redis.OperationBinding | None) -> Self | None:
        if not binding:
            return None

        return cls(
            replyTo=binding.reply_to,
        )

    @classmethod
    def from_pub(cls, binding: redis.OperationBinding | None) -> Self | None:
        if not binding:
            return None

        return cls(
            replyTo=binding.reply_to,
        )
