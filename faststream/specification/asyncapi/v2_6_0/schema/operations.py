from typing import Any

from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.schema.operation import Operation as OperationSpec

from .bindings import OperationBinding
from .message import Message
from .tag import Tag
from .utils import Reference


class Operation(BaseModel):
    """A class to represent an operation.

    Attributes:
        operationId : ID of the operation
        summary : summary of the operation
        description : description of the operation
        bindings : bindings of the operation
        message : message of the operation
        security : security details of the operation
        tags : tags associated with the operation
    """

    operationId: str | None = None
    summary: str | None = None
    description: str | None = None

    bindings: OperationBinding | None = None

    message: Message | Reference | None

    security: dict[str, list[str]] | None = None

    # TODO
    # traits

    tags: list[Tag | dict[str, Any]] | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"

    @classmethod
    def from_sub(cls, operation: OperationSpec) -> Self:
        return cls(
            message=Message.from_spec(operation.message),
            bindings=OperationBinding.from_sub(operation.bindings),
            operationId=None,
            summary=None,
            description=None,
            tags=None,
            security=None,
        )

    @classmethod
    def from_pub(cls, operation: OperationSpec) -> Self:
        return cls(
            message=Message.from_spec(operation.message),
            bindings=OperationBinding.from_pub(operation.bindings),
            operationId=None,
            summary=None,
            description=None,
            tags=None,
            security=None,
        )
