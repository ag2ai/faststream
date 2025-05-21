from typing import Optional, Union

from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream._internal.basic_types import AnyDict
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

    operationId: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None

    bindings: Optional[OperationBinding] = None

    message: Union[Message, Reference]

    security: Optional[dict[str, list[str]]] = None

    # TODO
    # traits

    tags: Optional[list[Union[Tag, AnyDict]]] = None

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
