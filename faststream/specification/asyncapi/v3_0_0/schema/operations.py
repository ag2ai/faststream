from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, Field
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream._internal.basic_types import AnyDict
from faststream.specification.schema.operation import Operation as OperationSpec

from .bindings import OperationBinding
from .channels import Channel
from .tag import Tag
from .utils import Reference


class Action(str, Enum):
    SEND = "send"
    RECEIVE = "receive"


class Operation(BaseModel):
    """A class to represent an operation.

    Attributes:
        operation_id : ID of the operation
        summary : summary of the operation
        description : description of the operation
        bindings : bindings of the operation
        message : message of the operation
        security : security details of the operation
        tags : tags associated with the operation
    """

    action: Action
    channel: Union[Channel, Reference]

    summary: Optional[str] = None
    description: Optional[str] = None

    bindings: Optional[OperationBinding] = None

    messages: list[Reference] = Field(default_factory=list)

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
    def from_sub(
        cls,
        messages: list[Reference],
        channel: Reference,
        operation: OperationSpec,
    ) -> Self:
        return cls(
            action=Action.RECEIVE,
            messages=messages,
            channel=channel,
            bindings=OperationBinding.from_sub(operation.bindings),
            summary=None,
            description=None,
            security=None,
            tags=None,
        )

    @classmethod
    def from_pub(
        cls,
        messages: list[Reference],
        channel: Reference,
        operation: OperationSpec,
    ) -> Self:
        return cls(
            action=Action.SEND,
            messages=messages,
            channel=channel,
            bindings=OperationBinding.from_pub(operation.bindings),
            summary=None,
            description=None,
            security=None,
            tags=None,
        )
