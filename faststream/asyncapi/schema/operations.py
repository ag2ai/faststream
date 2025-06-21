from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel

from faststream._compat import PYDANTIC_V2
from faststream.asyncapi.schema.bindings import OperationBinding
from faststream.asyncapi.schema.message import Message
from faststream.asyncapi.schema.utils import (
    ExternalDocs,
    ExternalDocsDict,
    Reference,
    Tag,
    TagDict,
)


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
        externalDocs : external documentation for the operation

    """

    operationId: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None

    bindings: Optional[OperationBinding] = None

    message: Union[Message, Reference, None] = None

    security: Optional[Dict[str, List[str]]] = None

    # TODO
    # traits

    tags: Optional[List[Union[Tag, TagDict, Dict[str, Any]]]] = None
    externalDocs: Optional[Union[ExternalDocs, ExternalDocsDict, Dict[str, Any]]] = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"
