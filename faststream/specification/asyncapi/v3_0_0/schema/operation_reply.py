from pydantic import BaseModel, Field
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.asyncapi.v3_0_0.schema.message import Message
from faststream.specification.schema import Operation

from .utils import Reference


class OperationReplyAddress(BaseModel):
    description: str | None = None
    location: str

class OperationReply(BaseModel):
    messages: list[Message | Reference]
    channel: Reference | None = None
    address: OperationReplyAddress | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}
    else:
        class Config:
            extra = "allow"

    @classmethod
    def from_sub(cls, messages: list[Reference] ) -> Self:
        return cls(
            messages=messages
        )