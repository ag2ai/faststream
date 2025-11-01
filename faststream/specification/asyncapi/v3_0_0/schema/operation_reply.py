from pydantic import BaseModel

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.asyncapi.v3_0_0.schema.message import Message

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
