from pydantic import BaseModel, Field
from .utils import Reference


from faststream._internal._compat import PYDANTIC_V2


class OperationReplyAddress(BaseModel):
    description: str | None = None
    location: str

class OperationReply(BaseModel):
    address: OperationReplyAddress
    channel: Reference
    messages: list[Reference] = Field(default_factory=list)

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}
    else:
        class Config:
            extra = "allow"