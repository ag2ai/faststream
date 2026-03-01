from typing import Any

from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.asyncapi.v2_6_0.schema.tag import Tag
from faststream.specification.schema.message import Message as SpecMessage


class CorrelationId(BaseModel):
    """A class to represent a correlation ID.

    Attributes:
        description : optional description of the correlation ID
        location : location of the correlation ID

    Configurations:
        extra : allows extra fields in the correlation ID model
    """

    location: str
    description: str | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"


class Message(BaseModel):
    """A class to represent a message.

    Attributes:
        title : title of the message
        name : name of the message
        summary : summary of the message
        description : description of the message
        messageId : ID of the message
        correlationId : correlation ID of the message
        contentType : content type of the message
        payload : dictionary representing the payload of the message
        tags : list of tags associated with the message
    """

    title: str | None = None
    name: str | None = None
    summary: str | None = None
    description: str | None = None
    messageId: str | None = None
    correlationId: CorrelationId | None = None
    contentType: str | None = None

    payload: dict[str, Any]
    # TODO:
    # headers
    # schemaFormat
    # bindings
    # examples
    # traits

    tags: list[Tag | dict[str, Any]] | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"

    @classmethod
    def from_spec(cls, message: SpecMessage) -> Self:
        return cls(
            title=message.title,
            payload=message.payload,
            correlationId=CorrelationId(
                description=None,
                location="$message.header#/correlation_id",
            ),
            name=None,
            summary=None,
            description=None,
            messageId=None,
            contentType=None,
            tags=None,
        )
