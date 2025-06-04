from typing import Optional, Union

from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream._internal.basic_types import AnyDict
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
    description: Optional[str] = None

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

    title: Optional[str] = None
    name: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None
    messageId: Optional[str] = None
    correlationId: Optional[CorrelationId] = None
    contentType: Optional[str] = None

    payload: AnyDict
    # TODO:
    # headers
    # schemaFormat
    # bindings
    # examples
    # traits

    tags: Optional[list[Union[Tag, AnyDict]]] = None

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
