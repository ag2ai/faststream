from typing import Any

from pydantic import BaseModel

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.asyncapi.v2_6_0.schema.tag import Tag
from faststream.specification.asyncapi.v2_6_0.schema.utils import Reference

SecurityRequirement = list[dict[str, list[str]]]


class ServerVariable(BaseModel):
    """A class to represent a server variable.

    Attributes:
        enum : list of possible values for the server variable (optional)
        default : default value for the server variable (optional)
        description : description of the server variable (optional)
        examples : list of example values for the server variable (optional)
    """

    enum: list[str] | None = None
    default: str | None = None
    description: str | None = None
    examples: list[str] | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"


class Server(BaseModel):
    """A class to represent a server.

    Attributes:
        url : URL of the server
        protocol : protocol used by the server
        description : optional description of the server
        protocolVersion : optional version of the protocol used by the server
        tags : optional list of tags associated with the server
        security : optional security requirement for the server
        variables : optional dictionary of server variables
        bindings : optional server binding

    Note:
        The attributes `description`, `protocolVersion`, `tags`, `security`, `variables`, and `bindings` are all optional.
    """

    url: str
    protocol: str
    protocolVersion: str | None
    description: str | None = None
    tags: list[Tag | dict[str, Any]] | None = None
    security: SecurityRequirement | None = None

    variables: dict[str, ServerVariable | Reference] | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"
