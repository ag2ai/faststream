from typing import Any

from pydantic import BaseModel

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.asyncapi.v2_6_0.schema import ServerVariable, Tag
from faststream.specification.asyncapi.v2_6_0.schema.utils import Reference

SecurityRequirement = list[Reference]


__all__ = (
    "Server",
    "ServerVariable",
)


class Server(BaseModel):
    """A class to represent a server.

    Attributes:
        host : host of the server
        pathname : pathname of the server
        protocol : protocol used by the server
        description : optional description of the server
        protocolVersion : optional version of the protocol used by the server
        tags : optional list of tags associated with the server
        security : optional security requirement for the server
        variables : optional dictionary of server variables

    Note:
        The attributes `description`, `protocolVersion`, `tags`, `security`, `variables`, and `bindings` are all optional.

    Configurations:
        If `PYDANTIC_V2` is True, the model configuration is set to allow extra attributes.
        Otherwise, the `Config` class is defined with the `extra` attribute set to "allow".

    """

    host: str
    pathname: str
    protocol: str
    description: str | None = None
    protocolVersion: str | None = None
    tags: list[Tag | dict[str, Any]] | None = None
    security: SecurityRequirement | None = None
    variables: dict[str, ServerVariable | Reference] | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"
