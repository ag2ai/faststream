from typing import Any

from pydantic import BaseModel

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
        The model configuration allows extra attributes.

    """

    host: str
    pathname: str
    protocol: str
    description: str | None = None
    protocolVersion: str | None = None
    tags: list[Tag | dict[str, Any]] | None = None
    security: SecurityRequirement | None = None
    variables: dict[str, ServerVariable | Reference] | None = None

    model_config = {"extra": "allow"}
