from typing import Any, Literal

from faststream.specification.asyncapi.v2_6_0.schema.channels import Channel
from faststream.specification.asyncapi.v2_6_0.schema.components import Components
from faststream.specification.asyncapi.v2_6_0.schema.docs import ExternalDocs
from faststream.specification.asyncapi.v2_6_0.schema.info import ApplicationInfo
from faststream.specification.asyncapi.v2_6_0.schema.servers import Server
from faststream.specification.asyncapi.v2_6_0.schema.tag import Tag
from faststream.specification.base.schema import BaseApplicationSchema


class ApplicationSchema(BaseApplicationSchema):
    """A class to represent an application schema.

    Attributes:
        asyncapi : version of the async API
        id : optional ID
        defaultContentType : optional default content type
        info : information about the schema
        servers : optional dictionary of servers
        channels : dictionary of channels
        components : optional components of the schema
        tags : optional list of tags
        externalDocs : optional external documentation
    """

    info: ApplicationInfo

    asyncapi: Literal["2.6.0"] | str
    id: str | None = None
    defaultContentType: str | None = None
    servers: dict[str, Server] | None = None
    channels: dict[str, Channel]
    components: Components | None = None
    tags: list[Tag | dict[str, Any]] | None = None
    externalDocs: ExternalDocs | dict[str, Any] | None = None
