from typing import Literal

from pydantic import Field

from faststream.specification.asyncapi.v3_0_0.schema.channels import Channel
from faststream.specification.asyncapi.v3_0_0.schema.components import Components
from faststream.specification.asyncapi.v3_0_0.schema.info import ApplicationInfo
from faststream.specification.asyncapi.v3_0_0.schema.operations import Operation
from faststream.specification.asyncapi.v3_0_0.schema.servers import Server
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
    """

    info: ApplicationInfo

    asyncapi: Literal["3.0.0"] | str = "3.0.0"
    id: str | None = None
    defaultContentType: str | None = None
    servers: dict[str, Server] | None = None
    channels: dict[str, Channel] = Field(default_factory=dict)
    operations: dict[str, Operation] = Field(default_factory=dict)
    components: Components | None = None
