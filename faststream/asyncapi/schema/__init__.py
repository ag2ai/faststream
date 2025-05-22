"""AsyncAPI schema related functions."""

from faststream.asyncapi.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    ServerBinding,
)
from faststream.asyncapi.schema.channels import Channel
from faststream.asyncapi.schema.info import (
    Contact,
    ContactDict,
    Info,
    License,
    LicenseDict,
)
from faststream.asyncapi.schema.main import ASYNC_API_VERSION, Components, Schema
from faststream.asyncapi.schema.message import CorrelationId, Message
from faststream.asyncapi.schema.operations import Operation
from faststream.asyncapi.schema.security import SecuritySchemaComponent
from faststream.asyncapi.schema.servers import Server
from faststream.asyncapi.schema.utils import (
    ExternalDocs,
    ExternalDocsDict,
    Reference,
    Tag,
    TagDict,
)

__all__ = (
    # main
    "ASYNC_API_VERSION",
    # channels
    "Channel",
    "ChannelBinding",
    "Components",
    "Contact",
    "ContactDict",
    "CorrelationId",
    "ExternalDocs",
    "ExternalDocsDict",
    # info
    "Info",
    "License",
    "LicenseDict",
    # messages
    "Message",
    # subscription
    "Operation",
    "OperationBinding",
    "Reference",
    "Schema",
    # security
    "SecuritySchemaComponent",
    # servers
    "Server",
    # bindings
    "ServerBinding",
    # utils
    "Tag",
    "TagDict",
)
