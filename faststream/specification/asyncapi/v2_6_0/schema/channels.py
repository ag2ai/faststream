from typing import Any

from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.schema import PublisherSpec, SubscriberSpec

from .bindings import ChannelBinding
from .operations import Operation
from .utils import Parameter


class Channel(BaseModel):
    """A class to represent a channel.

    Attributes:
        description : optional description of the channel
        servers : optional list of servers associated with the channel
        bindings : optional channel binding
        subscribe : optional operation for subscribing to the channel
        publish : optional operation for publishing to the channel

    Configurations:
        model_config : configuration for the model (only applicable for Pydantic version 2)
        Config : configuration for the class (only applicable for Pydantic version 1)
    """

    description: str | None = None
    servers: list[str] | None = None
    bindings: ChannelBinding | None = None
    subscribe: Operation | None = None
    publish: Operation | None = None

    parameters: dict[str, Parameter] | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"

    @classmethod
    def from_sub(
        cls,
        subscriber: SubscriberSpec,
        servers: list[str] | None = None,
        parameters: dict[str, dict[str, Any]] | None = None,
    ) -> Self:
        channel_parameters: dict[str, Parameter] | None = None
        if parameters:
            channel_parameters = {}
            for name, metadata in parameters.items():
                parameter: dict[str, Any] = {}
                if description := metadata.get("description"):
                    parameter["description"] = description

                schema = dict(metadata.get("schema", {}))
                if "default" in metadata:
                    schema.setdefault("default", metadata["default"])
                if schema:
                    parameter["schema"] = schema

                channel_parameters[name] = Parameter(**parameter)

        return cls(
            description=subscriber.description,
            servers=servers,
            bindings=ChannelBinding.from_sub(subscriber.bindings),
            subscribe=None,
            publish=Operation.from_sub(subscriber.operation),
            parameters=channel_parameters,
        )

    @classmethod
    def from_pub(cls, publisher: PublisherSpec, servers: list[str] | None = None) -> Self:
        return cls(
            description=publisher.description,
            servers=servers,
            bindings=ChannelBinding.from_pub(publisher.bindings),
            subscribe=Operation.from_pub(publisher.operation),
            publish=None,
        )
