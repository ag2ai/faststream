from pydantic import BaseModel
from typing_extensions import Self

from faststream._internal._compat import PYDANTIC_V2
from faststream.specification.asyncapi.v3_0_0.schema.bindings import ChannelBinding
from faststream.specification.asyncapi.v3_0_0.schema.message import Message
from faststream.specification.schema import PublisherSpec, SubscriberSpec

from .utils import Reference


class Channel(BaseModel):
    """A class to represent a channel.

    Attributes:
        address: A string representation of this channel's address.
        description : optional description of the channel
        servers : optional list of servers associated with the channel
        bindings : optional channel binding
        parameters : optional parameters associated with the channel

    Configurations:
        model_config : configuration for the model (only applicable for Pydantic version 2)
        Config : configuration for the class (only applicable for Pydantic version 1)
    """

    address: str
    description: str | None = None
    servers: list[dict[str, str]] | None = None
    messages: dict[str, Message | Reference]
    bindings: ChannelBinding | None = None

    # TODO:
    # parameters: Optional[Parameter] = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"

    @classmethod
    def from_sub(cls, address: str, subscriber: SubscriberSpec) -> Self:
        message = subscriber.operation.message
        assert message.title

        *left, right = message.title.split(":")
        message.title = ":".join((*left, f"Subscribe{right}"))

        return cls(
            description=subscriber.description,
            address=address,
            messages={
                "SubscribeMessage": Message.from_spec(message),
            },
            bindings=ChannelBinding.from_sub(subscriber.bindings),
            servers=None,
        )

    @classmethod
    def from_pub(cls, address: str, publisher: PublisherSpec) -> Self:
        return cls(
            description=publisher.description,
            address=address,
            messages={
                "Message": Message.from_spec(publisher.operation.message),
            },
            bindings=ChannelBinding.from_pub(publisher.bindings),
            servers=None,
        )
