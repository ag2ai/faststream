"""AsyncAPI AMQP bindings.

References: https://github.com/asyncapi/bindings/tree/master/amqp
"""

from typing import Literal, overload

from pydantic import BaseModel, Field
from typing_extensions import Self

from faststream.specification.schema.bindings import amqp


class Queue(BaseModel):
    """A class to represent a queue.

    Attributes:
        name : name of the queue
        durable : indicates if the queue is durable
        exclusive : indicates if the queue is exclusive
        autoDelete : indicates if the queue should be automatically deleted
        vhost : virtual host of the queue (default is "/")
    """

    name: str
    durable: bool
    exclusive: bool
    autoDelete: bool
    vhost: str = "/"

    @overload
    @classmethod
    def from_spec(cls, binding: None, vhost: str) -> None: ...

    @overload
    @classmethod
    def from_spec(cls, binding: amqp.Queue, vhost: str) -> Self: ...

    @classmethod
    def from_spec(cls, binding: amqp.Queue | None, vhost: str) -> Self | None:
        if binding is None:
            return None

        return cls(
            name=binding.name,
            durable=binding.durable,
            exclusive=binding.exclusive,
            autoDelete=binding.auto_delete,
            vhost=vhost,
        )


class Exchange(BaseModel):
    """A class to represent an exchange.

    Attributes:
        name : name of the exchange (optional)
        type : type of the exchange, can be one of "default", "direct", "topic", "fanout", "headers"
        durable : whether the exchange is durable (optional)
        autoDelete : whether the exchange is automatically deleted (optional)
        vhost : virtual host of the exchange, default is "/"
    """

    name: str | None = None
    type: Literal[
        "default",
        "direct",
        "topic",
        "fanout",
        "headers",
        "x-delayed-message",
        "x-consistent-hash",
        "x-modulus-hash",
    ]
    durable: bool | None = None
    autoDelete: bool | None = None
    vhost: str = "/"

    @overload
    @classmethod
    def from_spec(cls, binding: None, vhost: str) -> None: ...

    @overload
    @classmethod
    def from_spec(cls, binding: amqp.Exchange, vhost: str) -> Self: ...

    @classmethod
    def from_spec(cls, binding: amqp.Exchange | None, vhost: str) -> Self | None:
        if binding is None:
            return None

        return cls(
            name=binding.name,
            type=binding.type,
            durable=binding.durable,
            autoDelete=binding.auto_delete,
            vhost=vhost,
        )


class ChannelBinding(BaseModel):
    """A class to represent channel binding.

    Attributes:
        is_ : Type of binding, can be "queue" or "routingKey"
        bindingVersion : Version of the binding
        queue : Optional queue object
        exchange : Optional exchange object
    """

    is_: Literal["queue", "routingKey"] = Field(..., alias="is")
    bindingVersion: str = "0.2.0"
    queue: Queue | None = None
    exchange: Exchange | None = None

    @classmethod
    def from_sub(cls, binding: amqp.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            **{
                "is": "routingKey",
                "queue": Queue.from_spec(binding.queue, binding.virtual_host)
                if binding.exchange.is_respect_routing_key
                else None,
                "exchange": Exchange.from_spec(binding.exchange, binding.virtual_host),
            },
        )

    @classmethod
    def from_pub(cls, binding: amqp.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            **{
                "is": "routingKey",
                "queue": Queue.from_spec(binding.queue, binding.virtual_host)
                if binding.exchange.is_respect_routing_key and binding.queue.name
                else None,
                "exchange": Exchange.from_spec(binding.exchange, binding.virtual_host),
            },
        )
