"""The one place a RabbitMQ endpoint's addresses are read.

A Subscriber's or a Publisher's queue, exchange and routing key are declared once
and read many times: when the endpoint starts, when it publishes, when the
AsyncAPI schema is generated, when a log line is written. Every one of those reads
goes through this module, so the Router prefix is composed at the point of use
rather than baked into the declaration.

The other five brokers compose their prefix in the same place — a property on the
endpoint reading through a shared point. RabbitMQ carries three addresses per
endpoint instead of one, so the point is spelled out here and the endpoints'
properties delegate to it.

The value objects are built here too, rather than at the declaration site. They own
their broker's address grammar — `RabbitQueue` compiles its routing key's Address
template, and validates queue type against durability — and none of that can happen
until the address is known in full, which is first true here (ADR-0004).

These functions produce a Broker address — the address an endpoint actually gives
to RabbitMQ. They perform no Config Resolution, which is a separate step under a
reserved name (`ConfigResolutionMixin.resolve_address`); it arrives behind these
same points, ahead of the `validate()` calls below.
"""

from typing import TYPE_CHECKING, TypeVar, Union

from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

if TYPE_CHECKING:
    from faststream.rabbit.configs import RabbitBrokerConfig

T = TypeVar("T")


def broker_queue(
    config: "RabbitBrokerConfig",
    queue: Union["RabbitQueue", str],
) -> "RabbitQueue":
    """Return the queue an endpoint declared, as it reaches the broker."""
    return RabbitQueue.validate(queue).add_prefix(config.prefix)


def broker_exchange(
    config: "RabbitBrokerConfig",
    exchange: Union["RabbitExchange", str],
) -> "RabbitExchange":
    """Return the exchange an endpoint declared, as it reaches the broker.

    An exchange lives outside the Router's namespace: the prefix decorates the
    queues and the routing keys binding them, never the exchange they bind to.
    """
    return RabbitExchange.validate(exchange)


def broker_routing_key(config: "RabbitBrokerConfig", routing_key: str) -> str:
    """Return the routing key an endpoint declared, as it reaches the broker.

    An empty routing key is not a routing key — it means the endpoint declared
    none and its queue names the binding instead — so there is nothing to
    decorate with the prefix.
    """
    if not routing_key:
        return routing_key

    return f"{config.prefix}{routing_key}"


def as_declared(config: "RabbitBrokerConfig", option: T) -> T:
    """Return an address as the endpoint declared it, undecorated.

    A few reads describe the declaration rather than address the broker — the
    queue a Subscriber names its log lines after, the address an AsyncAPI channel
    for a Publisher is titled with — and today those keep the name the user wrote,
    without the Router prefix. They are read points all the same, so that an
    address supplied from outside has one place to arrive at.
    """
    return option


def as_declared_queue(
    config: "RabbitBrokerConfig",
    queue: Union["RabbitQueue", str],
) -> "RabbitQueue":
    """Return the queue an endpoint declared, undecorated. See `as_declared`."""
    return RabbitQueue.validate(queue)
