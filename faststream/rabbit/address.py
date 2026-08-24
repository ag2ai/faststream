"""The one place a RabbitMQ endpoint's addresses are resolved.

A Subscriber's or a Publisher's queue, exchange and routing key are declared once
and read many times: when the endpoint starts, when it publishes, when the
AsyncAPI schema is generated, when a log line is written. Resolution happens once
for all of them, at Preparation, and every read afterwards is a field access — so
the Router prefix is composed, and a Config value resolved, at the one moment the
composition is known to be final rather than baked into the declaration.

The other five brokers resolve in the same place — their endpoint's `_prepare`,
reading through their Broker config. RabbitMQ carries three addresses per endpoint
instead of one, so the resolution is spelled out here and both endpoints call into
it while they prepare. The Specifications call into it too, because schema
generation prepares its own Brokers.

The value objects are built here too, rather than at the declaration site. They own
their broker's address grammar — `RabbitQueue` compiles its routing key's Address
template, and validates queue type against durability — and none of that can happen
until the address is known in full, which is first true here (ADR-0004).

Resolution runs ahead of that: a Config placeholder is read out of the Config
values in scope, and what it resolves to — a name or a whole prepared object — is
then validated exactly as a literal declaration is. The Router prefix decorates
literal declarations only, so the two functions that prefix branch on how the
option was *declared*, never on what it resolved to (ADR-0003).
"""

from typing import TYPE_CHECKING, cast

from faststream._internal.config_value import Config
from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

if TYPE_CHECKING:
    from faststream._internal.config_value import Configurable
    from faststream.rabbit.configs import (
        ConfigurableExchange,
        ConfigurableQueue,
        RabbitBrokerConfig,
    )


def broker_queue(
    config: "RabbitBrokerConfig",
    queue: "ConfigurableQueue",
) -> "RabbitQueue":
    """Return the queue an endpoint declared, as it reaches the broker."""
    key = config.config_key(queue)

    # `add_prefix` copies, so what comes back is ours to stamp — and a resolved
    # value is used undecorated, prefix in scope or not.
    resolved = _resolved_queue(config, queue).add_prefix(
        "" if key is not None else config.prefix,
    )

    if key is not None:
        # A resolved address is otherwise indistinguishable from a literal one.
        # The key travels with it so that a template which cannot deliver its
        # `Path()` parameters can name the Config value to fix.
        resolved.routing_address.config_key = key

    return resolved


def broker_exchange(
    config: "RabbitBrokerConfig",
    exchange: "ConfigurableExchange",
) -> "RabbitExchange":
    """Return the exchange an endpoint declared, as it reaches the broker.

    An exchange lives outside the Router's namespace: the prefix decorates the
    queues and the routing keys binding them, never the exchange they bind to.
    """
    declared: RabbitExchange | str | None = config.resolve_option(exchange)
    return RabbitExchange.validate(declared)


def broker_routing_key(
    config: "RabbitBrokerConfig",
    routing_key: "Configurable[str]",
) -> str:
    """Return the routing key an endpoint declared, as it reaches the broker.

    An empty routing key is not a routing key — it means the endpoint declared
    none and its queue names the binding instead — so there is nothing to
    decorate with the prefix. A placeholder is never empty in that sense: it is
    a marker standing for a key, so it is resolved and, like every resolved
    value, reaches the broker undecorated (ADR-0003).
    """
    if not isinstance(routing_key, Config) and not routing_key:
        return routing_key

    return config.resolve_address(routing_key)


def broker_reply_to(
    config: "RabbitBrokerConfig",
    reply_to: "Configurable[str] | None",
) -> str:
    """Return the reply destination an endpoint declared, as it reaches the broker.

    Resolved but never prefixed, unlike every other address here: a literal
    `reply_to` has never been decorated with the Router prefix, and adopting a
    placeholder for it must not change that for the literal beside it.
    """
    return config.resolve_option(reply_to) or ""


def as_declared_queue(
    config: "RabbitBrokerConfig",
    queue: "ConfigurableQueue",
) -> "RabbitQueue":
    """Return the queue an endpoint declared, undecorated.

    One read describes the declaration rather than addresses the broker — the
    queue a Subscriber names its log lines after — and today it keeps the name
    the user wrote, without the Router prefix. Undecorated is not unresolved: a
    placeholder is a marker, never an address, so it is read here as it is
    everywhere else.
    """
    return _resolved_queue(config, queue)


def _resolved_queue(
    config: "RabbitBrokerConfig",
    queue: "ConfigurableQueue",
) -> "RabbitQueue":
    """Resolve a declared queue into the value object every read hands on.

    The `cast` states what `resolve_option` cannot infer out of this union: past
    this point a queue is a name or an object, never a placeholder.
    """
    return RabbitQueue.validate(cast("RabbitQueue | str", config.resolve_option(queue)))
