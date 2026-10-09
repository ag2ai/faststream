from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from faststream._internal.utils.path import Address
    from faststream.rabbit.configs import RabbitBrokerConfig
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

T = TypeVar("T")


# Functions taking the config, not methods on it: at runtime `_outer_config` is a
# `ConfigComposition` whose `__getattr__` binds a method to one level's prefix only.
def broker_queue(config: "RabbitBrokerConfig", queue: "RabbitQueue") -> "RabbitQueue":
    """Return the queue as it reaches the broker, with the Router prefix."""
    return queue.add_prefix(config.prefix)


def broker_queue_routing_key(
    config: "RabbitBrokerConfig",
    queue: "RabbitQueue",
) -> str:
    """Return the compiled queue routing key with a literal Router prefix."""
    # Routing has always treated the prefix literally, unlike queue bindings (PR #3109).
    return f"{config.prefix}{queue.routing()}"


def broker_queue_routing_template(
    config: "RabbitBrokerConfig",
    queue: "RabbitQueue",
) -> str:
    """Return the queue routing template with a literal Router prefix."""
    return f"{config.prefix}{queue.routing_template()}"


def broker_exchange(
    config: "RabbitBrokerConfig",
    exchange: "RabbitExchange",
) -> "RabbitExchange":
    """Return the exchange as it reaches the broker; the Router prefix never applies."""
    return exchange


def broker_routing_key(config: "RabbitBrokerConfig", routing_address: "Address") -> str:
    """Return a Publisher's routing key as it reaches the broker, with the Router prefix."""
    routing_key = routing_address.template
    if not routing_key:
        # No key declared: the queue names the binding, and there is nothing to prefix.
        return routing_key

    return f"{config.prefix}{routing_key}"


def as_declared(config: "RabbitBrokerConfig", address: T) -> T:
    """Return an address as the endpoint declared it, without the Router prefix."""
    return address
