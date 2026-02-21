from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase

# (BrokerClass, TestBrokerClass, publish_param_name). Confluent before Kafka.
_BROKER_REGISTRY: list[tuple[type[Any], type[Any], str]] = []


def _get_broker_registry() -> list[tuple[type[Any], type[Any], str]]:
    """Lazy registry: (BrokerClass, TestBrokerClass, publish_param_name)."""
    if _BROKER_REGISTRY:
        return _BROKER_REGISTRY
    from faststream.confluent.broker.broker import KafkaBroker as ConfluentKafkaBroker
    from faststream.confluent.testing import TestKafkaBroker as TestConfluentKafkaBroker
    from faststream.kafka.broker.broker import KafkaBroker as AioKafkaBroker
    from faststream.kafka.testing import TestKafkaBroker as TestAioKafkaBroker
    from faststream.nats.broker.broker import NatsBroker
    from faststream.nats.testing import TestNatsBroker
    from faststream.rabbit.broker.broker import RabbitBroker
    from faststream.rabbit.testing import TestRabbitBroker
    from faststream.redis.broker.broker import RedisBroker
    from faststream.redis.testing import TestRedisBroker

    _BROKER_REGISTRY.extend([
        (ConfluentKafkaBroker, TestConfluentKafkaBroker, "topic"),
        (AioKafkaBroker, TestAioKafkaBroker, "topic"),
        (RabbitBroker, TestRabbitBroker, "routing_key"),
        (NatsBroker, TestNatsBroker, "subject"),
        (RedisBroker, TestRedisBroker, "channel"),
    ])
    return _BROKER_REGISTRY


def get_publish_param_name(broker: "BrokerUsecase[Any, Any]") -> str:
    """Get the publish parameter name for the broker (topic, subject, etc.)."""
    for broker_cls, _, param in _get_broker_registry():
        if isinstance(broker, broker_cls):
            return param
    broker_name = type(broker).__name__
    msg = (
        f"Unsupported broker type for try-it-out: {broker_name}. "
        "Supported: Kafka, Confluent Kafka, RabbitMQ, NATS, Redis."
    )
    raise NotImplementedError(msg)


def _get_test_broker_for_broker(broker: "BrokerUsecase[Any, Any]") -> type[Any] | None:
    """Get the TestBroker class for the given broker instance."""
    for broker_cls, test_cls, _ in _get_broker_registry():
        if isinstance(broker, broker_cls):
            return test_cls
    return None


@contextmanager
def test_broker_publish_context(broker: "BrokerUsecase[Any, Any]") -> Iterator[None]:
    """Context manager that patches broker to use FakeProducer for in-memory publish."""
    test_broker_cls = _get_test_broker_for_broker(broker)
    if test_broker_cls is None:
        broker_name = type(broker).__name__
        msg = (
            f"TestBroker not available for {broker_name}. "
            "Supported: Kafka, Confluent Kafka, RabbitMQ, NATS, Redis."
        )
        raise ValueError(msg)
    test_broker = test_broker_cls(broker)
    with test_broker._patch_producer(broker):
        yield
