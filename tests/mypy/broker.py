"""Every broker must be accepted where the base ``BrokerUsecase`` is annotated.

Integrations (dishka, custom middleware, app wrappers) type their entry points
as ``BrokerUsecase[Any, Any]``. The third type parameter defaults to
``BrokerConfig``, so it must be covariant: each broker binds it to its own
config subclass, and an invariant parameter would reject every concrete broker.
"""

from typing import Any

from faststream._internal.broker import BrokerUsecase
from faststream.confluent import KafkaBroker as ConfluentBroker
from faststream.kafka import KafkaBroker
from faststream.mqtt import MQTTBroker
from faststream.nats import NatsBroker
from faststream.rabbit import RabbitBroker
from faststream.redis import RedisBroker


def setup_integration(broker: BrokerUsecase[Any, Any] | None = None) -> None: ...


def setup_any_integration(broker: BrokerUsecase[Any, Any, Any]) -> None: ...


def check_brokers_are_base_usecases() -> None:
    setup_integration(RabbitBroker())
    setup_integration(KafkaBroker())
    setup_integration(ConfluentBroker())
    setup_integration(NatsBroker())
    setup_integration(RedisBroker())
    setup_integration(MQTTBroker())

    setup_any_integration(RabbitBroker())
    setup_any_integration(KafkaBroker())
    setup_any_integration(ConfluentBroker())
    setup_any_integration(NatsBroker())
    setup_any_integration(RedisBroker())
    setup_any_integration(MQTTBroker())
