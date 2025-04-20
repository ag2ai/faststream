import pytest

from faststream.exceptions import SetupError
from faststream.kafka import KafkaRouter
from faststream.nats import NatsRouter
from faststream.rabbit import RabbitBroker, RabbitRouter


def test_use_only_rabbit_router() -> None:
    broker = RabbitBroker()
    router = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [RabbitRouter(), NatsRouter(), KafkaRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)
