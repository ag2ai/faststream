import pytest

from faststream.exceptions import SetupError
from faststream.kafka import KafkaRouter
from faststream.nats import NatsRouter
from faststream.nats.broker.broker import NatsBroker
from faststream.rabbit import RabbitRouter


def test_use_only_nats_router() -> None:
    broker = NatsBroker()
    router = RabbitRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [NatsRouter(), RabbitRouter(), KafkaRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)
