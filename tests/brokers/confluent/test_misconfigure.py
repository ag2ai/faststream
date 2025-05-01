import pytest

from faststream.confluent import KafkaBroker
from faststream.confluent.router import KafkaRouter
from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.redis import RedisRouter


def test_max_workers_with_manual(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.raises(SetupError):
        broker.subscriber(queue, max_workers=3, auto_commit=False)


def test_use_only_confluent_router() -> None:
    broker = KafkaBroker()
    router = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [KafkaRouter(), NatsRouter(), RedisRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)
