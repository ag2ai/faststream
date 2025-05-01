import pytest
from aiokafka import TopicPartition

from faststream.exceptions import SetupError
from faststream.kafka import KafkaBroker, KafkaRouter
from faststream.nats import NatsRouter
from faststream.rabbit import RabbitRouter


def test_max_workers_with_manual_commit_with_multiple_queues() -> None:
    broker = KafkaBroker()

    with pytest.raises(SetupError):
        broker.subscriber(["queue1", "queue2"], max_workers=3, auto_commit=False)


def test_max_workers_with_manual_commit_with_pattern() -> None:
    broker = KafkaBroker()

    with pytest.raises(SetupError):
        broker.subscriber(pattern="pattern", max_workers=3, auto_commit=False)


def test_max_workers_with_manual_commit_partitions() -> None:
    broker = KafkaBroker()

    with pytest.raises(SetupError):
        broker.subscriber(
            partitions=[TopicPartition(topic="topic", partition=1)],
            max_workers=3,
            auto_commit=False,
        )


def test_use_only_kafka_router() -> None:
    broker = KafkaBroker()
    router = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [KafkaRouter(), NatsRouter(), RabbitRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)
