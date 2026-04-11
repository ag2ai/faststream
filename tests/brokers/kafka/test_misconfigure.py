from typing import Any

import pytest

from faststream import AckPolicy
from faststream.exceptions import SetupError
from faststream.kafka import KafkaBroker, KafkaRouter, TopicPartition
from faststream.kafka.subscriber.usecase import (
    ConcurrentBetweenPartitionsSubscriber,
    ConcurrentDefaultSubscriber,
)
from faststream.nats import NatsRouter
from faststream.rabbit import RabbitRouter


@pytest.mark.kafka()
@pytest.mark.parametrize(
    ("args", "kwargs"),
    (
        pytest.param(
            (),
            {},
            id="no destination",
        ),
        pytest.param(
            ("topic",),
            {"partitions": [TopicPartition("topic", 1)]},
            id="topic and partitions",
        ),
        pytest.param(
            ("topic",),
            {"pattern": ".*"},
            id="topic and pattern",
        ),
        pytest.param(
            (),
            {
                "partitions": [TopicPartition("topic", 1)],
                "pattern": ".*",
            },
            id="partitions and pattern",
        ),
        pytest.param(
            ("queue1", "queue2"),
            {"max_workers": 3, "ack_policy": AckPolicy.ACK},
            id="multiple topics with manual commit",
        ),
        pytest.param(
            (),
            {
                "pattern": "pattern",
                "max_workers": 3,
                "ack_policy": AckPolicy.ACK,
            },
            id="pattern with manual commit",
        ),
        pytest.param(
            (),
            {
                "partitions": [TopicPartition(topic="topic", partition=1)],
                "max_workers": 3,
                "ack_policy": AckPolicy.ACK,
            },
            id="partitions with manual commit",
        ),
    ),
)
def test_wrong_destination(args: list[str], kwargs: dict[str, Any]) -> None:
    with pytest.raises(SetupError):
        KafkaBroker().subscriber(*args, **kwargs)


@pytest.mark.kafka()
def test_max_workers_configuration(queue: str) -> None:
    broker = KafkaBroker()

    sub = broker.subscriber(queue, max_workers=3, ack_policy=AckPolicy.ACK_FIRST)
    assert isinstance(sub, ConcurrentDefaultSubscriber)

    sub = broker.subscriber(queue, max_workers=3, ack_policy=AckPolicy.REJECT_ON_ERROR)
    assert isinstance(sub, ConcurrentBetweenPartitionsSubscriber)

    with pytest.raises(SetupError):
        broker.subscriber(
            partitions=[TopicPartition(topic="topic", partition=1)],
            max_workers=3,
            ack_policy=AckPolicy.MANUAL,
        )


@pytest.mark.kafka()
def test_use_only_kafka_router() -> None:
    broker = KafkaBroker()
    router = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [KafkaRouter(), NatsRouter(), RabbitRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)
