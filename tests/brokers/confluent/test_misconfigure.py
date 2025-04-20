import pytest

from faststream import AckPolicy
from faststream.confluent import KafkaBroker, TopicPartition
from faststream.confluent.router import KafkaRouter
from faststream.confluent.subscriber.specified import (
    SpecificationConcurrentDefaultSubscriber,
)
from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.redis import RedisRouter


def test_max_workers_with_manual(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.warns(DeprecationWarning):
        sub = broker.subscriber(queue, max_workers=3, auto_commit=True)
        assert isinstance(sub, SpecificationConcurrentDefaultSubscriber)

    with pytest.raises(SetupError), pytest.warns(DeprecationWarning):
        broker.subscriber(queue, max_workers=3, auto_commit=False)


def test_max_workers_with_ack_policy(queue: str) -> None:
    broker = KafkaBroker()

    sub = broker.subscriber(queue, max_workers=3, ack_policy=AckPolicy.ACK_FIRST)
    assert isinstance(sub, SpecificationConcurrentDefaultSubscriber)

    with pytest.raises(SetupError):
        broker.subscriber(queue, max_workers=3, ack_policy=AckPolicy.REJECT_ON_ERROR)


def test_deprecated_options(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.warns(DeprecationWarning):
        broker.subscriber(queue, group_id="test", auto_commit=False)

    with pytest.warns(DeprecationWarning):
        broker.subscriber(queue, auto_commit=True)

    with pytest.warns(DeprecationWarning):
        broker.subscriber(queue, group_id="test", no_ack=False)

    with pytest.warns(DeprecationWarning):
        broker.subscriber(queue, group_id="test", no_ack=True)


def test_deprecated_conflicts_actual(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.raises(SetupError), pytest.warns(DeprecationWarning):
        broker.subscriber(queue, auto_commit=False, ack_policy=AckPolicy.ACK)

    with pytest.raises(SetupError), pytest.warns(DeprecationWarning):
        broker.subscriber(queue, no_ack=False, ack_policy=AckPolicy.ACK)


def test_manual_ack_policy_without_group(queue: str) -> None:
    broker = KafkaBroker()

    broker.subscriber(queue, group_id="test", ack_policy=AckPolicy.DO_NOTHING)

    with pytest.raises(SetupError):
        broker.subscriber(queue, ack_policy=AckPolicy.DO_NOTHING)


def test_manual_commit_without_group(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.warns(DeprecationWarning):
        broker.subscriber(queue, group_id="test", auto_commit=False)

    with pytest.raises(SetupError), pytest.warns(DeprecationWarning):
        broker.subscriber(queue, auto_commit=False)


def test_wrong_destination(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.raises(SetupError):
        broker.subscriber()

    with pytest.raises(SetupError):
        broker.subscriber(queue, partitions=[TopicPartition(queue, 1)])


def test_use_only_confluent_router() -> None:
    broker = KafkaBroker()
    router = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [KafkaRouter(), NatsRouter(), RedisRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)
