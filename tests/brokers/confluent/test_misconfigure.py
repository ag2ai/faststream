import pytest

from faststream import AckPolicy
from faststream.confluent import KafkaBroker, TopicPartition
from faststream.confluent.broker.router import KafkaRouter
from faststream.confluent.subscriber.usecase import ConcurrentDefaultSubscriber
from faststream.exceptions import SetupError
from faststream.nats import NatsRouter


@pytest.mark.confluent()
def test_max_workers_with_ack_policy(queue: str) -> None:
    broker = KafkaBroker()

    sub = broker.subscriber(queue, max_workers=3, ack_policy=AckPolicy.ACK_FIRST)
    assert isinstance(sub, ConcurrentDefaultSubscriber)

    with pytest.raises(SetupError):
        broker.subscriber(queue, max_workers=3, ack_policy=AckPolicy.REJECT_ON_ERROR)


@pytest.mark.confluent()
def test_manual_ack_policy_without_group(queue: str) -> None:
    broker = KafkaBroker()

    broker.subscriber(queue, group_id="test", ack_policy=AckPolicy.MANUAL)

    with pytest.raises(SetupError):
        broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)


@pytest.mark.confluent()
def test_wrong_destination(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.raises(SetupError):
        broker.subscriber()

    with pytest.raises(SetupError):
        broker.subscriber(queue, partitions=[TopicPartition(queue, 1)])


@pytest.mark.confluent()
def test_use_only_confluent_router() -> None:
    broker = KafkaBroker()
    router = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [KafkaRouter(), NatsRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)
