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


@pytest.mark.confluent()
@pytest.mark.parametrize(
    "topic",
    (
        pytest.param("cache{{shard}}", id="brace"),
        pytest.param("logs/errors", id="slash"),
        pytest.param("orders v2", id="space"),
        pytest.param("заказы", id="non-ascii"),
        pytest.param("x" * 250, id="too-long"),
        pytest.param("..", id="reserved"),
        pytest.param("", id="empty"),
    ),
)
def test_a_topic_kafka_would_refuse_is_rejected_where_it_is_written(
    topic: str,
) -> None:
    broker = KafkaBroker()

    # Kafka answers such a name with INVALID_TOPIC_EXCEPTION, but only once a
    # consumer or producer reaches the cluster.
    with pytest.raises(SetupError):
        broker.subscriber(topic)

    with pytest.raises(SetupError):
        broker.publisher(topic)

    with pytest.raises(SetupError):
        broker.subscriber(partitions=[TopicPartition(topic, 1)])
