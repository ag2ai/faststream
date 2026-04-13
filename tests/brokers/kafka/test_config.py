import pytest

from faststream import AckPolicy
from faststream.kafka import KafkaBroker, KafkaRouter
from faststream.kafka.subscriber.config import KafkaSubscriberConfig


@pytest.mark.kafka()
def test_default() -> None:
    config = KafkaSubscriberConfig()

    assert config.auto_ack_disabled
    assert config.ack_policy is AckPolicy.ACK_FIRST
    assert config.connection_args == {"enable_auto_commit": True}


@pytest.mark.kafka()
def test_ack_first() -> None:
    config = KafkaSubscriberConfig(_ack_policy=AckPolicy.ACK_FIRST)

    assert config.auto_ack_disabled
    assert config.connection_args == {"enable_auto_commit": True}


@pytest.mark.kafka()
def test_custom_ack() -> None:
    config = KafkaSubscriberConfig(_ack_policy=AckPolicy.REJECT_ON_ERROR)

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR
    assert config.connection_args == {"enable_auto_commit": False}


@pytest.mark.kafka()
def test_broker_ack_policy() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = broker.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.kafka()
def test_router_ack_policy() -> None:
    router = KafkaRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.rabbit()
def test_broker_ack_policy_without_router() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    router = KafkaRouter()
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.kafka()
def test_router_overrides_broker() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.ACK)
    router = KafkaRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.kafka()
def test_sub_overrides_broker() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = broker.subscriber("test", ack_policy=AckPolicy.ACK)
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.kafka()
def test_sub_overrides_router() -> None:
    router = KafkaRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = router.subscriber("test", ack_policy=AckPolicy.ACK)
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.kafka()
def test_sub_overrides_broker_and_router() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    router = KafkaRouter(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber("test", ack_policy=AckPolicy.ACK)
    assert sub.ack_policy is AckPolicy.ACK
