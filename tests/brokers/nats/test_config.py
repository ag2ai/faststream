import pytest

from faststream import AckPolicy
from faststream.nats import ConsumerConfig, NatsBroker, NatsRouter
from faststream.nats.subscriber.config import NatsSubscriberConfig


@pytest.mark.nats()
def test_default() -> None:
    config = NatsSubscriberConfig(
        subject="test_subject",
        sub_config=ConsumerConfig(),
    )

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.nats()
def test_broker_ack_policy() -> None:
    broker = NatsBroker(ack_policy=AckPolicy.ACK)
    sub = broker.subscriber("test")
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.nats()
def test_router_ack_policy() -> None:
    router = NatsRouter(ack_policy=AckPolicy.ACK)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.rabbit()
def test_broker_ack_policy_without_router() -> None:
    broker = NatsBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    router = NatsRouter()
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.nats()
def test_router_overrides_broker() -> None:
    broker = NatsBroker(ack_policy=AckPolicy.ACK_FIRST)
    router = NatsRouter(ack_policy=AckPolicy.ACK)
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.nats()
def test_sub_overrides_broker() -> None:
    broker = NatsBroker(ack_policy=AckPolicy.ACK)
    sub = broker.subscriber("test", stream="t", ack_policy=AckPolicy.NACK_ON_ERROR)
    assert sub.ack_policy is AckPolicy.NACK_ON_ERROR


@pytest.mark.nats()
def test_sub_overrides_router() -> None:
    router = NatsRouter(ack_policy=AckPolicy.ACK)
    sub = router.subscriber("test", stream="t", ack_policy=AckPolicy.NACK_ON_ERROR)
    assert sub.ack_policy is AckPolicy.NACK_ON_ERROR


@pytest.mark.nats()
def test_sub_overrides_broker_and_router() -> None:
    broker = NatsBroker(ack_policy=AckPolicy.ACK)
    router = NatsRouter(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber("test", stream="t", ack_policy=AckPolicy.REJECT_ON_ERROR)
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR
