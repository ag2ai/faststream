import pytest

from faststream import AckPolicy
from faststream.mqtt import MQTTBroker, MQTTRouter


@pytest.mark.mqtt()
def test_default() -> None:
    broker = MQTTBroker()
    sub = broker.subscriber("test")
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.mqtt()
def test_broker_ack_policy() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = broker.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_router_ack_policy() -> None:
    router = MQTTRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_broker_ack_policy_without_router() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    router = MQTTRouter()
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_router_overrides_broker() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.ACK_FIRST)
    router = MQTTRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_sub_overrides_broker() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.ACK)
    sub = broker.subscriber("test", ack_policy=AckPolicy.REJECT_ON_ERROR)
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_sub_overrides_router() -> None:
    router = MQTTRouter(ack_policy=AckPolicy.ACK)
    sub = router.subscriber("test", ack_policy=AckPolicy.REJECT_ON_ERROR)
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.mqtt()
def test_sub_overrides_broker_and_router() -> None:
    broker = MQTTBroker(ack_policy=AckPolicy.ACK)

    router = MQTTRouter(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)

    sub = router.subscriber("test", ack_policy=AckPolicy.ACK_FIRST)
    assert sub.ack_policy is AckPolicy.ACK_FIRST
