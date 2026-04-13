from unittest.mock import MagicMock

import pytest

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitRouter
from faststream.rabbit.subscriber.config import RabbitSubscriberConfig


@pytest.mark.rabbit()
def test_ack_first() -> None:
    config = RabbitSubscriberConfig(
        _outer_config=MagicMock(),
        queue=MagicMock(),
        exchange=MagicMock(),
        _ack_policy=AckPolicy.ACK_FIRST,
    )

    assert config.ack_first
    assert config.auto_ack_disabled
    assert config.ack_policy is AckPolicy.ACK_FIRST


@pytest.mark.rabbit()
def test_default() -> None:
    broker = RabbitBroker()
    sub = broker.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.rabbit()
def test_broker_ack_policy() -> None:
    broker = RabbitBroker(ack_policy=AckPolicy.ACK)
    sub = broker.subscriber("test")
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.rabbit()
def test_router_ack_policy() -> None:
    router = RabbitRouter(ack_policy=AckPolicy.ACK)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.rabbit()
def test_router_overrides_broker() -> None:
    broker = RabbitBroker(ack_policy=AckPolicy.ACK_FIRST)
    router = RabbitRouter(ack_policy=AckPolicy.ACK)
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.rabbit()
def test_broker_ack_policy_without_router() -> None:
    broker = RabbitBroker(ack_policy=AckPolicy.ACK_FIRST)
    router = RabbitRouter()
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.ACK_FIRST


@pytest.mark.rabbit()
def test_sub_overrides_broker() -> None:
    broker = RabbitBroker(ack_policy=AckPolicy.ACK)
    sub = broker.subscriber("test", ack_policy=AckPolicy.ACK_FIRST)
    assert sub.ack_policy is AckPolicy.ACK_FIRST


@pytest.mark.rabbit()
def test_sub_overrides_router() -> None:
    router = RabbitRouter(ack_policy=AckPolicy.ACK)
    sub = router.subscriber("test", ack_policy=AckPolicy.ACK_FIRST)
    assert sub.ack_policy is AckPolicy.ACK_FIRST


@pytest.mark.rabbit()
def test_sub_overrides_broker_and_router() -> None:
    broker = RabbitBroker(ack_policy=AckPolicy.ACK)
    router = RabbitRouter(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber("test", ack_policy=AckPolicy.ACK_FIRST)
    assert sub.ack_policy is AckPolicy.ACK_FIRST
