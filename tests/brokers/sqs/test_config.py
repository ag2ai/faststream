from typing import Any

import pytest

from faststream import AckPolicy
from faststream.sqs import SQSBroker, SQSRouter
from faststream.sqs.schemas import SQSQueue
from faststream.sqs.subscriber.config import SQSSubscriberConfig


def make_config(**kwargs: Any) -> SQSSubscriberConfig:
    return SQSSubscriberConfig(
        queue="test",
        declare=SQSQueue(name="test"),
        **kwargs,
    )


@pytest.mark.sqs()
def test_default() -> None:
    config = make_config()

    assert config.ack_policy is AckPolicy.ACK


@pytest.mark.sqs()
def test_custom_ack() -> None:
    config = make_config(_ack_policy=AckPolicy.REJECT_ON_ERROR)

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.sqs()
def test_broker_ack_policy() -> None:
    broker = SQSBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = broker.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.sqs()
def test_router_ack_policy() -> None:
    router = SQSRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.sqs()
def test_broker_ack_policy_with_router() -> None:
    broker = SQSBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    router = SQSRouter()
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.sqs()
def test_router_overrides_broker() -> None:
    broker = SQSBroker(ack_policy=AckPolicy.ACK)
    router = SQSRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber("test")
    assert sub.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.sqs()
def test_sub_overrides_broker() -> None:
    broker = SQSBroker(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = broker.subscriber("test", ack_policy=AckPolicy.ACK)
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.sqs()
def test_sub_overrides_router() -> None:
    router = SQSRouter(ack_policy=AckPolicy.REJECT_ON_ERROR)
    sub = router.subscriber("test", ack_policy=AckPolicy.ACK)
    assert sub.ack_policy is AckPolicy.ACK
