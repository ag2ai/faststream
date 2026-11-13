from typing import Any

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


@pytest.mark.kafka()
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


@pytest.mark.kafka()
@pytest.mark.parametrize(
    ("aiokafka_v013", "api_version"),
    (
        pytest.param(False, "2.0", id="aiokafka<0.13"),
        pytest.param(True, None, id="aiokafka>=0.13"),
    ),
)
def test_protocol_version_reaches_aiokafka_before_013(
    monkeypatch: pytest.MonkeyPatch,
    aiokafka_v013: bool,
    api_version: str | None,
) -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3202."""
    monkeypatch.setattr("faststream.kafka.broker.broker.AIOKAFKA_V013", aiokafka_v013)

    broker = KafkaBroker(protocol_version="2.0")

    # the builder is a `functools.partial`, typed as a plain callable
    builder: Any = broker.config.broker_config.builder
    assert builder.keywords.get("api_version") == api_version
