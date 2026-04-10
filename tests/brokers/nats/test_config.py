import pytest

from faststream import AckPolicy
from faststream._internal.constants import EMPTY
from faststream.nats import ConsumerConfig
from faststream.nats.configs import NatsBrokerConfig
from faststream.nats.subscriber.config import NatsSubscriberConfig


@pytest.mark.nats()
def test_default() -> None:
    config = NatsSubscriberConfig(
        subject="test_subject",
        sub_config=ConsumerConfig(),
    )

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.nats()
def test_custom_ack() -> None:
    config = NatsSubscriberConfig(
        subject="test_subject",
        sub_config=ConsumerConfig(),
        _ack_policy=AckPolicy.ACK,
    )

    assert config.ack_policy is AckPolicy.ACK


@pytest.mark.nats()
def test_broker_level_ack_policy_fallback() -> None:
    broker_config = NatsBrokerConfig(ack_policy=AckPolicy.NACK_ON_ERROR)
    config = NatsSubscriberConfig(
        _outer_config=broker_config,
        subject="test_subject",
        sub_config=ConsumerConfig(),
    )

    assert config.ack_policy is AckPolicy.NACK_ON_ERROR


@pytest.mark.nats()
def test_subscriber_ack_policy_overrides_broker() -> None:
    broker_config = NatsBrokerConfig(ack_policy=AckPolicy.NACK_ON_ERROR)
    config = NatsSubscriberConfig(
        _outer_config=broker_config,
        subject="test_subject",
        sub_config=ConsumerConfig(),
        _ack_policy=AckPolicy.ACK,
    )

    assert config.ack_policy is AckPolicy.ACK


@pytest.mark.nats()
def test_broker_level_ack_policy_empty_uses_default() -> None:
    broker_config = NatsBrokerConfig(ack_policy=EMPTY)
    config = NatsSubscriberConfig(
        _outer_config=broker_config,
        subject="test_subject",
        sub_config=ConsumerConfig(),
    )

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR
