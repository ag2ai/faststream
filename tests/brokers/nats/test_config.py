from faststream import AckPolicy
from faststream.nats import ConsumerConfig
from faststream.nats.configs import NatsSubscriberConfig


def test_default() -> None:
    config = NatsSubscriberConfig(
        subject="test_subject",
        sub_config=ConsumerConfig(),
    )

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR


def test_no_ack() -> None:
    config = NatsSubscriberConfig(
        subject="test_subject",
        sub_config=ConsumerConfig(),
        _no_ack=True,
    )

    assert config.ack_policy is AckPolicy.DO_NOTHING


def test_ack_first() -> None:
    config = NatsSubscriberConfig(
        subject="test_subject",
        sub_config=ConsumerConfig(),
        _ack_first=True,
    )

    assert config.ack_policy is AckPolicy.ACK_FIRST


def test_custom_ack() -> None:
    config = NatsSubscriberConfig(
        subject="test_subject",
        sub_config=ConsumerConfig(),
        _ack_policy=AckPolicy.ACK,
    )

    assert config.ack_policy is AckPolicy.ACK
