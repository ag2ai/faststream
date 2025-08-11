import pytest

from faststream import AckPolicy
from faststream.kafka.subscriber.config import KafkaSubscriberConfig


@pytest.mark.kafka()
def test_default() -> None:
    config = KafkaSubscriberConfig()

    assert config.ack_policy is AckPolicy.MANUAL
    assert config.ack_first
    assert config.connection_args == {"enable_auto_commit": True}


@pytest.mark.kafka()
def test_ack_first() -> None:
    config = KafkaSubscriberConfig(_ack_policy=AckPolicy.ACK_FIRST)

    assert config.ack_policy is AckPolicy.MANUAL
    assert config.ack_first
    assert config.connection_args == {"enable_auto_commit": True}


@pytest.mark.kafka()
def test_custom_ack() -> None:
    config = KafkaSubscriberConfig(_ack_policy=AckPolicy.REJECT_ON_ERROR)

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR
    assert config.connection_args == {}


@pytest.mark.kafka()
def test_no_ack() -> None:
    config = KafkaSubscriberConfig(_no_ack=True, _ack_policy=AckPolicy.ACK_FIRST)

    assert config.ack_policy is AckPolicy.MANUAL
    assert config.connection_args == {}


@pytest.mark.kafka()
def test_auto_commit() -> None:
    config = KafkaSubscriberConfig(_auto_commit=True, _ack_policy=AckPolicy.ACK_FIRST)

    assert config.ack_policy is AckPolicy.MANUAL
    assert config.ack_first
    assert config.connection_args == {"enable_auto_commit": True}
