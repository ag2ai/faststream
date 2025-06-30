from faststream import AckPolicy
from faststream.kafka.subscriber.config import KafkaSubscriberConfig


def test_default() -> None:
    config = KafkaSubscriberConfig()

    assert config.ack_policy is AckPolicy.DO_NOTHING
    assert config.ack_first
    assert config.connection_args == {"enable_auto_commit": True}


def test_ack_first() -> None:
    config = KafkaSubscriberConfig(_ack_policy=AckPolicy.ACK_FIRST)

    assert config.ack_policy is AckPolicy.DO_NOTHING
    assert config.ack_first
    assert config.connection_args == {"enable_auto_commit": True}


def test_custom_ack() -> None:
    config = KafkaSubscriberConfig(_ack_policy=AckPolicy.REJECT_ON_ERROR)

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR
    assert config.connection_args == {}


def test_no_ack() -> None:
    config = KafkaSubscriberConfig(_no_ack=True, _ack_policy=AckPolicy.ACK_FIRST)

    assert config.ack_policy is AckPolicy.DO_NOTHING
    assert config.connection_args == {}


def test_auto_commit() -> None:
    config = KafkaSubscriberConfig(_auto_commit=True, _ack_policy=AckPolicy.ACK_FIRST)

    assert config.ack_policy is AckPolicy.DO_NOTHING
    assert config.ack_first
    assert config.connection_args == {"enable_auto_commit": True}
