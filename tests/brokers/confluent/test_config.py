from unittest.mock import MagicMock

import pytest

from faststream import AckPolicy
from faststream._internal.constants import EMPTY
from faststream.confluent.configs import KafkaBrokerConfig
from faststream.confluent.subscriber.config import KafkaSubscriberConfig


@pytest.mark.confluent()
def test_default() -> None:
    config = KafkaSubscriberConfig(_outer_config=KafkaBrokerConfig())

    assert config.auto_ack_disabled
    assert config.ack_policy is AckPolicy.ACK_FIRST
    assert config.connection_data == {"enable_auto_commit": True}


@pytest.mark.confluent()
def test_ack_first() -> None:
    config = KafkaSubscriberConfig(
        _outer_config=MagicMock(),
        _ack_policy=AckPolicy.ACK_FIRST,
    )

    assert config.auto_ack_disabled
    assert config.ack_policy is AckPolicy.ACK_FIRST
    assert config.connection_data == {"enable_auto_commit": True}


@pytest.mark.confluent()
def test_custom_ack() -> None:
    config = KafkaSubscriberConfig(
        _outer_config=MagicMock(),
        _ack_policy=AckPolicy.REJECT_ON_ERROR,
    )

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR
    assert config.connection_data == {"enable_auto_commit": False}


@pytest.mark.confluent()
def test_broker_level_ack_policy_fallback() -> None:
    broker_config = KafkaBrokerConfig(ack_policy=AckPolicy.NACK_ON_ERROR)
    config = KafkaSubscriberConfig(_outer_config=broker_config)

    assert config.ack_policy is AckPolicy.NACK_ON_ERROR
    assert config.connection_data == {"enable_auto_commit": False}


@pytest.mark.confluent()
def test_subscriber_ack_policy_overrides_broker() -> None:
    broker_config = KafkaBrokerConfig(ack_policy=AckPolicy.NACK_ON_ERROR)
    config = KafkaSubscriberConfig(
        _outer_config=broker_config,
        _ack_policy=AckPolicy.ACK,
    )

    assert config.ack_policy is AckPolicy.ACK
    assert config.connection_data == {"enable_auto_commit": False}


@pytest.mark.confluent()
def test_broker_level_ack_policy_empty_uses_default() -> None:
    broker_config = KafkaBrokerConfig(ack_policy=EMPTY)
    config = KafkaSubscriberConfig(_outer_config=broker_config)

    assert config.ack_policy is AckPolicy.ACK_FIRST
    assert config.connection_data == {"enable_auto_commit": True}
