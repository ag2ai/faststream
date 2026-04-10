from unittest.mock import MagicMock

import pytest

from faststream import AckPolicy
from faststream._internal.constants import EMPTY
from faststream.rabbit.configs import RabbitBrokerConfig
from faststream.rabbit.subscriber.config import RabbitSubscriberConfig


@pytest.mark.rabbit()
def test_default() -> None:
    config = RabbitSubscriberConfig(
        _outer_config=RabbitBrokerConfig(),
        queue=MagicMock(),
        exchange=MagicMock(),
    )

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR


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
def test_custom_ack() -> None:
    config = RabbitSubscriberConfig(
        _outer_config=MagicMock(),
        queue=MagicMock(),
        exchange=MagicMock(),
        _ack_policy=AckPolicy.ACK,
    )

    assert config.ack_policy is AckPolicy.ACK


@pytest.mark.rabbit()
def test_broker_level_ack_policy_fallback() -> None:
    broker_config = RabbitBrokerConfig(ack_policy=AckPolicy.NACK_ON_ERROR)
    config = RabbitSubscriberConfig(
        _outer_config=broker_config,
        queue=MagicMock(),
        exchange=MagicMock(),
    )

    assert config.ack_policy is AckPolicy.NACK_ON_ERROR


@pytest.mark.rabbit()
def test_subscriber_ack_policy_overrides_broker() -> None:
    broker_config = RabbitBrokerConfig(ack_policy=AckPolicy.NACK_ON_ERROR)
    config = RabbitSubscriberConfig(
        _outer_config=broker_config,
        queue=MagicMock(),
        exchange=MagicMock(),
        _ack_policy=AckPolicy.ACK,
    )

    assert config.ack_policy is AckPolicy.ACK


@pytest.mark.rabbit()
def test_broker_level_ack_policy_empty_uses_default() -> None:
    broker_config = RabbitBrokerConfig(ack_policy=EMPTY)
    config = RabbitSubscriberConfig(
        _outer_config=broker_config,
        queue=MagicMock(),
        exchange=MagicMock(),
    )

    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR
