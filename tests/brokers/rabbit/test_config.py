import pytest

from unittest.mock import MagicMock

from faststream import AckPolicy
from faststream.rabbit.subscriber.config import RabbitSubscriberConfig


@pytest.mark.rabbit()
def test_default() -> None:
    config = RabbitSubscriberConfig(
        _outer_config=MagicMock(),
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

    assert config.ack_policy is AckPolicy.DO_NOTHING
    assert config.ack_first


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
def test_no_ack() -> None:
    config = RabbitSubscriberConfig(
        _outer_config=MagicMock(),
        queue=MagicMock(),
        exchange=MagicMock(),
        _no_ack=True,
    )

    assert config.ack_policy is AckPolicy.DO_NOTHING
    assert not config.ack_first
