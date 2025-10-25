from unittest.mock import MagicMock

import pytest

from faststream import AckPolicy
from faststream._internal.constants import EMPTY
from faststream.confluent.subscriber.config import KafkaSubscriberConfig


@pytest.mark.confluent()
def test_default() -> None:
    config = KafkaSubscriberConfig(_outer_config=MagicMock())

    assert config.ack_policy is AckPolicy.MANUAL
    assert config.ack_first
    assert config.connection_data == {"enable_auto_commit": True}


@pytest.mark.confluent()
def test_ack_first() -> None:
    config = KafkaSubscriberConfig(
        _outer_config=MagicMock(),
        _ack_policy=AckPolicy.ACK_FIRST,
    )

    assert config.ack_policy is AckPolicy.MANUAL
    assert config.ack_first
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
def test_no_ack() -> None:
    config = KafkaSubscriberConfig(
        _outer_config=MagicMock(),
        _no_ack=True,
        _ack_policy=AckPolicy.ACK_FIRST,
    )

    assert config.ack_policy is AckPolicy.MANUAL
    assert config.connection_data == {"enable_auto_commit": False}


@pytest.mark.confluent()
@pytest.mark.parametrize(
    ("auto_commit", "expected"),
    (
        pytest.param(True, True, id="autocommit_specified_true"),
        pytest.param(False, False, id="autocommit_specified_false"),
        pytest.param(EMPTY, True, id="autocommit_on_ack_first_true"),
    ),
)
def test_auto_commit(auto_commit: bool, expected: bool) -> None:
    config = KafkaSubscriberConfig(
        _outer_config=MagicMock(),
        _auto_commit=auto_commit,
        _ack_policy=AckPolicy.ACK_FIRST,
    )

    assert config.connection_data == {"enable_auto_commit": expected}
