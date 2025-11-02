import pytest

from faststream import AckPolicy
from faststream._internal.constants import EMPTY
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
def test_no_ack_override_ack_policy() -> None:
    config = KafkaSubscriberConfig(_no_ack=True, _ack_policy=AckPolicy.ACK_FIRST)

    assert config.ack_policy is AckPolicy.MANUAL
    assert config.connection_args == {"enable_auto_commit": False}


@pytest.mark.kafka()
@pytest.mark.parametrize(
    ("auto_commit", "enable_auto_commit", "ack_policy"),
    (
        pytest.param(True, True, AckPolicy.ACK_FIRST, id="autocommit_specified_true"),
        pytest.param(
            False, False, AckPolicy.REJECT_ON_ERROR, id="autocommit_specified_false"
        ),
        pytest.param(EMPTY, True, AckPolicy.ACK_FIRST, id="autocommit_on_ack_first_true"),
    ),
)
def test_auto_commit_override_ack_policy(
    auto_commit: bool,
    enable_auto_commit: bool,
    ack_policy: AckPolicy,
) -> None:
    config = KafkaSubscriberConfig(
        _auto_commit=auto_commit,
        _ack_policy=AckPolicy.ACK_FIRST,
    )

    assert config.connection_args == {"enable_auto_commit": enable_auto_commit}
    assert config.ack_policy is ack_policy
