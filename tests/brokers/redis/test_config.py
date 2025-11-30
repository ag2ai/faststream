from unittest.mock import MagicMock

import pytest

from faststream import AckPolicy
from faststream.redis import ListSub, PubSub, StreamSub
from faststream.redis.subscriber.config import RedisSubscriberConfig


@pytest.mark.redis()
def test_channel_sub() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(),
        channel_sub=PubSub("test_channel"),
    )
    assert config.ack_policy is AckPolicy.MANUAL


@pytest.mark.redis()
def test_list_sub() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(),
        list_sub=ListSub("test_list"),
    )
    assert config.ack_policy is AckPolicy.MANUAL


@pytest.mark.redis()
def test_stream_sub() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(),
        stream_sub=StreamSub("test_stream"),
    )
    assert config.ack_policy is AckPolicy.MANUAL


@pytest.mark.redis()
def test_stream_with_group() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(),
        stream_sub=StreamSub(
            "test_stream",
            group="test_group",
            consumer="test_consumer",
        ),
    )
    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.redis()
def test_stream_sub_with_no_ack_group() -> None:
    with pytest.warns(
        RuntimeWarning,
        match="`no_ack` is not supported by consumer group with last_id other than `>`",
    ):
        config = RedisSubscriberConfig(
            _outer_config=MagicMock(),
            stream_sub=StreamSub(
                "test_stream",
                group="test_group",
                consumer="test_consumer",
                no_ack=True,
                last_id="$",
            ),
        )
    assert config.ack_policy is AckPolicy.MANUAL


@pytest.mark.redis()
def test_stream_with_group_and_min_idle_time() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(),
        stream_sub=StreamSub(
            "test_stream",
            group="test_group",
            consumer="test_consumer",
            min_idle_time=1000,
        ),
    )
    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.redis()
def test_custom_ack() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(),
        stream_sub=StreamSub(
            "test_stream",
            group="test_group",
            consumer="test_consumer",
        ),
        _ack_policy=AckPolicy.ACK,
    )
    assert config.ack_policy is AckPolicy.ACK


@pytest.mark.redis()
def test_no_ack() -> None:
    config = RedisSubscriberConfig(_outer_config=MagicMock(), _no_ack=True)
    assert config.ack_policy is AckPolicy.MANUAL
