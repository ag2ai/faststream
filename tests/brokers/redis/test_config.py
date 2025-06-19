from unittest.mock import MagicMock

from faststream import AckPolicy
from faststream.redis import ListSub, PubSub, StreamSub
from faststream.redis.subscriber.config import RedisSubscriberConfig


def test_channel_sub() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(), channel_sub=PubSub("test_channel")
    )
    assert config.ack_policy is AckPolicy.DO_NOTHING


def test_list_sub() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(), list_sub=ListSub("test_list")
    )
    assert config.ack_policy is AckPolicy.DO_NOTHING


def test_stream_sub() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(), stream_sub=StreamSub("test_stream")
    )
    assert config.ack_policy is AckPolicy.DO_NOTHING


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


def test_stream_sub_with_no_ack_group() -> None:
    config = RedisSubscriberConfig(
        _outer_config=MagicMock(),
        stream_sub=StreamSub(
            "test_stream",
            group="test_group",
            consumer="test_consumer",
            no_ack=True,
        ),
    )
    assert config.ack_policy is AckPolicy.DO_NOTHING


def test_no_ack() -> None:
    config = RedisSubscriberConfig(_outer_config=MagicMock(), _no_ack=True)
    assert config.ack_policy is AckPolicy.DO_NOTHING
