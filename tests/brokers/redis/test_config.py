from unittest.mock import MagicMock

import pytest

from faststream import AckPolicy
from faststream._internal.constants import EMPTY
from faststream.redis import ListSub, PubSub, RedisBroker, RedisRouter, StreamSub
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
        _outer_config=MagicMock(ack_policy=EMPTY),
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
        _outer_config=MagicMock(ack_policy=EMPTY),
        stream_sub=StreamSub(
            "test_stream",
            group="test_group",
            consumer="test_consumer",
            min_idle_time=1000,
        ),
    )
    assert config.ack_policy is AckPolicy.REJECT_ON_ERROR


@pytest.mark.redis()
def test_broker_ack_policy() -> None:
    broker = RedisBroker(ack_policy=AckPolicy.ACK)
    sub = broker.subscriber(
        stream=StreamSub("test", group="g", consumer="c"),
    )
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.redis()
def test_router_ack_policy() -> None:
    router = RedisRouter(ack_policy=AckPolicy.ACK)
    sub = router.subscriber(
        stream=StreamSub("test", group="g", consumer="c"),
    )
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.rabbit()
def test_broker_ack_policy_without_router() -> None:
    broker = RedisBroker(ack_policy=AckPolicy.ACK)
    router = RedisRouter()
    broker.include_router(router)
    sub = router.subscriber(stream=StreamSub("test", group="g", consumer="c"))
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.redis()
def test_router_overrides_broker() -> None:
    broker = RedisBroker(ack_policy=AckPolicy.ACK_FIRST)
    router = RedisRouter(ack_policy=AckPolicy.ACK)
    broker.include_router(router)
    sub = router.subscriber(
        stream=StreamSub("test", group="g", consumer="c"),
    )
    assert sub.ack_policy is AckPolicy.ACK


@pytest.mark.redis()
def test_sub_overrides_broker() -> None:
    broker = RedisBroker(ack_policy=AckPolicy.ACK)
    sub = broker.subscriber(
        stream=StreamSub("test", group="g", consumer="c"),
        ack_policy=AckPolicy.ACK_FIRST,
    )
    assert sub.ack_policy is AckPolicy.ACK_FIRST


@pytest.mark.redis()
def test_sub_overrides_router() -> None:
    router = RedisRouter(ack_policy=AckPolicy.ACK)
    sub = router.subscriber(
        stream=StreamSub("test", group="g", consumer="c"),
        ack_policy=AckPolicy.ACK_FIRST,
    )
    assert sub.ack_policy is AckPolicy.ACK_FIRST


@pytest.mark.redis()
def test_sub_overrides_broker_and_router() -> None:
    broker = RedisBroker(ack_policy=AckPolicy.ACK)
    router = RedisRouter(ack_policy=AckPolicy.NACK_ON_ERROR)
    broker.include_router(router)
    sub = router.subscriber(
        stream=StreamSub("test", group="g", consumer="c"),
        ack_policy=AckPolicy.ACK_FIRST,
    )
    assert sub.ack_policy is AckPolicy.ACK_FIRST
