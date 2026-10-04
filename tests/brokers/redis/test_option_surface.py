import pytest

from faststream.redis import RedisBroker, RedisPublisher, RedisRoute, RedisRouter
from tests.brokers.base.option_surface import (
    assert_every_level_takes_the_options_the_broker_takes,
)


@pytest.mark.redis()
def test_every_level_takes_the_options_the_broker_takes() -> None:
    assert_every_level_takes_the_options_the_broker_takes(
        RedisBroker,
        RedisRouter,
        RedisRoute,
        RedisPublisher,
    )
