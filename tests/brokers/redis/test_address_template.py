import pytest

from faststream.redis import RedisBroker, RedisRouter
from faststream.redis.schemas import PubSub


@pytest.mark.redis()
def test_channel_keeps_both_the_template_and_the_broker_address() -> None:
    channel = PubSub("logs.{level}")

    assert channel.channel_template == "logs.{level}"
    assert channel.name == "logs.*"


@pytest.mark.redis()
def test_pattern_is_a_flag_not_the_template() -> None:
    assert PubSub("logs.{level}").pattern is True
    assert PubSub("logs.*").pattern is True
    assert PubSub("logs", pattern=True).pattern is True
    assert PubSub("logs").pattern is False


@pytest.mark.redis()
def test_router_prefix_reaches_both_reads() -> None:
    broker = RedisBroker()
    router = RedisRouter(prefix="prefix_")

    @router.subscriber("logs.{level}")
    async def handler(msg: str) -> None: ...

    publisher = router.publisher("out.{id}")

    broker.include_router(router)

    channel = broker.subscribers[0].channel
    assert channel.channel_template == "prefix_logs.{level}"
    assert channel.name == "prefix_logs.*"

    assert publisher.channel.channel_template == "prefix_out.{id}"
    assert publisher.channel.name == "prefix_out.*"
