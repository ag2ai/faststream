import re

import pytest
from redis.asyncio.client import Redis

from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.redis import RedisBroker, RedisRouter, annotations


@pytest.mark.redis()
def test_use_only_redis_router() -> None:
    broker = RedisBroker()
    router = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [RedisRouter(), NatsRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)


@pytest.mark.redis()
def test_driver_class_annotation_names_the_import_to_use() -> None:
    expected = (
        "`handler` parameter `redis` is annotated with"
        " `redis.asyncio.client.Redis`,"
        " which FastStream cannot inject.\n"
        "Use the context annotation instead:\n"
        "\n    from faststream.redis.annotations import Redis\n"
    )

    broker = RedisBroker()

    with pytest.raises(SetupError, match=re.escape(expected)):

        @broker.subscriber("test")
        async def handler(redis: Redis) -> None: ...


@pytest.mark.redis()
def test_context_annotations_are_accepted() -> None:
    broker = RedisBroker()

    @broker.subscriber("test")
    async def handler(
        msg: annotations.RedisStreamMessage,
        redis: annotations.Redis,
        pipe: annotations.Pipeline,
        client: annotations.RedisBroker,
    ) -> None: ...
