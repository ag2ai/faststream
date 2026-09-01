import pytest
from redis.asyncio.client import Redis

from faststream import AckPolicy
from faststream._internal._compat import ExceptionGroup
from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.redis import RedisBroker, RedisRouter, StreamSub, annotations
from faststream.redis.subscriber.usecases import StreamConcurrentSubscriber


@pytest.mark.redis()
def test_manual_ack_with_max_workers() -> None:
    """`XACK` is per-entry, so acking manually from concurrent tasks cannot conflict.

    The combination used to raise on the direct argument only. Declaring the same
    policy as a router default already reached `StreamConcurrentSubscriber`, because
    `ack_policy` is still `EMPTY` when the subscriber is validated.
    """
    broker = RedisBroker()

    @broker.subscriber(
        stream=StreamSub("stream", group="group", consumer="consumer"),
        ack_policy=AckPolicy.MANUAL,
        max_workers=2,
    )
    async def handle(msg: str) -> None: ...

    (subscriber,) = broker.subscribers
    assert isinstance(subscriber, StreamConcurrentSubscriber)
    assert subscriber.ack_policy is AckPolicy.MANUAL


@pytest.mark.redis()
def test_manual_ack_with_max_workers_via_router_default() -> None:
    """The router-default path reaches the same subscriber, and always did."""
    router = RedisRouter(ack_policy=AckPolicy.MANUAL)

    @router.subscriber(
        stream=StreamSub("stream", group="group", consumer="consumer"),
        max_workers=2,
    )
    async def handle(msg: str) -> None: ...

    broker = RedisBroker()
    broker.include_router(router)

    (subscriber,) = broker.subscribers
    assert isinstance(subscriber, StreamConcurrentSubscriber)
    assert subscriber.ack_policy is AckPolicy.MANUAL


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

    with pytest.raises(ExceptionGroup) as excinfo:

        @broker.subscriber("test")
        async def handler(redis: Redis) -> None: ...

    assert [str(e) for e in excinfo.value.exceptions] == [expected]


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
