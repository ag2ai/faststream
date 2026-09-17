import pytest

from faststream import AckPolicy
from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.redis import RedisBroker, RedisRouter, StreamSub
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
