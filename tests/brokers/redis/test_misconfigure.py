from typing import Any

import pytest
from redis.asyncio.client import Pipeline, Redis

from faststream import AckPolicy
from faststream._internal._compat import ExceptionGroup
from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.redis import (
    ListSub,
    RedisBroker,
    RedisRouter,
    StreamSub,
    TestRedisBroker,
    annotations,
)
from faststream.redis.subscriber.usecases import StreamConcurrentSubscriber
from tests.brokers.base.driver_annotations import DriverAnnotationTestcase

from .basic import RedisMemoryTestcaseConfig


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
    router: Any = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers: list[Any] = [RedisRouter(), NatsRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(*routers)


@pytest.mark.redis()
@pytest.mark.parametrize(
    "destination",
    (
        pytest.param({"list": ListSub("list", batch=True)}, id="list"),
        pytest.param({"stream": StreamSub("stream", batch=True)}, id="stream"),
    ),
)
def test_max_workers_ignored_by_batch(destination: dict[str, Any]) -> None:
    broker = RedisBroker()

    with pytest.warns(RuntimeWarning, match="`max_workers` option is ignored") as record:
        broker.subscriber(**destination, max_workers=2)

    # the warning points at the line that registered the subscriber
    assert [w.filename for w in record if "max_workers" in str(w.message)] == [__file__]


@pytest.mark.redis()
class TestDriverAnnotations(RedisMemoryTestcaseConfig, DriverAnnotationTestcase):
    driver_class = Redis
    driver_path = "redis.asyncio.client.Redis"
    context_annotation = annotations.Redis
    annotation_import = "from faststream.redis.annotations import Redis"


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_every_driver_class_argument_is_reported() -> None:
    broker = RedisBroker()

    @broker.subscriber("test")
    async def handler(redis: Redis, pipe: Pipeline) -> None: ...  # type: ignore[type-arg]  # the bare driver generic is the mistake under test

    with pytest.raises(ExceptionGroup) as excinfo:
        async with TestRedisBroker(broker):
            pass

    assert excinfo.value.message == "`handler` has arguments FastStream cannot inject."
    assert [str(e).splitlines()[0] for e in excinfo.value.exceptions] == [
        (
            "`redis` is annotated with `redis.asyncio.client.Redis`,"
            " which FastStream cannot inject."
        ),
        (
            "`pipe` is annotated with `redis.asyncio.client.Pipeline`,"
            " which FastStream cannot inject."
        ),
    ]
