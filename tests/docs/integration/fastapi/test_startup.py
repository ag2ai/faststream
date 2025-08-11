import pytest
from fastapi.testclient import TestClient

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@require_aiokafka
async def test_fastapi_kafka_startup() -> None:
    from docs.docs_src.integrations.fastapi.kafka.startup import app, hello, router
    from faststream.kafka import TestKafkaBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestKafkaBroker(router.broker):
        with TestClient(app):
            hello.mock.assert_called_once_with("Hello!")


@pytest.mark.asyncio()
@require_confluent
async def test_fastapi_confluent_startup() -> None:
    from docs.docs_src.integrations.fastapi.confluent.startup import app, hello, router
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestConfluentKafkaBroker(router.broker):
        with TestClient(app):
            hello.mock.assert_called_once_with("Hello!")


@pytest.mark.asyncio()
@require_aiopika
async def test_fastapi_rabbit_startup() -> None:
    from docs.docs_src.integrations.fastapi.rabbit.startup import app, hello, router
    from faststream.rabbit import TestRabbitBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestRabbitBroker(router.broker):
        with TestClient(app):
            hello.mock.assert_called_once_with("Hello!")


@pytest.mark.asyncio()
@require_nats
async def test_fastapi_nats_startup() -> None:
    from docs.docs_src.integrations.fastapi.nats.startup import app, hello, router
    from faststream.nats import TestNatsBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestNatsBroker(router.broker):
        with TestClient(app):
            hello.mock.assert_called_once_with("Hello!")


@pytest.mark.asyncio()
@require_redis
async def test_fastapi_redis_startup() -> None:
    from docs.docs_src.integrations.fastapi.redis.startup import app, hello, router
    from faststream.redis import TestRedisBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestRedisBroker(router.broker):
        with TestClient(app):
            hello.mock.assert_called_once_with("Hello!")
