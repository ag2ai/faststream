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
async def test_fastapi_kafka_depends() -> None:
    from docs.docs_src.integrations.fastapi.kafka.depends import app, router
    from faststream.kafka import TestKafkaBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestKafkaBroker(router.broker):
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        handler.mock.assert_called_once_with("Hello, Kafka!")


@pytest.mark.asyncio()
@require_confluent
async def test_fastapi_confluent_depends() -> None:
    from docs.docs_src.integrations.fastapi.confluent.depends import app, router
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestConfluentKafkaBroker(router.broker):
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        handler.mock.assert_called_once_with("Hello, Kafka!")


@pytest.mark.asyncio()
@require_aiopika
async def test_fastapi_rabbit_depends() -> None:
    from docs.docs_src.integrations.fastapi.rabbit.depends import app, router
    from faststream.rabbit import TestRabbitBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestRabbitBroker(router.broker):
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        handler.mock.assert_called_once_with("Hello, Rabbit!")


@pytest.mark.asyncio()
@require_nats
async def test_fastapi_nats_depends() -> None:
    from docs.docs_src.integrations.fastapi.nats.depends import app, router
    from faststream.nats import TestNatsBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestNatsBroker(router.broker):
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        handler.mock.assert_called_once_with("Hello, NATS!")


@pytest.mark.asyncio()
@require_redis
async def test_fastapi_redis_depends() -> None:
    from docs.docs_src.integrations.fastapi.redis.depends import app, router
    from faststream.redis import TestRedisBroker

    @router.subscriber("test")
    async def handler() -> None: ...

    async with TestRedisBroker(router.broker):
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        handler.mock.assert_called_once_with("Hello, Redis!")
