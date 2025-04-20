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
async def test_fastapi_kafka_base() -> None:
    from docs.docs_src.integrations.fastapi.kafka.base import app, hello, router
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(router.broker) as br:
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        await br.publish({"m": {}}, "test")

        hello.mock.assert_called_once_with({"m": {}})

        list(br._publishers)[0].mock.assert_called_with(  # noqa: RUF015
            {"response": "Hello, Kafka!"},
        )


@pytest.mark.asyncio()
@require_confluent
async def test_fastapi_confluent_base() -> None:
    from docs.docs_src.integrations.fastapi.confluent.base import app, hello, router
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(router.broker) as br:
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        await br.publish({"m": {}}, "test")

        hello.mock.assert_called_once_with({"m": {}})

        list(br._publishers)[0].mock.assert_called_with(  # noqa: RUF015
            {"response": "Hello, Kafka!"},
        )


@pytest.mark.asyncio()
@require_aiopika
async def test_fastapi_rabbit_base() -> None:
    from docs.docs_src.integrations.fastapi.rabbit.base import app, hello, router
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(router.broker) as br:
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        await br.publish({"m": {}}, "test")

        hello.mock.assert_called_once_with({"m": {}})

        list(br._publishers)[0].mock.assert_called_with(  # noqa: RUF015
            {"response": "Hello, Rabbit!"},
        )


@pytest.mark.asyncio()
@require_nats
async def test_fastapi_nats_base() -> None:
    from docs.docs_src.integrations.fastapi.nats.base import app, hello, router
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(router.broker) as br:
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        await br.publish({"m": {}}, "test")

        hello.mock.assert_called_once_with({"m": {}})

        list(br._publishers)[0].mock.assert_called_with(  # noqa: RUF015
            {"response": "Hello, NATS!"},
        )


@pytest.mark.asyncio()
@require_redis
async def test_fastapi_redis_base() -> None:
    from docs.docs_src.integrations.fastapi.redis.base import app, hello, router
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(router.broker) as br:
        with TestClient(app) as client:
            assert client.get("/").text == '"Hello, HTTP!"'

        await br.publish({"m": {}}, "test")

        hello.mock.assert_called_once_with({"m": {}})

        list(br._publishers)[0].mock.assert_called_with(  # noqa: RUF015
            {"response": "Hello, Redis!"},
        )
