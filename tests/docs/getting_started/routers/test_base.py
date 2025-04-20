import pytest

from faststream import TestApp
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@require_aiokafka
async def test_base_router_kafka() -> None:
    from docs.docs_src.getting_started.routers.kafka.router import (
        app,
        broker,
        handle,
        handle_response,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        handle_response.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_confluent
async def test_base_router_confluent() -> None:
    from docs.docs_src.getting_started.routers.confluent.router import (
        app,
        broker,
        handle,
        handle_response,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        handle_response.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_aiopika
async def test_base_router_rabbit() -> None:
    from docs.docs_src.getting_started.routers.rabbit.router import (
        app,
        broker,
        handle,
        handle_response,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        handle_response.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_nats
async def test_base_router_nats() -> None:
    from docs.docs_src.getting_started.routers.nats.router import (
        app,
        broker,
        handle,
        handle_response,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        handle_response.mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_redis
async def test_base_router_redis() -> None:
    from docs.docs_src.getting_started.routers.redis.router import (
        app,
        broker,
        handle,
        handle_response,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with({"name": "John", "user_id": 1})
        handle_response.mock.assert_called_once_with("Hi!")
