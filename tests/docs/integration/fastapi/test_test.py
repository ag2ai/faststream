import pytest

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka() -> None:
    from docs.docs_src.integrations.fastapi.kafka.test import test_router

    await test_router()


@pytest.mark.asyncio()
@require_confluent
async def test_confluent() -> None:
    from docs.docs_src.integrations.fastapi.confluent.test import test_router

    await test_router()


@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit() -> None:
    from docs.docs_src.integrations.fastapi.rabbit.test import test_router

    await test_router()


@pytest.mark.asyncio()
@require_nats
async def test_nats() -> None:
    from docs.docs_src.integrations.fastapi.nats.test import test_router

    await test_router()


@pytest.mark.asyncio()
@require_redis
async def test_redis() -> None:
    from docs.docs_src.integrations.fastapi.redis.test import test_router

    await test_router()
