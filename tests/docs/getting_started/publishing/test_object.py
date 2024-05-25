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
async def test_handle_kafka():
    from docs.docs_src.getting_started.publishing.kafka.object_testing import (
        test_handle as test_handle_k,
    )

    await test_handle_k()


@pytest.mark.asyncio()
@require_confluent
async def test_handle_confluent():
    from docs.docs_src.getting_started.publishing.confluent.object_testing import (
        test_handle as test_handle_confluent,
    )

    await test_handle_confluent()


@pytest.mark.asyncio()
@require_aiopika
async def test_handle_rabbit():
    from docs.docs_src.getting_started.publishing.rabbit.object_testing import (
        test_handle as test_handle_r,
    )

    await test_handle_r()


@pytest.mark.asyncio()
@require_nats
async def test_handle_nats():
    from docs.docs_src.getting_started.publishing.nats.object_testing import (
        test_handle as test_handle_n,
    )

    await test_handle_n()


@pytest.mark.asyncio()
@require_redis
async def test_handle_redis():
    from docs.docs_src.getting_started.publishing.redis.object_testing import (
        test_handle as test_handle_red,
    )

    await test_handle_red()
