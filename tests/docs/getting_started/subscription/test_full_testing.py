import pytest

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@pytest.mark.kafka()
@require_aiokafka
async def test_handle_kafka() -> None:
    from docs.docs_src.getting_started.subscription.kafka.full_testing import (
        test_handle as test_handle_k,
    )

    await test_handle_k()


@pytest.mark.asyncio()
@pytest.mark.confluent()
@require_confluent
async def test_handle_confluent() -> None:
    from docs.docs_src.getting_started.subscription.confluent.full_testing import (
        test_handle as test_handle_confluent,
    )

    await test_handle_confluent()


@pytest.mark.asyncio()
@pytest.mark.rabbit()
@require_aiopika
async def test_handle_rabbit() -> None:
    from docs.docs_src.getting_started.subscription.rabbit.full_testing import (
        test_handle as test_handle_r,
    )

    await test_handle_r()


@pytest.mark.asyncio()
@pytest.mark.nats()
@require_nats
async def test_handle_nats() -> None:
    from docs.docs_src.getting_started.subscription.nats.full_testing import (
        test_handle as test_handle_n,
    )

    await test_handle_n()


@pytest.mark.asyncio()
@pytest.mark.redis()
@require_redis
async def test_handle_redis() -> None:
    from docs.docs_src.getting_started.subscription.redis.full_testing import (
        test_handle as test_handle_rd,
    )

    await test_handle_rd()
