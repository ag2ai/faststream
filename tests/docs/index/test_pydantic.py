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
async def test_kafka_correct() -> None:
    from docs.docs_src.index.kafka.test import test_correct as test_k_correct

    await test_k_correct()


@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_invalid() -> None:
    from docs.docs_src.index.kafka.test import test_invalid as test_k_invalid

    await test_k_invalid()


@pytest.mark.asyncio()
@require_confluent
async def test_confluent_correct() -> None:
    from docs.docs_src.index.confluent.test import (
        test_correct as test_confluent_correct,
    )

    await test_confluent_correct()


@pytest.mark.asyncio()
@require_confluent
async def test_confluent_invalid() -> None:
    from docs.docs_src.index.confluent.test import (
        test_invalid as test_confluent_invalid,
    )

    await test_confluent_invalid()


@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_correct() -> None:
    from docs.docs_src.index.rabbit.test import test_correct as test_r_correct

    await test_r_correct()


@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_invalid() -> None:
    from docs.docs_src.index.rabbit.test import test_invalid as test_r_invalid

    await test_r_invalid()


@pytest.mark.asyncio()
@require_nats
async def test_nats_correct() -> None:
    from docs.docs_src.index.nats.test import test_correct as test_n_correct

    await test_n_correct()


@pytest.mark.asyncio()
@require_nats
async def test_nats_invalid() -> None:
    from docs.docs_src.index.nats.test import test_invalid as test_n_invalid

    await test_n_invalid()


@pytest.mark.asyncio()
@require_redis
async def test_redis_correct() -> None:
    from docs.docs_src.index.redis.test import test_correct as test_red_correct

    await test_red_correct()


@pytest.mark.asyncio()
@require_redis
async def test_redis_invalid() -> None:
    from docs.docs_src.index.redis.test import test_invalid as test_red_invalid

    await test_red_invalid()
