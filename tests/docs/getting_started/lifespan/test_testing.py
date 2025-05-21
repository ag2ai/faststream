import pytest

from tests.marks import (
    python39,
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@python39
@require_redis
async def test_lifespan_redis() -> None:
    from docs.docs_src.getting_started.lifespan.redis.testing import (
        test_lifespan as _test_lifespan_red,
    )

    await _test_lifespan_red()


@pytest.mark.asyncio()
@python39
@require_confluent
async def test_lifespan_confluent() -> None:
    from docs.docs_src.getting_started.lifespan.confluent.testing import (
        test_lifespan as _test_lifespan_confluent,
    )

    await _test_lifespan_confluent()


@pytest.mark.asyncio()
@python39
@require_aiokafka
async def test_lifespan_kafka() -> None:
    from docs.docs_src.getting_started.lifespan.kafka.testing import (
        test_lifespan as _test_lifespan_k,
    )

    await _test_lifespan_k()


@pytest.mark.asyncio()
@python39
@require_aiopika
async def test_lifespan_rabbit() -> None:
    from docs.docs_src.getting_started.lifespan.rabbit.testing import (
        test_lifespan as _test_lifespan_r,
    )

    await _test_lifespan_r()


@pytest.mark.asyncio()
@python39
@require_nats
async def test_lifespan_nats() -> None:
    from docs.docs_src.getting_started.lifespan.nats.testing import (
        test_lifespan as _test_lifespan_n,
    )

    await _test_lifespan_n()
