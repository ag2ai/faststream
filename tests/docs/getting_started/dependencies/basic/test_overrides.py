import pytest

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)


@pytest.mark.asyncio()
@require_aiokafka
async def test_overrides_kafka() -> None:
    from docs.docs_src.getting_started.dependencies.basic.kafka.overrides import (
        test_handler,
    )

    await test_handler()


@pytest.mark.asyncio()
@require_confluent
async def test_overrides_confluent() -> None:
    from docs.docs_src.getting_started.dependencies.basic.confluent.overrides import (
        test_handler,
    )

    await test_handler()


@pytest.mark.asyncio()
@require_aiopika
async def test_overrides_rabbit() -> None:
    from docs.docs_src.getting_started.dependencies.basic.rabbit.overrides import (
        test_handler,
    )

    await test_handler()


@pytest.mark.asyncio()
@require_nats
async def test_overrides_nats() -> None:
    from docs.docs_src.getting_started.dependencies.basic.nats.overrides import (
        test_handler,
    )

    await test_handler()


@pytest.mark.asyncio()
@require_redis
async def test_overrides_redis() -> None:
    from docs.docs_src.getting_started.dependencies.basic.redis.overrides import (
        test_handler,
    )

    await test_handler()


@pytest.mark.asyncio()
@require_mqtt
async def test_overrides_mqtt() -> None:
    from docs.docs_src.getting_started.dependencies.basic.mqtt.overrides import (
        test_handler,
    )

    await test_handler()
