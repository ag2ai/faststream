import pytest

from faststream import TestApp
from tests.marks import (
    pydantic_v2,
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)
from tests.mocks import mock_pydantic_settings_env


@pydantic_v2
@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_basic_lifespan() -> None:
    from faststream.rabbit import TestRabbitBroker

    with mock_pydantic_settings_env({"host": "localhost"}):
        from docs.docs_src.getting_started.lifespan.rabbit.basic import app, broker

        async with TestRabbitBroker(broker), TestApp(app):
            assert app.context.get("settings").host == "localhost"


@pydantic_v2
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_basic_lifespan() -> None:
    from faststream.kafka import TestKafkaBroker

    with mock_pydantic_settings_env({"host": "localhost"}):
        from docs.docs_src.getting_started.lifespan.kafka.basic import app, broker

        async with TestKafkaBroker(broker), TestApp(app):
            assert app.context.get("settings").host == "localhost"


@pydantic_v2
@pytest.mark.asyncio()
@require_confluent
async def test_confluent_basic_lifespan() -> None:
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    with mock_pydantic_settings_env({"host": "localhost"}):
        from docs.docs_src.getting_started.lifespan.confluent.basic import app, broker

        async with TestConfluentKafkaBroker(broker), TestApp(app):
            assert app.context.get("settings").host == "localhost"


@pydantic_v2
@pytest.mark.asyncio()
@require_nats
async def test_nats_basic_lifespan() -> None:
    from faststream.nats import TestNatsBroker

    with mock_pydantic_settings_env({"host": "localhost"}):
        from docs.docs_src.getting_started.lifespan.nats.basic import app, broker

        async with TestNatsBroker(broker), TestApp(app):
            assert app.context.get("settings").host == "localhost"


@pydantic_v2
@pytest.mark.asyncio()
@require_redis
async def test_redis_basic_lifespan() -> None:
    from faststream.redis import TestRedisBroker

    with mock_pydantic_settings_env({"host": "localhost"}):
        from docs.docs_src.getting_started.lifespan.redis.basic import app, broker

        async with TestRedisBroker(broker), TestApp(app):
            assert app.context.get("settings").host == "localhost"
