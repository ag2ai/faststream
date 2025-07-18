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
@require_nats
async def test_parser_nats() -> None:
    from docs.docs_src.getting_started.serialization.parser_nats import (
        app,
        broker,
        handle,
    )
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")


@pytest.mark.asyncio()
@require_aiokafka
async def test_parser_kafka() -> None:
    from docs.docs_src.getting_started.serialization.parser_kafka import (
        app,
        broker,
        handle,
    )
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")


@pytest.mark.asyncio()
@require_confluent
async def test_parser_confluent() -> None:
    from docs.docs_src.getting_started.serialization.parser_confluent import (
        app,
        broker,
        handle,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")


@pytest.mark.asyncio()
@require_aiopika
async def test_parser_rabbit() -> None:
    from docs.docs_src.getting_started.serialization.parser_rabbit import (
        app,
        broker,
        handle,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")


@pytest.mark.asyncio()
@require_redis
async def test_parser_redis() -> None:
    from docs.docs_src.getting_started.serialization.parser_redis import (
        app,
        broker,
        handle,
    )
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker), TestApp(app):
        handle.mock.assert_called_once_with("")
