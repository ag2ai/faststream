import gc

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
async def test_delay_router_kafka() -> None:
    from docs.docs_src.getting_started.routers.kafka.delay_equal import (
        broker as control_broker,
    )
    from docs.docs_src.getting_started.routers.kafka.router_delay import app, broker
    from faststream.kafka import TestKafkaBroker

    gc.collect()

    assert len(broker.subscribers) == len(control_broker.subscribers) == 1
    assert len(broker.publishers) == len(control_broker.publishers) == 1

    async with TestKafkaBroker(broker) as br, TestApp(app):
        br.subscribers[1].calls[0].handler.mock.assert_called_once_with(
            {"name": "John", "user_id": 1},
        )

        br.publishers[0].mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_confluent
async def test_delay_router_confluent() -> None:
    from docs.docs_src.getting_started.routers.confluent.delay_equal import (
        broker as control_broker,
    )
    from docs.docs_src.getting_started.routers.confluent.router_delay import (
        app,
        broker,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    gc.collect()

    assert len(broker.subscribers) == len(control_broker.subscribers) == 1
    assert len(broker.publishers) == len(control_broker.publishers) == 1

    async with TestConfluentKafkaBroker(broker) as br, TestApp(app):
        br.subscribers[1].calls[0].handler.mock.assert_called_once_with(
            {"name": "John", "user_id": 1},
        )

        br.publishers[0].mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_aiopika
async def test_delay_router_rabbit() -> None:
    from docs.docs_src.getting_started.routers.rabbit.delay_equal import (
        broker as control_broker,
    )
    from docs.docs_src.getting_started.routers.rabbit.router_delay import (
        app,
        broker,
    )
    from faststream.rabbit import TestRabbitBroker

    gc.collect()

    assert len(broker.subscribers) == len(control_broker.subscribers) == 1
    assert len(broker.publishers) == len(control_broker.publishers) == 1

    async with TestRabbitBroker(broker) as br, TestApp(app):
        br.subscribers[1].calls[0].handler.mock.assert_called_once_with(
            {"name": "John", "user_id": 1},
        )

        br.publishers[0].mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_nats
async def test_delay_router_nats() -> None:
    from docs.docs_src.getting_started.routers.nats.delay_equal import (
        broker as control_broker,
    )
    from docs.docs_src.getting_started.routers.nats.router_delay import (
        app,
        broker,
    )
    from faststream.nats import TestNatsBroker

    gc.collect()

    assert len(broker.subscribers) == len(control_broker.subscribers) == 1
    assert len(broker.publishers) == len(control_broker.publishers) == 1

    async with TestNatsBroker(broker) as br, TestApp(app):
        br.subscribers[1].calls[0].handler.mock.assert_called_once_with(
            {"name": "John", "user_id": 1},
        )

        br.publishers[0].mock.assert_called_once_with("Hi!")


@pytest.mark.asyncio()
@require_redis
async def test_delay_router_redis() -> None:
    from docs.docs_src.getting_started.routers.redis.delay_equal import (
        broker as control_broker,
    )
    from docs.docs_src.getting_started.routers.redis.router_delay import (
        app,
        broker,
    )
    from faststream.redis import TestRedisBroker

    gc.collect()

    assert len(broker.subscribers) == len(control_broker.subscribers) == 1
    assert len(broker.publishers) == len(control_broker.publishers) == 1

    async with TestRedisBroker(broker) as br, TestApp(app):
        br.subscribers[1].calls[0].handler.mock.assert_called_once_with(
            {"name": "John", "user_id": 1},
        )

        br.publishers[0].mock.assert_called_once_with("Hi!")
