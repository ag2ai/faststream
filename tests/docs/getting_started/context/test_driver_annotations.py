import pytest

from faststream import TestApp
from faststream.exceptions import SetupError
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
async def test_driver_annotations_kafka() -> None:
    from docs.docs_src.getting_started.context.kafka.driver_annotations import (
        app,
        broker,
    )
    from faststream.kafka import TestKafkaBroker

    with pytest.raises(SetupError, match=r"from app\.dependencies import StorageClient"):
        async with TestKafkaBroker(broker), TestApp(app):
            pass


@pytest.mark.asyncio()
@require_confluent
async def test_driver_annotations_confluent() -> None:
    from docs.docs_src.getting_started.context.confluent.driver_annotations import (
        app,
        broker,
    )
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    with pytest.raises(SetupError, match=r"from app\.dependencies import StorageClient"):
        async with TestConfluentKafkaBroker(broker), TestApp(app):
            pass


@pytest.mark.asyncio()
@require_aiopika
async def test_driver_annotations_rabbit() -> None:
    from docs.docs_src.getting_started.context.rabbit.driver_annotations import (
        app,
        broker,
    )
    from faststream.rabbit import TestRabbitBroker

    with pytest.raises(SetupError, match=r"from app\.dependencies import StorageClient"):
        async with TestRabbitBroker(broker), TestApp(app):
            pass


@pytest.mark.asyncio()
@require_nats
async def test_driver_annotations_nats() -> None:
    from docs.docs_src.getting_started.context.nats.driver_annotations import (
        app,
        broker,
    )
    from faststream.nats import TestNatsBroker

    with pytest.raises(SetupError, match=r"from app\.dependencies import StorageClient"):
        async with TestNatsBroker(broker), TestApp(app):
            pass


@pytest.mark.asyncio()
@require_redis
async def test_driver_annotations_redis() -> None:
    from docs.docs_src.getting_started.context.redis.driver_annotations import (
        app,
        broker,
    )
    from faststream.redis import TestRedisBroker

    with pytest.raises(SetupError, match=r"from app\.dependencies import StorageClient"):
        async with TestRedisBroker(broker), TestApp(app):
            pass


@pytest.mark.asyncio()
@require_mqtt
async def test_driver_annotations_mqtt() -> None:
    from docs.docs_src.getting_started.context.mqtt.driver_annotations import (
        app,
        broker,
    )
    from faststream.mqtt import TestMQTTBroker

    with pytest.raises(SetupError, match=r"from app\.dependencies import StorageClient"):
        async with TestMQTTBroker(broker), TestApp(app):
            pass
