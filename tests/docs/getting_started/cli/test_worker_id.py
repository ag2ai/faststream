from typing import Any

import pytest

from faststream import TestApp
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)

# `faststream run main:app --workers 3` numbers the children 0..2 and passes
# each its own id; tests/cli/test_worker_id_extra_option.py spawns them
WORKER_ID: dict[str, Any] = {"worker_id": 0}
OUTPUT = "Worker 0 started\n"


@pytest.mark.kafka()
@pytest.mark.asyncio()
@require_aiokafka
async def test_kafka_worker_id(capsys: pytest.CaptureFixture[str]) -> None:
    from docs.docs_src.getting_started.cli.kafka.worker_id import app, broker
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker), TestApp(app, WORKER_ID):
        pass

    assert capsys.readouterr().out == OUTPUT


@pytest.mark.confluent()
@pytest.mark.asyncio()
@require_confluent
async def test_confluent_worker_id(capsys: pytest.CaptureFixture[str]) -> None:
    from docs.docs_src.getting_started.cli.confluent.worker_id import app, broker
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    async with TestConfluentKafkaBroker(broker), TestApp(app, WORKER_ID):
        pass

    assert capsys.readouterr().out == OUTPUT


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@require_aiopika
async def test_rabbit_worker_id(capsys: pytest.CaptureFixture[str]) -> None:
    from docs.docs_src.getting_started.cli.rabbit.worker_id import app, broker
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app, WORKER_ID):
        pass

    assert capsys.readouterr().out == OUTPUT


@pytest.mark.nats()
@pytest.mark.asyncio()
@require_nats
async def test_nats_worker_id(capsys: pytest.CaptureFixture[str]) -> None:
    from docs.docs_src.getting_started.cli.nats.worker_id import app, broker
    from faststream.nats import TestNatsBroker

    async with TestNatsBroker(broker), TestApp(app, WORKER_ID):
        pass

    assert capsys.readouterr().out == OUTPUT


@pytest.mark.redis()
@pytest.mark.asyncio()
@require_redis
async def test_redis_worker_id(capsys: pytest.CaptureFixture[str]) -> None:
    from docs.docs_src.getting_started.cli.redis.worker_id import app, broker
    from faststream.redis import TestRedisBroker

    async with TestRedisBroker(broker), TestApp(app, WORKER_ID):
        pass

    assert capsys.readouterr().out == OUTPUT


@pytest.mark.mqtt()
@pytest.mark.asyncio()
@require_mqtt
async def test_mqtt_worker_id(capsys: pytest.CaptureFixture[str]) -> None:
    from docs.docs_src.getting_started.cli.mqtt.worker_id import app, broker
    from faststream.mqtt import TestMQTTBroker

    async with TestMQTTBroker(broker), TestApp(app, WORKER_ID):
        pass

    assert capsys.readouterr().out == OUTPUT
