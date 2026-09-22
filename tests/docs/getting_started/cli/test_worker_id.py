import pytest
from typer.testing import CliRunner

from tests.docs.getting_started.cli.run_app import run_app
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)

# `faststream run main:app` has no worker to number; `--workers 2` numbers both
SINGLE_PROCESS = "Started in a single process\n"
WORKERS = ("--workers", "2")
WORKERS_OUTPUT = "Worker 0 started\nWorker 1 started\n"


@pytest.mark.kafka()
@require_aiokafka
def test_kafka_worker_id(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from docs.docs_src.getting_started.cli.kafka.worker_id import broker
    from faststream.kafka import TestKafkaBroker

    app_path = "docs.docs_src.getting_started.cli.kafka.worker_id:app"

    assert run_app(runner, monkeypatch, TestKafkaBroker(broker), app_path) == (
        SINGLE_PROCESS
    )
    assert (
        run_app(runner, monkeypatch, TestKafkaBroker(broker), app_path, *WORKERS)
        == WORKERS_OUTPUT
    )


@pytest.mark.confluent()
@require_confluent
def test_confluent_worker_id(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from docs.docs_src.getting_started.cli.confluent.worker_id import broker
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    app_path = "docs.docs_src.getting_started.cli.confluent.worker_id:app"

    assert run_app(
        runner,
        monkeypatch,
        TestConfluentKafkaBroker(broker),
        app_path,
    ) == (SINGLE_PROCESS)
    assert (
        run_app(
            runner,
            monkeypatch,
            TestConfluentKafkaBroker(broker),
            app_path,
            *WORKERS,
        )
        == WORKERS_OUTPUT
    )


@pytest.mark.rabbit()
@require_aiopika
def test_rabbit_worker_id(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from docs.docs_src.getting_started.cli.rabbit.worker_id import broker
    from faststream.rabbit import TestRabbitBroker

    app_path = "docs.docs_src.getting_started.cli.rabbit.worker_id:app"

    assert run_app(runner, monkeypatch, TestRabbitBroker(broker), app_path) == (
        SINGLE_PROCESS
    )
    assert (
        run_app(runner, monkeypatch, TestRabbitBroker(broker), app_path, *WORKERS)
        == WORKERS_OUTPUT
    )


@pytest.mark.nats()
@require_nats
def test_nats_worker_id(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from docs.docs_src.getting_started.cli.nats.worker_id import broker
    from faststream.nats import TestNatsBroker

    app_path = "docs.docs_src.getting_started.cli.nats.worker_id:app"

    assert run_app(runner, monkeypatch, TestNatsBroker(broker), app_path) == (
        SINGLE_PROCESS
    )
    assert (
        run_app(runner, monkeypatch, TestNatsBroker(broker), app_path, *WORKERS)
        == WORKERS_OUTPUT
    )


@pytest.mark.redis()
@require_redis
def test_redis_worker_id(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from docs.docs_src.getting_started.cli.redis.worker_id import broker
    from faststream.redis import TestRedisBroker

    app_path = "docs.docs_src.getting_started.cli.redis.worker_id:app"

    assert run_app(runner, monkeypatch, TestRedisBroker(broker), app_path) == (
        SINGLE_PROCESS
    )
    assert (
        run_app(runner, monkeypatch, TestRedisBroker(broker), app_path, *WORKERS)
        == WORKERS_OUTPUT
    )


@pytest.mark.mqtt()
@require_mqtt
def test_mqtt_worker_id(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    from docs.docs_src.getting_started.cli.mqtt.worker_id import broker
    from faststream.mqtt import TestMQTTBroker

    app_path = "docs.docs_src.getting_started.cli.mqtt.worker_id:app"

    assert run_app(runner, monkeypatch, TestMQTTBroker(broker), app_path) == (
        SINGLE_PROCESS
    )
    assert (
        run_app(runner, monkeypatch, TestMQTTBroker(broker), app_path, *WORKERS)
        == WORKERS_OUTPUT
    )
