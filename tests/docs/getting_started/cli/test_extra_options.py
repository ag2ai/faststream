import logging
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest
from typer.testing import CliRunner

from faststream import FastStream, TestApp
from faststream._internal.cli.main import cli
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)

# `faststream run main:app --port 5000 --foo bar`, as the page spells it
OPTIONS = ("--port", "5000", "--foo", "bar")
OUTPUT = "Port: 5000\nFoo: bar\n"


def run_app(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    patched_broker: AbstractAsyncContextManager[Any],
    app_path: str,
    *options: str,
) -> str:
    """Run `faststream run <app_path> <options>` and return what the app printed."""

    async def run(
        self: FastStream,
        log_level: int = logging.INFO,
        run_extra_options: dict[str, Any] | None = None,
    ) -> None:
        # the real `run` blocks until a signal: start the app against an
        # in-memory broker, run its hooks with the options the CLI parsed, stop it
        async with patched_broker, TestApp(self, run_extra_options):
            pass

    with monkeypatch.context() as patch:
        patch.setattr(FastStream, "run", run)
        result = runner.invoke(cli, ["run", app_path, *options])

    assert result.exit_code == 0, result.output
    return result.output


@pytest.mark.kafka()
@require_aiokafka
def test_kafka_extra_options(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from docs.docs_src.getting_started.cli.kafka.extra_options import broker
    from faststream.kafka import TestKafkaBroker

    assert (
        run_app(
            runner,
            monkeypatch,
            TestKafkaBroker(broker),
            "docs.docs_src.getting_started.cli.kafka.extra_options:app",
            *OPTIONS,
        )
        == OUTPUT
    )


@pytest.mark.confluent()
@require_confluent
def test_confluent_extra_options(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from docs.docs_src.getting_started.cli.confluent.extra_options import broker
    from faststream.confluent import TestKafkaBroker as TestConfluentKafkaBroker

    assert (
        run_app(
            runner,
            monkeypatch,
            TestConfluentKafkaBroker(broker),
            "docs.docs_src.getting_started.cli.confluent.extra_options:app",
            *OPTIONS,
        )
        == OUTPUT
    )


@pytest.mark.rabbit()
@require_aiopika
def test_rabbit_extra_options(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from docs.docs_src.getting_started.cli.rabbit.extra_options import broker
    from faststream.rabbit import TestRabbitBroker

    assert (
        run_app(
            runner,
            monkeypatch,
            TestRabbitBroker(broker),
            "docs.docs_src.getting_started.cli.rabbit.extra_options:app",
            *OPTIONS,
        )
        == OUTPUT
    )


@pytest.mark.nats()
@require_nats
def test_nats_extra_options(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from docs.docs_src.getting_started.cli.nats.extra_options import broker
    from faststream.nats import TestNatsBroker

    assert (
        run_app(
            runner,
            monkeypatch,
            TestNatsBroker(broker),
            "docs.docs_src.getting_started.cli.nats.extra_options:app",
            *OPTIONS,
        )
        == OUTPUT
    )


@pytest.mark.redis()
@require_redis
def test_redis_extra_options(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from docs.docs_src.getting_started.cli.redis.extra_options import broker
    from faststream.redis import TestRedisBroker

    assert (
        run_app(
            runner,
            monkeypatch,
            TestRedisBroker(broker),
            "docs.docs_src.getting_started.cli.redis.extra_options:app",
            *OPTIONS,
        )
        == OUTPUT
    )


@pytest.mark.mqtt()
@require_mqtt
def test_mqtt_extra_options(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from docs.docs_src.getting_started.cli.mqtt.extra_options import broker
    from faststream.mqtt import TestMQTTBroker

    assert (
        run_app(
            runner,
            monkeypatch,
            TestMQTTBroker(broker),
            "docs.docs_src.getting_started.cli.mqtt.extra_options:app",
            *OPTIONS,
        )
        == OUTPUT
    )
