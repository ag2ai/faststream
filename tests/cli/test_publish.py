from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

from typer.testing import CliRunner

from faststream import FastStream
from faststream._internal.cli.main import cli as faststream_app
from faststream.response.publish_type import PublishType
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_nats,
    require_redis,
)

if TYPE_CHECKING:
    from faststream.confluent.response import (
        KafkaPublishCommand as ConfluentPublishCommand,
    )
    from faststream.kafka.response import KafkaPublishCommand
    from faststream.nats.response import NatsPublishCommand
    from faststream.rabbit.response import RabbitPublishCommand
    from faststream.redis.response import RedisPublishCommand


def get_mock_app(broker_type: Any, producer_type: Any) -> tuple[FastStream, AsyncMock]:
    broker = broker_type()
    broker.connect = AsyncMock()
    mock_producer = AsyncMock(spec=producer_type)
    mock_producer.publish = AsyncMock()
    mock_producer._parser = MagicMock(return_value=None)
    mock_producer._decoder = MagicMock(return_value=None)
    broker.config.broker_config.producer = mock_producer

    broker.config.broker_config.broker_decoder = MagicMock(return_value=None)

    return FastStream(broker), mock_producer


@require_redis
def test_publish_command_with_redis_options(runner: CliRunner) -> None:
    from faststream.redis import RedisBroker
    from faststream.redis.publisher.producer import RedisFastProducer

    mock_app, producer_mock = get_mock_app(RedisBroker, RedisFastProducer)

    with patch(
        "faststream._internal.cli.utils.imports._import_object_or_factory",
        return_value=(None, mock_app),
    ):
        result = runner.invoke(
            faststream_app,
            [
                "publish",
                "fastream:app",
                "hello world",
                "--channel",
                "channelname",
                "--reply_to",
                "tester",
                "--correlation_id",
                "someId",
            ],
        )

        assert result.exit_code == 0

        cmd: RedisPublishCommand = producer_mock.publish.call_args.args[0]
        assert cmd.body == "hello world"
        assert cmd.reply_to == "tester"
        assert cmd.destination == "channelname"
        assert cmd.correlation_id == "someId"


@require_confluent
def test_publish_command_with_confluent_options(runner: CliRunner) -> None:
    from faststream.confluent import KafkaBroker as ConfluentBroker
    from faststream.confluent.publisher.producer import AsyncConfluentFastProducer

    mock_app, producer_mock = get_mock_app(ConfluentBroker, AsyncConfluentFastProducer)

    with patch(
        "faststream._internal.cli.utils.imports._import_object_or_factory",
        return_value=(None, mock_app),
    ):
        result = runner.invoke(
            faststream_app,
            [
                "publish",
                "fastream:app",
                "hello world",
                "--topic",
                "topicname",
                "--correlation_id",
                "someId",
            ],
        )

        assert result.exit_code == 0

        cmd: ConfluentPublishCommand = producer_mock.publish.call_args.args[0]
        assert cmd.body == "hello world"
        assert cmd.destination == "topicname"
        assert cmd.correlation_id == "someId"


@require_aiokafka
def test_publish_command_with_kafka_options(runner: CliRunner) -> None:
    from faststream.kafka import KafkaBroker
    from faststream.kafka.publisher.producer import AioKafkaFastProducer

    mock_app, producer_mock = get_mock_app(KafkaBroker, AioKafkaFastProducer)

    with patch(
        "faststream._internal.cli.utils.imports._import_object_or_factory",
        return_value=(None, mock_app),
    ):
        result = runner.invoke(
            faststream_app,
            [
                "publish",
                "fastream:app",
                "hello world",
                "--topic",
                "topicname",
                "--correlation_id",
                "someId",
            ],
        )

        assert result.exit_code == 0

        cmd: KafkaPublishCommand = producer_mock.publish.call_args.args[0]
        assert cmd.body == "hello world"
        assert cmd.destination == "topicname"
        assert cmd.correlation_id == "someId"


@require_nats
def test_publish_command_with_nats_options(runner: CliRunner) -> None:
    from faststream.nats import NatsBroker
    from faststream.nats.publisher.producer import NatsFastProducer

    mock_app, producer_mock = get_mock_app(NatsBroker, NatsFastProducer)

    with patch(
        "faststream._internal.cli.utils.imports._import_object_or_factory",
        return_value=(None, mock_app),
    ):
        result = runner.invoke(
            faststream_app,
            [
                "publish",
                "fastream:app",
                "hello world",
                "--subject",
                "subjectname",
                "--reply_to",
                "tester",
                "--correlation_id",
                "someId",
            ],
        )

        assert result.exit_code == 0

        cmd: NatsPublishCommand = producer_mock.publish.call_args.args[0]
        assert cmd.body == "hello world"
        assert cmd.destination == "subjectname"
        assert cmd.reply_to == "tester"
        assert cmd.correlation_id == "someId"


@require_aiopika
def test_publish_command_with_rabbit_options(runner: CliRunner) -> None:
    from faststream.rabbit import RabbitBroker
    from faststream.rabbit.publisher.producer import AioPikaFastProducer

    mock_app, producer_mock = get_mock_app(RabbitBroker, AioPikaFastProducer)

    with patch(
        "faststream._internal.cli.utils.imports._import_object_or_factory",
        return_value=(None, mock_app),
    ):
        result = runner.invoke(
            faststream_app,
            [
                "publish",
                "fastream:app",
                "hello world",
                "--queue",
                "queuename",
                "--correlation_id",
                "someId",
            ],
        )

        assert result.exit_code == 0

        cmd: RabbitPublishCommand = producer_mock.publish.call_args.args[0]
        assert cmd.body == "hello world"
        assert cmd.destination == "queuename"
        assert cmd.correlation_id == "someId"


@require_nats
def test_publish_nats_request_command(runner: CliRunner) -> None:
    from faststream.nats import NatsBroker
    from faststream.nats.publisher.producer import NatsFastProducer

    mock_app, producer_mock = get_mock_app(NatsBroker, NatsFastProducer)

    with patch(
        "faststream._internal.cli.utils.imports._import_object_or_factory",
        return_value=(None, mock_app),
    ):
        runner.invoke(
            faststream_app,
            [
                "publish",
                "fastream:app",
                "hello world",
                "--subject",
                "subjectname",
                "--rpc",
                "--timeout",
                "1.0",
            ],
        )

        cmd: NatsPublishCommand = producer_mock.request.call_args.args[0]

        assert cmd.destination == "subjectname"
        assert cmd.timeout == 1.0
        assert cmd.publish_type is PublishType.REQUEST
