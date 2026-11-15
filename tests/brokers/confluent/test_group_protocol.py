from unittest.mock import MagicMock

import pytest

from faststream._internal.logger.state import LoggerState
from faststream.confluent.helpers import config as config_module
from faststream.confluent.helpers.client import AsyncConfluentConsumer

CLASSIC_ONLY = (
    "partition.assignment.strategy",
    "session.timeout.ms",
    "heartbeat.interval.ms",
)


def _consumer(**consumer_config: object) -> AsyncConfluentConsumer:
    return AsyncConfluentConsumer(
        config=config_module.ConfluentFastConfig(
            config=config_module.ConfluentConfig(**consumer_config),
        ),
        logger=LoggerState(),
        admin_service=MagicMock(),
        partitions=[],
        group_id="test-group",
    )


@pytest.mark.confluent()
def test_classic_protocol_keeps_its_keys() -> None:
    consumer = _consumer()

    for key in CLASSIC_ONLY:
        assert key in consumer.config

    assert "group.protocol" not in consumer.config


@pytest.mark.confluent()
def test_consumer_protocol_drops_the_classic_only_keys() -> None:
    consumer = _consumer(**{"group.protocol": "consumer"})

    for key in CLASSIC_ONLY:
        assert key not in consumer.config

    assert consumer.config["group.protocol"] == "consumer"


@pytest.mark.confluent()
def test_consumer_protocol_keeps_the_remote_assignor() -> None:
    consumer = _consumer(
        **{"group.protocol": "consumer", "group.remote.assignor": "uniform"},
    )

    assert consumer.config["group.remote.assignor"] == "uniform"
    assert "partition.assignment.strategy" not in consumer.config


@pytest.mark.confluent()
def test_consumer_protocol_keeps_max_poll_interval() -> None:
    consumer = _consumer(**{"group.protocol": "consumer"})

    assert "max.poll.interval.ms" in consumer.config
