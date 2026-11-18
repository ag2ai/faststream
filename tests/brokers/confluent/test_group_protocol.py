from unittest.mock import MagicMock

import pytest
from dirty_equals import Contains, IsPartialDict

from faststream._internal.logger.state import LoggerState
from faststream.confluent.helpers.client import AsyncConfluentConsumer
from faststream.confluent.helpers.config import ConfluentConfig, ConfluentFastConfig
from tests.marks import require_confluent_v240

CLASSIC_ONLY = (
    "partition.assignment.strategy",
    "session.timeout.ms",
    "heartbeat.interval.ms",
)


def _consumer(
    consumer_config: ConfluentConfig | None = None,
) -> AsyncConfluentConsumer:
    return AsyncConfluentConsumer(
        config=ConfluentFastConfig(
            config=consumer_config,
        ),
        logger=LoggerState(),
        admin_service=MagicMock(),
        partitions=[],
        group_id="test-group",
    )


@pytest.mark.confluent()
def test_classic_protocol_keeps_its_keys() -> None:
    consumer = _consumer()

    assert consumer.config == IsPartialDict({
        "partition.assignment.strategy": "roundrobin",
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
    })
    assert "group.protocol" not in consumer.config


@pytest.mark.confluent()
@require_confluent_v240
def test_consumer_protocol_drops_the_classic_only_keys() -> None:
    consumer = _consumer({"group.protocol": "consumer"})

    assert consumer.config != Contains(*CLASSIC_ONLY)
    assert consumer.config == IsPartialDict({"group.protocol": "consumer"})


@pytest.mark.confluent()
@require_confluent_v240
def test_consumer_protocol_keeps_the_remote_assignor() -> None:
    consumer = _consumer(
        {"group.protocol": "consumer", "group.remote.assignor": "uniform"},
    )

    assert consumer.config == IsPartialDict({
        "group.protocol": "consumer",
        "group.remote.assignor": "uniform",
    })


@pytest.mark.confluent()
@require_confluent_v240
def test_consumer_protocol_keeps_max_poll_interval() -> None:
    consumer = _consumer({
        "group.protocol": "consumer",
        "max.poll.interval.ms": 123456,
    })

    assert consumer.config == IsPartialDict({
        "group.protocol": "consumer",
        "max.poll.interval.ms": 123456,
    })
