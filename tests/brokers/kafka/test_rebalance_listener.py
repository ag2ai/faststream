import logging
from typing import Any
from unittest.mock import MagicMock

import pytest
from aiokafka import TopicPartition

from faststream.kafka.helpers import make_logging_listener


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_assignment_is_logged_beside_a_custom_listener() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3202."""
    logger = MagicMock()
    custom_listener = MagicMock()
    consumer: Any = MagicMock()
    consumer._coordinator.member_id = "member-1"

    listener = make_logging_listener(
        consumer=consumer,
        logger=logger,
        log_extra={},
        listener=custom_listener,
    )
    assert listener

    assigned = {TopicPartition("topic", 0)}
    await listener.on_partitions_assigned(assigned)

    logger.log.assert_called_once_with(
        logging.INFO,
        "Consumer member-1 assigned to partitions: "
        "(TopicPartition(topic='topic', partition=0))",
        extra={},
    )
    custom_listener.on_partitions_assigned.assert_called_once_with(assigned)
