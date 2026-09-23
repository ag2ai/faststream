from unittest.mock import MagicMock

import pytest
from aiokafka import ConsumerRebalanceListener, TopicPartition

from faststream.kafka.helpers.rebalance_listener import (
    _LoggingListener,
    _LoggingListenerFacade,
)


@pytest.mark.asyncio()
async def test_facade_notifies_both_listeners_on_partition_assignment() -> None:
    logging_listener = MagicMock(spec=_LoggingListener)
    custom_listener = MagicMock(spec=ConsumerRebalanceListener)
    facade = _LoggingListenerFacade(
        logging_listener=logging_listener,
        listener=custom_listener,
    )
    assigned = {TopicPartition("topic", 0)}

    await facade.on_partitions_assigned(assigned)

    logging_listener.on_partitions_assigned.assert_awaited_once_with(assigned)
    logging_listener.on_partitions_revoked.assert_not_called()
    custom_listener.on_partitions_assigned.assert_called_once_with(assigned)
