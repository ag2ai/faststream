import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from aiokafka.structs import RecordMetadata

from faststream import Context
from faststream.kafka import KafkaPublishMessage, KafkaResponse
from faststream.kafka.exceptions import BatchBufferOverflowException
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import SqlaTestcaseConfig


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
class TestPublish(SqlaTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_reply_to(self) -> None:
        ...
    
    @pytest.mark.asyncio()
    async def test_no_reply(self) -> None:
        ...