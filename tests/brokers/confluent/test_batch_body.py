from typing import Any

import pytest

from faststream.confluent.response import KafkaPublishCommand, KafkaPublishMessage
from tests.brokers.base.publish_command import BatchKeysTestcase

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
class TestBatchKeys(ConfluentMemoryTestcaseConfig, BatchKeysTestcase):
    publish_command_cls = KafkaPublishCommand
    publish_message_cls = KafkaPublishMessage

    @staticmethod
    def get_message_key(raw_message: Any) -> bytes:
        return raw_message.key()
