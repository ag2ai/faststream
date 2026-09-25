import pytest

from faststream.kafka.response import KafkaPublishCommand, KafkaPublishMessage
from tests.brokers.base.publish_command import BatchKeysTestcase

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
class TestBatchKeys(KafkaMemoryTestcaseConfig, BatchKeysTestcase):
    publish_command_cls = KafkaPublishCommand
    publish_message_cls = KafkaPublishMessage
