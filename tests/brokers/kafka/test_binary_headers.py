import pytest

from tests.brokers.base.header import (
    BinaryHeadersPublishTestcase,
    BinaryHeadersTestcase,
)

from .basic import KafkaMemoryTestcaseConfig, KafkaTestcaseConfig


@pytest.mark.kafka()
class TestBinaryHeaders(KafkaMemoryTestcaseConfig, BinaryHeadersTestcase):
    pass


@pytest.mark.kafka()
@pytest.mark.connected()
class TestBinaryHeadersConnected(KafkaTestcaseConfig, BinaryHeadersPublishTestcase):
    pass
